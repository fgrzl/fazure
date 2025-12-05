package main

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/fgrzl/fazure/internal/blobs"
	"github.com/fgrzl/fazure/internal/common"
	"github.com/fgrzl/fazure/internal/queues"
	"github.com/fgrzl/fazure/internal/tables"
)

func main() {
	// Initialize logger with configurable log level
	logLevel := getLogLevel(os.Getenv("LOG_LEVEL"))
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	// Port configuration (matching Azurite defaults)
	const (
		blobPort  = "10000"
		queuePort = "10001"
		tablePort = "10002"
	)

	datadir := os.Getenv("DATA_DIR")
	if datadir == "" {
		datadir = filepath.Join(os.TempDir(), "data")
	}

	// Initialize shared Pebble store
	store, err := common.NewStore(datadir)
	if err != nil {
		logger.Error("failed to initialize storage", "path", datadir, "error", err)
		os.Exit(1)
	}
	defer store.Close()

	logger.Info("pebble database initialized", "path", datadir)

	// Get the underlying Pebble DB
	db := store.DB()

	// Initialize handlers
	blobHandler := blobs.NewHandler(db, datadir, logger)
	queueHandler := queues.NewHandler(db, logger)
	tableMetrics := tables.NewMetrics()

	// Initialize table store and handler
	tableStore, err := tables.NewTableStore(store, logger, tableMetrics)
	if err != nil {
		logger.Error("failed to initialize table storage", "error", err)
		os.Exit(1)
	}
	tableHandler := tables.NewHandler(tableStore, logger)

	// Create separate mux for each service
	blobMux := http.NewServeMux()
	queueMux := http.NewServeMux()
	tableMux := http.NewServeMux()

	// Health check endpoints
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
	blobMux.HandleFunc("/health", healthHandler)
	queueMux.HandleFunc("/health", healthHandler)
	tableMux.HandleFunc("/health", healthHandler)

	// Debug metrics endpoint
	metricsHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(store.Metrics()))
		w.Write([]byte("\n\n[tables]\n"))
		w.Write([]byte(tableMetrics.String()))
	}
	tableMux.HandleFunc("/debug/metrics", metricsHandler)

	// Request logging middleware
	logRequest := func(service string, next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			logger.Debug("request received",
				"service", service,
				"method", r.Method,
				"path", r.URL.Path,
				"query", r.URL.RawQuery,
				"contentLength", r.ContentLength,
			)
			next(w, r)
		}
	}

	// Route all requests to appropriate handlers with logging
	blobMux.HandleFunc("/", logRequest("blob", blobHandler.HandleRequest))
	queueMux.HandleFunc("/", logRequest("queue", queueHandler.HandleRequest))
	tableMux.HandleFunc("/", logRequest("table", tableHandler.HandleRequest))

	// Start servers
	errChan := make(chan error, 3)

	// Blob service
	go func() {
		logger.Info("starting blob service", "port", blobPort)
		srv := &http.Server{Addr: ":" + blobPort, Handler: blobMux}
		errChan <- srv.ListenAndServe()
	}()

	// Queue service
	go func() {
		logger.Info("starting queue service", "port", queuePort)
		srv := &http.Server{Addr: ":" + queuePort, Handler: queueMux}
		errChan <- srv.ListenAndServe()
	}()

	// Table service
	go func() {
		logger.Info("starting table service", "port", tablePort)
		srv := &http.Server{Addr: ":" + tablePort, Handler: tableMux}
		errChan <- srv.ListenAndServe()
	}()

	logger.Info("fazure storage emulator started",
		"blobService", "http://localhost:"+blobPort,
		"queueService", "http://localhost:"+queuePort,
		"tableService", "http://localhost:"+tablePort,
		"dataDir", datadir,
	)

	// Wait for any server to fail
	if err := <-errChan; err != nil {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}

// getLogLevel parses LOG_LEVEL env var into slog.Level
func getLogLevel(level string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return slog.LevelInfo // default
	}
	return l
}

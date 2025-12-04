package main

import (
	"fmt"
	"log"
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
	// Initialize logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
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
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	logger.Info("pebble database opened", "path", datadir)

	// Get the underlying Pebble DB
	db := store.DB()

	// Initialize handlers
	blobHandler := blobs.NewHandler(db, logger)
	queueHandler := queues.NewHandler(db, logger)

	// Initialize table store and handler
	tableStore, err := tables.NewTableStore(store)
	if err != nil {
		log.Fatalf("Failed to initialize table storage: %v", err)
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

	// Route all requests to appropriate handlers
	blobMux.HandleFunc("/", blobHandler.HandleRequest)
	queueMux.HandleFunc("/", queueHandler.HandleRequest)
	tableMux.HandleFunc("/", tableHandler.HandleRequest)

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

	fmt.Println("Azure Storage Emulator started")
	fmt.Printf("  Blob service:  http://localhost:%s\n", blobPort)
	fmt.Printf("  Queue service: http://localhost:%s\n", queuePort)
	fmt.Printf("  Table service: http://localhost:%s\n", tablePort)
	fmt.Printf("  Data directory: %s\n", datadir)

	// Wait for any server to fail
	log.Fatal(<-errChan)
}

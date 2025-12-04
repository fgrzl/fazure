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

	port := os.Getenv("PORT")
	if port == "" {
		port = "10000"
	}

	datadir := os.Getenv("DATA_DIR")
	if datadir == "" {
		datadir = filepath.Join(os.TempDir(), "fauxtable")
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

	// Initialize table store
	tableStore, err := tables.NewTableStore(store)
	if err != nil {
		log.Fatalf("Failed to initialize table storage: %v", err)
	}
	tables.SetStore(tableStore)

	// Create main mux
	mux := http.NewServeMux()

	// Register blob routes
	blobHandler.RegisterRoutes(mux)

	// Register queue routes
	queueHandler.RegisterRoutes(mux)

	// Azure Table Storage API endpoints
	// https://learn.microsoft.com/en-us/rest/api/storageservices/table-service-rest-api
	tables.RegisterRoutes(mux)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	logger.Info("starting azure storage emulator", "port", port, "datadir", datadir)
	fmt.Printf("Starting Azure Storage Emulator on port %s\n", port)
	fmt.Printf("Data directory: %s\n", datadir)
	log.Fatal(srv.ListenAndServe())
}

package main

import (
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

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

	// Initialize table store
	tableStore, err := tables.NewTableStore(store)
	if err != nil {
		log.Fatalf("Failed to initialize table storage: %v", err)
	}
	tables.SetStore(tableStore)

	// Create main mux
	mux := http.NewServeMux()

	// Health check endpoint - register first before "/" dispatcher
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Register blob routes
	blobHandler.RegisterRoutes(mux)

	// Register queue routes
	queueHandler.RegisterRoutes(mux)

	// Azure Table Storage API endpoints
	// https://learn.microsoft.com/en-us/rest/api/storageservices/table-service-rest-api
	tables.RegisterRoutes(mux)

	// Dispatcher for storage operations
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Dispatch to the appropriate handler based on path and query parameters
		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")
		
		// If first part is account name, it's a storage service request
		if len(parts) > 0 && parts[0] == "devstoreaccount1" {
			// Route to handlers based on path structure
			if len(parts) == 1 || (len(parts) == 2 && parts[1] == "") {
				// Account-level operation (listing or root)
				if strings.Contains(r.URL.RawQuery, "comp=list") {
					blobHandler.HandleRequest(w, r)
				} else {
					blobHandler.HandleRequest(w, r)
				}
			} else if len(parts) >= 2 {
				// Determine service by path depth and query params
				// Tables are: /devstoreaccount1/Tables or /devstoreaccount1/TableName
				// Queues are: /devstoreaccount1/queuename
				// Blobs are: /devstoreaccount1/containername/blobname
				
				secondPart := strings.ToLower(parts[1])
				if secondPart == "tables" {
					// Table Service request
					tables.HandleRequest(w, r)
				} else if strings.Contains(r.URL.RawQuery, "comp=queue") || r.Method == "POST" && strings.Contains(r.URL.RawQuery, "messages") {
					// Queue operation
					queueHandler.HandleRequest(w, r)
				} else {
					// Default to blob (containers or blobs)
					blobHandler.HandleRequest(w, r)
				}
			}
		} else {
			// Unknown request, try blob as default
			blobHandler.HandleRequest(w, r)
		}
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

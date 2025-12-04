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

	// Initialize table store and handler
	tableStore, err := tables.NewTableStore(store)
	if err != nil {
		log.Fatalf("Failed to initialize table storage: %v", err)
	}
	tableHandler := tables.NewHandler(tableStore, logger)

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

	// Register table routes
	tableHandler.RegisterRoutes(mux)

	// Dispatcher for storage operations
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Dispatch to the appropriate handler based on path and query parameters
		path := strings.Trim(r.URL.Path, "/")
		parts := strings.Split(path, "/")
		query := r.URL.RawQuery

		// If first part is account name, it's a storage service request
		if len(parts) > 0 && parts[0] == "devstoreaccount1" {
			// Route to handlers based on path structure
			if len(parts) == 1 || (len(parts) == 2 && parts[1] == "") {
				// Account-level operation (listing or root)
				blobHandler.HandleRequest(w, r)
			} else if len(parts) >= 2 {
				// Determine service by path depth and query params
				// Tables are: /devstoreaccount1/Tables or /devstoreaccount1/TableName(...)
				// Queues are: /devstoreaccount1/queuename/messages
				// Blobs are: /devstoreaccount1/containername/blobname

				secondPart := strings.ToLower(parts[1])

				// Check for table operations
				if secondPart == "tables" || strings.HasPrefix(secondPart, "tables(") || secondPart == "$batch" {
					tableHandler.HandleRequest(w, r)
					return
				}

				// Check for queue operations
				// Queues have /messages path or comp=list query with queue patterns
				if len(parts) >= 3 && parts[2] == "messages" {
					queueHandler.HandleRequest(w, r)
					return
				}

				// Check for queue list operation
				if strings.Contains(query, "comp=list") && !strings.Contains(query, "restype=container") {
					// Could be queue list - check if it looks like a queue request
					// Azure Queues use /{account}?comp=list format
					queueHandler.HandleRequest(w, r)
					return
				}

				// Default to blob (containers or blobs)
				blobHandler.HandleRequest(w, r)
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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/storage"
)

func main() {
	err := run(context.Background())
	if err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}

func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Parse command line flags
	var (
		addr = flag.String("addr", ":2379", "Address to listen on")
	)
	flag.Parse()

	// Create storage instance
	store, err := storage.NewMemoryStorage(persistence.NewMemoryLog())
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Create and start the etcd API server
	server := api.NewServer(store)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down server...")
		cancel()
	}()

	// Start the server
	if err := server.Start(ctx, *addr); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

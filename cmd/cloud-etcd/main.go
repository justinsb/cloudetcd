package main

import (
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
	// Parse command line flags
	var (
		addr = flag.String("addr", ":2379", "Address to listen on")
	)
	flag.Parse()

	fmt.Println("Starting cloud-etcd server...")

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
		server.Stop()
	}()

	// Start the server
	fmt.Printf("Starting etcd API server on %s\n", *addr)
	if err := server.Start(*addr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

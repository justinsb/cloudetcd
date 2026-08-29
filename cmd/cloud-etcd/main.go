// Copyright 2026 Justin Santa Barbara
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"justinsb.com/cloudetcd/pkg/api"
	"justinsb.com/cloudetcd/pkg/persistence/logfactory"
	"justinsb.com/cloudetcd/pkg/recording"
	"justinsb.com/cloudetcd/pkg/storage/memorystorage"
	"k8s.io/klog/v2"
)

func main() {
	err := run(context.Background())
	if err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}

func run(ctx context.Context) error {
	// Cancel the context on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Parse command line flag
	addr := ":2379"
	logURI := "memory://"
	flag.StringVar(&addr, "addr", addr, "Address to listen on")
	recordPath := ""
	flag.StringVar(&logURI, "log", logURI, "Log URI")
	flag.StringVar(&recordPath, "record", recordPath, "If set, record all etcd RPCs to this JSONL file (see pkg/recording)")
	klog.InitFlags(nil) // -v=4 logs every RPC
	flag.Parse()

	var serverOpts []api.Option
	if recordPath != "" {
		recorder, err := recording.NewFileRecorder(recordPath)
		if err != nil {
			return err
		}
		defer func() {
			if err := recorder.Close(); err != nil {
				klog.Errorf("closing recording: %v", err)
			}
		}()
		serverOpts = append(serverOpts, api.WithRecorder(recorder))
	}

	// Create log
	log, err := logfactory.NewLog(ctx, logURI)
	if err != nil {
		return fmt.Errorf("failed to create log: %w", err)
	}

	// Create storage instance
	store, err := memorystorage.NewMemoryStorage(log)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	// Create and start the etcd API server
	server := api.NewServer(store, serverOpts...)

	// Start the server
	if err := server.Start(ctx, addr); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

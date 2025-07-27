#!/bin/bash

# Example: Using etcdctl with cloud-etcd
# This script demonstrates how to use the standard etcdctl tool with our cloud-etcd server

echo "Starting cloud-etcd server..."
./cloud-etcd &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo "Testing with etcdctl..."

# Set a key
etcdctl put mykey myvalue

# Get the key
etcdctl get mykey

# Set another key
etcdctl put another-key another-value

# List all keys
etcdctl get --prefix ""

# Delete a key
etcdctl del mykey

# Verify deletion
etcdctl get mykey

# Test range operations
etcdctl put range/key1 value1
etcdctl put range/key2 value2
etcdctl put range/key3 value3

# Get range of keys
etcdctl get --prefix range/

echo "Cleaning up..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Example completed!" 
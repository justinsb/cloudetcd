#!/bin/bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
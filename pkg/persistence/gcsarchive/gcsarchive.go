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

// Package gcsarchive archives log files to Google Cloud Storage: each closed
// log file becomes one object, created with a DoesNotExist precondition so
// that two instances writing the same prefix are detected rather than
// silently overwriting each other.
package gcsarchive

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"github.com/justinsb/identityctl/pkg/workloadidentity"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"justinsb.com/cloudetcd/pkg/persistence"
)

// Archive implements filesystemlog.Archive over a GCS bucket and prefix.
type Archive struct {
	client *storage.Client
	bucket *storage.BucketHandle
	prefix string
}

// New creates an Archive for gs://bucket/prefix.
func New(ctx context.Context, bucket, prefix string) (*Archive, error) {
	client, err := newStorageClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &Archive{client: client, bucket: client.Bucket(bucket), prefix: prefix}, nil
}

// Close releases the client.
func (a *Archive) Close() error {
	return a.client.Close()
}

// List returns the names of the archived files.
func (a *Archive) List(ctx context.Context) ([]string, error) {
	var names []string
	it := a.bucket.Objects(ctx, &storage.Query{Prefix: a.prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing gs://%s/%s: %w", a.bucket.BucketName(), a.prefix, err)
		}
		names = append(names, strings.TrimPrefix(attrs.Name, a.prefix))
	}
	return names, nil
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Upload stores the file at path as object name, unless the archive already
// holds an identical object. A different object under the same name means
// another writer is using this prefix: persistence.ErrRevisionConflict.
func (a *Archive) Upload(ctx context.Context, name string, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	crc := crc32.Checksum(data, crcTable)
	obj := a.bucket.Object(a.prefix + name)

	// Object creation is the commit point: the precondition makes a race
	// with another writer a failure rather than an overwrite.
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.CRC32C = crc
	w.SendCRC32C = true
	if _, err := w.Write(data); err != nil {
		w.Close()
		return a.explainWriteError(ctx, obj, name, crc, int64(len(data)), err)
	}
	if err := w.Close(); err != nil {
		return a.explainWriteError(ctx, obj, name, crc, int64(len(data)), err)
	}
	klog.V(2).Infof("archived %s (%d bytes)", name, len(data))
	return nil
}

// explainWriteError turns a failed object creation into: nil if an
// identical object already exists (this file was archived before, e.g.
// before a crash); ErrRevisionConflict if a different one does; the error
// otherwise.
func (a *Archive) explainWriteError(ctx context.Context, obj *storage.ObjectHandle, name string, crc uint32, size int64, err error) error {
	if !isPreconditionFailed(err) {
		return fmt.Errorf("writing %s to GCS: %w", name, err)
	}
	attrs, aerr := obj.Attrs(ctx)
	if aerr != nil {
		return fmt.Errorf("archive already holds %s but its attributes could not be read: %w", name, aerr)
	}
	if attrs.Size == size && attrs.CRC32C == crc {
		return nil
	}
	return fmt.Errorf("archive already holds a different %s (%d bytes, crc %08x; ours %d bytes, crc %08x): %w",
		name, attrs.Size, attrs.CRC32C, size, crc, persistence.ErrRevisionConflict)
}

// Delete removes object name; a missing object is not an error.
func (a *Archive) Delete(ctx context.Context, name string) error {
	err := a.bucket.Object(a.prefix + name).Delete(ctx)
	if err == nil || errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return fmt.Errorf("deleting %s from GCS: %w", name, err)
}

// Download copies object name to the file at path.
func (a *Archive) Download(ctx context.Context, name string, path string) error {
	r, err := a.bucket.Object(a.prefix + name).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("reading %s from GCS: %w", name, err)
	}
	defer r.Close()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return fmt.Errorf("reading %s from GCS: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// isPreconditionFailed reports whether err is a GCS precondition failure
// (HTTP 412 from the JSON API, FailedPrecondition from the gRPC API).
func isPreconditionFailed(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
		return true
	}
	return status.Code(err) == codes.FailedPrecondition
}

// newStorageClient creates a GCS client. We prefer the gRPC client, but
// emulators (fake-gcs-server, justinsb/objectstorage) speak the JSON API over
// HTTP, so when STORAGE_EMULATOR_HOST is set we use the HTTP client instead.
//
// Credentials come from workloadidentity: if GOOGLE_APPLICATION_CREDENTIALS
// is set it is honored; otherwise a projected Kubernetes service account
// token at the identityctl well-known path is exchanged via workload
// identity federation; otherwise the normal application default credentials
// chain applies.
func newStorageClient(ctx context.Context) (*storage.Client, error) {
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		return storage.NewClient(ctx)
	}
	tokenSource, err := workloadidentity.TokenSource(ctx)
	if err != nil {
		return nil, fmt.Errorf("building GCP token source: %w", err)
	}
	opts := []option.ClientOption{option.WithTokenSource(tokenSource)}
	metricsOpts, err := metricsClientOptions(ctx)
	if err != nil {
		return nil, err
	}
	opts = append(opts, metricsOpts...)
	return storage.NewGRPCClient(ctx, opts...)
}

// metricsClientOptions configures where the storage client's client-side
// metrics go. By default the client exports them to Cloud Monitoring, which
// requires a project ID it cannot discover from federated credentials or on
// non-GCP nodes. So: if an OTLP endpoint is configured via the standard
// OTEL_EXPORTER_OTLP_METRICS_ENDPOINT / OTEL_EXPORTER_OTLP_ENDPOINT
// environment variables, metrics are exported there (an in-cluster
// collector; unix:///path endpoints are supported for collectors listening
// on a Unix domain socket); otherwise client-side metrics are disabled.
func metricsClientOptions(ctx context.Context) ([]option.ClientOption, error) {
	log := klog.FromContext(ctx)

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		return []option.ClientOption{storage.WithDisabledClientMetrics()}, nil
	}

	var otlpExporter sdkmetric.Exporter
	if strings.HasPrefix(endpoint, "unix:") {
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dialing OTLP collector %q: %w", endpoint, err)
		}
		otlpExporter, err = otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
		if err != nil {
			return nil, fmt.Errorf("building OTLP metric exporter for %q: %w", endpoint, err)
		}
	} else {
		// otlpmetricgrpc reads the standard OTEL_EXPORTER_OTLP_* environment
		// variables itself (an http:// scheme implies an insecure connection).
		var err error
		otlpExporter, err = otlpmetricgrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("building OTLP metric exporter for %q: %w", endpoint, err)
		}
	}
	log.Info("exporting storage client metrics via OTLP", "endpoint", endpoint)
	return []option.ClientOption{experimental.WithMetricExporter(&otlpExporter)}, nil
}

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

package logfactory

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
	"justinsb.com/cloudetcd/pkg/persistence/gcslog"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
	"justinsb.com/cloudetcd/pkg/persistence/tieredlog"
)

func NewLog(ctx context.Context, uri string) (persistence.Log, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log URI %q: %w", uri, err)
	}
	switch u.Scheme {
	case "filesystem":
		dir := "/" + u.Host + "/" + u.Path
		return filesystemlog.NewFilesystemLog(dir)
	case "gs":
		return gcslog.NewGCSLog(ctx, u.Host, u.Path)
	case "memory":
		return memorylog.New(), nil
	case "tiered":
		return newTieredLog(ctx, u)
	default:
		return nil, fmt.Errorf("unsupported log scheme %q", u.Scheme)
	}
}

// newTieredLog builds a tiered log from a URI of the form:
//
//	tiered:?fast=filesystem:///var/lib/cloudetcd/log&archive=gs://bucket/logs/&flushInterval=5m
//
// The fast and archive parameters are themselves log URIs (URL-encoded).
func newTieredLog(ctx context.Context, u *url.URL) (persistence.Log, error) {
	query := u.Query()

	fastURI := query.Get("fast")
	archiveURI := query.Get("archive")
	if fastURI == "" || archiveURI == "" {
		return nil, fmt.Errorf("tiered log URI must have fast and archive parameters, e.g. tiered:?fast=filesystem:///var/log&archive=gs://bucket/prefix/")
	}

	options := tieredlog.Options{}
	if flushInterval := query.Get("flushInterval"); flushInterval != "" {
		d, err := time.ParseDuration(flushInterval)
		if err != nil {
			return nil, fmt.Errorf("parsing flushInterval %q: %w", flushInterval, err)
		}
		options.FlushInterval = d
	}

	fast, err := NewLog(ctx, fastURI)
	if err != nil {
		return nil, fmt.Errorf("creating fast tier log: %w", err)
	}
	archive, err := NewLog(ctx, archiveURI)
	if err != nil {
		fast.Close()
		return nil, fmt.Errorf("creating archive tier log: %w", err)
	}

	log, err := tieredlog.NewTieredLog(ctx, fast, archive, options)
	if err != nil {
		fast.Close()
		archive.Close()
		return nil, err
	}
	return log, nil
}

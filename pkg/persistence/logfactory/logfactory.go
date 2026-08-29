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
	"strconv"
	"strings"
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
		// filesystem:///var/lib/cloudetcd/log?cache=256MB bounds the in-memory
		// record cache; values are read from disk on demand.
		dir := "/" + u.Host + "/" + u.Path
		cacheBytes, err := parseSize(u.Query().Get("cache"))
		if err != nil {
			return nil, err
		}
		return filesystemlog.NewFilesystemLogWithOptions(dir, filesystemlog.Options{CacheBytes: cacheBytes})
	case "gs":
		// gs://bucket/some/prefix/ => bucket "bucket", object prefix "some/prefix/"
		// (GCS object names do not start with a slash)
		cacheBytes, err := parseSize(u.Query().Get("cache"))
		if err != nil {
			return nil, err
		}
		return gcslog.NewGCSLogWithOptions(ctx, u.Host, strings.TrimPrefix(u.Path, "/"), gcslog.Options{CacheBytes: cacheBytes})
	case "memory":
		// memory://?commitLatency=50ms emulates an object-store backend's
		// commit round-trip on top of the in-memory log.
		var opts []memorylog.Option
		if v := u.Query().Get("commitLatency"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("parsing commitLatency %q: %w", v, err)
			}
			opts = append(opts, memorylog.WithCommitLatency(d))
		}
		return memorylog.New(opts...), nil
	case "tiered":
		return newTieredLog(ctx, u)
	default:
		return nil, fmt.Errorf("unsupported log scheme %q", u.Scheme)
	}
}

// newTieredLog builds a tiered log from a URI of the form:
//
//	tiered:?fast=filesystem:///var/lib/cloudetcd/log&archive=gs://bucket/logs/&flushInterval=5m&retain=true
//
// The fast and archive parameters are themselves log URIs (URL-encoded).
// retain=true keeps archived records in the fast tier so reads stay local.
func newTieredLog(ctx context.Context, u *url.URL) (persistence.Log, error) {
	query := u.Query()

	fastURI := query.Get("fast")
	archiveURI := query.Get("archive")
	if fastURI == "" || archiveURI == "" {
		return nil, fmt.Errorf("tiered log URI must have fast and archive parameters, e.g. tiered:?fast=filesystem:///var/log&archive=gs://bucket/prefix/")
	}

	options := tieredlog.Options{}
	if retain := query.Get("retain"); retain != "" {
		b, err := strconv.ParseBool(retain)
		if err != nil {
			return nil, fmt.Errorf("parsing retain %q: %w", retain, err)
		}
		options.RetainFastTier = b
	}
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

// parseSize parses a byte size such as "256MB", "1GiB", "64k" or "1048576".
// An empty string is 0.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	units := []struct {
		suffix string
		mult   int64
	}{
		{"GiB", 1 << 30}, {"GB", 1 << 30}, {"G", 1 << 30},
		{"MiB", 1 << 20}, {"MB", 1 << 20}, {"M", 1 << 20},
		{"KiB", 1 << 10}, {"KB", 1 << 10}, {"K", 1 << 10},
		{"B", 1},
	}
	mult := int64(1)
	num := s
	for _, u := range units {
		if strings.HasSuffix(strings.ToUpper(s), strings.ToUpper(u.suffix)) {
			mult = u.mult
			num = s[:len(s)-len(u.suffix)]
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing size %q: %w", s, err)
	}
	return n * mult, nil
}

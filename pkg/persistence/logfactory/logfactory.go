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

// Package logfactory constructs a log from a URI:
//
//	filesystem:///var/lib/cloudetcd/log
//	filesystem:///var/lib/cloudetcd/log?archive=gs://bucket/prefix&rotateBytes=64MB&rotateAfter=5m&cache=256MB
//	memory://
//
// The log is always local files; archive= attaches a GCS bucket that every
// closed log file is copied to (and restored from on a machine with no
// files). memory:// is a temporary directory removed on Close, for tests.
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
	"justinsb.com/cloudetcd/pkg/persistence/gcsarchive"
	"justinsb.com/cloudetcd/pkg/persistence/memorylog"
)

func NewLog(ctx context.Context, uri string) (persistence.Log, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log URI %q: %w", uri, err)
	}
	switch u.Scheme {
	case "filesystem":
		dir := "/" + u.Host + "/" + u.Path
		opts, err := parseOptions(ctx, u.Query())
		if err != nil {
			return nil, err
		}
		return filesystemlog.NewFilesystemLogWithOptions(dir, opts)
	case "memory":
		return memorylog.New(), nil
	case "gs", "tiered":
		return nil, fmt.Errorf("log URI %q: the log is always local files; archive to GCS with filesystem:///path?archive=gs://bucket/prefix", uri)
	default:
		return nil, fmt.Errorf("unsupported log scheme %q", u.Scheme)
	}
}

func parseOptions(ctx context.Context, q url.Values) (filesystemlog.Options, error) {
	var opts filesystemlog.Options
	var err error
	if opts.CacheBytes, err = parseSize(q.Get("cache")); err != nil {
		return opts, err
	}
	if opts.RotateBytes, err = parseSize(q.Get("rotateBytes")); err != nil {
		return opts, err
	}
	if v := q.Get("rotateAfter"); v != "" {
		if opts.RotateAfter, err = time.ParseDuration(v); err != nil {
			return opts, fmt.Errorf("parsing rotateAfter %q: %w", v, err)
		}
	}
	if v := q.Get("archive"); v != "" {
		au, err := url.Parse(v)
		if err != nil || au.Scheme != "gs" {
			return opts, fmt.Errorf("archive %q: want gs://bucket/prefix", v)
		}
		// gs://bucket/some/prefix/ => bucket "bucket", object prefix "some/prefix/"
		// (GCS object names do not start with a slash)
		archive, err := gcsarchive.New(ctx, au.Host, strings.TrimPrefix(au.Path, "/"))
		if err != nil {
			return opts, err
		}
		opts.Archive = archive
	}
	return opts, nil
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

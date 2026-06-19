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

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
	"justinsb.com/cloudetcd/pkg/persistence/gcslog"
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
		return filesystemlog.NewFilesystemLog(dir)
	case "gs":
		return gcslog.NewGCSLog(ctx, u.Host, u.Path)
	case "memory":
		return memorylog.New(), nil
	default:
		return nil, fmt.Errorf("unsupported log scheme %q", u.Scheme)
	}
}

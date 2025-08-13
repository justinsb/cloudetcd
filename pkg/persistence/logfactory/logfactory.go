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

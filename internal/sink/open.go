package sink

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// Open selects a sink from an output spec.
// A filesystem path or file:// path writes JSONL. sqlite:// and postgres://
// (or postgresql://) write to a database table named gofetl_records.
func Open(ctx context.Context, spec string) (Sink, error) {
	switch {
	case spec == "":
		return nil, fmt.Errorf("output is empty")
	case spec == "-":
		return OpenJSONL("-")
	case strings.HasPrefix(spec, "postgres://"), strings.HasPrefix(spec, "postgresql://"):
		return OpenPostgres(ctx, spec)
	case strings.HasPrefix(spec, "sqlite:"):
		path, err := sqlitePath(spec)
		if err != nil {
			return nil, err
		}
		return OpenSQLite(path)
	case strings.HasPrefix(spec, "file://"):
		return OpenJSONL(strings.TrimPrefix(spec, "file://"))
	default:
		return OpenJSONL(spec)
	}
}

func sqlitePath(spec string) (string, error) {
	u, err := url.Parse(spec)
	if err != nil {
		return "", fmt.Errorf("sqlite url: %w", err)
	}
	switch {
	case u.Opaque != "":
		return u.Opaque, nil
	case u.Host != "" && (u.Path == "" || u.Path == "/"):
		return u.Host, nil
	case u.Host != "" && u.Path != "":
		return u.Host + u.Path, nil
	case u.Path != "":
		return strings.TrimPrefix(u.Path, "//"), nil
	default:
		return "", fmt.Errorf("sqlite url %q is missing a path", spec)
	}
}

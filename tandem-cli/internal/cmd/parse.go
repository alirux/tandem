package cmd

import (
	"strconv"
	"time"

	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
)

// parseInt parses a positional integer argument, reporting a bad one as a usage error
// naming the argument. Generic over the width the caller's operation needs (the contract
// types outbox ids as int64 and bucket numbers as int), so both share one message.
func parseInt[T ~int | ~int64](name, raw string) (T, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, exitcode.New(exitcode.UsageError, "invalid %s %q: must be an integer", name, raw)
	}
	return T(n), nil
}

func parseTime(flag, raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, exitcode.New(exitcode.UsageError,
			"invalid %s %q: want RFC3339, e.g. 2026-08-05T00:00:00Z", flag, raw)
	}
	return t, nil
}

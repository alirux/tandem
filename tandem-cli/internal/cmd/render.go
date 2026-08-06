package cmd

import (
	"encoding/json"

	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
	"github.com/alirux/tandem/tandem-cli/internal/output"
)

// decode unmarshals an Admin API response body into T, classifying a parse failure as
// UnexpectedError: the contract is committed and the client generated from it, so a 200
// this build cannot read is a version mismatch or a broken server, never user error.
func decode[T any](body []byte) (T, error) {
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		return v, exitcode.Wrap(exitcode.UnexpectedError, err, "parsing response")
	}
	return v, nil
}

// renderObject writes a single-object response in whichever mode app selected: json
// reprints body verbatim (LLD-cli.md §6 - no CLI-invented envelope, and an unknown field
// still comes through unmangled), human decodes it into T and renders the key:value
// pairs. The pairs function takes the App so it can consult IO.Color; the ones with
// nothing to colorize simply ignore it.
func renderObject[T any](app *App, body []byte, pairs func(*App, T) [][2]string) error {
	if app.Output == output.JSON {
		return writeErr(output.Raw(app.IO.Out, body))
	}
	v, err := decode[T](body)
	if err != nil {
		return err
	}
	return writeErr(output.KeyValue(app.IO.Out, pairs(app, v)))
}

// renderList is renderObject for a response that is a JSON array: human mode decodes it
// into []T and renders one table row per element.
func renderList[T any](app *App, body []byte, header []string, row func(*App, T) []string) error {
	if app.Output == output.JSON {
		return writeErr(output.Raw(app.IO.Out, body))
	}
	items, err := decode[[]T](body)
	if err != nil {
		return err
	}
	return writeErr(output.Table(app.IO.Out, header, rows(app, items, row)))
}

func rows[T any](app *App, items []T, row func(*App, T) []string) [][]string {
	out := make([][]string, 0, len(items))
	for _, item := range items {
		out = append(out, row(app, item))
	}
	return out
}

// optional renders a pointer field as the empty string when absent - the table cell
// equivalent of renderObject's pairs functions simply omitting an absent key:value line.
func optional[T any](p *T, format func(T) string) string {
	if p == nil {
		return ""
	}
	return format(*p)
}

// Package rewriter turns a stock summary into a publication-ready report.
package rewriter

import "context"

// Rewriter rewrites one non-empty summary. Implementations must return a
// non-empty report or an error; they must not mutate the input.
type Rewriter interface {
	Rewrite(ctx context.Context, summary string) (string, error)
}

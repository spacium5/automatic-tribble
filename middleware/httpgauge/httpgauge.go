//go:build !solution

package httpgauge

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
)

type Gauge struct {
	mu     sync.Mutex
	counts map[string]int
}

func New() *Gauge {
	return &Gauge{
		counts: make(map[string]int),
	}
}

func (g *Gauge) Snapshot() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make(map[string]int, len(g.counts))
	for k, v := range g.counts {
		out[k] = v
	}
	return out
}

func (g *Gauge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snapshot := g.Snapshot()

	patterns := make([]string, 0, len(snapshot))
	for pattern := range snapshot {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)

	var b strings.Builder
	for _, pattern := range patterns {
		fmt.Fprintf(&b, "%s %d\n", pattern, snapshot[pattern])
	}

	_, _ = w.Write([]byte(b.String()))
}

func (g *Gauge) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pattern := ""

		defer func() {
			if pattern == "" {
				if rctx := chi.RouteContext(r.Context()); rctx != nil {
					pattern = rctx.RoutePattern()
				}
			}
			if pattern != "" {
				g.mu.Lock()
				g.counts[pattern]++
				g.mu.Unlock()
			}
			if rec := recover(); rec != nil {
				panic(rec)
			}
		}()

		next.ServeHTTP(w, r)

		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			pattern = rctx.RoutePattern()
		}
	})
}

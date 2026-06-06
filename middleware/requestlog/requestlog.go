//go:build !solution

package requestlog

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/felixge/httpsnoop"
	"go.uber.org/zap"
)

var requestSeq atomic.Uint64

func Log(l *zap.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := fmt.Sprintf("%d", requestSeq.Add(1))
			path := r.URL.Path
			method := r.Method

			l.Info("request started",
				zap.String("path", path),
				zap.String("method", method),
				zap.String("request_id", requestID),
			)

			start := time.Now()

			defer func() {
				if rec := recover(); rec != nil {
					l.Info("request panicked",
						zap.String("path", path),
						zap.String("method", method),
						zap.String("request_id", requestID),
					)
					panic(rec)
				}
			}()

			metrics := httpsnoop.CaptureMetrics(next, w, r)

			l.Info("request finished",
				zap.String("path", path),
				zap.String("method", method),
				zap.String("request_id", requestID),
				zap.Int("status_code", metrics.Code),
				zap.Duration("duration", time.Since(start)),
			)
		})
	}
}

package middleware

import (
	"encoding/json"
	"net/http"
	"time"
)

func Semaphore(maxConcurrent int, waitTimeout time.Duration) func(next http.Handler) http.Handler {
	sem := make(chan struct{}, maxConcurrent)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				next.ServeHTTP(w, r)

			case <-time.After(waitTimeout):
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "server is at maximum capacity; load shedding active",
				})

			case <-r.Context().Done():
				return
			}
		})
	}
}

package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSemaphore_PassThrough(t *testing.T) {
	handler := Semaphore(2, 50*time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("expected body 'ok', got '%s'", rec.Body.String())
	}
}

func TestSemaphore_QueueWaitSuccess(t *testing.T) {
	mw := Semaphore(1, 100*time.Millisecond)

	unblockFirst := make(chan struct{})
	var secondFinished atomic.Bool

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Request-ID") == "1" {
			<-unblockFirst
			w.WriteHeader(http.StatusOK)
			return
		}

		secondFinished.Store(true)
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "1")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "2")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected request 2 to succeed after queuing, got %d", rec.Code)
		}
	}()

	time.Sleep(20 * time.Millisecond)
	close(unblockFirst)
	wg.Wait()

	if !secondFinished.Load() {
		t.Fatalf("expected second request to have acquired the released slot and finished")
	}
}

func TestSemaphore_TimeoutRejection(t *testing.T) {
	mw := Semaphore(1, 30*time.Millisecond)

	blockChan := make(chan struct{})
	defer close(blockChan)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Request-ID") == "worker" {
			<-blockChan
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	go func() {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "worker")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()

	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Request-ID", "blocked-caller")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
	}

	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected Retry-After header = '1', got '%s'", rec.Header().Get("Retry-After"))
	}
}

func TestSemaphore_ContextCancellation(t *testing.T) {
	mw := Semaphore(1, 200*time.Millisecond)

	blockChan := make(chan struct{})
	defer close(blockChan)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blockChan
	}))

	go func() {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}()

	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/test", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if elapsed >= 150*time.Millisecond {
		t.Fatalf("expected early return on context cancellation, took %v", elapsed)
	}
}
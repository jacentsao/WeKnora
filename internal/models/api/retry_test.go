package api

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fastPolicy() RetryPolicy { return RetryPolicy{MaxRetries: 3} }

// The failure this helper exists to make impossible: every attempt fails at
// the transport, and the caller gets an error rather than a nil response with
// a nil error to check. The eight hand-written loops it replaces returned
// (nil, nil) here whenever a `:=` shadowed the enclosing error.
func TestPostJSONWithRetryAlwaysReportsAnUnreachableUpstream(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	var out map[string]any
	err := Endpoint{}.PostJSONWithRetry(
		context.Background(), url, map[string]any{"a": 1}, &out, fastPolicy(), "test",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "test")
}

func TestPostJSONWithRetrySucceedsAfterATransportFailure(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			// Hang up mid-request so the client sees a transport error.
			hijacker, ok := w.(http.Hijacker)
			require.True(t, ok)
			conn, _, err := hijacker.Hijack()
			require.NoError(t, err)
			_ = conn.Close()
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	var out map[string]any
	err := Endpoint{}.PostJSONWithRetry(
		context.Background(), server.URL, map[string]any{}, &out, fastPolicy(), "test",
	)
	require.NoError(t, err)
	assert.Equal(t, true, out["ok"])
	assert.Equal(t, 2, attempts)
}

// A non-2xx reply is the vendor answering. Repeating a rejected request costs
// quota, delays the error, and cannot change the answer.
func TestPostJSONWithRetryDoesNotRetryAVendorRejection(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"input too long"}`))
	}))
	defer server.Close()

	err := Endpoint{}.PostJSONWithRetry(
		context.Background(), server.URL, map[string]any{}, nil, fastPolicy(), "test",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input too long")
	assert.Equal(t, 1, attempts, "a rejection must not be repeated")
}

// A reply that arrived but does not decode is not a transport failure: the
// vendor did the work, and sending the request again bills it again.
func TestPostJSONWithRetryDoesNotRetryAnUndecodableReply(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		_, _ = w.Write([]byte(`not json`))
	}))
	defer server.Close()

	var out map[string]any
	err := Endpoint{}.PostJSONWithRetry(
		context.Background(), server.URL, map[string]any{}, &out, fastPolicy(), "test",
	)
	require.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestPostJSONWithRetryStopsWhenTheContextIsDone(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Endpoint{}.PostJSONWithRetry(
		ctx, url, map[string]any{}, nil,
		RetryPolicy{MaxRetries: 3, BaseDelay: time.Minute}, "test",
	)
	require.Error(t, err)
}

func TestRetryPolicyBackoffDoublesAndIsCapped(t *testing.T) {
	p := RetryPolicy{MaxRetries: 5, BaseDelay: time.Second, MaxDelay: 4 * time.Second}
	assert.Equal(t, time.Second, p.delay(1))
	assert.Equal(t, 2*time.Second, p.delay(2))
	assert.Equal(t, 4*time.Second, p.delay(3))
	assert.Equal(t, 4*time.Second, p.delay(4), "capped")
	assert.Zero(t, RetryPolicy{MaxRetries: 2}.delay(1), "no base delay means no waiting")
}

// A reply cut off mid-body never arrived, so it is sent again.
func TestPostJSONWithRetryRetriesAReplyCutOffWhileReading(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts == 1 {
			// Promise more than is sent, then hang up.
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write([]byte(`{"a":`))
			w.(http.Flusher).Flush() // the status and half a body are on the wire
			hj, ok := w.(http.Hijacker)
			require.True(t, ok)
			conn, _, err := hj.Hijack()
			require.NoError(t, err)
			_ = conn.Close()
			return
		}
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer server.Close()

	var out map[string]any
	err := Endpoint{}.PostJSONWithRetry(
		context.Background(), server.URL, map[string]any{}, &out, fastPolicy(), "test",
	)
	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	assert.Equal(t, float64(1), out["a"])
}

// The phase that failed is named, so the log says whether the request or the
// reply was lost.
func TestReadFailureIsReportedAsSuch(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte(`{"a":`))
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		require.NoError(t, err)
		_ = conn.Close()
	}))
	defer server.Close()

	err := Endpoint{}.PostJSON(context.Background(), server.URL, map[string]any{}, &map[string]any{})
	var transportErr *TransportError
	require.ErrorAs(t, err, &transportErr)
	assert.Equal(t, "read response", transportErr.Op)
}

func TestHTTPRetryPolicyHonoursRetryAfterBeforeCancellation(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	for _, hint := range []string{"60", time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)} {
		t.Run(hint, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.Header().Set("Retry-After", hint)
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			err := (Endpoint{}).PostJSONWithRetry(ctx, server.URL, map[string]any{}, nil, RetryPolicy{
				MaxRetries: 2, RetryHTTP: true, Jitter: true, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond,
			}, "embedding")
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 1, calls, "the backoff cap and jitter must not shorten the provider minimum")
		})
	}
}

func TestHTTPRetryPolicyClassifiesFailures(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 429, 500, 501, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls, admissions, failures := 0, 0, 0
			policy := RetryPolicy{
				MaxRetries: 2, RetryHTTP: true,
				BeforeAttempt: func(context.Context) error { admissions++; return nil },
				OnFailure:     func(context.Context, error, time.Duration) error { failures++; return nil },
			}
			err := withRetry(context.Background(), policy, "embedding", func() error {
				calls++
				return &HTTPError{StatusCode: status}
			})
			var upstream *HTTPError
			require.ErrorAs(t, err, &upstream)
			if status == 429 || status == 500 || status == 502 || status == 503 || status == 504 {
				require.Equal(t, 3, calls)
				require.Equal(t, calls, failures, "even the final 429 must cool down other callers")
			} else {
				require.Equal(t, 1, calls)
				require.Zero(t, failures)
			}
			require.Equal(t, calls, admissions, "each retry must reacquire quota")
		})
	}
}

func TestRetryAdmissionFailureDoesNotSend(t *testing.T) {
	blocked := errors.New("quota backend unavailable")
	err := withRetry(context.Background(), RetryPolicy{
		MaxRetries: 3, RetryHTTP: true,
		BeforeAttempt: func(context.Context) error { return blocked },
	}, "embedding", func() error { t.Fatal("request sent without quota"); return nil })
	require.ErrorIs(t, err, blocked)
}

func TestRetryDelayCannotOverflow(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Hour, MaxDelay: 2 * time.Hour}
	require.Equal(t, 2*time.Hour, p.delay(1000))
	p.MaxDelay = 0
	require.Equal(t, time.Duration(math.MaxInt64), p.delay(1000))
	require.Equal(t, time.Duration(math.MaxInt64), parseRetryAfter("9223372036854775807", time.Now()))
}

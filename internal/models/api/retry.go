package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
)

// RetryPolicy describes how often to retry a failed request and how long to
// wait between attempts. The zero value performs one attempt and no waiting.
type RetryPolicy struct {
	// MaxRetries is the number of retries after the first attempt.
	MaxRetries int
	// BaseDelay is the wait before the first retry; it doubles each time.
	BaseDelay time.Duration
	// MaxDelay caps the backoff.
	MaxDelay time.Duration
	// RetryHTTP opts into retrying 429 and transient server errors. Other
	// clients retain their transport-only policy unless they explicitly opt in.
	RetryHTTP bool
	// Jitter spreads concurrent retries over the upper half of the backoff.
	Jitter bool
	// BeforeAttempt admits each actual request, including retries and protocol
	// sub-batches, against a shared rate budget. Waiting is outside the HTTP
	// client's per-request timeout but remains bounded by ctx.
	BeforeAttempt func(context.Context) error
	// OnFailure observes every retryable failure, including the final attempt.
	// Embedding uses this to share a provider cooldown with other callers.
	OnFailure func(context.Context, error, time.Duration) error
}

// DefaultRetryPolicy is three retries with 1s, 2s, 4s backoff, which is what
// every embedding client used before this helper existed.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxRetries: 3, BaseDelay: time.Second, MaxDelay: 10 * time.Second}
}

func (p RetryPolicy) delay(attempt int) time.Duration {
	if p.BaseDelay <= 0 {
		return 0
	}
	ceiling := time.Duration(math.MaxInt64)
	if p.MaxDelay > 0 {
		ceiling = p.MaxDelay
	}
	wait := min(p.BaseDelay, ceiling)
	for i := 1; i < attempt; i++ {
		if wait > ceiling/2 {
			return ceiling
		}
		wait *= 2
	}
	return wait
}

// PostJSONWithRetry sends one authenticated JSON request, retrying transport
// failures, and decodes the reply into out.
//
// It never returns a nil error without decoding: the eight hand-written
// retry loops this replaces kept `resp` and `err` in the enclosing scope and
// returned `(nil, err)` at the end, so writing `req, err := ...` inside the
// loop shadowed the error, left the outer one nil, and handed the caller a
// nil response with no error to check — a nil-pointer panic that took the
// process down instead of reporting "connection refused" (Tencent/WeKnora#3484).
// Here the last error is captured in the enclosing scope on purpose and the
// function cannot fall through without it.
//
// Transport failures, including a reply cut off while reading, are retried.
// RetryHTTP additionally permits rate limits and transient server errors;
// permanent rejections and complete but undecodable replies are never retried.
func (e Endpoint) PostJSONWithRetry(
	ctx context.Context, url string, body, out any, policy RetryPolicy, label string,
) error {
	return withRetry(ctx, policy, label, func() error {
		return e.PostJSON(ctx, url, body, out)
	})
}

// withRetry owns the request retry budget. Synchronous callers must not wrap
// it in another retry loop that replays successful batches of a document.
func withRetry(ctx context.Context, policy RetryPolicy, label string, send func() error) error {
	var lastErr error
	var wait time.Duration
	for attempt := 0; attempt <= max(0, policy.MaxRetries); attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			logger.Infof(ctx, "[%s] retrying (%d/%d) after %v: %v",
				label, attempt, policy.MaxRetries, wait, lastErr)
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if policy.BeforeAttempt != nil {
			if err := policy.BeforeAttempt(ctx); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err := send()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !policy.retryable(err) {
			return err
		}
		lastErr = err
		wait = policy.delay(attempt + 1)
		if policy.Jitter && wait > 0 {
			wait = wait/2 + time.Duration(rand.Int64N(int64(wait-wait/2)+1))
		}
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			// A server minimum is applied AFTER the cap and jitter; neither
			// may shorten Retry-After, including an HTTP-date value.
			wait = max(wait, httpErr.RetryAfter())
		}
		if policy.OnFailure != nil {
			if observeErr := policy.OnFailure(ctx, err, wait); observeErr != nil {
				return errors.Join(err, observeErr)
			}
		}
	}
	return fmt.Errorf("%s: %w", label, lastErr)
}

func (p RetryPolicy) retryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		return true
	}
	var httpErr *HTTPError
	return p.RetryHTTP && errors.As(err, &httpErr) && httpErr.Retryable()
}

package embedding

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/models/limiter"
)

// retryPolicy is replaceable in protocol tests so failure cases stay fast.
var retryPolicy = embeddingRetryPolicy

func embeddingRetryPolicy() api.RetryPolicy {
	return api.RetryPolicy{
		MaxRetries: boundedEnvInt("EMBED_RETRY_ATTEMPTS", 6, 1, 32) - 1,
		BaseDelay:  time.Duration(boundedEnvInt("EMBED_RETRY_BASE_DELAY_MS", 5000, 1, 3600000)) * time.Millisecond,
		MaxDelay:   time.Duration(boundedEnvInt("EMBED_RETRY_MAX_DELAY_MS", 60000, 1, 3600000)) * time.Millisecond,
		RetryHTTP:  true,
		Jitter:     true,
	}
}

func boundedEnvInt(key string, fallback, minimum, maximum int) int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || n < minimum || n > maximum {
		return fallback
	}
	return n
}

func configureEmbeddingRetry(endpoint api.Endpoint, extra map[string]string) (api.RetryPolicy, error) {
	interval, err := embeddingRequestInterval(extra)
	if err != nil {
		return api.RetryPolicy{}, err
	}
	scope, err := embeddingRateScope(endpoint, extra["embedding_rate_limit_scope"])
	if err != nil {
		return api.RetryPolicy{}, err
	}
	p := retryPolicy()
	p.BeforeAttempt = func(ctx context.Context) error {
		return limiter.WaitRequest(ctx, scope, interval)
	}
	p.OnFailure = func(ctx context.Context, err error, delay time.Duration) error {
		var upstream *api.HTTPError
		if !errors.As(err, &upstream) ||
			(upstream.StatusCode != http.StatusTooManyRequests && upstream.RetryAfter() <= 0) {
			return nil
		}
		// A quota is often unknown until the first 429. In that case probe
		// recovery at one request/second, rather than waking all documents at
		// once. An explicitly configured interval governs recovery as well.
		recovery := interval
		if recovery == 0 {
			recovery = time.Second
		}
		return limiter.CooldownRequests(ctx, scope, delay, recovery)
	}
	return p, nil
}

func embeddingRequestInterval(extra map[string]string) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("EMBED_REQUEST_INTERVAL_MS"))
	if override := strings.TrimSpace(extra["embedding_request_interval_ms"]); override != "" {
		raw = override
	}
	if raw == "" {
		return 0, nil
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms < 0 || ms > 3600000 {
		return 0, fmt.Errorf("embedding request interval must be between 0 and 3600000 milliseconds")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// API keys and model row IDs intentionally do not partition a quota: multiple
// keys/rows can share one provider account. The default conservatively groups
// an endpoint host and wire model name. Operators can map aliases or separate
// independent accounts using an explicit quota scope in model extra_config.
// Only a digest reaches Redis; credentials and endpoint URLs are never stored.
func embeddingRateScope(endpoint api.Endpoint, override string) (string, error) {
	identity := "configured\x00" + strings.TrimSpace(override)
	if strings.TrimSpace(override) == "" {
		u, err := url.Parse(endpoint.Resolve(""))
		if err != nil || u.Host == "" {
			return "", fmt.Errorf("embedding rate limiter requires an absolute endpoint URL")
		}
		identity = "endpoint\x00" + strings.ToLower(u.Host) + "\x00" + endpoint.Model
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity))), nil
}

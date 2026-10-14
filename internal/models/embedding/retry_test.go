package embedding

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func fastEmbeddingRecovery(t *testing.T) {
	t.Helper()
	allowLoopback(t)
	t.Setenv("EMBED_RETRY_ATTEMPTS", "3")
	t.Setenv("EMBED_RETRY_BASE_DELAY_MS", "1")
	t.Setenv("EMBED_RETRY_MAX_DELAY_MS", "1")
	t.Setenv("EMBED_REQUEST_INTERVAL_MS", "1")
}

func TestEmbeddingProtocolsRetryOnlyTheFailedHTTPBatch(t *testing.T) {
	for _, tc := range []struct{ provider, model string }{
		{"generic", "test"},
		{"aliyun", "tongyi-embedding-vision-plus"},
		{"volcengine", "doubao-embedding-vision-251215"},
		{"gemini", "gemini-embedding-001"},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			fastEmbeddingRecovery(t)
			calls := map[string]int{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				raw, _ := json.Marshal(body)
				key := "a"
				if strings.Contains(string(raw), `"bb"`) {
					key = "bb"
				}
				calls[key]++
				if key == "bb" && calls[key] == 1 {
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				_, _ = fmt.Fprint(w, answer(r.URL.Path, body))
			}))
			defer server.Close()
			model, err := NewEmbedder(Config{
				Source: types.ModelSourceRemote, Provider: tc.provider, ModelName: tc.model,
				BaseURL: server.URL, APIKey: "test",
				Spec: &types.ModelSpecOverride{Compat: map[string]any{"max_batch_size": 1}},
			}, nil, nil)
			require.NoError(t, err)
			got, err := model.BatchEmbed(context.Background(), []string{"a", "bb"})
			require.NoError(t, err)
			require.Equal(t, [][]float32{{1}, {2}}, got)
			require.Equal(t, map[string]int{"a": 1, "bb": 2}, calls)
		})
	}
}

func TestEmbeddingClientsShareRequestPacingAcrossModelRows(t *testing.T) {
	fastEmbeddingRecovery(t)
	t.Setenv("EMBED_REQUEST_INTERVAL_MS", "25")
	var mu sync.Mutex
	var arrivals []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		_, _ = fmt.Fprint(w, answer(r.URL.Path, body))
	}))
	defer server.Close()
	var models []Embedder
	for _, id := range []string{"one", "two"} {
		model, err := NewEmbedder(Config{
			Source: types.ModelSourceRemote, Provider: "generic", ModelName: id, ModelID: id,
			BaseURL: server.URL, APIKey: id,
			ExtraConfig: map[string]string{"remote_model_name": "shared-wire-model"},
			Spec:        &types.ModelSpecOverride{Compat: map[string]any{"max_batch_size": 1}},
		}, nil, nil)
		require.NoError(t, err)
		models = append(models, model)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(models))
	for _, model := range models {
		wg.Go(func() {
			_, err := model.BatchEmbed(ctx, []string{"a", "bb", "ccc"})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, arrivals, 6)
	require.GreaterOrEqual(t, arrivals[5].Sub(arrivals[0]), 115*time.Millisecond,
		"model rows, API keys and protocol splitting must share one request budget")
}

func TestFinal429CoolsDownOtherEmbeddingClients(t *testing.T) {
	fastEmbeddingRecovery(t)
	t.Setenv("EMBED_RETRY_ATTEMPTS", "1")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	makeClient := func(id string) Embedder {
		model, err := NewEmbedder(Config{
			Source: types.ModelSourceRemote, Provider: "generic", ModelName: "m", ModelID: id,
			BaseURL: server.URL, APIKey: id,
		}, nil, nil)
		require.NoError(t, err)
		return model
	}
	_, err := makeClient("one").BatchEmbed(context.Background(), []string{"a"})
	var upstream *api.HTTPError
	require.ErrorAs(t, err, &upstream)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	// Queries use the same provider quota, even though they bypass the
	// background concurrency semaphore.
	_, err = makeClient("two").Embed(types.WithEmbedQuery(ctx), "query")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, calls)
}

func TestUnconfiguredEmbeddingRateRecoversWithoutAnotherBurst(t *testing.T) {
	fastEmbeddingRecovery(t)
	t.Setenv("EMBED_RETRY_ATTEMPTS", "1")
	t.Setenv("EMBED_REQUEST_INTERVAL_MS", "0")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1]}]}`)
	}))
	defer server.Close()
	model, err := NewEmbedder(Config{
		Source: types.ModelSourceRemote, Provider: "generic", ModelName: "m", BaseURL: server.URL,
	}, nil, nil)
	require.NoError(t, err)
	_, err = model.Embed(context.Background(), "first")
	require.Error(t, err)
	_, err = model.Embed(context.Background(), "recovery probe")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = model.Embed(ctx, "must wait for the next quota slot")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 2, calls, "default recovery must space callers even without a configured quota")
}

func TestEmbeddingQuotaScopeAndConfiguration(t *testing.T) {
	endpoint := api.Endpoint{BaseURL: "https://example.com/v1?api_key=secret", Model: "m", ModelID: "one"}
	first, err := embeddingRateScope(endpoint, "")
	require.NoError(t, err)
	endpoint.ModelID = "two"
	endpoint.BaseURL = "https://EXAMPLE.com/other"
	second, err := embeddingRateScope(endpoint, "")
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NotContains(t, first, "secret")
	endpoint.Model = "different"
	third, err := embeddingRateScope(endpoint, "")
	require.NoError(t, err)
	require.NotEqual(t, first, third)
	override, err := embeddingRateScope(endpoint, "account/model")
	require.NoError(t, err)
	endpoint.BaseURL = "https://another.example.com"
	same, err := embeddingRateScope(endpoint, "account/model")
	require.NoError(t, err)
	require.Equal(t, override, same)

	t.Setenv("EMBED_REQUEST_INTERVAL_MS", "1000")
	interval, err := embeddingRequestInterval(nil)
	require.NoError(t, err)
	require.Equal(t, time.Second, interval)
	interval, err = embeddingRequestInterval(map[string]string{"embedding_request_interval_ms": "20"})
	require.NoError(t, err)
	require.Equal(t, 20*time.Millisecond, interval)
	for _, invalid := range []string{"-1", "oops", "9223372036854775807"} {
		_, err := embeddingRequestInterval(map[string]string{"embedding_request_interval_ms": invalid})
		require.Error(t, err)
	}
}

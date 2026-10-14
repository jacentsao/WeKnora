package retriever

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type recoveryRepository struct {
	interfaces.RetrieveEngineRepository
	mu    sync.Mutex
	saved map[string][]float32
}

func (r *recoveryRepository) BatchSave(_ context.Context, _ []*types.IndexInfo, params map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, vector := range params["embedding"].(map[string][]float32) {
		r.saved[id] = vector
	}
	return nil
}

// Exercise the real factory, protocol, worker pool and BatchIndex together.
// A always succeeds; only B fails, so replaying A is directly observable.
func TestBatchIndexRetriesOnlyTheFailedProviderRequest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		failures  int
		wantB     int
		wantError bool
	}{
		{"transient rate limit", http.StatusTooManyRequests, 1, 2, false},
		{"transient unavailable", http.StatusServiceUnavailable, 1, 2, false},
		{"exhausted rate limit", http.StatusTooManyRequests, 100, 3, true},
		{"permanent rejection", http.StatusUnauthorized, 100, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SSRF_WHITELIST", "127.0.0.1")
			secutils.ResetSSRFWhitelistForTest()
			t.Cleanup(secutils.ResetSSRFWhitelistForTest)
			t.Setenv("BATCH_EMBED_SIZE", "1")
			t.Setenv("EMBED_RETRY_ATTEMPTS", "3")
			t.Setenv("EMBED_RETRY_BASE_DELAY_MS", "1")
			t.Setenv("EMBED_RETRY_MAX_DELAY_MS", "1")
			t.Setenv("EMBED_REQUEST_INTERVAL_MS", "1")

			calls := map[string]int{}
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Input []string `json:"input"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Input) != 1 {
					http.Error(w, "invalid test request", http.StatusBadRequest)
					return
				}
				text := body.Input[0]
				mu.Lock()
				calls[text]++
				call := calls[text]
				mu.Unlock()
				if text == "B" && call <= tc.failures {
					w.WriteHeader(tc.status)
					_, _ = fmt.Fprint(w, `{"error":{"code":"limit_requests","message":"Requests rate limit exceeded"}}`)
					return
				}
				_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,2]}]}`))
			}))
			defer server.Close()
			pool, err := ants.NewPool(1)
			require.NoError(t, err)
			defer pool.Release()
			model, err := embedding.NewEmbedder(embedding.Config{
				Source: types.ModelSourceRemote, Provider: "generic", ModelName: "test",
				BaseURL: server.URL, ModelID: "recovery-test",
			}, embedding.NewBatchEmbedder(pool), nil)
			require.NoError(t, err)
			repo := &recoveryRepository{saved: map[string][]float32{}}
			svc := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repo}
			err = svc.BatchIndex(context.Background(), model, []*types.IndexInfo{
				{SourceID: "A", Content: "A"}, {SourceID: "B", Content: "B"},
			}, []types.RetrieverType{types.VectorRetrieverType})
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, map[string]int{"A": 1, "B": tc.wantB}, calls,
				"a failed provider request must not replay successful batches or multiply retry budgets")
			if tc.wantError {
				require.Empty(t, repo.saved)
			} else {
				require.Equal(t, map[string][]float32{"A": {1, 2}, "B": {1, 2}}, repo.saved)
			}
		})
	}
}

func TestBatchIndexPreservesPrecomputedVectorsBeyondBatchSize(t *testing.T) {
	t.Setenv("BATCH_EMBED_SIZE", "2")
	var rows []*types.IndexInfo
	var vectors [][]float32
	want := map[string][]float32{}
	for i := range 45 {
		id := fmt.Sprint(i)
		vector := []float32{float32(i + 1)}
		rows = append(rows, &types.IndexInfo{SourceID: id, Content: "image caption"})
		vectors = append(vectors, vector)
		want[id] = vector
	}
	repo := &recoveryRepository{saved: map[string][]float32{}}
	svc := &KeywordsVectorHybridRetrieveEngineService{indexRepository: repo}
	// A nil inner embedder ensures no provider method can be invoked.
	err := svc.BatchIndex(context.Background(), &precomputedEmbedder{vectors: vectors}, rows,
		[]types.RetrieverType{types.VectorRetrieverType})
	require.NoError(t, err)
	require.Equal(t, want, repo.saved)
}

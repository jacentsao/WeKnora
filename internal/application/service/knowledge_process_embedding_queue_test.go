package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type queueReplayIndexEngine struct {
	failingIndexEngine
	index interfaces.RetrieveEngineService
}

func (e *queueReplayIndexEngine) BatchIndex(
	ctx context.Context, model embedding.Embedder, rows []*types.IndexInfo, kinds []types.RetrieverType,
) error {
	// Composite deduplication iterates a map. Pin this fixture's order so the
	// persistent rejection really occurs after 80 successful HTTP batches.
	sort.Slice(rows, func(i, j int) bool { return rows[i].Content < rows[j].Content })
	return e.index.BatchIndex(ctx, model, rows, kinds)
}

// This is a boundary test, not a promise of resumable ingestion: after the
// request budget is exhausted, a real Asynq redelivery recomputes earlier batches.
func TestProcessChunksQueueRetryReplaysSuccessfulEmbeddingBatches(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	secutils.ResetSSRFWhitelistForTest()
	t.Cleanup(secutils.ResetSSRFWhitelistForTest)
	t.Setenv("BATCH_EMBED_SIZE", "1")
	t.Setenv("EMBED_REQUEST_INTERVAL_MS", "1")
	t.Setenv("EMBED_RETRY_ATTEMPTS", "3")
	t.Setenv("EMBED_RETRY_BASE_DELAY_MS", "1")
	t.Setenv("EMBED_RETRY_MAX_DELAY_MS", "1")
	var mu sync.Mutex
	calls := map[string]int{}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Input) != 1 {
			http.Error(w, "invalid input", http.StatusBadRequest)
			return
		}
		mu.Lock()
		calls[body.Input[0]]++
		mu.Unlock()
		if body.Input[0] == "batch-081" {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1]}]}`)
	}))
	defer provider.Close()
	pool, err := ants.NewPool(1) // Keep the failure deterministically at batch 81.
	require.NoError(t, err)
	defer pool.Release()
	model, err := embedding.NewEmbedder(embedding.Config{
		Source: types.ModelSourceRemote, Provider: "generic", ModelName: "test",
		BaseURL: provider.URL, Dimensions: 1,
	}, embedding.NewBatchEmbedder(pool), nil)
	require.NoError(t, err)
	knowledge := &types.Knowledge{
		ID: "queue-replay", TenantID: 1, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing,
	}
	repo := &indexRetryKnowledgeRepo{parentChildKnowledgeRepo: parentChildKnowledgeRepo{knowledge: knowledge}}
	chunks := &indexRetryChunkRepo{}
	// No repository is needed: persistent batch 81 failure must prevent writes.
	engine := &queueReplayIndexEngine{
		index: retriever.NewKVHybridRetrieveEngine(nil, types.PostgresRetrieverEngineType),
	}
	svc := &knowledgeService{
		repo: repo, chunkRepo: chunks, modelService: parentChildModelService{embedder: model},
		retrieveEngine: parentChildRetrieveRegistry{engine: engine}, graphEngine: parentChildGraphRepo{},
		tenantRepo: parentChildTenantRepo{}, task: parentChildTaskEnqueuer{},
	}
	kb := &types.KnowledgeBase{
		ID: "kb", TenantID: 1, EmbeddingModelID: "model",
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
	}
	tenant := &types.Tenant{
		ID: 1, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{
			RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType,
		}}},
	}
	var parsed []types.ParsedChunk
	for i := 1; i <= 100; i++ {
		parsed = append(parsed, types.ParsedChunk{
			Content: fmt.Sprintf("batch-%03d", i), Seq: i - 1, Start: (i - 1) * 9, End: i * 9, ParentIndex: -1,
		})
	}
	redisServer := miniredis.RunT(t)
	redisOpt := asynq.RedisClientOpt{Addr: redisServer.Addr()}
	client := asynq.NewClient(redisOpt)
	defer func() { _ = client.Close() }()
	worker := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: 1, TaskCheckInterval: 10 * time.Millisecond, DelayedTaskCheckInterval: 10 * time.Millisecond,
		RetryDelayFunc:  func(int, error, *asynq.Task) time.Duration { return 0 },
		ShutdownTimeout: time.Second, LogLevel: asynq.FatalLevel,
	})
	type outcome struct {
		retried int
		status  string
		err     error
	}
	deliveries := make(chan outcome, 2)
	require.NoError(t, worker.Start(asynq.HandlerFunc(func(ctx context.Context, _ *asynq.Task) error {
		retried, _ := asynq.GetRetryCount(ctx)
		ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
		err := svc.processChunks(ctx, kb, knowledge, parsed)
		deliveries <- outcome{retried: retried, status: knowledge.ParseStatus, err: err}
		return err
	})))
	defer worker.Shutdown()
	_, err = client.Enqueue(asynq.NewTask(types.TypeDocumentProcess, nil),
		asynq.MaxRetry(1), asynq.Timeout(5*time.Second))
	require.NoError(t, err)
	for attempt := range 2 {
		select {
		case got := <-deliveries:
			require.Equal(t, attempt, got.retried)
			if attempt == 0 {
				var upstream *api.HTTPError
				require.ErrorAs(t, got.err, &upstream)
				require.Equal(t, http.StatusTooManyRequests, upstream.StatusCode)
				require.Equal(t, types.ParseStatusProcessing, got.status)
			} else {
				require.NoError(t, got.err, "final failure is recorded and acknowledged")
				require.Equal(t, types.ParseStatusFailed, got.status)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("queue did not redeliver the document")
		}
	}
	worker.Shutdown()
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i <= 100; i++ {
		want := 0
		if i <= 80 {
			want = 2 // Once in each document attempt, never an HTTP-layer replay.
		} else if i == 81 {
			want = 6 // Three HTTP attempts in each of two queue deliveries.
		}
		require.Equal(t, want, calls[fmt.Sprintf("batch-%03d", i)], "batch %d", i)
	}
	require.Equal(t, 4, chunks.deletes)
	require.Equal(t, 4, engine.indexDeletes)
}

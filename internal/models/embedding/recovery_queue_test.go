package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/limiter"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/panjf2000/ants/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// CI is self-contained; an isolated real Redis can exercise the same harness.
// Never point this at a production Redis: the tests enqueue synthetic jobs.
func embeddingTestRedis(t *testing.T) string {
	t.Helper()
	if addr := os.Getenv("WEKNORA_EMBEDDING_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return miniredis.RunT(t).Addr()
}

// Two OS processes have independent global governors and worker pools. Their
// different endpoints and wire models can share quota only via the explicit
// scope and Redis, not accidentally via a process-local singleton.
func TestEmbeddingQueueSharesQuotaAcrossProcesses(t *testing.T) {
	if os.Getenv("WEKNORA_EMBEDDING_TEST_WORKER") != "" {
		runEmbeddingQueueWorker(t)
		return
	}
	fastEmbeddingRecovery(t)
	t.Setenv("BATCH_EMBED_SIZE", "1")
	t.Setenv("EMBED_REQUEST_INTERVAL_MS", "0") // The per-model override must win.
	addr := embeddingTestRedis(t)
	var mu sync.Mutex
	var arrivals []time.Time
	inputs := map[string]int{}
	models := map[string]int{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Input) != 1 {
			http.Error(w, "invalid input", http.StatusBadRequest)
			return
		}
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		inputs[body.Input[0]]++
		models[body.Model]++
		mu.Unlock()
		_, _ = fmt.Fprint(w, `{"data":[{"index":0,"embedding":[1]}]}`)
	})
	first := httptest.NewServer(handler)
	defer first.Close()
	second := httptest.NewServer(handler)
	defer second.Close()
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
	defer func() { _ = client.Close() }()
	scope := fmt.Sprintf("embedding-review-%d", time.Now().UnixNano())
	for worker := range 2 {
		for doc := range 50 {
			_, err := client.Enqueue(asynq.NewTask("test:embedding", []byte(fmt.Sprintf("%d/%d", worker, doc))),
				asynq.Queue(fmt.Sprintf("%s-%d", scope, worker)), asynq.MaxRetry(0), asynq.Timeout(30*time.Second))
			require.NoError(t, err)
		}
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	type result struct {
		err    error
		output string
	}
	done := make(chan result, 2)
	for worker, endpoint := range []string{first.URL, second.URL} {
		cmd := exec.CommandContext(ctx, executable,
			"-test.run=^TestEmbeddingQueueSharesQuotaAcrossProcesses$", "-test.v")
		cmd.Env = append(os.Environ(),
			"WEKNORA_EMBEDDING_TEST_WORKER="+fmt.Sprint(worker),
			"WEKNORA_EMBEDDING_TEST_REDIS_ADDR="+addr,
			"WEKNORA_EMBEDDING_TEST_ENDPOINT="+endpoint,
			"WEKNORA_EMBEDDING_TEST_SCOPE="+scope,
		)
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &output
		require.NoError(t, cmd.Start())
		go func() {
			err := cmd.Wait()
			done <- result{err: err, output: output.String()}
		}()
	}
	for range 2 {
		got := <-done
		require.NoError(t, got.err, "%s", got.output)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, arrivals, 200, "100 documents, two HTTP batches each")
	require.Len(t, inputs, 200)
	for input, count := range inputs {
		require.Equal(t, 1, count, "repeated input %s", input)
	}
	require.Equal(t, map[string]int{"model-0": 100, "model-1": 100}, models)
	sort.Slice(arrivals, func(i, j int) bool { return arrivals[i].Before(arrivals[j]) })
	const interval = 20 * time.Millisecond
	// Arrival times include process scheduling and HTTP transport jitter. A
	// rolling 20-request window allows 25ms of jitter, but catches both the
	// initial burst and separate per-process/per-model rate budgets.
	for i := 19; i < len(arrivals); i++ {
		require.GreaterOrEqual(t, arrivals[i].Sub(arrivals[i-19]), 19*interval-25*time.Millisecond)
	}
	span := arrivals[len(arrivals)-1].Sub(arrivals[0])
	require.GreaterOrEqual(t, span, 199*interval-25*time.Millisecond)
	t.Logf("100 queued documents, 2 worker processes, 200 HTTP requests, 20ms shared interval: span=%s", span)
}

func runEmbeddingQueueWorker(t *testing.T) {
	t.Helper()
	allowLoopback(t)
	workerID := os.Getenv("WEKNORA_EMBEDDING_TEST_WORKER")
	scope := os.Getenv("WEKNORA_EMBEDDING_TEST_SCOPE")
	addr := os.Getenv("WEKNORA_EMBEDDING_TEST_REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	limiter.SetRequestLimiter(limiter.NewRedisRequestLimiter(rdb))
	pool, err := ants.NewPool(50)
	require.NoError(t, err)
	defer pool.Release()
	model, err := NewEmbedder(Config{
		Source: types.ModelSourceRemote, Provider: "generic", ModelName: "model-" + workerID,
		ModelID: workerID, APIKey: "test-" + workerID, BaseURL: os.Getenv("WEKNORA_EMBEDDING_TEST_ENDPOINT"),
		ExtraConfig: map[string]string{
			"embedding_rate_limit_scope": scope, "embedding_request_interval_ms": "20",
		},
	}, NewBatchEmbedder(pool), nil)
	require.NoError(t, err)
	worker := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{
		Concurrency: 50, Queues: map[string]int{scope + "-" + workerID: 1},
		TaskCheckInterval: 10 * time.Millisecond, ShutdownTimeout: time.Second, LogLevel: asynq.FatalLevel,
	})
	done := make(chan error, 50)
	require.NoError(t, worker.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		ctx = types.WithBackgroundTask(ctx)
		input := string(task.Payload())
		vectors, err := model.BatchEmbedWithPool(ctx, model, []string{input + "/a", input + "/b"})
		if err == nil && len(vectors) != 2 {
			err = fmt.Errorf("document %s returned %d vectors", input, len(vectors))
		}
		done <- err
		return err
	})))
	defer worker.Shutdown()
	for range 50 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Fatal("embedding queue did not drain")
		}
	}
}

func TestEmbeddingQueueLongRetryAfterReleasesPoolOnCancellation(t *testing.T) {
	for _, backend := range []string{"local", "redis"} {
		for _, stop := range []string{"deadline", "cancel"} {
			t.Run(backend+"/"+stop, func(t *testing.T) {
				fastEmbeddingRecovery(t)
				t.Setenv("BATCH_EMBED_SIZE", "1")
				addr := embeddingTestRedis(t)
				rdb := redis.NewClient(&redis.Options{Addr: addr})
				defer func() { _ = rdb.Close() }()
				limiter.SetRequestLimiter(nil)
				if backend == "redis" {
					limiter.SetRequestLimiter(limiter.NewRedisRequestLimiter(rdb))
				}
				defer limiter.SetRequestLimiter(nil)
				var calls atomic.Int32
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					w.Header().Set("Retry-After", "3600")
					w.WriteHeader(http.StatusTooManyRequests)
				}))
				defer provider.Close()
				pool, err := ants.NewPool(8)
				require.NoError(t, err)
				defer pool.Release()
				model, err := NewEmbedder(Config{
					Source: types.ModelSourceRemote, Provider: "generic", ModelName: "test", BaseURL: provider.URL,
				}, NewBatchEmbedder(pool), nil)
				require.NoError(t, err)
				client := asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
				defer func() { _ = client.Close() }()
				queue := fmt.Sprintf("embedding-cancel-%d", time.Now().UnixNano())
				for i := range 16 {
					_, err := client.Enqueue(asynq.NewTask("test:embedding", []byte(fmt.Sprint(i))),
						asynq.Queue(queue), asynq.MaxRetry(0), asynq.Timeout(time.Second))
					require.NoError(t, err)
				}
				base, cancel := context.WithCancel(context.Background())
				defer cancel()
				worker := asynq.NewServer(asynq.RedisClientOpt{Addr: addr}, asynq.Config{
					Concurrency: 16, Queues: map[string]int{queue: 1},
					BaseContext:       func() context.Context { return base },
					TaskCheckInterval: 10 * time.Millisecond, ShutdownTimeout: time.Second, LogLevel: asynq.FatalLevel,
				})
				started := make(chan struct{}, 16)
				done := make(chan error, 16)
				require.NoError(t, worker.Start(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
					started <- struct{}{}
					input := string(task.Payload())
					_, err := model.BatchEmbedWithPool(ctx, model, []string{input + "/a", input + "/b"})
					done <- err
					return err
				})))
				defer worker.Shutdown()
				for range 16 {
					select {
					case <-started:
					case <-time.After(3 * time.Second):
						t.Fatal("queued documents did not start")
					}
				}
				require.Eventually(t, func() bool { return calls.Load() > 0 }, time.Second, time.Millisecond)
				worker.Stop() // Do not redispatch canceled jobs during this assertion.
				want := context.DeadlineExceeded
				if stop == "cancel" {
					want = context.Canceled
					cancel()
				}
				for range 16 {
					select {
					case err := <-done:
						require.ErrorIs(t, err, want)
					case <-time.After(3 * time.Second):
						t.Fatal("hour-long provider cooldown outlived task cancellation")
					}
				}
				worker.Shutdown()
				require.LessOrEqual(t, calls.Load(), int32(8), "canceled waiters must not retry the provider")
				before := calls.Load()
				probe, cancelProbe := context.WithTimeout(context.Background(), 25*time.Millisecond)
				defer cancelProbe()
				_, err = model.Embed(probe, "new caller after cancellation")
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, before, calls.Load(), "canceling tasks must not erase the shared provider cooldown")
				// All eight slots must be reusable simultaneously, not merely one.
				released := make(chan struct{})
				defer close(released)
				var admitted atomic.Int32
				submitted := make(chan struct{})
				go func() {
					defer close(submitted)
					for range 8 {
						if err := pool.Submit(func() { admitted.Add(1); <-released }); err != nil {
							return
						}
					}
				}()
				require.Eventually(t, func() bool { return admitted.Load() == 8 }, time.Second, time.Millisecond,
					"canceled documents retained embedding pool slots")
				<-submitted
			})
		}
	}
}

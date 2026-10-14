package embedding

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/panjf2000/ants/v2"
)

type batchEmbedder struct {
	pool *ants.Pool
}

func NewBatchEmbedder(pool *ants.Pool) EmbedderPooler {
	return &batchEmbedder{pool: pool}
}

func (e *batchEmbedder) BatchEmbedWithPool(ctx context.Context, model Embedder, texts []string) ([][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(texts) == 0 {
		return nil, nil
	}
	batchSizeStr := os.Getenv("BATCH_EMBED_SIZE")
	if batchSizeStr == "" {
		batchSizeStr = "5"
	}
	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil || batchSize <= 0 {
		return nil, fmt.Errorf("BATCH_EMBED_SIZE must be a positive integer")
	}
	if e.pool == nil {
		return nil, fmt.Errorf("embedding worker pool is unavailable")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var failed sync.Once
	var firstErr error
	recordError := func(err error) {
		failed.Do(func() {
			firstErr = err
			cancel()
		})
	}
	results := make([][]float32, len(texts))
	for start := 0; start < len(texts); start += batchSize {
		if ctx.Err() != nil {
			break
		}
		end := start + min(batchSize, len(texts)-start)
		wg.Add(1)
		err := e.pool.Submit(func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			// Retries belong to the provider request, where successful protocol
			// sub-batches can be retained. The pool never replays a whole batch.
			vectors, err := model.BatchEmbed(ctx, texts[start:end])
			if err != nil {
				recordError(err)
				return
			}
			if len(vectors) != end-start {
				recordError(fmt.Errorf("embedding model returned %d embeddings for %d inputs", len(vectors), end-start))
				return
			}
			// Each callback owns a disjoint range; the caller reads after Wait.
			copy(results[start:end], vectors)
		})
		if err != nil {
			wg.Done() // Submit did not accept the callback.
			recordError(err)
			break
		}
	}
	// Even cancellation and submission failure must drain accepted callbacks.
	// Nothing may still access the caller's inputs after this method returns.
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

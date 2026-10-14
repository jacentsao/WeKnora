package embedding

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type batchTestEmbedder struct {
	Embedder
	embed func(context.Context, []string) ([][]float32, error)
}

func (m *batchTestEmbedder) BatchEmbed(ctx context.Context, texts []string) ([][]float32, error) {
	return m.embed(ctx, texts)
}

func TestBatchPoolConcurrentFailuresAreDrained(t *testing.T) {
	t.Setenv("BATCH_EMBED_SIZE", "1")
	pool, err := ants.NewPool(4)
	require.NoError(t, err)
	defer pool.Release()
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	failed := errors.New("provider failed")
	model := &batchTestEmbedder{embed: func(context.Context, []string) ([][]float32, error) {
		entered <- struct{}{}
		<-release
		return nil, failed
	}}
	done := make(chan error, 1)
	go func() {
		_, err := NewBatchEmbedder(pool).BatchEmbedWithPool(context.Background(), model, []string{"a", "b", "c", "d"})
		done <- err
	}()
	for range 4 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("callbacks did not enter")
		}
	}
	close(release)
	require.ErrorIs(t, <-done, failed)
}

func TestBatchPoolSubmissionFailureWaitsForAcceptedCallback(t *testing.T) {
	t.Setenv("BATCH_EMBED_SIZE", "1")
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	model := &batchTestEmbedder{embed: func(context.Context, []string) ([][]float32, error) {
		close(entered)
		<-release // Deliberately ignore cancellation, like an already-running adapter.
		return [][]float32{{1}}, nil
	}}
	done := make(chan error, 1)
	go func() {
		_, err := NewBatchEmbedder(pool).BatchEmbedWithPool(context.Background(), model, []string{"a", "b"})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first callback did not enter")
	}
	// The second Submit cannot enter this single-worker pool. Closing the
	// pool makes that submission fail while the accepted callback is blocked.
	pool.Release()
	select {
	case err := <-done:
		t.Fatalf("returned while an accepted callback still ran: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	require.ErrorIs(t, <-done, ants.ErrPoolClosed)
}

func TestBatchPoolRejectsInvalidSizeAndVectorCount(t *testing.T) {
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	model := &batchTestEmbedder{embed: func(context.Context, []string) ([][]float32, error) { return nil, nil }}
	for _, value := range []string{"0", "-1", "bad"} {
		t.Setenv("BATCH_EMBED_SIZE", value)
		_, err := NewBatchEmbedder(pool).BatchEmbedWithPool(context.Background(), model, []string{"a"})
		require.ErrorContains(t, err, "positive integer")
	}
	t.Setenv("BATCH_EMBED_SIZE", "1")
	_, err = NewBatchEmbedder(pool).BatchEmbedWithPool(context.Background(), model, []string{"a"})
	require.ErrorContains(t, err, "returned 0 embeddings for 1 inputs")
}

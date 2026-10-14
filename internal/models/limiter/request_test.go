package limiter

import (
	"context"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func requestLimiters(t *testing.T, backend string) []RequestLimiter {
	t.Helper()
	if backend == "local" {
		l := NewLocalRequestLimiter()
		return []RequestLimiter{l, l}
	}
	server := miniredis.RunT(t)
	var out []RequestLimiter
	for range 2 {
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		out = append(out, NewRedisRequestLimiter(client))
	}
	return out
}

func TestRequestLimiterSharesRateAcrossConcurrentClients(t *testing.T) {
	for _, backend := range []string{"local", "redis"} {
		t.Run(backend, func(t *testing.T) {
			clients := requestLimiters(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			const interval = 25 * time.Millisecond
			var wg sync.WaitGroup
			var mu sync.Mutex
			var admitted []time.Time
			errs := make(chan error, 6)
			for i := range 6 {
				wg.Go(func() {
					err := clients[i%len(clients)].Wait(ctx, "shared-account", interval)
					mu.Lock()
					admitted = append(admitted, time.Now())
					mu.Unlock()
					errs <- err
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
			sort.Slice(admitted, func(i, j int) bool { return admitted[i].Before(admitted[j]) })
			for i := 1; i < len(admitted); i++ {
				// Redis stores millisecond timestamps; allow that rounding.
				require.GreaterOrEqual(t, admitted[i].Sub(admitted[i-1]), interval-2*time.Millisecond)
			}
		})
	}
}

func TestRequestLimiterSharesCooldownAndPacesRecovery(t *testing.T) {
	for _, backend := range []string{"local", "redis"} {
		t.Run(backend, func(t *testing.T) {
			clients := requestLimiters(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, clients[0].Cooldown(ctx, "shared", 100*time.Millisecond, 20*time.Millisecond))
			// Another worker's shorter hint must not shorten the shared wait.
			require.NoError(t, clients[1].Cooldown(ctx, "shared", time.Millisecond, time.Millisecond))
			short, stop := context.WithTimeout(ctx, 30*time.Millisecond)
			defer stop()
			require.ErrorIs(t, clients[1].Wait(short, "shared", 0), context.DeadlineExceeded)
			require.NoError(t, clients[1].Wait(ctx, "independent", 0))
			require.NoError(t, clients[1].Wait(ctx, "shared", 0))
			start := time.Now()
			require.NoError(t, clients[0].Wait(ctx, "shared", 0))
			require.GreaterOrEqual(t, time.Since(start), 18*time.Millisecond)
		})
	}
}

func TestRequestLimiterCancelledWaitersDoNotReserveSlots(t *testing.T) {
	for _, backend := range []string{"local", "redis"} {
		t.Run(backend, func(t *testing.T) {
			clients := requestLimiters(t, backend)
			ctx := context.Background()
			interval := 200 * time.Millisecond
			require.NoError(t, clients[0].Wait(ctx, "scope", interval))
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					cancelled, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
					defer cancel()
					err := clients[1].Wait(cancelled, "scope", interval)
					if err != context.DeadlineExceeded {
						t.Errorf("cancelled waiter: %v", err)
					}
				})
			}
			wg.Wait()
			next, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			require.NoError(t, clients[0].Wait(next, "scope", interval), "cancelled callers consumed future quota")
		})
	}
}

func TestRequestLimiterVeryLongCooldownDoesNotOverflow(t *testing.T) {
	for _, backend := range []string{"local", "redis"} {
		t.Run(backend, func(t *testing.T) {
			clients := requestLimiters(t, backend)
			require.NoError(t, clients[0].Cooldown(context.Background(), "scope", math.MaxInt64, time.Second))
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, clients[1].Wait(ctx, "scope", 0), context.DeadlineExceeded)
		})
	}
}

func TestRequestLimiterDoesNotFailOpen(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	l := NewRedisRequestLimiter(client)
	server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Error(t, l.Wait(ctx, "scope", 0))
	require.Error(t, l.Cooldown(ctx, "scope", time.Second, time.Second))
}

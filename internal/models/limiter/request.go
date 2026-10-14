package limiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RequestLimiter spaces requests sharing a provider quota and propagates its
// cooldown. Unlike the concurrency governor, it covers interactive requests too
// and fails closed: a broken quota backend must not unleash queued ingestion.
type RequestLimiter interface {
	Wait(ctx context.Context, key string, interval time.Duration) error
	Cooldown(ctx context.Context, key string, delay, recoveryInterval time.Duration) error
}

var requestGovernor = struct {
	sync.RWMutex
	limiter RequestLimiter
}{limiter: NewLocalRequestLimiter()}

// SetRequestLimiter installs the shared request-rate backend at startup. A nil
// backend restores a fresh local limiter, as used by single-process Lite mode.
func SetRequestLimiter(l RequestLimiter) {
	if l == nil {
		l = NewLocalRequestLimiter()
	}
	requestGovernor.Lock()
	requestGovernor.limiter = l
	requestGovernor.Unlock()
}

// WaitRequest admits one provider request. Each retry must acquire again.
func WaitRequest(ctx context.Context, key string, interval time.Duration) error {
	requestGovernor.RLock()
	l := requestGovernor.limiter
	requestGovernor.RUnlock()
	return l.Wait(ctx, key, interval)
}

// CooldownRequests delays all callers in a quota scope. Recovery traffic is
// paced for a minute after the cooldown, preventing a simultaneous wake-up burst.
func CooldownRequests(ctx context.Context, key string, delay, recoveryInterval time.Duration) error {
	requestGovernor.RLock()
	l := requestGovernor.limiter
	requestGovernor.RUnlock()
	return l.Cooldown(ctx, key, delay, recoveryInterval)
}

const requestRecoveryWindow = time.Minute

type requestState struct {
	next             time.Time
	cooldown         time.Time
	recoveryUntil    time.Time
	recoveryInterval time.Duration
	expires          time.Time
}

type localRequestLimiter struct {
	mu     sync.Mutex
	states map[string]*requestState
	nextGC time.Time
}

// NewLocalRequestLimiter shares quotas between all clients in one process.
func NewLocalRequestLimiter() RequestLimiter {
	return &localRequestLimiter{states: make(map[string]*requestState)}
}

func (l *localRequestLimiter) state(key string, now time.Time) *requestState {
	if !now.Before(l.nextGC) {
		for k, state := range l.states {
			if !now.Before(state.expires) {
				delete(l.states, k)
			}
		}
		l.nextGC = now.Add(requestRecoveryWindow)
	}
	s := l.states[key]
	if s == nil {
		s = &requestState{}
		l.states[key] = s
	}
	s.expires = latestTime(now.Add(requestRecoveryWindow), s.next, s.recoveryUntil)
	return s
}

func (l *localRequestLimiter) Wait(ctx context.Context, key string, interval time.Duration) error {
	return waitForRequest(ctx, func() (time.Duration, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		now := time.Now()
		s := l.state(key, now)
		if wait := latestTime(s.next, s.cooldown).Sub(now); wait > 0 {
			return wait, nil
		}
		if now.Before(s.recoveryUntil) {
			interval = max(interval, s.recoveryInterval)
		}
		s.next = now.Add(interval)
		s.expires = latestTime(s.expires, s.next)
		return 0, nil
	})
}

func (l *localRequestLimiter) Cooldown(
	ctx context.Context, key string, delay, recoveryInterval time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	s := l.state(key, now)
	if !now.Before(s.recoveryUntil) {
		s.recoveryInterval = 0
	}
	s.cooldown = latestTime(s.cooldown, now.Add(delay))
	s.recoveryUntil = s.cooldown.Add(requestRecoveryWindow)
	s.recoveryInterval = max(s.recoveryInterval, recoveryInterval)
	s.expires = latestTime(s.expires, s.recoveryUntil)
	return nil
}

func latestTime(values ...time.Time) time.Time {
	var latest time.Time
	for _, value := range values {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}

// Admission is checked after each wait, rather than reserving future slots:
// a new cooldown affects existing waiters and cancellation leaves no holes.
func waitForRequest(ctx context.Context, acquire func() (time.Duration, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		wait, err := acquire()
		if err != nil || wait <= 0 {
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// Redis TIME avoids clock skew between workers. Admission and cooldown updates
// are atomic and idle keys expire; no worker reserves an unbounded future slot.
// TTLs are capped at the largest whole-millisecond Go duration, so extremely
// long Retry-After values also remain representable by Go-backed Redis servers.
var requestAcquireScript = redis.NewScript(`
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'next', 'cooldown', 'recovery_until', 'recovery_interval')
local next = tonumber(state[1]) or 0
local cooldown = tonumber(state[2]) or 0
local recovery = tonumber(state[3]) or 0
local wait = math.max(next, cooldown) - now
if wait > 0 then return wait end
local interval = tonumber(ARGV[1])
if recovery > now then interval = math.max(interval, tonumber(state[4]) or 0) end
next = now + interval
redis.call('HSET', KEYS[1], 'next', next)
redis.call('PEXPIRE', KEYS[1], math.min(9223372036854, math.max(60000, next - now, recovery - now)))
return 0
`)

var requestCooldownScript = redis.NewScript(`
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'cooldown', 'recovery_interval', 'next', 'recovery_until')
local cooldown = math.max(tonumber(state[1]) or 0, now + tonumber(ARGV[1]))
local previous = 0
if (tonumber(state[4]) or 0) > now then previous = tonumber(state[2]) or 0 end
local interval = math.max(previous, tonumber(ARGV[2]))
local recovery = cooldown + 60000
redis.call('HSET', KEYS[1], 'cooldown', cooldown, 'recovery_until', recovery, 'recovery_interval', interval)
redis.call('PEXPIRE', KEYS[1], math.min(9223372036854, math.max(recovery - now, (tonumber(state[3]) or 0) - now)))
return 0
`)

type redisRequestLimiter struct{ rdb *redis.Client }

// NewRedisRequestLimiter shares provider quotas and cooldowns across replicas.
func NewRedisRequestLimiter(rdb *redis.Client) RequestLimiter {
	return &redisRequestLimiter{rdb: rdb}
}

func (l *redisRequestLimiter) Wait(ctx context.Context, key string, interval time.Duration) error {
	return waitForRequest(ctx, func() (time.Duration, error) {
		if l.rdb == nil {
			return 0, fmt.Errorf("embedding request limiter: Redis is unavailable")
		}
		ms, err := requestAcquireScript.Run(ctx, l.rdb, []string{"weknora:embedding-rate:" + key},
			ceilMilliseconds(interval)).Int64()
		if err != nil {
			return 0, fmt.Errorf("embedding request admission: %w", err)
		}
		if ms > math.MaxInt64/int64(time.Millisecond) {
			return time.Duration(math.MaxInt64), nil
		}
		return time.Duration(ms) * time.Millisecond, nil
	})
}

func (l *redisRequestLimiter) Cooldown(
	ctx context.Context, key string, delay, recoveryInterval time.Duration,
) error {
	if l.rdb == nil {
		return fmt.Errorf("embedding request limiter: Redis is unavailable")
	}
	err := requestCooldownScript.Run(ctx, l.rdb, []string{"weknora:embedding-rate:" + key},
		ceilMilliseconds(delay), ceilMilliseconds(recoveryInterval)).Err()
	if err != nil {
		return fmt.Errorf("embedding request cooldown: %w", err)
	}
	return nil
}

func ceilMilliseconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d-1)/time.Millisecond) + 1
}

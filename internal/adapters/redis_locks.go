package adapters

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

var extendLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

func AcquireLock(ctx context.Context, client *redis.Client, key, token string, ttl time.Duration) (bool, error) {
	return client.SetNX(ctx, key, token, ttl).Result()
}

func ExtendLock(ctx context.Context, client *redis.Client, key, token string, ttl time.Duration) (bool, error) {
	res, err := extendLockScript.Run(ctx, client, []string{key}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return res > 0, nil
}

func ReleaseLock(ctx context.Context, client *redis.Client, key, token string) error {
	_, err := releaseLockScript.Run(ctx, client, []string{key}, token).Result()
	return err
}

type Locker struct {
	client   *redis.Client
	key      string
	token    string
	ttl      time.Duration
	interval time.Duration

	mu     sync.Mutex
	closed bool
	stopCh chan struct{}
}

type LockOptions struct {
	TTL             time.Duration
	RenewInterval   time.Duration
	StartRenewal    bool
	ReleaseOnCancel bool
}

func (o LockOptions) withDefaults() LockOptions {
	if o.TTL <= 0 {
		o.TTL = 30 * time.Second
	}
	if o.RenewInterval <= 0 {
		o.RenewInterval = o.TTL / 3
		if o.RenewInterval < 250*time.Millisecond {
			o.RenewInterval = 250 * time.Millisecond
		}
	}
	return o
}

func TryLock(ctx context.Context, client *redis.Client, key, token string, opts LockOptions) (*Locker, bool, error) {
	opts = opts.withDefaults()
	ok, err := AcquireLock(ctx, client, key, token, opts.TTL)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	l := &Locker{
		client:   client,
		key:      key,
		token:    token,
		ttl:      opts.TTL,
		interval: opts.RenewInterval,
		stopCh:   make(chan struct{}),
	}

	go l.renewLoop(ctx)
	if opts.ReleaseOnCancel {
		go func() {
			<-ctx.Done()
			_ = l.Release(context.Background())
		}()
	}
	return l, true, nil
}

func (l *Locker) renewLoop(ctx context.Context) {
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-t.C:
			_, _ = ExtendLock(ctx, l.client, l.key, l.token, l.ttl)
		}
	}
}

func (l *Locker) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.stopCh)
	l.mu.Unlock()
	return ReleaseLock(ctx, l.client, l.key, l.token)
}

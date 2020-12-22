package mysqlh

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	perrors "github.com/pkg/errors"

	"github.com/huangjunwen/golibs/logr"
)

var (
	// LockNotAcquired is returned when lock is held by other already.
	LockNotAcquired = errors.New("Lock not acquired")
)

var (
	// LockOptDefaultPingInterval is the default value of LockOptions.PingInterval.
	LockOptDefaultPingInterval = 1 * time.Second

	// LockOptDefaultCooldownInterval is the default value of LockOptions.CooldownInterval.
	LockOptDefaultCooldownInterval = 5 * time.Second

	// LockOptDefaultLogger is the default value of LockOptions.Logger.
	LockOptDefaultLogger = logr.Nop
)

// LockOptions is options used in WithLockOpts.
type LockOptions struct {
	// LockTimeout is used in GET_LOCK.
	LockTimeout uint

	// PingInterval is the interval between pings.
	// WithLockOpts needs a ping loop to keep/test the connection aliveness.
	//
	// Use LockOptDefaultPingInterval if not set.
	PingInterval time.Duration

	// CooldownInterval is the time wait after lock acquired and before running the actual callback.
	// This is necessary since even after lock acquired, the previous lock holder may be not
	// aware of it until next ping, also it needs some time to cleanup after context done.
	//
	// Must be larger than PingInterval.
	//
	// Use LockOptDefaultCooldownInterval if not set.
	CooldownInterval time.Duration

	// Logger for logging.
	//
	// Use LockOptDefaultLogger if not set.
	Logger logr.Logger
}

// WithLock is equivalent to WithLockOpts() with opts == nil.
func WithLock(ctx context.Context, db *sql.DB, lockStr string, do func(context.Context)) error {
	return WithLockOpts(ctx, db, lockStr, nil, do)
}

// WithLockOpts try to get/hold a MySQL lock (GET_LOCK) and run a callback, until context done or
// connection lost. This for example can be used to implement singleton pattern.
//
// It retuns LockNotAcquired if lock is not acquired, and nil if lock acquired and callback is called.
//
// NOTE: though a lock can not be held by more than one instance at any time, but the callbacks' time span
// of different instances can be overlapped.
//
// For example, instance 1 got a lock and run callback, then the connection lost due to network error
// or sys admin operation, instance 1 does not know that until its next ping; Meanwhile, instance 2 gets
// the lock and run its callback.
//
// To reduce this overlap, shorten PingInterval to detect more frequently,
// and lengthen CooldownInterval.
func WithLockOpts(ctx context.Context, db *sql.DB, lockStr string, opts *LockOptions, do func(context.Context)) error {

	// Options.
	lockTimeout := uint(0)
	if opts != nil && opts.LockTimeout > 0 {
		lockTimeout = opts.LockTimeout
	}
	pingInterval := LockOptDefaultPingInterval
	if opts != nil && opts.PingInterval != 0 {
		pingInterval = opts.PingInterval
	}
	cooldownInterval := LockOptDefaultCooldownInterval
	if opts != nil && opts.CooldownInterval != 0 {
		cooldownInterval = opts.CooldownInterval
	}
	logger := LockOptDefaultLogger
	if opts != nil && opts.Logger != nil {
		logger = opts.Logger
	}

	if cooldownInterval < pingInterval {
		panic("CooldownInterval must be larger than PingInterval")
	}

	// Use a single connection within this function.
	conn, err := db.Conn(ctx)
	if err != nil {
		return perrors.Wrap(err, "Get connection error")
	}
	defer conn.Close()

	// SELECT GET_LOCK.
	var locked sql.NullInt32
	if err := conn.QueryRowContext(
		ctx,
		"SELECT GET_LOCK(?, ?)",
		lockStr,
		lockTimeout,
	).Scan(&locked); err != nil || !locked.Valid {
		if err == nil && !locked.Valid {
			err = errors.New("invalid result")
		}
		return perrors.Wrap(err, "GET_LOCK error")
	}

	// Not acquired.
	if locked.Int32 != 1 {
		return LockNotAcquired
	}

	// Acquired.
	logger.Info("WithLock lock acquired")
	defer func() {
		var unused sql.NullInt32
		conn.QueryRowContext(
			ctx,
			"SELECT RELEASE_LOCK(?)",
			lockStr,
		).Scan(&unused)
	}()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	// Start a ping loop to test/keep connection aliveness.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer subCancel()
		defer logger.Info("WithLock ping loop end")

		for {
			select {
			case <-subCtx.Done():
				return

			case <-time.After(pingInterval):
				if err := conn.PingContext(ctx); err != nil {
					logger.Error(err, "WithLock ping returns error")
					return
				}
			}
		}
	}()

	// Wait a while before actual work.
	select {
	case <-subCtx.Done():
		return ctx.Err()
	case <-time.After(cooldownInterval):
	}

	logger.Info("WithLock work begin")
	defer logger.Info("WithLock work end")
	do(subCtx)
	return nil
}

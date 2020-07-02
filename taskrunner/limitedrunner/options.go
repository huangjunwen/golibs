package limitedrunner

import (
	"fmt"
	"time"
)

// Option is the option in creating LimitedRunner.
type Option func(*LimitedRunner) error

// MinWorkers sets minimum worker go routines. n >= 1.
func MinWorkers(n int) Option {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("MinWorkers < 1")
		}
		r.minWorkers = n
		return nil
	}
}

// MaxWorkers sets maximum worker go routines. n >= MinWorkers.
func MaxWorkers(n int) Option {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("MaxWorkers < 1")
		}
		r.maxWorkers = n
		return nil
	}
}

// QueueSize sets the task buffered channel. n >= 1.
// n should be set to a large enough value to handle burst.
// Memory overhead: sizeof(function pointer) * n (e.g. 8M if n is 1 million on 64 bit machine)
func QueueSize(n int) Option {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("QueueSize < 1")
		}
		r.queueSize = n
		return nil
	}
}

// IdleTime sets the idle time after which a
// non-persistent worker go routine should exit.
func IdleTime(t time.Duration) Option {
	return func(r *LimitedRunner) error {
		if t < 0 {
			return fmt.Errorf("IdleTime < 0")
		}
		r.idleTime = t
		return nil
	}
}

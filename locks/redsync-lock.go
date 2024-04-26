package main

import (
	"time"

	"github.com/go-redsync/redsync/v4"
)

func read_sync_lock(rs *redsync.Redsync, lockName string, timeout time.Duration) *redsync.Mutex {
	lock_key := LOCK_PREFIX + lockName
	options := []redsync.Option{
		redsync.WithExpiry(timeout),
		redsync.WithTries(3),
	}
	mutex := rs.NewMutex(lock_key, options...)

	if err := mutex.Lock(); err != nil {
		return nil
	}

	return mutex
}

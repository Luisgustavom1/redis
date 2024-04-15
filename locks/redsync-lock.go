package main

import (
	"time"

	"github.com/go-redsync/redsync/v4"
)

func read_sync_lock(rs *redsync.Redsync, lockName string, lockTimeout time.Duration) bool {
	// mutex := rs.NewMutex("test-redsync")

	// if err := mutex.Lock(); err != nil {
	// 	panic(err)
	// }

	// if _, err := mutex.Unlock(); err != nil {
	// 	panic(err)
	// }
	return false
}

package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

func simple_acquire_lock(client *redis.Client, lockName string, timeout time.Duration) bool {
	result := uuid.New().String()
	lockKey := LOCK_PREFIX + lockName

	res, err := client.SetNX(lockKey, result, timeout).Result()
	if err != nil {
		fmt.Println(err)
		return false
	}

	return res
}

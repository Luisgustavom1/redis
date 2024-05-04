package main

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	wg := sync.WaitGroup{}

	go producer(ctx, rdb)

	startConsumer(ctx, 1, rdb, &wg)
	startConsumer(ctx, 2, rdb, &wg)
	startReliableConsumer(ctx, 3, rdb, &wg)

	wg.Wait()
}

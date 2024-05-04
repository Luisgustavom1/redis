package main

import (
	"context"
	"sync"

	"github.com/luisgustavom1/redis/queue/queue"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	wg := sync.WaitGroup{}

	go queue.StartProducer(ctx, rdb)

	consumer := queue.NewConsumerClient()
	consumer.StartConsumer(ctx, rdb, &wg)
	consumer.StartConsumer(ctx, rdb, &wg)
	consumer.StartReliableConsumer(ctx, rdb, &wg)

	wg.Wait()
}

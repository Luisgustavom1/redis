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
	wg.Add(3)
	go consumer.StartConsumer(ctx, 1, rdb, &wg)
	go consumer.StartConsumer(ctx, 2, rdb, &wg)
	go consumer.StartReliableConsumer(ctx, 3, rdb, &wg)

	wg.Wait()
}

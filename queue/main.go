package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const JOB_QUEUE_KEY = "job_queue"

func consumer(ctx context.Context, k int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("Waiting for jobs %d\n", k)
	for {
		job, err := rdb.BRPop(ctx, 0, JOB_QUEUE_KEY).Result()
		if err != nil {
			fmt.Println(err)
			break
		}

		fmt.Printf("consumer %d job: %s\n", k, job)
	}
}

func startConsumer(ctx context.Context, k int, rdb *redis.Client, wg *sync.WaitGroup) {
	wg.Add(1)
	go consumer(ctx, k, rdb, wg)
}

func producer(ctx context.Context, rdb *redis.Client) {
	time.Sleep(3 * time.Second)
	fmt.Sprintln("Pushing jobs to the queue")

	for i := 0; i < 10; i++ {
		rdb.LPush(ctx, JOB_QUEUE_KEY, fmt.Sprintf("key: %d", i+1))
		time.Sleep(1 * time.Second)
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	wg := sync.WaitGroup{}

	go producer(ctx, rdb)

	startConsumer(ctx, 1, rdb, &wg)
	startConsumer(ctx, 2, rdb, &wg)
	startConsumer(ctx, 3, rdb, &wg)

	wg.Wait()
}

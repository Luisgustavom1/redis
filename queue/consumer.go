package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const JOB_QUEUE_KEY = "job_queue"
const JOB_RELIABLE_QUEUE_KEY = "job_reliable_queue"

func startConsumer(ctx context.Context, k int, rdb *redis.Client, wg *sync.WaitGroup) {
	wg.Add(1)
	go consumer(ctx, k, rdb, wg)
}

func startReliableConsumer(ctx context.Context, k int, rdb *redis.Client, wg *sync.WaitGroup) {
	wg.Add(1)

	// to simulate a crash
	randTimeout := time.Duration(1+rand.Intn(5)) * time.Second
	ctxWithTimeout, cancel := context.WithTimeout(ctx, randTimeout)
	defer cancel()

	go reliableConsumer(ctxWithTimeout, k, rdb, wg)
}

func consumer(ctx context.Context, k int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("Waiting for jobs %d\n", k)
	for {
		job, err := rdb.BRPop(ctx, 0, JOB_QUEUE_KEY).Result()
		if err != nil {
			fmt.Println(err)
			break
		}

		fmt.Printf("[%d::PROCESSING] %s\n", k, job[1])
	}
}

func reliableConsumer(ctx context.Context, k int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Waiting for jobs %d\n", k)

	for jobRecovered, err := getFromReliableQueue(ctx, rdb); jobRecovered != ""; jobRecovered, err = getFromReliableQueue(ctx, rdb) {
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("job from reliable queue %s\n", jobRecovered)
		_, err := popFromReliableQueue(ctx, rdb, jobRecovered)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	for {
		job, err := rdb.BRPopLPush(ctx, JOB_QUEUE_KEY, JOB_RELIABLE_QUEUE_KEY, 0).Result()
		if err != nil {
			fmt.Println(err)
			break
		}

		select {
		case <-ctx.Done():
			fmt.Println("crash!")
			return
		default:
			fmt.Printf("[%d::PROCESSING] %s\n", k, job)
			_, err := popFromReliableQueue(ctx, rdb, job)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}

func getFromReliableQueue(ctx context.Context, rdb *redis.Client) (string, error) {
	return rdb.LIndex(ctx, JOB_RELIABLE_QUEUE_KEY, 0).Result()
}

func popFromReliableQueue(ctx context.Context, rdb *redis.Client, job string) (int64, error) {
	return rdb.LRem(ctx, JOB_RELIABLE_QUEUE_KEY, -1, job).Result()
}

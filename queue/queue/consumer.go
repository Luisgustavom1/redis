package queue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const JOB_QUEUE_KEY = "job_queue"
const TEMP_QUEUE_KEY = "temp_queue"

type ConsumerClient interface {
	StartConsumer(ctx context.Context, key int, rdb *redis.Client, wg *sync.WaitGroup)
	StartReliableConsumer(ctx context.Context, key int, rdb *redis.Client, wg *sync.WaitGroup)
}

type Consumer struct{}

func NewConsumerClient() ConsumerClient {
	return &Consumer{}
}

func (c *Consumer) StartConsumer(ctx context.Context, key int, rdb *redis.Client, wg *sync.WaitGroup) {
	consumer(ctx, key, rdb, wg)
}

func (c *Consumer) StartReliableConsumer(ctx context.Context, key int, rdb *redis.Client, wg *sync.WaitGroup) {
	// to simulate a crash
	randTimeout := time.Duration(3+rand.Intn(5)) * time.Second
	ctxWithTimeout, cancel := context.WithTimeout(ctx, randTimeout)
	defer func() {
		cancel()
		if r := recover(); r != nil {
			fmt.Println("Reliable consumer crashes", r)
		}
	}()

	reliableConsumer(ctxWithTimeout, key, rdb, wg)
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

	for jobRecovered, err := getFromTempQueue(ctx, rdb); jobRecovered != ""; jobRecovered, err = getFromTempQueue(ctx, rdb) {
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("[%d::RELIABLE PROCESSING] %s\n", k, jobRecovered)
		_, err := popFromTempQueue(ctx, rdb, jobRecovered)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	for {
		job, err := rdb.BRPopLPush(ctx, JOB_QUEUE_KEY, TEMP_QUEUE_KEY, 0).Result()
		if err != nil {
			fmt.Println(err)
			break
		}

		select {
		case <-ctx.Done():
			panic("crash!!")
		default:
			fmt.Printf("[%d::RELIABLE PROCESSING] %s\n", k, job)
			_, err := popFromTempQueue(ctx, rdb, job)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}

func getFromTempQueue(ctx context.Context, rdb *redis.Client) (string, error) {
	return rdb.LIndex(ctx, TEMP_QUEUE_KEY, 0).Result()
}

func popFromTempQueue(ctx context.Context, rdb *redis.Client, job string) (int64, error) {
	return rdb.LRem(ctx, TEMP_QUEUE_KEY, -1, job).Result()
}

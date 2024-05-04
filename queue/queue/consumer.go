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
const MAX_RETRIES = 3

type ConsumerClient interface {
	StartConsumer(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup)
	StartReliableConsumer(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup)
}

type Consumer struct{}

func NewConsumerClient() ConsumerClient {
	return &Consumer{}
}

func (c *Consumer) StartConsumer(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	consumer(ctx, id, rdb)
}

func (c *Consumer) StartReliableConsumer(ctx context.Context, id int, rdb *redis.Client, wg *sync.WaitGroup) {
	// to simulate a crash
	randTimeout := time.Duration(3+rand.Intn(5)) * time.Second
	ctxWithTimeout, cancel := context.WithTimeout(ctx, randTimeout)
	defer func() {
		cancel()
		if r := recover(); r != nil {
			startReliableConsumerRecover(ctx, c, id, rdb, wg)
			return
		}
		wg.Done()
	}()

	reliableConsumer(ctxWithTimeout, id, rdb)
}

func startReliableConsumerRecover(ctx context.Context, c *Consumer, id int, rdb *redis.Client, wg *sync.WaitGroup) {
	fmt.Printf("[ERROR] consumer %d crashes, trying to recover...\n", id)
	for i := 0; i < MAX_RETRIES; i++ {
		fmt.Println("[RECOVERING] Attempt", i+1)
		// to simulate retries
		r := rand.Intn(100)
		if r%2 == 0 {
			c.StartReliableConsumer(ctx, id, rdb, wg)
			return
		}
		time.Sleep(1 * time.Second)
	}
	wg.Done()
}

func consumer(ctx context.Context, id int, rdb *redis.Client) {
	fmt.Printf("[%d::CONSUMER] working\n", id)
	for {
		job, err := rdb.BRPop(ctx, 0, JOB_QUEUE_KEY).Result()
		if err != nil {
			fmt.Println(err)
			break
		}

		fmt.Printf("[%d::CONSUMER] processing %s\n", id, job[1])
	}
}

func reliableConsumer(ctx context.Context, id int, rdb *redis.Client) {
	fmt.Printf("[%d::R CONSUMER] working\n", id)
	for jobRecovered, err := getFromTempQueue(ctx, rdb); jobRecovered != ""; jobRecovered, err = getFromTempQueue(ctx, rdb) {
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("[%d::R CONSUMER] processing recovered %s\n", id, jobRecovered)
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
			fmt.Printf("[%d::R CONSUMER] processing %s\n", id, job)
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

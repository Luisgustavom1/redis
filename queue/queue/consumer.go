package queue

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/luisgustavom1/redis/queue/logger"
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
	l := logger.NewLogger(id, "R CONSUMER")
	l.Errorln("consumer %d crashes, trying to recover...", id)
	for i := 0; i < MAX_RETRIES; i++ {
		l.Infof("[RECOVERING] Attempt %d\n", i+1)
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
	l := logger.NewLogger(id, "CONSUMER")
	l.Infof("working\n")
	for {
		job, err := rdb.BRPop(ctx, 0, JOB_QUEUE_KEY).Result()
		if err != nil {
			l.Errorln(err.Error())
			break
		}

		l.Infof("processing %s\n", job[1])
	}
}

func reliableConsumer(ctx context.Context, id int, rdb *redis.Client) {
	l := logger.NewLogger(id, "R CONSUMER")
	l.Infof("working\n")
	for jobRecovered, err := getFromTempQueue(ctx, rdb); jobRecovered != ""; jobRecovered, err = getFromTempQueue(ctx, rdb) {
		if err != nil {
			l.Errorln(err.Error())
			return
		}

		l.Infof("processing recovered %s\n", jobRecovered)
		_, err := popFromTempQueue(ctx, rdb, jobRecovered)
		if err != nil {
			l.Errorln(err.Error())
			return
		}
	}

	for {
		job, err := rdb.BRPopLPush(ctx, JOB_QUEUE_KEY, TEMP_QUEUE_KEY, 0).Result()
		if err != nil {
			l.Errorln(err.Error())
			break
		}
		select {
		case <-ctx.Done():
			panic("crash!!")
		default:
			l.Infof("processing %s\n", job)
			_, err := popFromTempQueue(ctx, rdb, job)
			if err != nil {
				l.Errorln(err.Error())
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

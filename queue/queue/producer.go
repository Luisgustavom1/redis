package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func StartProducer(ctx context.Context, rdb *redis.Client) {
	fmt.Sprintln("Pushing jobs to the queue")

	ticker := time.NewTicker(1 * time.Second)

	i := 1
	for {
		select {
		case <-ticker.C:
			rdb.LPush(ctx, JOB_QUEUE_KEY, fmt.Sprintf("job: %d", i))
			i++
		}
	}
}

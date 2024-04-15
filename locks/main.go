package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping().Err(); err != nil {
		panic(err)
	}

	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)

	r := gin.New()

	r.GET("/lock", func(c *gin.Context) {
		lock := simple_acquire_lock(rdb, "simple_lock", time.Second*5)
		must_acquire_lock(c, lock)
	})

	r.GET("/redsync-lock", func(c *gin.Context) {
		lock := read_sync_lock(rs, "distributed_lock", time.Second*5)
		must_acquire_lock(c, lock)
	})

	r.Run()
}

func must_acquire_lock(c *gin.Context, lock bool) {
	if lock {
		c.JSON(http.StatusOK, "lock acquired")
		return
	}

	c.JSON(http.StatusOK, "locked")
}

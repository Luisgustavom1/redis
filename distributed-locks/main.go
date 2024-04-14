package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := rdb.Ping().Err(); err != nil {
		panic(err)
	}

	r := gin.New()

	r.GET("/lock", func(c *gin.Context) {
		lock := simple_acquire_lock(rdb, "simple_lock", time.Second*5)
		if lock {
			c.JSON(http.StatusOK, "lock acquired")
			return
		}

		c.JSON(http.StatusOK, "locked")
	})

	r.GET("/redsync-lock", func(c *gin.Context) {
		fmt.Println("redsync lock")

		c.JSON(http.StatusOK, "redsync lock")
	})

	r.Run()
}

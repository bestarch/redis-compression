package connection

import (
	"github.com/redis/go-redis/v9"
)

func CreateConnection() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "admin",
	})
	return rdb
}

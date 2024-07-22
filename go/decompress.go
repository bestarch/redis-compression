package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bestarch/redis-compression/go/compression_util"
	"github.com/bestarch/redis-compression/go/connection"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Call this function when we already have compressed data in Redis, else it will through error.
// Alternatively, you can call compress.go first and then invoke this function.
func main() {
	db := connection.CreateConnection()

	cursor := uint64(0)
	var keys []string
	var err error

	for {
		keys, cursor, err = db.Scan(ctx, cursor, "_SAM_:*", 100).Result()
		if err != nil {
			log.Fatalf("An error occurred while scanning the keys: %v", err)
		}

		for _, key := range keys {

			kType := db.Type(ctx, key)
			switch kType.Val() {
			case "hash":
				result, err := db.HGetAll(ctx, key).Result()
				if err != nil {
					fmt.Printf("Error: %v\n", err)
					panic(err)
				}

				for field, value := range result {
					decompressed, err0 := compression_util.Decompress([]byte(value))
					if err0 != nil {
						log.Fatalf("An error occrred while compressing hash key: %v", err)
						panic(err0)
					}
					err := db.HMSet(ctx, key, map[string]interface{}{
						field: decompressed,
					}).Err()
					if err != nil {
						log.Fatalf("Could not set hash fields: %v", err)
						panic(err)
					}
				}

			case "zset":
				result, err := db.ZRangeWithScores(ctx, key, 0, -1).Result()
				db.Del(ctx, key)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
					return
				}

				for _, z := range result {
					decompressed, err0 := compression_util.Decompress([]byte(z.Member.(string)))
					if err0 != nil {
						log.Fatalf("An error occrred while compressing sorted-set key: %v", err)
						panic(err0)
					}

					member := []redis.Z{
						{Score: 10, Member: decompressed},
					}
					err := db.ZAdd(ctx, key, member...).Err()
					if err != nil {
						log.Fatalf("Could not add members to sorted set: %v", err)
					}
				}

			case "string":
				value, err := db.Get(ctx, key).Bytes()
				if err != nil {
					log.Printf("Could not get value for key %s: %v", key, err)
					panic(err)
				}
				decompressed, _ := compression_util.Decompress(value)
				newErr := db.Set(ctx, key, decompressed, 0).Err()
				if newErr != nil {
					panic(newErr)
				}

			default:
				log.Printf("Not supported for other commands")
			}
		}
		if cursor == 0 {
			break
		}
	}

}

package golang_redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)

	err := client.Close()
	assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "peppo", 3*time.Second)
	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "peppo", result)

	time.Sleep(3 * time.Second)
	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "name", "peppo")
	client.RPush(ctx, "name", "silpiana")
	client.RPush(ctx, "name", "opep")

	assert.Equal(t, "peppo", client.LPop(ctx, "name").Val())
	assert.Equal(t, "silpiana", client.LPop(ctx, "name").Val())
	assert.Equal(t, "opep", client.LPop(ctx, "name").Val())

	client.Del(ctx, "name")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "animals", "ayam")
	client.SAdd(ctx, "animals", "bebek")
	client.SAdd(ctx, "animals", "cacing")
	client.SAdd(ctx, "animals", "domba")
	client.SAdd(ctx, "animals", "ayam")
	client.SAdd(ctx, "animals", "ayam")
	client.SAdd(ctx, "animals", "ayam")
	client.SAdd(ctx, "animals", "ayam")

	assert.Equal(t, int64(4), client.SCard(ctx, "animals").Val())
	assert.Equal(t, []string{"ayam", "bebek", "cacing", "domba"}, client.SMembers(ctx, "animals").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score: 100, Member: "ayam"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 85, Member: "bebek"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 90, Member: "cacing"})

	assert.Equal(t, []string{"bebek", "cacing", "ayam"}, client.ZRange(ctx, "scores", 0, -1).Val())
	assert.Equal(t, "ayam", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "cacing", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "bebek", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "ayam")
	client.HSet(ctx, "user:1", "email", "ayam@ayam.com")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "ayam", user["name"])
	assert.Equal(t, "ayam@ayam.com", user["email"])
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko A",
		Longitude: 106.822702,
		Latitude:  -6.177590,
	})

	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko B",
		Longitude: 106.820889,
		Latitude:  -6.174964,
	})

	assert.Equal(t, 0.3543, client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val())

	sellers := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude:  106.821825,
		Latitude:   -6.175105,
		Radius:     5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A", "Toko B"}, sellers)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "ayam", "bebek", "cacing")
	client.PFAdd(ctx, "visitors", "ayam", "domba", "cacing")
	client.PFAdd(ctx, "visitors", "elang", "gajah", "cacing")

	assert.Equal(t, int64(6), client.PFCount(ctx, "visitors").Val())
}

func TestPipeline(t *testing.T) {
	_, err := client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "ayam", 3*time.Second)
		pipeliner.SetEx(ctx, "address", "indonesia", 3*time.Second)
		return nil
	})
	// jika ingin mengirimkan banyak data secara langsung ke redis

	assert.Nil(t, err)

	assert.Equal(t, "ayam", client.Get(ctx, "name").Val())
	assert.Equal(t, "indonesia", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "ayam", 3*time.Second)
		pipeliner.SetEx(ctx, "address", "indonesia", 3*time.Second)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, "ayam", client.Get(ctx, "name").Val())
	assert.Equal(t, "indonesia", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "ayam",
				"address": "indonesia",
			},
		})
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestGetStream(t *testing.T) {
	result := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    2,
		Block:    5 * time.Second,
	}).Val()

	for _, stream := range result {
		for _, msg := range stream.Messages {
			fmt.Println(msg.ID)
			fmt.Println(msg.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	subscribe := client.Subscribe(ctx, "channel-1")
	for i := 0; i < 10; i++ {
		message, _ := subscribe.ReceiveMessage(ctx)
		fmt.Println(message.Payload)
	}
	err := subscribe.Close()
	assert.Nil(t, err)
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		client.Publish(ctx, "channel-1", "hello "+strconv.Itoa(i))
	}
}

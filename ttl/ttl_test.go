package ttl_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ukashazia/memcache/ttl"
)

func TestPutAndGet(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      1 * time.Second,
		ShardSize:       2,
		CleanupInterval: 500 * time.Millisecond,
	}
	cache.Init()

	item := &ttl.Item{Key: "test", Value: "value", TTL: 1 * time.Second}
	shardID, err := cache.Put(item)
	assert.Nil(t, err)

	val := cache.Get("test", shardID)
	assert.Equal(t, "value", val)
}

func TestExpiration(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      500 * time.Millisecond,
		ShardSize:       2,
		CleanupInterval: 250 * time.Millisecond,
	}
	cache.Init()

	item := &ttl.Item{Key: "temp", Value: "to expire", TTL: 500 * time.Millisecond}
	shardID, _ := cache.Put(item)

	time.Sleep(600 * time.Millisecond)
	val := cache.Get("temp", shardID)
	assert.Nil(t, val)
}

func TestShardCreation(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      1 * time.Second,
		ShardSize:       1,
		CleanupInterval: 500 * time.Millisecond,
	}
	cache.Init()

	item1 := &ttl.Item{Key: "item1", Value: "data1", TTL: 1 * time.Second}
	shard1, _ := cache.Put(item1)
	item2 := &ttl.Item{Key: "item2", Value: "data2", TTL: 1 * time.Second}
	shard2, _ := cache.Put(item2)

	assert.NotEqual(t, shard1, shard2)
}

func TestConcurrentAccess(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      1 * time.Second,
		ShardSize:       10,
		CleanupInterval: 500 * time.Millisecond,
	}
	cache.Init()

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			item := &ttl.Item{Key: strconv.Itoa(i), Value: i, TTL: 1 * time.Second}
			_, _ = cache.Put(item)
		}(i)
	}

	wg.Wait()
	assert.NotEmpty(t, cache.Get("1", 1))
}

func TestDelete(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      1 * time.Second,
		ShardSize:       2,
		CleanupInterval: 500 * time.Millisecond,
	}
	cache.Init()

	item := &ttl.Item{Key: "delete_me", Value: "gone", TTL: 1 * time.Second}
	shardID, _ := cache.Put(item)
	cache.Delete("delete_me", shardID)

	assert.Nil(t, cache.Get("delete_me", shardID))
}

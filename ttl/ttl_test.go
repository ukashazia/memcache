package ttl_test

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestWorkload(t *testing.T) {
	newTtl := ttl.TTL{
		DefaultTTL:      10 * time.Second,
		ShardSize:       500,
		CleanupInterval: 100 * time.Millisecond,
	}
	newTtl.Init()

	var wg sync.WaitGroup

	numGoroutines := 5

	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key_%d_%d", gID, j)
				item := ttl.Item{
					Key:   key,
					Value: fmt.Sprintf("value_%d_%d", gID, j),
					TTL:   100 * time.Millisecond,
				}
				_, err := newTtl.Put(&item)
				if err != nil {
					return
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key_%d_%d", gID, j)
				value := newTtl.Get(key, 1)
				_ = value
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrentPutGet(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      10 * time.Minute,
		ShardSize:       100,
		CleanupInterval: time.Hour,
	}
	err := cache.Init()
	require.NoError(t, err)

	const numGoroutines = 1000
	var (
		wg          sync.WaitGroup
		keyCounter  uint64
		keyShardMap sync.Map
	)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := generateKey(&keyCounter)
			item := &ttl.Item{Key: key, Value: key}
			shardID, err := cache.Put(item)
			require.NoError(t, err)
			keyShardMap.Store(key, shardID)
		}()
	}
	wg.Wait()

	for i := 1; i < numGoroutines; i++ {
		key := generateTestKey(i)
		shardID, ok := keyShardMap.Load(key)
		require.True(t, ok, "Key %s not found in shard map", key)

		value := cache.Get(key, shardID.(uint64))
		require.Equal(t, key, value, "Key %s mismatch", key)
	}
}

func TestConcurrentPutDeleteGet(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      time.Minute,
		ShardSize:       50,
		CleanupInterval: time.Hour,
	}
	err := cache.Init()
	require.NoError(t, err)

	const (
		numGoroutines = 500
		keyRange      = 100
	)
	var (
		wg          sync.WaitGroup
		keyCounter  uint64
		keyShardMap sync.Map
	)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := generateTestKey(i % keyRange)

			switch atomic.AddUint64(&keyCounter, 1) % 3 {
			case 0:
				item := &ttl.Item{Key: key, Value: key}
				shardID, err := cache.Put(item)
				require.NoError(t, err)
				keyShardMap.Store(key, shardID)
			case 1:
				if shardID, ok := keyShardMap.Load(key); ok {
					cache.Delete(key, shardID.(uint64))
				}
			case 2:
				if shardID, ok := keyShardMap.Load(key); ok {
					value := cache.Get(key, shardID.(uint64))
					if value != nil {
						require.Equal(t, key, value)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestShardCleanupUnderLoad(t *testing.T) {
	cache := &ttl.TTL{
		DefaultTTL:      time.Second,
		ShardSize:       100,
		CleanupInterval: 100 * time.Millisecond,
	}
	err := cache.Init()
	require.NoError(t, err)

	const numGoroutines = 1000
	var (
		wg          sync.WaitGroup
		keyCounter  uint64
		keyShardMap sync.Map
	)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := generateKey(&keyCounter)
			item := &ttl.Item{
				Key:   key,
				Value: key,
				TTL:   time.Second,
			}
			shardID, err := cache.Put(item)
			require.NoError(t, err)
			keyShardMap.Store(key, shardID)
		}()
	}
	wg.Wait()

	time.Sleep(2*time.Second + 100*time.Millisecond)

	keyShardMap.Range(func(key, shardID any) bool {
		value := cache.Get(key.(string), shardID.(uint64))
		require.Nil(t, value, "Key %s should be expired", key)
		return true
	})
}

func generateKey(counter *uint64) string {
	return fmt.Sprintf("key-%d", atomic.AddUint64(counter, 1))
}

func generateTestKey(i int) string {
	return fmt.Sprintf("key-%d", i)
}

package ttl

import (
	"context"
	"sync"
	"time"
)

type TTL struct {
	DefaultTTL      time.Duration
	ShardSize       uint64
	CleanupInterval time.Duration

	shardLookupTable shardLookupTable
}

type Item struct {
	Key            string
	Value          any
	expirationTime time.Time
	TTL            time.Duration
	mutex          sync.RWMutex
}

type shard struct {
	id    uint64
	data  map[string]*Item
	mutex sync.RWMutex
}

type shards map[uint64]*shard

type shardLookupTable struct {
	shards         shards
	currentShardId uint64
	mutex          sync.RWMutex
}

func (ttl *TTL) Init() error {

	newShard := shard{}
	ttl.shardLookupTable = shardLookupTable{shards: shards{}}

	ttl.newShard(&newShard)

	return nil
}

func (ttl *TTL) Put(item *Item) (uint64, error) {
	shardId := ttl.shardLookupTable.currentShardId
	currentShard := ttl.shardLookupTable.shards[shardId]

	if item.TTL.Nanoseconds() > 0 {
		item.expirationTime = time.Now().Add(item.TTL)
	} else {
		item.expirationTime = time.Now().Add(ttl.DefaultTTL)
	}

	if uint64(len(currentShard.data)) < ttl.ShardSize {

		currentShard.mutex.Lock()
		defer currentShard.mutex.Unlock()

		currentShard.data[item.Key] = item
	} else {

		newShard := shard{}
		ttl.newShard(&newShard)

		newShard.mutex.Lock()
		defer newShard.mutex.Unlock()

		newShard.data[item.Key] = item
		shardId = newShard.id
	}

	return shardId, nil
}

func (ttl *TTL) Get(key string, shardId uint64) any {
	data, exists := ttl.shardLookupTable.shards[shardId].data[key]

	if !exists {
		return nil
	}

	data.mutex.RLock()
	defer data.mutex.RUnlock()

	if data.expirationTime.After(time.Now()) {
		return data.Value
	} else {
		return nil
	}
}

func (ttl *TTL) Delete(key string, shardId uint64) {
	shard := ttl.shardLookupTable.shards[shardId]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	delete(shard.data, key)
}

func (ttl *TTL) newShard(shard *shard) {

	ttl.shardLookupTable.mutex.Lock()
	defer ttl.shardLookupTable.mutex.Unlock()

	newShardId := ttl.shardLookupTable.currentShardId + 1
	shard.id = newShardId
	shard.data = make(map[string]*Item)

	ttl.shardLookupTable.shards[newShardId] = shard
	ttl.shardLookupTable.currentShardId = newShardId

	ctx := context.Background()
	go shard.cleanup(ctx, ttl)
}

func (shard *shard) cleanup(ctx context.Context, ttl *TTL) {
	ticker := time.NewTicker(*&ttl.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var expiredKeys []string

			for k, v := range shard.data {
				if v.expirationTime.Before(time.Now()) {
					expiredKeys = append(expiredKeys, k)
				}
			}

			if len(expiredKeys) > 0 {
				shard.mutex.Lock()
				for _, k := range expiredKeys {
					delete(shard.data, k)
				}
				if len(shard.data) == 0 {
					delete(ttl.shardLookupTable.shards, shard.id) // idk if deleting the shard which is references in its own cleanup goroutine would work

					return
				}
				shard.mutex.Unlock()
			}
		}
	}
}

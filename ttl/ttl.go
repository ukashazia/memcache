package ttl

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
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
	id       uint64
	uuid     uuid.UUID
	data     map[string]*Item
	termFunc context.CancelFunc
	mutex    sync.RWMutex
}

type shards map[uint64]*shard

type shardLookupTable struct {
	shards         shards
	currentShardId uint64
	mutex          sync.RWMutex
}

func (ttl *TTL) Init() error {

	newShard := shard{}
	ttl.shardLookupTable = shardLookupTable{shards: make(shards)}

	ttl.newShard(&newShard)

	return nil
}

func (ttl *TTL) Put(item *Item) (uint64, error) {

	ttl.shardLookupTable.mutex.RLock()
	shardId := ttl.shardLookupTable.currentShardId
	currentShard := ttl.shardLookupTable.shards[shardId]
	ttl.shardLookupTable.mutex.RUnlock()

	if item.TTL.Nanoseconds() > 0 {
		item.expirationTime = time.Now().Add(item.TTL)
	} else {
		item.expirationTime = time.Now().Add(ttl.DefaultTTL)
	}

	currentShard.mutex.Lock()
	if uint64(len(currentShard.data)) < ttl.ShardSize {

		defer currentShard.mutex.Unlock()

		currentShard.data[item.Key] = item
	} else {
		currentShard.mutex.Unlock()

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
	ttl.shardLookupTable.mutex.RLock()
	shard, exists := ttl.shardLookupTable.shards[shardId]
	ttl.shardLookupTable.mutex.RUnlock()

	if !exists {
		return nil
	}

	shard.mutex.RLock()
	data, exists := shard.data[key]
	shard.mutex.RUnlock()

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
	shard, exists := ttl.shardLookupTable.shards[shardId]

	if !exists {
		return
	}

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	delete(shard.data, key)
}

func (ttl *TTL) newShard(shard *shard) {

	ttl.shardLookupTable.mutex.Lock()
	defer ttl.shardLookupTable.mutex.Unlock()

	newShardId := ttl.shardLookupTable.currentShardId + 1
	ctx, cancel := context.WithCancel(context.Background())

	shard.id = newShardId
	shard.data = make(map[string]*Item)
	shard.uuid = uuid.New()
	shard.termFunc = cancel

	ttl.shardLookupTable.shards[newShardId] = shard
	ttl.shardLookupTable.currentShardId = newShardId

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

			shard.mutex.Lock()
			for k, v := range shard.data {
				if v.expirationTime.Before(time.Now()) {
					expiredKeys = append(expiredKeys, k)
				}
			}

			if len(expiredKeys) > 0 {
				for _, k := range expiredKeys {
					delete(shard.data, k)
				}
			}

			shardEmpty := len(shard.data) == 0

			if shardEmpty {
				shard.mutex.Unlock()
				ttl.terminateShard(shard)
				return
			}
			shard.mutex.Unlock()
		}
	}
}

func (ttl *TTL) terminateShard(shard *shard) {

	ttl.shardLookupTable.mutex.Lock()
	defer ttl.shardLookupTable.mutex.Unlock()

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, exists := ttl.shardLookupTable.shards[shard.id]; exists {
		shard.termFunc()
		delete(ttl.shardLookupTable.shards, shard.id)
	}
}

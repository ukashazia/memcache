package ttl

import (
	"context"
	"sync"
	"time"
)

// TTL represents a time-based cache system with sharding and dynamic eviction.
type TTL struct {
	DefaultTTL      time.Duration // Default time-to-live for items if not specified
	ShardSize       uint64        // Maximum number of items per shard
	CleanupInterval time.Duration // Frequency of cleanup operations

	shardLookupTable shardLookupTable // Internal structure to manage shards
}

// Item represents a cache entry with expiration management.
type Item struct {
	Key            string        // Unique key identifier for the item
	Value          any           // The stored value
	expirationTime time.Time     // The time when the item expires
	TTL            time.Duration // Custom TTL for the item (overrides DefaultTTL)
}

// shard represents an individual partition of the cache.
type shard struct {
	id           uint64             // Unique identifier for the shard
	data         map[string]*Item   // Storage for cached items
	termFunc     context.CancelFunc // Function to terminate cleanup routines
	isTerminated bool               // Flag to indicate if the shard has been terminated
	mutex        sync.RWMutex       // Mutex for safe concurrent access
}

// shards is a map of shard IDs to shard instances.
type shards map[uint64]*shard

// shardLookupTable maintains the mapping of shard IDs and the current active shard.
type shardLookupTable struct {
	shards         shards       // Mapping of shard IDs to shards
	currentShardId uint64       // ID of the currently active shard
	mutex          sync.RWMutex // Mutex for safe concurrent access
}

// Init initializes the TTL cache by creating the first shard.
func (ttl *TTL) Init() error {
	newShard := shard{}
	ttl.shardLookupTable = shardLookupTable{shards: make(shards)}

	ttl.newShard(&newShard)
	return nil
}

// Put inserts an item into the cache and returns the shard ID it was stored in.
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

	if uint64(len(currentShard.data)) < ttl.ShardSize && !currentShard.isTerminated && currentShard != nil {

		currentShard.data[item.Key] = item
		currentShard.mutex.Unlock()
	} else {
		currentShard.mutex.Unlock()

		newShard := shard{}
		ttl.newShard(&newShard)

		newShard.mutex.Lock()
		newShard.data[item.Key] = item
		shardId = newShard.id
		newShard.mutex.Unlock()
	}
	return shardId, nil
}

// Get retrieves an item from the cache given a key and shard ID.
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

	if data.expirationTime.After(time.Now()) {
		return data.Value
	}
	return nil
}

// Delete removes an item from the cache.
func (ttl *TTL) Delete(key string, shardId uint64) {
	ttl.shardLookupTable.mutex.RLock()
	shard, exists := ttl.shardLookupTable.shards[shardId]
	ttl.shardLookupTable.mutex.RUnlock()

	if !exists {
		return
	}

	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	delete(shard.data, key)
}

// newShard creates and initializes a new shard.
func (ttl *TTL) newShard(shard *shard) {
	ttl.shardLookupTable.mutex.Lock()
	defer ttl.shardLookupTable.mutex.Unlock()

	newShardId := ttl.shardLookupTable.currentShardId + 1
	ctx, cancel := context.WithCancel(context.Background())

	shard.id = newShardId
	shard.data = make(map[string]*Item)
	shard.termFunc = cancel

	ttl.shardLookupTable.shards[newShardId] = shard
	ttl.shardLookupTable.currentShardId = newShardId

	go shard.cleanup(ctx, ttl)
}

// cleanup periodically removes expired items and terminates empty shards.
func (shard *shard) cleanup(ctx context.Context, ttl *TTL) {
	ticker := time.NewTicker(ttl.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			shard.mutex.Lock()
			for k, v := range shard.data {
				if v.expirationTime.Before(time.Now()) {
					delete(shard.data, k)
				}
			}

			if len(shard.data) == 0 {

				shard.mutex.Unlock()
				ttl.terminateShard(shard)
				return
			}
			shard.mutex.Unlock()
		}
	}
}

// terminateShard removes an empty shard from the lookup table.
func (ttl *TTL) terminateShard(shard *shard) {

	ttl.shardLookupTable.mutex.Lock()
	defer ttl.shardLookupTable.mutex.Unlock()

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, exists := ttl.shardLookupTable.shards[shard.id]; exists {
		shard.termFunc()
		shard.isTerminated = true
		delete(ttl.shardLookupTable.shards, shard.id)
	}
}

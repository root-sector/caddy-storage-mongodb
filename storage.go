package caddymongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/google/uuid" // Added for unique lock IDs
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var (
	ErrNotExist   = errors.New("key does not exist")
	ErrLockExists = errors.New("lock already exists")
	// ErrLockNotHeld indicates an attempt to refresh or unlock a lock not held by the current instance
	ErrLockNotHeld = errors.New("lock not held by this instance")
)

type CacheItem struct {
	Value    []byte
	Modified time.Time
}

const (
	// Lock time-to-live duration
	lockTTL = 5 * time.Second

	// Delay between attempts to obtain Lock
	lockPollInterval = 1 * time.Second

	// How frequently the Lock's TTL should be updated
	lockRefreshInterval = 3 * time.Second
)

type MongoDBStorage struct {
	URI                  string        `json:"uri,omitempty"`
	Database             string        `json:"database,omitempty"`
	Collection           string        `json:"collection,omitempty"`
	Timeout              time.Duration `json:"timeout,omitempty"`
	CacheTTL             time.Duration `json:"cache_ttl,omitempty"`
	CacheCleanupInterval time.Duration `json:"cache_cleanup_interval,omitempty"`
	MaxCacheSize         int           `json:"max_cache_size,omitempty"`
	MaxPoolSize          uint64        `json:"max_pool_size,omitempty"`
	MinPoolSize          uint64        `json:"min_pool_size,omitempty"`
	MaxConnIdleTime      time.Duration `json:"max_conn_idle_time,omitempty"`

	client       *mongo.Client `json:"-"`
	logger       *zap.Logger   `json:"-"`
	cache        map[string]CacheItem
	cacheLock    sync.RWMutex
	requestGroup singleflight.Group
	locks        sync.Map // Track active locks: map[string]*lockHandle
}

// lockHandle stores information about an active lock
type lockHandle struct {
	lockID     string             // Unique ID for this specific lock instance
	cancelFunc context.CancelFunc // Function to cancel the refresh goroutine
}

func init() {
	caddy.RegisterModule(&MongoDBStorage{})
}

// CaddyModule returns the Caddy module information.
func (s *MongoDBStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.mongodb",
		New: func() caddy.Module { return new(MongoDBStorage) },
	}
}

// CertMagicStorage returns the storage instance itself,
// ensuring it's provisioned by Caddy's lifecycle.
func (s *MongoDBStorage) CertMagicStorage() (certmagic.Storage, error) {
	// Caddy calls Provision automatically, so the client should be ready.
	// We could add a check here, but it might be redundant.
	if s.client == nil {
		// This case should ideally not happen if Caddy's module lifecycle works as expected.
		// Attempting a late provision might be problematic.
		return nil, fmt.Errorf("mongodb storage not provisioned; client is nil")
	}
	return s, nil
}

// Provision sets up the module: loads config, establishes connection, ensures indexes, starts background tasks.
func (s *MongoDBStorage) Provision(ctx caddy.Context) error {
	s.logger = ctx.Logger(s)
	s.locks = sync.Map{} // Initialize locks map

	// Load Environment Variables and set defaults if not set via Caddyfile
	if s.URI == "" {
		s.URI = os.Getenv("MONGODB_URI")
	}
	if s.Database == "" {
		s.Database = os.Getenv("MONGODB_DATABASE")
	}
	if s.Collection == "" {
		s.Collection = os.Getenv("MONGODB_COLLECTION")
	}
	if s.Timeout == 0 {
		s.Timeout = 10 * time.Second
	}
	if s.CacheCleanupInterval == 0 {
		s.CacheCleanupInterval = 5 * time.Minute
	}
	if s.MaxCacheSize == 0 {
		s.MaxCacheSize = 1000 // Default max cache items
	}
	if s.CacheTTL == 0 {
		s.CacheTTL = 10 * time.Minute // Default cache item TTL
	}
	if s.MaxPoolSize == 0 {
		if val := os.Getenv("MONGODB_MAX_POOL_SIZE"); val != "" {
			if size, err := strconv.ParseUint(val, 10, 64); err == nil {
				s.MaxPoolSize = size
			} else {
				s.logger.Warn("Invalid MONGODB_MAX_POOL_SIZE", zap.String("value", val), zap.Error(err))
				s.MaxPoolSize = 100 // Default max pool size
			}
		} else {
			s.MaxPoolSize = 100 // Default max pool size
		}
	}
	if s.MinPoolSize == 0 {
		if val := os.Getenv("MONGODB_MIN_POOL_SIZE"); val != "" {
			if size, err := strconv.ParseUint(val, 10, 64); err == nil {
				s.MinPoolSize = size
			} else {
				s.logger.Warn("Invalid MONGODB_MIN_POOL_SIZE", zap.String("value", val), zap.Error(err))
				// Default min pool size is 0 (no minimum enforced by default in driver)
			}
		}
		// Default min pool size is 0
	}
	if s.MaxConnIdleTime == 0 {
		if val := os.Getenv("MONGODB_MAX_CONN_IDLE_TIME"); val != "" {
			if dur, err := time.ParseDuration(val); err == nil {
				s.MaxConnIdleTime = dur
			} else {
				s.logger.Warn("Invalid MONGODB_MAX_CONN_IDLE_TIME", zap.String("value", val), zap.Error(err))
				s.MaxConnIdleTime = 5 * time.Minute // Default idle time
			}
		} else {
			s.MaxConnIdleTime = 5 * time.Minute // Default idle time
		}
	}

	// Validate required configuration
	if err := s.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Establish MongoDB Connection
	if err := s.connect(ctx); err != nil { // Use lowercase 'connect'
		return fmt.Errorf("failed to connect/setup mongodb: %w", err)
	}

	// Start background tasks
	go s.cleanupCache()
	go s.periodicLockCleanup(ctx) // Use lowercase 'periodicLockCleanup'

	s.logger.Info("MongoDB storage provisioned successfully")
	return nil
}

// UnmarshalCaddyfile sets up the storage module from Caddyfile tokens.
func (s *MongoDBStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "uri":
			s.URI = value
		case "database":
			s.Database = value
		case "collection":
			s.Collection = value
		case "timeout":
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return d.Errf("invalid timeout duration: %v", err)
			}
			s.Timeout = timeout
		case "cache_ttl":
			ttl, err := time.ParseDuration(value)
			if err != nil {
				return d.Errf("invalid cache_ttl duration: %v", err)
			}
			s.CacheTTL = ttl
		case "cache_cleanup_interval":
			interval, err := time.ParseDuration(value)
			if err != nil {
				return d.Errf("invalid cache_cleanup_interval duration: %v", err)
			}
			s.CacheCleanupInterval = interval
		case "max_cache_size":
			size, err := strconv.Atoi(value)
			if err != nil {
				return d.Errf("invalid max_cache_size: %v", err)
			}
			s.MaxCacheSize = size
		case "max_pool_size":
			size, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return d.Errf("invalid max_pool_size: %v", err)
			}
			s.MaxPoolSize = size
		case "min_pool_size":
			size, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return d.Errf("invalid min_pool_size: %v", err)
			}
			s.MinPoolSize = size
		case "max_conn_idle_time":
			dur, err := time.ParseDuration(value)
			if err != nil {
				return d.Errf("invalid max_conn_idle_time duration: %v", err)
			}
			s.MaxConnIdleTime = dur
		}
	}

	return nil
}

// Store puts value at key
func (s *MongoDBStorage) Store(ctx context.Context, key string, value []byte) error {
	s.logger.Debug("Storing key", zap.String("key", key))

	now := time.Now().UTC()
	doc := bson.M{
		"_id":      key,
		"value":    value,
		"modified": now,
	}

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": key},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return err
	}

	// Update cache
	s.cacheLock.Lock()
	s.cache[key] = CacheItem{
		Value:    value,
		Modified: now,
	}
	s.cacheLock.Unlock()

	return nil
}

// Load retrieves the value at key, using cache if available.
func (s *MongoDBStorage) Load(ctx context.Context, key string) ([]byte, error) {
	s.cacheLock.RLock()
	item, cached := s.cache[key]
	s.cacheLock.RUnlock()

	if cached && time.Since(item.Modified) < s.CacheTTL {
		s.logger.Debug("Key loaded from cache", zap.String("key", key))
		return item.Value, nil
	}

	// Prevent redundant calls using singleflight
	val, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		s.logger.Debug("Loading key from MongoDB", zap.String("key", key))

		ctx, cancel := context.WithTimeout(ctx, s.Timeout)
		defer cancel()

		collection := s.client.Database(s.Database).Collection(s.Collection)
		result := collection.FindOne(ctx, bson.M{"_id": key})

		var doc struct {
			Value []byte `bson:"value"`
		}
		err := result.Decode(&doc)
		if err != nil {
			s.logger.Warn("Key not found", zap.String("key", key))
			return nil, fs.ErrNotExist
		}

		// Cache the result
		s.cacheLock.Lock()
		s.cache[key] = CacheItem{Value: doc.Value, Modified: time.Now()}
		s.cacheLock.Unlock()

		return doc.Value, nil
	})

	if err != nil {
		return nil, err
	}
	return val.([]byte), nil
}

// Delete deletes the named key and clears the cache.
func (s *MongoDBStorage) Delete(ctx context.Context, key string) error {
	s.logger.Debug("Deleting key", zap.String("key", key))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	// Use DeleteOne to remove only the specific key requested by CertMagic
	result, err := collection.DeleteOne(ctx, bson.M{"_id": key})

	if err != nil {
		// Don't log ErrNoDocuments as an error, it just means the key wasn't there
		if err != mongo.ErrNoDocuments {
			s.logger.Error("Failed to delete key", zap.String("key", key), zap.Error(err))
		}
		// If the document didn't exist, it's not an error from CertMagic's perspective
		return nil
	}

	if result.DeletedCount == 0 {
		s.logger.Debug("Attempted to delete non-existent key", zap.String("key", key))
		// Not an error if the key didn't exist
		return nil
	}

	s.logger.Debug("Successfully deleted key", zap.String("key", key))

	// Remove from cache (regardless of whether it was deleted from DB or not found)
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	delete(s.cache, key)

	return nil
}

// Exists returns true if the key exists, checking the cache first.
func (s *MongoDBStorage) Exists(ctx context.Context, key string) bool {
	s.logger.Debug("Checking existence of key", zap.String("key", key))

	s.cacheLock.RLock()
	_, cached := s.cache[key]
	s.cacheLock.RUnlock()

	if cached {
		s.logger.Debug("Key exists in cache", zap.String("key", key))
		return true
	}

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	count, err := collection.CountDocuments(ctx, bson.M{"_id": key})

	if err != nil {
		s.logger.Error("Failed to check existence of key", zap.String("key", key), zap.Error(err))
		return false
	}

	exists := count > 0
	s.logger.Debug("Key existence check result", zap.String("key", key), zap.Bool("exists", exists))
	return exists
}

// List lists all keys in the given path.
func (s *MongoDBStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	collection := s.client.Database(s.Database).Collection(s.Collection)

	// Ensure prefix doesn't start with /
	prefix = strings.TrimPrefix(prefix, "/")

	var filter bson.M
	if recursive {
		// For recursive listing, match anything that starts with the prefix
		filter = bson.M{
			"_id": bson.M{
				"$regex": fmt.Sprintf("^%s", regexp.QuoteMeta(prefix)),
			},
		}
	} else {
		if prefix == "" {
			// For empty prefix, match only top-level entries
			filter = bson.M{
				"_id": bson.M{
					"$regex": "^[^/]+(?=/|$)",
				},
			}
		} else {
			// For non-recursive with prefix, match direct children
			prefix = strings.TrimSuffix(prefix, "/") + "/"
			filter = bson.M{
				"_id": bson.M{
					"$regex": fmt.Sprintf("^%s[^/]+(?=/|$)", regexp.QuoteMeta(prefix)),
				},
			}
		}
	}

	// Add lock file exclusion
	filter["_id"].(bson.M)["$not"] = primitive.Regex{
		Pattern: "\\.lock$",
		Options: "",
	}

	s.logger.Debug("List operation",
		zap.String("prefix", prefix),
		zap.Bool("recursive", recursive),
		zap.Any("filter", filter))

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %v", err)
	}
	defer cursor.Close(ctx)

	var results []string
	seen := make(map[string]bool)

	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode document: %v", err)
		}

		// For non-recursive listing, extract the top-level component
		if !recursive {
			parts := strings.Split(doc.ID, "/")
			if len(parts) > 0 {
				if prefix == "" {
					// For root listing, take the first component
					if !seen[parts[0]] {
						results = append(results, parts[0])
						seen[parts[0]] = true
					}
				} else {
					// For prefixed listing, take the component after the prefix
					prefixParts := strings.Split(strings.TrimSuffix(prefix, "/"), "/")
					if len(parts) > len(prefixParts) {
						nextPart := parts[len(prefixParts)]
						if !seen[nextPart] {
							results = append(results, nextPart)
							seen[nextPart] = true
						}
					}
				}
			}
		} else {
			results = append(results, doc.ID)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %v", err)
	}

	s.logger.Debug("List results",
		zap.Int("count", len(results)),
		zap.Strings("results", results))

	return results, nil
}

// Stat returns information about the key.
func (s *MongoDBStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	s.logger.Debug("Stating key", zap.String("key", key))

	collection := s.client.Database(s.Database).Collection(s.Collection)

	var doc struct {
		Key      string    `bson:"_id"`
		Value    []byte    `bson:"value"`
		Modified time.Time `bson:"modified"`
	}

	err := collection.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return certmagic.KeyInfo{}, fs.ErrNotExist
		}
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:      doc.Key,
		Modified: doc.Modified.UTC(),
		Size:     int64(len(doc.Value)),
	}, nil
}

// Lock acquires a distributed lock for the given key.
func (s *MongoDBStorage) Lock(ctx context.Context, key string) error {
	s.logger.Debug("Attempting to acquire lock", zap.String("key", key))
	lockKey := key + ".lock"
	lockID := uuid.NewString() // Unique ID for this lock attempt

	// Use a timeout for the entire lock acquisition attempt
	lockCtx, cancel := context.WithTimeout(ctx, s.Timeout) // Use configured timeout
	defer cancel()

	for {
		select {
		case <-lockCtx.Done():
			s.logger.Warn("Lock acquisition timed out or context cancelled", zap.String("key", key), zap.Error(lockCtx.Err()))
			return lockCtx.Err()
		default:
			collection := s.client.Database(s.Database).Collection(s.Collection)
			now := time.Now()
			expires := now.Add(lockTTL)

			// Attempt to insert the lock document atomically. Include a 'type' field.
			_, err := collection.InsertOne(lockCtx, bson.M{
				"_id":     lockKey,
				"type":    "lock", // Add type field for indexing
				"lock_id": lockID,
				"expires": expires,
				"created": now,
			})

			if err == nil {
				// Successfully inserted - lock acquired
				s.logger.Debug("Lock acquired successfully", zap.String("key", key), zap.String("lockID", lockID))
				// Start refresh goroutine with a cancellable context derived from background
				refreshCtx, cancelRefresh := context.WithCancel(context.Background())
				handle := &lockHandle{
					lockID:     lockID,
					cancelFunc: cancelRefresh,
				}
				s.locks.Store(key, handle)                // Store the handle associated with the original key
				go s.refreshLock(refreshCtx, key, lockID) // Use lowercase 'refreshLock'
				return nil
			}

			// If it wasn't a successful insert, check if it was a duplicate key error
			if mongo.IsDuplicateKeyError(err) {
				s.logger.Debug("Lock exists, checking expiration", zap.String("key", key))
				// Lock exists, check if it's expired
				var existingLock struct {
					Expires time.Time `bson:"expires"`
				}
				findErr := collection.FindOne(lockCtx, bson.M{"_id": lockKey}).Decode(&existingLock)

				if findErr == nil && existingLock.Expires.After(now) {
					// Lock exists and is not expired, wait and retry
					s.logger.Debug("Lock held by another instance, polling", zap.String("key", key))
					select {
					case <-time.After(lockPollInterval):
						continue // Retry the loop
					case <-lockCtx.Done():
						return lockCtx.Err() // Context cancelled while polling
					}
				} else if findErr == mongo.ErrNoDocuments || (findErr == nil && !existingLock.Expires.After(now)) {
					// Lock document exists but is expired, or was deleted between InsertOne and FindOne.
					// Try to update the expired/missing lock with our ID. Use UpdateOne with filter for _id.
					s.logger.Debug("Attempting to take over expired/missing lock", zap.String("key", key))
					updateResult, updateErr := collection.UpdateOne(lockCtx,
						bson.M{"_id": lockKey, "expires": bson.M{"$lt": now}}, // Condition: only update if expired
						// Ensure 'type' field is set during takeover as well
						bson.M{"$set": bson.M{"type": "lock", "lock_id": lockID, "expires": expires, "created": now}},
					)

					if updateErr == nil && updateResult.MatchedCount > 0 {
						// Successfully updated the expired lock
						s.logger.Debug("Successfully took over expired lock", zap.String("key", key), zap.String("lockID", lockID))
						refreshCtx, cancelRefresh := context.WithCancel(context.Background())
						handle := &lockHandle{
							lockID:     lockID,
							cancelFunc: cancelRefresh,
						}
						s.locks.Store(key, handle)
						go s.refreshLock(refreshCtx, key, lockID) // Use lowercase 'refreshLock'
						return nil
					} else {
						// Failed to update (maybe another instance got it first, or transient error)
						s.logger.Debug("Failed to take over expired lock, retrying", zap.String("key", key), zap.Error(updateErr))
						// Fall through to poll/retry logic
					}
				} else if findErr != nil {
					// Error finding the lock document other than ErrNoDocuments
					s.logger.Error("Error checking existing lock", zap.String("key", key), zap.Error(findErr))
					return fmt.Errorf("failed to check existing lock for key %s: %w", key, findErr)
				}
			} else {
				// Other error during InsertOne
				s.logger.Error("Error acquiring lock", zap.String("key", key), zap.Error(err))
				return fmt.Errorf("failed to acquire lock for key %s: %w", key, err)
			}

			// If we reached here (e.g., failed update, non-duplicate insert error handled), wait and retry
			select {
			case <-time.After(lockPollInterval):
				continue // Retry the loop
			case <-lockCtx.Done():
				return lockCtx.Err() // Context cancelled while polling
			}
		}
	}
}

// refreshLock periodically updates the lock's expiration time in MongoDB.
// It uses a background context for the ticker but respects the cancellation
// signal from the passed context (refreshCtx) which is cancelled by Unlock.
func (s *MongoDBStorage) refreshLock(refreshCtx context.Context, key string, expectedLockID string) {
	ticker := time.NewTicker(lockRefreshInterval)
	defer ticker.Stop()
	lockKey := key + ".lock"

	s.logger.Debug("Starting lock refresh routine", zap.String("key", key), zap.String("lockID", expectedLockID))

	for {
		select {
		case <-ticker.C:
			// Check if the lock is still supposed to be held by us locally.
			// This check prevents unnecessary DB updates if Unlock was already called.
			if handleGeneric, exists := s.locks.Load(key); exists {
				if handle, ok := handleGeneric.(*lockHandle); !ok || handle.lockID != expectedLockID {
					s.logger.Warn("Lock handle mismatch or type assertion failed during refresh check; stopping refresh.", zap.String("key", key), zap.String("expectedLockID", expectedLockID))
					return // Lock ID changed locally or type issue, stop refreshing
				}
			} else {
				s.logger.Debug("Lock no longer tracked locally; stopping refresh.", zap.String("key", key), zap.String("expectedLockID", expectedLockID))
				return // Lock no longer tracked locally, stop.
			}

			// Create a short-lived context for the database operation
			updateCtx, cancelUpdate := context.WithTimeout(context.Background(), s.Timeout) // Use configured timeout

			collection := s.client.Database(s.Database).Collection(s.Collection)
			newExpires := time.Now().Add(lockTTL)

			// Atomically update the expiration only if the lock ID matches
			result, err := collection.UpdateOne(updateCtx,
				bson.M{"_id": lockKey, "lock_id": expectedLockID}, // Condition: ID and lock_id must match
				bson.M{"$set": bson.M{"expires": newExpires}},
			)
			cancelUpdate() // Release context resources promptly

			if err != nil {
				// Log error but continue ticker, maybe transient network issue
				s.logger.Error("Failed to refresh lock in DB", zap.String("key", key), zap.String("lockID", expectedLockID), zap.Error(err))
				// Consider stopping refresh after several consecutive errors?
			} else if result.MatchedCount == 0 {
				// The lock document was not found or the lock_id didn't match.
				// This means we lost the lock (e.g., expired and another instance took it, or it was manually deleted).
				s.logger.Warn("Failed to refresh lock: lock not found or lock ID mismatch. Stopping refresh.", zap.String("key", key), zap.String("lockID", expectedLockID))
				// Clean up local tracking as we no longer hold the lock
				s.locks.Delete(key)
				return // Stop the refresh goroutine
			}
			// Removed noisy debug log for successful refresh:
			// s.logger.Debug("Lock refreshed successfully", zap.String("key", key), zap.String("lockID", expectedLockID), zap.Time("newExpires", newExpires))

		case <-refreshCtx.Done():
			s.logger.Debug("Lock refresh routine cancelled", zap.String("key", key), zap.String("lockID", expectedLockID))
			return // Exit goroutine due to Unlock or other cancellation
		}
	}
}

// Unlock releases the distributed lock for the given key.
// It only deletes the lock if the current instance holds it (based on lockID).
func (s *MongoDBStorage) Unlock(ctx context.Context, key string) error {
	s.logger.Debug("Attempting to release lock", zap.String("key", key))
	lockKey := key + ".lock"

	// Retrieve the lock handle from local tracking
	handleGeneric, loaded := s.locks.LoadAndDelete(key)
	if !loaded {
		s.logger.Warn("Unlock called for a key not tracked locally", zap.String("key", key))
		// Attempt deletion anyway, but without checking lock ID? Or just return?
		// Let's try deleting without ID check for robustness, but log it.
		// return nil // Or maybe attempt deletion without ID check?
	}

	handle, ok := handleGeneric.(*lockHandle)
	if !ok && loaded {
		// This indicates a programming error (wrong type stored in sync.Map)
		s.logger.Error("Invalid type found in locks map during Unlock", zap.String("key", key))
		// Still attempt deletion without ID check?
		// return fmt.Errorf("internal error: invalid type in locks map for key %s", key)
	}

	// If we had a valid handle, cancel the refresh goroutine
	if handle != nil {
		s.logger.Debug("Cancelling lock refresh routine", zap.String("key", key), zap.String("lockID", handle.lockID))
		handle.cancelFunc()
	} else {
		s.logger.Warn("No valid lock handle found during unlock, cannot cancel refresh", zap.String("key", key))
	}

	// Create a short-lived context for the database operation
	deleteCtx, cancelDelete := context.WithTimeout(ctx, s.Timeout) // Use configured timeout
	defer cancelDelete()

	collection := s.client.Database(s.Database).Collection(s.Collection)

	// Atomically delete the lock only if the lock ID matches the one we tracked
	var err error
	var result *mongo.DeleteResult
	if handle != nil {
		result, err = collection.DeleteOne(deleteCtx, bson.M{"_id": lockKey, "lock_id": handle.lockID})
	} else {
		// If we didn't have a local handle, attempt deletion without checking lock_id as a fallback.
		s.logger.Warn("Attempting lock deletion without lock ID check as no local handle was found", zap.String("key", key))
		result, err = collection.DeleteOne(deleteCtx, bson.M{"_id": lockKey})
	}

	if err != nil {
		// Don't treat ErrNoDocuments as an error for Unlock
		if err == mongo.ErrNoDocuments {
			s.logger.Debug("Lock document not found during Unlock", zap.String("key", key))
			return nil
		}
		s.logger.Error("Failed to delete lock from DB", zap.String("key", key), zap.Error(err))
		return err // Return the actual DB error
	}

	if result.DeletedCount == 0 {
		// This could happen if the lock expired and was deleted by cleanup,
		// or if the lock_id didn't match (another instance took it). Not necessarily an error.
		s.logger.Warn("Lock document not deleted during Unlock (perhaps already gone or lock ID mismatch?)", zap.String("key", key))
	} else {
		s.logger.Debug("Lock released successfully", zap.String("key", key))
	}

	return nil
}

// Cleanup disconnects the MongoDB client.
func (s *MongoDBStorage) Cleanup() error {
	s.logger.Info("Cleaning up MongoDB storage module")

	// Disconnect MongoDB client if connected
	if s.client != nil {
		s.logger.Debug("Disconnecting MongoDB client")
		disconnectCtx, cancel := context.WithTimeout(context.Background(), s.Timeout) // Use background context for cleanup
		defer cancel()
		if err := s.client.Disconnect(disconnectCtx); err != nil {
			s.logger.Error("Failed to disconnect MongoDB client during cleanup", zap.Error(err))
			// Don't return error on cleanup failure, just log it
		}
		s.client = nil
	} else {
		s.logger.Debug("MongoDB client already disconnected or never connected")
	}

	// Note: Background goroutines (cache cleanup, lock cleanup) will stop
	// implicitly when the main process exits. If explicit cancellation is needed,
	// the Caddy context passed to Provision could potentially be used,
	// or separate cancellation contexts managed.

	return nil
}

func (s *MongoDBStorage) cleanupCache() {
	ticker := time.NewTicker(s.CacheCleanupInterval)
	for range ticker.C {
		s.cacheLock.Lock()
		now := time.Now()
		for key, item := range s.cache {
			if now.Sub(item.Modified) > s.CacheTTL {
				delete(s.cache, key)
			}
		}
		// Enforce max cache size if needed
		if len(s.cache) > s.MaxCacheSize {
			// Create a slice of cache items sorted by last modified time
			items := make([]struct {
				key      string
				modified time.Time
			}, 0, len(s.cache))

			for k, v := range s.cache {
				items = append(items, struct {
					key      string
					modified time.Time
				}{k, v.Modified})
			}

			// Sort items by modified time (oldest first)
			sort.Slice(items, func(i, j int) bool {
				return items[i].modified.Before(items[j].modified)
			})

			// Remove oldest items until we're under the limit
			for i := 0; i < len(items)-s.MaxCacheSize; i++ {
				delete(s.cache, items[i].key)
			}
		}
		s.cacheLock.Unlock()
	}
}

func (s *MongoDBStorage) withRetry(op func() error) error {
	var err error
	for i := 0; i < 3; i++ {
		if err = op(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	return err
}

// periodicLockCleanup runs periodically to remove expired lock documents from the database.
func (s *MongoDBStorage) periodicLockCleanup(ctx context.Context) {
	ticker := time.NewTicker(s.CacheCleanupInterval) // Reuse cache cleanup interval for lock cleanup frequency
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.logger.Debug("Running periodic lock cleanup")
			cleanupCtx, cancel := context.WithTimeout(context.Background(), s.Timeout) // Short timeout for cleanup op

			collection := s.client.Database(s.Database).Collection(s.Collection)
			// Filter by type: "lock" and expired time
			filter := bson.M{
				"type":    "lock",
				"expires": bson.M{"$lt": time.Now()},
			}

			result, err := collection.DeleteMany(cleanupCtx, filter)
			cancel() // Release context

			if err != nil {
				s.logger.Error("Failed during periodic lock cleanup", zap.Error(err))
			} else if result.DeletedCount > 0 {
				s.logger.Info("Cleaned up expired locks", zap.Int64("count", result.DeletedCount))
			} else {
				s.logger.Debug("No expired locks found during cleanup")
			}

		case <-ctx.Done():
			s.logger.Info("Stopping periodic lock cleanup due to context cancellation.")
			return // Exit goroutine if Caddy context is cancelled
		}
	}
}

// connect establishes the MongoDB connection and ensures indexes. Called by Provision.
func (s *MongoDBStorage) connect(ctx context.Context) error {
	if s.client != nil {
		s.logger.Debug("MongoDB client already connected.")
		// Optionally ping here to ensure connection is alive?
		return nil
	}

	s.logger.Info("Connecting to MongoDB", zap.String("uri", s.URI)) // Avoid logging full URI in production if sensitive
	s.cache = make(map[string]CacheItem)                             // Initialize cache here

	connectCtx, cancel := context.WithTimeout(ctx, s.Timeout) // Use Caddy context with timeout
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(s.URI).
		SetServerSelectionTimeout(s.Timeout).
		SetConnectTimeout(s.Timeout).
		SetMaxPoolSize(s.MaxPoolSize).        // Use configured pool size
		SetMinPoolSize(s.MinPoolSize).        // Use configured pool size
		SetMaxConnIdleTime(s.MaxConnIdleTime) // Use configured idle time

	client, err := mongo.Connect(connectCtx, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test connection
	pingCtx, cancelPing := context.WithTimeout(ctx, s.Timeout) // Separate timeout for ping
	defer cancelPing()
	if err = client.Ping(pingCtx, readpref.Primary()); err != nil {
		// Disconnect if ping fails
		disconnectCtx, cancelDisconnect := context.WithTimeout(context.Background(), 5*time.Second) // Short background timeout for disconnect
		_ = client.Disconnect(disconnectCtx)
		cancelDisconnect()
		return fmt.Errorf("failed to ping MongoDB primary: %w", err)
	}

	s.client = client
	s.logger.Info("Successfully connected to MongoDB")

	// Ensure indexes are created after successful connection
	indexCtx, cancelIndex := context.WithTimeout(ctx, s.Timeout) // Separate timeout for index creation
	defer cancelIndex()
	if err := s.ensureIndexes(indexCtx); err != nil { // Use lowercase 'ensureIndexes'
		// Don't necessarily fail provisioning if index creation fails, but log prominently
		s.logger.Error("Failed to ensure MongoDB indexes", zap.Error(err))
		// return fmt.Errorf("failed to ensure indexes: %w", err) // Or just log?
	}

	return nil
}

// ensureIndexes creates the necessary indexes if they don't exist. Called by connect.
func (s *MongoDBStorage) ensureIndexes(ctx context.Context) error {
	if s.client == nil {
		return errors.New("cannot ensure indexes without a connected client")
	}
	collection := s.client.Database(s.Database).Collection(s.Collection)
	s.logger.Debug("Ensuring necessary MongoDB indexes exist", zap.String("database", s.Database), zap.String("collection", s.Collection))

	// Index for efficient lock cleanup by expiry time
	// This index allows MongoDB to automatically remove expired lock documents
	// if the 'expires' field is set correctly.
	lockExpiryIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "expires", Value: 1}}, // Index on the 'expires' field
		Options: options.Index().
			SetName("lock_expiry_ttl_idx"). // Descriptive name
			SetExpireAfterSeconds(0).       // Documents expire immediately when 'expires' time is reached
			// Apply this index only to documents that have an 'expires' field and are of type "lock"
			SetPartialFilterExpression(bson.M{"type": "lock"}),
	}

	// Index for finding locks by lock_id (used in refresh/unlock)
	lockIdIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "lock_id", Value: 1}},
		Options: options.Index().
			SetName("lock_id_idx").
			SetUnique(false). // Lock IDs are unique per acquisition, but not globally across time
			// Apply only to lock documents that have a lock_id
			SetPartialFilterExpression(bson.M{"lock_id": bson.M{"$exists": true}}),
	}

	// Create the indexes
	indexView := collection.Indexes()
	_, err := indexView.CreateMany(ctx, []mongo.IndexModel{lockExpiryIndex, lockIdIndex})
	if err != nil {
		// Check if the error is just about existing indexes, which is fine
		if mongo.IsDuplicateKeyError(err) || strings.Contains(err.Error(), "index already exists") || strings.Contains(err.Error(), "IndexOptionsConflict") {
			s.logger.Debug("Indexes already exist or conflict, which is likely okay.", zap.Error(err))
			return nil // Not a fatal error
		}
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	s.logger.Info("Successfully ensured MongoDB indexes")
	return nil
}

// Validate ensures the essential MongoDB configuration is present. Called by Provision.
func (s *MongoDBStorage) Validate() error {
	if s.URI == "" {
		return errors.New("mongodb URI is required")
	}
	if s.Database == "" {
		return errors.New("mongodb database name is required")
	}
	if s.Collection == "" {
		return errors.New("mongodb collection name is required")
	}
	// Add validation for other fields if necessary (e.g., positive timeouts)
	return nil
}

// Interface guards
var (
	_ caddy.Provisioner  = (*MongoDBStorage)(nil)
	_ caddy.CleanerUpper = (*MongoDBStorage)(nil)
	_ caddy.Validator    = (*MongoDBStorage)(nil)
	_ certmagic.Storage  = (*MongoDBStorage)(nil)
	_ certmagic.Locker   = (*MongoDBStorage)(nil) // Implement Locker interface
)

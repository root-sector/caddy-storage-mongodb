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
	URI                 string        `json:"uri,omitempty"`
	Database            string        `json:"database,omitempty"`
	Collection          string        `json:"collection,omitempty"`
	Timeout            time.Duration `json:"timeout,omitempty"`
	CacheTTL           time.Duration `json:"cache_ttl,omitempty"`
	CacheCleanupInterval time.Duration `json:"cache_cleanup_interval,omitempty"`
	MaxCacheSize        int          `json:"max_cache_size,omitempty"`
	
	client       *mongo.Client `json:"-"`
	logger       *zap.Logger   `json:"-"`
	cache        map[string]CacheItem
	cacheLock    sync.RWMutex
	requestGroup singleflight.Group
	locks      sync.Map // Track active locks
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

// CertMagicStorage converts the MongoDBStorage to a certmagic.Storage.
func (s *MongoDBStorage) CertMagicStorage() (certmagic.Storage, error) {
	return NewStorage(s)
}

// NewStorage creates a new MongoDBStorage instance and sets up the MongoDB client.
func NewStorage(c *MongoDBStorage) (certmagic.Storage, error) {
	clientOptions := options.Client().
		ApplyURI(c.URI).
		SetMaxPoolSize(100).
		SetMinPoolSize(10).
		SetMaxConnecting(50).
		SetMaxConnIdleTime(time.Minute * 5)

	mongoCtx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	client, err := mongo.Connect(mongoCtx, clientOptions)
	if err != nil {
		return nil, err
	}

	c.client = client

	// Create TTL index for lock expiry
	collection := client.Database(c.Database).Collection(c.Collection)
	
	// Only create the TTL index, let MongoDB handle _id index
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: "modified", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(int32(c.CacheTTL.Seconds())),
	}
	
	_, err = collection.Indexes().CreateOne(context.Background(), indexModel)
	if err != nil {
		return nil, err
	}

	c.cache = make(map[string]CacheItem)
	c.CacheTTL = 10 * time.Minute

	return c, nil
}

// Provision sets up the module.
func (s *MongoDBStorage) Provision(ctx caddy.Context) error {
	s.logger = ctx.Logger(s)

	// Load Environment Variables if not set
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
		s.MaxCacheSize = 1000
	}
	if s.CacheTTL == 0 {
		s.CacheTTL = 10 * time.Minute
	}

	// Start cache cleanup routine
	go s.cleanupCache()

	// Start lock cleanup routine
	go func() {
		ticker := time.NewTicker(s.CacheCleanupInterval)
		for range ticker.C {
			if err := s.withRetry(func() error {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), s.Timeout)
				defer cancel()
				return s.cleanupExpiredLocks(cleanupCtx)
			}); err != nil {
				s.logger.Error("Failed to cleanup locks", zap.Error(err))
			}
		}
	}()

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
	_, err := collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$regex": "^" + key}})

	if err != nil {
		s.logger.Error("Failed to delete key", zap.String("key", key), zap.Error(err))
		return err
	}

	// Remove from cache
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

// Lock acquires a lock for the given key
func (s *MongoDBStorage) Lock(ctx context.Context, key string) error {
	s.logger.Debug("Acquiring lock", zap.String("key", key))

	lockKey := key + ".lock"

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			collection := s.client.Database(s.Database).Collection(s.Collection)
			
			// Check if lock exists and is not expired
			var existingLock struct {
				Expires time.Time `bson:"expires"`
			}
			err := collection.FindOne(ctx, bson.M{
				"_id": lockKey,
				"expires": bson.M{"$gt": time.Now()},
			}).Decode(&existingLock)
			
			if err == nil {
				// Lock exists and is not expired
				return ErrLockExists
			}
			
			if err != mongo.ErrNoDocuments {
				return err
			}

			// Try to create or update the lock
			lockExpiration := time.Now().Add(lockTTL)
			_, err = collection.UpdateOne(ctx,
				bson.M{"_id": lockKey},
				bson.M{"$set": bson.M{
					"created": time.Now(),
					"expires": lockExpiration,
				}},
				options.Update().SetUpsert(true),
			)

			if err == nil {
				// Store the lock information
				s.locks.Store(lockKey, lockExpiration)
				// Start lock refresh goroutine
				go s.refreshLock(ctx, lockKey)
				return nil
			}

			// If we got a duplicate key error, wait and try again
			if mongo.IsDuplicateKeyError(err) {
				select {
				case <-time.After(lockPollInterval):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return err
		}
	}
}

// refreshLock periodically refreshes the lock TTL
func (s *MongoDBStorage) refreshLock(ctx context.Context, lockKey string) {
	ticker := time.NewTicker(lockRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if lock still exists in our tracking
			if _, exists := s.locks.Load(lockKey); !exists {
				return
			}

			// Refresh lock expiration
			collection := s.client.Database(s.Database).Collection(s.Collection)
			_, err := collection.UpdateOne(ctx,
				bson.M{"_id": lockKey},
				bson.M{"$set": bson.M{"expires": time.Now().Add(lockTTL)}},
			)
			if err != nil {
				s.logger.Error("Failed to refresh lock", zap.String("key", lockKey), zap.Error(err))
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// Unlock releases the lock for the given key
func (s *MongoDBStorage) Unlock(ctx context.Context, key string) error {
	s.logger.Debug("Releasing lock", zap.String("key", key))
	
	lockKey := key + ".lock"
	
	// Remove from our lock tracking
	s.locks.Delete(lockKey)
	
	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.DeleteOne(ctx, bson.M{"_id": lockKey})
	
	if err != nil {
		s.logger.Error("Failed to release lock", zap.String("key", key), zap.Error(err))
		return err
	}
	
	return nil
}

// Cleanup closes the MongoDB client connection.
func (s *MongoDBStorage) Cleanup() error {
	s.logger.Info("Cleaning up MongoDB storage")

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	err := s.client.Disconnect(ctx)
	if err != nil {
		s.logger.Error("Failed to disconnect MongoDB client", zap.Error(err))
		return err
	}

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
				key     string
				modified time.Time
			}, 0, len(s.cache))
			
			for k, v := range s.cache {
				items = append(items, struct {
					key     string
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

// cleanupExpiredLocks removes expired locks from the database
func (s *MongoDBStorage) cleanupExpiredLocks(ctx context.Context) error {
	collection := s.client.Database(s.Database).Collection(s.Collection)
	
	// Delete all locks that have expired
	filter := bson.M{
		"_id": bson.M{"$regex": "\\.lock$"},
		"expires": bson.M{"$lt": time.Now()},
	}
	
	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired locks: %v", err)
	}
	
	if result.DeletedCount > 0 {
		s.logger.Debug("Cleaned up expired locks", zap.Int64("count", result.DeletedCount))
	}
	
	return nil
}

func (s *MongoDBStorage) GetClient() any {
	return s.client
}

// Connect establishes the MongoDB connection and sets up required indexes
func (s *MongoDBStorage) Connect() error {
	if s.client != nil {
		return nil
	}

	s.cache = make(map[string]CacheItem)

	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(s.URI).
		SetServerSelectionTimeout(s.Timeout).
		SetConnectTimeout(s.Timeout)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Test connection
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	s.client = client
	
	// Ensure indexes are created
	if err := s.ensureIndexes(ctx); err != nil {
		return fmt.Errorf("failed to ensure indexes: %v", err)
	}

	s.logger.Info("MongoDB storage initialized successfully")
	return nil
}

// ensureIndexes creates the necessary indexes if they don't exist
func (s *MongoDBStorage) ensureIndexes(ctx context.Context) error {
	collection := s.client.Database(s.Database).Collection(s.Collection)

	// Create TTL index for lock expiry
	lockIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "expires", Value: 1}},
		Options: options.Index().
			SetName("lock_expiry_idx").
			SetExpireAfterSeconds(0),
	}

	// Create the index
	if _, err := collection.Indexes().CreateOne(ctx, lockIndex); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create TTL index: %v", err)
		}
	}

	return nil
}

// Validate ensures the MongoDB configuration is valid
func (s *MongoDBStorage) Validate() error {
	if s.URI == "" {
		return fmt.Errorf("MongoDB URI is required")
	}
	if s.Database == "" {
		return fmt.Errorf("MongoDB database name is required")
	}
	if s.Collection == "" {
		return fmt.Errorf("MongoDB collection name is required")
	}
	return nil
}

func (s *MongoDBStorage) Disconnect(ctx context.Context) error {
	if s.client != nil {
		if err := s.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("failed to disconnect from MongoDB: %v", err)
		}
		s.client = nil
	}
	return nil
}


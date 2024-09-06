package caddymongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

type MongoDBStorage struct {
	URI          string        `json:"uri,omitempty"`
	Database     string        `json:"database,omitempty"`
	Collection   string        `json:"collection,omitempty"`
	Timeout      time.Duration `json:"timeout,omitempty"`
	client       *mongo.Client `json:"-"`
	logger       *zap.Logger   `json:"-"`
	cache        map[string]CacheItem
	cacheLock    sync.RWMutex
	cacheTTL     time.Duration
	requestGroup singleflight.Group // Prevents redundant calls for the same key
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
		SetMaxPoolSize(100) // Enable connection pooling

	mongoCtx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	client, err := mongo.Connect(mongoCtx, clientOptions)
	if err != nil {
		return nil, err
	}

	c.client = client

	// Ensure the collection exists and create necessary indexes
	collection := client.Database(c.Database).Collection(c.Collection)
	indexes := collection.Indexes()
	_, err = indexes.CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.M{"_id": 1},
	})
	if err != nil {
		return nil, err
	}

	c.cache = make(map[string]CacheItem)
	c.cacheTTL = 10 * time.Minute // Set cache TTL

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
		timeoutStr := os.Getenv("MONGODB_TIMEOUT")
		if timeoutStr != "" {
			timeout, err := time.ParseDuration(timeoutStr)
			if err != nil {
				s.logger.Error("Invalid timeout duration", zap.Error(err))
				return err
			}
			s.Timeout = timeout
		} else {
			s.Timeout = 10 * time.Second // Default timeout
		}
	}

	s.logger.Info("Provisioning MongoDB storage", zap.String("uri", s.URI), zap.String("database", s.Database), zap.String("collection", s.Collection))

	// Validate the URI scheme
	if !isValidURIScheme(s.URI) {
		err := fmt.Errorf("invalid URI scheme: must be 'mongodb' or 'mongodb+srv', got '%s'", s.URI)
		s.logger.Error("Invalid URI scheme", zap.Error(err))
		return err
	}

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
		}
	}

	return nil
}

// Helper function to validate the URI scheme
func isValidURIScheme(uri string) bool {
	return strings.HasPrefix(uri, "mongodb://") || strings.HasPrefix(uri, "mongodb+srv://")
}

// Store puts value at key. It creates the key if it does not exist and overwrites any existing value at this key.
func (s *MongoDBStorage) Store(ctx context.Context, key string, value []byte) error {
	s.logger.Debug("Storing key", zap.String("key", key))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.UpdateOne(ctx, bson.M{"_id": key}, bson.M{
		"$set": bson.M{
			"value":    value,
			"modified": time.Now(),
		},
	}, options.Update().SetUpsert(true))

	if err != nil {
		s.logger.Error("Failed to store key", zap.String("key", key), zap.Error(err))
		return err
	}

	// Update cache asynchronously
	go func() {
		s.cacheLock.Lock()
		defer s.cacheLock.Unlock()
		s.cache[key] = CacheItem{Value: value, Modified: time.Now()}
	}()

	return nil
}

// Load retrieves the value at key, using cache if available.
func (s *MongoDBStorage) Load(ctx context.Context, key string) ([]byte, error) {
	s.cacheLock.RLock()
	item, cached := s.cache[key]
	s.cacheLock.RUnlock()

	if cached && time.Since(item.Modified) < s.cacheTTL {
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
	s.logger.Debug("Listing keys", zap.String("prefix", prefix), zap.Bool("recursive", recursive))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	filter := bson.M{"_id": bson.M{"$regex": "^" + prefix}}

	cur, err := collection.Find(ctx, filter)
	if err != nil {
		s.logger.Error("Failed to list keys", zap.String("prefix", prefix), zap.Error(err))
		return nil, err
	}
	defer cur.Close(ctx)

	var keys []string
	for cur.Next(ctx) {
		var doc struct {
			Key string `bson:"_id"`
		}
		err := cur.Decode(&doc)
		if err != nil {
			s.logger.Error("Failed to decode key", zap.Error(err))
			continue
		}
		keys = append(keys, doc.Key)
	}

	return keys, nil
}

// Stat returns information about the key.
func (s *MongoDBStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	s.logger.Debug("Stating key", zap.String("key", key))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	result := collection.FindOne(ctx, bson.M{"_id": key})

	var doc struct {
		Value    []byte    `bson:"value"`
		Modified time.Time `bson:"modified"`
	}
	err := result.Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return certmagic.KeyInfo{}, fs.ErrNotExist
		}
		s.logger.Error("Failed to stat key", zap.String("key", key), zap.Error(err))
		return certmagic.KeyInfo{}, err
	}

	ki := certmagic.KeyInfo{
		Key:        key,
		Size:       int64(len(doc.Value)),
		Modified:   doc.Modified,
		IsTerminal: false,
	}

	return ki, nil
}

// Lock acquires a lock for the given key.
func (s *MongoDBStorage) Lock(ctx context.Context, key string) error {
	s.logger.Debug("Acquiring lock", zap.String("key", key))

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.InsertOne(ctx, bson.M{
		"_id":     key + ".lock",
		"created": time.Now(),
	})
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrLockExists
		}
		s.logger.Error("Failed to acquire lock", zap.String("key", key), zap.Error(err))
		return err
	}

	return nil
}

// Unlock releases the lock for the given key.
func (s *MongoDBStorage) Unlock(ctx context.Context, key string) error {
	s.logger.Debug("Releasing lock", zap.String("key", key))

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.DeleteOne(ctx, bson.M{"_id": key + ".lock"})
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

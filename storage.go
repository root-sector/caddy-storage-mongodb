package caddymongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

var (
	ErrNotExist   = errors.New("key does not exist")
	ErrLockExists = errors.New("lock already exists")
)

type MongoDBStorage struct {
	URI        string        `json:"uri,omitempty"`
	Database   string        `json:"database,omitempty"`
	Collection string        `json:"collection,omitempty"`
	Timeout    time.Duration `json:"timeout,omitempty"`
	client     *mongo.Client `json:"-"`
	logger     *zap.Logger   `json:"-"`
}

func init() {
	caddy.RegisterModule(MongoDBStorage{})
}

// CaddyModule returns the Caddy module information.
func (MongoDBStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.mongodb",
		New: func() caddy.Module { return new(MongoDBStorage) },
	}
}

// CertMagicStorage converts the MongoDBStorage to a certmagic.Storage.
func (s MongoDBStorage) CertMagicStorage() (certmagic.Storage, error) {
	return NewStorage(s)
}

// NewStorage creates a new MongoDBStorage instance and sets up the MongoDB client.
func NewStorage(c MongoDBStorage) (certmagic.Storage, error) {
	clientOptions := options.Client().ApplyURI(c.URI)
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

	return &c, nil
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
	s.logger.Info("Storing key", zap.String("key", key))

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
	s.logger.Info("Successfully stored key", zap.String("key", key))
	return nil
}

// Load retrieves the value at key.
func (s *MongoDBStorage) Load(ctx context.Context, key string) ([]byte, error) {
	s.logger.Info("Loading key", zap.String("key", key))

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

	s.logger.Info("Successfully loaded key", zap.String("key", key))
	return doc.Value, nil
}

// Delete deletes the named key. If the name is a directory (i.e. prefix of other keys), all keys prefixed by this key should be deleted.
func (s *MongoDBStorage) Delete(ctx context.Context, key string) error {
	s.logger.Info("Deleting key", zap.String("key", key))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$regex": "^" + key}})

	if err != nil {
		s.logger.Error("Failed to delete key", zap.String("key", key), zap.Error(err))
		return err
	}
	s.logger.Info("Successfully deleted key", zap.String("key", key))
	return nil
}

// Exists returns true if the key exists either as a directory (prefix to other keys) or a file, and there was no error checking.
func (s *MongoDBStorage) Exists(ctx context.Context, key string) bool {
	s.logger.Info("Checking existence of key", zap.String("key", key))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	count, err := collection.CountDocuments(ctx, bson.M{"_id": key})

	if err != nil {
		s.logger.Error("Failed to check existence of key", zap.String("key", key), zap.Error(err))
		return false
	}

	exists := count > 0
	s.logger.Info("Key existence check result", zap.String("key", key), zap.Bool("exists", exists))
	return exists
}

// List lists all keys in the given path.
func (s *MongoDBStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	s.logger.Info("Listing keys", zap.String("prefix", prefix), zap.Bool("recursive", recursive))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	filter := bson.M{"_id": bson.M{"$regex": "^" + prefix}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		s.logger.Error("Failed to list keys", zap.String("prefix", prefix), zap.Error(err))
		return nil, err
	}
	defer cursor.Close(ctx)

	var keys []string
	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Error("Failed to decode key", zap.Error(err))
			return nil, err
		}
		keys = append(keys, doc.ID)
	}
	s.logger.Info("Listed keys", zap.Int("count", len(keys)))
	return keys, nil
}

// Stat returns metadata about a key.
func (s *MongoDBStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	s.logger.Info("Statting key", zap.String("key", key))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	result := collection.FindOne(ctx, bson.M{"_id": key})

	var doc struct {
		Modified time.Time `bson:"modified"`
		Value    []byte    `bson:"value"`
	}
	err := result.Decode(&doc)
	if err != nil {
		s.logger.Warn("Key not found in Stat", zap.String("key", key))
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	s.logger.Info("Stat result for key", zap.String("key", key), zap.Time("modified", doc.Modified), zap.Int("size", len(doc.Value)))

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   doc.Modified,
		Size:       int64(len(doc.Value)),
		IsTerminal: true,
	}, nil
}

// Lock locks a key to prevent concurrent modifications.
func (s *MongoDBStorage) Lock(ctx context.Context, key string) error {
	lockKey := key + ".lock"
	s.logger.Info("Locking key", zap.String("key", lockKey))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.InsertOne(ctx, bson.M{
		"_id":   lockKey,
		"value": "locked",
	})

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			s.logger.Warn("Lock already exists", zap.String("key", lockKey))
			return ErrLockExists
		}
		s.logger.Error("Failed to acquire lock", zap.String("key", lockKey), zap.Error(err))
		return err
	}
	s.logger.Info("Successfully locked key", zap.String("key", lockKey))
	return nil
}

// Unlock unlocks a previously locked key.
func (s *MongoDBStorage) Unlock(ctx context.Context, key string) error {
	lockKey := key + ".lock"
	s.logger.Info("Unlocking key", zap.String("key", lockKey))

	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	collection := s.client.Database(s.Database).Collection(s.Collection)
	_, err := collection.DeleteOne(ctx, bson.M{"_id": lockKey})

	if err != nil {
		s.logger.Error("Failed to unlock key", zap.String("key", lockKey), zap.Error(err))
		return err
	}
	s.logger.Info("Successfully unlocked key", zap.String("key", lockKey))
	return nil
}

// Validate validates that the module has a usable config.
func (s MongoDBStorage) Validate() error {
	s.logger.Info("Validate")
	return nil
}

// Interface guards
var (
	_ caddy.Module          = (*MongoDBStorage)(nil)
	_ caddy.Provisioner     = (*MongoDBStorage)(nil)
	_ caddy.Validator       = (*MongoDBStorage)(nil)
	_ caddyfile.Unmarshaler = (*MongoDBStorage)(nil)
	_ certmagic.Storage     = (*MongoDBStorage)(nil)
)

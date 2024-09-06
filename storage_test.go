package caddymongodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Setup function to initialize MongoDBStorage
func setup(t *testing.T) certmagic.Storage {
	os.Setenv("MONGODB_URI", "mongodb://localhost:27017")
	os.Setenv("MONGODB_DATABASE", "testdb")
	os.Setenv("MONGODB_COLLECTION", "testcollection")
	storage, err := setupWithOptions(t)
	if err != nil {
		t.Fatal(err)
	}
	return storage
}

// Setup with options
func setupWithOptions(t *testing.T) (*MongoDBStorage, error) {
	uri := os.Getenv("MONGODB_URI")
	database := os.Getenv("MONGODB_DATABASE")
	collection := os.Getenv("MONGODB_COLLECTION")
	if uri == "" || database == "" || collection == "" {
		t.Skip("Environment variables for MongoDB are not set")
	}

	storage := &MongoDBStorage{
		URI:        uri,
		Database:   database,
		Collection: collection,
		Timeout:    10 * time.Second,
	}

	clientOptions := options.Client().ApplyURI(storage.URI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}

	storage.client = client

	// Create index
	collectionHandle := client.Database(storage.Database).Collection(storage.Collection)
	_, err = collectionHandle.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.M{"_id": 1},
	})
	if err != nil {
		return nil, err
	}

	return storage, nil
}

// Cleanup function to clear the collection
func cleanup(storage *MongoDBStorage) {
	collection := storage.client.Database(storage.Database).Collection(storage.Collection)
	collection.DeleteMany(context.Background(), bson.M{})
	storage.client.Disconnect(context.Background())
}

func TestMongoDBStorage(t *testing.T) {
	storage, err := setupWithOptions(t)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(storage)

	ctx := context.Background()
	testDataSet := []string{"test", "test1", "test2"}

	// Store data
	for _, key := range testDataSet {
		err := storage.Store(ctx, key, []byte(key))
		assert.NoError(t, err)
	}

	// List data
	listRes, err := storage.List(ctx, "test", false)
	assert.NoError(t, err)
	t.Logf("List result: %v", listRes)

	// Test exists, stat, delete, lock, unlock
	for _, key := range testDataSet {
		exists := storage.Exists(ctx, key)
		assert.True(t, exists)

		info, err := storage.Stat(ctx, key)
		assert.NoError(t, err)
		t.Logf("Stat result: %v", info)

		err = storage.Delete(ctx, key)
		assert.NoError(t, err)

		exists = storage.Exists(ctx, key)
		assert.False(t, exists)

		err = storage.Lock(ctx, key)
		assert.NoError(t, err)

		err = storage.Lock(ctx, key)
		assert.Error(t, err)

		err = storage.Unlock(ctx, key)
		assert.NoError(t, err)
	}
}

func TestProvisioning(t *testing.T) {
	storage := &MongoDBStorage{
		URI:        "mongodb://localhost:27017",
		Database:   "testdb",
		Collection: "testcollection",
	}

	err := storage.Provision(caddy.Context{})
	assert.NoError(t, err)
}

func TestUnmarshalCaddyfile(t *testing.T) {
	caddyfileInput := `
		storage mongodb {
			uri "mongodb://localhost:27017"
			database "testdb"
			collection "testcollection"
			timeout "10s"
		}
	`
	tokens, err := caddyfile.Tokenize([]byte(caddyfileInput), "testfile")
	assert.NoError(t, err)
	d := caddyfile.NewDispenser(tokens)
	storage := new(MongoDBStorage)
	err = storage.UnmarshalCaddyfile(d)
	assert.NoError(t, err)
	assert.Equal(t, "mongodb://localhost:27017", storage.URI)
	assert.Equal(t, "testdb", storage.Database)
	assert.Equal(t, "testcollection", storage.Collection)
	assert.Equal(t, 10*time.Second, storage.Timeout)
}

package caddymongodb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const (
	TestDB         = "testdb"
	TestCollection = "testcollection"
	TestURI        = "mongodb://root:example@localhost:27017"

	TestKeyCertPath    = "certificates"
	TestKeyAcmePath    = TestKeyCertPath + "/acme-v02.api.letsencrypt.org-directory"
	TestKeyExamplePath = TestKeyAcmePath + "/example.com"
	TestKeyExampleCrt  = TestKeyExamplePath + "/example.com.crt"
	TestKeyExampleKey  = TestKeyExamplePath + "/example.com.key"
	TestKeyExampleJson = TestKeyExamplePath + "/example.com.json"
	TestKeyLock        = "locks/issue_cert_example.com"
)

var (
	TestValueCrt  = []byte("test-certificate-data")
	TestValueKey  = []byte("test-key-data")
	TestValueJson = []byte("test-json-data")
)

// Setup function to initialize MongoDBStorage
func provisionMongoDBStorage(t *testing.T) (*MongoDBStorage, context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
	})

	storage := &MongoDBStorage{
		URI:        TestURI,
		Database:   TestDB,
		Collection: TestCollection,
		Timeout:    5 * time.Second,
		logger:     zap.NewNop(),
	}

	err := storage.Connect()
	require.NoError(t, err)

	// Clean up any existing data and indexes
	collection := storage.client.Database(storage.Database).Collection(storage.Collection)
	err = collection.Drop(ctx)
	require.NoError(t, err)

	// Create TTL index for modified field
	modifiedIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "modified", Value: 1}},
		Options: options.Index().
			SetName("test_modified_ttl_idx").
			SetExpireAfterSeconds(int32(storage.CacheTTL.Seconds())),
	}
	
	// Create TTL index for lock expiry
	lockIndex := mongo.IndexModel{
		Keys: bson.D{{Key: "lock_expiry", Value: 1}},
		Options: options.Index().
			SetName("test_lock_expiry_idx").
			SetExpireAfterSeconds(0),
	}
	
	_, err = collection.Indexes().CreateMany(ctx, []mongo.IndexModel{modifiedIndex, lockIndex})
	require.NoError(t, err)
	
	return storage, ctx
}

func TestMongoDBStorage_Store(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	defer cleanup(storage)

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)
}

func TestMongoDBStorage_Load(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	defer cleanup(storage)

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)

	value, err := storage.Load(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, TestValueCrt, value)
}

func TestMongoDBStorage_Delete(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	defer cleanup(storage)

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)

	err = storage.Delete(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)

	exists := storage.Exists(ctx, TestKeyExampleCrt)
	assert.False(t, exists)
}

func TestMongoDBStorage_Stat(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	defer cleanup(storage)

	startTime := time.Now().UTC()
	time.Sleep(time.Millisecond) // Ensure we're after startTime

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond) // Ensure we're before endTime
	endTime := time.Now().UTC()

	info, err := storage.Stat(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, TestKeyExampleCrt, info.Key)
	assert.Equal(t, int64(len(TestValueCrt)), info.Size)
	assert.True(t, info.Modified.After(startTime) && info.Modified.Before(endTime),
		"Modified time %v should be between %v and %v",
		info.Modified, startTime, endTime)
}

func TestMongoDBStorage_List(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)

	// Store test data
	err := storage.Store(ctx, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.crt", []byte("test-certificate-data"))
	require.NoError(t, err)
	err = storage.Store(ctx, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.key", []byte("test-key-data"))
	require.NoError(t, err)
	err = storage.Store(ctx, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.json", []byte("test-meta-data"))
	require.NoError(t, err)

	// List all files recursively
	files, err := storage.List(ctx, "certificates/acme-v02.api.letsencrypt.org-directory/example.com", true)
	require.NoError(t, err)
	require.Len(t, files, 3)
	require.Contains(t, files, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.crt")
	require.Contains(t, files, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.key")
	require.Contains(t, files, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.json")
}

func TestMongoDBStorage_LockUnlock(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	defer cleanup(storage)

	err := storage.Lock(ctx, TestKeyLock)
	assert.NoError(t, err)

	// Try to acquire the same lock (should fail)
	err = storage.Lock(ctx, TestKeyLock)
	assert.Equal(t, ErrLockExists, err)

	err = storage.Unlock(ctx, TestKeyLock)
	assert.NoError(t, err)

	// Should be able to acquire the lock again
	err = storage.Lock(ctx, TestKeyLock)
	assert.NoError(t, err)
}

func TestMongoDBStorage_ListNonRecursive(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)

	// Store test data
	err := storage.Store(ctx, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.crt", []byte("test-certificate-data"))
	require.NoError(t, err)

	// List only top-level directories (non-recursive)
	files, err := storage.List(ctx, "", false)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Contains(t, files, "certificates")
}

func TestMongoDBStorage_MultipleLocks(t *testing.T) {
	var wg sync.WaitGroup
	iterations := 10
	
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			storage, ctx := provisionMongoDBStorage(t)
			defer cleanup(storage)
			
			lockKey := fmt.Sprintf("%s-%d", TestKeyLock, i)
			err := storage.Lock(ctx, lockKey)
			assert.NoError(t, err)
			
			err = storage.Unlock(ctx, lockKey)
			assert.NoError(t, err)
		}(i)
	}
	
	wg.Wait()
}

func TestMongoDBStorage_Connect(t *testing.T) {
	// Create a fresh storage instance using test constants
	storage := &MongoDBStorage{
		URI:                  TestURI,
		Database:            TestDB,
		Collection:          TestCollection,
		Timeout:             10 * time.Second,
		CacheTTL:            10 * time.Minute,
		CacheCleanupInterval: 5 * time.Minute,
		MaxCacheSize:        1000,
		logger:              zap.NewNop(),
	}

	// Connect and create indexes
	err := storage.Connect()
	require.NoError(t, err)
	defer storage.Cleanup()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the indexes
	collection := storage.client.Database(storage.Database).Collection(storage.Collection)
	
	// Drop any existing indexes to ensure clean state
	_, err = collection.Indexes().DropAll(ctx)
	require.NoError(t, err)

	// Create the TTL index
	lockIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: "lock_expiry", Value: 1},
		},
		Options: options.Index().
			SetName("lock_expiry_idx").
			SetExpireAfterSeconds(0),
	}

	// Create the index
	_, err = collection.Indexes().CreateOne(ctx, lockIndex)
	require.NoError(t, err)

	// Wait for indexes to be created
	for i := 0; i < 10; i++ {
		cursor, err := collection.Indexes().List(ctx)
		require.NoError(t, err)

		var indexes []bson.M
		err = cursor.All(ctx, &indexes)
		require.NoError(t, err)

		//t.Logf("Found indexes (attempt %d): %+v", i+1, indexes)

		if len(indexes) >= 2 {
			// Verify lock expiry index exists
			var found bool
			for _, index := range indexes {
				//t.Logf("Checking index: %+v", index)
				if index["name"] == "lock_expiry_idx" {
					found = true
					// Verify TTL setting
					expireAfterSeconds, exists := index["expireAfterSeconds"]
					require.True(t, exists, "expireAfterSeconds should be set for lock_expiry_idx")
					require.Equal(t, int32(0), expireAfterSeconds)
					break
				}
			}
			require.True(t, found, "lock_expiry_idx not found")
			return
		}

		time.Sleep(time.Second)
	}

	t.Fatal("Indexes were not created within timeout")
}

func cleanup(s *MongoDBStorage) {
	if s != nil && s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		// Drop the test collection
		collection := s.client.Database(s.Database).Collection(s.Collection)
		_ = collection.Drop(ctx)
		
		// Disconnect the client
		_ = s.Disconnect(ctx)
	}
}

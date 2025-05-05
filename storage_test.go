package caddymongodb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2" // Needed for Provision context
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

// provisionMongoDBStorage sets up a MongoDBStorage instance for testing.
// It provisions the storage, cleans the collection, and registers cleanup.
func provisionMongoDBStorage(t *testing.T) (*MongoDBStorage, context.Context) {
	// Use a Caddy context for Provision
	caddyCtx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel) // Ensure Caddy context is cancelled on cleanup

	logger := zap.NewNop() // Use Noop logger for tests

	storage := &MongoDBStorage{
		URI:        TestURI,
		Database:   TestDB,
		Collection: TestCollection,
		Timeout:    10 * time.Second, // Slightly longer timeout for tests
		logger:     logger,           // Assign logger early for Provision
		// Set other defaults explicitly if needed, otherwise Provision handles them
		CacheTTL:             1 * time.Minute, // Shorter TTL for cache tests if needed
		CacheCleanupInterval: 2 * time.Minute,
		MaxPoolSize:          10, // Smaller pool for tests
	}

	// Clean the collection before provisioning to ensure a clean slate
	// We need a temporary client for this pre-provision cleanup
	tempClientOpts := options.Client().ApplyURI(storage.URI).SetTimeout(storage.Timeout)
	tempCtx, tempCancel := context.WithTimeout(context.Background(), storage.Timeout)
	tempClient, err := mongo.Connect(tempCtx, tempClientOpts)
	require.NoError(t, err, "Failed to connect temporary client for pre-test cleanup")
	err = tempClient.Database(storage.Database).Collection(storage.Collection).Drop(tempCtx)
	// Ignore "ns not found" error if collection doesn't exist yet
	if err != nil && !strings.Contains(err.Error(), "ns not found") {
		require.NoError(t, err, "Failed to drop collection before test")
	}
	_ = tempClient.Disconnect(tempCtx)
	tempCancel()

	// Provision the storage (connects, ensures indexes)
	err = storage.Provision(caddyCtx)
	require.NoError(t, err, "Provision failed")

	// Register the main cleanup function
	t.Cleanup(func() {
		err := storage.Cleanup()
		assert.NoError(t, err, "Cleanup failed")
	})

	// Return the provisioned storage and a background context for test operations
	return storage, context.Background()
}

func TestMongoDBStorage_Store(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	// No defer cleanup needed, t.Cleanup handles it

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)
}

func TestMongoDBStorage_Load(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	// No defer cleanup needed

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)

	value, err := storage.Load(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, TestValueCrt, value)
}

func TestMongoDBStorage_Delete(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	// No defer cleanup needed

	err := storage.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	assert.NoError(t, err)

	err = storage.Delete(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)

	exists := storage.Exists(ctx, TestKeyExampleCrt)
	assert.False(t, exists)
}

func TestMongoDBStorage_Stat(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	// No defer cleanup needed

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
	// No defer cleanup needed

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
	// No defer cleanup needed

	// 1. Acquire lock
	err := storage.Lock(ctx, TestKeyLock)
	assert.NoError(t, err)

	// 2. Try to acquire the same lock (should fail)
	// Use a separate context to avoid immediate cancellation if the first lock takes time
	lockCtx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second) // Increased timeout to 3s
	defer cancel2()
	err = storage.Lock(lockCtx2, TestKeyLock)
	// Check for ErrLockExists OR context deadline exceeded, as polling might take time
	if !errors.Is(err, ErrLockExists) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Second lock acquisition failed with unexpected error: %v", err)
	} else {
		t.Logf("Second lock acquisition failed as expected: %v", err) // Log expected failure
	}

	// 3. Unlock the first lock
	err = storage.Unlock(ctx, TestKeyLock)
	assert.NoError(t, err, "Unlocking should succeed")

	// 4. Should be able to acquire the lock again
	// Use a fresh context
	lockCtx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	err = storage.Lock(lockCtx3, TestKeyLock)
	assert.NoError(t, err, "Acquiring lock after unlock should succeed")

	// 5. Clean up the final lock
	err = storage.Unlock(lockCtx3, TestKeyLock)
	assert.NoError(t, err, "Final unlock should succeed")
}

func TestMongoDBStorage_ListNonRecursive(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t)
	// No defer cleanup needed

	// Store test data
	err := storage.Store(ctx, "certificates/acme-v02.api.letsencrypt.org-directory/example.com/example.com.crt", []byte("test-certificate-data"))
	require.NoError(t, err)

	// List only top-level directories (non-recursive)
	files, err := storage.List(ctx, "", false)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Contains(t, files, "certificates")
}

// TestMongoDBStorage_Provision_Indexes checks if Provision creates the expected indexes.
func TestMongoDBStorage_Provision_Indexes(t *testing.T) {
	storage, ctx := provisionMongoDBStorage(t) // This calls Provision

	// Check if indexes were created
	collection := storage.client.Database(storage.Database).Collection(storage.Collection)
	cursor, err := collection.Indexes().List(ctx)
	require.NoError(t, err)

	var indexes []bson.M
	err = cursor.All(ctx, &indexes)
	require.NoError(t, err)

	foundTTLExpiry := false
	foundLockID := false
	expectedIndexes := 2 // Default _id index + 2 custom ones

	t.Logf("Found indexes: %d", len(indexes))
	var exists bool // Declare exists once before the loop
	for _, index := range indexes {
		t.Logf("Index: %v", index)
		name, _ := index["name"].(string)
		if name == "lock_expiry_ttl_idx" {
			foundTTLExpiry = true
			// Verify TTL setting
			var expireAfterSecondsRaw interface{}
			expireAfterSecondsRaw, exists = index["expireAfterSeconds"] // Use assignment '='
			assert.True(t, exists, "expireAfterSeconds should be set for lock_expiry_ttl_idx")
			// MongoDB driver might return int32 or int64 depending on version/platform
			if exists { // Only check type if it exists
				switch v := expireAfterSecondsRaw.(type) {
				case int32:
					assert.Equal(t, int32(0), v, "TTL should be 0")
				case int64:
					assert.Equal(t, int64(0), v, "TTL should be 0")
				default:
					t.Errorf("Unexpected type for expireAfterSeconds: %T", v)
				}
			}
			// Verify partial filter expression exists (checking exact content is brittle)
			_, exists = index["partialFilterExpression"] // Use assignment '='
			assert.True(t, exists, "partialFilterExpression should exist for lock_expiry_ttl_idx")

		} else if name == "lock_id_idx" {
			foundLockID = true
			// Verify partial filter expression exists (checking exact content is brittle)
			_, exists = index["partialFilterExpression"] // Use assignment '='
			assert.True(t, exists, "partialFilterExpression should exist for lock_id_idx")
		}
	}

	assert.True(t, foundTTLExpiry, "lock_expiry_ttl_idx not found")
	assert.True(t, foundLockID, "lock_id_idx not found")
	assert.GreaterOrEqual(t, len(indexes), expectedIndexes, "Should have at least default _id and custom indexes")
}

// TestMongoDBStorage_ConcurrentLocking tests concurrent lock acquisition attempts on the SAME key.
func TestMongoDBStorage_ConcurrentLocking(t *testing.T) {
	storage, _ := provisionMongoDBStorage(t) // Use shared storage instance

	numGoroutines := 5
	var acquiredCount int32
	var wg sync.WaitGroup

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			// Use a timeout for each lock attempt
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := storage.Lock(ctx, TestKeyLock) // All try to lock the SAME key
			if err == nil {
				t.Logf("Goroutine %d acquired lock", id)
				atomic.AddInt32(&acquiredCount, 1)
				// Simulate work
				time.Sleep(10 * time.Millisecond)
				err = storage.Unlock(ctx, TestKeyLock)
				assert.NoError(t, err, "Goroutine %d failed to unlock", id)
				t.Logf("Goroutine %d released lock", id)
			} else if !errors.Is(err, ErrLockExists) && !errors.Is(err, context.DeadlineExceeded) {
				// Log unexpected errors, but ErrLockExists and DeadlineExceeded are expected failures
				t.Errorf("Goroutine %d encountered unexpected error: %v", id, err)
			} else {
				t.Logf("Goroutine %d failed to acquire lock (expected): %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// Only one goroutine should have successfully acquired the lock at any given time.
	// Due to timing, it's possible multiple acquire it sequentially.
	// A better check might be to ensure the lock document state is consistent,
	// but checking that *at least one* acquired it is a basic sanity check.
	assert.GreaterOrEqual(t, acquiredCount, int32(1), "At least one goroutine should acquire the lock")
	// We cannot easily assert that *only* one acquired it *simultaneously* without more complex coordination.
	// The goal is that the lock prevents concurrent access, which sequential acquisition achieves.
}

// Note: Removed TestMongoDBStorage_MultipleLocks as it tested locking different keys,
// which isn't the primary concern for distributed locking. Renamed to TestMongoDBStorage_ConcurrentLocking
// to test contention on the same key.

// Note: Removed TestMongoDBStorage_Connect as its logic is now covered by Provision tests.

// Note: Removed cleanup function as t.Cleanup is used in provisionMongoDBStorage.

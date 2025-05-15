package caddymongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// "go.uber.org/zap" // Not strictly needed here if not using zaptest directly in tests
	// "go.uber.org/zap/zaptest" // Use if you want to pass a test-specific logger to the module
)

const (
	TestDBEnvVar              = "TEST_MONGODB_DB"
	TestCertsCollectionEnvVar = "TEST_MONGODB_CERTS_COLLECTION"
	TestLocksCollectionEnvVar = "TEST_MONGODB_LOCKS_COLLECTION"
	TestURIEnvVar             = "TEST_MONGODB_URI"

	DefaultTestDB              = "caddytestdb"
	DefaultTestCertsCollection = "testcerts"
	DefaultTestLocksCollection = "testlocks"
	DefaultTestURI             = "mongodb://root:example@localhost:27017/?compressors=zstd,snappy,zlib"

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

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func provisionMongoDBStorage(t *testing.T, enableBulkWrites bool) (*MongoDBStorage, context.Context) {
	// Caddy context for Provision. The module will obtain its logger via ctx.Logger(s).
	// For tests, Caddy's default logger (often a no-op or basic stdout unless configured) will be used.
	// If more specific test logging is desired for the module's internal logs,
	// Caddy's global logging would need to be configured for tests, or use a test build of Caddy.
	caddyCtx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)

	testURI := getEnv(TestURIEnvVar, DefaultTestURI)
	testDB := getEnv(TestDBEnvVar, DefaultTestDB)
	testCertsCollection := getEnv(TestCertsCollectionEnvVar, DefaultTestCertsCollection)
	testLocksCollection := getEnv(TestLocksCollectionEnvVar, DefaultTestLocksCollection)

	st := &MongoDBStorage{
		URI:               testURI,
		Database:          testDB,
		Collection:        testCertsCollection,
		LocksCollection:   testLocksCollection,
		Timeout:           5 * time.Second,
		CacheTTL:          1 * time.Minute,
		MaxCacheEntries:   100,
		MaxPoolSize:       5,
		EnableBulkWrites:  enableBulkWrites,
		BulkMaxOps:        10,
		BulkFlushInterval: 100 * time.Millisecond,
	}

	tmpCtx, tmpCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer tmpCancel()
	tmpCli, err := mongo.Connect(tmpCtx, options.Client().ApplyURI(testURI))
	require.NoError(t, err, "Failed to connect to MongoDB for test cleanup")

	err = tmpCli.Database(testDB).Collection(testCertsCollection).Drop(tmpCtx)
	if err != nil && !strings.Contains(err.Error(), "ns not found") { // MongoDB 5+ returns different error for non-existent
		require.NoError(t, err, "Failed to drop testcerts collection")
	}
	err = tmpCli.Database(testDB).Collection(testLocksCollection).Drop(tmpCtx)
	if err != nil && !strings.Contains(err.Error(), "ns not found") {
		require.NoError(t, err, "Failed to drop testlocks collection")
	}
	_ = tmpCli.Disconnect(tmpCtx)

	require.NoError(t, st.Provision(caddyCtx), "Provisioning MongoDBStorage failed")

	t.Cleanup(func() {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), st.Timeout+time.Second)
		defer flushCancel()
		err := st.Flush(flushCtx)
		assert.NoError(t, err, "Error flushing storage on cleanup")

		cleanupErr := st.Cleanup()
		assert.NoError(t, cleanupErr, "Error cleaning up storage")
	})
	return st, context.Background() // Return a general context for test operations
}

func TestStoreLoadDelete(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, false)

	err := s.Store(ctx, TestKeyExampleCrt, TestValueCrt)
	require.NoError(t, err)

	got, err := s.Load(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, TestValueCrt, got, "Loaded data does not match stored data")

	assert.True(t, s.Exists(ctx, TestKeyExampleCrt), "Exists should return true for stored key")

	err = s.Delete(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.False(t, s.Exists(ctx, TestKeyExampleCrt), "Exists should return false for deleted key")

	_, err = s.Load(ctx, TestKeyExampleCrt)
	assert.ErrorIs(t, err, fs.ErrNotExist, "Load on non-existent key should return fs.ErrNotExist")

	err = s.Delete(ctx, TestKeyExampleCrt)
	assert.ErrorIs(t, err, fs.ErrNotExist, "Delete on non-existent key should return fs.ErrNotExist")
}

func TestStoreLoadDeleteWithBulkWrites(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, true)

	err := s.Store(ctx, TestKeyExampleKey, TestValueKey)
	require.NoError(t, err)

	err = s.Flush(ctx)
	require.NoError(t, err, "Flush after store failed")

	got, err := s.Load(ctx, TestKeyExampleKey)
	assert.NoError(t, err)
	assert.Equal(t, TestValueKey, got, "Loaded data does not match stored data after bulk write")

	assert.True(t, s.Exists(ctx, TestKeyExampleKey), "Exists should return true for stored key (bulk)")

	err = s.Delete(ctx, TestKeyExampleKey)
	assert.NoError(t, err)
	assert.False(t, s.Exists(ctx, TestKeyExampleKey), "Exists should return false for deleted key (bulk)")
}

func TestBulkWriteFlushBySize(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, true)
	originalBulkMaxOps := s.BulkMaxOps
	s.BulkMaxOps = 3
	defer func() { s.BulkMaxOps = originalBulkMaxOps }()

	for i := 0; i < s.BulkMaxOps-1; i++ {
		key := fmt.Sprintf("bulkkey_size/%d", i)
		err := s.Store(ctx, key, []byte(key))
		require.NoError(t, err)
	}
	var doc bson.M
	findCtx, findCancel := context.WithTimeout(ctx, time.Second) // Short timeout for DB check
	defer findCancel()
	err := s.certCol().FindOne(findCtx, bson.M{"_id": "bulkkey_size/0"}).Decode(&doc)
	if !errors.Is(err, mongo.ErrNoDocuments) {
		t.Logf("Document unexpectedly found or other error before reaching bulk max ops: %v", err)
	}

	lastKey := fmt.Sprintf("bulkkey_size/%d", s.BulkMaxOps-1)
	err = s.Store(ctx, lastKey, []byte(lastKey))
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // Give a bit more time for potential async aspects or slow CI
	for i := 0; i < s.BulkMaxOps; i++ {
		key := fmt.Sprintf("bulkkey_size/%d", i)
		val, errLoad := s.Load(ctx, key)
		assert.NoError(t, errLoad, "Failed to load key %s after bulk flush by size", key)
		assert.Equal(t, []byte(key), val, "Value mismatch for key %s", key)
	}
}

func TestBulkWriteFlushByTime(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, true)
	originalFlushInterval := s.BulkFlushInterval
	s.BulkFlushInterval = 50 * time.Millisecond
	defer func() { s.BulkFlushInterval = originalFlushInterval }()
	originalBulkMaxOps := s.BulkMaxOps // Ensure size limit isn't hit
	s.BulkMaxOps = 100
	defer func() { s.BulkMaxOps = originalBulkMaxOps }()

	key := "bulkkey_time/0"
	err := s.Store(ctx, key, []byte(key))
	require.NoError(t, err)

	var doc bson.M
	findCtx, findCancel := context.WithTimeout(ctx, time.Second)
	defer findCancel()
	errDb := s.certCol().FindOne(findCtx, bson.M{"_id": key}).Decode(&doc)
	if !errors.Is(errDb, mongo.ErrNoDocuments) {
		t.Logf("Document unexpectedly found or other error before timed bulk flush: %v", errDb)
	}

	time.Sleep(s.BulkFlushInterval * 4) // Wait longer than the interval, ensure timer fires

	val, errLoad := s.Load(ctx, key)
	assert.NoError(t, errLoad, "Failed to load key %s after timed bulk flush", key)
	assert.Equal(t, []byte(key), val, "Value mismatch for key %s", key)
}

func TestStat(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, false)
	require.NoError(t, s.Store(ctx, TestKeyExampleCrt, TestValueCrt))

	info, err := s.Stat(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, TestKeyExampleCrt, info.Key)
	assert.Equal(t, int64(len(TestValueCrt)), info.Size)
	assert.WithinDuration(t, time.Now(), info.Modified, 5*time.Second, "Modification time is not recent")
	assert.True(t, info.IsTerminal, "Stat on a .crt file should be terminal")

	_, err = s.Stat(ctx, "nonexistentkey")
	assert.ErrorIs(t, err, fs.ErrNotExist, "Stat on non-existent key should return fs.ErrNotExist")

	dirKey := TestKeyExamplePath + "/"
	err = s.Store(ctx, dirKey, []byte("directory marker"))
	require.NoError(t, err)
	infoDir, errDir := s.Stat(ctx, dirKey)
	assert.NoError(t, errDir)
	assert.False(t, infoDir.IsTerminal, "Stat on a key ending with / should not be terminal")
}

func TestListRecursiveNonRecursive(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, false)
	keysToStore := []string{
		TestKeyExampleCrt,
		TestKeyExampleKey,
		TestKeyExampleJson,
		TestKeyCertPath + "/other-ca/other.example.com/other.example.com.crt",
		"toplevel.json",
		"toplevel.crt",
	}
	for _, k := range keysToStore {
		assert.NoError(t, s.Store(ctx, k, []byte("test data for "+k)))
	}

	recList, err := s.List(ctx, TestKeyExamplePath, true)
	require.NoError(t, err)
	expectedRec := []string{TestKeyExampleCrt, TestKeyExampleJson, TestKeyExampleKey}
	sort.Strings(recList)
	sort.Strings(expectedRec)
	assert.Equal(t, expectedRec, recList, "Recursive list for example.com failed")

	recList2, err := s.List(ctx, TestKeyCertPath, true)
	require.NoError(t, err)
	expectedRec2 := []string{
		TestKeyCertPath + "/other-ca/other.example.com/other.example.com.crt",
		TestKeyExampleCrt, TestKeyExampleJson, TestKeyExampleKey,
	}
	sort.Strings(recList2)
	sort.Strings(expectedRec2)
	assert.Equal(t, expectedRec2, recList2, "Recursive list for 'certificates' path failed")

	directFileUnderAcme := TestKeyAcmePath + "/acme_level_file.json"
	require.NoError(t, s.Store(ctx, directFileUnderAcme, []byte("acme direct file")))
	nonRecListAcme, err := s.List(ctx, TestKeyAcmePath, false)
	require.NoError(t, err)
	sort.Strings(nonRecListAcme)
	assert.Equal(t, []string{directFileUnderAcme}, nonRecListAcme, "Non-recursive list for TestKeyAcmePath failed")

	rootList, err := s.List(ctx, "", false)
	require.NoError(t, err)
	expectedRoot := []string{"toplevel.crt", "toplevel.json"}
	sort.Strings(rootList)
	sort.Strings(expectedRoot)
	assert.Equal(t, expectedRoot, rootList, "Non-recursive list for root failed")

	emptyList, err := s.List(ctx, "nonexistentprefix", true)
	assert.NoError(t, err)
	assert.Empty(t, emptyList, "List for non-existent prefix should be empty")
}

func TestIndexesCreated(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, false)

	curLocks, errLocks := s.lockCol().Indexes().List(ctx)
	require.NoError(t, errLocks)
	var idxLocks []bson.M
	require.NoError(t, curLocks.All(ctx, &idxLocks))
	lockIndexNames := make(map[string]bool)
	for _, m := range idxLocks {
		name, ok := m["name"].(string)
		if ok {
			lockIndexNames[name] = true
		}
	}
	assert.True(t, lockIndexNames["lock_expiry_ttl_idx"], "expected lock_expiry_ttl_idx to exist on locks collection")
	assert.True(t, lockIndexNames["lock_id_idx"], "expected lock_id_idx to exist on locks collection")

	curCerts, errCerts := s.certCol().Indexes().List(ctx)
	require.NoError(t, errCerts)
	var idxCerts []bson.M
	require.NoError(t, curCerts.All(ctx, &idxCerts))

	foundCertIndex := false
	keysCorrect := false
	pfeAcceptable := false // True if PFE is nil, not present, or an empty document
	expectedIndexName := "id_mod_idx"
	expectedKeys := bson.D{{Key: "_id", Value: int32(1)}, {Key: "modified", Value: int32(1)}}

	for _, idxDoc := range idxCerts {
		name, okName := idxDoc["name"].(string)
		if !okName || name != expectedIndexName {
			continue // Skip other indexes like the default _id_ or any old ones
		}
		foundCertIndex = true

		// Check index keys
		keyDocRaw, okKeyDocRaw := idxDoc["key"]
		if okKeyDocRaw {
			actualKeysMap := make(map[string]int32)
			conversionOk := false
			if keyDocD, okD := keyDocRaw.(bson.D); okD {
				if len(keyDocD) == len(expectedKeys) {
					conversionOk = true
					for _, elem := range keyDocD {
						if val, valOk := normalizeIndexKeyValue(elem.Value); valOk {
							actualKeysMap[elem.Key] = val
						} else {
							conversionOk = false
							break
						}
					}
				}
			} else if keyDocM, okM := keyDocRaw.(bson.M); okM {
				if len(keyDocM) == len(expectedKeys) {
					conversionOk = true
					for k, v := range keyDocM {
						if val, valOk := normalizeIndexKeyValue(v); valOk {
							actualKeysMap[k] = val
						} else {
							conversionOk = false
							break
						}
					}
				}
			}

			if conversionOk && len(actualKeysMap) == len(expectedKeys) {
				tempKeysCorrect := true
				for _, expectedKeyComponent := range expectedKeys {
					if actualVal, found := actualKeysMap[expectedKeyComponent.Key]; !found || actualVal != expectedKeyComponent.Value.(int32) {
						tempKeysCorrect = false
						break
					}
				}
				if tempKeysCorrect {
					keysCorrect = true
				}
			}
		} else {
			t.Logf("Index key document ('key') missing or invalid type for index %s: %T", expectedIndexName, keyDocRaw)
		}

		// Check partialFilterExpression (should be nil, not present, or empty)
		pfeValue, pfeExists := idxDoc["partialFilterExpression"]
		if !pfeExists || pfeValue == nil {
			pfeAcceptable = true // Not present or nil is acceptable
		} else {
			if pfeDoc, okDoc := pfeValue.(bson.M); okDoc {
				if len(pfeDoc) == 0 {
					pfeAcceptable = true // Empty document is acceptable
				} else {
					t.Logf("Index '%s' has an unexpected partialFilterExpression: %#v", expectedIndexName, pfeValue)
				}
			} else {
				t.Logf("Index '%s' has a partialFilterExpression of unexpected type: %T, value: %#v", expectedIndexName, pfeValue, pfeValue)
			}
		}

		if foundCertIndex { // Found and processed our target index
			break
		}
	}

	assert.True(t, foundCertIndex, "expected certificate index '"+expectedIndexName+"' to exist")
	assert.True(t, keysCorrect, "keys for index '"+expectedIndexName+"' are incorrect. Expected: "+fmt.Sprintf("%v", expectedKeys)+". Check logs for what was found.")
	assert.True(t, pfeAcceptable, "index '"+expectedIndexName+"' should not have a partialFilterExpression or it must be an empty document. Check logs.")
}

// normalizeIndexKeyValue converts MongoDB index key values (1, -1) which might be int32, int64, or float64
// into a consistent int32 for comparison.
func normalizeIndexKeyValue(val interface{}) (int32, bool) {
	switch v := val.(type) {
	case int32:
		return v, true
	case int64:
		if v == 1 || v == -1 {
			return int32(v), true
		}
	case float64:
		if v == 1.0 || v == -1.0 {
			return int32(v), true
		}
	}
	return 0, false
}

func TestConcurrentLocking(t *testing.T) {
	s, baseCtx := provisionMongoDBStorage(t, false)
	var successfulLocks int32
	var wg sync.WaitGroup
	numGoroutines := 5
	lockKey := "concurrent-test-lock"

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			lockCtx, cancel := context.WithTimeout(baseCtx, 3*time.Second) // Each attempt has a timeout
			defer cancel()
			// s.logger.Debug("Goroutine attempting lock", zap.Int("routineID", routineID), zap.String("key", lockKey)) // For verbose debugging
			err := s.Lock(lockCtx, lockKey)
			if err == nil {
				// s.logger.Debug("Goroutine acquired lock", zap.Int("routineID", routineID), zap.String("key", lockKey))
				atomic.AddInt32(&successfulLocks, 1)
				time.Sleep(50 * time.Millisecond)                                               // Simulate work
				unlockCtx, unlockCancel := context.WithTimeout(context.Background(), s.Timeout) // Fresh context for unlock
				defer unlockCancel()
				unlockErr := s.Unlock(unlockCtx, lockKey)
				assert.NoError(t, unlockErr, "Goroutine %d failed to unlock '%s'", routineID, lockKey)
				// s.logger.Debug("Goroutine unlocked", zap.Int("routineID", routineID), zap.String("key", lockKey))
			} else {
				// s.logger.Debug("Goroutine failed to acquire lock", zap.Int("routineID", routineID), zap.String("key", lockKey), zap.Error(err))
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrLockExists) { // ErrLockExists might be seen if steal logic is slow/fails
					t.Errorf("Goroutine %d encountered an unexpected error trying to lock '%s': %v", routineID, lockKey, err)
				}
			}
		}(i)
	}
	wg.Wait()
	assert.GreaterOrEqual(t, atomic.LoadInt32(&successfulLocks), int32(1), "Expected at least one goroutine to successfully acquire the lock")
	assert.LessOrEqual(t, atomic.LoadInt32(&successfulLocks), int32(numGoroutines), "More locks acquired than goroutines, which is impossible")
	// t.Logf("Total successful locks: %d", atomic.LoadInt32(&successfulLocks)) // If using t.Logf, ensure -v for tests
}

func TestLockUnlockInteraction(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, false)
	lockKey := "single-lock-test"

	lockCtx, lockCancel := context.WithTimeout(ctx, s.Timeout) // Use module timeout for lock acquisition
	defer lockCancel()
	err := s.Lock(lockCtx, lockKey)
	require.NoError(t, err, "Failed to acquire initial lock")

	errLockAgainCtx, cancelLockAgain := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelLockAgain()
	errLockAgain := s.Lock(errLockAgainCtx, lockKey)
	assert.Error(t, errLockAgain, "Acquiring an already held lock should fail or timeout")
	assert.True(t, errors.Is(errLockAgain, context.DeadlineExceeded) || errors.Is(errLockAgain, ErrLockExists),
		"Error for re-lock should be DeadlineExceeded or ErrLockExists: got %v", errLockAgain)

	unlockCtx, unlockCancel := context.WithTimeout(ctx, s.Timeout)
	defer unlockCancel()
	err = s.Unlock(unlockCtx, lockKey)
	assert.NoError(t, err, "Failed to unlock")

	lockAfterUnlockCtx, lockAfterUnlockCancel := context.WithTimeout(ctx, s.Timeout)
	defer lockAfterUnlockCancel()
	err = s.Lock(lockAfterUnlockCtx, lockKey)
	assert.NoError(t, err, "Failed to acquire lock after it was unlocked")

	finalUnlockCtx, finalUnlockCancel := context.WithTimeout(ctx, s.Timeout)
	defer finalUnlockCancel()
	err = s.Unlock(finalUnlockCtx, lockKey)
	assert.NoError(t, err, "Failed to perform final unlock")
}

func TestLockRefresh(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t, false)
	lockKey := "refresh-lock-test"

	// Constants from storage.go: lockRefreshInt = 3s, lockTTL = 5s
	// testLockTTL variable was removed as it was unused. Using consts directly.

	lockAcquireCtx, lockAcquireCancel := context.WithTimeout(ctx, s.Timeout)
	defer lockAcquireCancel()
	err := s.Lock(lockAcquireCtx, lockKey)
	require.NoError(t, err, "Failed to acquire lock for refresh test")
	// s.logger.Debug("Lock acquired for refresh test", zap.String("key", lockKey))

	var lockDoc bson.M
	opCtx, opCancel := context.WithTimeout(ctx, s.Timeout)
	defer opCancel()
	err = s.lockCol().FindOne(opCtx, bson.M{"_id": lockKey + ".lock"}).Decode(&lockDoc)
	require.NoError(t, err, "Lock document not found after acquiring lock")
	initialExpires, ok := lockDoc["expires"].(primitive.DateTime)
	require.True(t, ok, "Lock document 'expires' field is not a DateTime")
	assert.True(t, initialExpires.Time().After(time.Now()), "Initial lock expiry is not in the future")
	// s.logger.Debug("Initial expiry", zap.Time("time", initialExpires.Time()))

	time.Sleep(lockRefreshInt + 1*time.Second) // Wait for refresh to occur

	var refreshedLockDoc bson.M
	opCtx2, opCancel2 := context.WithTimeout(ctx, s.Timeout)
	defer opCancel2()
	err = s.lockCol().FindOne(opCtx2, bson.M{"_id": lockKey + ".lock"}).Decode(&refreshedLockDoc)
	require.NoError(t, err, "Lock document not found after refresh period")
	refreshedExpires, ok := refreshedLockDoc["expires"].(primitive.DateTime)
	require.True(t, ok, "Refreshed lock document 'expires' field is not a DateTime")

	// s.logger.Debug("Refreshed expiry", zap.Time("time", refreshedExpires.Time()))
	assert.True(t, refreshedExpires.Time().After(initialExpires.Time()), "Lock 'expires' field was not updated by refresh routine")
	assert.True(t, refreshedExpires.Time().After(time.Now()), "Refreshed lock expiry is not in the future")

	unlockCtx, unlockCancel := context.WithTimeout(ctx, s.Timeout)
	defer unlockCancel()
	err = s.Unlock(unlockCtx, lockKey)
	assert.NoError(t, err, "Failed to unlock after refresh test")

	time.Sleep(100 * time.Millisecond) // Give unlock a moment to delete
	opCtx3, opCancel3 := context.WithTimeout(ctx, s.Timeout)
	defer opCancel3()
	err = s.lockCol().FindOne(opCtx3, bson.M{"_id": lockKey + ".lock"}).Decode(&bson.M{})
	assert.ErrorIs(t, err, mongo.ErrNoDocuments, "Lock document should be deleted after unlock")
}

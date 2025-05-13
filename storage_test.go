package caddymongodb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	TestDB              = "testdb"
	TestCertsCollection = "testcerts"
	TestLocksCollection = "testlocks"
	TestURI             = "mongodb://root:example@localhost:27017/?compressors=zstd,snappy"

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

// helper: provision a clean storage instance
func provisionMongoDBStorage(t *testing.T) (*MongoDBStorage, context.Context) {
	caddyCtx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)

	st := &MongoDBStorage{
		URI:              TestURI,
		Database:         TestDB,
		Collection:       TestCertsCollection,
		LocksCollection:  TestLocksCollection,
		Timeout:          10 * time.Second,
		CacheTTL:         1 * time.Minute,
		MaxCacheEntries:  500,
		MaxPoolSize:      10,
		EnableBulkWrites: true,
	}

	// wipe collections before each run
	tmpCli, _ := mongo.Connect(context.Background(), options.Client().ApplyURI(TestURI))
	_ = tmpCli.Database(TestDB).Collection(TestCertsCollection).Drop(context.Background())
	_ = tmpCli.Database(TestDB).Collection(TestLocksCollection).Drop(context.Background())
	_ = tmpCli.Disconnect(context.Background())

	require.NoError(t, st.Provision(caddyCtx))
	t.Cleanup(func() { _ = st.Cleanup() })
	return st, context.Background()
}

func TestStoreLoadDelete(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t)

	require.NoError(t, s.Store(ctx, TestKeyExampleCrt, TestValueCrt))
	require.NoError(t, s.Flush(ctx))

	got, err := s.Load(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, TestValueCrt, got)

	assert.NoError(t, s.Delete(ctx, TestKeyExampleCrt))
	assert.False(t, s.Exists(ctx, TestKeyExampleCrt))
}

func TestStat(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t)
	require.NoError(t, s.Store(ctx, TestKeyExampleCrt, TestValueCrt))
	require.NoError(t, s.Flush(ctx))

	info, err := s.Stat(ctx, TestKeyExampleCrt)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(TestValueCrt)), info.Size)
}

func TestListRecursiveNonRecursive(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t)
	for _, k := range []string{TestKeyExampleCrt, TestKeyExampleKey, TestKeyExampleJson} {
		assert.NoError(t, s.Store(ctx, k, []byte("x")))
	}
	require.NoError(t, s.Flush(ctx))

	rec, err := s.List(ctx, TestKeyExamplePath, true)
	require.NoError(t, err)
	assert.Len(t, rec, 3)

	root, err := s.List(ctx, "", false)
	require.NoError(t, err)
	assert.Contains(t, root, "certificates")
}

func TestIndexesCreated(t *testing.T) {
	s, ctx := provisionMongoDBStorage(t)
	cur, err := s.lockCol().Indexes().List(ctx)
	require.NoError(t, err)
	var idx []bson.M
	require.NoError(t, cur.All(ctx, &idx))

	var names []string
	for _, m := range idx {
		names = append(names, m["name"].(string))
	}
	assert.Contains(t, names, "lock_expiry_ttl_idx")
	assert.Contains(t, names, "lock_id_idx")
}

func TestConcurrentLocking(t *testing.T) {
	s, _ := provisionMongoDBStorage(t)

	var got int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := s.Lock(ctx, TestKeyLock)
			if err == nil {
				atomic.AddInt32(&got, 1)
				time.Sleep(10 * time.Millisecond)
				_ = s.Unlock(ctx, TestKeyLock)
			} else if !errors.Is(err, context.DeadlineExceeded) &&
				!errors.Is(err, ErrLockExists) {
				t.Errorf("unexpected lock error: %v", err)
			}
		}()
	}
	wg.Wait()
	assert.GreaterOrEqual(t, got, int32(1))
}

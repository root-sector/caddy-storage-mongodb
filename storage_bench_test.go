package caddymongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/mongo"
)

// setupBenchmarkEnv sets up a MongoDB container and a MongoDBStorage instance for benchmarking.
// It returns the storage instance, a direct mongo client (for test setup/teardown if needed),
// the unique database name, and a cleanup function.
func setupBenchmarkEnv(b *testing.B, enableBulkWrites bool) (*MongoDBStorage, *mongo.Client, string, func()) {
	b.Helper() // Marks this as a helper function for benchmarking

	ctx := context.Background() // A general context for setup

	// Use GenericContainer for more control and current best practices
	req := testcontainers.ContainerRequest{
		Image:        "mongo:latest", // Using a specific version
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForListeningPort("27017/tcp").WithStartupTimeout(120 * time.Second),
	}
	mongoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		b.Fatalf("Failed to run MongoDB container: %v", err)
	}

	host, err := mongoContainer.Host(ctx)
	if err != nil {
		_ = mongoContainer.Terminate(ctx)
		b.Fatalf("Failed to get container host: %v", err)
	}
	port, err := mongoContainer.MappedPort(ctx, "27017")
	if err != nil {
		_ = mongoContainer.Terminate(ctx)
		b.Fatalf("Failed to get container port: %v", err)
	}
	uri := fmt.Sprintf("mongodb://%s:%s", host, port.Port())

	// Generate a unique database name for this benchmark run to ensure isolation
	dbName := "benchdb_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	s := &MongoDBStorage{
		URI:               uri,
		Database:          dbName,
		Collection:        "benchcerts",
		LocksCollection:   "benchlocks",
		Timeout:           10 * time.Second,
		CacheTTL:          1 * time.Minute,
		MaxCacheEntries:   10000,
		EnableBulkWrites:  enableBulkWrites,
		BulkMaxOps:        1000,                   // Adjusted for benchmarks
		BulkFlushInterval: 200 * time.Millisecond, // Adjusted for benchmarks
	}

	caddyProvisionCtx, cancelProvisionCtx := caddy.NewContext(caddy.Context{Context: ctx})

	if err := s.Provision(caddyProvisionCtx); err != nil {
		cancelProvisionCtx()
		_ = mongoContainer.Terminate(ctx)
		b.Fatalf("Failed to provision MongoDBStorage: %v", err)
	}

	cleanupFunc := func() {
		if err := s.Cleanup(); err != nil {
			b.Logf("Error during MongoDBStorage cleanup: %v", err)
		}
		cancelProvisionCtx()
		// Optionally drop the database using s.client if direct access is needed post-test
		// Note: s.client is unexported but accessible within the same package.
		// if s.client != nil {
		//    if dropErr := s.client.Database(dbName).Drop(context.Background()); dropErr != nil {
		//        b.Logf("Failed to drop database %s: %v", dbName, dropErr)
		//    }
		// }
		if err := mongoContainer.Terminate(ctx); err != nil {
			b.Logf("Failed to terminate MongoDB container: %v", err)
		}
	}
	return s, s.client, dbName, cleanupFunc
}

func BenchmarkStore(b *testing.B) {
	ctx := context.Background()

	runStoreBenchmark := func(b *testing.B, enableBulkWrites bool) {
		s, _, _, cleanup := setupBenchmarkEnv(b, enableBulkWrites)
		defer cleanup()

		keys := make([]string, b.N)
		for i := 0; i < b.N; i++ {
			keys[i] = fmt.Sprintf("benchstorekey_%s_%d", uuid.NewString(), i)
		}
		value := []byte("benchmark_store_value_payload_" + uuid.NewString())

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := s.Store(ctx, keys[i], value)
			if err != nil {
				b.Fatalf("Store failed: %v", err)
			}
		}
		b.StopTimer() // Stop timer before explicit flush if it's not part of single op cost

		if enableBulkWrites {
			if err := s.Flush(ctx); err != nil { // Ensure all buffered writes are committed
				b.Fatalf("Flush failed: %v", err)
			}
		}
	}

	b.Run("WithBulkWrites", func(b *testing.B) {
		runStoreBenchmark(b, true)
	})

	b.Run("WithoutBulkWrites", func(b *testing.B) {
		runStoreBenchmark(b, false)
	})
}

func BenchmarkLoad(b *testing.B) {
	ctx := context.Background()
	const numItemsToPreload = 1000

	runLoadBenchmark := func(b *testing.B, storageConfigEnableBulkWrites bool, testCacheHit bool) {
		s, _, _, cleanup := setupBenchmarkEnv(b, storageConfigEnableBulkWrites)
		defer cleanup()

		keys := make([]string, numItemsToPreload)
		value := []byte("benchmark_load_value_" + uuid.NewString())

		b.Logf("Pre-populating %d items for Load benchmark (direct to DB)...", numItemsToPreload)
		for i := 0; i < numItemsToPreload; i++ {
			keys[i] = fmt.Sprintf("benchloadkey_%s_%d", uuid.NewString(), i)
			if err := s.directStore(ctx, keys[i], value); err != nil {
				b.Fatalf("Failed to directStore pre-load data: %v", err)
			}
		}
		b.Logf("Pre-population complete.")

		if testCacheHit {
			b.Logf("Warming cache for cache-hit scenario...")
			for i := 0; i < numItemsToPreload; i++ {
				if _, err := s.Load(ctx, keys[i]); err != nil {
					b.Fatalf("Failed to warm cache with key %s: %v", keys[i], err)
				}
			}
			b.Logf("Cache warmed.")
		} else {
			if s.cache != nil {
				s.cache.Clear()
				s.cache.Wait() // Wait for Ristretto to clear
				b.Logf("Cache cleared for cache-miss scenario.")
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := s.Load(ctx, keys[i%numItemsToPreload])
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					b.Fatalf("Load failed with fs.ErrNotExist for key %s. Preloading issue?", keys[i%numItemsToPreload])
				}
				b.Fatalf("Load failed for key %s: %v", keys[i%numItemsToPreload], err)
			}
		}
		b.StopTimer()
	}

	// Using false for EnableBulkWrites in storage setup for Load benchmarks as it's less directly relevant to Load.
	b.Run("CacheHit", func(b *testing.B) {
		runLoadBenchmark(b, false, true)
	})

	b.Run("CacheMiss", func(b *testing.B) {
		runLoadBenchmark(b, false, false)
	})
}

func BenchmarkList(b *testing.B) {
	ctx := context.Background()
	const numItemsToPreload = 5000 // Number of items to preload for List
	const listPrefix = "benchlist/autocert/"

	runListBenchmark := func(b *testing.B, recursive bool) {
		s, _, _, cleanup := setupBenchmarkEnv(b, false) // Setup with bulk writes disabled for simpler preload
		defer cleanup()

		b.Logf("Pre-populating %d items for List benchmark with prefix '%s'...", numItemsToPreload, listPrefix)
		value := []byte("benchmark_list_value")
		for i := 0; i < numItemsToPreload; i++ {
			key := ""
			if i%3 == 0 { // Some items directly under prefix
				key = fmt.Sprintf("%sitem_%d.json", listPrefix, i)
			} else if i%3 == 1 { // Some items in a common subdirectory
				key = fmt.Sprintf("%ssites/item_%d.crt", listPrefix, i)
			} else { // Some items in unique subdirectories
				key = fmt.Sprintf("%ssites/domain%d.com/item_%d.key", listPrefix, i, i)
			}
			if err := s.directStore(ctx, key, value); err != nil {
				b.Fatalf("Failed to directStore pre-load data for List: %v", err)
			}
		}
		b.Logf("Pre-population complete.")

		if s.cache != nil { // List doesn't directly use cache, but clear for consistency
			s.cache.Clear()
			s.cache.Wait()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := s.List(ctx, listPrefix, recursive)
			if err != nil {
				b.Fatalf("List failed: %v", err)
			}
		}
		b.StopTimer()
	}

	b.Run("Recursive", func(b *testing.B) {
		runListBenchmark(b, true)
	})

	b.Run("NonRecursive", func(b *testing.B) {
		runListBenchmark(b, false) // Listing "benchlist/autocert/" non-recursively
	})

	b.Run("NonRecursiveSubfolder", func(b *testing.B) { // More specific non-recursive
		s, _, _, cleanup := setupBenchmarkEnv(b, false)
		defer cleanup()
		// minimal pre-population for this specific list call
		prefixToTest := listPrefix + "sites/"
		_ = s.directStore(ctx, prefixToTest+"example.com.crt", []byte("data"))
		_ = s.directStore(ctx, prefixToTest+"example.com.key", []byte("data"))
		_ = s.directStore(ctx, listPrefix+"another.json", []byte("data"))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// This will list "example.com.crt" and "example.com.key" effectively,
			// or "domainX.com" if that's how List interprets non-recursive path segments.
			// Based on current List logic, this should list directory-like entries under "sites/"
			_, err := s.List(ctx, prefixToTest, false)
			if err != nil {
				b.Fatalf("List (NonRecursiveSubfolder) failed: %v", err)
			}
		}
		b.StopTimer()

	})
}

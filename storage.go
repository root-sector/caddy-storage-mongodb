package caddymongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs" // Import for fs.ErrNotExist
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
	"github.com/dgraph-io/ristretto/v2"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/singleflight"
)

/* ---------- Public errors ---------- */

var (
	ErrNotExist    = fs.ErrNotExist
	ErrLockExists  = errors.New("lock already exists")
	ErrLockNotHeld = errors.New("lock not held by this instance")
)

/* ---------- Tunables ---------- */

const (
	lockTTL          = 5 * time.Second
	lockPollInterval = 1 * time.Second
	lockRefreshInt   = 3 * time.Second
	defaultBulkLimit = 100
	defaultBulkWin   = 500 * time.Millisecond
)

/* ---------- Storage object ---------- */

type MongoDBStorage struct {
	URI             string        `json:"uri,omitempty"`
	Database        string        `json:"database,omitempty"`
	Collection      string        `json:"collection,omitempty"`
	LocksCollection string        `json:"locks_collection,omitempty"`
	Timeout         time.Duration `json:"timeout,omitempty"`

	CacheTTL        time.Duration `json:"cache_ttl,omitempty"`
	MaxCacheEntries int           `json:"max_cache_entries,omitempty"`
	MaxPoolSize     uint64        `json:"max_pool_size,omitempty"`
	MinPoolSize     uint64        `json:"min_pool_size,omitempty"`
	MaxConnIdleTime time.Duration `json:"max_conn_idle_time,omitempty"`

	EnableBulkWrites  bool          `json:"enable_bulk_writes,omitempty"`
	BulkMaxOps        int           `json:"bulk_max_ops,omitempty"`
	BulkFlushInterval time.Duration `json:"bulk_flush_interval,omitempty"`

	client       *mongo.Client
	logger       *zap.Logger
	cache        *ristretto.Cache[string, []byte]
	requestGroup singleflight.Group
	locks        sync.Map

	bulkMu     sync.Mutex
	bulkQueue  []mongo.WriteModel
	bulkTimer  *time.Timer
	bulkCtx    context.Context
	bulkCancel context.CancelFunc
}

/* ---------- Module boiler-plate ---------- */

func init() { caddy.RegisterModule(&MongoDBStorage{}) }

func (s *MongoDBStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.mongodb",
		New: func() caddy.Module { return new(MongoDBStorage) },
	}
}

func (s *MongoDBStorage) CertMagicStorage() (certmagic.Storage, error) { return s, nil }

/* ---------- Provision & validation ---------- */

func (s *MongoDBStorage) Provision(ctx caddy.Context) error {
	// Benchmark-specific log level override
	benchmarkLogLevelOverride := os.Getenv("CADDY_MONGODB_BENCHMARK_LOG_LEVEL")
	if benchmarkLogLevelOverride != "" {
		atomicLevel := zap.NewAtomicLevel()
		err := atomicLevel.UnmarshalText([]byte(strings.ToLower(benchmarkLogLevelOverride)))
		if err == nil {
			encoderCfg := zap.NewDevelopmentEncoderConfig()
			core := zapcore.NewCore(
				zapcore.NewConsoleEncoder(encoderCfg),
				zapcore.Lock(os.Stderr),
				atomicLevel,
			)
			moduleID := "caddy.storage.mongodb"
			if modInfo := s.CaddyModule(); modInfo.ID != "" {
				moduleID = modInfo.ID.Name()
			}
			s.logger = zap.New(core).Named(moduleID)
			if atomicLevel.Enabled(zapcore.WarnLevel) { // Log only if the new level allows WARN
				s.logger.Warn("Benchmark log level override active", zap.String("level", benchmarkLogLevelOverride))
			}
		} else {
			s.logger = ctx.Logger(s) // Get default logger first to be able to log the error
			s.logger.Error("Invalid CADDY_MONGODB_BENCHMARK_LOG_LEVEL, using default logger.", zap.String("value", benchmarkLogLevelOverride), zap.Error(err))
		}
	} else {
		s.logger = ctx.Logger(s)
	}

	// -------- Fill defaults / env fallbacks --------
	if s.URI == "" {
		s.URI = os.Getenv("MONGODB_URI")
	}
	if s.Database == "" {
		s.Database = os.Getenv("MONGODB_DATABASE")
	}
	if s.Collection == "" {
		s.Collection = os.Getenv("MONGODB_COLLECTION")
	}
	if s.LocksCollection == "" {
		s.LocksCollection = os.Getenv("MONGODB_LOCKS_COLLECTION")
		if s.LocksCollection == "" {
			s.LocksCollection = "locks"
		}
	}
	if s.Timeout == 0 {
		timeoutStr := os.Getenv("MONGODB_TIMEOUT")
		if timeoutStr != "" {
			if dur, err := time.ParseDuration(timeoutStr); err == nil {
				s.Timeout = dur
			} else {
				s.logger.Warn("Invalid MONGODB_TIMEOUT duration, using default 10s", zap.String("value", timeoutStr), zap.Error(err))
				s.Timeout = 10 * time.Second
			}
		} else {
			s.Timeout = 10 * time.Second
		}
	}
	if s.CacheTTL == 0 {
		s.CacheTTL = 10 * time.Minute
	}
	if s.MaxCacheEntries == 0 {
		s.MaxCacheEntries = 1_000
	}
	// Pool size and idle time often default to driver's behavior if 0, which is fine.
	// Env vars are checked here for completeness if direct config is not provided.
	if s.MaxPoolSize == 0 && os.Getenv("MONGODB_MAX_POOL_SIZE") != "" {
		if val, err := strconv.ParseUint(os.Getenv("MONGODB_MAX_POOL_SIZE"), 10, 64); err == nil {
			s.MaxPoolSize = val
		} else {
			s.logger.Warn("Invalid MONGODB_MAX_POOL_SIZE", zap.Error(err))
		}
	}
	if s.MinPoolSize == 0 && os.Getenv("MONGODB_MIN_POOL_SIZE") != "" {
		if val, err := strconv.ParseUint(os.Getenv("MONGODB_MIN_POOL_SIZE"), 10, 64); err == nil {
			s.MinPoolSize = val
		} else {
			s.logger.Warn("Invalid MONGODB_MIN_POOL_SIZE", zap.Error(err))
		}
	}
	if s.MaxConnIdleTime == 0 && os.Getenv("MONGODB_MAX_CONN_IDLE_TIME") != "" {
		if dur, err := time.ParseDuration(os.Getenv("MONGODB_MAX_CONN_IDLE_TIME")); err == nil {
			s.MaxConnIdleTime = dur
		} else {
			s.logger.Warn("Invalid MONGODB_MAX_CONN_IDLE_TIME", zap.Error(err))
		}
	}
	if s.BulkMaxOps == 0 {
		s.BulkMaxOps = defaultBulkLimit
	}
	if s.BulkFlushInterval == 0 {
		s.BulkFlushInterval = defaultBulkWin
	}

	// -------- Validate mandatory fields --------
	if err := s.Validate(); err != nil {
		return err // Validate() should return a well-formed error
	}

	// -------- Connect to MongoDB --------
	if err := s.connect(ctx); err != nil { // Pass the original caddy.Context
		return fmt.Errorf("failed to connect or setup MongoDB: %w", err)
	}

	// -------- Build Ristretto cache --------
	cacheCfg := &ristretto.Config[string, []byte]{
		NumCounters: int64(s.MaxCacheEntries) * 10, // Recommended 10x MaxCost
		MaxCost:     int64(s.MaxCacheEntries),      // Max number of items (cost 1 each)
		BufferItems: 64,                            // Default, recommended by Ristretto
		Metrics:     false,                         // Disable metrics for performance unless needed
	}
	cache, err := ristretto.NewCache(cacheCfg)
	if err != nil {
		return fmt.Errorf("failed to initialize Ristretto cache: %w", err)
	}
	s.cache = cache

	// -------- Bulk writer --------
	if s.EnableBulkWrites {
		s.bulkCtx, s.bulkCancel = context.WithCancel(context.Background())
		s.bulkQueue = make([]mongo.WriteModel, 0, s.BulkMaxOps) // Pre-allocate based on max ops
		s.bulkTimer = time.NewTimer(s.BulkFlushInterval)
		go s.runBulkWriter()
		s.logger.Info("Bulk writer enabled", zap.Int("max_ops", s.BulkMaxOps), zap.Duration("flush_interval", s.BulkFlushInterval))
	}

	s.logger.Info("MongoDB storage provisioned successfully",
		zap.String("uri_host", firstHost(s.URI)), // Log only host for security
		zap.String("database", s.Database),
		zap.String("collection", s.Collection),
		zap.String("locks_collection", s.LocksCollection),
	)
	return nil
}

// firstHost extracts the first host from a MongoDB URI for safe logging
func firstHost(uri string) string {
	// mongodb://user:pass@host1,host2/db?options -> host1
	// mongodb+srv://user:pass@cluster.mongodb.net/db?options -> cluster.mongodb.net
	noScheme := uri
	if idx := strings.Index(noScheme, "://"); idx != -1 {
		noScheme = noScheme[idx+3:]
	}
	if idx := strings.Index(noScheme, "@"); idx != -1 {
		noScheme = noScheme[idx+1:]
	}
	if idx := strings.Index(noScheme, "/"); idx != -1 {
		noScheme = noScheme[:idx]
	}
	// Take the first host if it's a comma-separated list (for replica sets not using SRV)
	return strings.SplitN(noScheme, ",", 2)[0]
}

func (s *MongoDBStorage) Validate() error {
	switch {
	case s.URI == "":
		return errors.New("mongodb URI is required")
	case s.Database == "":
		return errors.New("mongodb database name is required")
	case s.Collection == "":
		return errors.New("mongodb collection name is required")
	case s.Timeout <= 0:
		return fmt.Errorf("timeout must be positive: %v", s.Timeout)
	case s.CacheTTL < 0: // 0 is fine (disables TTL essentially, or uses Ristretto default if it has one for 0)
		return fmt.Errorf("cache_ttl cannot be negative: %v", s.CacheTTL)
	case s.MaxCacheEntries < 0: // 0 might mean no limit or default, typically positive is expected
		return fmt.Errorf("max_cache_entries cannot be negative: %v", s.MaxCacheEntries)
	case s.EnableBulkWrites && s.BulkMaxOps <= 0:
		return fmt.Errorf("bulk_max_ops must be positive if bulk writes enabled: %d", s.BulkMaxOps)
	case s.EnableBulkWrites && s.BulkFlushInterval <= 0:
		return fmt.Errorf("bulk_flush_interval must be positive if bulk writes enabled: %v", s.BulkFlushInterval)
	// MaxPoolSize, MinPoolSize, MaxConnIdleTime can be 0 to use driver defaults.
	default:
		return nil
	}
}

/* ---------- UnmarshalCaddyfile ---------- */

func (s *MongoDBStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if d.NextArg() {
			s.URI = d.Val()
		}
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			key := d.Val()
			var val string
			if !d.AllArgs(&val) {
				if !d.Args(&val) {
					return d.ArgErr()
				}
			}
			switch key {
			case "uri":
				s.URI = val
			case "database":
				s.Database = val
			case "collection":
				s.Collection = val
			case "locks_collection":
				s.LocksCollection = val
			case "timeout":
				dur, err := time.ParseDuration(val)
				if err != nil {
					return d.Errf("invalid timeout duration '%s': %v", val, err)
				}
				s.Timeout = dur
			case "cache_ttl":
				dur, err := time.ParseDuration(val)
				if err != nil {
					return d.Errf("invalid cache_ttl duration '%s': %v", val, err)
				}
				s.CacheTTL = dur
			case "max_cache_entries":
				n, err := strconv.Atoi(val)
				if err != nil || n < 0 {
					return d.Errf("invalid max_cache_entries '%s': %v", val, err)
				}
				s.MaxCacheEntries = n
			case "max_pool_size":
				n, err := strconv.ParseUint(val, 10, 64)
				if err != nil {
					return d.Errf("invalid max_pool_size '%s': %v", val, err)
				}
				s.MaxPoolSize = n
			case "min_pool_size":
				n, err := strconv.ParseUint(val, 10, 64)
				if err != nil {
					return d.Errf("invalid min_pool_size '%s': %v", val, err)
				}
				s.MinPoolSize = n
			case "max_conn_idle_time":
				dur, err := time.ParseDuration(val)
				if err != nil {
					return d.Errf("invalid max_conn_idle_time duration '%s': %v", val, err)
				}
				s.MaxConnIdleTime = dur
			case "enable_bulk_writes":
				switch strings.ToLower(val) {
				case "true", "yes", "on", "1":
					s.EnableBulkWrites = true
				case "false", "no", "off", "0":
					s.EnableBulkWrites = false
				default:
					return d.Errf("invalid boolean for enable_bulk_writes: '%s'", val)
				}
			case "bulk_max_ops":
				n, err := strconv.Atoi(val)
				if err != nil || n <= 0 {
					return d.Errf("invalid bulk_max_ops '%s': must be a positive integer", val)
				}
				s.BulkMaxOps = n
			case "bulk_flush_interval":
				dur, err := time.ParseDuration(val)
				if err != nil || dur <= 0 {
					return d.Errf("invalid bulk_flush_interval duration '%s': must be positive", val)
				}
				s.BulkFlushInterval = dur
			default:
				return d.Errf("unrecognized mongodb storage option: %s", key)
			}
		}
	}
	return nil
}

/* ---------- MongoDB connection & indexes ---------- */

func (s *MongoDBStorage) connect(caddyCtx caddy.Context) error {
	// Use the standard context.Context from the Caddy context for driver operations
	driverOperCtx, driverOperCancel := context.WithTimeout(caddyCtx.Context, s.Timeout)
	defer driverOperCancel()

	opts := options.Client().
		ApplyURI(s.URI).
		// SetServerSelectionTimeout is often implicitly handled by ApplyURI based on connectTimeoutMS, etc.
		// Explicitly setting it ensures behavior if URI doesn't specify timeouts.
		SetServerSelectionTimeout(s.Timeout).
		SetConnectTimeout(s.Timeout). // Connect timeout
		SetMaxPoolSize(s.MaxPoolSize).
		SetMinPoolSize(s.MinPoolSize).
		SetMaxConnIdleTime(s.MaxConnIdleTime).
		SetCompressors([]string{"zstd", "snappy"})

	client, err := mongo.Connect(driverOperCtx, opts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping with a new context to ensure it's not cancelled by driverOperCancel if connect was very fast.
	pingCtx, pingCancel := context.WithTimeout(caddyCtx.Context, s.Timeout)
	defer pingCancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		// Attempt to disconnect if ping fails after a successful connect call
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second) // Short timeout for cleanup disconnect
		defer disconnectCancel()
		_ = client.Disconnect(disconnectCtx)
		return fmt.Errorf("failed to ping MongoDB primary: %w", err)
	}
	s.client = client
	s.logger.Info("Successfully connected to MongoDB and pinged primary.")

	// Context for index operations, give them more time than a single operation timeout.
	indexOpCtx, indexOpCancel := context.WithTimeout(caddyCtx.Context, s.Timeout*3) // Increased timeout for index ops
	defer indexOpCancel()

	if err := s.ensureLockIndexes(indexOpCtx); err != nil {
		s.logger.Error("Failed to ensure lock indexes, proceeding. Performance or locking reliability may be impacted.", zap.Error(err))
	}
	if err := s.ensureCertIndexes(indexOpCtx); err != nil {
		s.logger.Error("Failed to ensure certificate indexes, proceeding. Query performance may be impacted.", zap.Error(err))
	}

	return nil
}

func (s *MongoDBStorage) ensureCertIndexes(ctx context.Context) error {
	col := s.certCol()
	idxName := "id_mod_idx" // Index for _id and modified
	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
			{Key: "modified", Value: 1},
		},
		Options: options.Index().SetName(idxName),
	}
	_, err := col.Indexes().CreateOne(ctx, index)
	// Gracefully handle "already exists" errors or specific codes
	if err != nil {
		if e, ok := err.(mongo.CommandError); ok && (e.Code == 85 || e.Code == 86 || strings.Contains(e.Error(), "already exists with different options") || strings.Contains(e.Error(), "Index with name "+idxName+" already exists with different options")) {
			s.logger.Warn("certificate index '"+idxName+"' already exists with different options/spec. Manual review might be needed.", zap.String("index_name", idxName), zap.Error(e))
			return nil // Not a fatal error for provisioning
		}
		if strings.Contains(err.Error(), "already exists") {
			s.logger.Info("certificate index '"+idxName+"' already exists.", zap.String("index_name", idxName))
			return nil // Not a fatal error
		}
		return fmt.Errorf("creating certificate index '%s': %w", idxName, err)
	}
	s.logger.Info("Certificate index ensured", zap.String("index_name", idxName))
	return nil
}

func (s *MongoDBStorage) ensureLockIndexes(ctx context.Context) error {
	col := s.lockCol()
	idx := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "expires", Value: 1}},
			Options: options.Index().SetName("lock_expiry_ttl_idx").SetExpireAfterSeconds(0),
		},
		{
			Keys:    bson.D{{Key: "lock_id", Value: 1}},
			Options: options.Index().SetName("lock_id_idx"), // Removed .SetUnique(false) as it's default
		},
	}
	_, err := col.Indexes().CreateMany(ctx, idx)
	// Handle "already exists" type errors more gracefully for CreateMany
	if err != nil {
		if e, ok := err.(mongo.CommandError); ok && (e.Code == 85 || e.Code == 86 || strings.Contains(e.Message, "already exists")) {
			s.logger.Info("Lock indexes (or some components) already exist or created with different options that are compatible.", zap.Error(e))
			return nil // Not a fatal error
		}
		// Catching general string for "already exists" as a fallback, specific to CreateMany scenarios.
		if strings.Contains(err.Error(), "already exist") {
			s.logger.Info("Lock indexes (or some components) already exist.")
			return nil
		}
		return fmt.Errorf("creating lock indexes: %w", err)
	}
	s.logger.Info("Lock indexes ensured.")
	return nil
}

/* ---------- Collection helpers ---------- */

func (s *MongoDBStorage) certCol() *mongo.Collection {
	return s.client.Database(s.Database).Collection(s.Collection)
}
func (s *MongoDBStorage) lockCol() *mongo.Collection {
	return s.client.Database(s.Database).Collection(s.LocksCollection)
}

/* ---------- Storage operations ---------- */

func (s *MongoDBStorage) Store(ctx context.Context, key string, value []byte) error {
	if s.EnableBulkWrites {
		return s.enqueueBulk(ctx, key, value)
	}
	opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	return s.directStore(opCtx, key, value)
}

func (s *MongoDBStorage) directStore(ctx context.Context, key string, value []byte) error {
	now := time.Now().UTC()
	doc := bson.M{
		"_id":      key,
		"value":    primitive.Binary{Subtype: 0x00, Data: value},
		"size":     int64(len(value)),
		"modified": now,
	}
	_, err := s.certCol().UpdateOne(
		ctx,
		bson.M{"_id": key},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err == nil {
		if !s.cache.SetWithTTL(key, value, 1, s.CacheTTL) {
			s.logger.Warn("failed to set item in cache via directStore", zap.String("key", key))
		}
		s.logger.Debug("stored and cached item", zap.String("key", key), zap.Int("size", len(value)))
	} else {
		s.logger.Error("failed to store item", zap.String("key", key), zap.Error(err))
	}
	return err
}

func (s *MongoDBStorage) Load(ctx context.Context, key string) ([]byte, error) {
	if v, ok := s.cache.Get(key); ok {
		return v, nil
	}
	val, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		s.logger.Debug("loading item from db (cache miss)", zap.String("key", key))
		opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
		defer cancel()
		var doc struct {
			Value primitive.Binary `bson:"value"`
		}
		errDb := s.certCol().FindOne(opCtx, bson.M{"_id": key}, options.FindOne().SetProjection(bson.M{"value": 1})).Decode(&doc)
		if errDb != nil {
			if errors.Is(errDb, mongo.ErrNoDocuments) {
				s.logger.Debug("item not found in db", zap.String("key", key))
				return nil, fs.ErrNotExist
			}
			s.logger.Error("failed to load item from db", zap.String("key", key), zap.Error(errDb))
			return nil, errDb
		}
		if !s.cache.SetWithTTL(key, doc.Value.Data, 1, s.CacheTTL) {
			s.logger.Warn("failed to set item in cache via Load (cache miss)", zap.String("key", key))
		}
		s.logger.Debug("loaded and cached item from db", zap.String("key", key), zap.Int("size", len(doc.Value.Data)))
		return doc.Value.Data, nil
	})
	if err != nil {
		return nil, err
	}
	return val.([]byte), nil
}

func (s *MongoDBStorage) Delete(ctx context.Context, key string) error {
	opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	res, err := s.certCol().DeleteOne(opCtx, bson.M{"_id": key})
	if err != nil {
		s.logger.Error("failed to delete item", zap.String("key", key), zap.Error(err))
		return err
	}
	if res.DeletedCount == 0 {
		s.logger.Debug("attempted to delete non-existent item", zap.String("key", key))
		return fs.ErrNotExist
	}
	s.cache.Del(key)
	s.logger.Debug("deleted item from db and cache", zap.String("key", key))
	return nil
}

func (s *MongoDBStorage) Exists(ctx context.Context, key string) bool {
	if _, ok := s.cache.Get(key); ok {
		s.logger.Debug("item exists in cache", zap.String("key", key))
		return true
	}
	opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	var result struct {
		ID string `bson:"_id"`
	}
	err := s.certCol().FindOne(opCtx, bson.M{"_id": key}, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&result)
	if err == nil {
		s.logger.Debug("item exists in db", zap.String("key", key))
		return true
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		s.logger.Error("error checking if item exists", zap.String("key", key), zap.Error(err))
	} else {
		s.logger.Debug("item does not exist in db", zap.String("key", key))
	}
	return false
}

func (s *MongoDBStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	prefix = strings.TrimPrefix(prefix, "/")
	originalPrefixForLog := prefix
	var filter bson.M
	baseIdFilter := bson.M{"$not": primitive.Regex{Pattern: `\.lock$`, Options: ""}}
	if recursive {
		if prefix == "" {
			filter = bson.M{"_id": baseIdFilter}
		} else {
			filter = bson.M{"_id": bson.M{
				"$regex": primitive.Regex{Pattern: "^" + regexp.QuoteMeta(prefix), Options: ""},
				"$not":   primitive.Regex{Pattern: `\.lock$`, Options: ""},
			}}
		}
	} else {
		var regexPattern string
		if prefix == "" {
			regexPattern = "^[^/]+$"
		} else {
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			regexPattern = "^" + regexp.QuoteMeta(prefix) + "[^/]+$"
		}
		filter = bson.M{"_id": bson.M{
			"$regex": primitive.Regex{Pattern: regexPattern, Options: ""},
			"$not":   primitive.Regex{Pattern: `\.lock$`, Options: ""},
		}}
	}
	s.logger.Debug("listing keys", zap.String("original_prefix", originalPrefixForLog), zap.Bool("recursive", recursive), zap.Any("filter_regex", filter["_id"]))
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cur, err := s.certCol().Find(opCtx, filter, opts)
	if err != nil {
		s.logger.Error("failed to list keys from db", zap.String("prefix", prefix), zap.Bool("recursive", recursive), zap.Error(err))
		return nil, err
	}
	defer cur.Close(opCtx)
	var results []string
	for cur.Next(opCtx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&doc); err != nil {
			s.logger.Error("failed to decode listed key", zap.Error(err))
			return nil, err
		}
		results = append(results, doc.ID)
	}
	if err := cur.Err(); err != nil {
		s.logger.Error("cursor error during list", zap.Error(err))
		return nil, err
	}
	sort.Strings(results)
	s.logger.Debug("listed keys successfully", zap.String("prefix", prefix), zap.Bool("recursive", recursive), zap.Int("count", len(results)))
	return results, nil
}

func (s *MongoDBStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	var doc struct {
		Size     int64     `bson:"size"`
		Modified time.Time `bson:"modified"`
	}
	opts := options.FindOne().SetProjection(bson.M{"size": 1, "modified": 1, "_id": 0})
	if err := s.certCol().FindOne(ctx, bson.M{"_id": key}, opts).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return certmagic.KeyInfo{}, fs.ErrNotExist
		}
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        key,
		Size:       doc.Size,
		Modified:   doc.Modified.UTC(),
		IsTerminal: !strings.HasSuffix(key, "/"),
	}, nil
}

type lockHandle struct {
	lockID     string
	key        string
	cancelFunc context.CancelFunc
	logger     *zap.Logger
}

func (s *MongoDBStorage) Lock(ctx context.Context, key string) error {
	docID := key + ".lock"
	myID := uuid.NewString()
	s.logger.Debug("attempting to acquire lock", zap.String("key", key), zap.String("lock_doc_id", docID), zap.String("holder_id", myID))
	for {
		select {
		case <-ctx.Done():
			s.logger.Warn("lock acquisition timed out or context cancelled before attempt", zap.String("key", key), zap.Error(ctx.Err()))
			return ctx.Err()
		default:
		}
		opCtx, opCancel := context.WithTimeout(context.Background(), s.Timeout)
		now := time.Now().UTC()
		expiresAt := now.Add(lockTTL)
		_, err := s.lockCol().InsertOne(opCtx, bson.M{
			"_id":     docID,
			"lock_id": myID,
			"expires": expiresAt,
			"created": now,
		})
		opCancel()
		if err == nil {
			refreshCtx, refreshCancel := context.WithCancel(context.Background())
			s.locks.Store(key, &lockHandle{
				lockID:     myID,
				key:        key,
				cancelFunc: refreshCancel,
				logger:     s.logger.With(zap.String("lock_key", key), zap.String("holder_id", myID)),
			})
			go s.refreshLock(refreshCtx, key, docID, myID)
			s.logger.Info("lock acquired", zap.String("key", key), zap.String("holder_id", myID))
			return nil
		}
		if mongo.IsDuplicateKeyError(err) {
			s.logger.Debug("lock already exists, attempting to check expiry or steal", zap.String("key", key))
			stealCtx, stealCancel := context.WithTimeout(context.Background(), s.Timeout)
			filter := bson.M{"_id": docID, "expires": bson.M{"$lt": now}}
			update := bson.M{"$set": bson.M{"lock_id": myID, "expires": expiresAt, "created": now}}
			res, updateErr := s.lockCol().UpdateOne(stealCtx, filter, update)
			stealCancel()
			if updateErr != nil {
				s.logger.Error("error trying to steal expired lock", zap.String("key", key), zap.Error(updateErr))
			} else if res != nil && res.ModifiedCount == 1 {
				refreshCtx, refreshCancel := context.WithCancel(context.Background())
				s.locks.Store(key, &lockHandle{
					lockID:     myID,
					key:        key,
					cancelFunc: refreshCancel,
					logger:     s.logger.With(zap.String("lock_key", key), zap.String("holder_id", myID)),
				})
				go s.refreshLock(refreshCtx, key, docID, myID)
				s.logger.Info("expired lock stolen and acquired", zap.String("key", key), zap.String("holder_id", myID))
				return nil
			} else {
				s.logger.Debug("failed to steal lock", zap.String("key", key), zap.Int64("matched", res.MatchedCount), zap.Int64("modified", res.ModifiedCount))
			}
		} else {
			s.logger.Error("error acquiring lock on insert", zap.String("key", key), zap.Error(err))
			return fmt.Errorf("failed to insert lock document for key '%s': %w", key, err)
		}
		s.logger.Debug("lock not acquired, polling", zap.String("key", key), zap.Duration("interval", lockPollInterval))
		select {
		case <-time.After(lockPollInterval):
		case <-ctx.Done():
			s.logger.Warn("lock acquisition polling interrupted by context cancellation", zap.String("key", key), zap.Error(ctx.Err()))
			return ctx.Err()
		}
	}
}

func (s *MongoDBStorage) refreshLock(ctx context.Context, originalKey, lockDocID, expectedID string) {
	var handleLogger *zap.Logger
	if h, ok := s.locks.Load(originalKey); ok {
		handleLogger = h.(*lockHandle).logger
	} else {
		handleLogger = s.logger.With(zap.String("lock_key", originalKey), zap.String("holder_id", expectedID))
	}
	handleLogger.Debug("starting lock refresh routine")
	ticker := time.NewTicker(lockRefreshInt)
	defer func() {
		ticker.Stop()
		handleLogger.Debug("stopped lock refresh routine")
	}()
	for {
		select {
		case <-ticker.C:
			opCtx, opCancel := context.WithTimeout(context.Background(), s.Timeout)
			newExp := time.Now().UTC().Add(lockTTL)
			res, err := s.lockCol().UpdateOne(opCtx,
				bson.M{"_id": lockDocID, "lock_id": expectedID},
				bson.M{"$set": bson.M{"expires": newExp}},
			)
			opCancel()
			if err != nil {
				handleLogger.Error("failed to refresh lock due to db error, releasing lock", zap.Error(err))
				s.locks.Delete(originalKey)
				return
			}
			if res == nil || res.MatchedCount == 0 {
				handleLogger.Warn("lock refresh found no matching document or lock_id changed; lock lost or stolen")
				s.locks.Delete(originalKey)
				return
			}
			handleLogger.Debug("lock refreshed successfully", zap.Time("new_expiry", newExp))
		case <-ctx.Done():
			handleLogger.Info("lock refresh routine cancelled", zap.Error(ctx.Err()))
			return
		}
	}
}

func (s *MongoDBStorage) Unlock(ctx context.Context, key string) error {
	docID := key + ".lock"
	s.logger.Debug("unlock requested", zap.String("key", key), zap.String("lock_doc_id", docID))
	if handleI, loaded := s.locks.LoadAndDelete(key); loaded {
		h := handleI.(*lockHandle)
		h.logger.Info("stopping lock refresh routine and attempting to delete lock from db")
		h.cancelFunc()
		opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
		defer cancel()
		delRes, err := s.lockCol().DeleteOne(opCtx, bson.M{"_id": docID, "lock_id": h.lockID})
		if err != nil {
			h.logger.Error("failed to delete lock document from db during unlock", zap.Error(err))
			return fmt.Errorf("failed to delete lock '%s' for holder '%s': %w", docID, h.lockID, err)
		}
		if delRes.DeletedCount == 0 {
			h.logger.Warn("lock document not found or not owned by this instance during unlock")
		} else {
			h.logger.Info("lock document successfully deleted from db")
		}
	} else {
		s.logger.Warn("unlock called for a key not actively locked by this instance or already unlocked", zap.String("key", key))
	}
	return nil
}

func (s *MongoDBStorage) Flush(ctx context.Context) error {
	if !s.EnableBulkWrites {
		s.logger.Debug("flush called but bulk writes not enabled, no-op")
		return nil
	}
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()
	s.logger.Info("flush called, processing queued bulk writes", zap.Int("queue_size", len(s.bulkQueue)))
	opCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	return s.flushBulkLocked(opCtx)
}

func (s *MongoDBStorage) enqueueBulk(ctx context.Context, key string, value []byte) error {
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()
	if s.bulkCtx.Err() != nil {
		s.logger.Error("enqueueBulk called after bulk writer shutdown, performing direct store", zap.String("key", key))
		opCtxDirect, cancelDirect := context.WithTimeout(ctx, s.Timeout)
		defer cancelDirect()
		return s.directStore(opCtxDirect, key, value)
	}
	model := mongo.NewUpdateOneModel().
		SetFilter(bson.M{"_id": key}).
		SetUpdate(bson.M{"$set": bson.M{
			"_id":      key,
			"value":    primitive.Binary{Subtype: 0x00, Data: value},
			"size":     int64(len(value)),
			"modified": time.Now().UTC(),
		}}).
		SetUpsert(true)
	s.bulkQueue = append(s.bulkQueue, model)
	s.logger.Debug("enqueued item for bulk write", zap.String("key", key), zap.Int("new_queue_size", len(s.bulkQueue)))
	if len(s.bulkQueue) >= s.BulkMaxOps {
		s.logger.Info("bulk queue reached max ops, flushing synchronously", zap.Int("queue_size", len(s.bulkQueue)))
		opCtxFlush, cancelFlush := context.WithTimeout(ctx, s.Timeout)
		defer cancelFlush()
		return s.flushBulkLocked(opCtxFlush)
	}
	if !s.bulkTimer.Stop() {
		select {
		case <-s.bulkTimer.C:
		default:
		}
	}
	s.bulkTimer.Reset(s.BulkFlushInterval)
	return nil
}

func (s *MongoDBStorage) runBulkWriter() {
	s.logger.Info("bulk writer goroutine started")
	defer s.logger.Info("bulk writer goroutine stopped")
	for {
		select {
		case <-s.bulkCtx.Done():
			s.logger.Info("bulk writer shutdown signal received, performing final flush")
			s.bulkMu.Lock()
			opCtx, cancel := context.WithTimeout(context.Background(), s.Timeout)
			if err := s.flushBulkLocked(opCtx); err != nil {
				s.logger.Error("error during final bulk flush on shutdown", zap.Error(err))
			}
			cancel()
			s.bulkMu.Unlock()
			return
		case <-s.bulkTimer.C:
			s.bulkMu.Lock()
			if len(s.bulkQueue) > 0 {
				s.logger.Debug("bulk writer timer triggered, flushing queue", zap.Int("queue_size", len(s.bulkQueue)))
				opCtx, cancel := context.WithTimeout(context.Background(), s.Timeout)
				if err := s.flushBulkLocked(opCtx); err != nil {
					s.logger.Error("error during timed bulk flush", zap.Error(err))
				}
				cancel()
			}
			if !s.bulkTimer.Stop() {
				select {
				case <-s.bulkTimer.C:
				default:
				}
			}
			s.bulkTimer.Reset(s.BulkFlushInterval)
			s.bulkMu.Unlock()
		}
	}
}

func (s *MongoDBStorage) flushBulkLocked(ctx context.Context) error {
	if len(s.bulkQueue) == 0 {
		s.logger.Debug("flushBulkLocked called with empty queue, no-op")
		return nil
	}
	currentBatch := make([]mongo.WriteModel, len(s.bulkQueue))
	copy(currentBatch, s.bulkQueue)
	s.bulkQueue = s.bulkQueue[:0]
	s.logger.Info("performing bulk write to db", zap.Int("num_ops", len(currentBatch)))
	_, err := s.certCol().BulkWrite(ctx, currentBatch, options.BulkWrite().SetOrdered(false))
	if err != nil {
		s.logger.Error("mongodb bulkwrite operation failed", zap.Error(err))
		return err
	}
	for _, wm := range currentBatch {
		if updateModel, ok := wm.(*mongo.UpdateOneModel); ok {
			filter, filterOk := updateModel.Filter.(bson.M)
			key, keyOk := filter["_id"].(string)
			update, updateOk := updateModel.Update.(bson.M)
			setFields, setOk := update["$set"].(bson.M)
			if !filterOk || !keyOk || !updateOk || !setOk {
				s.logger.Warn("could not parse key or update fields from bulk model for caching", zap.Any("filter", filter), zap.Any("update", update))
				continue
			}
			value, valOk := setFields["value"].(primitive.Binary)
			if !valOk {
				s.logger.Warn("could not parse value from bulk model for caching", zap.String("key", key), zap.Any("setFields", setFields))
				continue
			}
			if !s.cache.SetWithTTL(key, value.Data, 1, s.CacheTTL) {
				s.logger.Warn("failed to set item in cache via flushBulkLocked", zap.String("key", key))
			}
			s.logger.Debug("cached item after bulk write", zap.String("key", key))
		}
	}
	s.logger.Info("bulk write successful, cache updated", zap.Int("num_ops_flushed", len(currentBatch)))
	return nil
}

func (s *MongoDBStorage) Cleanup() error {
	s.logger.Info("cleanup called for mongodb storage")
	if s.EnableBulkWrites && s.bulkCancel != nil {
		s.logger.Debug("shutting down bulk writer")
		s.bulkCancel()
	}
	s.locks.Range(func(key, value interface{}) bool {
		if lh, ok := value.(*lockHandle); ok {
			lh.logger.Info("cleanup: cancelling lock refresh routine")
			lh.cancelFunc()
		}
		s.locks.Delete(key)
		return true
	})
	if s.cache != nil {
		s.cache.Close()
		s.logger.Debug("ristretto cache closed")
	}
	if s.client != nil {
		s.logger.Debug("disconnecting mongodb client")
		ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		defer cancel()
		if err := s.client.Disconnect(ctx); err != nil {
			s.logger.Error("error disconnecting mongodb client", zap.Error(err))
			return err
		}
		s.logger.Info("mongodb client disconnected successfully")
	}
	return nil
}

/* ---------- Interface guards ---------- */

var (
	_ caddy.Provisioner     = (*MongoDBStorage)(nil)
	_ caddy.Validator       = (*MongoDBStorage)(nil)
	_ caddy.CleanerUpper    = (*MongoDBStorage)(nil)
	_ caddyfile.Unmarshaler = (*MongoDBStorage)(nil)
	_ certmagic.Storage     = (*MongoDBStorage)(nil)
	_ certmagic.Locker      = (*MongoDBStorage)(nil)
)

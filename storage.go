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
	"github.com/dgraph-io/ristretto/v2"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

/* ---------- Public errors ---------- */

var (
	ErrNotExist    = errors.New("key does not exist")
	ErrLockExists  = errors.New("lock already exists")
	ErrLockNotHeld = errors.New("lock not held by this instance")
)

/* ---------- Tunables ---------- */

const (
	lockTTL          = 5 * time.Second        // time-to-live for a lock document
	lockPollInterval = 1 * time.Second        // how often to retry when contended
	lockRefreshInt   = 3 * time.Second        // how often we extend our own lock
	defaultBulkLimit = 100                    // max ops per bulk batch
	defaultBulkWin   = 500 * time.Millisecond // flush window
)

/* ---------- Storage object ---------- */

type MongoDBStorage struct {
	// ---- User-configurable fields (exposed in JSON / Caddyfile) ----
	URI             string        `json:"uri,omitempty"`              // connection string
	Database        string        `json:"database,omitempty"`         // DB name
	Collection      string        `json:"collection,omitempty"`       // certificate documents
	LocksCollection string        `json:"locks_collection,omitempty"` // lock documents
	Timeout         time.Duration `json:"timeout,omitempty"`          // op timeout

	CacheTTL        time.Duration `json:"cache_ttl,omitempty"`         // TTL inside Ristretto
	MaxCacheEntries int           `json:"max_cache_entries,omitempty"` // max items (cost==1)
	MaxPoolSize     uint64        `json:"max_pool_size,omitempty"`     // driver pool
	MinPoolSize     uint64        `json:"min_pool_size,omitempty"`
	MaxConnIdleTime time.Duration `json:"max_conn_idle_time,omitempty"`

	EnableBulkWrites  bool          `json:"enable_bulk_writes,omitempty"`  // group Store() calls
	BulkMaxOps        int           `json:"bulk_max_ops,omitempty"`        // override defaultBulkLimit
	BulkFlushInterval time.Duration `json:"bulk_flush_interval,omitempty"` // override defaultBulkWin

	// ---- Internal fields (not serialised) ----
	client       *mongo.Client
	logger       *zap.Logger
	cache        *ristretto.Cache[string, []byte]
	requestGroup singleflight.Group
	locks        sync.Map // map[string]*lockHandle

	// bulk writer
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

// Caddy calls this where certmagic expects a Storage.
func (s *MongoDBStorage) CertMagicStorage() (certmagic.Storage, error) { return s, nil }

/* ---------- Provision & validation ---------- */

func (s *MongoDBStorage) Provision(ctx caddy.Context) error {
	s.logger = ctx.Logger(s)

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
		s.LocksCollection = "locks"
	}
	if s.Timeout == 0 {
		s.Timeout = 10 * time.Second
	}
	if s.CacheTTL == 0 {
		s.CacheTTL = 10 * time.Minute
	}
	if s.MaxCacheEntries == 0 {
		s.MaxCacheEntries = 1_000
	}
	if s.BulkMaxOps == 0 {
		s.BulkMaxOps = defaultBulkLimit
	}
	if s.BulkFlushInterval == 0 {
		s.BulkFlushInterval = defaultBulkWin
	}

	// -------- Validate mandatory fields --------
	if err := s.Validate(); err != nil {
		return err
	}

	// -------- Connect to MongoDB --------
	if err := s.connect(ctx); err != nil {
		return err
	}

	// -------- Build Ristretto cache --------
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: int64(s.MaxCacheEntries) * 10,
		MaxCost:     int64(s.MaxCacheEntries),
		BufferItems: 64,
	})
	if err != nil {
		return fmt.Errorf("init ristretto: %w", err)
	}
	s.cache = cache

	// -------- Bulk writer --------
	if s.EnableBulkWrites {
		s.bulkCtx, s.bulkCancel = context.WithCancel(context.Background())
		s.bulkTimer = time.NewTimer(s.BulkFlushInterval)
		go s.runBulkWriter()
	}

	return nil
}

func (s *MongoDBStorage) Validate() error {
	switch {
	case s.URI == "":
		return errors.New("mongodb URI is required")
	case s.Database == "":
		return errors.New("mongodb database name is required")
	case s.Collection == "":
		return errors.New("mongodb collection name is required")
	default:
		return nil
	}
}

/* ---------- UnmarshalCaddyfile ---------- */

func (s *MongoDBStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			key := d.Val()
			var val string
			if !d.Args(&val) {
				return d.ArgErr()
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
				if dur, err := time.ParseDuration(val); err == nil {
					s.Timeout = dur
				} else {
					return d.Errf("invalid timeout: %v", err)
				}
			case "cache_ttl":
				if dur, err := time.ParseDuration(val); err == nil {
					s.CacheTTL = dur
				} else {
					return d.Errf("invalid cache_ttl: %v", err)
				}
			case "max_cache_entries":
				if n, err := strconv.Atoi(val); err == nil {
					s.MaxCacheEntries = n
				} else {
					return d.Errf("invalid max_cache_entries: %v", err)
				}
			case "enable_bulk_writes":
				s.EnableBulkWrites = (val == "true" || val == "on" || val == "yes")
			case "bulk_max_ops":
				if n, err := strconv.Atoi(val); err == nil {
					s.BulkMaxOps = n
				} else {
					return d.Errf("invalid bulk_max_ops: %v", err)
				}
			case "bulk_flush_interval":
				if dur, err := time.ParseDuration(val); err == nil {
					s.BulkFlushInterval = dur
				} else {
					return d.Errf("invalid bulk_flush_interval: %v", err)
				}
			}
		}
	}
	return nil
}

/* ---------- MongoDB connection & indexes ---------- */

func (s *MongoDBStorage) connect(ctx caddy.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	opts := options.Client().
		ApplyURI(s.URI).
		SetServerSelectionTimeout(s.Timeout).
		SetConnectTimeout(s.Timeout).
		SetMaxPoolSize(s.MaxPoolSize).
		SetMinPoolSize(s.MinPoolSize).
		SetMaxConnIdleTime(s.MaxConnIdleTime).
		SetCompressors([]string{"zstd", "snappy"})

	client, err := mongo.Connect(connectCtx, opts)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, s.Timeout)
	defer pingCancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return fmt.Errorf("mongo ping: %w", err)
	}
	s.client = client

	// ensure indexes on lock collection
	return s.ensureLockIndexes(ctx)
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
			Options: options.Index().SetName("lock_id_idx"),
		},
	}
	_, err := col.Indexes().CreateMany(ctx, idx)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
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
		return s.enqueueBulk(key, value)
	}
	return s.directStore(ctx, key, value)
}

func (s *MongoDBStorage) directStore(ctx context.Context, key string, value []byte) error {
	now := time.Now().UTC()
	doc := bson.M{
		"_id":      key,
		"value":    primitive.Binary{Subtype: 0x00, Data: value},
		"modified": now,
	}

	_, err := s.certCol().UpdateOne(
		ctx,
		bson.M{"_id": key},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err == nil {
		s.cache.SetWithTTL(key, value, 1, s.CacheTTL)
	}
	return err
}

func (s *MongoDBStorage) Load(ctx context.Context, key string) ([]byte, error) {
	if v, ok := s.cache.Get(key); ok {
		return v, nil
	}

	val, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		var doc struct {
			Value primitive.Binary `bson:"value"`
		}
		if err := s.certCol().FindOne(ctx, bson.M{"_id": key}).Decode(&doc); err != nil {
			return nil, fs.ErrNotExist
		}
		s.cache.SetWithTTL(key, doc.Value.Data, 1, s.CacheTTL)
		return doc.Value.Data, nil
	})
	if err != nil {
		return nil, err
	}
	return val.([]byte), nil
}

func (s *MongoDBStorage) Delete(ctx context.Context, key string) error {
	_, err := s.certCol().DeleteOne(ctx, bson.M{"_id": key})
	s.cache.Del(key)
	return err
}

func (s *MongoDBStorage) Exists(ctx context.Context, key string) bool {
	if _, ok := s.cache.Get(key); ok {
		return true
	}
	cnt, err := s.certCol().CountDocuments(ctx, bson.M{"_id": key}, options.Count().SetLimit(1))
	return err == nil && cnt > 0
}

func (s *MongoDBStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	prefix = strings.TrimPrefix(prefix, "/")

	// Build filter
	var filter bson.M
	if recursive {
		filter = bson.M{"_id": bson.M{"$regex": fmt.Sprintf("^%s", regexp.QuoteMeta(prefix))}}
	} else {
		if prefix == "" {
			filter = bson.M{"_id": bson.M{"$regex": "^[^/]+(?=/|$)"}}
		} else {
			prefix = strings.TrimSuffix(prefix, "/") + "/"
			filter = bson.M{"_id": bson.M{"$regex": fmt.Sprintf("^%s[^/]+(?=/|$)", regexp.QuoteMeta(prefix))}}
		}
	}
	filter["_id"].(bson.M)["$not"] = primitive.Regex{Pattern: "\\.lock$", Options: ""}

	cur, err := s.certCol().Find(ctx, filter, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	res := make([]string, 0)
	seen := map[string]struct{}{}
	for cur.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}

		if recursive {
			res = append(res, doc.ID)
			continue
		}

		parts := strings.Split(doc.ID, "/")
		if prefix == "" {
			if _, ok := seen[parts[0]]; !ok {
				seen[parts[0]] = struct{}{}
				res = append(res, parts[0])
			}
		} else {
			pp := strings.Split(strings.TrimSuffix(prefix, "/"), "/")
			if len(parts) > len(pp) {
				next := parts[len(pp)]
				if _, ok := seen[next]; !ok {
					seen[next] = struct{}{}
					res = append(res, next)
				}
			}
		}
	}

	sort.Strings(res)
	return res, cur.Err()
}

func (s *MongoDBStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	var doc struct {
		Value    primitive.Binary `bson:"value"`
		Modified time.Time        `bson:"modified"`
	}
	if err := s.certCol().FindOne(ctx, bson.M{"_id": key}).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return certmagic.KeyInfo{}, fs.ErrNotExist
		}
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:      key,
		Size:     int64(len(doc.Value.Data)),
		Modified: doc.Modified.UTC(),
	}, nil
}

/* ---------- Locks ---------- */

type lockHandle struct {
	lockID     string
	cancelFunc context.CancelFunc
}

func (s *MongoDBStorage) Lock(ctx context.Context, key string) error {
	docID := key + ".lock"
	myID := uuid.NewString()
	lockCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	for {
		select {
		case <-lockCtx.Done():
			return lockCtx.Err()
		default:
			now := time.Now()
			exp := now.Add(lockTTL)
			_, err := s.lockCol().InsertOne(lockCtx, bson.M{
				"_id":     docID,
				"lock_id": myID,
				"expires": exp,
				"created": now,
			})
			if err == nil {
				refreshCtx, cf := context.WithCancel(context.Background())
				s.locks.Store(key, &lockHandle{lockID: myID, cancelFunc: cf})
				go s.refreshLock(refreshCtx, key, myID)
				return nil
			}
			if mongo.IsDuplicateKeyError(err) {
				var existing struct{ Expires time.Time }
				_ = s.lockCol().FindOne(lockCtx, bson.M{"_id": docID}).Decode(&existing)
				if existing.Expires.Before(now) {
					res, _ := s.lockCol().UpdateOne(lockCtx,
						bson.M{"_id": docID, "expires": bson.M{"$lt": now}},
						bson.M{"$set": bson.M{"lock_id": myID, "expires": exp}},
					)
					if res != nil && res.ModifiedCount == 1 {
						refreshCtx, cf := context.WithCancel(context.Background())
						s.locks.Store(key, &lockHandle{lockID: myID, cancelFunc: cf})
						go s.refreshLock(refreshCtx, key, myID)
						return nil
					}
				}
				time.Sleep(lockPollInterval)
				continue
			}
			return err
		}
	}
}

func (s *MongoDBStorage) refreshLock(ctx context.Context, key, expectedID string) {
	ticker := time.NewTicker(lockRefreshInt)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			newExp := time.Now().Add(lockTTL)
			res, err := s.lockCol().UpdateOne(context.Background(),
				bson.M{"_id": key + ".lock", "lock_id": expectedID},
				bson.M{"$set": bson.M{"expires": newExp}},
			)
			if err != nil || res.MatchedCount == 0 {
				s.locks.Delete(key)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *MongoDBStorage) Unlock(ctx context.Context, key string) error {
	docID := key + ".lock"
	if handleI, ok := s.locks.LoadAndDelete(key); ok {
		h := handleI.(*lockHandle)
		h.cancelFunc()
		_, _ = s.lockCol().DeleteOne(ctx, bson.M{"_id": docID, "lock_id": h.lockID})
	} else {
		_, _ = s.lockCol().DeleteOne(ctx, bson.M{"_id": docID})
	}
	return nil
}

func (s *MongoDBStorage) Flush(ctx context.Context) error {
	if !s.EnableBulkWrites {
		return nil
	}
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()

	// It's safe to call flushBulkLocked even if the queue is empty
	// or if the bulk writer goroutine is concurrently attempting to flush,
	// as bulkMu synchronizes access.
	return s.flushBulkLocked()
}

/* ---------- Bulk writer ---------- */

func (s *MongoDBStorage) enqueueBulk(key string, value []byte) error {
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()

	model := mongo.NewUpdateOneModel().
		SetFilter(bson.M{"_id": key}).
		SetUpdate(bson.M{"$set": bson.M{
			"_id":      key,
			"value":    primitive.Binary{Subtype: 0x00, Data: value},
			"modified": time.Now().UTC(),
		}}).
		SetUpsert(true)

	s.bulkQueue = append(s.bulkQueue, model)
	if len(s.bulkQueue) >= s.BulkMaxOps {
		return s.flushBulkLocked()
	}

	// reset timer
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
	for {
		select {
		case <-s.bulkCtx.Done():
			s.bulkMu.Lock()
			_ = s.flushBulkLocked()
			s.bulkMu.Unlock()
			return
		case <-s.bulkTimer.C:
			s.bulkMu.Lock()
			_ = s.flushBulkLocked()
			s.bulkMu.Unlock()
		}
	}
}

func (s *MongoDBStorage) flushBulkLocked() error {
	if len(s.bulkQueue) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
	defer cancel()
	_, err := s.certCol().BulkWrite(ctx, s.bulkQueue, options.BulkWrite().SetOrdered(false))
	if err == nil {
		for _, wm := range s.bulkQueue {
			m := wm.(*mongo.UpdateOneModel)
			key := m.Filter.(bson.M)["_id"].(string)
			val := m.Update.(bson.M)["$set"].(bson.M)["value"].(primitive.Binary).Data
			s.cache.SetWithTTL(key, val, 1, s.CacheTTL)
		}
	}
	s.bulkQueue = s.bulkQueue[:0]
	return err
}

/* ---------- Clean-up ---------- */

func (s *MongoDBStorage) Cleanup() error {
	if s.bulkCancel != nil {
		s.bulkCancel()
	}
	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		defer cancel()
		_ = s.client.Disconnect(ctx)
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

# MongoDB Storage module for Caddy / Certmagic

MongoDB storage for CertMagic/Caddy TLS data with advanced caching and connection pooling. High-throughput, production-ready backend with Ristretto cache & bulk writes

## Features

- **MongoDB Backend**: Persists Caddy's TLS certificates and related CertMagic data in MongoDB.
- **CertMagic Compatible**: Implements `certmagic.Storage` and `certmagic.Locker` interfaces.
- **High-Performance Caching**: Utilizes Ristretto for in-memory caching of frequently accessed data.
  - Configurable Time-To-Live (TTL) for cache entries.
  - Configurable maximum number of cache entries.
- **Optimized Connection Pooling**: Leverages the official MongoDB driver's connection pooling.
  - Configurable maximum and minimum pool sizes.
  - Configurable maximum connection idle time.
- **Efficient Bulk Writes**: Optionally groups multiple `Store` operations into batches for improved write throughput.
  - Configurable maximum operations per batch.
  - Configurable flush interval for batching.
- **Singleflight Load Operations**: Prevents thundering herd by ensuring that for a given key, only one request to the database is made if the item is not in the cache.
- **Robust Distributed Locking**: Implements a locking mechanism suitable for distributed environments.
  - Utilizes MongoDB TTL indexes for automatic expiration of stale locks.
  - Periodic refresh of active locks.
  - Retry mechanism for lock acquisition.
- **Flexible Configuration**:
  - Caddyfile directives.
  - JSON configuration.
  - Environment variable fallbacks for key parameters.
- **Comprehensive Key Listing**: Supports both recursive and non-recursive listing of stored items.
- **Configurable Timeouts**: Global timeout setting for MongoDB operations.
- **Structured Logging**: Integrated with Zap logger for detailed operational and debug logging.
- **Automatic Index Management**: Ensures creation of necessary MongoDB indexes for the locks collection to optimize performance and TTL functionality.

## Configuration

Enable MongoDB storage for Caddy by specifying the module configuration in the Caddyfile:

```caddyfile
{
    storage mongodb {
        uri "mongodb://localhost:27017"
        database "caddy"
        collection "certificates"
        timeout "10s"
        cache_ttl "10m"
        max_cache_entries 1000
        max_pool_size 100 # Optional: Max MongoDB connections
        min_pool_size 10  # Optional: Min MongoDB connections
        max_conn_idle_time 5m # Optional: Max idle time for a connection
    }
}
```

### Configuration Options

| Option              | Description                    | Default                        | Required |
| ------------------- | ------------------------------ | ------------------------------ | -------- |
| uri                 | MongoDB connection string      | -                              | Yes      |
| database            | Database name                  | -                              | Yes      |
| collection          | Collection name                | -                              | Yes      |
| locks_collection    | Locks Collection name          | `locks`                        | No       |
| timeout             | Operation timeout              | `10s`                          | No       |
| cache_ttl           | Cache entry lifetime           | `10m`                          | No       |
| max_cache_entries   | Maximum number of cached items | `1000`                         | No       |
| max_pool_size       | Max connections in pool        | `100` (driver default)         | No       |
| min_pool_size       | Min connections in pool        | `0` (driver default)           | No       |
| max_conn_idle_time  | Max connection idle time       | `0` (driver default, no limit) | No       |
| enable_bulk_writes  | Group Store() calls            | `false`                        | No       |
| bulk_max_ops        | Max ops per bulk batch         | `100`                          | No       |
| bulk_flush_interval | Flush window for bulk writes   | `500ms`                        | No       |

- NOTE: if you operate a replica set you can keep sweep reads off the primary
  simply by appending `readPreference=secondaryPreferred` to the `uri`.

## JSON Configuration

```json
{
  "storage": {
    "module": "mongodb",
    "uri": "mongodb://root:example@mongo:27017/?compressors=zstd,snappy",
    "database": "caddy",
    "collection": "certs",
    "locks_collection": "locks",
    "timeout": "10s",

    "cache_ttl": "10m",
    "max_cache_entries": 1000,

    "max_pool_size": 100,
    "enable_bulk_writes": true,
    "bulk_max_ops": 100,
    "bulk_flush_interval": "500ms"
  }
}
```

## Environment Variables

You can also configure the storage module using environment variables:

```
| Variable                     | Purpose                                                  |
| ---------------------------- | -------------------------------------------------------- |
| `MONGODB_URI`                | Connection string                                        |
| `MONGODB_DATABASE`           | Database name                                            |
| `MONGODB_COLLECTION`         | Cert collection                                          |
| `MONGODB_LOCKS_COLLECTION`   | Lock collection (optional)                               |
| `MONGODB_TIMEOUT`            | Operation timeout (defaults to `10s` if not set/invalid) |
| `MONGODB_MAX_POOL_SIZE`      | Max connections                                          |
| `MONGODB_MIN_POOL_SIZE`      | Min connections                                          |
| `MONGODB_MAX_CONN_IDLE_TIME` | Max idle time (driver default is no limit if not set)    |
```

## Building with xcaddy

To build Caddy with the MongoDB storage module:

```bash
xcaddy build \
    --with github.com/root-sector/caddy-storage-mongodb
```

## Docker

### Production Dockerfile

```dockerfile
# Version to build
ARG CADDY_VERSION="2.8.4"

# Build stage
FROM caddy:${CADDY_VERSION}-builder AS builder

# Add module with xcaddy
RUN xcaddy build \
    --with github.com/root-sector/caddy-storage-mongodb

# Final stage
FROM caddy:${CADDY_VERSION}

# Copy the built Caddy binary
COPY --from=builder /usr/bin/caddy /usr/bin/caddy

# Copy the Caddyfile
COPY Caddyfile /etc/caddy/Caddyfile

# Format the Caddyfile
RUN caddy fmt --overwrite /etc/caddy/Caddyfile
```

### Development Dockerfile

```dockerfile
# Version to build
ARG CADDY_VERSION="2.8.4"

# Build stage
FROM caddy:${CADDY_VERSION}-builder AS builder

# Add module with xcaddy
COPY caddy-storage-mongodb /caddy-storage-mongodb
RUN xcaddy build \
    --with github.com/root-sector/caddy-storage-mongodb=/caddy-storage-mongodb

# Final stage
FROM caddy:${CADDY_VERSION}

# Copy the built Caddy binary
COPY --from=builder /usr/bin/caddy /usr/bin/caddy

# Copy the Caddyfile
COPY Caddyfile /etc/caddy/Caddyfile

# Format the Caddyfile
RUN caddy fmt --overwrite /etc/caddy/Caddyfile
```

## Testing

To run the tests, first start a test MongoDB instance:

```bash
docker-compose up -d mongodb
```

Then run the tests:

```bash
go test -v ./...
```

## Benchmarking

The module includes benchmark tests to evaluate the performance of core storage operations. These benchmarks use `testcontainers-go` to spin up an isolated MongoDB instance for each run, ensuring consistent and reliable results.

### Prerequisites

- **Docker**: Ensure Docker is installed and running on your system, as `testcontainers-go` relies on it.

### Running Benchmarks

Navigate to the module directory (`caddy-storage-mongodb`) and use the standard `go test` command with the `-bench` flag. To avoid running regular tests, you can use `-run="^$"`.

To achieve cleaner benchmark output (suppressing `INFO` and `DEBUG` logs from the storage module itself), you can set the `CADDY_MONGODB_BENCHMARK_LOG_LEVEL` environment variable to `error` or `panic` before running the benchmarks. This feature requires the version of the module that includes this environment variable check.

**Windows (PowerShell):**

```powershell
$env:CADDY_MONGODB_BENCHMARK_LOG_LEVEL="error"
go test -run="^$" -bench="."
# To clear after: Remove-Item Env:CADDY_MONGODB_BENCHMARK_LOG_LEVEL
```

**Linux/macOS (bash):**

```bash
CADDY_MONGODB_BENCHMARK_LOG_LEVEL=error go test -run="^$" -bench="."
```

**Common Benchmark Commands:**

```bash
# Run all benchmarks in the package
# On Windows, ensure the dot for "all benchmarks" is quoted.
go test -run="^$" -bench="."

# Run a specific benchmark function (e.g., BenchmarkStore)
go test -run="^$" -bench="^BenchmarkStore$"

# Run benchmarks with memory allocation statistics
go test -run="^$" -bench="." -benchmem

# Run benchmarks multiple times (e.g., 5 times) for more stable results
# Adding -v can show b.Log output from your benchmarks, useful for seeing setup steps.
go test -run="^$" -bench="." -count=5 -v

# Run benchmarks for a specific duration (e.g., 3 seconds per benchmark)
go test -run="^$" -bench="." -benchtime=3s
```

### Example Benchmark Output

Here is a sample output from running `go test -run="^$" -bench="."` on a Windows machine with a 12th Gen Intel i7 CPU:

```
goos: windows
goarch: amd64
pkg: github.com/root-sector/caddy-storage-mongodb
cpu: 12th Gen Intel(R) Core(TM) i7-12700KF
BenchmarkStore/WithBulkWrites-20                   16597             64680 ns/op
BenchmarkStore/WithoutBulkWrites-20                 2736            422473 ns/op
BenchmarkLoad/CacheHit-20                           5421            317896 ns/op
BenchmarkLoad/CacheMiss-20                          2925            359599 ns/op
BenchmarkList/Recursive-20                           154           7549510 ns/op
BenchmarkList/NonRecursive-20                        229           5147897 ns/op
BenchmarkList/NonRecursiveSubfolder-20              2778            426876 ns/op
BenchmarkStat-20                                    3358            358434 ns/op
PASS
ok      github.com/root-sector/caddy-storage-mongodb    71.759s
```

### Interpreting Benchmark Results

The output from `go test -bench` provides several key metrics. For example:

- **`ns/op` (Nanoseconds per operation)**: This is the average time taken to execute the benchmarked code once. **This is a primary performance indicator. Lower values are better.**
- **`B/op` (Bytes per operation)**: (Requires `-benchmem` flag) The average number of bytes allocated on the heap per operation. **Lower values are better.**
- **`allocs/op` (Allocations per operation)**: (Requires `-benchmem` flag) The average number of distinct memory allocations per operation. **Lower values are better.**

Lines like `--- BENCH: BenchmarkLoad/CacheHit-20` denote the start of logged output (`b.Log(...)`) for a specific benchmark run. The lines following it, such as `storage_bench_test.go:144: Pre-populating...`, are the actual log messages from the benchmark code, useful for understanding the setup and execution flow for each benchmark case. This detailed logging appears when `go test` runs each benchmark function multiple times to stabilize results, or when using the `-v` flag (especially with `-count=1`).

### Evaluating Performance

When analyzing benchmark results:

- **Compare `ns/op`**: This is the most direct measure of speed. For instance, compare `BenchmarkStore/WithBulkWrites` vs. `BenchmarkStore/WithoutBulkWrites` to see the performance impact of bulk writes.
- **Look at `B/op` and `allocs/op`**: These metrics help understand memory efficiency. Fewer allocations and bytes per operation generally lead to less garbage collector overhead and better overall performance.
- **Cache Impact**: Compare `BenchmarkLoad/CacheHit` vs. `BenchmarkLoad/CacheMiss`. A significantly lower `ns/op` for cache hits demonstrates the effectiveness of the Ristretto cache.
- **Consistency**: Running benchmarks multiple times (using `-count`) can help identify variability in performance. Look for consistent results across runs.
- **Context Matters**: Absolute numbers are less important than relative differences between configurations or code versions. Use benchmarks to track performance improvements or regressions as the code evolves.

The provided benchmarks (`BenchmarkStore`, `BenchmarkLoad`, `BenchmarkList`) cover fundamental operations. You can expand on these or create new ones to test specific scenarios relevant to your use case.

## Debug Logging

The module uses Zap logger for debug logging. Enable debug logging in your Caddy configuration:

```caddyfile
{
    debug
    storage mongodb {
        ...
    }
}
```

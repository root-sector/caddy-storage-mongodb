# MongoDB Storage module for Caddy / Certmagic

MongoDB storage for CertMagic/Caddy TLS data with advanced caching and connection pooling.

## Features

- MongoDB-based storage for Caddy's TLS certificates and data
- In-memory caching with TTL and size limits
- Connection pooling optimization
- Automatic cleanup of expired locks and cache entries
- Retry mechanism for improved reliability
- Configurable through Caddyfile or JSON
- Support for both recursive and non-recursive listing
- Proper handling of lock files
- Debug logging with Zap logger

## Configuration

Enable MongoDB storage for Caddy by specifying the module configuration in the Caddyfile:

````caddyfile
{
    storage mongodb {
        uri "mongodb://localhost:27017"
        database "caddy"
        collection "certificates"
        timeout "10s"
        cache_ttl "10m"
        cache_cleanup_interval "5m"
        max_cache_size 1000
        max_pool_size 100 # Optional: Max MongoDB connections
        min_pool_size 10  # Optional: Min MongoDB connections
        max_conn_idle_time 5m # Optional: Max idle time for a connection
    }
}
```

### Configuration Options

| Option                 | Description                    | Default | Required |
| ---------------------- | ------------------------------ | ------- | -------- |
| uri                    | MongoDB connection string      | -       | Yes      |
| database               | Database name                  | -       | Yes      |
| collection             | Collection name                | -       | Yes      |
| timeout                | Operation timeout              | 10s     | No       |
| cache_ttl              | Cache entry lifetime           | 10m     | No       |
| cache_cleanup_interval | Interval for cache cleanup     | 5m      | No       |
| max_cache_size         | Maximum number of cached items | 1000    | No       |
| max_pool_size          | Max connections in pool        | 100     | No       |
| min_pool_size          | Min connections in pool        | 0       | No       |
| max_conn_idle_time     | Max connection idle time       | 5m      | No       |

## JSON Configuration

```json
{
  "storage": {
    "module": "mongodb",
    "uri": "mongodb://localhost:27017",
    "database": "caddy",
    "collection": "certificates",
    "timeout": "10s",
    "cache_ttl": "10m",
    "cache_cleanup_interval": "5m",
    "max_cache_size": 1000,
    "max_pool_size": 100,
    "min_pool_size": 10,
    "max_conn_idle_time": "5m"
   }
  }
````

## Environment Variables

You can also configure the storage module using environment variables:

```bash
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=caddy
MONGODB_COLLECTION=certificates
MONGODB_TIMEOUT=10s
# Optional Pool Settings
MONGODB_MAX_POOL_SIZE=100
MONGODB_MIN_POOL_SIZE=10
MONGODB_MAX_CONN_IDLE_TIME=5m
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

## Performance Optimizations

The module includes several performance optimizations:

1. Connection Pooling

   - Configurable pool size
   - Connection lifetime management
   - Automatic connection cleanup

2. Caching

   - In-memory cache with TTL
   - Size-based cache eviction
   - Periodic cache cleanup

3. Lock Management

   - Automatic lock expiration
   - Periodic cleanup of expired locks
   - Race condition prevention

4. Query Optimization

   - Efficient regex patterns for listing
   - Proper indexing support
   - Projection queries to minimize data transfer

5. Error Handling
   - Automatic retries for transient errors
   - Configurable retry policy
   - Detailed error logging

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

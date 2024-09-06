# MongoDB Storage module for Caddy / Certmagic

MongoDB storage for CertMagic/Caddy TLS data.

This module allows you to use MongoDB as a storage backend for Caddy’s TLS data, leveraging the flexibility and scalability of MongoDB.

## Configuration

Enable MongoDB storage for Caddy by specifying the module configuration in the Caddyfile:

```
{
    storage mongodb {
        uri "mongodb://localhost:27017"
        database "caddy"
        collection "certificates"
        timeout "10s"
    }
}

:443 {

}
```

## JSON Configuration

```json
{
    "storage": {
        "module": "mongodb",
        "uri": "mongodb://localhost:27017",
        "database": "caddy",
        "collection": "certificates",
        "timeout": "10s"
    },
    "apps": {
        ...
    }
}
```

## Environment Variables

You can also configure the storage module using environment variables:

MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=caddy
MONGODB_COLLECTION=certificates
MONGODB_TIMEOUT=10s

## Building with xcaddy

To build Caddy with the MongoDB storage module, use xcaddy:

xcaddy build \
 --with github.com/root-sector/caddy-storage-mongodb

## Docker - Dockerfile

To build a Docker image with the MongoDB storage module:

```Dockerfile
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

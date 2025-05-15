# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Your new feature here.

### Changed

- Your change here.

### Deprecated

- Your deprecated feature here.

### Removed

- Your removed feature here.

### Fixed

- Your bug fix here.

### Security

- Your security update here.

## [2.1.0] - 2025-05-15

This version reflects internal changes and improvements primarily focused on performance and compatibility.

### Changed

- **Certificate Indexing Strategy**:
  - The `id_mod_no_lock_idx` on the certificate collection is now a compound index on `(_id: 1, modified: 1)` without a partial filter expression.
  - This change was made to ensure compatibility with MongoDB 7.0+ which does not support the `$not` operator within partial filter expressions as previously intended.
  - The compound index continues to provide significant performance benefits for `List()` operations (via covered queries on `_id`) and aids `Stat()` operations, preventing full collection scans.
- **"Skinny Stat()"**: The `Stat()` operation was optimized to fetch only the `size` and `modified` fields, reducing data transfer from the database, especially during large sweeps.
- **Cache Set Logging**: Added logging for failures to set items in the Ristretto cache in `directStore`, `Load` (cache-miss path), and `flushBulkLocked` to improve diagnosability of caching issues.
- **Benchmarking**: Updated example benchmark results in `Readme.md` to reflect latest performance figures.

### Added

- **Document Size Tracking**: The `size` of the stored data is now an explicit field in the certificate documents. Existing documents are treated as `size = 0` until they are updated.

### Fixed

- Resolved test failures related to certificate index creation on MongoDB 7.0+ by adopting the revised indexing strategy.

### Performance

- **`Store/WithoutBulkWrites`**: Improved performance by approximately 19% compared to v2.0.0.
- **`List/NonRecursive`**: Significantly improved performance by approximately 45% compared to v2.0.0.
- **`Store/WithBulkWrites`**: Minor performance fluctuations, remains highly efficient.

## [2.0.0] - 2025-05-13

This version marked a major overhaul, introducing a more robust and feature-rich storage backend for Caddy and CertMagic.

### Added

- **CertMagic Compatibility**: Implemented `certmagic.Storage` and `certmagic.Locker` interfaces, making it a fully compatible CertMagic storage module.
- **High-Performance Caching (Ristretto)**: Integrated Ristretto for efficient in-memory caching, replacing or significantly enhancing previous caching mechanisms.
  - Added configurable Time-To-Live (TTL) for cache entries.
  - Added configurable maximum number of cache entries.
- **Optimized Connection Pooling**: Leveraged the official MongoDB driver's connection pooling with more granular controls:
  - Configurable maximum and minimum pool sizes.
  - Configurable maximum connection idle time.
- **Efficient Bulk Writes**: Introduced an optional feature to group multiple `Store` operations into batches for improved write throughput.
  - Configurable maximum operations per batch.
  - Configurable flush interval for batching.
- **Singleflight Load Operations**: Implemented singleflight logic for `Load` operations to prevent thundering herd issues on cache misses.
- **Robust Distributed Locking**: Enhanced the locking mechanism for distributed environments.
  - Utilizes MongoDB TTL indexes for automatic expiration of stale locks.
  - Added periodic refresh of active locks.
- **Flexible Configuration via Environment Variables**: Added support for configuring key parameters (URI, database, collection names, timeouts, pool sizes) via environment variables as fallbacks.
- **Configurable Timeouts**: Introduced a global timeout setting for MongoDB operations.
- **Automatic Index Management (Locks Collection)**: Implemented automatic creation and management of necessary MongoDB indexes for the locks collection to optimize performance and TTL functionality.

### Changed

- **Core Storage Logic**: Refactored to align with CertMagic interfaces and to support the new features like Ristretto caching, bulk writes, and singleflight.
- **Configuration Options**: Expanded Caddyfile and JSON configuration options to support new features (caching, pooling, bulk writes, timeouts).
- **Logging**: Standardized on Zap logger for more structured and detailed operational/debug logging.

### Removed

- Older, less efficient caching or locking mechanisms if they were replaced by Ristretto and the enhanced distributed locking.

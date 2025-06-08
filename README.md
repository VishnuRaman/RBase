# RedBase: An HBase-like Database in Rust

RedBase is a lightweight implementation of an HBase-like distributed database system written in Rust. It provides a simplified but functional subset of Apache HBase's features, focusing on core functionality while maintaining a clean, memory-safe implementation.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
  - [Creating Tables and Column Families](#creating-tables-and-column-families)
  - [Writing Data](#writing-data)
  - [Reading Data](#reading-data)
  - [Deleting Data](#deleting-data)
  - [Scanning Data](#scanning-data)
  - [Flushing and Compaction](#flushing-and-compaction)
- [Advanced Features](#advanced-features)
  - [Multi-Version Concurrency Control](#multi-version-concurrency-control)
  - [Tombstones and TTL](#tombstones-and-ttl)
  - [Filtering](#filtering)
  - [Aggregation](#aggregation)
- [Advanced Client Features](#advanced-client-features)
  - [Asynchronous API](#asynchronous-api)
  - [Batch Operations](#batch-operations)
  - [Connection Pooling](#connection-pooling)
  - [REST Interface](#rest-interface)
- [Examples](#examples)
  - [User Profile Management](#user-profile-management)
  - [Time Series Data](#time-series-data)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Overview

RedBase implements a columnar storage system with:
- Tables and Column Families
- Multi-Version Concurrency Control (MVCC)
- Tombstone markers for deleted data with TTL
- Background compaction with various strategies
- Version filtering and cleanup

## Architecture

RedBase follows a similar architecture to HBase with some simplifications:

- **MemStore**: In-memory storage with Write-Ahead Log (WAL) for durability
- **SSTable**: Immutable on-disk storage format for persisted data
- **Column Family**: Logical grouping of columns with versioning support
- **Table**: Container for multiple Column Families

## Features

### Features Consistent with HBase

RedBase implements the following features that are consistent with Apache HBase:

1. **Data Model**
   - Tables containing Column Families
   - Row-oriented storage with column qualifiers
   - Timestamps for versioning
   - Binary storage (keys and values are byte arrays)

2. **MVCC (Multi-Version Concurrency Control)**
   - Support for multiple versions of data
   - Timestamp-based versioning
   - Version retrieval with configurable limits

3. **Storage Architecture**
   - MemStore for in-memory storage
   - Write-Ahead Log (WAL) for durability
   - SSTables for on-disk storage
   - Automatic flushing of MemStore to disk

4. **Compaction**
   - Minor compaction (subset of SSTables)
   - Major compaction (all SSTables)
   - Version-based cleanup during compaction
   - Age-based cleanup during compaction
   - Tombstone cleanup

5. **Tombstones**
   - Delete markers with optional TTL
   - Proper handling in reads and compactions

6. **API**
   - Put operations
   - Get operations (single version and multi-version)
   - Delete operations
   - Scan operations
   - Flush operations
   - Filtering operations with various predicates
   - Aggregation operations (count, sum, avg, min, max)

7. **Client Features**
   - Connection pooling for efficient resource management
   - Batch operations for efficient multi-operation transactions
   - Async API for non-blocking operations
   - REST interface for HTTP access

### Features Missing Compared to HBase

RedBase is a simplified implementation and lacks several features present in Apache HBase:

1. **Distributed Architecture**
   - No RegionServers or distributed storage
   - No ZooKeeper integration for coordination
   - No sharding/partitioning of data across nodes
   - No replication or high availability features

2. **Advanced Features**
   - No coprocessors or custom filters
   - No bloom filters for optimized lookups
   - No block cache for frequently accessed data
   - No compression of stored data
   - No encryption
   - No snapshots
   - No bulk loading

3. **Performance Optimizations**
   - Limited indexing capabilities
   - No bloom filters to skip SSTables
   - No block encoding or compression
   - No off-heap memory management

4. **Administration**
   - No web UI or administrative tools
   - No metrics collection
   - No dynamic configuration
   - No region splitting or merging
   - No balancing of data across nodes

5. **Query Capabilities**
   - No secondary indices

6. **Operational Features**
   - No rolling upgrades
   - No backup and restore utilities
   - No replication for disaster recovery

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/RedBase.git
cd RedBase

# Build the project
cargo build --release
```

## Basic Usage

Here's a simple example of using RedBase:

```rust
use std::path::Path;
use RedBase::api::Table;

fn main() -> std::io::Result<()> {
    // Create a table with a column family
    let mut table = Table::open("./data/example_table")?;
    if table.cf("default").is_none() {
        table.create_cf("default")?;
    }

    let cf = table.cf("default").unwrap();

    // Write some data
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;

    // Read data
    let value = cf.get(b"row1", b"col1")?;
    println!("Value: {:?}", value.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Read multiple versions
    let versions = cf.get_versions(b"row1", b"col1", 10)?;
    for (ts, value) in versions {
        println!("  {} -> {}", ts, String::from_utf8_lossy(&value).to_string());
    }

    // Delete data
    cf.delete(b"row1".to_vec(), b"col1".to_vec())?;

    // Flush to disk
    cf.flush()?;

    // Run compaction
    cf.compact()?;

    // Use advanced filters
    use RedBase::filter::{Filter, FilterSet};

    // Simple filter
    let filter = Filter::Equal(b"value1".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter)?;

    // Create a filter set for scanning
    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::GreaterThan(b"value1".to_vec())
    );

    // Scan with filter
    let scan_result = cf.scan_row_with_filter(b"row1", &filter_set)?;

    // Use aggregations
    use RedBase::aggregation::{AggregationType, AggregationSet};

    // Create an aggregation set
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Count);
    agg_set.add_aggregation(b"col2".to_vec(), AggregationType::Sum);

    // Perform aggregations
    let agg_result = cf.aggregate(b"row1", None, &agg_set)?;

    // Combined filtering and aggregation
    let agg_result = cf.aggregate(b"row1", Some(&filter_set), &agg_set)?;

    Ok(())
}
```

## Creating Tables and Column Families

Tables in RedBase are directories on disk, and column families are subdirectories within a table directory.

```rust
// Open a table (creates the directory if it doesn't exist)
let mut table = Table::open("./data/my_table")?;

// Create a column family
if table.cf("default").is_none() {
    table.create_cf("default")?;
}

// Get a reference to the column family
let cf = table.cf("default").unwrap();
```

You can create multiple column families in a table:

```rust
table.create_cf("users")?;
table.create_cf("posts")?;

let users_cf = table.cf("users").unwrap();
let posts_cf = table.cf("posts").unwrap();
```

## Writing Data

Data in RedBase is organized by row key, column name, and timestamp. Each write operation automatically assigns a timestamp based on the current time.

### Single Column Put

The basic way to write data is using the `put` method, which writes a single column value:

```rust
// Write data to a column family
cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Doe".to_vec())?;
cf.put(b"user1".to_vec(), b"email".to_vec(), b"john@example.com".to_vec())?;
cf.put(b"user1".to_vec(), b"age".to_vec(), b"30".to_vec())?;

// Writing to the same row and column creates a new version
cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Smith".to_vec())?;
```

### Multi-Column Put

For efficiency, you can write multiple columns to the same row in a single operation using the `Put` object:

```rust
use RedBase::api::Put;

// Create a Put operation for a specific row
let mut put = Put::new(b"user1".to_vec());

// Add multiple columns to the Put operation
put.add_column(b"name".to_vec(), b"John Doe".to_vec())
   .add_column(b"email".to_vec(), b"john@example.com".to_vec())
   .add_column(b"age".to_vec(), b"30".to_vec());

// Execute the Put operation (writes all columns with the same timestamp)
cf.execute_put(put)?;
```

This is more efficient than calling `put` multiple times, especially when writing many columns to the same row, as all columns will share the same timestamp.

## Reading Data

RedBase provides several ways to read data:

### Get the Latest Value

```rust
// Get the latest value for a specific row and column
let name = cf.get(b"user1", b"name")?;
if let Some(value) = name {
    println!("Name: {}", String::from_utf8_lossy(&value));
}
```

### Get Multiple Versions

```rust
// Get multiple versions of a value (up to 10 versions)
let name_versions = cf.get_versions(b"user1", b"name", 10)?;
for (timestamp, value) in name_versions {
    println!("At {}: {}", timestamp, String::from_utf8_lossy(&value));
}
```

### Multi-Column Get

For more advanced read operations, you can use the `Get` object, which is similar to the HBase/Java Get API:

```rust
use RedBase::api::Get;

// Create a Get operation for a specific row
let mut get = Get::new(b"user1".to_vec());

// Set the maximum number of versions to retrieve (optional)
get.set_max_versions(3);

// Or set a time range to filter versions (optional)
get.set_time_range(start_time, end_time);

// Execute the Get operation to retrieve all columns for the row
let result = cf.execute_get(&get)?;

// Process the results
for (column, versions) in result {
    println!("Column: {}", String::from_utf8_lossy(&column));
    for (timestamp, value) in versions {
        println!("  {} -> {}", timestamp, String::from_utf8_lossy(&value));
    }
}

// Or retrieve a specific column
let versions = cf.execute_get_column(&get, b"name")?;
for (timestamp, value) in versions {
    println!("Name at {}: {}", timestamp, String::from_utf8_lossy(&value));
}
```

The `Get` object provides more control over the read operation, allowing you to:
- Retrieve multiple columns for a row in a single operation
- Specify the maximum number of versions to retrieve
- Filter versions by time range

### Multi-Column Get

For more advanced read operations, you can use the `Get` object, which is similar to the HBase/Java Get API:

```rust
use RedBase::api::Get;

// Create a Get operation for a specific row
let mut get = Get::new(b"user1".to_vec());

// Set the maximum number of versions to retrieve (optional)
get.set_max_versions(3);

// Or set a time range to filter versions (optional)
get.set_time_range(start_time, end_time);

// Execute the Get operation to retrieve all columns for the row
let result = cf.execute_get(&get)?;

// Process the results
for (column, versions) in result {
    println!("Column: {}", String::from_utf8_lossy(&column));
    for (timestamp, value) in versions {
        println!("  {} -> {}", timestamp, String::from_utf8_lossy(&value));
    }
}

// Or retrieve a specific column
let versions = cf.execute_get_column(&get, b"name")?;
for (timestamp, value) in versions {
    println!("Name at {}: {}", timestamp, String::from_utf8_lossy(&value));
}
```

The `Get` object provides more control over the read operation, allowing you to:
- Retrieve multiple columns for a row in a single operation
- Specify the maximum number of versions to retrieve
- Filter versions by time range

## Deleting Data

Deleting data in RedBase creates a tombstone marker:

```rust
// Delete a value (creates a tombstone with no TTL)
cf.delete(b"user1".to_vec(), b"age".to_vec())?;
```

You can also delete with a Time-To-Live (TTL):

```rust
// Delete with TTL (tombstone expires after 1 hour)
cf.delete_with_ttl(b"user1".to_vec(), b"email".to_vec(), Some(3600 * 1000))?;
```

After the TTL expires, the tombstone can be removed during compaction. Until then, it will hide any older versions of the data.

## Scanning Data

RedBase allows you to scan all columns for a specific row:

```rust
// Scan all columns for a row, getting up to 10 versions per column
let row_data = cf.scan_row_versions(b"user1", 10)?;
for (column, versions) in row_data {
    println!("Column: {}", String::from_utf8_lossy(&column));
    for (ts, value) in versions {
        println!("  {} -> {}", ts, String::from_utf8_lossy(&value));
    }
}
```

## Flushing and Compaction

RedBase uses a MemStore for in-memory storage before flushing to disk. By default, the MemStore is flushed to disk when it reaches 10,000 entries. You can manually flush the MemStore:

```rust
// Flush the MemStore to disk
cf.flush()?;
```

Compaction is the process of merging multiple SSTables and optionally removing old versions or expired tombstones. RedBase supports several compaction strategies:

```rust
// Default compaction (minor compaction)
cf.compact()?;

// Major compaction (merges all SSTables)
cf.major_compact()?;

// Compaction with version limits (keep only the 2 newest versions)
cf.compact_with_max_versions(2)?;

// Compaction with age limits (keep only data from the last hour)
cf.compact_with_max_age(3600 * 1000)?;
```

You can also use custom compaction options:

```rust
use RedBase::api::{CompactionOptions, CompactionType};

// Custom compaction options
let options = CompactionOptions {
    compaction_type: CompactionType::Major,
    max_versions: Some(3),
    max_age_ms: Some(24 * 3600 * 1000), // 1 day
    cleanup_tombstones: true,
};
cf.compact_with_options(options)?;
```

RedBase runs a background compaction thread every 60 seconds, but you can also trigger compaction manually as shown above.

## Advanced Features

### Multi-Version Concurrency Control

RedBase implements MVCC (Multi-Version Concurrency Control) which allows it to maintain multiple versions of data. Each write operation creates a new version with a timestamp, and read operations can retrieve specific versions.

```rust
// Write multiple versions
cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;
cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value2".to_vec())?;
cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value3".to_vec())?;

// Get all versions (up to 10)
let versions = cf.get_versions(b"row1", b"col1", 10)?;
for (ts, value) in versions {
    println!("Timestamp: {}, Value: {}", ts, String::from_utf8_lossy(&value));
}

// The latest version is returned by default
let latest = cf.get(b"row1", b"col1")?;
println!("Latest value: {}", String::from_utf8_lossy(&latest.unwrap()));
```

### Tombstones and TTL

When you delete data in RedBase, it creates a tombstone marker rather than immediately removing the data. Tombstones can have an optional Time-To-Live (TTL) after which they are eligible for removal during compaction.

```rust
// Delete with no TTL (tombstone never expires automatically)
cf.delete(b"row1".to_vec(), b"col1".to_vec())?;

// Delete with a TTL of 1 hour (3,600,000 milliseconds)
cf.delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000))?;

// After the TTL expires, the tombstone can be removed during compaction
// But until then, it will hide any older versions of the data
```

### Filtering

RedBase supports filtering data based on various predicates:

```rust
use RedBase::filter::{Filter, FilterSet};

// Simple filter: get a value that equals a specific value
let filter = Filter::Equal(b"John Doe".to_vec());
let result = cf.get_with_filter(b"user1", b"name", &filter)?;

// Contains filter: get a value that contains a substring
let filter = Filter::Contains(b"Smith".to_vec());
let result = cf.get_with_filter(b"user2", b"name", &filter)?;

// Create a filter set for scanning
let mut filter_set = FilterSet::new();
filter_set.add_column_filter(
    b"age".to_vec(),
    Filter::GreaterThan(b"25".to_vec())
);

// Scan with filter
let scan_result = cf.scan_with_filter(b"user1", b"user3", &filter_set)?;
for (row, columns) in scan_result {
    println!("Row: {}", String::from_utf8_lossy(&row));
    for (col, versions) in columns {
        for (ts, value) in versions {
            println!("  {} -> {} -> {}", 
                String::from_utf8_lossy(&col),
                ts,
                String::from_utf8_lossy(&value)
            );
        }
    }
}
```

Available filter types:
- `Equal`: Exact match
- `NotEqual`: Not an exact match
- `GreaterThan`: Greater than comparison
- `GreaterThanOrEqual`: Greater than or equal comparison
- `LessThan`: Less than comparison
- `LessThanOrEqual`: Less than or equal comparison
- `Contains`: Contains a substring
- `StartsWith`: Starts with a prefix
- `EndsWith`: Ends with a suffix
- `Regex`: Match using a regular expression pattern (requires UTF-8 values)
- `And`: Logical AND of multiple filters
- `Or`: Logical OR of multiple filters
- `Not`: Logical NOT of a filter

### Aggregation

RedBase supports aggregation operations on data:

```rust
use RedBase::aggregation::{AggregationType, AggregationSet};

// Create an aggregation set
let mut agg_set = AggregationSet::new();
agg_set.add_aggregation(b"value1".to_vec(), AggregationType::Count);
agg_set.add_aggregation(b"value2".to_vec(), AggregationType::Sum);
agg_set.add_aggregation(b"value3".to_vec(), AggregationType::Average);
agg_set.add_aggregation(b"value4".to_vec(), AggregationType::Min);
agg_set.add_aggregation(b"value5".to_vec(), AggregationType::Max);

// Perform aggregations
let agg_result = cf.aggregate(b"stats", None, &agg_set)?;
for (col, result) in agg_result {
    println!("  {} -> {}", String::from_utf8_lossy(&col), result.to_string());
}

// Combined filtering and aggregation
let mut filter_set = FilterSet::new();
filter_set.add_column_filter(
    b"cpu".to_vec(),
    Filter::GreaterThan(b"20".to_vec())
);

let mut agg_set = AggregationSet::new();
agg_set.add_aggregation(b"cpu".to_vec(), AggregationType::Average);

let agg_result = cf.aggregate(b"metrics", Some(&filter_set), &agg_set)?;
```

Available aggregation types:
- `Count`: Count the number of values
- `Sum`: Sum the values (must be numeric)
- `Average`: Calculate the average of the values (must be numeric)
- `Min`: Find the minimum value
- `Max`: Find the maximum value

## Advanced Client Features

RedBase provides several advanced client features that are similar to those found in HBase:

### Asynchronous API

RedBase provides an asynchronous API that allows you to interact with the database without blocking the current thread. This is particularly useful for applications that need to handle multiple concurrent requests.

```rust
use std::path::Path;
use RedBase::async_api::Table;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Open a table asynchronously
    let table = Table::open("./data/my_table").await?;

    // Create a column family
    table.create_cf("default").await?;

    // Get a reference to the column family
    let cf = table.cf("default").await?.unwrap();

    // Write data asynchronously
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await?;

    // Read data asynchronously
    let value = cf.get(b"row1", b"col1").await?;
    println!("Value: {:?}", value.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Delete data asynchronously
    cf.delete(b"row1".to_vec(), b"col1".to_vec()).await?;

    // Flush to disk asynchronously
    cf.flush().await?;

    // Run compaction asynchronously
    cf.compact().await?;

    Ok(())
}
```

### Batch Operations

Batch operations allow you to perform multiple operations in a single transaction, which is more efficient than performing them one by one.

```rust
use RedBase::api::Table;
use RedBase::batch::{Batch, SyncBatchExt};

fn main() -> std::io::Result<()> {
    let mut table = Table::open("./data/my_table")?;
    let cf = table.cf("default").unwrap();

    // Create a batch
    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

    // Execute the batch
    cf.execute_batch(&batch)?;

    // Create a batch with delete operations
    let mut batch = Batch::new();
    batch.delete(b"row1".to_vec(), b"col1".to_vec())
         .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

    // Execute the batch
    cf.execute_batch(&batch)?;

    Ok(())
}
```

### Connection Pooling [NOT READY]

Connection pooling allows you to efficiently reuse connections to the database, which is important for performance in multi-user scenarios.

```rust
use RedBase::pool::SyncConnectionPool;

fn main() -> std::io::Result<()> {
    // Create a connection pool with 10 connections
    let pool = SyncConnectionPool::new("./data/my_table", 10);

    // Get a connection from the pool
    let conn = pool.get()?;

    // Use the connection
    let cf = conn.table.cf("default").unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;

    // Return the connection to the pool
    pool.put(conn);

    // Get another connection from the pool
    let conn2 = pool.get()?;

    // The connection will have access to the same data
    let cf2 = conn2.table.cf("default").unwrap();
    let value = cf2.get(b"row1", b"col1")?;
    assert_eq!(value.unwrap(), b"value1");

    Ok(())
}
```

### REST Interface [NOT READY]

RedBase provides a REST API that allows you to interact with the database over HTTP. This is useful for web applications and microservices.

```rust
use RedBase::rest::{RestConfig, start_server};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create a REST server configuration
    let config = RestConfig {
        base_dir: "./data".into(),
        host: "127.0.0.1".into(),
        port: 8080,
        pool_size: 10,
    };

    // Start the REST server
    start_server(config).await
}
```

Once the REST server is running, you can interact with it using HTTP requests. For example:

```
POST /tables/my_table/cf/default/put
{
  "row": "row1",
  "column": "col1",
  "value": "value1"
}
```

## Examples

### User Profile Management

```rust
// Create a users column family
let mut table = Table::open("./data/my_app")?;
if table.cf("users").is_none() {
    table.create_cf("users")?;
}
let users_cf = table.cf("users").unwrap();

// Create a user profile
let user_id = b"user123".to_vec();
users_cf.put(user_id.clone(), b"name".to_vec(), b"John Doe".to_vec())?;
users_cf.put(user_id.clone(), b"email".to_vec(), b"john@example.com".to_vec())?;
users_cf.put(user_id.clone(), b"age".to_vec(), b"30".to_vec())?;

// Update a user profile
users_cf.put(user_id.clone(), b"name".to_vec(), b"John Smith".to_vec())?;

// Get user profile
let name = users_cf.get(&user_id, b"name")?;
let email = users_cf.get(&user_id, b"email")?;
let age = users_cf.get(&user_id, b"age")?;

println!("User: {}", String::from_utf8_lossy(&name.unwrap()));
println!("Email: {}", String::from_utf8_lossy(&email.unwrap()));
println!("Age: {}", String::from_utf8_lossy(&age.unwrap()));

// Get profile history
let name_history = users_cf.get_versions(&user_id, b"name", 10)?;
println!("Name history:");
for (ts, name) in name_history {
    println!("  {} -> {}", ts, String::from_utf8_lossy(&name));
}
```

### Time Series Data

```rust
// Create a metrics column family
let mut table = Table::open("./data/metrics")?;
if table.cf("cpu").is_none() {
    table.create_cf("cpu")?;
}
let cpu_cf = table.cf("cpu").unwrap();

// Record CPU metrics for different servers
for i in 1..=5 {
    let server_id = format!("server{}", i);
    cpu_cf.put(server_id.into_bytes(), b"usage".to_vec(), format!("{}", i * 10).into_bytes())?;
}

// Query with filtering
let mut filter_set = FilterSet::new();
filter_set.add_column_filter(
    b"usage".to_vec(),
    Filter::GreaterThan(b"20".to_vec())
);

// Scan with filter
let high_cpu_servers = cpu_cf.scan_with_filter(b"server1", b"server5", &filter_set)?;
println!("Servers with high CPU usage:");
for (server, columns) in high_cpu_servers {
    println!("Server: {}", String::from_utf8_lossy(&server));
    for (col, versions) in columns {
        for (_, value) in versions {
            println!("  {} -> {}", 
                String::from_utf8_lossy(&col),
                String::from_utf8_lossy(&value)
            );
        }
    }
}

// Aggregate CPU usage
let mut agg_set = AggregationSet::new();
agg_set.add_aggregation(b"usage".to_vec(), AggregationType::Average);

let avg_cpu = cpu_cf.aggregate_range(b"server1", b"server5", None, &agg_set)?;
println!("Average CPU usage across all servers:");
for (_, columns) in avg_cpu {
    for (col, result) in columns {
        println!("  {} -> {}", String::from_utf8_lossy(&col), result.to_string());
    }
}
```

## Testing

Run the test suite with:

```bash
cargo test
```

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

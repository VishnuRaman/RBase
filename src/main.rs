use std::path::Path;
use std::time::Duration;
use std::thread;
use RedBase::api::{Table, CompactionOptions, CompactionType, Put};

/// RedBase: An HBase-like database in Rust
/// 
/// Features:
/// - Tables and Column Families
/// - Multi-Version Concurrency Control (MVCC)
/// - Tombstone markers for deleted data with TTL
/// - Background compaction with various strategies
/// - Version filtering and cleanup
fn main() -> std::io::Result<()> {
    println!("RedBase: An HBase-like database in Rust");

    let mut table = Table::open("./data/example_table")?;
    if table.cf("default").is_none() {
        table.create_cf("default")?;
    }

    let cf = table.cf("default").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())?;
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value2".to_vec())?;
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value3".to_vec())?;

    let value = cf.get(b"row1", b"col1")?;
    println!("Latest value for row1:col1: {:?}", value.map(|v| String::from_utf8_lossy(&v).to_string()));

    let versions = cf.get_versions(b"row1", b"col1", 10)?;
    println!("Versions for row1:col1:");
    versions.iter().for_each(|(ts, value)| {
        println!("  {} -> {}", ts, String::from_utf8_lossy(value).to_string());
    });

    cf.delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000))?;

    let row_data = cf.scan_row_versions(b"row1", 10)?;
    println!("All columns for row1:");
    row_data.iter().for_each(|(col, versions)| {
        println!("  Column: {}", String::from_utf8_lossy(col).to_string());
        versions.iter().for_each(|(ts, value)| {
            println!("    {} -> {}", ts, String::from_utf8_lossy(value).to_string());
        });
    });

    cf.flush()?;

    cf.compact()?;
    println!("Ran minor compaction");

    cf.major_compact()?;
    println!("Ran major compaction");

    cf.compact_with_max_versions(2)?;
    println!("Ran compaction with max 2 versions");

    cf.compact_with_max_age(3600 * 1000)?;
    println!("Ran compaction with 1 hour age limit");

    let options = CompactionOptions {
        compaction_type: CompactionType::Major,
        max_versions: Some(3),
        max_age_ms: Some(24 * 3600 * 1000),
        cleanup_tombstones: true,
    };
    cf.compact_with_options(options)?;
    println!("Ran custom compaction");

    println!("Waiting for background compaction (60 seconds)...");
    thread::sleep(Duration::from_secs(5));

    println!("\n=== Advanced Filters ===");

    cf.put(b"user1".to_vec(), b"name".to_vec(), b"John Doe".to_vec())?;
    cf.put(b"user1".to_vec(), b"email".to_vec(), b"john@example.com".to_vec())?;
    cf.put(b"user1".to_vec(), b"age".to_vec(), b"30".to_vec())?;

    cf.put(b"user2".to_vec(), b"name".to_vec(), b"Jane Smith".to_vec())?;
    cf.put(b"user2".to_vec(), b"email".to_vec(), b"jane@example.com".to_vec())?;
    cf.put(b"user2".to_vec(), b"age".to_vec(), b"25".to_vec())?;

    cf.put(b"user3".to_vec(), b"name".to_vec(), b"Bob Johnson".to_vec())?;
    cf.put(b"user3".to_vec(), b"email".to_vec(), b"bob@example.com".to_vec())?;
    cf.put(b"user3".to_vec(), b"age".to_vec(), b"40".to_vec())?;

    use RedBase::filter::{Filter, FilterSet};

    let filter = Filter::Equal(b"John Doe".to_vec());
    let result = cf.get_with_filter(b"user1", b"name", &filter)?;
    println!("Equal filter result: {:?}", result.map(|v| String::from_utf8_lossy(&v).to_string()));

    let filter = Filter::Contains(b"Smith".to_vec());
    let result = cf.get_with_filter(b"user2", b"name", &filter)?;
    println!("Contains filter result: {:?}", result.map(|v| String::from_utf8_lossy(&v).to_string()));

    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"age".to_vec(),
        Filter::GreaterThan(b"25".to_vec())
    );

    println!("\nScanning rows with age > 25:");
    let scan_result = cf.scan_with_filter(b"user1", b"user3", &filter_set)?;
    scan_result.iter().for_each(|(row, columns)| {
        println!("Row: {}", String::from_utf8_lossy(row));
        columns.iter().for_each(|(col, versions)| {
            versions.iter().for_each(|(ts, value)| {
                println!("  {} -> {} -> {}", 
                    String::from_utf8_lossy(col),
                    ts,
                    String::from_utf8_lossy(value)
                );
            });
        });
    });

    // Demonstrate aggregations
    println!("\n=== Aggregations ===");

    // Add some numeric data
    cf.put(b"stats".to_vec(), b"value1".to_vec(), b"10".to_vec())?;
    cf.put(b"stats".to_vec(), b"value2".to_vec(), b"20".to_vec())?;
    cf.put(b"stats".to_vec(), b"value3".to_vec(), b"30".to_vec())?;
    cf.put(b"stats".to_vec(), b"value4".to_vec(), b"40".to_vec())?;
    cf.put(b"stats".to_vec(), b"value5".to_vec(), b"50".to_vec())?;

    // Create an aggregation set
    use RedBase::aggregation::{AggregationType, AggregationSet};

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"value1".to_vec(), AggregationType::Count);
    agg_set.add_aggregation(b"value2".to_vec(), AggregationType::Sum);
    agg_set.add_aggregation(b"value3".to_vec(), AggregationType::Average);
    agg_set.add_aggregation(b"value4".to_vec(), AggregationType::Min);
    agg_set.add_aggregation(b"value5".to_vec(), AggregationType::Max);

    // Perform aggregations
    let agg_result = cf.aggregate(b"stats", None, &agg_set)?;
    println!("Aggregation results:");
    agg_result.iter().for_each(|(col, result)| {
        println!("  {} -> {}", String::from_utf8_lossy(col), result.to_string());
    });

    // Combined filtering and aggregation
    println!("\n=== Filtered Aggregation ===");

    // Add multiple versions of data
    for i in 1..=5 {
        cf.put(b"metrics".to_vec(), b"cpu".to_vec(), format!("{}", i * 10).into_bytes())?;
        thread::sleep(Duration::from_millis(10));
    }

    // Create a filter set for values > 20
    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"cpu".to_vec(),
        Filter::GreaterThan(b"20".to_vec())
    );

    // Create an aggregation set for averaging
    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"cpu".to_vec(), AggregationType::Average);

    // Perform filtered aggregation
    let agg_result = cf.aggregate(b"metrics", Some(&filter_set), &agg_set)?;
    println!("Filtered aggregation results (cpu values > 20):");
    agg_result.iter().for_each(|(col, result)| {
        println!("  {} -> {}", String::from_utf8_lossy(col), result.to_string());
    });

    // Demonstrate the Java-like Put API
    println!("\n=== Java-like Put API ===");

    // Create a new Put object for row "user4"
    let mut put = Put::new(b"user4".to_vec());

    // Add multiple columns to the Put object
    put.add_column(b"name".to_vec(), b"Alice Brown".to_vec())
       .add_column(b"email".to_vec(), b"alice@example.com".to_vec())
       .add_column(b"age".to_vec(), b"35".to_vec())
       .add_column(b"city".to_vec(), b"New York".to_vec());

    // Execute the Put operation
    cf.execute_put(put)?;
    println!("Added user4 with multiple columns in a single Put operation");

    // Verify the data was written
    let row_data = cf.scan_row_versions(b"user4", 10)?;
    println!("All columns for user4:");
    row_data.iter().for_each(|(col, versions)| {
        println!("  Column: {}", String::from_utf8_lossy(col).to_string());
        versions.iter().for_each(|(ts, value)| {
            println!("    {} -> {}", ts, String::from_utf8_lossy(value).to_string());
        });
    });

    println!("\nRedBase example completed successfully!");
    Ok(())
}

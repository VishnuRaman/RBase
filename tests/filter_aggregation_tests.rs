use std::{
    collections::BTreeMap,
    path::PathBuf,
    thread,
    time::Duration,
};
use tempfile::tempdir;
use RedBase::api::{Table, ColumnFamily};
use RedBase::filter::{Filter, FilterSet, ColumnFilter};
use RedBase::aggregation::{AggregationType, AggregationSet, AggregationResult};

fn temp_table_dir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempdir().unwrap();
    let table_path = dir.path().to_path_buf();
    (dir, table_path)
}

#[test]
fn test_filter_equal() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).unwrap();

    let filter = Filter::Equal(b"value1".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"value1");

    let filter = Filter::Equal(b"value2".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_none());

    drop(dir);
}

#[test]
fn test_filter_contains() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"hello world".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"goodbye world".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"hello rust".to_vec()).unwrap();

    let filter = Filter::Contains(b"world".to_vec());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"hello world");

    let result = cf.get_with_filter(b"row1", b"col2", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"goodbye world");

    let result = cf.get_with_filter(b"row2", b"col1", &filter).unwrap();
    assert!(result.is_none());

    drop(dir);
}

#[test]
fn test_filter_set() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col2".to_vec(), b"value4".to_vec()).unwrap();

    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::Equal(b"value1".to_vec())
    );

    let result = cf.scan_row_with_filter(b"row1", &filter_set).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains_key(&b"col1".to_vec()));
    assert!(!result.contains_key(&b"col2".to_vec()));

    filter_set.add_column_filter(
        b"col2".to_vec(),
        Filter::Equal(b"value2".to_vec())
    );

    let result = cf.scan_row_with_filter(b"row1", &filter_set).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains_key(&b"col1".to_vec()));
    assert!(result.contains_key(&b"col2".to_vec()));

    drop(dir);
}

#[test]
fn test_aggregation_count() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    (1..=3).for_each(|i| {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    });

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Count);

    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 1);

    if let Some(AggregationResult::Count(count)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*count, 3);
    } else {
        panic!("Expected Count aggregation result");
    }

    drop(dir);
}

#[test]
fn test_aggregation_sum() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"20".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col3".to_vec(), b"30".to_vec()).unwrap();

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Sum);
    agg_set.add_aggregation(b"col2".to_vec(), AggregationType::Sum);
    agg_set.add_aggregation(b"col3".to_vec(), AggregationType::Sum);

    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 3);

    if let Some(AggregationResult::Sum(sum)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*sum, 10);
    } else {
        panic!("Expected Sum aggregation result for col1");
    }

    if let Some(AggregationResult::Sum(sum)) = result.get(&b"col2".to_vec()) {
        assert_eq!(*sum, 20);
    } else {
        panic!("Expected Sum aggregation result for col2");
    }

    if let Some(AggregationResult::Sum(sum)) = result.get(&b"col3".to_vec()) {
        assert_eq!(*sum, 30);
    } else {
        panic!("Expected Sum aggregation result for col3");
    }

    drop(dir);
}

#[test]
fn test_aggregation_average() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"20".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col3".to_vec(), b"30".to_vec()).unwrap();

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Average);
    agg_set.add_aggregation(b"col2".to_vec(), AggregationType::Average);
    agg_set.add_aggregation(b"col3".to_vec(), AggregationType::Average);

    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 3);

    if let Some(AggregationResult::Average(avg)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*avg, 10.0);
    } else {
        panic!("Expected Average aggregation result for col1");
    }

    if let Some(AggregationResult::Average(avg)) = result.get(&b"col2".to_vec()) {
        assert_eq!(*avg, 20.0);
    } else {
        panic!("Expected Average aggregation result for col2");
    }

    if let Some(AggregationResult::Average(avg)) = result.get(&b"col3".to_vec()) {
        assert_eq!(*avg, 30.0);
    } else {
        panic!("Expected Average aggregation result for col3");
    }

    drop(dir);
}

#[test]
fn test_aggregation_min_max() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col_apple".to_vec(), b"apple".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col_banana".to_vec(), b"banana".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col_cherry".to_vec(), b"cherry".to_vec()).unwrap();

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col_apple".to_vec(), AggregationType::Min);
    agg_set.add_aggregation(b"col_banana".to_vec(), AggregationType::Min);
    agg_set.add_aggregation(b"col_cherry".to_vec(), AggregationType::Min);

    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 3);

    if let Some(AggregationResult::Min(min)) = result.get(&b"col_apple".to_vec()) {
        assert_eq!(min, &b"apple".to_vec());
    } else {
        panic!("Expected Min aggregation result for col_apple");
    }

    if let Some(AggregationResult::Min(min)) = result.get(&b"col_banana".to_vec()) {
        assert_eq!(min, &b"banana".to_vec());
    } else {
        panic!("Expected Min aggregation result for col_banana");
    }

    if let Some(AggregationResult::Min(min)) = result.get(&b"col_cherry".to_vec()) {
        assert_eq!(min, &b"cherry".to_vec());
    } else {
        panic!("Expected Min aggregation result for col_cherry");
    }

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col_apple".to_vec(), AggregationType::Max);
    agg_set.add_aggregation(b"col_banana".to_vec(), AggregationType::Max);
    agg_set.add_aggregation(b"col_cherry".to_vec(), AggregationType::Max);

    let result = cf.aggregate(b"row1", None, &agg_set).unwrap();
    assert_eq!(result.len(), 3);

    if let Some(AggregationResult::Max(max)) = result.get(&b"col_apple".to_vec()) {
        assert_eq!(max, &b"apple".to_vec());
    } else {
        panic!("Expected Max aggregation result for col_apple");
    }

    if let Some(AggregationResult::Max(max)) = result.get(&b"col_banana".to_vec()) {
        assert_eq!(max, &b"banana".to_vec());
    } else {
        panic!("Expected Max aggregation result for col_banana");
    }

    if let Some(AggregationResult::Max(max)) = result.get(&b"col_cherry".to_vec()) {
        assert_eq!(max, &b"cherry".to_vec());
    } else {
        panic!("Expected Max aggregation result for col_cherry");
    }

    drop(dir);
}

#[test]
fn test_filter_regex() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"user123@example.com".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"user456@example.org".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"not-an-email".to_vec()).unwrap();
    cf.put(b"row2".to_vec(), b"col2".to_vec(), b"12345".to_vec()).unwrap();

    let filter = Filter::Regex(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$".to_string());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"user123@example.com");

    let result = cf.get_with_filter(b"row1", b"col2", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"user456@example.org");

    let result = cf.get_with_filter(b"row2", b"col1", &filter).unwrap();
    assert!(result.is_none());

    let filter = Filter::Regex(r"^\d+$".to_string());
    let result = cf.get_with_filter(b"row2", b"col2", &filter).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), b"12345");

    let filter = Filter::Regex(r"[unclosed-bracket".to_string());
    let result = cf.get_with_filter(b"row1", b"col1", &filter).unwrap();
    assert!(result.is_none());

    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::Regex(r"@example\.com$".to_string())
    );

    let result = cf.scan_with_filter(b"row1", b"row2", &filter_set).unwrap();

    assert!(result.contains_key(&b"row1".to_vec()));
    if let Some(columns) = result.get(&b"row1".to_vec()) {
        assert!(columns.contains_key(&b"col1".to_vec()));
        assert_eq!(columns.get(&b"col1".to_vec()).unwrap()[0].1, b"user123@example.com".to_vec());
    } else {
        panic!("Expected row1 to be in the result");
    }

    drop(dir); // Cleanup
}

fn test_filter_and_aggregation() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"20".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"30".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"40".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"50".to_vec()).unwrap();

    let mut filter_set = FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        Filter::GreaterThan(b"20".to_vec())
    );

    let mut agg_set = AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), AggregationType::Average);

    let result = cf.aggregate(b"row1", Some(&filter_set), &agg_set).unwrap();
    assert_eq!(result.len(), 1);

    if let Some(AggregationResult::Average(avg)) = result.get(&b"col1".to_vec()) {
        assert_eq!(*avg, 40.0); // Average of 30, 40, 50
    } else {
        panic!("Expected Average aggregation result");
    }

    drop(dir);
}

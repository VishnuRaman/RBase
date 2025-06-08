use std::{
    collections::BTreeMap,
    path::PathBuf,
    thread,
    time::Duration,
};
use tempfile::tempdir;
use RedBase::api::{Table, ColumnFamily, CompactionOptions, CompactionType, Get, Put};

fn temp_table_dir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempdir().unwrap();
    let table_path = dir.path().to_path_buf();
    (dir, table_path)
}

#[test]
fn test_table_open_empty() {
    let (dir, table_path) = temp_table_dir();

    let table = Table::open(&table_path).unwrap();

    assert!(table.cf("default").is_none());

    drop(dir);
}

#[test]
fn test_table_create_cf() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();

    table.create_cf("test_cf").unwrap();

    assert!(table.cf("test_cf").is_some());

    let result = table.create_cf("test_cf");
    assert!(result.is_err());

    drop(dir);
}

#[test]
fn test_table_cf() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();

    table.create_cf("test_cf").unwrap();

    let cf = table.cf("test_cf");
    assert!(cf.is_some());

    let cf = table.cf("nonexistent");
    assert!(cf.is_none());

    drop(dir);
}

#[test]
fn test_column_family_put_and_get() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();

    let value = cf.get(b"row1", b"col1").unwrap();
    assert!(value.is_some());
    assert_eq!(value.unwrap(), b"value1");

    let value = cf.get(b"row2", b"col1").unwrap();
    assert!(value.is_none());

    drop(dir);
}

#[test]
fn test_column_family_delete() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();

    let value = cf.get(b"row1", b"col1").unwrap();
    assert!(value.is_some());

    cf.delete(b"row1".to_vec(), b"col1".to_vec()).unwrap();

    let value = cf.get(b"row1", b"col1").unwrap();
    assert!(value.is_none());

    drop(dir);
}

#[test]
fn test_column_family_delete_with_ttl() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();

    cf.delete_with_ttl(b"row1".to_vec(), b"col1".to_vec(), Some(1000)).unwrap(); // 1 second TTL

    let value = cf.get(b"row1", b"col1").unwrap();
    assert!(value.is_none());
    
    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert!(versions.len() <= 1);

    drop(dir);
}

#[test]
fn test_column_family_get_versions() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=3 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();

    assert_eq!(versions.len(), 3);

    assert!(versions[0].0 > versions[1].0);
    assert!(versions[1].0 > versions[2].0);

    assert_eq!(String::from_utf8_lossy(&versions[0].1), "value3");
    assert_eq!(String::from_utf8_lossy(&versions[1].1), "value2");
    assert_eq!(String::from_utf8_lossy(&versions[2].1), "value1");

    let versions = cf.get_versions(b"row1", b"col1", 2).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(String::from_utf8_lossy(&versions[0].1), "value3");
    assert_eq!(String::from_utf8_lossy(&versions[1].1), "value2");

    drop(dir);
}

#[test]
fn test_column_family_scan_row_versions() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=3 {
        cf.put(
            b"row1".to_vec(), 
            format!("col{}", i).into_bytes(), 
            format!("value{}", i).into_bytes()
        ).unwrap();
    }

    for i in 1..=2 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("updated{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    let row_data = cf.scan_row_versions(b"row1", 10).unwrap();

    assert_eq!(row_data.len(), 3);

    let col1_versions = row_data.get(&b"col1".to_vec()).unwrap();
    assert!(col1_versions.len() >= 2);

    let col2_versions = row_data.get(&b"col2".to_vec()).unwrap();
    assert_eq!(col2_versions.len(), 1);

    let col3_versions = row_data.get(&b"col3".to_vec()).unwrap();
    assert_eq!(col3_versions.len(), 1);

    let row_data = cf.scan_row_versions(b"row1", 2).unwrap();
    let col1_versions = row_data.get(&b"col1".to_vec()).unwrap();
    assert_eq!(col1_versions.len(), 2);

    drop(dir);
}

#[test]
fn test_column_family_flush() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=5 {
        cf.put(
            format!("row{}", i).into_bytes(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();
    }

    cf.flush().unwrap();

    for i in 1..=5 {
        let row = format!("row{}", i).into_bytes();
        let value = cf.get(&row, b"col1").unwrap();
        assert!(value.is_some());
        assert_eq!(String::from_utf8_lossy(&value.unwrap()), format!("value{}", i));
    }

    drop(dir);
}

#[test]
fn test_column_family_compaction() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for batch in 1..=3 {
        for i in 1..=3 {
            cf.put(
                format!("row{}", i).into_bytes(), 
                b"col1".to_vec(), 
                format!("batch{}_value{}", batch, i).into_bytes()
            ).unwrap();
        }
        cf.flush().unwrap();
    }

    cf.compact().unwrap();

    for i in 1..=3 {
        let row = format!("row{}", i).into_bytes();
        let value = cf.get(&row, b"col1").unwrap();
        assert!(value.is_some());
    }

    cf.major_compact().unwrap();


    for i in 1..=3 {
        let row = format!("row{}", i).into_bytes();
        let value = cf.get(&row, b"col1").unwrap();
        assert!(value.is_some());
        
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(value_str.contains(&format!("value{}", i)));
    }

    drop(dir);
}

#[test]
fn test_column_family_version_compaction() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=5 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    cf.flush().unwrap();

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert_eq!(versions.len(), 5);

    let options = CompactionOptions {
        compaction_type: CompactionType::Major,
        max_versions: Some(2),
        max_age_ms: None,
        cleanup_tombstones: true,
    };
    cf.compact_with_options(options).unwrap();

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(String::from_utf8_lossy(&versions[0].1), "value5");
    assert_eq!(String::from_utf8_lossy(&versions[1].1), "value4");

    drop(dir);
}

#[test]
fn test_column_family_custom_compaction() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=5 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    cf.delete_with_ttl(b"row2".to_vec(), b"col1".to_vec(), Some(10000)).unwrap();

    cf.flush().unwrap();

    let options = CompactionOptions {
        compaction_type: CompactionType::Major,
        max_versions: Some(2),
        max_age_ms: None,
        cleanup_tombstones: false,
    };

    cf.compact_with_options(options).unwrap();

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(String::from_utf8_lossy(&versions[0].1), "value5");
    assert_eq!(String::from_utf8_lossy(&versions[1].1), "value4");

    let value = cf.get(b"row2", b"col1").unwrap();
    assert!(value.is_none());

    drop(dir);
}

#[test]
fn test_column_family_execute_put() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    let mut put = RedBase::api::Put::new(b"row1".to_vec());
    put.add_column(b"col1".to_vec(), b"value1".to_vec())
       .add_column(b"col2".to_vec(), b"value2".to_vec());

    cf.execute_put(put).unwrap();

    let value1 = cf.get(b"row1", b"col1").unwrap();
    let value2 = cf.get(b"row1", b"col2").unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");

    drop(dir);
}

#[test]
fn test_column_family_compact_with_max_versions() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=5 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    cf.flush().unwrap();

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert_eq!(versions.len(), 5);

    let mut options = CompactionOptions::default();
    options.compaction_type = CompactionType::Major;
    options.max_versions = Some(2);
    cf.compact_with_options(options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(String::from_utf8_lossy(&versions[0].1), "value5");
    assert_eq!(String::from_utf8_lossy(&versions[1].1), "value4");

    drop(dir);
}

#[test]
fn test_column_family_compact_with_max_age() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=5 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(200));
    }

    cf.flush().unwrap();

    cf.put(
        b"row1".to_vec(), 
        b"col1".to_vec(), 
        b"recent_value".to_vec()
    ).unwrap();

    thread::sleep(Duration::from_millis(300));

    let mut options = CompactionOptions::default();
    options.compaction_type = CompactionType::Major;
    options.max_age_ms = Some(200);
    cf.compact_with_options(options).unwrap();

    thread::sleep(Duration::from_millis(500));

    let versions = cf.get_versions(b"row1", b"col1", 10).unwrap();
    assert!(!versions.is_empty(), "Expected at least one version after compaction");

    if !versions.is_empty() {
        assert_eq!(String::from_utf8_lossy(&versions[0].1), "recent_value", 
                   "Expected the newest version to be recent_value");
    }

    drop(dir);
}


#[test]
fn test_column_family_aggregate_range() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"10".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"20".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.put(b"row3".to_vec(), b"col1".to_vec(), b"30".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.flush().unwrap();
    thread::sleep(Duration::from_millis(100));

    let mut agg_set = RedBase::aggregation::AggregationSet::new();
    agg_set.add_aggregation(b"col1".to_vec(), RedBase::aggregation::AggregationType::Sum);

    let result = cf.aggregate_range(b"row1", b"row3", None, &agg_set).unwrap();

    assert!(!result.is_empty(), "Expected at least one row in the result");

    if let Some(row1_result) = result.get(&b"row1".to_vec()) {
        assert!(row1_result.contains_key(&b"col1".to_vec()), 
                "Expected col1 in row1 result");

        if let Some(RedBase::aggregation::AggregationResult::Sum(sum)) = row1_result.get(&b"col1".to_vec()) {
            assert_eq!(*sum, 10, "Expected sum of 10 for row1/col1");
        } else {
            panic!("Expected Sum aggregation result for row1/col1");
        }
    }

    if let Some(row2_result) = result.get(&b"row2".to_vec()) {
        assert!(row2_result.contains_key(&b"col1".to_vec()), 
                "Expected col1 in row2 result");

        if let Some(RedBase::aggregation::AggregationResult::Sum(sum)) = row2_result.get(&b"col1".to_vec()) {
            assert_eq!(*sum, 20, "Expected sum of 20 for row2/col1");
        } else {
            panic!("Expected Sum aggregation result for row2/col1");
        }
    }

    assert!(result.contains_key(&b"row1".to_vec()), 
            "Expected row1 to be included in the result");
    assert!(result.contains_key(&b"row2".to_vec()), 
            "Expected row2 to be included in the result");

    drop(dir);
}

#[test]
fn test_column_family_scan_with_filter() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.put(b"row2".to_vec(), b"col2".to_vec(), b"other4".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.put(b"row3".to_vec(), b"col1".to_vec(), b"value5".to_vec()).unwrap();
    thread::sleep(Duration::from_millis(10));

    cf.flush().unwrap();
    thread::sleep(Duration::from_millis(100));

    let mut filter_set = RedBase::filter::FilterSet::new();
    filter_set.add_column_filter(
        b"col1".to_vec(),
        RedBase::filter::Filter::Contains(b"value".to_vec())
    );

    let result = cf.scan_with_filter(b"row1", b"row3", &filter_set).unwrap();

    assert!(!result.is_empty(), "Expected at least one row in the result");
    assert!(result.contains_key(&b"row1".to_vec()), "Expected row1 in the result");

    if let Some(row1_cols) = result.get(&b"row1".to_vec()) {
        assert!(row1_cols.contains_key(&b"col1".to_vec()), "Expected col1 in row1");

        if let Some(versions) = row1_cols.get(&b"col1".to_vec()) {
            assert!(!versions.is_empty(), "Expected at least one version for row1/col1");
            if !versions.is_empty() {
                assert_eq!(String::from_utf8_lossy(&versions[0].1), "value1", 
                           "Expected value1 for row1/col1");
            }
        }
    }

    if let Some(row2_cols) = result.get(&b"row2".to_vec()) {
        assert!(row2_cols.contains_key(&b"col1".to_vec()), "Expected col1 in row2");

        if let Some(versions) = row2_cols.get(&b"col1".to_vec()) {
            assert!(!versions.is_empty(), "Expected at least one version for row2/col1");
            if !versions.is_empty() {
                assert_eq!(String::from_utf8_lossy(&versions[0].1), "value3", 
                           "Expected value3 for row2/col1");
            }
        }
    }

    drop(dir);
}

#[test]
fn test_column_family_execute_get() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
    cf.put(b"row1".to_vec(), b"col3".to_vec(), b"value3".to_vec()).unwrap();

    let get = Get::new(b"row1".to_vec());

    let result = cf.execute_get(&get).unwrap();

    assert_eq!(result.len(), 3);
    assert!(result.contains_key(&b"col1".to_vec()));
    assert!(result.contains_key(&b"col2".to_vec()));
    assert!(result.contains_key(&b"col3".to_vec()));

    let col1_versions = result.get(&b"col1".to_vec()).unwrap();
    assert_eq!(col1_versions.len(), 1);
    assert_eq!(String::from_utf8_lossy(&col1_versions[0].1), "value1");

    let col2_versions = result.get(&b"col2".to_vec()).unwrap();
    assert_eq!(col2_versions.len(), 1);
    assert_eq!(String::from_utf8_lossy(&col2_versions[0].1), "value2");

    let col3_versions = result.get(&b"col3".to_vec()).unwrap();
    assert_eq!(col3_versions.len(), 1);
    assert_eq!(String::from_utf8_lossy(&col3_versions[0].1), "value3");

    drop(dir);
}

#[test]
fn test_column_family_execute_get_with_max_versions() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=3 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    let mut get = Get::new(b"row1".to_vec());
    get.set_max_versions(2);

    let result = cf.execute_get(&get).unwrap();

    assert_eq!(result.len(), 1);
    assert!(result.contains_key(&b"col1".to_vec()));

    let col1_versions = result.get(&b"col1".to_vec()).unwrap();
    assert_eq!(col1_versions.len(), 2);
    assert_eq!(String::from_utf8_lossy(&col1_versions[0].1), "value3");
    assert_eq!(String::from_utf8_lossy(&col1_versions[1].1), "value2");

    drop(dir);
}

#[test]
fn test_column_family_execute_get_with_time_range() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    let mut timestamps = Vec::new();
    for i in 1..=3 {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        timestamps.push(now);

        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(100));
    }

    let mut get = Get::new(b"row1".to_vec());
    get.set_time_range(timestamps[0], timestamps[1] + 50);

    let result = cf.execute_get(&get).unwrap();

    assert!(result.contains_key(&b"col1".to_vec()));

    let col1_versions = result.get(&b"col1".to_vec()).unwrap();
    assert!(col1_versions.len() >= 1 && col1_versions.len() <= 2);

    let found_value2 = col1_versions.iter().any(|(_, v)| {
        String::from_utf8_lossy(v) == "value2"
    });
    assert!(found_value2, "Should contain value2");

    drop(dir);
}

#[test]
fn test_column_family_execute_get_column() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    for i in 1..=3 {
        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(10));
    }

    let mut get = Get::new(b"row1".to_vec());
    get.set_max_versions(2);

    let versions = cf.execute_get_column(&get, b"col1").unwrap();

    assert_eq!(versions.len(), 2); // Should have 2 versions
    assert_eq!(String::from_utf8_lossy(&versions[0].1), "value3");
    assert_eq!(String::from_utf8_lossy(&versions[1].1), "value2");

    drop(dir);
}

#[test]
fn test_column_family_get_versions_with_time_range() {
    let (dir, table_path) = temp_table_dir();

    let mut table = Table::open(&table_path).unwrap();
    table.create_cf("test_cf").unwrap();
    let cf = table.cf("test_cf").unwrap();

    let mut timestamps = Vec::new();
    for i in 1..=3 {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        timestamps.push(now);

        cf.put(
            b"row1".to_vec(), 
            b"col1".to_vec(), 
            format!("value{}", i).into_bytes()
        ).unwrap();

        thread::sleep(Duration::from_millis(100));
    }

    let versions = cf.get_versions_with_time_range(
        b"row1", 
        b"col1", 
        10, 
        timestamps[0], 
        timestamps[1] + 50
    ).unwrap();

    assert!(versions.len() >= 1 && versions.len() <= 2);

    let found_value2 = versions.iter().any(|(_, v)| {
        String::from_utf8_lossy(v) == "value2"
    });
    assert!(found_value2, "Should contain value2");

    drop(dir);
}

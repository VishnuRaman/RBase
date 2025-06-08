use std::path::Path;
use tempfile::tempdir;

use RedBase::api::Table as SyncTable;
use RedBase::async_api::Table as AsyncTable;
use RedBase::batch::{Batch, SyncBatchExt, AsyncBatchExt};
use RedBase::pool::{ConnectionPool, SyncConnectionPool};

#[tokio::test]
async fn test_async_api() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    let table = AsyncTable::open(table_path).await.unwrap();

    table.create_cf("test_cf").await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let cf = table.cf("test_cf").await.unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await.unwrap();
    cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).await.unwrap();
    cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).await.unwrap();

    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    let value2 = cf.get(b"row1", b"col2").await.unwrap();
    let value3 = cf.get(b"row2", b"col1").await.unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");
    assert_eq!(value3.unwrap(), b"value3");

    cf.delete(b"row1".to_vec(), b"col1".to_vec()).await.unwrap();

    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    assert!(value1.is_none());

    cf.flush().await.unwrap();

    cf.compact().await.unwrap();
}

#[tokio::test]
async fn test_async_batch_operations() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    let table = AsyncTable::open(table_path).await.unwrap();

    table.create_cf("test_cf").await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let cf = table.cf("test_cf").await.unwrap();

    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

    cf.execute_batch(&batch).await.unwrap();

    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    let value2 = cf.get(b"row1", b"col2").await.unwrap();
    let value3 = cf.get(b"row2", b"col1").await.unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");
    assert_eq!(value3.unwrap(), b"value3");

    let mut batch = Batch::new();
    batch.delete(b"row1".to_vec(), b"col1".to_vec())
         .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

    cf.execute_batch(&batch).await.unwrap();

    let value1 = cf.get(b"row1", b"col1").await.unwrap();
    let value2 = cf.get(b"row1", b"col2").await.unwrap();

    assert!(value1.is_none());
    assert!(value2.is_none());
}

#[test]
fn test_sync_batch_operations() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    let mut table = SyncTable::open(table_path).unwrap();

    table.create_cf("test_cf").unwrap();

    let cf = table.cf("test_cf").unwrap();

    let mut batch = Batch::new();
    batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
         .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
         .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

    cf.execute_batch(&batch).unwrap();

    let value1 = cf.get(b"row1", b"col1").unwrap();
    let value2 = cf.get(b"row1", b"col2").unwrap();
    let value3 = cf.get(b"row2", b"col1").unwrap();

    assert_eq!(value1.unwrap(), b"value1");
    assert_eq!(value2.unwrap(), b"value2");
    assert_eq!(value3.unwrap(), b"value3");
}

#[tokio::test]
async fn test_connection_pool() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    let pool = ConnectionPool::new(table_path, 5);

    let conn = pool.get().await.unwrap();

    conn.table.create_cf("test_cf").await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let cf = conn.table.cf("test_cf").await.unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await.unwrap();

    let value = cf.get(b"row1", b"col1").await.unwrap();
    assert_eq!(value.unwrap(), b"value1");

    drop(conn);

    let conn2 = pool.get().await.unwrap();

    let cf2 = conn2.table.cf("test_cf").await.unwrap();

    let value2 = cf2.get(b"row1", b"col1").await.unwrap();
    assert_eq!(value2.unwrap(), b"value1");
}

#[test]
fn test_sync_connection_pool() {
    let dir = tempdir().unwrap();
    let table_path = dir.path();

    let pool = SyncConnectionPool::new(table_path, 5);

    let mut conn = pool.get().unwrap();

    conn.table.create_cf("test_cf").unwrap();

    let cf = conn.table.cf("test_cf").unwrap();

    cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();

    let value = cf.get(b"row1", b"col1").unwrap();
    assert_eq!(value.unwrap(), b"value1");

    pool.put(conn);

    let conn2 = pool.get().unwrap();

    let cf2 = conn2.table.cf("test_cf").unwrap();

    let value2 = cf2.get(b"row1", b"col1").unwrap();
    assert_eq!(value2.unwrap(), b"value1");
}

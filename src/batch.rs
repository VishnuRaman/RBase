use std::{
    collections::{VecDeque, BTreeMap, HashMap},
    io::Result as IoResult,
    sync::Arc,
};

use crate::api::{ColumnFamily as SyncColumnFamily, RowKey, Column, Timestamp, Get, Put};
use crate::async_api::ColumnFamily as AsyncColumnFamily;

/// A wrapper for Get that implements Debug and Clone
#[derive(Debug, Clone)]
pub struct BatchGet {
    row: RowKey,
    max_versions: Option<usize>,
    time_range: Option<(Timestamp, Timestamp)>,
}

impl BatchGet {
    pub fn new(row: RowKey) -> Self {
        Self {
            row,
            max_versions: None,
            time_range: None,
        }
    }

    pub fn set_max_versions(&mut self, max_versions: usize) -> &mut Self {
        self.max_versions = Some(max_versions);
        self
    }

    pub fn set_time_range(&mut self, start_time: Timestamp, end_time: Timestamp) -> &mut Self {
        self.time_range = Some((start_time, end_time));
        self
    }

    pub fn to_get(&self) -> Get {
        let mut get = Get::new(self.row.clone());
        if let Some(max_versions) = self.max_versions {
            get.set_max_versions(max_versions);
        }
        if let Some((start_time, end_time)) = self.time_range {
            get.set_time_range(start_time, end_time);
        }
        get
    }
}

/// A wrapper for Put that implements Debug and Clone
#[derive(Debug, Clone)]
pub struct BatchPut {
    row: RowKey,
    columns: HashMap<Column, Vec<u8>>,
}

impl BatchPut {
    pub fn new(row: RowKey) -> Self {
        Self {
            row,
            columns: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, column: Column, value: Vec<u8>) -> &mut Self {
        self.columns.insert(column, value);
        self
    }

    pub fn to_put(&self) -> Put {
        let mut put = Put::new(self.row.clone());
        for (column, value) in &self.columns {
            put.add_column(column.clone(), value.clone());
        }
        put
    }
}

/// Represents a single operation in a batch
#[derive(Debug, Clone)]
pub enum BatchOperation {
    Put(RowKey, Column, Vec<u8>),
    Delete(RowKey, Column),
    DeleteWithTTL(RowKey, Column, Option<u64>),
    GetRow(BatchGet),
    PutRow(BatchPut),
}

#[derive(Debug, Clone)]
pub struct Batch {
    operations: VecDeque<BatchOperation>,
}

impl Batch {
    pub fn new() -> Self {
        Self {
            operations: VecDeque::new(),
        }
    }

    pub fn put(&mut self, row: RowKey, column: Column, value: Vec<u8>) -> &mut Self {
        self.operations.push_back(BatchOperation::Put(row, column, value));
        self
    }

    pub fn delete(&mut self, row: RowKey, column: Column) -> &mut Self {
        self.operations.push_back(BatchOperation::Delete(row, column));
        self
    }

    pub fn delete_with_ttl(&mut self, row: RowKey, column: Column, ttl_ms: Option<u64>) -> &mut Self {
        self.operations.push_back(BatchOperation::DeleteWithTTL(row, column, ttl_ms));
        self
    }

    pub fn get_row(&mut self, row: RowKey) -> &mut Self {
        let batch_get = BatchGet::new(row);
        self.operations.push_back(BatchOperation::GetRow(batch_get));
        self
    }

    pub fn get_row_with_max_versions(&mut self, row: RowKey, max_versions: usize) -> &mut Self {
        let mut batch_get = BatchGet::new(row);
        batch_get.set_max_versions(max_versions);
        self.operations.push_back(BatchOperation::GetRow(batch_get));
        self
    }

    pub fn get_row_with_time_range(&mut self, row: RowKey, start_time: Timestamp, end_time: Timestamp) -> &mut Self {
        let mut batch_get = BatchGet::new(row);
        batch_get.set_time_range(start_time, end_time);
        self.operations.push_back(BatchOperation::GetRow(batch_get));
        self
    }

    pub fn put_row(&mut self, row: RowKey, columns: HashMap<Column, Vec<u8>>) -> &mut Self {
        let mut batch_put = BatchPut::new(row);
        for (column, value) in columns {
            batch_put.add_column(column, value);
        }
        self.operations.push_back(BatchOperation::PutRow(batch_put));
        self
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn clear(&mut self) {
        self.operations.clear();
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

pub trait SyncBatchExt {
    fn execute_batch(&self, batch: &Batch) -> IoResult<()>;
    fn execute_batch_with_results(&self, batch: &Batch) -> IoResult<Vec<BatchResult>>;
}

/// Result of a batch operation
#[derive(Debug)]
pub enum BatchResult {
    Success,
    RowData(BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>),
}

impl SyncBatchExt for SyncColumnFamily {
    fn execute_batch(&self, batch: &Batch) -> IoResult<()> {
        for op in &batch.operations {
            match op {
                BatchOperation::Put(row, column, value) => {
                    self.put(row.clone(), column.clone(), value.clone())?;
                }
                BatchOperation::Delete(row, column) => {
                    self.delete(row.clone(), column.clone())?;
                }
                BatchOperation::DeleteWithTTL(row, column, ttl_ms) => {
                    self.delete_with_ttl(row.clone(), column.clone(), *ttl_ms)?;
                }
                BatchOperation::GetRow(_) => {
                    // Get operations don't modify state, so skipped
                }
                BatchOperation::PutRow(batch_put) => {
                    let put = batch_put.to_put();
                    self.execute_put(put)?;
                }
            }
        }
        Ok(())
    }

    fn execute_batch_with_results(&self, batch: &Batch) -> IoResult<Vec<BatchResult>> {
        let mut results = Vec::new();

        for op in &batch.operations {
            match op {
                BatchOperation::Put(row, column, value) => {
                    self.put(row.clone(), column.clone(), value.clone())?;
                    results.push(BatchResult::Success);
                }
                BatchOperation::Delete(row, column) => {
                    self.delete(row.clone(), column.clone())?;
                    results.push(BatchResult::Success);
                }
                BatchOperation::DeleteWithTTL(row, column, ttl_ms) => {
                    self.delete_with_ttl(row.clone(), column.clone(), *ttl_ms)?;
                    results.push(BatchResult::Success);
                }
                BatchOperation::GetRow(batch_get) => {
                    let get = batch_get.to_get();
                    let row_data = self.execute_get(&get)?;
                    results.push(BatchResult::RowData(row_data));
                }
                BatchOperation::PutRow(batch_put) => {
                    let put = batch_put.to_put();
                    self.execute_put(put)?;
                    results.push(BatchResult::Success);
                }
            }
        }

        Ok(results)
    }
}

pub trait AsyncBatchExt {
    async fn execute_batch(&self, batch: &Batch) -> IoResult<()>;
    async fn execute_batch_with_results(&self, batch: &Batch) -> IoResult<Vec<BatchResult>>;
}

impl AsyncBatchExt for AsyncColumnFamily {
    async fn execute_batch(&self, batch: &Batch) -> IoResult<()> {
        for op in &batch.operations {
            match op {
                BatchOperation::Put(row, column, value) => {
                    self.put(row.clone(), column.clone(), value.clone()).await?;
                }
                BatchOperation::Delete(row, column) => {
                    self.delete(row.clone(), column.clone()).await?;
                }
                BatchOperation::DeleteWithTTL(row, column, ttl_ms) => {
                    self.delete_with_ttl(row.clone(), column.clone(), *ttl_ms).await?;
                }
                BatchOperation::GetRow(_) => {
                    // Get operations don't modify state, so skipped.
                }
                BatchOperation::PutRow(batch_put) => {
                    let put = batch_put.to_put();
                    self.execute_put(put).await?;
                }
            }
        }
        Ok(())
    }

    async fn execute_batch_with_results(&self, batch: &Batch) -> IoResult<Vec<BatchResult>> {
        let mut results = Vec::new();

        for op in &batch.operations {
            match op {
                BatchOperation::Put(row, column, value) => {
                    self.put(row.clone(), column.clone(), value.clone()).await?;
                    results.push(BatchResult::Success);
                }
                BatchOperation::Delete(row, column) => {
                    self.delete(row.clone(), column.clone()).await?;
                    results.push(BatchResult::Success);
                }
                BatchOperation::DeleteWithTTL(row, column, ttl_ms) => {
                    self.delete_with_ttl(row.clone(), column.clone(), *ttl_ms).await?;
                    results.push(BatchResult::Success);
                }
                BatchOperation::GetRow(batch_get) => {
                    let get = batch_get.to_get();
                    let row_data = self.execute_get(get).await?;
                    results.push(BatchResult::RowData(row_data));
                }
                BatchOperation::PutRow(batch_put) => {
                    let put = batch_put.to_put();
                    self.execute_put(put).await?;
                    results.push(BatchResult::Success);
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::api::Table;

    #[test]
    fn test_sync_batch_operations() {
        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let mut table = Table::open(table_path).unwrap();
        table.create_cf("test_cf").unwrap();
        let cf = table.cf("test_cf").unwrap();

        let mut batch = Batch::new();
        batch.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec())
             .put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec())
             .put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec());

        cf.execute_batch(&batch).unwrap();

        assert_eq!(cf.get(b"row1", b"col1").unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").unwrap().unwrap(), b"value2");
        assert_eq!(cf.get(b"row2", b"col1").unwrap().unwrap(), b"value3");

        let mut batch = Batch::new();
        batch.delete(b"row1".to_vec(), b"col1".to_vec())
             .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

        cf.execute_batch(&batch).unwrap();

        assert!(cf.get(b"row1", b"col1").unwrap().is_none());
        assert!(cf.get(b"row1", b"col2").unwrap().is_none());
        assert_eq!(cf.get(b"row2", b"col1").unwrap().unwrap(), b"value3");
    }

    #[test]
    fn test_sync_batch_get_row() {
        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let mut table = Table::open(table_path).unwrap();
        table.create_cf("test_cf").unwrap();
        let cf = table.cf("test_cf").unwrap();

        cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).unwrap();
        cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).unwrap();
        cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).unwrap();

        let mut batch = Batch::new();
        batch.get_row(b"row1".to_vec());

        let results = cf.execute_batch_with_results(&batch).unwrap();

        assert_eq!(results.len(), 1);
        match &results[0] {
            BatchResult::RowData(row_data) => {
                assert_eq!(row_data.len(), 2);
                assert!(row_data.contains_key(&b"col1".to_vec()));
                assert!(row_data.contains_key(&b"col2".to_vec()));

                let col1_versions = row_data.get(&b"col1".to_vec()).unwrap();
                assert_eq!(col1_versions.len(), 1);
                assert_eq!(String::from_utf8_lossy(&col1_versions[0].1), "value1");

                let col2_versions = row_data.get(&b"col2".to_vec()).unwrap();
                assert_eq!(col2_versions.len(), 1);
                assert_eq!(String::from_utf8_lossy(&col2_versions[0].1), "value2");
            },
            _ => panic!("Expected RowData result"),
        }
    }

    #[test]
    fn test_sync_batch_put_row() {
        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let mut table = Table::open(table_path).unwrap();
        table.create_cf("test_cf").unwrap();
        let cf = table.cf("test_cf").unwrap();

        let mut batch = Batch::new();
        let mut columns = HashMap::new();
        columns.insert(b"col1".to_vec(), b"value1".to_vec());
        columns.insert(b"col2".to_vec(), b"value2".to_vec());
        batch.put_row(b"row1".to_vec(), columns);

        cf.execute_batch(&batch).unwrap();

        assert_eq!(cf.get(b"row1", b"col1").unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").unwrap().unwrap(), b"value2");
    }

    #[tokio::test]
    async fn test_async_batch_operations() {
        use crate::async_api::Table as AsyncTable;

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

        assert_eq!(cf.get(b"row1", b"col1").await.unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").await.unwrap().unwrap(), b"value2");
        assert_eq!(cf.get(b"row2", b"col1").await.unwrap().unwrap(), b"value3");

        let mut batch = Batch::new();
        batch.delete(b"row1".to_vec(), b"col1".to_vec())
             .delete_with_ttl(b"row1".to_vec(), b"col2".to_vec(), Some(3600 * 1000));

        cf.execute_batch(&batch).await.unwrap();

        assert!(cf.get(b"row1", b"col1").await.unwrap().is_none());
        assert!(cf.get(b"row1", b"col2").await.unwrap().is_none());
        assert_eq!(cf.get(b"row2", b"col1").await.unwrap().unwrap(), b"value3");
    }

    #[tokio::test]
    async fn test_async_batch_get_row() {
        use crate::async_api::Table as AsyncTable;

        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let table = AsyncTable::open(table_path).await.unwrap();
        table.create_cf("test_cf").await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let cf = table.cf("test_cf").await.unwrap();

        cf.put(b"row1".to_vec(), b"col1".to_vec(), b"value1".to_vec()).await.unwrap();
        cf.put(b"row1".to_vec(), b"col2".to_vec(), b"value2".to_vec()).await.unwrap();
        cf.put(b"row2".to_vec(), b"col1".to_vec(), b"value3".to_vec()).await.unwrap();

        let mut batch = Batch::new();
        batch.get_row(b"row1".to_vec());

        let results = cf.execute_batch_with_results(&batch).await.unwrap();

        assert_eq!(results.len(), 1);
        match &results[0] {
            BatchResult::RowData(row_data) => {
                assert_eq!(row_data.len(), 2);
                assert!(row_data.contains_key(&b"col1".to_vec()));
                assert!(row_data.contains_key(&b"col2".to_vec()));

                let col1_versions = row_data.get(&b"col1".to_vec()).unwrap();
                assert_eq!(col1_versions.len(), 1);
                assert_eq!(String::from_utf8_lossy(&col1_versions[0].1), "value1");

                let col2_versions = row_data.get(&b"col2".to_vec()).unwrap();
                assert_eq!(col2_versions.len(), 1);
                assert_eq!(String::from_utf8_lossy(&col2_versions[0].1), "value2");
            },
            _ => panic!("Expected RowData result"),
        }
    }

    #[tokio::test]
    async fn test_async_batch_put_row() {
        use crate::async_api::Table as AsyncTable;

        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let table = AsyncTable::open(table_path).await.unwrap();
        table.create_cf("test_cf").await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let cf = table.cf("test_cf").await.unwrap();

        let mut batch = Batch::new();
        let mut columns = HashMap::new();
        columns.insert(b"col1".to_vec(), b"value1".to_vec());
        columns.insert(b"col2".to_vec(), b"value2".to_vec());
        batch.put_row(b"row1".to_vec(), columns);

        cf.execute_batch(&batch).await.unwrap();

        assert_eq!(cf.get(b"row1", b"col1").await.unwrap().unwrap(), b"value1");
        assert_eq!(cf.get(b"row1", b"col2").await.unwrap().unwrap(), b"value2");
    }
}

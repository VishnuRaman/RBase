use std::{
    collections::BTreeMap,
    io::Result as IoResult,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::task;
use futures::future::{self, Future};

use crate::api::{
    Table as SyncTable, 
    ColumnFamily as SyncColumnFamily,
    RowKey, Column, Timestamp, CellValue, CompactionOptions, Put, Get
};
use crate::aggregation::AggregationResult;
use crate::filter::{Filter, FilterSet};
use crate::aggregation::AggregationSet;

/// Async wrapper around the synchronous ColumnFamily
#[derive(Clone)]
pub struct ColumnFamily {
    inner: Arc<SyncColumnFamily>,
}

impl ColumnFamily {
    /// Create a new async ColumnFamily wrapper
    pub fn new(cf: SyncColumnFamily) -> Self {
        Self {
            inner: Arc::new(cf),
        }
    }

    /// Write a new versioned cell (row, column) = value with a fresh timestamp.
    pub async fn put(&self, row: RowKey, column: Column, value: Vec<u8>) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.put(row, column, value)
        }).await.unwrap()
    }

    /// Execute a Put operation with multiple columns.
    /// This is similar to the HBase/Java Put API.
    pub async fn execute_put(&self, put: Put) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.execute_put(put)
        }).await.unwrap()
    }

    /// Mark (row, column) as deleted by writing a tombstone at the current timestamp.
    pub async fn delete(&self, row: RowKey, column: Column) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.delete(row, column)
        }).await.unwrap()
    }

    /// Mark (row, column) as deleted by writing a tombstone with a specified TTL.
    pub async fn delete_with_ttl(&self, row: RowKey, column: Column, ttl_ms: Option<u64>) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.delete_with_ttl(row, column, ttl_ms)
        }).await.unwrap()
    }

    /// Get the single latest value for (row, column).
    pub async fn get(&self, row: &[u8], column: &[u8]) -> IoResult<Option<Vec<u8>>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        let column = column.to_vec();
        task::spawn_blocking(move || {
            cf.get(&row, &column)
        }).await.unwrap()
    }

    /// Return up to max_versions recent (timestamp, value) for (row, column).
    pub async fn get_versions(
        &self,
        row: &[u8],
        column: &[u8],
        max_versions: usize,
    ) -> IoResult<Vec<(Timestamp, Vec<u8>)>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        let column = column.to_vec();
        task::spawn_blocking(move || {
            cf.get_versions(&row, &column, max_versions)
        }).await.unwrap()
    }

    /// Return versions within a specific time range for (row, column).
    pub async fn get_versions_with_time_range(
        &self,
        row: &[u8],
        column: &[u8],
        max_versions: usize,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> IoResult<Vec<(Timestamp, Vec<u8>)>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        let column = column.to_vec();
        task::spawn_blocking(move || {
            cf.get_versions_with_time_range(&row, &column, max_versions, start_time, end_time)
        }).await.unwrap()
    }

    /// Execute a Get operation to retrieve data for a specific row.
    pub async fn execute_get(&self, get: Get) -> IoResult<BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.execute_get(&get)
        }).await.unwrap()
    }

    /// Execute a Get operation for a specific column.
    pub async fn execute_get_column(&self, get: Get, column: &[u8]) -> IoResult<Vec<(Timestamp, Vec<u8>)>> {
        let cf = self.inner.clone();
        let column = column.to_vec();
        task::spawn_blocking(move || {
            cf.execute_get_column(&get, &column)
        }).await.unwrap()
    }

    /// For each column under row, return up to max_versions_per_column recent (timestamp, value).
    pub async fn scan_row_versions(
        &self,
        row: &[u8],
        max_versions_per_column: usize,
    ) -> IoResult<BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        task::spawn_blocking(move || {
            cf.scan_row_versions(&row, max_versions_per_column)
        }).await.unwrap()
    }

    /// Flush the MemStore into a new SSTable file, then clear the MemStore + WAL.
    pub async fn flush(&self) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.flush()
        }).await.unwrap()
    }

    /// Compact all on-disk SSTables into one, preserving all versions (no dropping).
    pub async fn compact(&self) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.compact()
        }).await.unwrap()
    }

    /// Run a major compaction that merges all SSTables into one.
    pub async fn major_compact(&self) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.major_compact()
        }).await.unwrap()
    }

    /// Run a compaction with version cleanup, keeping only the specified number of versions.
    pub async fn compact_with_max_versions(&self, max_versions: usize) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.compact_with_max_versions(max_versions)
        }).await.unwrap()
    }

    /// Run a compaction with age-based cleanup, removing versions older than the specified age.
    pub async fn compact_with_max_age(&self, max_age_ms: u64) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.compact_with_max_age(max_age_ms)
        }).await.unwrap()
    }

    /// Get a value with a filter applied
    pub async fn get_with_filter(&self, row: &[u8], column: &[u8], filter: &Filter) -> IoResult<Option<Vec<u8>>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        let column = column.to_vec();
        let filter = filter.clone();
        task::spawn_blocking(move || {
            cf.get_with_filter(&row, &column, &filter)
        }).await.unwrap()
    }

    /// Scan a row with a filter set applied
    pub async fn scan_row_with_filter(
        &self,
        row: &[u8],
        filter_set: &FilterSet,
    ) -> IoResult<BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        let filter_set = filter_set.clone();
        task::spawn_blocking(move || {
            cf.scan_row_with_filter(&row, &filter_set)
        }).await.unwrap()
    }

    /// Scan multiple rows with a filter set applied
    pub async fn scan_with_filter(
        &self,
        start_row: &[u8],
        end_row: &[u8],
        filter_set: &FilterSet,
    ) -> IoResult<BTreeMap<RowKey, BTreeMap<Column, Vec<(Timestamp, Vec<u8>)>>>> {
        let cf = self.inner.clone();
        let start_row = start_row.to_vec();
        let end_row = end_row.to_vec();
        let filter_set = filter_set.clone();
        task::spawn_blocking(move || {
            cf.scan_with_filter(&start_row, &end_row, &filter_set)
        }).await.unwrap()
    }

    /// Perform aggregations on query results
    pub async fn aggregate(
        &self,
        row: &[u8],
        filter_set: Option<&FilterSet>,
        aggregation_set: &AggregationSet,
    ) -> IoResult<BTreeMap<Column, AggregationResult>> {
        let cf = self.inner.clone();
        let row = row.to_vec();
        let filter_set = filter_set.cloned();
        let aggregation_set = aggregation_set.clone();
        task::spawn_blocking(move || {
            cf.aggregate(&row, filter_set.as_ref(), &aggregation_set)
        }).await.unwrap()
    }

    /// Perform aggregations on multiple rows
    pub async fn aggregate_range(
        &self,
        start_row: &[u8],
        end_row: &[u8],
        filter_set: Option<&FilterSet>,
        aggregation_set: &AggregationSet,
    ) -> IoResult<BTreeMap<RowKey, BTreeMap<Column, AggregationResult>>> {
        let cf = self.inner.clone();
        let start_row = start_row.to_vec();
        let end_row = end_row.to_vec();
        let filter_set = filter_set.cloned();
        let aggregation_set = aggregation_set.clone();
        task::spawn_blocking(move || {
            cf.aggregate_range(&start_row, &end_row, filter_set.as_ref(), &aggregation_set)
        }).await.unwrap()
    }

    /// Compact SSTables with the specified options.
    pub async fn compact_with_options(&self, options: CompactionOptions) -> IoResult<()> {
        let cf = self.inner.clone();
        task::spawn_blocking(move || {
            cf.compact_with_options(options)
        }).await.unwrap()
    }
}

/// Async wrapper around the synchronous Table
#[derive(Clone)]
pub struct Table {
    path: PathBuf,
    inner: Arc<SyncTable>,
}

impl Table {
    /// Open (or create) a table directory asynchronously.
    pub async fn open(table_dir: impl AsRef<Path>) -> IoResult<Self> {
        let path = table_dir.as_ref().to_path_buf();
        let path_clone = path.clone();

        let inner = task::spawn_blocking(move || {
            SyncTable::open(path_clone)
        }).await.unwrap()?;

        Ok(Self {
            path,
            inner: Arc::new(inner),
        })
    }

    /// Create a new column family named cf_name asynchronously. Fails if it already exists.
    pub async fn create_cf(&self, cf_name: &str) -> IoResult<()> {
        let inner = self.inner.clone();
        let cf_name = cf_name.to_string();

        task::spawn_blocking(move || {
            let mut table = inner.as_ref().clone();
            table.create_cf(&cf_name)
        }).await.unwrap()
    }

    /// Retrieve a handle to an existing ColumnFamily (or None if it doesn't exist).
    /// If the column family doesn't exist but was created earlier in the same process,
    /// this method will attempt to find it by opening the table directory again.
    pub async fn cf(&self, cf_name: &str) -> Option<ColumnFamily> {
        let inner = self.inner.clone();
        let cf_name = cf_name.to_string();
        let path = self.path.clone();

        let sync_cf = task::spawn_blocking(move || {
            if let Some(cf) = inner.as_ref().clone().cf(&cf_name) {
                return Some(cf);
            }

            match SyncTable::open(&path) {
                Ok(fresh_table) => fresh_table.cf(&cf_name),
                Err(_) => None
            }
        }).await.unwrap();

        sync_cf.map(ColumnFamily::new)
    }
}

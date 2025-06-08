use bincode;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Result as IoResult, Seek, SeekFrom, Write},
    path::Path,
};
use crate::api::{CellValue, Entry, EntryKey, Timestamp};

/// A single WAL record: binary‐encoded Entry.
#[derive(Serialize, Deserialize, Debug)]
pub struct WalEntry(Entry);

/// MemStore holds an in‐memory BTreeMap<EntryKey, CellValue> plus an append‐only WAL file.
pub struct MemStore {
    map: BTreeMap<EntryKey, CellValue>,
    wal: File,
    wal_path: String,
}

impl MemStore {
    /// Open (or create) a WAL at wal_path and replay it to rebuild map.
    pub fn open(wal_path: impl AsRef<Path>) -> IoResult<Self> {
        let path_str = wal_path.as_ref().to_string_lossy().into_owned();
        let wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)?;
        let mut store = MemStore {
            map: BTreeMap::new(),
            wal,
            wal_path: path_str.clone(),
        };

        let mut reader = BufReader::new(store.wal.try_clone()?);
        loop {
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            let WalEntry(entry) = bincode::deserialize(&buf).unwrap();
            store.map.insert(entry.key, entry.value);
        }
        store.wal.seek(SeekFrom::End(0))?;
        Ok(store)
    }

    /// Number of entries in the in-memory map
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Append one Entry to both the WAL file (on disk) and map (in memory).
    pub fn append(&mut self, entry: Entry) -> IoResult<()> {
        let buf = bincode::serialize(&WalEntry(entry.clone())).unwrap();
        let len = (buf.len() as u32).to_be_bytes();
        self.wal.write_all(&len)?;
        self.wal.write_all(&buf)?;
        self.wal.flush()?;

        self.map.insert(entry.key, entry.value);
        Ok(())
    }

    /// Get the *latest* CellValue for (row, column) from in‐memory map (if any).
    pub fn get_full(&self, row: &[u8], column: &[u8]) -> Option<&CellValue> {
        let range_start = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: u64::MAX,
        };
        self.map
            .range(range_start..=range_end)
            .last()
            .map(|(_k, v)| v)
    }

    /// *MVCC helper*: return all versions (timestamp + CellValue) for (row, column), sorted descending by timestamp.
    pub fn get_versions_full(&self, row: &[u8], column: &[u8]) -> Vec<(Timestamp, CellValue)> {
        let range_start = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: row.to_vec(),
            column: column.to_vec(),
            timestamp: u64::MAX,
        };
        let mut versions: Vec<(Timestamp, CellValue)> = self.map
            .range(range_start..=range_end)
            .map(|(k, v)| (k.timestamp, v.clone()))
            .collect();

        versions.sort_by(|a, b| b.0.cmp(&a.0));
        versions
    }

    pub fn drain_all(&mut self) -> IoResult<Vec<Entry>> {
        let mut all: Vec<Entry> = self.map.iter()
            .map(|(k, v)| Entry {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();

        all.sort_by(|a, b| a.key.cmp(&b.key));
        self.map.clear();

        //drop(&self.wal);
        std::fs::remove_file(&self.wal_path)?;
        self.wal = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.wal_path)?;
        Ok(all)
    }

    /// For scanning: return all (EntryKey, CellValue) for a given row (in-memory).  
    /// Useful to merge with SSTables when doing versioned scans.
    pub fn scan_row_full(&self, row: &[u8]) -> Vec<(EntryKey, CellValue)> {
        let range_start = EntryKey {
            row: row.to_vec(),
            column: vec![],
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: row.to_vec(),
            column: vec![0xFF],
            timestamp: u64::MAX,
        };

        // Use filter_map to transform and filter the range iterator
        self.map.range(range_start..=range_end)
            .filter(|(k, _)| k.row == row)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Scan a range of rows and return all (EntryKey, CellValue) pairs.
    /// The range is inclusive of start_row and end_row.
    pub fn scan_range(&self, start_row: &[u8], end_row: &[u8]) -> Vec<(EntryKey, CellValue)> {
        let range_start = EntryKey {
            row: start_row.to_vec(),
            column: vec![],
            timestamp: 0,
        };
        let range_end = EntryKey {
            row: end_row.to_vec(),
            column: vec![0xFF],
            timestamp: u64::MAX,
        };

        // Use filter and map to transform the range iterator
        self.map.range(range_start..=range_end)
            .filter(|(k, _)| k.row.as_slice() >= start_row && k.row.as_slice() <= end_row)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get all unique row keys in a range.
    pub fn get_row_keys_in_range(&self, start_row: &[u8], end_row: &[u8]) -> Vec<Vec<u8>> {
        // Use fold to collect unique row keys into a BTreeSet
        let row_keys = self.scan_range(start_row, end_row)
            .into_iter()
            .fold(std::collections::BTreeSet::new(), |mut set, (k, _)| {
                set.insert(k.row);
                set
            });

        row_keys.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{CellValue, Entry, EntryKey};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn temp_wal_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        (dir, wal_path)
    }

    #[test]
    fn test_memstore_open_empty() {
        let (dir, wal_path) = temp_wal_path();
        let store = MemStore::open(&wal_path).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        drop(store);
        drop(dir);
    }

    #[test]
    fn test_memstore_append_and_get() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        let entry = Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 100,
            },
            value: CellValue::Put(b"value1".to_vec()),
        };
        store.append(entry).unwrap();

        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        let value = store.get_full(b"row1", b"col1");
        assert!(value.is_some());
        match value.unwrap() {
            CellValue::Put(data) => assert_eq!(data, b"value1"),
            _ => panic!("Expected Put value"),
        }

        let value = store.get_full(b"row2", b"col1");
        assert!(value.is_none());

        drop(store);
        drop(dir);
    }

    #[test]
    fn test_memstore_get_versions_full() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        for i in 1..=3 {
            let entry = Entry {
                key: EntryKey {
                    row: b"row1".to_vec(),
                    column: b"col1".to_vec(),
                    timestamp: i * 100,
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            };
            store.append(entry).unwrap();
        }

        let versions = store.get_versions_full(b"row1", b"col1");
        assert_eq!(versions.len(), 3);

        assert_eq!(versions[0].0, 300);
        assert_eq!(versions[1].0, 200);
        assert_eq!(versions[2].0, 100);

        match &versions[0].1 {
            CellValue::Put(data) => assert_eq!(data, b"value3"),
            _ => panic!("Expected Put value"),
        }

        drop(store);
        drop(dir);
    }

    #[test]
    fn test_memstore_drain_all() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        for i in 1..=3 {
            let entry = Entry {
                key: EntryKey {
                    row: format!("row{}", i).into_bytes(),
                    column: b"col1".to_vec(),
                    timestamp: 100,
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            };
            store.append(entry).unwrap();
        }

        assert_eq!(store.len(), 3);

        let entries = store.drain_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());

        assert_eq!(String::from_utf8_lossy(&entries[0].key.row), "row1");
        assert_eq!(String::from_utf8_lossy(&entries[1].key.row), "row2");
        assert_eq!(String::from_utf8_lossy(&entries[2].key.row), "row3");

        drop(store);
        drop(dir);
    }

    #[test]
    fn test_memstore_scan_row_full() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        let _ = (1..=3).map(|i| {
            let entry = Entry {
                key: EntryKey {
                    row: b"row1".to_vec(),
                    column: format!("col{}", i).into_bytes(),
                    timestamp: 100 + i as u64,
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            };
            store.append(entry).unwrap()
        }).collect::<Vec<_>>();

        let entry = Entry {
            key: EntryKey {
                row: b"row2".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 100,
            },
            value: CellValue::Put(b"other_value".to_vec()),
        };
        store.append(entry).unwrap();

        let results = store.scan_row_full(b"row1");
        assert_eq!(results.len(), 3);

        for (key, _) in &results {
            assert_eq!(key.row, b"row1");
        }

        let results = store.scan_row_full(b"row2");
        assert_eq!(results.len(), 1);

        let results = store.scan_row_full(b"row3");
        assert_eq!(results.len(), 0);

        drop(store);
        drop(dir);
    }

    #[test]
    fn test_memstore_wal_persistence() {
        let (dir, wal_path) = temp_wal_path();

        {
            let mut store = MemStore::open(&wal_path).unwrap();
            for i in 1..=3 {
                let entry = Entry {
                    key: EntryKey {
                        row: b"row1".to_vec(),
                        column: format!("col{}", i).into_bytes(),
                        timestamp: 100 + i as u64,
                    },
                    value: CellValue::Put(format!("value{}", i).into_bytes()),
                };
                store.append(entry).unwrap();
            }
            assert_eq!(store.len(), 3);
        }

        {
            let store = MemStore::open(&wal_path).unwrap();
            assert_eq!(store.len(), 3);

            let _ = (1..=3).map(|i| {
                let col = format!("col{}", i).into_bytes();
                let value = store.get_full(b"row1", &col);
                assert!(value.is_some());
                match value.unwrap() {
                    CellValue::Put(data) => assert_eq!(data, format!("value{}", i).as_bytes()),
                    _ => panic!("Expected Put value"),
                }
            }).collect::<Vec<_>>();
        }

        drop(dir);
    }

    #[test]
    fn test_memstore_tombstone() {
        let (dir, wal_path) = temp_wal_path();
        let mut store = MemStore::open(&wal_path).unwrap();

        let entry = Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 100,
            },
            value: CellValue::Put(b"value1".to_vec()),
        };
        store.append(entry).unwrap();

        let entry = Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 200,
            },
            value: CellValue::Delete(None),
        };
        store.append(entry).unwrap();

        let value = store.get_full(b"row1", b"col1");
        assert!(value.is_some());
        match value.unwrap() {
            CellValue::Delete(_) => {},
            _ => panic!("Expected Delete value"),
        }

        let versions = store.get_versions_full(b"row1", b"col1");
        assert_eq!(versions.len(), 2);

        match &versions[0].1 {
            CellValue::Delete(_) => assert_eq!(versions[0].0, 200),
            _ => panic!("Expected Delete value"),
        }

        match &versions[1].1 {
            CellValue::Put(data) => {
                assert_eq!(versions[1].0, 100);
                assert_eq!(data, b"value1");
            },
            _ => panic!("Expected Put value"),
        }

        drop(store);
        drop(dir);
    }
}

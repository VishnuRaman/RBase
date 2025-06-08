use crate::api::{Entry, EntryKey, CellValue, Column, Timestamp};
use bincode;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Result as IoResult, Write},
    path::Path,
};

/// An on-disk SSTable.
/// Format (all big-endian u32 for lengths):
///
/// 1) [u32: number_of_entries]
/// 2) For each entry:
///    a) [u32: length of serialized EntryKey]
///    b) [bytes: bincode(serialized EntryKey)]
///    c) [u32: length of serialized CellValue]
///    d) [bytes: bincode(serialized CellValue)]
pub struct SSTable;

impl SSTable {
    /// Create an SSTable at path from a sorted slice of Entry.
    pub fn create(path: impl AsRef<Path>, entries: &[Entry]) -> IoResult<()> {
        let f = File::create(path)?;
        let mut w = BufWriter::new(f);

        let count = (entries.len() as u32).to_be_bytes();
        w.write_all(&count)?;

        for entry in entries {
            let key_ser = bincode::serialize(&entry.key).unwrap();
            let key_len = (key_ser.len() as u32).to_be_bytes();
            w.write_all(&key_len)?;
            w.write_all(&key_ser)?;

            let val_ser = bincode::serialize(&entry.value).unwrap();
            let val_len = (val_ser.len() as u32).to_be_bytes();
            w.write_all(&val_len)?;
            w.write_all(&val_ser)?;
        }
        w.flush()?;
        Ok(())
    }
}

/// A reader for a single SSTable. For simplicity, we load all entries into memory on open().
#[derive(Clone)]
pub struct SSTableReader {
    entries: Vec<(EntryKey, CellValue)>,
}

impl SSTableReader {
    /// Open an SSTable file, read all entries (key + CellValue) into memory.
    pub fn open(path: impl AsRef<Path>) -> IoResult<Self> {
        let f = File::open(path)?;
        let mut r = BufReader::new(f);

        let mut buf4 = [0u8; 4];
        r.read_exact(&mut buf4)?;
        let count = u32::from_be_bytes(buf4) as usize;

        let entries = (0..count)
            .map(|_| -> IoResult<(EntryKey, CellValue)> {
                r.read_exact(&mut buf4)?;
                let key_len = u32::from_be_bytes(buf4) as usize;
                let mut key_buf = vec![0u8; key_len];
                r.read_exact(&mut key_buf)?;
                let key: EntryKey = bincode::deserialize(&key_buf).unwrap();

                r.read_exact(&mut buf4)?;
                let val_len = u32::from_be_bytes(buf4) as usize;
                let mut val_buf = vec![0u8; val_len];
                r.read_exact(&mut val_buf)?;
                let cell: CellValue = bincode::deserialize(&val_buf).unwrap();

                Ok((key, cell))
            })
            .collect::<IoResult<Vec<_>>>()?;
        Ok(SSTableReader { entries })
    }

    /// Look up the latest CellValue for (row, column) by scanning backwards.
    pub fn get_full(&mut self, row: &[u8], column: &[u8]) -> IoResult<Option<CellValue>> {
        for (key, cell) in self.entries.iter().rev() {
            if key.row.as_slice() == row && key.column.as_slice() == column {
                return Ok(Some(cell.clone()));
            }
        }
        Ok(None)
    }

    /// *MVCC helper*: return all versions (timestamp + CellValue) for (row, column), sorted descending by timestamp.
    pub fn get_versions_full(&mut self, row: &[u8], column: &[u8]) -> IoResult<Vec<(Timestamp, CellValue)>> {
        let mut versions = Vec::new();

        for (key, cell) in self.entries.iter() {
            if key.row.as_slice() == row && key.column.as_slice() == column {
                versions.push((key.timestamp, cell.clone()));
            }
        }

        versions.sort_by(|a, b| b.0.cmp(&a.0));

        Ok(versions)
    }

    /// Scan all entries for a given row, returning (column, timestamp, CellValue) tuples.
    pub fn scan_row_full(
        &mut self,
        row: &[u8],
    ) -> IoResult<impl Iterator<Item = (Column, Timestamp, CellValue)>> {
        let mut matches = Vec::new();
        for (key, cell) in self.entries.iter() {
            if key.row.as_slice() == row {
                matches.push((key.column.clone(), key.timestamp, cell.clone()));
            }
        }
        Ok(matches.into_iter())
    }

    /// *Return ALL (EntryKey, CellValue) pairs* from this SSTable.
    /// Used by the compaction routine.
    pub fn scan_all(&self) -> IoResult<Vec<(EntryKey, CellValue)>> {
        Ok(self.entries.clone())
    }

    /// Scan a range of rows and return all entries within that range.
    /// The range is inclusive of start_row and end_row.
    pub fn scan_range(&mut self, start_row: &[u8], end_row: &[u8]) -> IoResult<Vec<(EntryKey, CellValue)>> {
        let mut result = Vec::new();

        for (key, cell) in &self.entries {
            if key.row.as_slice() >= start_row && key.row.as_slice() <= end_row {
                result.push((key.clone(), cell.clone()));
            }
        }

        Ok(result)
    }

    /// Get all unique row keys in a range.
    pub fn get_row_keys_in_range(&mut self, start_row: &[u8], end_row: &[u8]) -> IoResult<Vec<Vec<u8>>> {
        let mut row_keys = std::collections::BTreeSet::new();

        for (key, _) in self.scan_range(start_row, end_row)? {
            row_keys.insert(key.row);
        }

        Ok(row_keys.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{CellValue, Entry, EntryKey};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn create_test_entries() -> Vec<Entry> {
        let mut entries = Vec::new();

        entries.extend((1..=3).map(|i| Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: format!("col{}", i).into_bytes(),
                timestamp: 100 + i as u64,
            },
            value: CellValue::Put(format!("value{}", i).into_bytes()),
        }));

        entries.push(Entry {
            key: EntryKey {
                row: b"row2".to_vec(),
                column: b"col1".to_vec(),
                timestamp: 200,
            },
            value: CellValue::Put(b"row2value".to_vec()),
        });

        entries.push(Entry {
            key: EntryKey {
                row: b"row1".to_vec(),
                column: b"col4".to_vec(),
                timestamp: 300,
            },
            value: CellValue::Delete(Some(3600 * 1000)), // 1 hour TTL
        });

        entries.sort_by(|a, b| a.key.cmp(&b.key));

        entries
    }

    #[test]
    fn test_sstable_create_and_read() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("test.sst");

        let entries = create_test_entries();

        SSTable::create(&sst_path, &entries).unwrap();

        assert!(sst_path.exists());

        let reader = SSTableReader::open(&sst_path).unwrap();

        assert_eq!(reader.entries.len(), entries.len());

        drop(reader);
        drop(dir);
    }

    #[test]
    fn test_sstable_reader_get_full() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("test.sst");

        let entries = create_test_entries();

        SSTable::create(&sst_path, &entries).unwrap();

        let mut reader = SSTableReader::open(&sst_path).unwrap();

        let result = reader.get_full(b"row1", b"col1").unwrap();
        assert!(result.is_some());
        match result.unwrap() {
            CellValue::Put(data) => assert_eq!(data, b"value1"),
            _ => panic!("Expected Put value"),
        }

        let result = reader.get_full(b"row3", b"col1").unwrap();
        assert!(result.is_none());

        let result = reader.get_full(b"row1", b"col4").unwrap();
        assert!(result.is_some());
        match result.unwrap() {
            CellValue::Delete(ttl) => {
                assert!(ttl.is_some());
                assert_eq!(ttl.unwrap(), 3600 * 1000);
            },
            _ => panic!("Expected Delete value"),
        }

        drop(reader);
        drop(dir);
    }

    #[test]
    fn test_sstable_reader_get_versions_full() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("test.sst");

        let mut entries = Vec::new();
        for i in 1..=3 {
            entries.push(Entry {
                key: EntryKey {
                    row: b"row1".to_vec(),
                    column: b"col1".to_vec(),
                    timestamp: i * 100,
                },
                value: CellValue::Put(format!("value{}", i).into_bytes()),
            });
        }

        SSTable::create(&sst_path, &entries).unwrap();

        let mut reader = SSTableReader::open(&sst_path).unwrap();

        let versions = reader.get_versions_full(b"row1", b"col1").unwrap();

        assert_eq!(versions.len(), 3);

        assert_eq!(versions[0].0, 300);
        assert_eq!(versions[1].0, 200);
        assert_eq!(versions[2].0, 100);

        match &versions[0].1 {
            CellValue::Put(data) => assert_eq!(data, b"value3"),
            _ => panic!("Expected Put value"),
        }

        drop(reader);
        drop(dir);
    }

    #[test]
    fn test_sstable_reader_scan_row_full() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("test.sst");

        let entries = create_test_entries();

        SSTable::create(&sst_path, &entries).unwrap();

        let mut reader = SSTableReader::open(&sst_path).unwrap();

        let results: Vec<_> = reader.scan_row_full(b"row1").unwrap().collect();

        assert_eq!(results.len(), 4);

        for (col, _, _) in &results {
            assert!(col.starts_with(b"col"));
        }

        let results: Vec<_> = reader.scan_row_full(b"row2").unwrap().collect();
        assert_eq!(results.len(), 1);

        let results: Vec<_> = reader.scan_row_full(b"row3").unwrap().collect();
        assert_eq!(results.len(), 0);

        drop(reader);
        drop(dir);
    }

    #[test]
    fn test_sstable_reader_scan_all() {
        let dir = tempdir().unwrap();
        let sst_path = dir.path().join("test.sst");

        let entries = create_test_entries();

        SSTable::create(&sst_path, &entries).unwrap();

        let reader = SSTableReader::open(&sst_path).unwrap();

        let all_entries = reader.scan_all().unwrap();

        assert_eq!(all_entries.len(), entries.len());

        for i in 0..entries.len() {
            assert_eq!(all_entries[i].0, entries[i].key);

            match (&all_entries[i].1, &entries[i].value) {
                (CellValue::Put(data1), CellValue::Put(data2)) => {
                    assert_eq!(data1, data2);
                },
                (CellValue::Delete(ttl1), CellValue::Delete(ttl2)) => {
                    assert_eq!(ttl1, ttl2);
                },
                _ => panic!("Mismatched CellValue types"),
            }
        }

        drop(reader);
        drop(dir);
    }
}

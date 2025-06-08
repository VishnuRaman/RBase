use std::{
    io::Result as IoResult,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use deadpool::managed::{Manager, Object, Pool, PoolError};
use async_trait::async_trait;

use crate::api::Table as SyncTable;
use crate::async_api::Table as AsyncTable;

/// A connection to a RedBase table
#[derive(Clone)]
pub struct Connection {
    /// The path to the table
    pub path: PathBuf,
    /// The async table handle
    pub table: AsyncTable,
}

/// A manager for RedBase connections
pub struct ConnectionManager {
    /// The base directory for tables
    base_dir: PathBuf,
}

impl ConnectionManager {
    /// Create a new connection manager with the given base directory
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl Manager for ConnectionManager {
    type Type = Connection;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Connection, Self::Error> {
        let table_path = self.base_dir.clone();
        let table = AsyncTable::open(&table_path).await?;

        Ok(Connection {
            path: table_path,
            table,
        })
    }

    async fn recycle(&self, conn: &mut Connection) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        match AsyncTable::open(&conn.path).await {
            Ok(_) => Ok(()),
            Err(e) => Err(deadpool::managed::RecycleError::Backend(e)),
        }
    }
}

/// A pool of RedBase connections
pub struct ConnectionPool {
    pool: Pool<ConnectionManager>,
}

impl ConnectionPool {
    /// Create a new connection pool with the given base directory and size
    pub fn new<P: AsRef<Path>>(base_dir: P, size: usize) -> Self {
        let manager = ConnectionManager::new(base_dir);
        let pool = Pool::builder(manager)
            .max_size(size)
            .build()
            .expect("Failed to create connection pool");

        Self { pool }
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> Result<Object<ConnectionManager>, PoolError<std::io::Error>> {
        self.pool.get().await
    }
}

/// A synchronous connection to a RedBase table
pub struct SyncConnection {
    /// The path to the table
    pub path: PathBuf,
    /// The sync table handle
    pub table: SyncTable,
}

/// A synchronous manager for RedBase connections
pub struct SyncConnectionManager {
    /// The base directory for tables
    base_dir: PathBuf,
    /// Lock to ensure thread safety
    lock: Arc<Mutex<()>>,
}

impl SyncConnectionManager {
    /// Create a new synchronous connection manager with the given base directory
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Create a new connection
    pub fn create(&self) -> IoResult<SyncConnection> {
        let _guard = self.lock.lock().unwrap();
        let table_path = self.base_dir.clone();
        let table = SyncTable::open(&table_path)?;

        Ok(SyncConnection {
            path: table_path,
            table,
        })
    }

    /// Check if a connection is still valid
    pub fn recycle(&self, conn: &mut SyncConnection) -> IoResult<()> {
        match SyncTable::open(&conn.path) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// A simple synchronous connection pool
pub struct SyncConnectionPool {
    manager: SyncConnectionManager,
    connections: Arc<Mutex<Vec<SyncConnection>>>,
    max_size: usize,
}

impl SyncConnectionPool {
    /// Create a new synchronous connection pool with the given base directory and size
    pub fn new<P: AsRef<Path>>(base_dir: P, size: usize) -> Self {
        let manager = SyncConnectionManager::new(base_dir);

        Self {
            manager,
            connections: Arc::new(Mutex::new(Vec::with_capacity(size))),
            max_size: size,
        }
    }

    /// Get a connection from the pool
    pub fn get(&self) -> IoResult<SyncConnection> {
        let mut connections = self.connections.lock().unwrap();

        if let Some(conn) = connections.pop() {
            if self.manager.recycle(&mut SyncConnection { 
                path: conn.path.clone(), 
                table: conn.table.clone() 
            }).is_ok() {
                return Ok(conn);
            }
        }

        self.manager.create()
    }

    /// Return a connection to the pool
    pub fn put(&self, conn: SyncConnection) {
        let mut connections = self.connections.lock().unwrap();

        if connections.len() < self.max_size {
            connections.push(conn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_async_connection_pool() {
        let dir = tempdir().unwrap();
        let table_path = dir.path();

        let pool = ConnectionPool::new(table_path, 5);

        let conn1 = pool.get().await.unwrap();

        drop(conn1);

        let _conn2 = pool.get().await.unwrap();

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
}

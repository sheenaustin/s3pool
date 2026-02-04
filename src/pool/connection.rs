//! Connection pooling with HTTP/2 support and adaptive scaling
//!
//! This module provides per-backend HTTP connection pools with:
//! - HTTP/2 multiplexing for efficient connection reuse
//! - Adaptive pool sizing based on load
//! - Connection health monitoring
//! - Automatic cleanup of idle connections

use hyper::client::conn::http2;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Unique identifier for a backend
pub type BackendId = String;

/// Error types for connection pool operations
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("Failed to connect to backend: {0}")]
    ConnectionFailed(String),

    #[error("Pool is exhausted for backend: {0}")]
    PoolExhausted(String),

    #[error("Connection is unhealthy")]
    UnhealthyConnection,

    #[error("Backend not found: {0}")]
    BackendNotFound(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),
}

/// Configuration for connection pool behavior
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections per backend
    pub max_connections: usize,

    /// Minimum number of connections to keep warm
    pub min_connections: usize,

    /// Maximum idle time before closing a connection
    pub max_idle_time: Duration,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Keep-alive timeout
    pub keepalive_timeout: Duration,

    /// Enable HTTP/2
    pub http2_enabled: bool,

    /// Maximum concurrent streams per HTTP/2 connection
    pub max_http2_streams: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            min_connections: 10,
            max_idle_time: Duration::from_secs(90),
            connect_timeout: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(90),
            http2_enabled: true,
            max_http2_streams: 100,
        }
    }
}

/// Statistics for a connection pool
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total connections created
    pub total_created: u64,

    /// Total connections reused
    pub total_reused: u64,

    /// Active connections
    pub active_connections: usize,

    /// Idle connections
    pub idle_connections: usize,

    /// Failed connection attempts
    pub failed_attempts: u64,
}

/// A pooled connection wrapper
pub struct PooledConnection {
    /// The underlying HTTP/2 sender
    sender: http2::SendRequest<hyper::body::Incoming>,

    /// Last used timestamp
    last_used: Instant,

    /// Number of times this connection has been used
    use_count: u64,

    /// Connection creation time
    created_at: Instant,
}

impl PooledConnection {
    fn new(sender: http2::SendRequest<hyper::body::Incoming>) -> Self {
        let now = Instant::now();
        Self {
            sender,
            last_used: now,
            created_at: now,
            use_count: 0,
        }
    }

    /// Check if the connection is still healthy
    fn is_healthy(&self, max_idle: Duration) -> bool {
        // Check if connection is ready and not idle for too long
        self.sender.is_ready() && self.last_used.elapsed() < max_idle
    }

    /// Mark connection as used
    fn mark_used(&mut self) {
        self.last_used = Instant::now();
        self.use_count += 1;
    }

    /// Get the HTTP/2 sender
    pub fn sender(&mut self) -> &mut http2::SendRequest<hyper::body::Incoming> {
        self.mark_used();
        &mut self.sender
    }
}

/// Per-backend connection pool
struct BackendPool {
    /// Backend URL
    backend_url: String,

    /// Available connections
    connections: Vec<PooledConnection>,

    /// Pool configuration
    config: PoolConfig,

    /// Pool statistics
    stats: PoolStats,

    /// Last time we cleaned up idle connections
    last_cleanup: Instant,
}

impl BackendPool {
    fn new(backend_url: String, config: PoolConfig) -> Self {
        Self {
            backend_url,
            connections: Vec::with_capacity(config.max_connections),
            config,
            stats: PoolStats::default(),
            last_cleanup: Instant::now(),
        }
    }

    /// Get a connection from the pool or create a new one
    async fn get_connection(&mut self) -> Result<http2::SendRequest<hyper::body::Incoming>, PoolError> {
        // Try to reuse an existing healthy connection
        while let Some(mut conn) = self.connections.pop() {
            if conn.is_healthy(self.config.max_idle_time) {
                self.stats.total_reused += 1;
                self.stats.active_connections += 1;
                debug!(
                    backend = %self.backend_url,
                    use_count = conn.use_count,
                    age_secs = conn.created_at.elapsed().as_secs(),
                    "Reusing connection"
                );
                return Ok(conn.sender);
            } else {
                debug!(
                    backend = %self.backend_url,
                    "Discarding unhealthy connection"
                );
            }
        }

        // No healthy connections available, create a new one
        self.create_connection().await
    }

    /// Create a new connection to the backend
    async fn create_connection(&mut self) -> Result<http2::SendRequest<hyper::body::Incoming>, PoolError> {
        let url = self.backend_url.parse::<hyper::Uri>()
            .map_err(|e| PoolError::ConnectionFailed(e.to_string()))?;

        let host = url.host()
            .ok_or_else(|| PoolError::ConnectionFailed("No host in URL".to_string()))?;
        let port = url.port_u16().unwrap_or(80);

        debug!(
            backend = %self.backend_url,
            host = %host,
            port = %port,
            "Creating new connection"
        );

        // Connect with timeout
        let addr = format!("{}:{}", host, port);
        let stream = tokio::time::timeout(
            self.config.connect_timeout,
            TcpStream::connect(&addr)
        )
        .await
        .map_err(|_| PoolError::ConnectionFailed("Connection timeout".to_string()))?
        .map_err(|e| PoolError::ConnectionFailed(e.to_string()))?;

        // Configure TCP keep-alive
        let socket = socket2::Socket::from(stream.into_std()?);
        socket.set_keepalive(true)?;
        let stream = TcpStream::from_std(socket.into())?;

        // Build HTTP/2 connection
        let (sender, conn) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .map_err(|e| {
                self.stats.failed_attempts += 1;
                PoolError::ConnectionFailed(e.to_string())
            })?;

        // Spawn connection driver task
        let backend_url = self.backend_url.clone();
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                warn!(backend = %backend_url, error = %e, "HTTP/2 connection error");
            }
        });

        self.stats.total_created += 1;
        self.stats.active_connections += 1;

        info!(
            backend = %self.backend_url,
            total_created = self.stats.total_created,
            "Created new connection"
        );

        Ok(sender)
    }

    /// Return a connection to the pool
    fn return_connection(&mut self, sender: http2::SendRequest<hyper::body::Incoming>) {
        self.stats.active_connections = self.stats.active_connections.saturating_sub(1);

        // Only keep the connection if we're below max size
        if self.connections.len() < self.config.max_connections {
            self.connections.push(PooledConnection::new(sender));
            self.stats.idle_connections = self.connections.len();
        }
    }

    /// Clean up idle and unhealthy connections
    fn cleanup(&mut self) {
        let before_count = self.connections.len();

        // Remove unhealthy connections
        self.connections.retain(|conn| conn.is_healthy(self.config.max_idle_time));

        // Keep at least min_connections, but remove excess idle connections
        if self.connections.len() > self.config.min_connections {
            let target_size = self.config.min_connections.max(self.connections.len() / 2);
            self.connections.truncate(target_size);
        }

        let removed = before_count - self.connections.len();
        if removed > 0 {
            debug!(
                backend = %self.backend_url,
                removed = removed,
                remaining = self.connections.len(),
                "Cleaned up idle connections"
            );
        }

        self.stats.idle_connections = self.connections.len();
        self.last_cleanup = Instant::now();
    }

    /// Get pool statistics
    fn stats(&self) -> PoolStats {
        let mut stats = self.stats.clone();
        stats.idle_connections = self.connections.len();
        stats
    }
}

/// Connection pool manager for all backends
pub struct ConnectionPool {
    /// Per-backend connection pools
    pools: Arc<RwLock<HashMap<BackendId, BackendPool>>>,

    /// Pool configuration
    config: PoolConfig,

    /// Cleanup interval
    cleanup_interval: Duration,
}

impl ConnectionPool {
    /// Create a new connection pool manager
    pub fn new(config: PoolConfig) -> Self {
        let pool = Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            config,
            cleanup_interval: Duration::from_secs(60),
        };

        // Start background cleanup task
        pool.start_cleanup_task();

        pool
    }

    /// Register a backend with the pool
    pub async fn register_backend(&self, backend_id: BackendId, backend_url: String) {
        let mut pools = self.pools.write().await;
        if !pools.contains_key(&backend_id) {
            info!(
                backend_id = %backend_id,
                backend_url = %backend_url,
                "Registering backend"
            );
            pools.insert(
                backend_id.clone(),
                BackendPool::new(backend_url, self.config.clone())
            );
        }
    }

    /// Get a connection for a specific backend
    pub async fn get_connection(
        &self,
        backend_id: &BackendId
    ) -> Result<http2::SendRequest<hyper::body::Incoming>, PoolError> {
        let mut pools = self.pools.write().await;

        let pool = pools.get_mut(backend_id)
            .ok_or_else(|| PoolError::BackendNotFound(backend_id.clone()))?;

        pool.get_connection().await
    }

    /// Return a connection to the pool
    pub async fn return_connection(
        &self,
        backend_id: &BackendId,
        sender: http2::SendRequest<hyper::body::Incoming>
    ) {
        let mut pools = self.pools.write().await;

        if let Some(pool) = pools.get_mut(backend_id) {
            pool.return_connection(sender);
        }
    }

    /// Get statistics for a specific backend pool
    pub async fn get_stats(&self, backend_id: &BackendId) -> Option<PoolStats> {
        let pools = self.pools.read().await;
        pools.get(backend_id).map(|pool| pool.stats())
    }

    /// Get statistics for all backend pools
    pub async fn get_all_stats(&self) -> HashMap<BackendId, PoolStats> {
        let pools = self.pools.read().await;
        pools.iter()
            .map(|(id, pool)| (id.clone(), pool.stats()))
            .collect()
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let pools = Arc::clone(&self.pools);
        let interval = self.cleanup_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;

                let mut pools_lock = pools.write().await;
                for (backend_id, pool) in pools_lock.iter_mut() {
                    if pool.last_cleanup.elapsed() >= interval {
                        debug!(backend_id = %backend_id, "Running pool cleanup");
                        pool.cleanup();
                    }
                }
            }
        });
    }

    /// Manually trigger cleanup for all pools
    pub async fn cleanup_all(&self) {
        let mut pools = self.pools.write().await;
        for (backend_id, pool) in pools.iter_mut() {
            debug!(backend_id = %backend_id, "Manual cleanup triggered");
            pool.cleanup();
        }
    }

    /// Remove a backend from the pool
    pub async fn remove_backend(&self, backend_id: &BackendId) {
        let mut pools = self.pools.write().await;
        if pools.remove(backend_id).is_some() {
            info!(backend_id = %backend_id, "Removed backend from pool");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_creation() {
        let config = PoolConfig::default();
        let pool = ConnectionPool::new(config);

        // Register a backend
        pool.register_backend(
            "test-backend".to_string(),
            "http://localhost:9000".to_string()
        ).await;

        // Verify stats
        let stats = pool.get_stats(&"test-backend".to_string()).await;
        assert!(stats.is_some());
    }

    #[test]
    fn test_pool_config_defaults() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.min_connections, 10);
        assert!(config.http2_enabled);
    }
}

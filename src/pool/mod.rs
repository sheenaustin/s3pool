//! Connection pooling and circuit breaker module
//!
//! This module provides:
//! - HTTP/2 connection pooling with adaptive scaling
//! - Circuit breaker pattern for fault tolerance
//! - Per-backend connection management
//! - Automatic failure detection and recovery

pub mod circuit;
pub mod connection;

pub use circuit::{CircuitBreaker, CircuitBreakerConfig, CircuitError, CircuitState, CircuitStats};
pub use connection::{BackendId, ConnectionPool, PoolConfig, PoolError, PoolStats};

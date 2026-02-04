//! Load balancer module for s3pool
//!
//! This module provides load balancing and health checking functionality for S3 backends.
//!
//! # Components
//!
//! - [`Backend`]: Represents a single S3 backend with health and connection tracking
//! - [`LoadBalancer`]: Distributes requests across backends using various algorithms
//! - [`HealthChecker`]: Performs active and passive health checks on backends
//!
//! # Load Balancing Algorithms
//!
//! - **Round-robin**: Simple sequential distribution
//! - **Power-of-two**: Pick 2 random backends, choose the one with fewer connections
//! - **Least-connections**: Always pick the backend with the fewest active connections
//!
//! # Health Checking
//!
//! The health checker supports two modes:
//!
//! - **Active**: Periodic HTTP GET requests to health endpoints (e.g., `/minio/health/ready`)
//! - **Passive**: Track success/failure of actual requests
//!
//! Health scores range from 0-100, with backends considered healthy if score > 30.
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use s3pool::lb::{Backend, LoadBalancer, Algorithm, HealthChecker, HealthCheckConfig};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create backends
//! let backends = vec![
//!     Backend::new("http://s3-1.example.com:9000".to_string()),
//!     Backend::new("http://s3-2.example.com:9000".to_string()),
//!     Backend::new("http://s3-3.example.com:9000".to_string()),
//! ];
//!
//! let backends = Arc::new(backends);
//!
//! // Create load balancer with power-of-two algorithm
//! let lb = LoadBalancer::new(
//!     backends.as_ref().clone(),
//!     Algorithm::PowerOfTwo
//! );
//!
//! // Create and start health checker
//! let config = HealthCheckConfig {
//!     enabled: true,
//!     interval: Duration::from_secs(5),
//!     timeout: Duration::from_secs(10),
//!     path: "/minio/health/ready".to_string(),
//!     failure_threshold: 3,
//!     success_threshold: 2,
//! };
//!
//! let health_checker = Arc::new(HealthChecker::new(Arc::clone(&backends), config));
//! let health_task = health_checker.start();
//!
//! // Select a healthy backend for a request
//! if let Some(idx) = lb.select_healthy_backend() {
//!     let backend = lb.get_backend(idx).unwrap();
//!     println!("Selected backend: {}", backend.url);
//!
//!     // Track connection
//!     backend.increment_connections();
//!
//!     // ... perform request ...
//!
//!     // Release connection and record result
//!     backend.decrement_connections();
//!     backend.record_success();
//! }
//!
//! // Get health statistics
//! let stats = health_checker.get_health_stats();
//! for stat in stats {
//!     println!(
//!         "Backend {}: health={}, connections={}, healthy={}",
//!         stat.url, stat.health_score, stat.active_connections, stat.is_healthy
//!     );
//! }
//!
//! # Ok(())
//! # }
//! ```
//!
//! # Thread Safety
//!
//! All types in this module are designed to be thread-safe and can be safely shared
//! across async tasks using `Arc`:
//!
//! - Backends use atomic types (`AtomicU32`, `AtomicU8`) for counters and scores
//! - LoadBalancer can be cloned and shared (uses `Arc<Vec<Backend>>` internally)
//! - HealthChecker runs in a background tokio task
//!
//! # Performance
//!
//! - Backend operations are lock-free (atomic operations)
//! - Health checks run in parallel for all backends
//! - Load balancing algorithms are O(1) or O(n) where n is the number of backends
//!
//! # Health Score Calculation
//!
//! Health scores are calculated based on multiple factors:
//!
//! - **Success rate**: Consecutive failures reduce score, successes improve it
//! - **Response time**: Slow responses reduce score
//! - **HTTP status**: 5xx errors heavily penalize, 4xx errors lightly penalize
//! - **Active connections**: High connection count reduces score (for least-connections)
//!
//! The health checker automatically adjusts scores over time based on both active
//! probes and passive monitoring of actual request results.

pub mod backend;
pub mod balancer;
pub mod health;

pub use backend::Backend;
pub use balancer::{Algorithm, LoadBalancer};
pub use health::{BackendHealthStats, HealthCheckConfig, HealthChecker};

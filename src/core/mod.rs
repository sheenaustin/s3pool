use anyhow::Result;
use std::sync::Arc;
use crate::config::Config;
use crate::lb::{LoadBalancer, HealthChecker, Backend, Algorithm, HealthCheckConfig};
use crate::pool::{ConnectionPool, CircuitBreaker};
use crate::s3::S3Client;
use std::time::Duration;

/// Core shared by both CLI and proxy modes
///
/// The S3Client is created once and shared. Clones share the same underlying
/// HTTP connection pool, enabling connection reuse across operations and
/// connection pre-warming.
#[derive(Clone)]
pub struct Core {
    pub config: Arc<Config>,
    pub load_balancer: Arc<LoadBalancer>,
    pub health_checker: Arc<HealthChecker>,
    pub connection_pool: Arc<ConnectionPool>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    /// Shared S3 client - clones share the same HTTP connection pool
    s3_client_shared: S3Client,
}

impl Core {
    pub async fn new(config: Config) -> Result<Self> {
        let config = Arc::new(config);

        // Get the default profile
        let profile = config.get_profile(None)
            .ok_or_else(|| anyhow::anyhow!("No profile found in configuration"))?;

        // Create backends from endpoints
        let backends: Vec<Backend> = profile.endpoints
            .iter()
            .map(|url| Backend::new(url.clone()))
            .collect();

        let backends_arc = Arc::new(backends);

        // Initialize load balancer with backends
        let algorithm = match config.load_balancer.strategy.as_str() {
            "power_of_two" => Algorithm::PowerOfTwo,
            "least_connections" => Algorithm::LeastConnections,
            _ => Algorithm::RoundRobin,
        };
        let load_balancer = Arc::new(LoadBalancer::new(
            backends_arc.as_ref().clone(),
            algorithm,
        ));

        // Initialize health checker
        let health_config = HealthCheckConfig {
            enabled: true,
            interval: Duration::from_secs(config.load_balancer.health_check_interval),
            timeout: Duration::from_secs(config.load_balancer.request_timeout),
            path: "/".to_string(),
            failure_threshold: 3,
            success_threshold: 2,
        };
        let health_checker = Arc::new(HealthChecker::new(
            backends_arc.clone(),
            health_config,
        ));

        // Initialize connection pool with default configuration
        let pool_config = crate::pool::PoolConfig::default();
        let connection_pool = Arc::new(ConnectionPool::new(pool_config));

        // Initialize circuit breaker with default configuration
        let circuit_config = crate::pool::CircuitBreakerConfig::default();
        let circuit_breaker = Arc::new(CircuitBreaker::new(circuit_config));

        // Create S3 client ONCE - all clones share the same HTTP connection pool
        let s3_client_shared = S3Client::new(
            profile.access_key.clone(),
            profile.secret_key.clone(),
            profile.bucket.clone().unwrap_or_default(),
            Some(profile.region.clone()),
        );

        // NOTE: Pre-warming disabled for CLI mode. Each CLI invocation is a
        // short-lived process; warming 30 backends causes CPU contention from
        // concurrent TLS handshakes that slows down the actual operation.
        // Pre-warming should only be enabled for long-running proxy mode.
        // TODO: Re-enable for proxy mode via start_health_checks() or similar.

        Ok(Self {
            config,
            load_balancer,
            health_checker,
            connection_pool,
            circuit_breaker,
            s3_client_shared,
        })
    }

    /// Start health checks (call this for long-running proxy mode, not for CLI)
    pub fn start_health_checks(&self) {
        self.health_checker.clone().start();
    }

    /// Get a clone of the shared S3 client (shares connection pool)
    ///
    /// The returned client shares the same HTTP connection pool as all other
    /// clones, enabling connection reuse and benefiting from pre-warming.
    pub fn s3_client(&self) -> Result<S3Client> {
        Ok(self.s3_client_shared.clone())
    }

    /// Select a healthy endpoint URL from the load balancer
    ///
    /// Each call may return a different endpoint for load distribution.
    /// For paginated operations (list), use the same endpoint for consistency.
    /// For independent operations (upload, download, delete), call per-request.
    pub fn select_endpoint(&self) -> Result<&str> {
        let idx = self.load_balancer.select_healthy_backend()
            .ok_or_else(|| anyhow::anyhow!("No healthy backends available"))?;
        let backend = self.load_balancer.get_backend(idx)
            .ok_or_else(|| anyhow::anyhow!("Backend not found"))?;
        Ok(&backend.url)
    }

    /// Select a healthy endpoint and return both URL and backend index for tracking
    ///
    /// Use this in the proxy to record success/failure after each request.
    pub fn select_endpoint_with_index(&self) -> Result<(&str, usize)> {
        let idx = self.load_balancer.select_healthy_backend()
            .ok_or_else(|| anyhow::anyhow!("No healthy backends available"))?;
        let backend = self.load_balancer.get_backend(idx)
            .ok_or_else(|| anyhow::anyhow!("Backend not found"))?;
        Ok((&backend.url, idx))
    }

    /// Select an endpoint deterministically based on a hash key.
    ///
    /// Used for multipart uploads where all requests with the same uploadId
    /// MUST go to the same backend (parts are stored per-node in MinIO).
    ///
    /// If the primary hashed backend is unhealthy, tries alternate backends
    /// using hash + offset until a healthy one is found.
    pub fn select_endpoint_by_hash(&self, hash_key: &str) -> Result<(&str, usize)> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let backend_count = self.load_balancer.backend_count();
        if backend_count == 0 {
            return Err(anyhow::anyhow!("No backends configured"));
        }

        // Hash the key to get a deterministic starting index
        let mut hasher = DefaultHasher::new();
        hash_key.hash(&mut hasher);
        let hash = hasher.finish();
        let primary_idx = (hash % backend_count as u64) as usize;

        // Try the primary hashed backend first
        let primary_backend = self.load_balancer.get_backend(primary_idx)
            .ok_or_else(|| anyhow::anyhow!("Backend not found at index {}", primary_idx))?;

        if primary_backend.is_healthy() {
            return Ok((&primary_backend.url, primary_idx));
        }

        // Primary backend is unhealthy - try alternates using hash+offset
        // This ensures the SAME alternate is chosen for all requests with the same key
        for offset in 1..backend_count {
            let alt_idx = (primary_idx + offset) % backend_count;
            let backend = self.load_balancer.get_backend(alt_idx).unwrap();
            if backend.is_healthy() {
                tracing::info!(
                    hash_key = %hash_key,
                    primary_idx = primary_idx,
                    fallback_idx = alt_idx,
                    backend = %backend.url,
                    "multipart_routing_fallback"
                );
                return Ok((&backend.url, alt_idx));
            }
        }

        // All backends are unhealthy - fall back to primary (proxy retry may help)
        tracing::warn!(
            hash_key = %hash_key,
            backend = %primary_backend.url,
            "multipart_all_unhealthy_using_primary"
        );
        Ok((&primary_backend.url, primary_idx))
    }

    /// Record a successful request for a backend
    pub fn record_success(&self, backend_idx: usize) {
        if let Some(backend) = self.load_balancer.get_backend(backend_idx) {
            backend.record_success();
        }
    }

    /// Record a failed request for a backend
    pub fn record_failure(&self, backend_idx: usize) {
        if let Some(backend) = self.load_balancer.get_backend(backend_idx) {
            let was_healthy = backend.is_healthy();
            backend.record_failure();
            // Log when backend becomes unhealthy
            if was_healthy && !backend.is_healthy() {
                tracing::warn!(
                    backend = %backend.url,
                    health = backend.get_health_score(),
                    failures = backend.get_failure_count(),
                    "backend unhealthy"
                );
            }
        }
    }
}

use super::backend::Backend;
use hyper::client::conn::http1::SendRequest;
use hyper::{body::Incoming, Request, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Configuration for health checking
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Whether active health checking is enabled
    pub enabled: bool,
    /// Interval between health checks
    pub interval: Duration,
    /// Timeout for health check requests
    pub timeout: Duration,
    /// HTTP path to check (e.g., "/minio/health/ready")
    pub path: String,
    /// Number of consecutive failures before marking as unhealthy
    pub failure_threshold: u32,
    /// Number of consecutive successes before marking as healthy
    pub success_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(10),
            path: "/minio/health/ready".to_string(),
            failure_threshold: 3,
            success_threshold: 2,
        }
    }
}

/// Health checker that performs active and passive health checks
pub struct HealthChecker {
    /// List of backends to monitor
    backends: Arc<Vec<Backend>>,
    /// Health check configuration
    config: HealthCheckConfig,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(backends: Arc<Vec<Backend>>, config: HealthCheckConfig) -> Self {
        Self { backends, config }
    }

    /// Start the health check background task
    /// Returns a task handle that runs until cancelled
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if !self.config.enabled {
                info!("Health checker disabled - all backends will remain healthy by default");
                return;
            }

            info!(
                "Health checker started: interval={}s, path={}",
                self.config.interval.as_secs(),
                self.config.path
            );

            loop {
                let start = Instant::now();
                self.check_all_backends().await;
                let duration = start.elapsed();

                debug!(
                    "Health check cycle completed in {}ms",
                    duration.as_millis()
                );

                // Sleep until next interval
                sleep(self.config.interval).await;
            }
        })
    }

    /// Check health of all backends
    async fn check_all_backends(&self) {
        let mut handles = Vec::new();

        for (idx, backend) in self.backends.iter().enumerate() {
            let backend_clone = backend.clone();
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                Self::check_backend(idx, &backend_clone, &config).await;
            });

            handles.push(handle);
        }

        // Wait for all health checks to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Check health of a single backend
    async fn check_backend(idx: usize, backend: &Backend, config: &HealthCheckConfig) {
        let start = Instant::now();
        let result = Self::probe_backend(backend, config).await;
        let latency = start.elapsed();

        backend.update_last_health_check();

        match result {
            Ok(score) => {
                backend.set_health_score(score);
                debug!(
                    "Backend {} ({}) health check: OK (score={}, latency={}ms)",
                    idx,
                    backend.url,
                    score,
                    latency.as_millis()
                );
            }
            Err(e) => {
                // On connection failure, only reduce score slightly to avoid marking as unhealthy too quickly
                let current_score = backend.get_health_score();
                let new_score = current_score.saturating_sub(10);  // Reduced from 25 to 10
                backend.set_health_score(new_score);

                // Only warn, don't error - connection failures are expected during startup or temporary outages
                debug!(
                    "Backend {} ({}) health check failed: {} (score: {} -> {}) - backend may still be usable",
                    idx, backend.url, e, current_score, new_score
                );
            }
        }
    }

    /// Probe a backend's health endpoint
    async fn probe_backend(
        backend: &Backend,
        config: &HealthCheckConfig,
    ) -> Result<u8, Box<dyn std::error::Error + Send + Sync>> {
        // Parse the backend URL
        let url = url::Url::parse(&backend.url)?;
        let host = url
            .host_str()
            .ok_or("Invalid URL: missing host")?
            .to_string();
        let port = url.port().unwrap_or(if url.scheme() == "https" { 443 } else { 9000 });

        // Create TCP connection with timeout
        let addr = format!("{}:{}", host, port);
        let stream = match tokio::time::timeout(config.timeout, TcpStream::connect(&addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(format!("Connection failed: {}", e).into()),
            Err(_) => return Err("Connection timeout".into()),
        };

        let io = TokioIo::new(stream);

        // Create HTTP/1.1 connection
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("Connection error during health check: {}", e);
            }
        });

        // Build health check request
        let uri = format!("{}{}", backend.url, config.path);
        let req = Request::builder()
            .method("GET")
            .uri(uri)
            .header("Host", host)
            .body(String::new())?;

        // Send request with timeout
        let response = match tokio::time::timeout(config.timeout, sender.send_request(req)).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => return Err(format!("Request failed: {}", e).into()),
            Err(_) => return Err("Request timeout".into()),
        };

        // Calculate health score based on response
        let score = Self::calculate_health_score(response.status(), Duration::from_secs(0));

        Ok(score)
    }

    /// Calculate health score based on response status and latency
    fn calculate_health_score(status: StatusCode, latency: Duration) -> u8 {
        let mut score = 100u8;

        // Factor 1: HTTP status code
        if status.is_success() {
            // 2xx: healthy
            score = 100;
        } else if status.is_server_error() {
            // 5xx: unhealthy
            score = score.saturating_sub(50);
        } else if status.is_client_error() {
            // 4xx: probably misconfigured, but backend is responding
            // For root path checks, 403/404 is acceptable - backend is alive
            if status == StatusCode::FORBIDDEN || status == StatusCode::NOT_FOUND {
                score = 80;  // Backend is alive, just doesn't allow access to this path
            } else {
                score = score.saturating_sub(20);
            }
        }

        // Factor 2: Response time
        if latency > Duration::from_millis(1000) {
            score = score.saturating_sub(30);
        } else if latency > Duration::from_millis(500) {
            score = score.saturating_sub(20);
        } else if latency > Duration::from_millis(100) {
            score = score.saturating_sub(10);
        }

        score
    }

    /// Record a passive health check result (called after each request)
    pub fn record_passive_result(backend: &Backend, success: bool, latency: Duration) {
        if success {
            backend.record_success();

            // Bonus points for fast responses
            if latency < Duration::from_millis(50) {
                let current = backend.get_health_score();
                backend.set_health_score(current.saturating_add(5));
            }
        } else {
            backend.record_failure();

            // Degrade health score more aggressively for slow failures
            if latency > Duration::from_secs(5) {
                let current = backend.get_health_score();
                backend.set_health_score(current.saturating_sub(10));
            }
        }
    }

    /// Get health statistics for all backends
    pub fn get_health_stats(&self) -> Vec<BackendHealthStats> {
        self.backends
            .iter()
            .enumerate()
            .map(|(idx, backend)| BackendHealthStats {
                index: idx,
                url: backend.url.clone(),
                health_score: backend.get_health_score(),
                active_connections: backend.get_active_connections(),
                failure_count: backend.get_failure_count(),
                is_healthy: backend.is_healthy(),
                last_check: backend.time_since_last_check(),
            })
            .collect()
    }
}

/// Health statistics for a single backend
#[derive(Debug, Clone)]
pub struct BackendHealthStats {
    pub index: usize,
    pub url: String,
    pub health_score: u8,
    pub active_connections: u32,
    pub failure_count: u32,
    pub is_healthy: bool,
    pub last_check: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(5));
        assert_eq!(config.path, "/minio/health/ready");
    }

    #[test]
    fn test_calculate_health_score_success() {
        let score = HealthChecker::calculate_health_score(
            StatusCode::OK,
            Duration::from_millis(50),
        );
        assert_eq!(score, 100);
    }

    #[test]
    fn test_calculate_health_score_slow_response() {
        let score = HealthChecker::calculate_health_score(
            StatusCode::OK,
            Duration::from_millis(200),
        );
        assert_eq!(score, 90); // -10 for slow response
    }

    #[test]
    fn test_calculate_health_score_server_error() {
        let score = HealthChecker::calculate_health_score(
            StatusCode::INTERNAL_SERVER_ERROR,
            Duration::from_millis(50),
        );
        assert_eq!(score, 50); // -50 for 5xx
    }

    #[test]
    fn test_calculate_health_score_forbidden() {
        let score = HealthChecker::calculate_health_score(
            StatusCode::FORBIDDEN,
            Duration::from_millis(50),
        );
        assert_eq!(score, 80); // Backend is alive, just access denied
    }

    #[test]
    fn test_passive_health_check() {
        let backend = Backend::new("http://s3-1:9000".to_string());

        // Record success
        HealthChecker::record_passive_result(&backend, true, Duration::from_millis(30));
        assert_eq!(backend.get_failure_count(), 0);
        assert!(backend.get_health_score() > 100); // Gets bonus

        // Record failure
        HealthChecker::record_passive_result(&backend, false, Duration::from_millis(100));
        assert_eq!(backend.get_failure_count(), 1);
        assert!(backend.get_health_score() < 100);
    }

    #[tokio::test]
    async fn test_health_checker_creation() {
        let backends = vec![
            Backend::new("http://s3-1:9000".to_string()),
            Backend::new("http://s3-2:9000".to_string()),
        ];

        let config = HealthCheckConfig {
            enabled: false,
            ..Default::default()
        };

        let checker = Arc::new(HealthChecker::new(Arc::new(backends), config));
        let stats = checker.get_health_stats();

        assert_eq!(stats.len(), 2);
        assert!(stats[0].is_healthy);
        assert!(stats[1].is_healthy);
    }
}

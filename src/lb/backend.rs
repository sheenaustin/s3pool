use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::RwLock;
use std::time::Instant;

/// Represents a single S3 backend with health and connection tracking
#[derive(Debug)]
pub struct Backend {
    /// URL of the backend (e.g., "http://s3-1.example.com:9000")
    pub url: String,

    /// Number of currently active connections to this backend
    pub active_connections: AtomicU32,

    /// Number of consecutive failures (reset on success)
    pub failure_count: AtomicU32,

    /// Health score from 0-100 (100 = perfectly healthy)
    pub health_score: AtomicU8,

    /// Timestamp of last health check
    pub last_health_check: RwLock<Instant>,
}

impl Backend {
    /// Create a new backend with the given URL
    pub fn new(url: String) -> Self {
        Self {
            url,
            active_connections: AtomicU32::new(0),
            failure_count: AtomicU32::new(0),
            health_score: AtomicU8::new(100),
            last_health_check: RwLock::new(Instant::now()),
        }
    }

    /// Increment the active connection counter
    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active connection counter
    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the current number of active connections
    pub fn get_active_connections(&self) -> u32 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Record a successful request (resets failure count)
    pub fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        // Improve health score (max 100)
        let current = self.health_score.load(Ordering::Relaxed);
        let new_score = current.saturating_add(10).min(100);
        self.health_score.store(new_score, Ordering::Relaxed);
    }

    /// Record a failed request (increments failure count, reduces health score)
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        // Reduce health score (min 0)
        let current = self.health_score.load(Ordering::Relaxed);
        let new_score = current.saturating_sub(20);
        self.health_score.store(new_score, Ordering::Relaxed);
    }

    /// Record a connection failure (immediately marks backend as unhealthy)
    ///
    /// Connection failures are a strong signal that the backend is down.
    /// This reduces health score to ≤30, making the backend unhealthy for
    /// subsequent selections in the same CLI invocation.
    pub fn record_connect_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        // Severe reduction to mark as unhealthy (score ≤30)
        let current = self.health_score.load(Ordering::Relaxed);
        let new_score = current.saturating_sub(70);
        self.health_score.store(new_score, Ordering::Relaxed);
    }

    /// Get the current failure count
    pub fn get_failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Relaxed)
    }

    /// Get the current health score (0-100)
    pub fn get_health_score(&self) -> u8 {
        self.health_score.load(Ordering::Relaxed)
    }

    /// Set the health score (0-100)
    pub fn set_health_score(&self, score: u8) {
        self.health_score.store(score.min(100), Ordering::Relaxed);
    }

    /// Check if the backend is healthy (health score > 30)
    pub fn is_healthy(&self) -> bool {
        self.get_health_score() > 30
    }

    /// Update last health check timestamp
    pub fn update_last_health_check(&self) {
        if let Ok(mut last) = self.last_health_check.write() {
            *last = Instant::now();
        }
    }

    /// Get the time since last health check
    pub fn time_since_last_check(&self) -> Option<std::time::Duration> {
        self.last_health_check
            .read()
            .ok()
            .map(|last| last.elapsed())
    }
}

impl Clone for Backend {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            active_connections: AtomicU32::new(self.active_connections.load(Ordering::Relaxed)),
            failure_count: AtomicU32::new(self.failure_count.load(Ordering::Relaxed)),
            health_score: AtomicU8::new(self.health_score.load(Ordering::Relaxed)),
            last_health_check: RwLock::new(
                self.last_health_check.read().map(|x| *x).unwrap_or_else(|_| Instant::now())
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_backend_creation() {
        let backend = Backend::new("http://s3-1:9000".to_string());
        assert_eq!(backend.url, "http://s3-1:9000");
        assert_eq!(backend.get_active_connections(), 0);
        assert_eq!(backend.get_failure_count(), 0);
        assert_eq!(backend.get_health_score(), 100);
        assert!(backend.is_healthy());
    }

    #[test]
    fn test_connection_tracking() {
        let backend = Backend::new("http://s3-1:9000".to_string());

        backend.increment_connections();
        assert_eq!(backend.get_active_connections(), 1);

        backend.increment_connections();
        assert_eq!(backend.get_active_connections(), 2);

        backend.decrement_connections();
        assert_eq!(backend.get_active_connections(), 1);
    }

    #[test]
    fn test_failure_tracking() {
        let backend = Backend::new("http://s3-1:9000".to_string());

        backend.record_failure();
        assert_eq!(backend.get_failure_count(), 1);
        assert_eq!(backend.get_health_score(), 80);

        backend.record_failure();
        assert_eq!(backend.get_failure_count(), 2);
        assert_eq!(backend.get_health_score(), 60);

        backend.record_success();
        assert_eq!(backend.get_failure_count(), 0);
        assert_eq!(backend.get_health_score(), 70);
    }

    #[test]
    fn test_health_score_bounds() {
        let backend = Backend::new("http://s3-1:9000".to_string());

        // Test upper bound
        for _ in 0..20 {
            backend.record_success();
        }
        assert_eq!(backend.get_health_score(), 100);

        // Test lower bound
        for _ in 0..10 {
            backend.record_failure();
        }
        assert_eq!(backend.get_health_score(), 0);
        assert!(!backend.is_healthy());
    }

    #[test]
    fn test_concurrent_access() {
        let backend = Backend::new("http://s3-1:9000".to_string());
        let backend_clone = backend.clone();

        let handle = thread::spawn(move || {
            for _ in 0..100 {
                backend_clone.increment_connections();
                thread::sleep(Duration::from_micros(1));
                backend_clone.decrement_connections();
            }
        });

        for _ in 0..100 {
            backend.increment_connections();
            thread::sleep(Duration::from_micros(1));
            backend.decrement_connections();
        }

        handle.join().unwrap();
        assert_eq!(backend.get_active_connections(), 0);
    }
}

//! Circuit breaker implementation for fault tolerance
//!
//! This module implements a circuit breaker pattern with three states:
//! - Closed: Normal operation, requests are allowed
//! - Open: Backend has failed, requests are rejected
//! - HalfOpen: Testing recovery, limited requests allowed
//!
//! The circuit breaker automatically transitions between states based on
//! success/failure patterns, preventing cascading failures.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::connection::BackendId;

/// Circuit breaker error types
#[derive(Debug, thiserror::Error)]
pub enum CircuitError {
    #[error("Circuit breaker is open for backend: {0}")]
    CircuitOpen(String),

    #[error("Backend not found: {0}")]
    BackendNotFound(String),

    #[error("Half-open circuit has reached maximum test requests")]
    HalfOpenLimitReached,
}

/// Circuit breaker states
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests are allowed
    Closed,

    /// Backend has failed - requests are rejected
    Open {
        /// When the circuit should transition to HalfOpen
        retry_at: Instant,

        /// Number of consecutive failures that caused the circuit to open
        failure_count: u32,
    },

    /// Testing recovery - limited requests allowed
    HalfOpen {
        /// Number of successful requests in half-open state
        success_count: u32,

        /// Number of failed requests in half-open state
        failure_count: u32,
    },
}

impl CircuitState {
    /// Check if the circuit allows requests
    pub fn is_request_allowed(&self) -> bool {
        match self {
            CircuitState::Closed => true,
            CircuitState::Open { retry_at, .. } => Instant::now() >= *retry_at,
            CircuitState::HalfOpen { .. } => true,
        }
    }

    /// Get a human-readable state name
    pub fn name(&self) -> &str {
        match self {
            CircuitState::Closed => "Closed",
            CircuitState::Open { .. } => "Open",
            CircuitState::HalfOpen { .. } => "HalfOpen",
        }
    }
}

/// Configuration for circuit breaker behavior
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    pub failure_threshold: u32,

    /// Number of consecutive successes to close the circuit from half-open
    pub success_threshold: u32,

    /// How long to wait before transitioning from open to half-open
    pub timeout: Duration,

    /// Maximum number of test requests in half-open state
    pub half_open_max_requests: u32,

    /// Time window for tracking failures
    pub failure_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout: Duration::from_secs(30),
            half_open_max_requests: 3,
            failure_window: Duration::from_secs(60),
        }
    }
}

/// Per-backend circuit breaker state and statistics
struct BackendCircuit {
    /// Current circuit state
    state: CircuitState,

    /// Configuration
    config: CircuitBreakerConfig,

    /// Consecutive failure count (in Closed state)
    consecutive_failures: u32,

    /// Total number of requests
    total_requests: u64,

    /// Total successful requests
    total_successes: u64,

    /// Total failed requests
    total_failures: u64,

    /// Last state transition time
    last_transition: Instant,

    /// Number of times the circuit has opened
    open_count: u64,

    /// Recent failure timestamps (for sliding window)
    recent_failures: Vec<Instant>,
}

impl BackendCircuit {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitState::Closed,
            config,
            consecutive_failures: 0,
            total_requests: 0,
            total_successes: 0,
            total_failures: 0,
            last_transition: Instant::now(),
            open_count: 0,
            recent_failures: Vec::new(),
        }
    }

    /// Check if a request is allowed
    fn check_request(&mut self) -> Result<(), CircuitError> {
        self.total_requests += 1;

        match &self.state {
            CircuitState::Closed => Ok(()),

            CircuitState::Open { retry_at, .. } => {
                if Instant::now() >= *retry_at {
                    // Transition to HalfOpen
                    info!("Circuit transitioning from Open to HalfOpen");
                    self.transition_to_half_open();
                    Ok(())
                } else {
                    Err(CircuitError::CircuitOpen(
                        format!("Retry in {:?}", retry_at.saturating_duration_since(Instant::now()))
                    ))
                }
            }

            CircuitState::HalfOpen { success_count, failure_count } => {
                // Check if we've reached the test request limit
                let total_tests = success_count + failure_count;
                if total_tests >= self.config.half_open_max_requests {
                    Err(CircuitError::HalfOpenLimitReached)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Record a successful request
    fn record_success(&mut self) {
        self.total_successes += 1;
        self.consecutive_failures = 0;

        match &self.state {
            CircuitState::Closed => {
                // Stay closed
                debug!("Request succeeded in Closed state");
            }

            CircuitState::Open { .. } => {
                // This shouldn't happen, but if it does, stay open
                warn!("Recorded success in Open state - this is unexpected");
            }

            CircuitState::HalfOpen { success_count, failure_count } => {
                let new_success_count = success_count + 1;
                debug!(
                    success_count = new_success_count,
                    failure_count = failure_count,
                    "Request succeeded in HalfOpen state"
                );

                if new_success_count >= self.config.success_threshold {
                    // Transition to Closed
                    info!(
                        success_count = new_success_count,
                        "Circuit transitioning from HalfOpen to Closed"
                    );
                    self.transition_to_closed();
                } else {
                    // Update HalfOpen state
                    self.state = CircuitState::HalfOpen {
                        success_count: new_success_count,
                        failure_count: *failure_count,
                    };
                }
            }
        }
    }

    /// Record a failed request
    fn record_failure(&mut self) {
        self.total_failures += 1;
        self.consecutive_failures += 1;

        // Add to recent failures for sliding window
        let now = Instant::now();
        self.recent_failures.push(now);

        // Clean up old failures outside the window
        self.recent_failures.retain(|&t| now.duration_since(t) < self.config.failure_window);

        match &self.state {
            CircuitState::Closed => {
                debug!(
                    consecutive_failures = self.consecutive_failures,
                    threshold = self.config.failure_threshold,
                    "Request failed in Closed state"
                );

                if self.consecutive_failures >= self.config.failure_threshold {
                    // Transition to Open
                    warn!(
                        consecutive_failures = self.consecutive_failures,
                        "Circuit transitioning from Closed to Open"
                    );
                    self.transition_to_open();
                }
            }

            CircuitState::Open { .. } => {
                // Stay open
                debug!("Request failed in Open state");
            }

            CircuitState::HalfOpen { success_count, failure_count } => {
                let new_failure_count = failure_count + 1;
                warn!(
                    success_count = success_count,
                    failure_count = new_failure_count,
                    "Request failed in HalfOpen state - reopening circuit"
                );

                // Any failure in HalfOpen immediately reopens the circuit
                self.transition_to_open();
            }
        }
    }

    /// Transition to Closed state
    fn transition_to_closed(&mut self) {
        self.state = CircuitState::Closed;
        self.consecutive_failures = 0;
        self.last_transition = Instant::now();
        self.recent_failures.clear();
    }

    /// Transition to Open state
    fn transition_to_open(&mut self) {
        let retry_at = Instant::now() + self.config.timeout;
        self.state = CircuitState::Open {
            retry_at,
            failure_count: self.consecutive_failures,
        };
        self.open_count += 1;
        self.last_transition = Instant::now();
    }

    /// Transition to HalfOpen state
    fn transition_to_half_open(&mut self) {
        self.state = CircuitState::HalfOpen {
            success_count: 0,
            failure_count: 0,
        };
        self.last_transition = Instant::now();
    }

    /// Get error rate in the recent window
    fn error_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        self.total_failures as f64 / self.total_requests as f64
    }

    /// Get recent error rate (within failure window)
    fn recent_error_rate(&self) -> f64 {
        let recent_count = self.recent_failures.len();
        if recent_count == 0 {
            return 0.0;
        }
        // This is a simplified rate - in production you'd want to track successes too
        recent_count as f64 / (recent_count.max(10) as f64)
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone)]
pub struct CircuitStats {
    /// Current state
    pub state: CircuitState,

    /// Total requests
    pub total_requests: u64,

    /// Total successes
    pub total_successes: u64,

    /// Total failures
    pub total_failures: u64,

    /// Overall error rate
    pub error_rate: f64,

    /// Recent error rate
    pub recent_error_rate: f64,

    /// Number of times circuit has opened
    pub open_count: u64,

    /// Time since last state transition
    pub time_in_state: Duration,
}

/// Circuit breaker manager for all backends
pub struct CircuitBreaker {
    /// Per-backend circuit breakers
    circuits: Arc<RwLock<HashMap<BackendId, BackendCircuit>>>,

    /// Configuration
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    /// Create a new circuit breaker manager
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            circuits: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Register a backend with the circuit breaker
    pub async fn register_backend(&self, backend_id: BackendId) {
        let mut circuits = self.circuits.write().await;
        if !circuits.contains_key(&backend_id) {
            info!(backend_id = %backend_id, "Registering backend with circuit breaker");
            circuits.insert(
                backend_id.clone(),
                BackendCircuit::new(self.config.clone())
            );
        }
    }

    /// Check if a request is allowed for a backend
    pub async fn check_request(&self, backend_id: &BackendId) -> Result<(), CircuitError> {
        let mut circuits = self.circuits.write().await;

        let circuit = circuits.get_mut(backend_id)
            .ok_or_else(|| CircuitError::BackendNotFound(backend_id.clone()))?;

        circuit.check_request()
    }

    /// Record a successful request for a backend
    pub async fn record_success(&self, backend_id: &BackendId) {
        let mut circuits = self.circuits.write().await;

        if let Some(circuit) = circuits.get_mut(backend_id) {
            circuit.record_success();
        }
    }

    /// Record a failed request for a backend
    pub async fn record_failure(&self, backend_id: &BackendId) {
        let mut circuits = self.circuits.write().await;

        if let Some(circuit) = circuits.get_mut(backend_id) {
            circuit.record_failure();
        }
    }

    /// Get the current state of a backend's circuit
    pub async fn get_state(&self, backend_id: &BackendId) -> Option<CircuitState> {
        let circuits = self.circuits.read().await;
        circuits.get(backend_id).map(|c| c.state.clone())
    }

    /// Get statistics for a backend's circuit
    pub async fn get_stats(&self, backend_id: &BackendId) -> Option<CircuitStats> {
        let circuits = self.circuits.read().await;
        circuits.get(backend_id).map(|circuit| {
            CircuitStats {
                state: circuit.state.clone(),
                total_requests: circuit.total_requests,
                total_successes: circuit.total_successes,
                total_failures: circuit.total_failures,
                error_rate: circuit.error_rate(),
                recent_error_rate: circuit.recent_error_rate(),
                open_count: circuit.open_count,
                time_in_state: circuit.last_transition.elapsed(),
            }
        })
    }

    /// Get statistics for all backends
    pub async fn get_all_stats(&self) -> HashMap<BackendId, CircuitStats> {
        let circuits = self.circuits.read().await;
        circuits.iter()
            .map(|(id, circuit)| {
                let stats = CircuitStats {
                    state: circuit.state.clone(),
                    total_requests: circuit.total_requests,
                    total_successes: circuit.total_successes,
                    total_failures: circuit.total_failures,
                    error_rate: circuit.error_rate(),
                    recent_error_rate: circuit.recent_error_rate(),
                    open_count: circuit.open_count,
                    time_in_state: circuit.last_transition.elapsed(),
                };
                (id.clone(), stats)
            })
            .collect()
    }

    /// Manually reset a circuit to closed state
    pub async fn reset_circuit(&self, backend_id: &BackendId) {
        let mut circuits = self.circuits.write().await;

        if let Some(circuit) = circuits.get_mut(backend_id) {
            info!(backend_id = %backend_id, "Manually resetting circuit to Closed");
            circuit.transition_to_closed();
        }
    }

    /// Remove a backend from the circuit breaker
    pub async fn remove_backend(&self, backend_id: &BackendId) {
        let mut circuits = self.circuits.write().await;
        if circuits.remove(backend_id).is_some() {
            info!(backend_id = %backend_id, "Removed backend from circuit breaker");
        }
    }

    /// Check if a backend is healthy (circuit is closed)
    pub async fn is_healthy(&self, backend_id: &BackendId) -> bool {
        let circuits = self.circuits.read().await;
        circuits.get(backend_id)
            .map(|c| matches!(c.state, CircuitState::Closed))
            .unwrap_or(false)
    }

    /// Get list of all healthy backends
    pub async fn get_healthy_backends(&self, backend_ids: &[BackendId]) -> Vec<BackendId> {
        let circuits = self.circuits.read().await;
        backend_ids.iter()
            .filter(|id| {
                circuits.get(*id)
                    .map(|c| matches!(c.state, CircuitState::Closed))
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_closed_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_secs(1),
            half_open_max_requests: 2,
            failure_window: Duration::from_secs(10),
        };

        let breaker = CircuitBreaker::new(config);
        breaker.register_backend("test".to_string()).await;

        // Circuit should start closed
        assert!(breaker.is_healthy(&"test".to_string()).await);

        // Record failures
        breaker.record_failure(&"test".to_string()).await;
        breaker.record_failure(&"test".to_string()).await;
        breaker.record_failure(&"test".to_string()).await;

        // Circuit should now be open
        assert!(!breaker.is_healthy(&"test".to_string()).await);

        let result = breaker.check_request(&"test".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_circuit_half_open_to_closed() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
            half_open_max_requests: 3,
            failure_window: Duration::from_secs(10),
        };

        let breaker = CircuitBreaker::new(config);
        breaker.register_backend("test".to_string()).await;

        // Open the circuit
        breaker.record_failure(&"test".to_string()).await;
        breaker.record_failure(&"test".to_string()).await;

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should transition to half-open on next check
        let result = breaker.check_request(&"test".to_string()).await;
        assert!(result.is_ok());

        // Record successes
        breaker.record_success(&"test".to_string()).await;
        breaker.record_success(&"test".to_string()).await;

        // Should be closed now
        assert!(breaker.is_healthy(&"test".to_string()).await);
    }

    #[tokio::test]
    async fn test_circuit_half_open_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
            half_open_max_requests: 3,
            failure_window: Duration::from_secs(10),
        };

        let breaker = CircuitBreaker::new(config);
        breaker.register_backend("test".to_string()).await;

        // Open the circuit
        breaker.record_failure(&"test".to_string()).await;
        breaker.record_failure(&"test".to_string()).await;

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Transition to half-open
        let result = breaker.check_request(&"test".to_string()).await;
        assert!(result.is_ok());

        // Record a failure - should reopen
        breaker.record_failure(&"test".to_string()).await;

        // Should be open again
        assert!(!breaker.is_healthy(&"test".to_string()).await);
    }

    #[test]
    fn test_circuit_state_names() {
        assert_eq!(CircuitState::Closed.name(), "Closed");
        assert_eq!(
            CircuitState::Open {
                retry_at: Instant::now(),
                failure_count: 5
            }.name(),
            "Open"
        );
        assert_eq!(
            CircuitState::HalfOpen {
                success_count: 1,
                failure_count: 0
            }.name(),
            "HalfOpen"
        );
    }
}

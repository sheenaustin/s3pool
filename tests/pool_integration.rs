//! Integration tests for connection pool and circuit breaker
//!
//! These tests verify that the connection pool and circuit breaker
//! work correctly together in realistic scenarios.

use s3pool::pool::{
    CircuitBreaker, CircuitBreakerConfig, CircuitState,
    ConnectionPool, PoolConfig,
};
use std::time::Duration;

#[tokio::test]
async fn test_pool_and_circuit_integration() {
    // Setup
    let pool_config = PoolConfig {
        max_connections: 10,
        min_connections: 2,
        max_idle_time: Duration::from_secs(30),
        connect_timeout: Duration::from_secs(5),
        keepalive_timeout: Duration::from_secs(60),
        http2_enabled: true,
        max_http2_streams: 50,
    };

    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        timeout: Duration::from_millis(500),
        half_open_max_requests: 2,
        failure_window: Duration::from_secs(10),
    };

    let pool = ConnectionPool::new(pool_config);
    let circuit = CircuitBreaker::new(circuit_config);

    // Register a backend
    let backend_id = "test-backend".to_string();
    pool.register_backend(
        backend_id.clone(),
        "http://localhost:9000".to_string()
    ).await;
    circuit.register_backend(backend_id.clone()).await;

    // Verify initial state
    assert!(circuit.is_healthy(&backend_id).await);

    // Verify circuit breaker allows requests initially
    let check_result = circuit.check_request(&backend_id).await;
    assert!(check_result.is_ok());

    // Verify we can get backend stats
    let stats = pool.get_stats(&backend_id).await;
    assert!(stats.is_some());

    let circuit_stats = circuit.get_stats(&backend_id).await;
    assert!(circuit_stats.is_some());
}

#[tokio::test]
async fn test_circuit_opens_after_failures() {
    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 3,
        success_threshold: 2,
        timeout: Duration::from_millis(500),
        half_open_max_requests: 2,
        failure_window: Duration::from_secs(10),
    };

    let circuit = CircuitBreaker::new(circuit_config);
    let backend_id = "failing-backend".to_string();
    circuit.register_backend(backend_id.clone()).await;

    // Record failures
    for _ in 0..3 {
        circuit.record_failure(&backend_id).await;
    }

    // Circuit should be open
    assert!(!circuit.is_healthy(&backend_id).await);

    let state = circuit.get_state(&backend_id).await.unwrap();
    assert!(matches!(state, CircuitState::Open { .. }));

    // Requests should be rejected
    let result = circuit.check_request(&backend_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_circuit_recovery_flow() {
    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 2,
        success_threshold: 2,
        timeout: Duration::from_millis(200),
        half_open_max_requests: 3,
        failure_window: Duration::from_secs(10),
    };

    let circuit = CircuitBreaker::new(circuit_config);
    let backend_id = "recovery-backend".to_string();
    circuit.register_backend(backend_id.clone()).await;

    // Open the circuit
    circuit.record_failure(&backend_id).await;
    circuit.record_failure(&backend_id).await;

    assert!(!circuit.is_healthy(&backend_id).await);

    // Wait for timeout
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Should transition to half-open
    let result = circuit.check_request(&backend_id).await;
    assert!(result.is_ok());

    let state = circuit.get_state(&backend_id).await.unwrap();
    assert!(matches!(state, CircuitState::HalfOpen { .. }));

    // Record successes to close
    circuit.record_success(&backend_id).await;
    circuit.record_success(&backend_id).await;

    // Should be closed now
    assert!(circuit.is_healthy(&backend_id).await);

    let state = circuit.get_state(&backend_id).await.unwrap();
    assert!(matches!(state, CircuitState::Closed));
}

#[tokio::test]
async fn test_multiple_backends() {
    let pool = ConnectionPool::new(PoolConfig::default());
    let circuit = CircuitBreaker::new(CircuitBreakerConfig::default());

    // Register multiple backends
    let backends = vec![
        "backend-1".to_string(),
        "backend-2".to_string(),
        "backend-3".to_string(),
    ];

    for backend_id in &backends {
        pool.register_backend(
            backend_id.clone(),
            format!("http://{}.example.com:9000", backend_id)
        ).await;
        circuit.register_backend(backend_id.clone()).await;
    }

    // Verify all are healthy
    let healthy = circuit.get_healthy_backends(&backends).await;
    assert_eq!(healthy.len(), backends.len());

    // Fail one backend
    circuit.record_failure(&backends[0]).await;
    circuit.record_failure(&backends[0]).await;
    circuit.record_failure(&backends[0]).await;
    circuit.record_failure(&backends[0]).await;
    circuit.record_failure(&backends[0]).await;

    // Verify it's excluded
    let healthy = circuit.get_healthy_backends(&backends).await;
    assert_eq!(healthy.len(), backends.len() - 1);
    assert!(!healthy.contains(&backends[0]));

    // Get all stats
    let all_stats = circuit.get_all_stats().await;
    assert_eq!(all_stats.len(), backends.len());
}

#[tokio::test]
async fn test_pool_statistics() {
    let pool = ConnectionPool::new(PoolConfig::default());
    let backend_id = "stats-backend".to_string();

    pool.register_backend(
        backend_id.clone(),
        "http://localhost:9000".to_string()
    ).await;

    // Get initial stats
    let stats = pool.get_stats(&backend_id).await.unwrap();
    assert_eq!(stats.total_created, 0);
    assert_eq!(stats.total_reused, 0);

    // Note: We can't easily test actual connections without a real server,
    // but we can verify the stats structure is correct
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.idle_connections, 0);
}

#[tokio::test]
async fn test_circuit_manual_reset() {
    let circuit = CircuitBreaker::new(CircuitBreakerConfig::default());
    let backend_id = "reset-backend".to_string();
    circuit.register_backend(backend_id.clone()).await;

    // Open the circuit
    for _ in 0..5 {
        circuit.record_failure(&backend_id).await;
    }

    assert!(!circuit.is_healthy(&backend_id).await);

    // Manually reset
    circuit.reset_circuit(&backend_id).await;

    // Should be healthy now
    assert!(circuit.is_healthy(&backend_id).await);

    let state = circuit.get_state(&backend_id).await.unwrap();
    assert!(matches!(state, CircuitState::Closed));
}

#[tokio::test]
async fn test_backend_removal() {
    let pool = ConnectionPool::new(PoolConfig::default());
    let circuit = CircuitBreaker::new(CircuitBreakerConfig::default());
    let backend_id = "remove-backend".to_string();

    // Register
    pool.register_backend(
        backend_id.clone(),
        "http://localhost:9000".to_string()
    ).await;
    circuit.register_backend(backend_id.clone()).await;

    // Verify registered
    assert!(pool.get_stats(&backend_id).await.is_some());
    assert!(circuit.get_state(&backend_id).await.is_some());

    // Remove
    pool.remove_backend(&backend_id).await;
    circuit.remove_backend(&backend_id).await;

    // Verify removed
    assert!(pool.get_stats(&backend_id).await.is_none());
    assert!(circuit.get_state(&backend_id).await.is_none());
}

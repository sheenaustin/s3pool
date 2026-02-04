//! Example demonstrating connection pool and circuit breaker usage
//!
//! This example shows how to:
//! 1. Configure connection pools and circuit breakers
//! 2. Register backends
//! 3. Make requests with automatic circuit breaking
//! 4. Monitor pool and circuit statistics

use s3pool::pool::{
    CircuitBreaker, CircuitBreakerConfig, ConnectionPool, PoolConfig,
};
use std::time::Duration;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Configure connection pool
    let pool_config = PoolConfig {
        max_connections: 50,
        min_connections: 5,
        max_idle_time: Duration::from_secs(90),
        connect_timeout: Duration::from_secs(10),
        keepalive_timeout: Duration::from_secs(90),
        http2_enabled: true,
        max_http2_streams: 100,
    };

    // Configure circuit breaker
    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 3,
        timeout: Duration::from_secs(30),
        half_open_max_requests: 3,
        failure_window: Duration::from_secs(60),
    };

    // Create pool and circuit breaker
    let pool = ConnectionPool::new(pool_config);
    let circuit = CircuitBreaker::new(circuit_config);

    // Register backends
    let backends = vec![
        ("backend-1".to_string(), "http://s3-1.example.com:9000".to_string()),
        ("backend-2".to_string(), "http://s3-2.example.com:9000".to_string()),
        ("backend-3".to_string(), "http://s3-3.example.com:9000".to_string()),
    ];

    for (id, url) in backends.iter() {
        pool.register_backend(id.clone(), url.clone()).await;
        circuit.register_backend(id.clone()).await;
        info!("Registered backend: {} -> {}", id, url);
    }

    // Simulate making requests with circuit breaker protection
    for i in 0..20 {
        let backend_id = &backends[i % backends.len()].0;

        // Check circuit breaker before making request
        match circuit.check_request(backend_id).await {
            Ok(()) => {
                info!("Request #{} to {} - circuit allows", i + 1, backend_id);

                // Get connection from pool
                match pool.get_connection(backend_id).await {
                    Ok(mut conn) => {
                        info!("Got connection for {}", backend_id);

                        // Simulate request (in real code, you'd use the connection)
                        // let response = conn.sender().send_request(request).await;

                        // Simulate success/failure pattern
                        if i % 7 == 0 {
                            // Simulate failure
                            warn!("Request #{} to {} - FAILED", i + 1, backend_id);
                            circuit.record_failure(backend_id).await;
                        } else {
                            // Simulate success
                            info!("Request #{} to {} - SUCCESS", i + 1, backend_id);
                            circuit.record_success(backend_id).await;

                            // Return connection to pool
                            pool.return_connection(backend_id, conn).await;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get connection for {}: {}", backend_id, e);
                        circuit.record_failure(backend_id).await;
                    }
                }
            }
            Err(e) => {
                warn!("Request #{} to {} - circuit blocked: {}", i + 1, backend_id, e);
            }
        }

        // Small delay between requests
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Print statistics
    println!("\n=== FINAL STATISTICS ===\n");

    // Pool statistics
    println!("Connection Pool Stats:");
    let pool_stats = pool.get_all_stats().await;
    for (backend_id, stats) in pool_stats {
        println!("  Backend: {}", backend_id);
        println!("    Total created: {}", stats.total_created);
        println!("    Total reused: {}", stats.total_reused);
        println!("    Active connections: {}", stats.active_connections);
        println!("    Idle connections: {}", stats.idle_connections);
        println!("    Failed attempts: {}", stats.failed_attempts);
        println!();
    }

    // Circuit breaker statistics
    println!("Circuit Breaker Stats:");
    let circuit_stats = circuit.get_all_stats().await;
    for (backend_id, stats) in circuit_stats {
        println!("  Backend: {}", backend_id);
        println!("    State: {}", stats.state.name());
        println!("    Total requests: {}", stats.total_requests);
        println!("    Total successes: {}", stats.total_successes);
        println!("    Total failures: {}", stats.total_failures);
        println!("    Error rate: {:.2}%", stats.error_rate * 100.0);
        println!("    Times opened: {}", stats.open_count);
        println!("    Time in current state: {:?}", stats.time_in_state);
        println!();
    }

    // Demonstrate circuit breaker recovery
    println!("\n=== TESTING CIRCUIT RECOVERY ===\n");

    // Find a backend with open circuit
    for (backend_id, _) in backends.iter() {
        if !circuit.is_healthy(backend_id).await {
            info!("Found unhealthy backend: {}", backend_id);

            // Wait for circuit timeout
            info!("Waiting for circuit breaker timeout...");
            tokio::time::sleep(Duration::from_secs(31)).await;

            // Try request again (should transition to half-open)
            info!("Attempting request after timeout...");
            match circuit.check_request(backend_id).await {
                Ok(()) => {
                    info!("Circuit transitioned to HalfOpen, request allowed");

                    // Simulate successful requests to close the circuit
                    for j in 0..3 {
                        circuit.record_success(backend_id).await;
                        info!("Recovery request #{} succeeded", j + 1);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }

                    if circuit.is_healthy(backend_id).await {
                        info!("Circuit successfully closed!");
                    }
                }
                Err(e) => {
                    warn!("Circuit still blocking: {}", e);
                }
            }

            break;
        }
    }

    // Final health check
    println!("\n=== FINAL HEALTH STATUS ===\n");
    for (backend_id, _) in backends.iter() {
        let healthy = circuit.is_healthy(backend_id).await;
        let state = circuit.get_state(backend_id).await;
        println!(
            "  {}: {} (state: {})",
            backend_id,
            if healthy { "HEALTHY" } else { "UNHEALTHY" },
            state.as_ref().map(|s| s.name()).unwrap_or("unknown")
        );
    }

    Ok(())
}

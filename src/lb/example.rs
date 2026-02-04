//! Example usage of the load balancer module
//!
//! This file demonstrates how to use the load balancer, backends, and health checker
//! together in a typical s3pool scenario.
//!
//! Note: This is for documentation purposes and is not compiled as part of the binary.

#![allow(dead_code)]

use super::{Algorithm, Backend, BackendHealthStats, HealthCheckConfig, HealthChecker, LoadBalancer};
use std::sync::Arc;
use std::time::Duration;

/// Example 1: Basic round-robin load balancing
pub async fn example_round_robin() {
    // Create backends
    let backends = vec![
        Backend::new("http://s3-1.example.com:9000".to_string()),
        Backend::new("http://s3-2.example.com:9000".to_string()),
        Backend::new("http://s3-3.example.com:9000".to_string()),
    ];

    // Create load balancer
    let lb = LoadBalancer::new(backends, Algorithm::RoundRobin);

    // Select backends in round-robin fashion
    for i in 0..10 {
        if let Some(idx) = lb.select_backend() {
            let backend = lb.get_backend(idx).unwrap();
            println!("Request {}: Selected backend {}: {}", i, idx, backend.url);
        }
    }
}

/// Example 2: Power-of-two with connection tracking
pub async fn example_power_of_two() {
    let backends = vec![
        Backend::new("http://s3-1.example.com:9000".to_string()),
        Backend::new("http://s3-2.example.com:9000".to_string()),
        Backend::new("http://s3-3.example.com:9000".to_string()),
    ];

    let lb = LoadBalancer::new(backends, Algorithm::PowerOfTwo);

    // Simulate 100 concurrent requests
    let mut handles = vec![];

    for i in 0..100 {
        let lb_clone = lb.clone();
        let handle = tokio::spawn(async move {
            if let Some(idx) = lb_clone.select_backend() {
                let backend = lb_clone.get_backend(idx).unwrap();

                // Track connection
                backend.increment_connections();

                // Simulate work
                tokio::time::sleep(Duration::from_millis(10 + (i % 50))).await;

                // Release connection
                backend.decrement_connections();

                println!(
                    "Request {} → Backend {} (active: {})",
                    i,
                    backend.url,
                    backend.get_active_connections()
                );
            }
        });
        handles.push(handle);
    }

    // Wait for all requests
    for handle in handles {
        let _ = handle.await;
    }

    // Print final connection counts
    println!("\nFinal connection counts:");
    for (idx, backend) in lb.get_backends().iter().enumerate() {
        println!(
            "  Backend {}: {} active connections",
            idx,
            backend.get_active_connections()
        );
    }
}

/// Example 3: Health-aware load balancing
pub async fn example_health_aware() {
    let backends = Arc::new(vec![
        Backend::new("http://s3-1.example.com:9000".to_string()),
        Backend::new("http://s3-2.example.com:9000".to_string()),
        Backend::new("http://s3-3.example.com:9000".to_string()),
    ]);

    let lb = LoadBalancer::new(backends.as_ref().clone(), Algorithm::LeastConnections);

    // Simulate backend 1 becoming unhealthy
    backends[1].record_failure();
    backends[1].record_failure();
    backends[1].record_failure();
    backends[1].set_health_score(20); // Mark as unhealthy

    println!("Backend health scores:");
    for (idx, backend) in backends.iter().enumerate() {
        println!(
            "  Backend {}: score={}, healthy={}",
            idx,
            backend.get_health_score(),
            backend.is_healthy()
        );
    }

    // Select healthy backends only
    println!("\nSelecting healthy backends:");
    for i in 0..10 {
        if let Some(idx) = lb.select_healthy_backend() {
            let backend = lb.get_backend(idx).unwrap();
            println!("Request {}: Selected backend {} (health={})", i, idx, backend.get_health_score());
        }
    }
}

/// Example 4: Complete integration with health checker
pub async fn example_with_health_checker() {
    // Create backends
    let backends = Arc::new(vec![
        Backend::new("http://localhost:9001".to_string()),
        Backend::new("http://localhost:9002".to_string()),
        Backend::new("http://localhost:9003".to_string()),
    ]);

    // Create load balancer
    let lb = LoadBalancer::new(backends.as_ref().clone(), Algorithm::PowerOfTwo);

    // Configure health checker
    let config = HealthCheckConfig {
        enabled: true,
        interval: Duration::from_secs(5),
        timeout: Duration::from_secs(10),
        path: "/minio/health/ready".to_string(),
        failure_threshold: 3,
        success_threshold: 2,
    };

    // Start health checker
    let checker = Arc::new(HealthChecker::new(Arc::clone(&backends), config));
    let _health_task = checker.start();

    // Simulate request loop
    println!("Starting request loop (press Ctrl+C to stop)...\n");

    for i in 0..20 {
        // Select healthy backend
        if let Some(idx) = lb.select_healthy_backend() {
            let backend = lb.get_backend(idx).unwrap();

            // Track connection
            backend.increment_connections();

            // Simulate request
            let start = std::time::Instant::now();
            let success = simulate_request(backend, i).await;
            let latency = start.elapsed();

            // Release connection
            backend.decrement_connections();

            // Record result (passive health check)
            HealthChecker::record_passive_result(backend, success, latency);

            println!(
                "Request {}: {} → {} ({}ms, health={})",
                i,
                backend.url,
                if success { "OK" } else { "FAIL" },
                latency.as_millis(),
                backend.get_health_score()
            );
        } else {
            println!("Request {}: No healthy backends available!", i);
        }

        // Wait between requests
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Print health stats every 5 requests
        if i % 5 == 4 {
            print_health_stats(&checker);
        }
    }
}

/// Example 5: Failure handling and recovery
pub async fn example_failure_handling() {
    let backends = vec![
        Backend::new("http://s3-1.example.com:9000".to_string()),
        Backend::new("http://s3-2.example.com:9000".to_string()),
        Backend::new("http://s3-3.example.com:9000".to_string()),
    ];

    let lb = LoadBalancer::new(backends, Algorithm::LeastConnections);

    println!("Simulating failures and recovery:\n");

    for i in 0..20 {
        if let Some(idx) = lb.select_healthy_backend() {
            let backend = lb.get_backend(idx).unwrap();

            // Simulate occasional failures
            let success = if i % 7 == 0 {
                backend.record_failure();
                false
            } else {
                backend.record_success();
                true
            };

            println!(
                "Request {}: Backend {} → {} (health={}, failures={})",
                i,
                idx,
                if success { "OK" } else { "FAIL" },
                backend.get_health_score(),
                backend.get_failure_count()
            );
        }
    }

    println!("\nFinal health scores:");
    for (idx, backend) in lb.get_backends().iter().enumerate() {
        println!(
            "  Backend {}: health={}, failures={}, healthy={}",
            idx,
            backend.get_health_score(),
            backend.get_failure_count(),
            backend.is_healthy()
        );
    }
}

/// Helper: Simulate an S3 request (for demonstration)
async fn simulate_request(backend: &Backend, request_num: usize) -> bool {
    // Simulate variable latency
    let delay_ms = 50 + (request_num % 100);
    tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;

    // Simulate occasional failures (10% failure rate)
    let success = request_num % 10 != 0;

    success
}

/// Helper: Print health statistics
fn print_health_stats(checker: &HealthChecker) {
    println!("\n=== Health Statistics ===");
    let stats = checker.get_health_stats();

    for stat in stats {
        println!(
            "Backend {}: health={:3}, connections={:3}, failures={:2}, healthy={}",
            stat.url, stat.health_score, stat.active_connections, stat.failure_count, stat.is_healthy
        );
    }
    println!();
}

/// Run all examples (for testing)
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_example_round_robin() {
        example_round_robin().await;
    }

    #[tokio::test]
    async fn run_example_power_of_two() {
        example_power_of_two().await;
    }

    #[tokio::test]
    async fn run_example_health_aware() {
        example_health_aware().await;
    }

    #[tokio::test]
    async fn run_example_failure_handling() {
        example_failure_handling().await;
    }
}

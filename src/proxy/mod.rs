//! S3Pool Proxy Server
//!
//! This module implements an HTTP proxy server that accepts S3 API requests
//! and forwards them to the s3pool core with built-in load balancing.

mod server;

pub use server::{ProxyServer, S3Operation};

use anyhow::Result;
use crate::core::Core;

/// Run the proxy server with the given core and listen address
///
/// This function starts the HTTP proxy server that accepts S3 API requests
/// and forwards them to the s3pool core for load balancing and execution.
///
/// # Arguments
/// * `core` - The s3pool core with load balancer and connection pool
/// * `listen` - The address to listen on (e.g., "0.0.0.0:8000")
/// * `daemon` - Whether to run as a daemon (not yet implemented)
pub async fn run_server(core: &Core, listen: &str, _daemon: bool) -> Result<()> {
    // Start health checks for long-running proxy mode
    core.start_health_checks();

    let server = ProxyServer::new(core.clone(), listen.to_string());
    server.run().await
}

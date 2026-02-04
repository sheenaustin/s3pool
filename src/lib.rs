//! s3pool v2 - Ultra-fast S3 client with built-in load balancing

pub mod cli;
pub mod config;
pub mod core;
pub mod lb;
pub mod pool;
pub mod proxy;
pub mod s3;

pub use core::Core;
pub use config::Config;

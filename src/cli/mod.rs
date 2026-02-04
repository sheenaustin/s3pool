//! CLI module for s3pool v2
//!
//! This module provides a command-line interface for s3pool, offering mc-compatible
//! commands for S3 operations with built-in load balancing and parallel execution.
//!
//! # Features
//!
//! - **mc-compatible commands**: ls, cp, rm, sync, stat, mb, rb
//! - **Load balancing**: Automatic distribution across multiple S3 endpoints
//! - **Parallel operations**: Multi-threaded uploads, downloads, and deletions
//! - **Progress tracking**: Real-time progress bars using indicatif
//! - **Multiple output formats**: Text and JSON output
//! - **Configuration profiles**: Support for named configuration profiles
//!
//! # Usage
//!
//! ```bash
//! # List objects
//! s3pool ls s3://bucket/prefix --recursive
//!
//! # Upload files
//! s3pool cp /local/file.txt s3://bucket/key --progress
//!
//! # Download files
//! s3pool cp s3://bucket/key /local/file.txt --progress
//!
//! # Sync directories
//! s3pool sync /local/dir s3://bucket/prefix --delete
//!
//! # Delete objects
//! s3pool rm s3://bucket/prefix --recursive --force
//!
//! # Object info
//! s3pool stat s3://bucket/key --metadata
//!
//! # Create bucket
//! s3pool mb s3://bucket --versioning --encryption
//!
//! # Remove bucket
//! s3pool rb s3://bucket --force
//! ```

pub mod args;
pub mod commands;
pub mod handler;

use anyhow::{Context, Result};
use tracing::{debug, info};

use args::{Cli, Commands};
use commands::*;
use handler::CLIHandler;

/// Run the CLI application
pub async fn run() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse_args();

    // Initialize logging
    init_logging(cli.verbose, cli.debug)?;

    // Validate arguments
    cli.validate().context("Invalid arguments")?;

    info!("Starting s3pool CLI");
    debug!("CLI arguments: {:?}", cli);

    // Get configuration
    let config = get_config(&cli).context("Failed to get configuration")?;

    // Create CLI handler
    let handler = CLIHandler::new(
        config.endpoints,
        config.access_key,
        config.secret_key,
        cli.region.clone(),
        cli.workers,
        cli.format.clone(),
    );

    // Execute command
    match cli.command {
        Commands::Ls {
            path,
            recursive,
            long,
            human_readable,
        } => {
            let (bucket, prefix) = args::parse_s3_path(&path)?;
            let cmd = CmdLs::new(bucket, prefix, recursive, long, human_readable);
            handler.execute_ls(cmd).await?;
        }

        Commands::Cp {
            source,
            dest,
            recursive,
            parallel,
            progress,
            storage_class,
            content_type,
        } => {
            let cmd = CmdCp::new(
                &source,
                &dest,
                recursive,
                parallel,
                progress,
                storage_class,
                content_type,
            )?;
            handler.execute_cp(cmd).await?;
        }

        Commands::Rm {
            path,
            recursive,
            force,
            parallel,
        } => {
            let (bucket, key) = args::parse_s3_path(&path)?;
            let cmd = CmdRm::new(bucket, key, recursive, force, parallel);
            handler.execute_rm(cmd).await?;
        }

        Commands::Sync {
            source,
            dest,
            delete,
            parallel,
            dry_run,
            progress,
        } => {
            let cmd = CmdSync::new(&source, &dest, delete, parallel, dry_run, progress)?;
            handler.execute_sync(cmd).await?;
        }

        Commands::Stat { path, metadata } => {
            let (bucket, key) = args::parse_s3_path(&path)?;
            let key = key.ok_or_else(|| anyhow::anyhow!("Key required for stat command"))?;
            let cmd = CmdStat::new(bucket, key, metadata);
            handler.execute_stat(cmd).await?;
        }

        Commands::Mb {
            bucket,
            versioning,
            encryption,
        } => {
            let (bucket, _) = args::parse_s3_path(&bucket)?;
            let cmd = CmdMb::new(bucket, versioning, encryption);
            handler.execute_mb(cmd).await?;
        }

        Commands::Rb { bucket, force } => {
            let (bucket, _) = args::parse_s3_path(&bucket)?;
            let cmd = CmdRb::new(bucket, force);
            handler.execute_rb(cmd).await?;
        }
    }

    info!("Command completed successfully");
    Ok(())
}

/// Configuration for CLI operations
#[derive(Debug, Clone)]
struct Config {
    endpoints: Vec<String>,
    access_key: String,
    secret_key: String,
}

/// Get configuration from CLI args, environment, or config file
fn get_config(cli: &Cli) -> Result<Config> {
    // Priority: CLI args > environment variables > config file > defaults

    let endpoints = if let Some(endpoints) = &cli.endpoints {
        endpoints.clone()
    } else if let Some(profile) = &cli.profile {
        // TODO: Load from config file using profile
        load_endpoints_from_profile(profile)?
    } else {
        // Default to AWS S3 endpoint
        vec!["https://s3.amazonaws.com".to_string()]
    };

    let access_key = cli
        .access_key
        .clone()
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
        .ok_or_else(|| anyhow::anyhow!("AWS_ACCESS_KEY_ID not set"))?;

    let secret_key = cli
        .secret_key
        .clone()
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
        .ok_or_else(|| anyhow::anyhow!("AWS_SECRET_ACCESS_KEY not set"))?;

    Ok(Config {
        endpoints,
        access_key,
        secret_key,
    })
}

/// Load endpoints from configuration profile
fn load_endpoints_from_profile(profile: &str) -> Result<Vec<String>> {
    // TODO: Implement config file loading
    // For now, return default endpoint
    debug!("Loading profile: {}", profile);
    Ok(vec!["https://s3.amazonaws.com".to_string()])
}

/// Initialize logging based on verbosity flags
fn init_logging(verbose: bool, debug: bool) -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = if debug {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"))
    } else if verbose {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false).with_thread_ids(false))
        .init();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_priority() {
        // Test that configuration is loaded correctly
        // TODO: Add more comprehensive tests
    }
}
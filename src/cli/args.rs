use clap::{Parser, Subcommand};

/// s3pool - Ultra-fast S3 client with built-in load balancing
#[derive(Parser, Debug)]
#[command(name = "s3pool")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Configuration profile to use
    #[arg(long, global = true, env = "S3POOL_PROFILE")]
    pub profile: Option<String>,

    /// S3 endpoints (comma-separated)
    #[arg(long, global = true, env = "S3POOL_ENDPOINTS", value_delimiter = ',')]
    pub endpoints: Option<Vec<String>>,

    /// Number of worker threads for parallel operations
    #[arg(long, global = true, default_value = "4", env = "S3POOL_WORKERS")]
    pub workers: usize,

    /// AWS Access Key ID
    #[arg(long, global = true, env = "AWS_ACCESS_KEY_ID")]
    pub access_key: Option<String>,

    /// AWS Secret Access Key
    #[arg(long, global = true, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_key: Option<String>,

    /// AWS Region
    #[arg(long, global = true, default_value = "us-east-1", env = "AWS_REGION")]
    pub region: String,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Enable debug logging
    #[arg(short, long, global = true)]
    pub debug: bool,

    /// Output format (text, json)
    #[arg(long, global = true, default_value = "text")]
    pub format: OutputFormat,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// List objects in a bucket (mc ls compatible)
    Ls {
        /// S3 path (s3://bucket/prefix)
        #[arg(value_name = "PATH")]
        path: String,

        /// Recursive listing
        #[arg(short, long)]
        recursive: bool,

        /// Show detailed information
        #[arg(short, long)]
        long: bool,

        /// Show human-readable sizes
        #[arg(short = 'H', long)]
        human_readable: bool,
    },

    /// Copy files to/from S3 (mc cp compatible)
    Cp {
        /// Source path (local or s3://bucket/key)
        #[arg(value_name = "SOURCE")]
        source: String,

        /// Destination path (local or s3://bucket/key)
        #[arg(value_name = "DEST")]
        dest: String,

        /// Recursive copy
        #[arg(short, long)]
        recursive: bool,

        /// Number of parallel uploads/downloads
        #[arg(short = 'P', long, default_value = "4")]
        parallel: usize,

        /// Show progress bar
        #[arg(short, long)]
        progress: bool,

        /// Storage class (STANDARD, STANDARD_IA, GLACIER, etc.)
        #[arg(long)]
        storage_class: Option<String>,

        /// Content type
        #[arg(long)]
        content_type: Option<String>,
    },

    /// Remove objects from S3 (mc rm compatible)
    Rm {
        /// S3 path (s3://bucket/key or s3://bucket/prefix)
        #[arg(value_name = "PATH")]
        path: String,

        /// Recursive delete
        #[arg(short, long)]
        recursive: bool,

        /// Force delete without confirmation
        #[arg(short, long)]
        force: bool,

        /// Number of parallel deletions
        #[arg(short = 'P', long, default_value = "4")]
        parallel: usize,
    },

    /// Synchronize directories (mc mirror compatible)
    Sync {
        /// Source path (local or s3://bucket/prefix)
        #[arg(value_name = "SOURCE")]
        source: String,

        /// Destination path (local or s3://bucket/prefix)
        #[arg(value_name = "DEST")]
        dest: String,

        /// Delete files in destination not present in source
        #[arg(long)]
        delete: bool,

        /// Number of parallel operations
        #[arg(short = 'P', long, default_value = "8")]
        parallel: usize,

        /// Dry run - show what would be synced
        #[arg(long)]
        dry_run: bool,

        /// Show progress bar
        #[arg(short, long)]
        progress: bool,
    },

    /// Show object statistics (mc stat compatible)
    Stat {
        /// S3 path (s3://bucket/key)
        #[arg(value_name = "PATH")]
        path: String,

        /// Show metadata
        #[arg(short, long)]
        metadata: bool,
    },

    /// Create a bucket (mc mb compatible)
    Mb {
        /// Bucket name (s3://bucket)
        #[arg(value_name = "BUCKET")]
        bucket: String,

        /// Enable versioning
        #[arg(long)]
        versioning: bool,

        /// Enable encryption
        #[arg(long)]
        encryption: bool,
    },

    /// Remove a bucket (mc rb compatible)
    Rb {
        /// Bucket name (s3://bucket)
        #[arg(value_name = "BUCKET")]
        bucket: String,

        /// Force removal of non-empty bucket
        #[arg(short, long)]
        force: bool,
    },
}

impl Cli {
    /// Parse command line arguments
    pub fn parse_args() -> Self {
        Cli::parse()
    }

    /// Validate arguments
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate workers count
        if self.workers == 0 {
            anyhow::bail!("Workers count must be greater than 0");
        }

        // Validate endpoints if provided
        if let Some(endpoints) = &self.endpoints {
            if endpoints.is_empty() {
                anyhow::bail!("Endpoints list cannot be empty");
            }
        }

        // Validate command-specific arguments
        match &self.command {
            Commands::Cp { parallel, .. } | Commands::Rm { parallel, .. } => {
                if *parallel == 0 {
                    anyhow::bail!("Parallel count must be greater than 0");
                }
            }
            Commands::Sync { parallel, .. } => {
                if *parallel == 0 {
                    anyhow::bail!("Parallel count must be greater than 0");
                }
            }
            _ => {}
        }

        Ok(())
    }
}

/// Parse S3 path into bucket and key components
///
/// Accepts both mc-compatible `s3/bucket/key` and URI-style `s3://bucket/key`.
pub fn parse_s3_path(path: &str) -> anyhow::Result<(String, Option<String>)> {
    let path = path.trim();

    // Accept both "s3://bucket/key" (URI) and "s3/bucket/key" (mc-compatible)
    let stripped = if let Some(p) = path.strip_prefix("s3://") {
        p
    } else if let Some(p) = path.strip_prefix("s3/") {
        p
    } else {
        anyhow::bail!("Invalid S3 path format. Expected: s3/bucket/key");
    };

    let parts: Vec<&str> = stripped.splitn(2, '/').collect();

    let bucket = parts[0].to_string();
    if bucket.is_empty() {
        anyhow::bail!("Bucket name cannot be empty");
    }

    let key = if parts.len() > 1 && !parts[1].is_empty() {
        Some(parts[1].to_string())
    } else {
        None
    };

    Ok((bucket, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_path() {
        // mc-compatible format: s3/bucket/key
        assert_eq!(
            parse_s3_path("s3/mybucket").unwrap(),
            ("mybucket".to_string(), None)
        );
        assert_eq!(
            parse_s3_path("s3/mybucket/mykey").unwrap(),
            ("mybucket".to_string(), Some("mykey".to_string()))
        );
        assert_eq!(
            parse_s3_path("s3/mybucket/path/to/object.txt").unwrap(),
            ("mybucket".to_string(), Some("path/to/object.txt".to_string()))
        );

        // URI format also accepted: s3://bucket/key
        assert_eq!(
            parse_s3_path("s3://mybucket").unwrap(),
            ("mybucket".to_string(), None)
        );
        assert_eq!(
            parse_s3_path("s3://mybucket/mykey").unwrap(),
            ("mybucket".to_string(), Some("mykey".to_string()))
        );

        // Invalid paths
        assert!(parse_s3_path("mybucket").is_err());
        assert!(parse_s3_path("s3/").is_err());
        assert!(parse_s3_path("http://mybucket").is_err());
    }
}

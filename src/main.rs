use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cli;
mod config;
mod core;
mod lb;
mod pool;
mod proxy;
mod s3;

#[derive(Parser)]
#[command(name = "s3pool")]
#[command(version, about = "Ultra-fast S3 client with built-in load balancing", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Config file path
    #[arg(long, global = true)]
    config: Option<String>,

    /// Profile to use from config
    #[arg(long, global = true)]
    profile: Option<String>,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, global = true, default_value = "info")]
    log_level: String,

    /// Disable SSL certificate verification (like mc --insecure)
    #[arg(long, global = true)]
    insecure: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// List objects (like mc ls)
    Ls {
        /// S3 path (s3://bucket/prefix/)
        path: String,

        /// List recursively
        #[arg(short, long)]
        recursive: bool,

        /// Maximum number of results to show (0 = unlimited)
        #[arg(long, default_value = "0")]
        max_keys: usize,
    },

    /// Copy files (upload/download)
    Cp {
        /// Source path
        source: String,

        /// Destination path
        destination: String,

        /// Copy recursively
        #[arg(short, long)]
        recursive: bool,

        /// Number of parallel workers
        #[arg(long, default_value = "10")]
        workers: usize,
    },

    /// Remove objects
    Rm {
        /// S3 path to remove
        path: String,

        /// Remove recursively
        #[arg(short, long)]
        recursive: bool,

        /// Force deletion without confirmation
        #[arg(short, long)]
        force: bool,
    },

    /// Sync directories (like rsync)
    Sync {
        /// Source path
        source: String,

        /// Destination path
        destination: String,

        /// Number of parallel workers
        #[arg(long, default_value = "10")]
        workers: usize,

        /// Delete files that don't exist in source
        #[arg(long)]
        delete: bool,
    },

    /// Show object info
    Stat {
        /// S3 path
        path: String,
    },

    /// Make bucket
    Mb {
        /// Bucket name (s3://bucket/)
        bucket: String,
    },

    /// Remove bucket
    Rb {
        /// Bucket name (s3://bucket/)
        bucket: String,

        /// Force removal even if not empty
        #[arg(long)]
        force: bool,
    },

    /// Disk usage
    Du {
        /// S3 path
        path: String,
    },

    /// Find objects
    Find {
        /// S3 path
        path: String,

        /// Name pattern (glob)
        #[arg(long)]
        name: Option<String>,

        /// Minimum size (e.g., 10M, 1G)
        #[arg(long)]
        larger: Option<String>,

        /// Maximum size
        #[arg(long)]
        smaller: Option<String>,
    },

    /// Speed test
    Speedtest {
        /// S3 path for testing
        path: String,

        /// Test duration in seconds
        #[arg(long, default_value = "10")]
        duration: u64,
    },

    /// Run as HTTP proxy server (like sidekick)
    Proxy {
        /// Address to listen on
        #[arg(long, default_value = "0.0.0.0:8000")]
        listen: String,

        /// Run as daemon
        #[arg(long)]
        daemon: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    // Set insecure TLS if flag is set (before config loading)
    if cli.insecure {
        std::env::set_var("S3POOL_INSECURE_TLS", "true");
    }

    // Choose runtime based on mode:
    // - CLI commands: current_thread for minimal overhead
    // - Proxy mode: multi_thread for concurrent request handling
    let is_proxy = matches!(cli.command, Commands::Proxy { .. });

    let runtime = if is_proxy {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
    } else {
        // CLI mode: current_thread is sufficient for sequential I/O.
        // Prefetch was tested but provides no benefit: hyper/rustls HTTP overhead
        // dominates (~400-600ms/page) vs. XML parsing (~4ms/page).
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    };

    runtime.block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    // Load configuration
    let config = config::load_config(cli.config.as_deref(), cli.profile.as_deref())?;

    // Initialize core
    let core = core::Core::new(config.clone()).await?;

    // Route to CLI or proxy mode
    match cli.command {
        Commands::Ls { path, recursive, max_keys } => {
            cli::commands::cmd_ls(&core, &path, recursive, max_keys).await?;
        }
        Commands::Cp {
            source,
            destination,
            recursive,
            workers,
        } => {
            cli::commands::cmd_cp(&core, &source, &destination, recursive, workers).await?;
        }
        Commands::Rm {
            path,
            recursive,
            force,
        } => {
            cli::commands::cmd_rm(&core, &path, recursive, force).await?;
        }
        Commands::Sync {
            source,
            destination,
            workers,
            delete,
        } => {
            cli::commands::cmd_sync(&core, &source, &destination, workers, delete).await?;
        }
        Commands::Stat { path } => {
            cli::commands::cmd_stat(&core, &path).await?;
        }
        Commands::Mb { bucket } => {
            cli::commands::cmd_mb(&core, &bucket).await?;
        }
        Commands::Rb { bucket, force } => {
            cli::commands::cmd_rb(&core, &bucket, force).await?;
        }
        Commands::Du { path } => {
            cli::commands::cmd_du(&core, &path).await?;
        }
        Commands::Find {
            path,
            name,
            larger,
            smaller,
        } => {
            cli::commands::cmd_find(&core, &path, name.as_deref(), larger.as_deref(), smaller.as_deref()).await?;
        }
        Commands::Speedtest { path, duration } => {
            cli::commands::cmd_speedtest(&core, &path, duration).await?;
        }
        Commands::Proxy { listen, daemon } => {
            proxy::run_server(&core, &listen, daemon).await?;
        }
    }

    Ok(())
}

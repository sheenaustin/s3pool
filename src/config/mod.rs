use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// S3 Profile configuration with endpoints and credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    /// List of S3 endpoint URLs
    pub endpoints: Vec<String>,

    /// AWS access key ID
    pub access_key: String,

    /// AWS secret access key
    pub secret_key: String,

    /// AWS region (default: us-east-1)
    #[serde(default = "default_region")]
    pub region: String,

    /// Optional bucket name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// Load balancer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    /// Load balancing strategy: round_robin, least_connections, random, weighted
    #[serde(default = "default_strategy")]
    pub strategy: String,

    /// Health check interval in seconds
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval: u64,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout")]
    pub request_timeout: u64,

    /// Max retries per request
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Enable connection pooling
    #[serde(default = "default_connection_pooling")]
    pub connection_pooling: bool,
}

fn default_strategy() -> String {
    "round_robin".to_string()
}

fn default_health_check_interval() -> u64 {
    30
}

fn default_request_timeout() -> u64 {
    300
}

fn default_max_retries() -> u32 {
    3
}

fn default_connection_pooling() -> bool {
    true
}

impl Default for LoadBalancerConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            health_check_interval: default_health_check_interval(),
            request_timeout: default_request_timeout(),
            max_retries: default_max_retries(),
            connection_pooling: default_connection_pooling(),
        }
    }
}

/// Proxy server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Listen address (default: 0.0.0.0:8000)
    #[serde(default = "default_proxy_listen")]
    pub listen: String,

    /// Default profile to use
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
}

fn default_proxy_listen() -> String {
    "0.0.0.0:8000".to_string()
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen: default_proxy_listen(),
            default_profile: None,
        }
    }
}

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Named profiles for different S3 configurations
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,

    /// Load balancer settings
    #[serde(default)]
    pub load_balancer: LoadBalancerConfig,

    /// Proxy server settings
    #[serde(default)]
    pub proxy: ProxyConfig,
}

impl Config {
    /// Create a new empty configuration
    pub fn new() -> Self {
        Self {
            profiles: HashMap::new(),
            load_balancer: LoadBalancerConfig::default(),
            proxy: ProxyConfig::default(),
        }
    }

    /// Get a profile by name, or the default profile if not specified
    pub fn get_profile(&self, name: Option<&str>) -> Option<&Profile> {
        if let Some(name) = name {
            self.profiles.get(name)
        } else if let Some(default) = &self.proxy.default_profile {
            self.profiles.get(default)
        } else {
            self.profiles.values().next()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Load configuration from a YAML file
pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> Result<Config> {
    let content = std::fs::read_to_string(path.as_ref())
        .context(format!("Failed to read config file: {:?}", path.as_ref()))?;

    let config: Config = serde_yaml::from_str(&content)
        .context("Failed to parse YAML configuration")?;

    Ok(config)
}

/// Load configuration from environment variables
///
/// Supports both AWS standard variables and legacy S3_POOL format:
/// - AWS_ACCESS_KEY_ID / S3_KEY
/// - AWS_SECRET_ACCESS_KEY / S3_SECRET
/// - AWS_REGION (optional, defaults to us-east-1)
/// - S3_POOL (comma-separated list of endpoints)
/// - S3_BUCKET (optional)
/// - PROXY_LISTEN (optional, defaults to 0.0.0.0:8000)
pub fn load_from_env() -> Result<Config> {
    // Try to load .env file if it exists (don't fail if it doesn't)
    let _ = dotenvy::dotenv();

    let mut config = Config::new();

    // Load S3 pool endpoints
    let endpoints_str = std::env::var("S3_POOL")
        .context("S3_POOL environment variable not set")?;

    let endpoints: Vec<String> = endpoints_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if endpoints.is_empty() {
        anyhow::bail!("S3_POOL contains no valid endpoints");
    }

    // Load credentials (support both AWS standard and legacy format)
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .or_else(|_| std::env::var("S3_KEY"))
        .context("Neither AWS_ACCESS_KEY_ID nor S3_KEY environment variable is set")?;

    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .or_else(|_| std::env::var("S3_SECRET"))
        .context("Neither AWS_SECRET_ACCESS_KEY nor S3_SECRET environment variable is set")?;

    // Optional region (defaults to us-east-1)
    let region = std::env::var("AWS_REGION")
        .unwrap_or_else(|_| "us-east-1".to_string());

    // Optional bucket
    let bucket = std::env::var("S3_BUCKET").ok();

    // Create default profile
    let profile = Profile {
        endpoints,
        access_key,
        secret_key,
        region,
        bucket,
    };

    config.profiles.insert("default".to_string(), profile);
    config.proxy.default_profile = Some("default".to_string());

    // Optional proxy listen address
    if let Ok(listen) = std::env::var("PROXY_LISTEN") {
        config.proxy.listen = listen;
    }

    // Load balancer configuration from env
    if let Ok(strategy) = std::env::var("LB_STRATEGY") {
        config.load_balancer.strategy = strategy;
    }

    if let Ok(interval) = std::env::var("LB_HEALTH_CHECK_INTERVAL") {
        if let Ok(val) = interval.parse() {
            config.load_balancer.health_check_interval = val;
        }
    }

    if let Ok(timeout) = std::env::var("LB_REQUEST_TIMEOUT") {
        if let Ok(val) = timeout.parse() {
            config.load_balancer.request_timeout = val;
        }
    }

    if let Ok(retries) = std::env::var("LB_MAX_RETRIES") {
        if let Ok(val) = retries.parse() {
            config.load_balancer.max_retries = val;
        }
    }

    Ok(config)
}

/// Load configuration from file or environment
///
/// This is a convenience function that tries to load from a YAML file first,
/// then falls back to environment variables if no file is specified.
///
/// # Arguments
/// * `config_path` - Optional path to YAML config file
/// * `profile_name` - Optional profile name to use (only relevant for YAML configs)
///
/// # Returns
/// * `Result<Config>` - Loaded configuration or error
pub fn load_config(config_path: Option<&str>, profile_name: Option<&str>) -> Result<Config> {
    if let Some(path) = config_path {
        // Load from YAML file
        let mut config = load_from_yaml(path)?;

        // If a specific profile is requested, make it the default
        if let Some(name) = profile_name {
            if !config.profiles.contains_key(name) {
                anyhow::bail!("Profile '{}' not found in config file", name);
            }
            config.proxy.default_profile = Some(name.to_string());
        }

        Ok(config)
    } else {
        // Load from environment variables
        load_from_env()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_from_yaml() {
        let yaml = r#"
profiles:
  production:
    endpoints:
      - https://s3-1.example.com
      - https://s3-2.example.com
    access_key: AKIAIOSFODNN7EXAMPLE
    secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    region: us-west-2
    bucket: my-bucket

load_balancer:
  strategy: round_robin
  health_check_interval: 30
  request_timeout: 300
  max_retries: 3
  connection_pooling: true

proxy:
  listen: "0.0.0.0:8000"
  default_profile: production
"#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.profiles.len(), 1);
        assert!(config.profiles.contains_key("production"));

        let profile = config.profiles.get("production").unwrap();
        assert_eq!(profile.endpoints.len(), 2);
        assert_eq!(profile.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(profile.region, "us-west-2");

        assert_eq!(config.load_balancer.strategy, "round_robin");
        assert_eq!(config.proxy.listen, "0.0.0.0:8000");
    }

    #[test]
    fn test_default_values() {
        let yaml = r#"
profiles:
  minimal:
    endpoints:
      - https://s3.example.com
    access_key: key
    secret_key: secret
"#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let profile = config.profiles.get("minimal").unwrap();

        // Should use default region
        assert_eq!(profile.region, "us-east-1");

        // Should use default load balancer settings
        assert_eq!(config.load_balancer.strategy, "round_robin");
        assert_eq!(config.load_balancer.max_retries, 3);
    }
}

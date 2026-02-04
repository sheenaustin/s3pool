use std::env;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Test loading configuration from YAML file
#[test]
fn test_load_yaml_config() {
    let yaml = r#"
profiles:
  test:
    endpoints:
      - https://s3-1.example.com
      - https://s3-2.example.com
    access_key: AKIATEST
    secret_key: secrettest
    region: us-west-2
    bucket: test-bucket

load_balancer:
  strategy: least_connections
  health_check_interval: 60
  request_timeout: 120
  max_retries: 5

proxy:
  listen: "127.0.0.1:9000"
  default_profile: test
"#;

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.yaml");
    fs::write(&config_path, yaml).unwrap();

    let config = s3pool::config::load_from_yaml(&config_path).unwrap();

    assert_eq!(config.profiles.len(), 1);
    assert!(config.profiles.contains_key("test"));

    let profile = config.profiles.get("test").unwrap();
    assert_eq!(profile.endpoints.len(), 2);
    assert_eq!(profile.access_key, "AKIATEST");
    assert_eq!(profile.secret_key, "secrettest");
    assert_eq!(profile.region, "us-west-2");
    assert_eq!(profile.bucket, Some("test-bucket".to_string()));

    assert_eq!(config.load_balancer.strategy, "least_connections");
    assert_eq!(config.load_balancer.health_check_interval, 60);
    assert_eq!(config.load_balancer.request_timeout, 120);
    assert_eq!(config.load_balancer.max_retries, 5);

    assert_eq!(config.proxy.listen, "127.0.0.1:9000");
    assert_eq!(config.proxy.default_profile, Some("test".to_string()));
}

/// Test loading configuration from environment variables (AWS standard format)
#[test]
fn test_load_env_config_aws_format() {
    // Save original env vars
    let orig_key = env::var("AWS_ACCESS_KEY_ID").ok();
    let orig_secret = env::var("AWS_SECRET_ACCESS_KEY").ok();
    let orig_region = env::var("AWS_REGION").ok();
    let orig_pool = env::var("S3_POOL").ok();
    let orig_bucket = env::var("S3_BUCKET").ok();

    // Set test env vars
    env::set_var("AWS_ACCESS_KEY_ID", "test_key");
    env::set_var("AWS_SECRET_ACCESS_KEY", "test_secret");
    env::set_var("AWS_REGION", "eu-west-1");
    env::set_var("S3_POOL", "https://s3-1.test.com,https://s3-2.test.com,https://s3-3.test.com");
    env::set_var("S3_BUCKET", "test-bucket");

    let config = s3pool::config::load_from_env().unwrap();

    assert_eq!(config.profiles.len(), 1);
    assert!(config.profiles.contains_key("default"));

    let profile = config.profiles.get("default").unwrap();
    assert_eq!(profile.endpoints.len(), 3);
    assert_eq!(profile.endpoints[0], "https://s3-1.test.com");
    assert_eq!(profile.endpoints[1], "https://s3-2.test.com");
    assert_eq!(profile.endpoints[2], "https://s3-3.test.com");
    assert_eq!(profile.access_key, "test_key");
    assert_eq!(profile.secret_key, "test_secret");
    assert_eq!(profile.region, "eu-west-1");
    assert_eq!(profile.bucket, Some("test-bucket".to_string()));

    assert_eq!(config.proxy.default_profile, Some("default".to_string()));

    // Restore original env vars
    cleanup_env("AWS_ACCESS_KEY_ID", orig_key);
    cleanup_env("AWS_SECRET_ACCESS_KEY", orig_secret);
    cleanup_env("AWS_REGION", orig_region);
    cleanup_env("S3_POOL", orig_pool);
    cleanup_env("S3_BUCKET", orig_bucket);
}

/// Test loading configuration from environment variables (legacy format)
#[test]
fn test_load_env_config_legacy_format() {
    // Save original env vars
    let orig_key = env::var("S3_KEY").ok();
    let orig_secret = env::var("S3_SECRET").ok();
    let orig_pool = env::var("S3_POOL").ok();

    // Set test env vars (legacy format)
    env::set_var("S3_KEY", "legacy_key");
    env::set_var("S3_SECRET", "legacy_secret");
    env::set_var("S3_POOL", "https://legacy-s3.test.com");

    let config = s3pool::config::load_from_env().unwrap();

    let profile = config.profiles.get("default").unwrap();
    assert_eq!(profile.access_key, "legacy_key");
    assert_eq!(profile.secret_key, "legacy_secret");
    assert_eq!(profile.endpoints.len(), 1);
    assert_eq!(profile.endpoints[0], "https://legacy-s3.test.com");
    // Should use default region when not specified
    assert_eq!(profile.region, "us-east-1");

    // Restore original env vars
    cleanup_env("S3_KEY", orig_key);
    cleanup_env("S3_SECRET", orig_secret);
    cleanup_env("S3_POOL", orig_pool);
}

/// Test load balancer configuration from environment
#[test]
fn test_load_balancer_env_config() {
    // Save original env vars
    let orig_key = env::var("AWS_ACCESS_KEY_ID").ok();
    let orig_secret = env::var("AWS_SECRET_ACCESS_KEY").ok();
    let orig_pool = env::var("S3_POOL").ok();
    let orig_strategy = env::var("LB_STRATEGY").ok();
    let orig_interval = env::var("LB_HEALTH_CHECK_INTERVAL").ok();
    let orig_timeout = env::var("LB_REQUEST_TIMEOUT").ok();
    let orig_retries = env::var("LB_MAX_RETRIES").ok();

    // Set test env vars
    env::set_var("AWS_ACCESS_KEY_ID", "key");
    env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
    env::set_var("S3_POOL", "https://s3.test.com");
    env::set_var("LB_STRATEGY", "random");
    env::set_var("LB_HEALTH_CHECK_INTERVAL", "120");
    env::set_var("LB_REQUEST_TIMEOUT", "600");
    env::set_var("LB_MAX_RETRIES", "10");

    let config = s3pool::config::load_from_env().unwrap();

    assert_eq!(config.load_balancer.strategy, "random");
    assert_eq!(config.load_balancer.health_check_interval, 120);
    assert_eq!(config.load_balancer.request_timeout, 600);
    assert_eq!(config.load_balancer.max_retries, 10);

    // Restore original env vars
    cleanup_env("AWS_ACCESS_KEY_ID", orig_key);
    cleanup_env("AWS_SECRET_ACCESS_KEY", orig_secret);
    cleanup_env("S3_POOL", orig_pool);
    cleanup_env("LB_STRATEGY", orig_strategy);
    cleanup_env("LB_HEALTH_CHECK_INTERVAL", orig_interval);
    cleanup_env("LB_REQUEST_TIMEOUT", orig_timeout);
    cleanup_env("LB_MAX_RETRIES", orig_retries);
}

/// Test default values
#[test]
fn test_default_values() {
    let yaml = r#"
profiles:
  minimal:
    endpoints:
      - https://s3.test.com
    access_key: key
    secret_key: secret
"#;

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.yaml");
    fs::write(&config_path, yaml).unwrap();

    let config = s3pool::config::load_from_yaml(&config_path).unwrap();

    let profile = config.profiles.get("minimal").unwrap();
    // Should default to us-east-1
    assert_eq!(profile.region, "us-east-1");
    // Bucket should be None
    assert_eq!(profile.bucket, None);

    // Should use default load balancer settings
    assert_eq!(config.load_balancer.strategy, "round_robin");
    assert_eq!(config.load_balancer.health_check_interval, 30);
    assert_eq!(config.load_balancer.request_timeout, 300);
    assert_eq!(config.load_balancer.max_retries, 3);
    assert!(config.load_balancer.connection_pooling);

    // Should use default proxy settings
    assert_eq!(config.proxy.listen, "0.0.0.0:8000");
}

/// Test get_profile method
#[test]
fn test_get_profile() {
    let yaml = r#"
profiles:
  prod:
    endpoints:
      - https://s3-prod.test.com
    access_key: prod_key
    secret_key: prod_secret
  dev:
    endpoints:
      - https://s3-dev.test.com
    access_key: dev_key
    secret_key: dev_secret

proxy:
  default_profile: prod
"#;

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.yaml");
    fs::write(&config_path, yaml).unwrap();

    let config = s3pool::config::load_from_yaml(&config_path).unwrap();

    // Get specific profile
    let dev_profile = config.get_profile(Some("dev")).unwrap();
    assert_eq!(dev_profile.access_key, "dev_key");

    // Get default profile (None specified, should use proxy.default_profile)
    let default_profile = config.get_profile(None).unwrap();
    assert_eq!(default_profile.access_key, "prod_key");

    // Get non-existent profile
    assert!(config.get_profile(Some("nonexistent")).is_none());
}

/// Helper function to cleanup environment variables
fn cleanup_env(key: &str, orig_val: Option<String>) {
    match orig_val {
        Some(val) => env::set_var(key, val),
        None => env::remove_var(key),
    }
}

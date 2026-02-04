use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Write as FmtWrite;
use std::path::PathBuf;

/// List command - list objects in S3
#[derive(Debug, Clone)]
pub struct CmdLs {
    pub bucket: String,
    pub prefix: Option<String>,
    pub recursive: bool,
    pub long_format: bool,
    pub human_readable: bool,
}

impl CmdLs {
    pub fn new(
        bucket: String,
        prefix: Option<String>,
        recursive: bool,
        long_format: bool,
        human_readable: bool,
    ) -> Self {
        Self {
            bucket,
            prefix,
            recursive,
            long_format,
            human_readable,
        }
    }

    /// Get the prefix for listing, handling trailing slashes
    pub fn get_prefix(&self) -> Option<String> {
        self.prefix.as_ref().map(|p| {
            if self.recursive {
                p.clone()
            } else {
                // For non-recursive listing, ensure prefix ends with /
                if p.ends_with('/') {
                    p.clone()
                } else {
                    format!("{}/", p)
                }
            }
        })
    }
}

/// Copy command - upload/download files
#[derive(Debug, Clone)]
pub struct CmdCp {
    pub source: CpSource,
    pub dest: CpDestination,
    pub recursive: bool,
    pub parallel: usize,
    pub show_progress: bool,
    pub storage_class: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone)]
pub enum CpSource {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

#[derive(Debug, Clone)]
pub enum CpDestination {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

impl CmdCp {
    pub fn new(
        source: &str,
        dest: &str,
        recursive: bool,
        parallel: usize,
        show_progress: bool,
        storage_class: Option<String>,
        content_type: Option<String>,
    ) -> Result<Self> {
        let source = Self::parse_source(source)?;
        let dest = Self::parse_destination(dest)?;

        Ok(Self {
            source,
            dest,
            recursive,
            parallel,
            show_progress,
            storage_class,
            content_type,
        })
    }

    fn parse_source(path: &str) -> Result<CpSource> {
        if path.starts_with("s3://") || path.starts_with("s3/") {
            let (bucket, key) = crate::cli::args::parse_s3_path(path)?;
            let key = key.ok_or_else(|| anyhow::anyhow!("S3 source must include a key"))?;
            Ok(CpSource::S3 { bucket, key })
        } else {
            Ok(CpSource::Local(PathBuf::from(path)))
        }
    }

    fn parse_destination(path: &str) -> Result<CpDestination> {
        if path.starts_with("s3://") || path.starts_with("s3/") {
            let (bucket, key) = crate::cli::args::parse_s3_path(path)?;
            let key = key.unwrap_or_default();
            Ok(CpDestination::S3 { bucket, key })
        } else {
            Ok(CpDestination::Local(PathBuf::from(path)))
        }
    }

    pub fn is_upload(&self) -> bool {
        matches!(self.source, CpSource::Local(_)) && matches!(self.dest, CpDestination::S3 { .. })
    }

    pub fn is_download(&self) -> bool {
        matches!(self.source, CpSource::S3 { .. }) && matches!(self.dest, CpDestination::Local(_))
    }
}

/// Remove command - delete objects from S3
#[derive(Debug, Clone)]
pub struct CmdRm {
    pub bucket: String,
    pub key: Option<String>,
    pub recursive: bool,
    pub force: bool,
    pub parallel: usize,
}

impl CmdRm {
    pub fn new(
        bucket: String,
        key: Option<String>,
        recursive: bool,
        force: bool,
        parallel: usize,
    ) -> Self {
        Self {
            bucket,
            key,
            recursive,
            force,
            parallel,
        }
    }
}

/// Sync command - synchronize directories
#[derive(Debug, Clone)]
pub struct CmdSync {
    pub source: SyncSource,
    pub dest: SyncDestination,
    pub delete: bool,
    pub parallel: usize,
    pub dry_run: bool,
    pub show_progress: bool,
}

#[derive(Debug, Clone)]
pub enum SyncSource {
    Local(PathBuf),
    S3 { bucket: String, prefix: String },
}

#[derive(Debug, Clone)]
pub enum SyncDestination {
    Local(PathBuf),
    S3 { bucket: String, prefix: String },
}

impl CmdSync {
    pub fn new(
        source: &str,
        dest: &str,
        delete: bool,
        parallel: usize,
        dry_run: bool,
        show_progress: bool,
    ) -> Result<Self> {
        let source = Self::parse_source(source)?;
        let dest = Self::parse_destination(dest)?;

        Ok(Self {
            source,
            dest,
            delete,
            parallel,
            dry_run,
            show_progress,
        })
    }

    fn parse_source(path: &str) -> Result<SyncSource> {
        if path.starts_with("s3://") || path.starts_with("s3/") {
            let (bucket, prefix) = crate::cli::args::parse_s3_path(path)?;
            let prefix = prefix.unwrap_or_default();
            Ok(SyncSource::S3 { bucket, prefix })
        } else {
            Ok(SyncSource::Local(PathBuf::from(path)))
        }
    }

    fn parse_destination(path: &str) -> Result<SyncDestination> {
        if path.starts_with("s3://") || path.starts_with("s3/") {
            let (bucket, prefix) = crate::cli::args::parse_s3_path(path)?;
            let prefix = prefix.unwrap_or_default();
            Ok(SyncDestination::S3 { bucket, prefix })
        } else {
            Ok(SyncDestination::Local(PathBuf::from(path)))
        }
    }

    pub fn is_upload(&self) -> bool {
        matches!(self.source, SyncSource::Local(_))
            && matches!(self.dest, SyncDestination::S3 { .. })
    }

    pub fn is_download(&self) -> bool {
        matches!(self.source, SyncSource::S3 { .. })
            && matches!(self.dest, SyncDestination::Local(_))
    }
}

/// Stat command - show object information
#[derive(Debug, Clone)]
pub struct CmdStat {
    pub bucket: String,
    pub key: String,
    pub show_metadata: bool,
}

impl CmdStat {
    pub fn new(bucket: String, key: String, show_metadata: bool) -> Self {
        Self {
            bucket,
            key,
            show_metadata,
        }
    }
}

/// Make bucket command - create a new bucket
#[derive(Debug, Clone)]
pub struct CmdMb {
    pub bucket: String,
    pub versioning: bool,
    pub encryption: bool,
}

impl CmdMb {
    pub fn new(bucket: String, versioning: bool, encryption: bool) -> Self {
        Self {
            bucket,
            versioning,
            encryption,
        }
    }
}

/// Remove bucket command - delete a bucket
#[derive(Debug, Clone)]
pub struct CmdRb {
    pub bucket: String,
    pub force: bool,
}

impl CmdRb {
    pub fn new(bucket: String, force: bool) -> Self {
        Self { bucket, force }
    }
}

// ============================================================================
// Data structures for command results
// ============================================================================

/// S3 object information (re-export from s3::types)
pub use crate::s3::types::S3Object as S3ObjectInternal;

/// S3 object information with display methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}

impl S3Object {
    /// Format size in human-readable form
    pub fn format_size(&self, human_readable: bool) -> String {
        if human_readable {
            format_bytes(self.size)
        } else {
            self.size.to_string()
        }
    }

    /// Format last modified time
    pub fn format_time(&self) -> String {
        self.last_modified.as_ref()
            .map(|s| s.clone())
            .unwrap_or_else(|| "Unknown".to_string())
    }
}

/// Object metadata for stat command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub etag: String,
    pub content_type: Option<String>,
    pub storage_class: Option<String>,
    pub metadata: std::collections::HashMap<String, String>,
}

/// Sync operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub uploaded: usize,
    pub downloaded: usize,
    pub deleted: usize,
    pub skipped: usize,
    pub total_bytes: u64,
}

impl SyncResult {
    pub fn new() -> Self {
        Self {
            uploaded: 0,
            downloaded: 0,
            deleted: 0,
            skipped: 0,
            total_bytes: 0,
        }
    }
}

impl Default for SyncResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Operation type for progress tracking
#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Upload,
    Download,
    Delete,
    List,
    Sync,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Upload => write!(f, "Upload"),
            OperationType::Download => write!(f, "Download"),
            OperationType::Delete => write!(f, "Delete"),
            OperationType::List => write!(f, "List"),
            OperationType::Sync => write!(f, "Sync"),
        }
    }
}

// ============================================================================
// Utility functions
// ============================================================================

/// Format bytes in human-readable form (B, KB, MB, GB, TB) - verbose
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let bytes_f64 = bytes as f64;
    let exponent = (bytes_f64.ln() / 1024_f64.ln()).floor() as usize;
    let exponent = exponent.min(UNITS.len() - 1);

    let value = bytes_f64 / 1024_f64.powi(exponent as i32);

    if exponent == 0 {
        format!("{} {}", bytes, UNITS[exponent])
    } else {
        format!("{:.2} {}", value, UNITS[exponent])
    }
}

/// Format bytes in mc-compatible compact form (0B, 1.0KiB, 10MiB, etc.)
pub fn format_bytes_compact(bytes: u64) -> String {
    let mut buf = String::with_capacity(8);
    write_bytes_compact(&mut buf, bytes);
    buf
}

/// Write file size directly to a fmt::Write (avoids intermediate String allocation)
fn write_bytes_compact(w: &mut dyn std::fmt::Write, bytes: u64) {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

    if bytes == 0 {
        let _ = w.write_str("0B");
        return;
    }

    let bytes_f64 = bytes as f64;
    let exponent = (bytes_f64.ln() / 1024_f64.ln()).floor() as usize;
    let exponent = exponent.min(UNITS.len() - 1);

    let value = bytes_f64 / 1024_f64.powi(exponent as i32);

    if exponent == 0 {
        let _ = write!(w, "{}B", bytes);
    } else if value >= 10.0 {
        let _ = write!(w, "{:.0}{}", value, UNITS[exponent]);
    } else {
        let _ = write!(w, "{:.1}{}", value, UNITS[exponent]);
    }
}

/// Format an S3 date string to mc-compatible format [YYYY-MM-DD HH:MM:SS UTC]
pub fn format_s3_date(date_str: Option<&str>) -> String {
    let mut buf = String::with_capacity(24);
    write_s3_date(&mut buf, date_str);
    buf
}

/// Write formatted date directly to a fmt::Write (avoids intermediate String allocation)
fn write_s3_date(w: &mut dyn std::fmt::Write, date_str: Option<&str>) {
    if let Some(s) = date_str {
        // Input is like "2026-01-22T20:44:33.219Z"
        // Output should be "2026-01-22 20:44:33 UTC"
        if let Some(t_pos) = s.find('T') {
            let date_part = &s[..t_pos];
            let time_rest = &s[t_pos + 1..];
            let time_part = if time_rest.len() >= 8 {
                &time_rest[..8]
            } else {
                time_rest.trim_end_matches('Z')
            };
            let _ = write!(w, "{} {} UTC", date_part, time_part);
            return;
        }
        let _ = w.write_str(s);
    } else {
        let _ = w.write_str("                   "); // blank placeholder matching width
    }
}

/// Calculate progress percentage
pub fn calculate_progress(current: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (current as f64 / total as f64) * 100.0
    }
}

// ============================================================================
// Stub functions for main.rs CLI entry point
// ============================================================================

use crate::core::Core;

/// List objects command — streaming XML parser.
///
/// Key optimizations:
/// - Single endpoint for all pages (TLS connection reuse, no per-page handshake)
/// - Streaming XML parser: parse + output inline, no Vec<S3Object> intermediary
/// - Reusable buffers for all string formatting (zero per-object allocation)
/// - BufWriter stdout (one syscall per flush, not per line)
/// - Retry with endpoint rotation on failure
pub async fn cmd_ls(core: &Core, path: &str, recursive: bool, max_keys: usize) -> Result<()> {
    use std::io::Write;
    use quick_xml::events::Event;
    use quick_xml::Reader;

    // Parse the S3 path
    let (bucket, prefix) = crate::cli::args::parse_s3_path(path)?;

    // Get S3 client with bucket from path
    let client = core.s3_client()?.with_bucket(bucket.clone());

    // Buffer stdout to avoid per-line flush syscalls (huge win for large listings)
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::with_capacity(64 * 1024, stdout.lock());

    let mut total_entries = 0usize;
    let limit = if max_keys == 0 { usize::MAX } else { max_keys };

    // Use delimiter "/" for non-recursive listing to get CommonPrefixes
    let delimiter = if recursive { None } else { Some("/") };

    // Track seen common prefixes to avoid duplicates across pages
    let mut seen_prefixes = std::collections::HashSet::new();

    // Pre-format "now" once for directory entries (mc uses current time for dirs)
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    // Reusable output formatting buffers — allocated once, cleared per object
    let mut line_buf = String::with_capacity(256);
    let mut size_buf = String::with_capacity(16);

    // Reusable XML field buffers — no per-object String allocation
    let mut obj_key = String::with_capacity(256);
    let mut obj_last_modified = String::with_capacity(32);
    let mut obj_storage_class = String::with_capacity(16);
    let mut obj_size: u64 = 0;
    let mut current_text = String::with_capacity(256);
    let mut cp_prefix = String::with_capacity(256);

    // Fetch first page with endpoint rotation on failure.
    // Try different endpoints if connection fails (backend may be down).
    // Uses single-attempt requests to fail fast on dead backends (~100ms vs 700ms).
    // Failed endpoints are marked unhealthy so future requests skip them.
    // Once successful, reuse the same endpoint for all remaining pages
    // to benefit from TLS connection reuse and server-side caching.
    let (endpoint, raw_xml) = {
        let mut last_err = None;
        let mut result = None;
        for retry in 0..5 {
            let (ep, idx) = core.select_endpoint_with_index()?;
            let ep = ep.to_string();
            match client.list_objects_v2_raw_once(
                &ep,
                prefix.as_deref(),
                None,  // Let server decide max keys per page
                None,
                delimiter,
            ).await {
                Ok(bytes) => {
                    core.record_success(idx);
                    result = Some((ep, bytes));
                    break;
                }
                Err(e) => {
                    core.record_failure(idx);
                    tracing::debug!("Fetch from {} failed ({}), trying another endpoint", ep, e);
                    last_err = Some(e);
                }
            }
            if retry == 4 {
                return Err(anyhow::anyhow!("All endpoints failed for first page: {}", last_err.unwrap()));
            }
        }
        result.ok_or_else(|| anyhow::anyhow!("All endpoints failed"))?
    };

    // Sequential pagination with endpoint rotation
    // MinIO throttles per-connection, so rotating endpoints is faster
    let mut continuation_token: Option<String> = None;
    let mut is_truncated;
    let mut raw_xml = raw_xml;

    loop {
        // Parse XML page and write output directly.
        let mut reader = Reader::from_reader(raw_xml.as_ref());
        reader.config_mut().trim_text_start = true;
        reader.config_mut().trim_text_end = true;

        is_truncated = false;
        continuation_token = None;
        let mut in_contents = false;
        let mut in_common_prefixes = false;

        loop {
            match reader.read_event() {
                Ok(Event::Start(e)) => {
                    match e.local_name().as_ref() {
                        b"Contents" => {
                            in_contents = true;
                            obj_key.clear();
                            obj_last_modified.clear();
                            obj_storage_class.clear();
                            obj_size = 0;
                        }
                        b"CommonPrefixes" => {
                            in_common_prefixes = true;
                            cp_prefix.clear();
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    current_text.clear();
                    let unescaped = e.unescape()
                        .map_err(|err| anyhow::anyhow!("XML unescape error: {}", err))?;
                    current_text.push_str(&unescaped);
                }
                Ok(Event::End(e)) => {
                    match e.local_name().as_ref() {
                        b"Key" if in_contents => {
                            obj_key.clear();
                            obj_key.push_str(&current_text);
                        }
                        b"Size" if in_contents => {
                            obj_size = current_text.parse().unwrap_or(0);
                        }
                        b"LastModified" if in_contents => {
                            obj_last_modified.clear();
                            obj_last_modified.push_str(&current_text);
                        }
                        b"StorageClass" if in_contents => {
                            obj_storage_class.clear();
                            obj_storage_class.push_str(&current_text);
                        }
                        b"IsTruncated" => {
                            is_truncated = current_text == "true";
                        }
                        b"NextContinuationToken" => {
                            continuation_token = Some(current_text.clone());
                        }
                        b"Contents" => {
                            in_contents = false;
                            if total_entries >= limit {
                                continue;
                            }

                            // Write output immediately — no S3Object allocation
                            let display_key = if let Some(ref p) = prefix {
                                obj_key.strip_prefix(p.as_str()).unwrap_or(&obj_key)
                            } else {
                                &obj_key
                            };

                            line_buf.clear();
                            line_buf.push('[');
                            write_s3_date(
                                &mut line_buf,
                                if obj_last_modified.is_empty() { None } else { Some(&obj_last_modified) },
                            );
                            line_buf.push_str("] ");

                            size_buf.clear();
                            write_bytes_compact(&mut size_buf, obj_size);
                            // Right-align size in 6 chars
                            for _ in 0..6usize.saturating_sub(size_buf.len()) {
                                line_buf.push(' ');
                            }
                            line_buf.push_str(&size_buf);
                            line_buf.push(' ');
                            line_buf.push_str(
                                if obj_storage_class.is_empty() { "STANDARD" } else { &obj_storage_class },
                            );
                            line_buf.push(' ');
                            line_buf.push_str(display_key);
                            line_buf.push('\n');

                            out.write_all(line_buf.as_bytes())?;
                            total_entries += 1;
                        }
                        b"Prefix" if in_common_prefixes => {
                            cp_prefix.clear();
                            cp_prefix.push_str(&current_text);
                        }
                        b"CommonPrefixes" => {
                            in_common_prefixes = false;
                            if total_entries >= limit {
                                continue;
                            }
                            if !seen_prefixes.insert(cp_prefix.clone()) {
                                continue;
                            }
                            let display_prefix = if let Some(ref p) = prefix {
                                cp_prefix.strip_prefix(p.as_str()).unwrap_or(&cp_prefix)
                            } else {
                                &cp_prefix
                            };
                            writeln!(out, "[{}]     0B {}", now, display_prefix)?;
                            total_entries += 1;
                        }
                        _ => {}
                    }
                    current_text.clear();
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(anyhow::anyhow!("XML parse error: {}", e));
                }
                _ => {}
            }
        }

        // Check if we've reached the limit or no more pages
        if total_entries >= limit || !is_truncated {
            break;
        }

        // Fetch next page - rotate endpoint to avoid per-connection throttling
        // Retry with different endpoints if connection fails
        if let Some(token) = continuation_token.as_deref() {
            let mut last_err = None;
            let mut fetched = false;
            for _retry in 0..5 {
                let (ep, idx) = core.select_endpoint_with_index()?;
                let ep = ep.to_string();
                match client.list_objects_v2_raw_once(
                    &ep,
                    prefix.as_deref(),
                    None,
                    Some(token),
                    delimiter,
                ).await {
                    Ok(bytes) => {
                        core.record_success(idx);
                        raw_xml = bytes;
                        fetched = true;
                        break;
                    }
                    Err(e) => {
                        core.record_failure(idx);
                        tracing::debug!("Page fetch from {} failed ({}), trying another endpoint", ep, e);
                        last_err = Some(e);
                    }
                }
            }
            if !fetched {
                return Err(anyhow::anyhow!("All endpoints failed for page fetch: {:?}", last_err));
            }
        } else {
            break;
        }
    }

    // Flush remaining output
    out.flush()?;

    if total_entries == 0 {
        println!("No objects found");
    }

    Ok(())
}

/// Copy command - mc-compatible behavior with automatic multipart for large files
///
/// Like mc, automatically uses multipart upload for files larger than 64MB.
/// Part size scales with file size to stay within S3's 10,000 part limit.
pub async fn cmd_cp(
    core: &Core,
    source: &str,
    dest: &str,
    recursive: bool,
    workers: usize,
) -> Result<()> {
    use crate::s3::MultipartConfig;

    let cmd = CmdCp::new(source, dest, recursive, workers, true, None, None)?;

    // Determine the bucket from the S3 source or destination
    let bucket_name = match (&cmd.source, &cmd.dest) {
        (CpSource::S3 { bucket, .. }, _) => bucket.clone(),
        (_, CpDestination::S3 { bucket, .. }) => bucket.clone(),
        _ => anyhow::bail!("At least one of source or destination must be an S3 path"),
    };

    // Get S3 client (with bucket from path)
    let client = core.s3_client()?.with_bucket(bucket_name);

    if cmd.is_upload() {
        // Upload from local to S3
        if let (CpSource::Local(local_path), CpDestination::S3 { bucket, key }) =
            (&cmd.source, &cmd.dest) {

            // Get file size to decide on upload strategy
            let metadata = std::fs::metadata(local_path)?;
            let file_size = metadata.len();

            // mc-compatible threshold: 64MB for multipart
            const MULTIPART_THRESHOLD: u64 = 64 * 1024 * 1024;

            if file_size < MULTIPART_THRESHOLD {
                // Small file: simple PUT (like mc)
                let data = std::fs::read(local_path)?;
                println!("{} -> s3://{}/{}", local_path.display(), bucket, key);

                let endpoint = core.select_endpoint()?;
                client.put_object(endpoint, key, Bytes::from(data)).await?;
            } else {
                // Large file: multipart upload (like mc)
                println!("{} -> s3://{}/{} (multipart)", local_path.display(), bucket, key);

                // Calculate optimal part size (mc uses 64MB minimum, scales up for large files)
                // S3 allows max 10,000 parts, so part_size = max(64MB, file_size / 10000)
                let min_part_size = 64 * 1024 * 1024; // 64MB like mc
                let parts_at_min = (file_size + min_part_size as u64 - 1) / min_part_size as u64;
                let part_size = if parts_at_min > 10000 {
                    // Scale up part size to stay under 10,000 parts
                    ((file_size / 9999) + 1) as usize
                } else {
                    min_part_size
                };

                let config = MultipartConfig::default()
                    .with_part_size(part_size)
                    .with_threshold(MULTIPART_THRESHOLD)
                    .with_concurrency(workers.max(4)); // mc uses 4 concurrent parts by default

                let endpoint = core.select_endpoint()?;
                let num_parts = (file_size as usize + part_size - 1) / part_size;
                println!("  {} parts of {} each, {} concurrent uploads",
                    num_parts, format_bytes(part_size as u64), config.concurrency);

                client.upload_file(endpoint, key, local_path, &config).await?;
            }

            println!("  {} uploaded", format_bytes(file_size));
        }
    } else if cmd.is_download() {
        // Download from S3 to local (streaming - no full buffering)
        if let (CpSource::S3 { bucket, key }, CpDestination::Local(local_path)) =
            (&cmd.source, &cmd.dest) {

            println!("s3://{}/{} -> {}", bucket, key, local_path.display());

            // Create parent directories if needed
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Select endpoint per download (distributes load across backends)
            let endpoint = core.select_endpoint()?;
            let bytes_written = client.download_object_to_file(endpoint, key, local_path).await?;
            println!("  {} downloaded", format_bytes(bytes_written));
        }
    } else {
        anyhow::bail!("S3-to-S3 copy not yet implemented");
    }

    Ok(())
}

/// Remove command
pub async fn cmd_rm(core: &Core, path: &str, recursive: bool, force: bool) -> Result<()> {
    // Parse the S3 path
    let (bucket, key) = crate::cli::args::parse_s3_path(path)?;

    // Get S3 client (with bucket from path)
    let client = core.s3_client()?.with_bucket(bucket.clone());

    if let Some(key) = key {
        if recursive {
            // Recursive delete: delete all objects with the prefix
            println!("Recursively deleting objects with prefix: {}", key);

            // Single endpoint for listing (TLS connection reuse across pages)
            let list_endpoint = core.select_endpoint()?.to_string();
            let mut continuation_token = None;
            let mut total_deleted = 0;

            loop {
                // List objects with prefix (single backend for connection reuse)
                let response = client.list_objects_v2(
                    &list_endpoint,
                    Some(&key),
                    Some(1000),
                    continuation_token.as_deref(),
                    None,
                ).await?;

                if !response.contents.is_empty() {
                    let keys: Vec<String> = response.contents.iter()
                        .map(|obj| obj.key.clone())
                        .collect();

                    // Delete batch spread across backends (load distribution)
                    let delete_ep = core.select_endpoint()?;
                    let delete_response = client.delete_objects(delete_ep, &keys).await?;
                    total_deleted += delete_response.deleted.len();

                    for deleted in &delete_response.deleted {
                        println!("Deleted: {}", deleted.key);
                    }

                    if !delete_response.errors.is_empty() {
                        for error in &delete_response.errors {
                            println!("Error deleting {}: {} - {}", error.key, error.code, error.message);
                        }
                    }
                }

                // Check if there are more objects
                if response.is_truncated {
                    continuation_token = response.next_continuation_token;
                } else {
                    break;
                }
            }

            println!("Total deleted: {} objects", total_deleted);
        } else {
            // Delete single object — select endpoint for the operation
            if !force {
                print!("Delete s3://{}/{}? [y/N]: ", bucket, key);
                use std::io::{self, BufRead};
                let stdin = io::stdin();
                let mut line = String::new();
                stdin.lock().read_line(&mut line)?;
                if !line.trim().eq_ignore_ascii_case("y") {
                    println!("Delete cancelled");
                    return Ok(());
                }
            }

            let endpoint = core.select_endpoint()?;
            client.delete_object(endpoint, &key).await?;
            println!("Deleted: s3://{}/{}", bucket, key);
        }
    } else {
        anyhow::bail!("Object key is required for delete operation");
    }

    Ok(())
}

/// Sync command
pub async fn cmd_sync(
    core: &Core,
    source: &str,
    dest: &str,
    workers: usize,
    delete: bool,
) -> Result<()> {
    let cmd = CmdSync::new(source, dest, delete, workers, false, true)?;

    println!("Sync operation not fully implemented yet");
    println!("Source: {}", source);
    println!("Destination: {}", dest);
    println!("Workers: {}", workers);
    println!("Delete: {}", delete);
    println!();
    println!("This command requires more complex logic to:");
    println!("  1. Compare source and destination");
    println!("  2. Determine what needs to be synced");
    println!("  3. Handle parallel transfers");
    println!("  4. Optionally delete files not in source");
    println!();
    println!("Use 'cp' command for individual file transfers.");

    Ok(())
}

/// Stat command
pub async fn cmd_stat(core: &Core, path: &str) -> Result<()> {
    // Parse the S3 path
    let (bucket, key) = crate::cli::args::parse_s3_path(path)?;
    let key = key.ok_or_else(|| anyhow::anyhow!("Object key is required for stat command"))?;

    // Get S3 client (with bucket from path) and endpoint
    let client = core.s3_client()?.with_bucket(bucket.clone());
    let endpoint = core.select_endpoint()?;

    // Get object metadata by listing with the exact key as prefix
    let response = client.list_objects_v2(
        endpoint,
        Some(&key),
        Some(1),
        None,
        None,
    ).await?;

    if response.contents.is_empty() {
        anyhow::bail!("Object not found: {}", key);
    }

    let obj = &response.contents[0];

    // Display object information
    println!("Object: s3://{}/{}", bucket, obj.key);
    println!("Size: {} ({})", format_bytes(obj.size), obj.size);
    println!("Last Modified: {}", obj.last_modified.as_deref().unwrap_or("Unknown"));
    if let Some(ref etag) = obj.etag {
        println!("ETag: {}", etag);
    }

    Ok(())
}

/// Make bucket command
pub async fn cmd_mb(core: &Core, bucket: &str) -> Result<()> {
    // Parse the S3 path to extract bucket name
    let (bucket_name, _) = crate::cli::args::parse_s3_path(bucket)?;

    // Get S3 client (with bucket from path) and endpoint
    let client = core.s3_client()?.with_bucket(bucket_name.clone());
    let endpoint = core.select_endpoint()?;

    // Create the bucket
    client.create_bucket(endpoint, &bucket_name).await?;

    println!("Bucket created: s3://{}", bucket_name);

    Ok(())
}

/// Remove bucket command
pub async fn cmd_rb(core: &Core, bucket: &str, force: bool) -> Result<()> {
    // Parse the S3 path to extract bucket name
    let (bucket_name, _) = crate::cli::args::parse_s3_path(bucket)?;

    // Get S3 client (with bucket from path)
    let client = core.s3_client()?.with_bucket(bucket_name.clone());

    // If force flag is set, delete all objects in the bucket first
    if force {
        println!("Force delete enabled - removing all objects from bucket...");

        // Single endpoint for listing (TLS connection reuse across pages)
        let list_endpoint = core.select_endpoint()?.to_string();
        let mut continuation_token = None;
        let mut total_deleted = 0;

        loop {
            // List objects (single backend for connection reuse)
            let response = client.list_objects_v2(
                &list_endpoint,
                None,
                Some(1000),
                continuation_token.as_deref(),
                None,
            ).await?;

            if !response.contents.is_empty() {
                // Collect keys to delete (max 1000 per batch)
                let keys: Vec<String> = response.contents.iter()
                    .map(|obj| obj.key.clone())
                    .collect();

                // Delete batch spread across backends (load distribution)
                let delete_ep = core.select_endpoint()?;
                let delete_response = client.delete_objects(delete_ep, &keys).await?;
                total_deleted += delete_response.deleted.len();

                if !delete_response.errors.is_empty() {
                    println!("Warning: {} objects failed to delete", delete_response.errors.len());
                    for error in &delete_response.errors {
                        println!("  Error deleting {}: {} - {}", error.key, error.code, error.message);
                    }
                }
            }

            // Check if there are more objects
            if response.is_truncated {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        if total_deleted > 0 {
            println!("Deleted {} objects from bucket", total_deleted);
        }
    }

    // Delete the bucket (select endpoint for this operation)
    let endpoint = core.select_endpoint()?;
    client.delete_bucket(endpoint, &bucket_name).await?;

    println!("Bucket deleted: s3://{}", bucket_name);

    Ok(())
}

/// Disk usage command
pub async fn cmd_du(core: &Core, path: &str) -> Result<()> {
    // Parse the S3 path
    let (bucket, prefix) = crate::cli::args::parse_s3_path(path)?;

    // Get S3 client (with bucket from path) and endpoint
    let client = core.s3_client()?.with_bucket(bucket.clone());
    let endpoint = core.select_endpoint()?;

    // Calculate total size
    let mut continuation_token = None;
    let mut total_size: u64 = 0;
    let mut object_count = 0;

    println!("Calculating disk usage for s3://{}/{}...", bucket, prefix.as_deref().unwrap_or(""));

    loop {
        let response = client.list_objects_v2(
            endpoint,
            prefix.as_deref(),
            Some(1000),
            continuation_token.as_deref(),
            None,
        ).await?;

        for obj in &response.contents {
            total_size += obj.size;
            object_count += 1;
        }

        // Check if there are more objects
        if response.is_truncated {
            continuation_token = response.next_continuation_token;
        } else {
            break;
        }
    }

    println!();
    println!("Total objects: {}", object_count);
    println!("Total size: {} ({})", format_bytes(total_size), total_size);

    Ok(())
}

/// Find command
pub async fn cmd_find(
    core: &Core,
    path: &str,
    name: Option<&str>,
    larger: Option<&str>,
    smaller: Option<&str>,
) -> Result<()> {
    // Parse the S3 path
    let (bucket, prefix) = crate::cli::args::parse_s3_path(path)?;

    // Get S3 client (with bucket from path) and endpoint
    let client = core.s3_client()?.with_bucket(bucket.clone());
    let endpoint = core.select_endpoint()?;

    // Parse size filters
    let min_size = if let Some(larger_str) = larger {
        Some(parse_size(larger_str)?)
    } else {
        None
    };

    let max_size = if let Some(smaller_str) = smaller {
        Some(parse_size(smaller_str)?)
    } else {
        None
    };

    // Find matching objects
    let mut continuation_token = None;
    let mut found_count = 0;

    loop {
        let response = client.list_objects_v2(
            endpoint,
            prefix.as_deref(),
            Some(1000),
            continuation_token.as_deref(),
            None,
        ).await?;

        for obj in &response.contents {
            // Check name pattern
            if let Some(pattern) = name {
                if !obj.key.contains(pattern) {
                    continue;
                }
            }

            // Check size filters
            if let Some(min) = min_size {
                if obj.size < min {
                    continue;
                }
            }

            if let Some(max) = max_size {
                if obj.size > max {
                    continue;
                }
            }

            // Object matches all filters
            println!("{:>12}  {}  s3://{}/{}",
                format_bytes(obj.size),
                obj.last_modified.as_deref().unwrap_or("Unknown"),
                bucket,
                obj.key
            );
            found_count += 1;
        }

        // Check if there are more objects
        if response.is_truncated {
            continuation_token = response.next_continuation_token;
        } else {
            break;
        }
    }

    if found_count == 0 {
        println!("No matching objects found");
    } else {
        println!("\nFound {} matching objects", found_count);
    }

    Ok(())
}

/// Parse size string like "10M", "1G", etc.
fn parse_size(size_str: &str) -> Result<u64> {
    let size_str = size_str.trim().to_uppercase();
    let (num_str, multiplier) = if size_str.ends_with('K') {
        (&size_str[..size_str.len()-1], 1024u64)
    } else if size_str.ends_with('M') {
        (&size_str[..size_str.len()-1], 1024u64 * 1024)
    } else if size_str.ends_with('G') {
        (&size_str[..size_str.len()-1], 1024u64 * 1024 * 1024)
    } else if size_str.ends_with('T') {
        (&size_str[..size_str.len()-1], 1024u64 * 1024 * 1024 * 1024)
    } else {
        (size_str.as_str(), 1u64)
    };

    let num: u64 = num_str.parse()
        .map_err(|_| anyhow::anyhow!("Invalid size format: {}", size_str))?;

    Ok(num * multiplier)
}

/// Speed test command
pub async fn cmd_speedtest(core: &Core, path: &str, duration: u64) -> Result<()> {
    use std::time::{Duration, Instant};

    // Parse the S3 path
    let (bucket, prefix) = crate::cli::args::parse_s3_path(path)?;
    let test_key = format!("{}/speedtest-{}.dat",
        prefix.as_deref().unwrap_or(""),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );

    // Get S3 client (with bucket from path) and endpoint
    let client = core.s3_client()?.with_bucket(bucket.clone());
    let endpoint = core.select_endpoint()?;

    println!("Running speed test for {} seconds...", duration);
    println!("Endpoint: {}", endpoint);
    println!("Test object: s3://{}/{}", bucket, test_key);
    println!();

    // Generate test data (1MB) as Bytes for zero-copy put_object
    let test_data = Bytes::from(vec![0u8; 1024 * 1024]);
    let test_data_len = test_data.len() as u64;
    let test_duration = Duration::from_secs(duration);

    // Upload speed test - load-balance across backends
    println!("Testing upload speed...");
    let upload_start = Instant::now();
    let mut upload_count = 0;
    let mut upload_bytes = 0u64;

    while upload_start.elapsed() < test_duration {
        let key = format!("{}-{}", test_key, upload_count);
        // Select different endpoint per request for load distribution
        let ep = core.select_endpoint()?;
        // Bytes::clone() is cheap (reference-counted, no data copy)
        client.put_object(ep, &key, test_data.clone()).await?;
        upload_count += 1;
        upload_bytes += test_data_len;
    }

    let upload_elapsed = upload_start.elapsed().as_secs_f64();
    let upload_mbps = (upload_bytes as f64 / 1024.0 / 1024.0) / upload_elapsed;

    println!("Upload: {} MB in {:.2}s = {:.2} MB/s",
        upload_bytes / 1024 / 1024,
        upload_elapsed,
        upload_mbps
    );

    // Download speed test - load-balance across backends
    println!("\nTesting download speed...");
    let download_start = Instant::now();
    let mut download_count = 0;
    let mut download_bytes = 0u64;

    while download_start.elapsed() < test_duration && download_count < upload_count {
        let key = format!("{}-{}", test_key, download_count);
        // Select different endpoint per request for load distribution
        let ep = core.select_endpoint()?;
        let data = client.get_object(ep, &key).await?;
        download_count += 1;
        download_bytes += data.len() as u64;
    }

    let download_elapsed = download_start.elapsed().as_secs_f64();
    let download_mbps = (download_bytes as f64 / 1024.0 / 1024.0) / download_elapsed;

    println!("Download: {} MB in {:.2}s = {:.2} MB/s",
        download_bytes / 1024 / 1024,
        download_elapsed,
        download_mbps
    );

    // Cleanup test objects
    println!("\nCleaning up test objects...");
    let keys: Vec<String> = (0..upload_count)
        .map(|i| format!("{}-{}", test_key, i))
        .collect();

    if !keys.is_empty() {
        // Delete in batches of 1000, load-balanced
        for chunk in keys.chunks(1000) {
            let ep = core.select_endpoint()?;
            client.delete_objects(ep, chunk).await?;
        }
    }

    println!("Speed test complete.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_calculate_progress() {
        assert_eq!(calculate_progress(0, 100), 0.0);
        assert_eq!(calculate_progress(50, 100), 50.0);
        assert_eq!(calculate_progress(100, 100), 100.0);
        assert_eq!(calculate_progress(0, 0), 0.0);
    }

    #[test]
    fn test_cmd_cp_is_upload() {
        let cmd = CmdCp::new(
            "/local/file.txt",
            "s3://bucket/key",
            false,
            1,
            false,
            None,
            None,
        )
        .unwrap();
        assert!(cmd.is_upload());
        assert!(!cmd.is_download());
    }

    #[test]
    fn test_cmd_cp_is_download() {
        let cmd = CmdCp::new(
            "s3://bucket/key",
            "/local/file.txt",
            false,
            1,
            false,
            None,
            None,
        )
        .unwrap();
        assert!(!cmd.is_upload());
        assert!(cmd.is_download());
    }
}

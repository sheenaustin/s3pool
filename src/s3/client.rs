//! S3 client implementation with core operations
//!
//! Optimized with:
//! - HTTP/1.1 only (no HTTP/2, matching mc behavior for S3 workloads)
//! - Tuned connection pool (1024 idle per host, 90s timeout, matching mc)
//! - TCP_NODELAY for low latency
//! - native-tls (OpenSSL) for TLS
//! - Zero-copy Bytes for get/put operations
//! - UNSIGNED-PAYLOAD for PUT (skips SHA256 of body)
//! - Optimized XML parsing with byte-slice tag matching
//! - Automatic retry with jitter for 429/503 responses

use crate::s3::signer::S3SignerV4;
use crate::s3::types::{
    DeleteError, DeleteObjectsResponse, DeletedObject, ListObjectsResponse, S3Object,
    // Multipart upload types
    CreateMultipartUploadResponse, UploadPartResponse, CompletedPart,
    CompleteMultipartUploadResponse, MultipartConfig,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use native_tls::TlsConnector;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::time::Duration;
use thiserror::Error;

/// Hex lookup table for URI encoding
static HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// S3 client errors
#[derive(Error, Debug)]
pub enum S3Error {
    #[error("HTTP error: {0}")]
    Http(#[from] hyper::http::Error),

    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("XML parse error: {0}")]
    XmlParse(String),

    #[error("S3 error: {status} - {message}")]
    S3Response { status: StatusCode, message: String },

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

impl From<quick_xml::Error> for S3Error {
    fn from(err: quick_xml::Error) -> Self {
        S3Error::XmlParse(format!("XML parse error: {}", err))
    }
}

pub type Result<T> = std::result::Result<T, S3Error>;

impl From<hyper_util::client::legacy::Error> for S3Error {
    fn from(err: hyper_util::client::legacy::Error) -> Self {
        S3Error::InvalidResponse(format!("Client error: {}", err))
    }
}

/// Simple pseudo-random jitter (0.0 - 1.0) without pulling in rand crate.
/// Uses current time nanoseconds as entropy source.
fn rand_jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}

/// High-performance S3 client
///
/// Clone is cheap - the underlying HTTP client uses Arc internally.
#[derive(Clone)]
pub struct S3Client {
    /// Hyper HTTP client with tuned connection pool
    client: HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    /// AWS SigV4 signer (with signing key cache)
    signer: S3SignerV4,
    /// S3 bucket name
    bucket: String,
    /// Request timeout
    timeout: Duration,
}

impl S3Client {
    /// Create a new S3 client with optimized HTTP settings
    ///
    /// HTTP optimizations matching mc/sidekick:
    /// - HTTP/1.1 only (HTTP/2 disabled, as mc does)
    /// - 1024 idle connections per host
    /// - 90s idle connection timeout
    /// - TCP_NODELAY enabled
    /// - 90s TCP keepalive
    pub fn new(
        access_key: String,
        secret_key: String,
        bucket: String,
        region: Option<String>,
    ) -> Self {
        let insecure_tls = std::env::var("S3POOL_INSECURE_TLS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        // Configure TCP connector with performance settings
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        http.enforce_http(false);
        http.set_connect_timeout(Some(Duration::from_secs(10)));
        http.set_keepalive(Some(Duration::from_secs(90)));

        // Build TLS connector using native-tls (OpenSSL)
        let tls = if insecure_tls {
            tracing::warn!("INSECURE TLS MODE ENABLED: Certificate verification is disabled!");
            TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .expect("Failed to build TLS connector")
        } else {
            TlsConnector::new().expect("Failed to build TLS connector")
        };

        let https = HttpsConnector::from((http, tls.into()));

        // Create hyper client with tuned connection pool matching mc
        let client = HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(1024)
            .retry_canceled_requests(true)
            .set_host(true)
            .build(https);

        let signer = S3SignerV4::new(access_key, secret_key, region);

        Self {
            client,
            signer,
            bucket,
            timeout: Duration::from_secs(300),
        }
    }

    /// Set request timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Override the bucket name
    pub fn with_bucket(mut self, bucket: String) -> Self {
        self.bucket = bucket;
        self
    }

    /// Send an HTTP request with automatic retry on 429/503.
    ///
    /// Retries up to 3 times with exponential backoff + jitter, matching mc behavior.
    /// Returns (status, body_bytes) on success or the last error.
    async fn request_with_retry(
        &self,
        method: Method,
        url: &str,
        headers: BTreeMap<String, String>,
        body: Bytes,
    ) -> std::result::Result<(StatusCode, Bytes), S3Error> {
        const MAX_RETRIES: u32 = 3;

        for attempt in 0..=MAX_RETRIES {
            // Re-sign on each attempt (timestamp changes)
            let signed_headers = if body.is_empty() {
                self.signer.sign(method.as_str(), url, headers.clone(), b"")
            } else {
                self.signer.sign_unsigned_payload(method.as_str(), url, headers.clone())
            };

            let mut req = Request::builder().method(method.clone()).uri(url);
            for (key, value) in signed_headers.iter() {
                req = req.header(key, value);
            }

            let request = req.body(Full::new(body.clone()))
                .map_err(|e| S3Error::InvalidResponse(format!("Request build error: {}", e)))?;

            match self.client.request(request).await {
                Ok(response) => {
                    let status = response.status();
                    let is_retryable = status == StatusCode::TOO_MANY_REQUESTS
                        || status == StatusCode::SERVICE_UNAVAILABLE;

                    if is_retryable && attempt < MAX_RETRIES {
                        // Drain body to return connection to pool, then retry
                        let _ = response.collect().await;
                        let base_ms = 100u64 * (1 << attempt);
                        let jitter = (base_ms as f64 * 0.2 * rand_jitter()) as u64;
                        tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
                        continue;
                    }

                    // Collect body (success or final attempt)
                    let body_bytes = response.collect().await
                        .map_err(|e| S3Error::InvalidResponse(format!("Body error: {}", e)))?
                        .to_bytes();
                    return Ok((status, body_bytes));
                }
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        let base_ms = 100u64 * (1 << attempt);
                        tokio::time::sleep(Duration::from_millis(base_ms)).await;
                        continue;
                    }
                    return Err(S3Error::InvalidResponse(format!("Request failed: {}", e)));
                }
            }
        }
        unreachable!()
    }

    /// Send a single HTTP request without retry.
    ///
    /// Used during endpoint discovery to fail fast on dead backends.
    async fn request_once(
        &self,
        method: Method,
        url: &str,
        headers: BTreeMap<String, String>,
        body: Bytes,
    ) -> std::result::Result<(StatusCode, Bytes), S3Error> {
        let signed_headers = if body.is_empty() {
            self.signer.sign(method.as_str(), url, headers, b"")
        } else {
            self.signer.sign_unsigned_payload(method.as_str(), url, headers)
        };

        let mut req = Request::builder().method(method).uri(url);
        for (key, value) in signed_headers.iter() {
            req = req.header(key, value);
        }

        let request = req.body(Full::new(body))
            .map_err(|e| S3Error::InvalidResponse(format!("Request build error: {}", e)))?;

        let response = self.client.request(request).await
            .map_err(|e| S3Error::InvalidResponse(format!("Request failed: {}", e)))?;

        let status = response.status();
        let body_bytes = response.collect().await
            .map_err(|e| S3Error::InvalidResponse(format!("Body error: {}", e)))?
            .to_bytes();

        Ok((status, body_bytes))
    }

    /// Encode an S3 key, preserving forward slashes
    /// Returns Cow::Borrowed when no encoding is needed (common case = zero allocation)
    fn encode_s3_key(key: &str) -> Cow<str> {
        let needs_encoding = key
            .bytes()
            .any(|b| !matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/'));

        if !needs_encoding {
            return Cow::Borrowed(key);
        }

        let mut result = String::with_capacity(key.len() + 32);
        for byte in key.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                    result.push(byte as char);
                }
                _ => {
                    result.push('%');
                    result.push(HEX_UPPER[(byte >> 4) as usize] as char);
                    result.push(HEX_UPPER[(byte & 0xf) as usize] as char);
                }
            }
        }
        Cow::Owned(result)
    }

    /// Build full S3 URL for a key with pre-allocated capacity
    fn build_url(&self, endpoint: &str, key: &str) -> String {
        let endpoint = endpoint.trim_end_matches('/');
        let encoded_key = Self::encode_s3_key(key);
        let mut url =
            String::with_capacity(endpoint.len() + 1 + self.bucket.len() + 1 + encoded_key.len());
        url.push_str(endpoint);
        url.push('/');
        url.push_str(&self.bucket);
        url.push('/');
        url.push_str(&encoded_key);
        url
    }

    /// Build bucket URL (no key) with pre-allocated capacity
    fn build_bucket_url(&self, endpoint: &str) -> String {
        let endpoint = endpoint.trim_end_matches('/');
        let mut url = String::with_capacity(endpoint.len() + 1 + self.bucket.len());
        url.push_str(endpoint);
        url.push('/');
        url.push_str(&self.bucket);
        url
    }

    /// Encode a string for use in a URL query parameter value (RFC 3986).
    /// Writes directly into the target buffer - zero intermediate allocation.
    fn url_encode_into(buf: &mut String, s: &str) {
        for byte in s.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    buf.push(byte as char);
                }
                _ => {
                    buf.push('%');
                    buf.push(HEX_UPPER[(byte >> 4) as usize] as char);
                    buf.push(HEX_UPPER[(byte & 0xf) as usize] as char);
                }
            }
        }
    }

    /// Pre-warm a connection to the given endpoint
    ///
    /// Makes a lightweight HEAD request to establish TCP+TLS connection.
    /// The connection is returned to the pool for reuse by subsequent operations.
    /// Errors are silently ignored (best-effort warmup).
    pub async fn warm_connection(&self, endpoint: &str) {
        let url = self.build_bucket_url(endpoint);
        let headers = self.signer.sign("HEAD", &url, BTreeMap::new(), b"");

        let mut req = Request::builder().method(Method::HEAD).uri(&url);
        for (key, value) in headers.iter() {
            req = req.header(key, value);
        }

        if let Ok(request) = req.body(Full::new(Bytes::new())) {
            if let Ok(response) = self.client.request(request).await {
                // Drain body to return connection to pool
                let _ = response.collect().await;
            }
        }
    }

    /// Build the full URL for a ListObjectsV2 request.
    ///
    /// Parameters are ordered alphabetically (c, d, l, m, p) so the signer's
    /// canonical query string fast path can skip re-sorting.
    pub fn build_list_url(
        &self,
        endpoint: &str,
        prefix: Option<&str>,
        max_keys: Option<i32>,
        continuation_token: Option<&str>,
        delimiter: Option<&str>,
    ) -> String {
        let base_url = self.build_bucket_url(endpoint);
        let max_keys_val = max_keys.unwrap_or(1000);

        let mut url = String::with_capacity(base_url.len() + 256);
        url.push_str(&base_url);
        url.push_str("/?");

        if let Some(token) = continuation_token {
            url.push_str("continuation-token=");
            Self::url_encode_into(&mut url, token);
            url.push('&');
        }
        if let Some(d) = delimiter {
            url.push_str("delimiter=");
            Self::url_encode_into(&mut url, d);
            url.push('&');
        }
        url.push_str("list-type=2&max-keys=");
        let _ = write!(url, "{}", max_keys_val);
        if let Some(p) = prefix {
            url.push_str("&prefix=");
            Self::url_encode_into(&mut url, p);
        }

        url
    }

    /// List objects in bucket (S3 ListObjectsV2)
    ///
    /// Returns a fully parsed `ListObjectsResponse`. For high-throughput listing
    /// where per-object output is needed, use `list_objects_v2_raw()` with
    /// inline XML parsing to avoid intermediate Vec<S3Object> allocations.
    /// Automatically retries on 429/503 with exponential backoff.
    pub async fn list_objects_v2(
        &self,
        endpoint: &str,
        prefix: Option<&str>,
        max_keys: Option<i32>,
        continuation_token: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<ListObjectsResponse> {
        let url = self.build_list_url(endpoint, prefix, max_keys, continuation_token, delimiter);

        let (status, body_bytes) = self.request_with_retry(
            Method::GET,
            &url,
            BTreeMap::new(),
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        self.parse_list_response(&body_bytes)
    }

    /// List objects V2 - raw response bytes for streaming parsers.
    ///
    /// Returns the raw XML response body. Callers parse inline to avoid
    /// intermediate `Vec<S3Object>` allocation, writing output during parse.
    /// This is the fastest path for listing operations (cmd_ls).
    pub async fn list_objects_v2_raw(
        &self,
        endpoint: &str,
        prefix: Option<&str>,
        max_keys: Option<i32>,
        continuation_token: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<Bytes> {
        let url = self.build_list_url(endpoint, prefix, max_keys, continuation_token, delimiter);

        let (status, body_bytes) = self.request_with_retry(
            Method::GET,
            &url,
            BTreeMap::new(),
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(body_bytes)
    }

    /// List objects V2 - single attempt (no retry).
    ///
    /// Used during endpoint discovery to fail fast on dead backends.
    /// Callers rotate endpoints on failure rather than retrying same one.
    pub async fn list_objects_v2_raw_once(
        &self,
        endpoint: &str,
        prefix: Option<&str>,
        max_keys: Option<i32>,
        continuation_token: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<Bytes> {
        let url = self.build_list_url(endpoint, prefix, max_keys, continuation_token, delimiter);

        let (status, body_bytes) = self.request_once(
            Method::GET,
            &url,
            BTreeMap::new(),
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(body_bytes)
    }

    /// Get object from S3 - returns Bytes (zero-copy, no .to_vec() needed)
    ///
    /// Automatically retries on 429/503 with exponential backoff.
    pub async fn get_object(&self, endpoint: &str, key: &str) -> Result<Bytes> {
        let url = self.build_url(endpoint, key);

        let (status, body_bytes) = self.request_with_retry(
            Method::GET,
            &url,
            BTreeMap::new(),
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(body_bytes)
    }

    /// Download object from S3 directly to a file (streaming)
    ///
    /// Streams response body chunks to a BufWriter-wrapped file.
    /// TCP kernel buffers ensure network reads continue during disk writes.
    /// BufWriter batches small chunks into 256KB writes for efficiency.
    /// Automatically retries on 429/503 with exponential backoff.
    ///
    /// Returns the number of bytes written.
    pub async fn download_object_to_file(
        &self,
        endpoint: &str,
        key: &str,
        path: &std::path::Path,
    ) -> Result<u64> {
        let url = self.build_url(endpoint, key);

        const MAX_RETRIES: u32 = 3;
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            let headers = self.signer.sign("GET", &url, BTreeMap::new(), b"");
            let mut req = Request::builder().method(Method::GET).uri(&url);
            for (k, v) in headers.iter() {
                req = req.header(k, v);
            }

            let request = match req.body(Full::new(Bytes::new())) {
                Ok(r) => r,
                Err(e) => return Err(S3Error::InvalidResponse(format!("Request build error: {}", e))),
            };

            let response = match self.client.request(request).await {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(S3Error::InvalidResponse(format!("Request failed: {}", e)));
                    if attempt < MAX_RETRIES {
                        let base_ms = 100u64 * (1 << attempt);
                        tokio::time::sleep(Duration::from_millis(base_ms)).await;
                        continue;
                    }
                    return Err(last_err.unwrap());
                }
            };

            let status = response.status();

            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
                let _ = response.collect().await;
                if attempt < MAX_RETRIES {
                    let base_ms = 100u64 * (1 << attempt);
                    let jitter = (base_ms as f64 * 0.2 * rand_jitter()) as u64;
                    tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
                    continue;
                }
                return Err(S3Error::S3Response {
                    status,
                    message: "Too many retries".to_string(),
                });
            }

            if !status.is_success() {
                let body_bytes = response.collect().await
                    .map_err(|e| S3Error::InvalidResponse(format!("Body error: {}", e)))?
                    .to_bytes();
                let message = String::from_utf8_lossy(&body_bytes).to_string();
                return Err(S3Error::S3Response { status, message });
            }

            // Stream body to file. Sync write is fine for CLI (single operation).
            // BufWriter batches small network chunks into large disk writes.
            use http_body_util::BodyStream;
            use futures::StreamExt;
            use std::io::Write;

            let file = std::fs::File::create(path)?;
            let mut writer = std::io::BufWriter::with_capacity(256 * 1024, file);
            let mut body = BodyStream::new(response.into_body());
            let mut total_bytes = 0u64;

            while let Some(frame_result) = body.next().await {
                let frame = frame_result.map_err(|e| S3Error::InvalidResponse(format!("Body error: {}", e)))?;
                if let Some(chunk) = frame.data_ref() {
                    writer.write_all(chunk)?;
                    total_bytes += chunk.len() as u64;
                }
            }

            writer.flush()?;
            return Ok(total_bytes);
        }

        Err(last_err.unwrap_or_else(|| S3Error::InvalidResponse("Max retries exceeded".to_string())))
    }

    /// Put object to S3 - accepts Bytes (zero-copy) with UNSIGNED-PAYLOAD
    ///
    /// Uses UNSIGNED-PAYLOAD for the content hash, avoiding SHA256 computation
    /// of the entire body. This is safe for MinIO and most S3 implementations.
    /// Automatically retries on 429/503 with exponential backoff.
    pub async fn put_object(&self, endpoint: &str, key: &str, data: Bytes) -> Result<String> {
        let url = self.build_url(endpoint, key);

        let mut extra_headers = BTreeMap::new();
        extra_headers.insert(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        );
        extra_headers.insert("content-length".to_string(), data.len().to_string());

        // request_with_retry uses sign_unsigned_payload for non-empty bodies
        let (status, body_bytes) = self.request_with_retry(
            Method::PUT,
            &url,
            extra_headers,
            data,
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        // ETag is not easily available via request_with_retry, return empty
        Ok(String::new())
    }

    /// Delete object from S3
    ///
    /// Automatically retries on 429/503 with exponential backoff.
    pub async fn delete_object(&self, endpoint: &str, key: &str) -> Result<()> {
        let url = self.build_url(endpoint, key);

        let (status, body_bytes) = self.request_with_retry(
            Method::DELETE,
            &url,
            BTreeMap::new(),
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(())
    }

    /// Delete multiple objects from S3 (batch delete, up to 1000 keys)
    pub async fn delete_objects(
        &self,
        endpoint: &str,
        keys: &[String],
    ) -> Result<DeleteObjectsResponse> {
        if keys.is_empty() {
            return Ok(DeleteObjectsResponse::new());
        }

        if keys.len() > 1000 {
            return Err(S3Error::InvalidResponse(
                "Cannot delete more than 1000 objects at once".to_string(),
            ));
        }

        // Build delete XML
        let mut xml = String::with_capacity(keys.len() * 60 + 80);
        xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete>");
        for key in keys {
            xml.push_str("<Object><Key>");
            Self::xml_escape_into(&mut xml, key);
            xml.push_str("</Key></Object>");
        }
        xml.push_str("</Delete>");

        // Convert to bytes, compute hashes, then move into Bytes (zero copy)
        let xml_bytes = xml.into_bytes();

        // Calculate MD5 hash of body
        let md5_hash = md5::compute(&xml_bytes);
        let md5_base64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &md5_hash[..]);

        // Build URL with delete query parameter
        // Use "?delete=" (with explicit empty value) so the canonical query string
        // is "delete=" which matches AWS SigV4 expectations for valueless params.
        let base_url = self.build_bucket_url(endpoint);
        let url = format!("{}/?delete=", base_url);

        // Prepare headers
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/xml".to_string());
        headers.insert("content-length".to_string(), xml_bytes.len().to_string());
        headers.insert("content-md5".to_string(), md5_base64);

        // Sign request (needs to hash the XML body for SigV4)
        let headers = self.signer.sign("POST", &url, headers, &xml_bytes);

        let mut req = Request::builder().method(Method::POST).uri(&url);

        for (key, value) in headers.iter() {
            req = req.header(key, value);
        }

        // Move xml_bytes into Bytes (takes ownership, no copy)
        let request = req.body(Full::new(Bytes::from(xml_bytes)))?;
        let response = self.client.request(request).await?;
        let status = response.status();
        let body_bytes = response.collect().await?.to_bytes();

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        self.parse_delete_response(&body_bytes)
    }

    /// Parse ListObjectsV2 XML response
    ///
    /// Optimized with:
    /// - Byte-slice tag matching (no String allocation per tag, saves ~336K allocs for 24K objects)
    /// - std::mem::take for moving strings instead of cloning
    /// - Pre-allocated response Vec and text buffer
    fn parse_list_response(&self, xml_data: &[u8]) -> Result<ListObjectsResponse> {
        let mut reader = Reader::from_reader(xml_data);
        reader.config_mut().trim_text_start = true;
        reader.config_mut().trim_text_end = true;

        let mut response = ListObjectsResponse::new();
        response.contents.reserve(1000); // Pre-allocate for typical page size

        let mut current_object: Option<S3Object> = None;
        let mut current_text = String::with_capacity(256);
        let mut in_common_prefixes = false;

        loop {
            match reader.read_event() {
                Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                    // Byte-slice matching: no String allocation per tag
                    match e.local_name().as_ref() {
                        b"Contents" => {
                            current_object = Some(S3Object::new(String::new(), 0));
                        }
                        b"CommonPrefixes" => {
                            in_common_prefixes = true;
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    // Reuse buffer capacity
                    current_text.clear();
                    current_text.push_str(&e.unescape()?);
                }
                Ok(Event::End(e)) => {
                    // Byte-slice matching: no String allocation per tag
                    match e.local_name().as_ref() {
                        b"Key" => {
                            if let Some(ref mut obj) = current_object {
                                obj.key = std::mem::take(&mut current_text);
                            }
                        }
                        b"Size" => {
                            if let Some(ref mut obj) = current_object {
                                obj.size = current_text.parse().unwrap_or(0);
                            }
                        }
                        b"LastModified" => {
                            if let Some(ref mut obj) = current_object {
                                obj.last_modified = Some(std::mem::take(&mut current_text));
                            }
                        }
                        b"ETag" => {
                            if let Some(ref mut obj) = current_object {
                                obj.etag = Some(std::mem::take(&mut current_text));
                            }
                        }
                        b"StorageClass" => {
                            if let Some(ref mut obj) = current_object {
                                obj.storage_class = Some(std::mem::take(&mut current_text));
                            }
                        }
                        b"Contents" => {
                            if let Some(obj) = current_object.take() {
                                response.contents.push(obj);
                            }
                        }
                        b"CommonPrefixes" => {
                            in_common_prefixes = false;
                        }
                        b"Prefix" => {
                            if in_common_prefixes {
                                response
                                    .common_prefixes
                                    .push(std::mem::take(&mut current_text));
                            } else {
                                response.prefix = Some(std::mem::take(&mut current_text));
                            }
                        }
                        b"IsTruncated" => {
                            response.is_truncated = current_text == "true";
                        }
                        b"NextContinuationToken" => {
                            response.next_continuation_token =
                                Some(std::mem::take(&mut current_text));
                        }
                        b"MaxKeys" => {
                            response.max_keys = current_text.parse().ok();
                        }
                        b"KeyCount" => {
                            response.key_count = current_text.parse().ok();
                        }
                        _ => {}
                    }

                    current_text.clear();
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(S3Error::XmlParse(format!("XML parse error: {}", e)));
                }
                _ => {}
            }
        }

        Ok(response)
    }

    /// Parse DeleteObjects XML response (optimized with byte-slice matching)
    fn parse_delete_response(&self, xml_data: &[u8]) -> Result<DeleteObjectsResponse> {
        let mut reader = Reader::from_reader(xml_data);
        reader.config_mut().trim_text_start = true;
        reader.config_mut().trim_text_end = true;

        let mut response = DeleteObjectsResponse::new();
        let mut current_deleted: Option<DeletedObject> = None;
        let mut current_error: Option<DeleteError> = None;
        let mut current_text = String::with_capacity(256);

        loop {
            match reader.read_event() {
                Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                    match e.local_name().as_ref() {
                        b"Deleted" => {
                            current_deleted = Some(DeletedObject::new(String::new()));
                        }
                        b"Error" => {
                            current_error = Some(DeleteError {
                                key: String::new(),
                                code: String::new(),
                                message: String::new(),
                            });
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    current_text.clear();
                    current_text.push_str(&e.unescape()?);
                }
                Ok(Event::End(e)) => {
                    match e.local_name().as_ref() {
                        b"Key" => {
                            if let Some(ref mut deleted) = current_deleted {
                                deleted.key = std::mem::take(&mut current_text);
                            } else if let Some(ref mut error) = current_error {
                                error.key = std::mem::take(&mut current_text);
                            }
                        }
                        b"VersionId" => {
                            if let Some(ref mut deleted) = current_deleted {
                                deleted.version_id = Some(std::mem::take(&mut current_text));
                            }
                        }
                        b"Code" => {
                            if let Some(ref mut error) = current_error {
                                error.code = std::mem::take(&mut current_text);
                            }
                        }
                        b"Message" => {
                            if let Some(ref mut error) = current_error {
                                error.message = std::mem::take(&mut current_text);
                            }
                        }
                        b"Deleted" => {
                            if let Some(deleted) = current_deleted.take() {
                                response.deleted.push(deleted);
                            }
                        }
                        b"Error" => {
                            if let Some(error) = current_error.take() {
                                response.errors.push(error);
                            }
                        }
                        _ => {}
                    }

                    current_text.clear();
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    return Err(S3Error::XmlParse(format!("XML parse error: {}", e)));
                }
                _ => {}
            }
        }

        Ok(response)
    }

    /// Escape XML special characters into an existing buffer (no intermediate allocation)
    fn xml_escape_into(buf: &mut String, s: &str) {
        for ch in s.chars() {
            match ch {
                '&' => buf.push_str("&amp;"),
                '<' => buf.push_str("&lt;"),
                '>' => buf.push_str("&gt;"),
                '"' => buf.push_str("&quot;"),
                '\'' => buf.push_str("&apos;"),
                _ => buf.push(ch),
            }
        }
    }

    /// Escape XML special characters (legacy compat)
    fn xml_escape(s: &str) -> String {
        let mut buf = String::with_capacity(s.len() + 16);
        Self::xml_escape_into(&mut buf, s);
        buf
    }

    /// Forward a raw HTTP request to a backend URL (used by proxy).
    ///
    /// This is a transparent forwarding method: it takes the full backend URL,
    /// signs the request with AWS SigV4 credentials, sends it, and returns
    /// the raw response (status, headers, body).
    ///
    /// Extra headers (Content-Type, Content-MD5, etc.) are included in signing.
    /// For non-empty bodies, uses UNSIGNED-PAYLOAD to avoid hashing large uploads.
    pub async fn proxy_request(
        &self,
        method: Method,
        url: &str,
        extra_headers: BTreeMap<String, String>,
        body: Bytes,
    ) -> Result<(StatusCode, hyper::header::HeaderMap, Bytes)> {
        let signed_headers = if body.is_empty() {
            self.signer.sign(method.as_str(), url, extra_headers, b"")
        } else {
            let mut headers = extra_headers;
            if !headers.contains_key("content-length") {
                headers.insert("content-length".to_string(), body.len().to_string());
            }
            self.signer.sign_unsigned_payload(method.as_str(), url, headers)
        };

        let mut req = Request::builder().method(method).uri(url);
        for (key, value) in signed_headers.iter() {
            req = req.header(key, value);
        }

        let request = req
            .body(Full::new(body))
            .map_err(|e| S3Error::InvalidResponse(format!("Request build error: {}", e)))?;

        let response = self.client.request(request).await?;
        let status = response.status();
        let resp_headers = response.headers().clone();
        let body_bytes = response
            .collect()
            .await
            .map_err(|e| S3Error::InvalidResponse(format!("Body error: {}", e)))?
            .to_bytes();

        Ok((status, resp_headers, body_bytes))
    }

    /// Create a bucket (PUT bucket)
    pub async fn create_bucket(&self, endpoint: &str, bucket_name: &str) -> Result<()> {
        let endpoint = endpoint.trim_end_matches('/');
        let url = format!("{}/{}", endpoint, bucket_name);

        let headers = self.signer.sign("PUT", &url, BTreeMap::new(), b"");

        let mut req = Request::builder().method(Method::PUT).uri(&url);

        for (key, value) in headers.iter() {
            req = req.header(key, value);
        }

        let request = req.body(Full::new(Bytes::new()))?;
        let response = self.client.request(request).await?;
        let status = response.status();

        // Always drain body to return connection to pool
        let body_bytes = response.collect().await?.to_bytes();

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(())
    }

    /// Delete a bucket (DELETE bucket)
    pub async fn delete_bucket(&self, endpoint: &str, bucket_name: &str) -> Result<()> {
        let endpoint = endpoint.trim_end_matches('/');
        let url = format!("{}/{}", endpoint, bucket_name);

        let headers = self.signer.sign("DELETE", &url, BTreeMap::new(), b"");

        let mut req = Request::builder().method(Method::DELETE).uri(&url);

        for (key, value) in headers.iter() {
            req = req.header(key, value);
        }

        let request = req.body(Full::new(Bytes::new()))?;
        let response = self.client.request(request).await?;
        let status = response.status();

        // Always drain body to return connection to pool
        let body_bytes = response.collect().await?.to_bytes();

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(())
    }

    // =========================================================================
    // Multipart Upload Operations
    // =========================================================================

    /// Initiate a multipart upload (CreateMultipartUpload)
    ///
    /// Returns an upload ID that must be used in subsequent UploadPart and
    /// CompleteMultipartUpload or AbortMultipartUpload calls.
    pub async fn create_multipart_upload(
        &self,
        endpoint: &str,
        key: &str,
    ) -> Result<CreateMultipartUploadResponse> {
        let url = format!("{}?uploads", self.build_url(endpoint, key));

        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/octet-stream".to_string());

        let (status, body_bytes) = self.request_with_retry(
            Method::POST,
            &url,
            headers,
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        self.parse_create_multipart_response(&body_bytes)
    }

    /// Upload a part of a multipart upload (UploadPart)
    ///
    /// Part numbers are 1-indexed (1 to 10000).
    /// Each part must be at least 5MB except for the last part.
    /// Returns the ETag of the uploaded part.
    pub async fn upload_part(
        &self,
        endpoint: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<UploadPartResponse> {
        let base_url = self.build_url(endpoint, key);
        let mut url = String::with_capacity(base_url.len() + 64);
        url.push_str(&base_url);
        url.push_str("?partNumber=");
        let _ = write!(url, "{}", part_number);
        url.push_str("&uploadId=");
        Self::url_encode_into(&mut url, upload_id);

        let mut headers = BTreeMap::new();
        headers.insert("content-length".to_string(), data.len().to_string());

        // Use UNSIGNED-PAYLOAD to avoid SHA256 computation of large parts
        let signed_headers = self.signer.sign_unsigned_payload("PUT", &url, headers);

        let mut req = Request::builder().method(Method::PUT).uri(&url);
        for (k, v) in signed_headers.iter() {
            req = req.header(k, v);
        }

        let request = req.body(Full::new(data))?;
        let response = self.client.request(request).await?;
        let status = response.status();

        // Extract ETag from response headers before consuming body
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();

        // Drain body to return connection to pool
        let body_bytes = response.collect().await?.to_bytes();

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(UploadPartResponse::new(part_number, etag))
    }

    /// Complete a multipart upload (CompleteMultipartUpload)
    ///
    /// Takes a list of completed parts (part number + ETag) in order.
    /// Parts must be sorted by part number.
    pub async fn complete_multipart_upload(
        &self,
        endpoint: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<CompleteMultipartUploadResponse> {
        let base_url = self.build_url(endpoint, key);
        let mut url = String::with_capacity(base_url.len() + 64);
        url.push_str(&base_url);
        url.push_str("?uploadId=");
        Self::url_encode_into(&mut url, upload_id);

        // Build XML body
        let mut xml = String::with_capacity(parts.len() * 100 + 100);
        xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.push_str("<CompleteMultipartUpload>");
        for part in parts {
            xml.push_str("<Part><PartNumber>");
            let _ = write!(xml, "{}", part.part_number);
            xml.push_str("</PartNumber><ETag>\"");
            // ETag should already be without quotes, but handle both cases
            let etag = part.etag.trim_matches('"');
            xml.push_str(etag);
            xml.push_str("\"</ETag></Part>");
        }
        xml.push_str("</CompleteMultipartUpload>");

        let xml_bytes = xml.into_bytes();

        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/xml".to_string());
        headers.insert("content-length".to_string(), xml_bytes.len().to_string());

        // Sign with actual body hash (XML is small)
        let signed_headers = self.signer.sign("POST", &url, headers, &xml_bytes);

        let mut req = Request::builder().method(Method::POST).uri(&url);
        for (k, v) in signed_headers.iter() {
            req = req.header(k, v);
        }

        let request = req.body(Full::new(Bytes::from(xml_bytes)))?;
        let response = self.client.request(request).await?;
        let status = response.status();
        let body_bytes = response.collect().await?.to_bytes();

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        self.parse_complete_multipart_response(&body_bytes)
    }

    /// Abort a multipart upload (AbortMultipartUpload)
    ///
    /// Cancels the upload and deletes all uploaded parts.
    /// Should be called if an upload fails partway through.
    pub async fn abort_multipart_upload(
        &self,
        endpoint: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        let base_url = self.build_url(endpoint, key);
        let mut url = String::with_capacity(base_url.len() + 64);
        url.push_str(&base_url);
        url.push_str("?uploadId=");
        Self::url_encode_into(&mut url, upload_id);

        let (status, body_bytes) = self.request_with_retry(
            Method::DELETE,
            &url,
            BTreeMap::new(),
            Bytes::new(),
        ).await?;

        if !status.is_success() {
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(S3Error::S3Response { status, message });
        }

        Ok(())
    }

    /// High-level multipart upload: uploads a file using multiple parts in parallel
    ///
    /// Automatically splits the data into parts and uploads them concurrently.
    /// Handles abort on failure. Returns the ETag of the completed object.
    ///
    /// This is the recommended way to upload large files (>100MB).
    pub async fn multipart_upload(
        &self,
        endpoint: &str,
        key: &str,
        data: Bytes,
        config: &MultipartConfig,
    ) -> Result<String> {
        // If file is below threshold, use simple PUT
        if (data.len() as u64) < config.threshold {
            return self.put_object(endpoint, key, data).await;
        }

        // Initiate multipart upload
        let init_response = self.create_multipart_upload(endpoint, key).await?;
        let upload_id = init_response.upload_id;

        // Calculate part boundaries
        let total_size = data.len();
        let part_size = config.part_size;
        let num_parts = (total_size + part_size - 1) / part_size;

        // Upload parts with controlled concurrency
        let mut completed_parts: Vec<CompletedPart> = Vec::with_capacity(num_parts);
        let mut upload_error: Option<S3Error> = None;

        // Use a semaphore-like approach with chunks
        let mut part_futures = Vec::with_capacity(num_parts);

        for part_num in 0..num_parts {
            let start = part_num * part_size;
            let end = std::cmp::min(start + part_size, total_size);
            let part_data = data.slice(start..end);
            let part_number = (part_num + 1) as u32; // S3 part numbers are 1-indexed

            part_futures.push((part_number, part_data));
        }

        // Upload parts with concurrency limit
        use futures::stream::{self, StreamExt};

        let results: Vec<Result<UploadPartResponse>> = stream::iter(part_futures)
            .map(|(part_number, part_data)| {
                let endpoint = endpoint.to_string();
                let key = key.to_string();
                let upload_id = upload_id.clone();
                let client = self.clone();

                async move {
                    client.upload_part(&endpoint, &key, &upload_id, part_number, part_data).await
                }
            })
            .buffer_unordered(config.concurrency)
            .collect()
            .await;

        // Check for errors and collect completed parts
        for result in results {
            match result {
                Ok(part_response) => {
                    completed_parts.push(CompletedPart::new(
                        part_response.part_number,
                        part_response.etag,
                    ));
                }
                Err(e) => {
                    upload_error = Some(e);
                    break;
                }
            }
        }

        // If any upload failed, abort the multipart upload
        if let Some(err) = upload_error {
            let _ = self.abort_multipart_upload(endpoint, key, &upload_id).await;
            return Err(err);
        }

        // Sort parts by part number (required by S3)
        completed_parts.sort_by_key(|p| p.part_number);

        // Complete the multipart upload
        let complete_response = self
            .complete_multipart_upload(endpoint, key, &upload_id, &completed_parts)
            .await?;

        Ok(complete_response.etag)
    }

    /// Upload a file using multipart if it exceeds the threshold
    ///
    /// Reads the file and decides whether to use single PUT or multipart.
    /// Uses streaming for large files to minimize memory usage.
    pub async fn upload_file(
        &self,
        endpoint: &str,
        key: &str,
        path: &std::path::Path,
        config: &MultipartConfig,
    ) -> Result<String> {
        let metadata = std::fs::metadata(path)?;
        let file_size = metadata.len();

        // For small files, use simple PUT
        if file_size < config.threshold {
            let data = std::fs::read(path)?;
            return self.put_object(endpoint, key, Bytes::from(data)).await;
        }

        // For large files, use streaming multipart upload
        self.upload_file_multipart(endpoint, key, path, file_size, config).await
    }

    /// Streaming multipart upload for large files
    ///
    /// Reads file in chunks to avoid loading entire file into memory.
    async fn upload_file_multipart(
        &self,
        endpoint: &str,
        key: &str,
        path: &std::path::Path,
        file_size: u64,
        config: &MultipartConfig,
    ) -> Result<String> {
        use std::io::Read;

        // Initiate multipart upload
        let init_response = self.create_multipart_upload(endpoint, key).await?;
        let upload_id = init_response.upload_id;

        // Calculate number of parts
        let part_size = config.part_size;
        let num_parts = ((file_size as usize) + part_size - 1) / part_size;

        let mut completed_parts: Vec<CompletedPart> = Vec::with_capacity(num_parts);
        let mut upload_error: Option<S3Error> = None;

        // Open file and read/upload parts
        let file = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::with_capacity(part_size, file);

        for part_num in 0..num_parts {
            let mut buffer = vec![0u8; part_size];
            let mut total_read = 0;

            // Read exactly part_size bytes (or less for last part)
            while total_read < part_size {
                match reader.read(&mut buffer[total_read..]) {
                    Ok(0) => break, // EOF
                    Ok(n) => total_read += n,
                    Err(e) => {
                        upload_error = Some(S3Error::Io(e));
                        break;
                    }
                }
            }

            if upload_error.is_some() {
                break;
            }

            if total_read == 0 {
                break;
            }

            buffer.truncate(total_read);
            let part_data = Bytes::from(buffer);
            let part_number = (part_num + 1) as u32;

            match self.upload_part(endpoint, key, &upload_id, part_number, part_data).await {
                Ok(part_response) => {
                    completed_parts.push(CompletedPart::new(
                        part_response.part_number,
                        part_response.etag,
                    ));
                }
                Err(e) => {
                    upload_error = Some(e);
                    break;
                }
            }
        }

        // If any upload failed, abort
        if let Some(err) = upload_error {
            let _ = self.abort_multipart_upload(endpoint, key, &upload_id).await;
            return Err(err);
        }

        // Complete the multipart upload
        let complete_response = self
            .complete_multipart_upload(endpoint, key, &upload_id, &completed_parts)
            .await?;

        Ok(complete_response.etag)
    }

    /// Parse CreateMultipartUpload XML response
    fn parse_create_multipart_response(&self, xml_data: &[u8]) -> Result<CreateMultipartUploadResponse> {
        let mut reader = Reader::from_reader(xml_data);
        reader.config_mut().trim_text_start = true;
        reader.config_mut().trim_text_end = true;

        let mut bucket = String::new();
        let mut key = String::new();
        let mut upload_id = String::new();
        let mut current_text = String::with_capacity(256);

        loop {
            match reader.read_event() {
                Ok(Event::Text(e)) => {
                    current_text.clear();
                    current_text.push_str(&e.unescape()?);
                }
                Ok(Event::End(e)) => {
                    match e.local_name().as_ref() {
                        b"Bucket" => bucket = std::mem::take(&mut current_text),
                        b"Key" => key = std::mem::take(&mut current_text),
                        b"UploadId" => upload_id = std::mem::take(&mut current_text),
                        _ => {}
                    }
                    current_text.clear();
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(S3Error::XmlParse(format!("XML parse error: {}", e))),
                _ => {}
            }
        }

        if upload_id.is_empty() {
            return Err(S3Error::InvalidResponse("Missing UploadId in response".to_string()));
        }

        Ok(CreateMultipartUploadResponse::new(bucket, key, upload_id))
    }

    /// Parse CompleteMultipartUpload XML response
    fn parse_complete_multipart_response(&self, xml_data: &[u8]) -> Result<CompleteMultipartUploadResponse> {
        let mut reader = Reader::from_reader(xml_data);
        reader.config_mut().trim_text_start = true;
        reader.config_mut().trim_text_end = true;

        let mut location = None;
        let mut bucket = String::new();
        let mut key = String::new();
        let mut etag = String::new();
        let mut current_text = String::with_capacity(256);

        loop {
            match reader.read_event() {
                Ok(Event::Text(e)) => {
                    current_text.clear();
                    current_text.push_str(&e.unescape()?);
                }
                Ok(Event::End(e)) => {
                    match e.local_name().as_ref() {
                        b"Location" => location = Some(std::mem::take(&mut current_text)),
                        b"Bucket" => bucket = std::mem::take(&mut current_text),
                        b"Key" => key = std::mem::take(&mut current_text),
                        b"ETag" => etag = std::mem::take(&mut current_text).trim_matches('"').to_string(),
                        _ => {}
                    }
                    current_text.clear();
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(S3Error::XmlParse(format!("XML parse error: {}", e))),
                _ => {}
            }
        }

        let mut response = CompleteMultipartUploadResponse::new(bucket, key, etag);
        response.location = location;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xml_escape() {
        assert_eq!(S3Client::xml_escape("hello"), "hello");
        assert_eq!(S3Client::xml_escape("hello<world>"), "hello&lt;world&gt;");
        assert_eq!(S3Client::xml_escape("a&b"), "a&amp;b");
    }

    #[test]
    fn test_client_creation() {
        let client = S3Client::new(
            "access_key".to_string(),
            "secret_key".to_string(),
            "my-bucket".to_string(),
            Some("us-east-1".to_string()),
        );

        assert_eq!(client.bucket, "my-bucket");
    }

    #[test]
    fn test_encode_s3_key_no_encoding() {
        // Common case: ASCII key with slashes - should return borrowed (zero alloc)
        let key = "path/to/file.txt";
        let result = S3Client::encode_s3_key(key);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result, "path/to/file.txt");
    }

    #[test]
    fn test_encode_s3_key_with_encoding() {
        let key = "path/to/file with spaces.txt";
        let result = S3Client::encode_s3_key(key);
        assert!(matches!(result, Cow::Owned(_)));
        assert_eq!(result, "path/to/file%20with%20spaces.txt");
    }

    #[test]
    fn test_client_is_clone() {
        let client = S3Client::new(
            "key".to_string(),
            "secret".to_string(),
            "bucket".to_string(),
            None,
        );
        let _clone = client.clone();
    }
}

use anyhow::{Context, Result};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::core::Core;

/// HTTP body type for responses
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

/// S3 operation extracted from HTTP request (for logging only)
#[derive(Debug, Clone)]
pub enum S3Operation {
    GetObject { bucket: String, key: String },
    PutObject { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
    HeadObject { bucket: String, key: String },
    ListObjects { bucket: String, prefix: Option<String> },
    HeadBucket { bucket: String },
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    BatchDelete { bucket: String },
    Unknown { method: String, path: String },
}

impl S3Operation {
    /// Extract S3 operation from HTTP request (best-effort, for logging)
    pub fn from_request(req: &Request<Incoming>) -> Self {
        let path = req.uri().path();
        let method = req.method();
        let query = req.uri().query().unwrap_or("");

        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

        if parts.is_empty() || parts[0].is_empty() {
            return S3Operation::Unknown {
                method: method.to_string(),
                path: path.to_string(),
            };
        }

        let bucket = parts[0].to_string();

        // POST with ?delete= is batch delete
        if method == Method::POST && query.contains("delete") {
            return S3Operation::BatchDelete { bucket };
        }

        match (method, parts.len()) {
            (&Method::HEAD, 1) => S3Operation::HeadBucket { bucket },
            (&Method::PUT, 1) => S3Operation::CreateBucket { bucket },
            (&Method::DELETE, 1) => S3Operation::DeleteBucket { bucket },
            (&Method::GET, 1) => {
                let prefix = query
                    .split('&')
                    .find(|p| p.starts_with("prefix="))
                    .map(|p| p.trim_start_matches("prefix=").to_string());
                S3Operation::ListObjects { bucket, prefix }
            }
            (&Method::GET, _) => {
                let key = parts[1..].join("/");
                S3Operation::GetObject { bucket, key }
            }
            (&Method::PUT, _) => {
                let key = parts[1..].join("/");
                S3Operation::PutObject { bucket, key }
            }
            (&Method::DELETE, _) => {
                let key = parts[1..].join("/");
                S3Operation::DeleteObject { bucket, key }
            }
            (&Method::HEAD, _) => {
                let key = parts[1..].join("/");
                S3Operation::HeadObject { bucket, key }
            }
            _ => S3Operation::Unknown {
                method: method.to_string(),
                path: path.to_string(),
            },
        }
    }
}

/// Proxy server state
pub struct ProxyServer {
    core: Core,
    listen: String,
}

impl ProxyServer {
    /// Create a new proxy server with the given core and listen address
    pub fn new(core: Core, listen: String) -> Self {
        Self { core, listen }
    }

    /// Start the proxy server and listen for incoming connections
    pub async fn run(self) -> Result<()> {
        let addr: SocketAddr = self
            .listen
            .parse()
            .context(format!("Invalid listen address: {}", self.listen))?;

        let listener = TcpListener::bind(addr)
            .await
            .context(format!("Failed to bind to {}", addr))?;

        info!("S3Pool proxy server listening on {}", addr);

        // Log configured profiles
        for (name, profile) in &self.core.config.profiles {
            info!(
                "Profile '{}': {} endpoints, region={}",
                name,
                profile.endpoints.len(),
                profile.region
            );
        }

        let server = Arc::new(self);

        loop {
            let (stream, remote_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    continue;
                }
            };

            let server = Arc::clone(&server);

            tokio::spawn(async move {
                let io = TokioIo::new(stream);

                let service = service_fn(move |req| {
                    let server = Arc::clone(&server);
                    async move { server.handle_request(req).await }
                });

                if let Err(e) = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(io, service)
                    .await
                {
                    // Filter out benign connection reset errors
                    let err_str = format!("{}", e);
                    if !err_str.contains("connection reset")
                        && !err_str.contains("broken pipe")
                    {
                        error!("Error serving connection from {}: {}", remote_addr, e);
                    }
                }
            });
        }
    }

    /// Handle an incoming HTTP request by transparently forwarding to a backend
    async fn handle_request(&self, req: Request<Incoming>) -> Result<Response<BoxBody>> {
        let method = req.method().clone();
        let uri = req.uri().clone();

        // Log the operation (debug level)
        debug!("{} {}", method, uri);

        // Normalize path: URL-encode special chars in S3 keys (?, &, :, etc.)
        let path_and_query = self.normalize_s3_path(&method, &uri);

        // Extract multipart hash key for sticky routing.
        // Multipart uploads MUST stay on the same backend - parts are stored per-node.
        // We use the bucket/key as the hash key (not uploadId) because it's available
        // for ALL multipart operations including CreateMultipartUpload.
        let multipart_hash_key = Self::extract_multipart_hash_key(&method, &uri);

        // Extract relevant headers from the incoming request to forward
        let mut extra_headers = BTreeMap::new();
        let req_headers = req.headers();

        // Forward Content-Type if present
        if let Some(ct) = req_headers.get("content-type") {
            if let Ok(v) = ct.to_str() {
                extra_headers.insert("content-type".to_string(), v.to_string());
            }
        }

        // Forward Content-MD5 if present (needed for batch delete)
        if let Some(md5) = req_headers.get("content-md5") {
            if let Ok(v) = md5.to_str() {
                extra_headers.insert("content-md5".to_string(), v.to_string());
            }
        }

        // Collect incoming request body
        let body = match req.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                warn!("Failed to read request body: {}", e);
                return self.error_response(
                    StatusCode::BAD_REQUEST,
                    format!("Failed to read request body: {}", e),
                );
            }
        };

        // Add content-length for non-empty bodies
        if !body.is_empty() && !extra_headers.contains_key("content-length") {
            extra_headers.insert("content-length".to_string(), body.len().to_string());
        }

        // Get S3 client
        let s3_client = match self.core.s3_client() {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "s3_client_error");
                return self.error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal proxy error".to_string(),
                );
            }
        };

        // Retry loop for 429/503 errors with exponential backoff + jitter
        // Aggressive retry policy: 7 attempts with delays up to ~5s
        const MAX_RETRIES: u32 = 7;
        let mut last_status = StatusCode::SERVICE_UNAVAILABLE;
        let mut last_resp_headers = hyper::HeaderMap::new();
        let mut last_resp_body = bytes::Bytes::new();
        let mut last_backend = String::new();
        let mut tried_backends: std::collections::HashSet<usize> = std::collections::HashSet::new();

        for attempt in 0..MAX_RETRIES {
            // Select a backend endpoint (with index for health tracking)
            // For multipart uploads, use sticky routing (hash-based on bucket/key)
            // to ensure all parts go to the same backend
            let (endpoint, backend_idx) = if let Some(ref hash_key) = multipart_hash_key {
                // Sticky routing: hash bucket/key to consistently select same backend
                match self.core.select_endpoint_by_hash(hash_key) {
                    Ok((ep, idx)) => {
                        // Debug: log multipart routing
                        info!(
                            hash_key = %hash_key,
                            backend_idx = idx,
                            endpoint = %ep,
                            "multipart_routing"
                        );
                        (ep.to_string(), idx)
                    },
                    Err(e) => {
                        error!(error = %e, "no_healthy_backend_multipart");
                        return self.error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "No healthy backends available for multipart upload".to_string(),
                        );
                    }
                }
            } else {
                // Normal rotation - use shared helper for untried endpoint selection
                match self.core.select_untried_endpoint(&tried_backends) {
                    Some(result) => result,
                    None => {
                        error!("no_healthy_backend_after_retries");
                        return self.error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "No healthy backends available".to_string(),
                        );
                    }
                }
            };

            let endpoint_trimmed = endpoint.trim_end_matches('/');
            let backend_url = format!("{}{}", endpoint_trimmed, path_and_query);
            last_backend = endpoint_trimmed.to_string();

            // Track this backend as tried (for retry logic)
            tried_backends.insert(backend_idx);

            // Track connection for load balancing (LeastConnections, PowerOfTwo)
            self.core.begin_request(backend_idx);

            let start = std::time::Instant::now();
            let result = s3_client
                .proxy_request(method.clone(), &backend_url, extra_headers.clone(), body.clone())
                .await;

            match result {
                Ok((status, resp_headers, resp_body)) => {
                    // Check for retryable status codes (429, 503)
                    if (status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE)
                        && attempt < MAX_RETRIES - 1
                    {
                        self.core.end_request(backend_idx);
                        self.core.record_failure(backend_idx);
                        // Exponential backoff: 200ms, 400ms, 800ms, 1600ms, 3200ms, 5000ms (capped)
                        // Add jitter: ±25% to avoid thundering herd
                        let base_delay = std::cmp::min(200 * (1u64 << attempt), 5000);
                        let jitter = (base_delay / 4) as i64;
                        let jitter_offset = ((std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .subsec_nanos() as i64) % (jitter * 2 + 1)) - jitter;
                        let delay_ms = (base_delay as i64 + jitter_offset).max(50) as u64;
                        debug!(
                            status = status.as_u16(),
                            attempt = attempt + 1,
                            delay_ms = delay_ms,
                            backend = %endpoint_trimmed,
                            "retry_backoff"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    // Success or non-retryable error
                    self.core.end_request(backend_idx);
                    if status.is_success() {
                        self.core.record_success(backend_idx);
                    } else if status.is_server_error() {
                        self.core.record_failure(backend_idx);
                        let elapsed = start.elapsed();
                        warn!(
                            method = %method,
                            backend = %endpoint_trimmed,
                            status = status.as_u16(),
                            duration_ms = elapsed.as_millis() as u64,
                            "backend_error"
                        );
                    }

                    last_status = status;
                    last_resp_headers = resp_headers;
                    last_resp_body = resp_body;
                    break;
                }
                Err(e) => {
                    self.core.end_request(backend_idx);
                    // Connection errors mark backend immediately unhealthy
                    let err_str = e.to_string();
                    if crate::core::Core::is_connect_error(&err_str) {
                        self.core.record_connect_failure(backend_idx);
                    } else {
                        self.core.record_failure(backend_idx);
                    }
                    let elapsed = start.elapsed();

                    if attempt < MAX_RETRIES - 1 {
                        // Same backoff strategy for connection errors
                        let base_delay = std::cmp::min(200 * (1u64 << attempt), 5000);
                        let jitter = (base_delay / 4) as i64;
                        let jitter_offset = ((std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .subsec_nanos() as i64) % (jitter * 2 + 1)) - jitter;
                        let delay_ms = (base_delay as i64 + jitter_offset).max(50) as u64;
                        debug!(
                            error = %e,
                            attempt = attempt + 1,
                            delay_ms = delay_ms,
                            backend = %endpoint_trimmed,
                            "retry_after_error"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }

                    error!(
                        method = %method,
                        backend = %endpoint_trimmed,
                        duration_ms = elapsed.as_millis() as u64,
                        error = %e,
                        "request_failed"
                    );
                    return self.error_response(
                        StatusCode::BAD_GATEWAY,
                        format!("Backend request failed: {}", e),
                    );
                }
            }
        }

        let (status, resp_headers, resp_body) = (last_status, last_resp_headers, last_resp_body);

        // Build response, forwarding relevant headers
        let mut response = Response::builder()
            .status(status)
            .header("x-s3pool-proxy", "v2")
            .header("connection", "keep-alive");

        // Forward S3 response headers
        for (name, value) in resp_headers.iter() {
            let n = name.as_str();
            if n.starts_with("x-amz-")
                || n == "content-type"
                || n == "content-length"
                || n == "etag"
                || n == "last-modified"
                || n == "accept-ranges"
                || n == "content-range"
            {
                response = response.header(name, value);
            }
        }

        Ok(response.body(self.bytes_body(resp_body)).unwrap())
    }

    /// Create an error response
    fn error_response(&self, status: StatusCode, message: String) -> Result<Response<BoxBody>> {
        Ok(Response::builder()
            .status(status)
            .header("Content-Type", "text/plain")
            .header("Connection", "keep-alive")
            .body(self.string_body(message))
            .unwrap())
    }

    /// Convert a string into a BoxBody
    fn string_body(&self, s: String) -> BoxBody {
        use http_body_util::Full;
        Full::new(Bytes::from(s))
            .map_err(|never| match never {})
            .boxed()
    }

    /// Convert Bytes into a BoxBody
    fn bytes_body(&self, b: Bytes) -> BoxBody {
        use http_body_util::Full;
        Full::new(b).map_err(|never| match never {}).boxed()
    }

    /// Normalize S3 path for object operations.
    ///
    /// S3 keys can contain special characters like `?`, `&`, `#` that HTTP parsers
    /// interpret as query string delimiters. For object operations (PUT/GET/DELETE/HEAD
    /// with a key), we need to URL-encode these characters so they're treated as part
    /// of the key, not as query parameters.
    ///
    /// For bucket-level operations (list, batch delete), query strings are legitimate
    /// S3 parameters and should be preserved as-is.
    fn normalize_s3_path(&self, method: &Method, uri: &hyper::Uri) -> String {
        let raw_path = uri.path();
        let raw_query = uri.query();

        // Parse path: /{bucket} or /{bucket}/{key...}
        let trimmed = raw_path.trim_start_matches('/');
        let (bucket, key) = match trimmed.find('/') {
            Some(pos) => (&trimmed[..pos], Some(&trimmed[pos + 1..])),
            None => (trimmed, None),
        };

        // Determine if this is an object operation vs bucket operation
        // Object ops: have a non-empty key
        // Bucket ops: no key, or known S3 query params (list-type, delete, uploads, etc.)
        let has_key = key.map(|k| !k.is_empty()).unwrap_or(false);

        // Check for legitimate S3 query parameters (bucket-level operations)
        // Be precise: S3 uses ?param (no value) or ?param=value for specific params
        // URL keys like ?location=foo should NOT match (that's a web URL query param)
        let has_s3_query = raw_query.map(|q| {
            // List operations: ?list-type=2&prefix=...
            q.starts_with("list-type=")
                // Batch delete: ?delete
                || q == "delete" || q.starts_with("delete&")
                // Multipart: ?uploads, ?uploads=, ?uploadId=xxx, ?partNumber=N
                || q == "uploads" || q.starts_with("uploads=") || q.starts_with("uploads&")
                || q.starts_with("uploadId=") || q.starts_with("partNumber=")
                // Versioning: ?versions or ?versioning
                || q == "versions" || q.starts_with("versions&")
                || q == "versioning" || q.starts_with("versioning&")
                // Bucket location: ?location (no value!)
                || q == "location" || q.starts_with("location&")
                // Policy/ACL/etc: ?policy, ?acl, ?cors, etc (no value!)
                || q == "policy" || q.starts_with("policy&")
                || q == "acl" || q.starts_with("acl&")
                || q == "cors" || q.starts_with("cors&")
                || q == "lifecycle" || q.starts_with("lifecycle&")
                || q == "tagging" || q.starts_with("tagging&")
        }).unwrap_or(false);

        // For object operations without S3 query params, any query string is part of the key
        if has_key && !has_s3_query {
            let key_str = key.unwrap();
            let encoded_key = Self::encode_uri_component(key_str);

            if let Some(query) = raw_query {
                // The query was actually part of the key (e.g., URL as S3 key)
                // Encode it and append with %3F (encoded ?)
                let encoded_query = Self::encode_uri_component(query);
                format!("/{}/{}%3F{}", bucket, encoded_key, encoded_query)
            } else {
                format!("/{}/{}", bucket, encoded_key)
            }
        } else {
            // Bucket operation or legitimate S3 query - pass through as-is
            uri.path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or("/")
                .to_string()
        }
    }

    /// Extract hash key for multipart sticky routing.
    ///
    /// Returns Some(bucket/key) if this is a multipart operation:
    /// - CreateMultipartUpload: POST /bucket/key?uploads
    /// - UploadPart: PUT /bucket/key?partNumber=N&uploadId=xxx
    /// - CompleteMultipartUpload: POST /bucket/key?uploadId=xxx
    /// - AbortMultipartUpload: DELETE /bucket/key?uploadId=xxx
    ///
    /// Uses bucket/key (not uploadId) because it's available for ALL operations
    /// including CreateMultipartUpload before the uploadId is assigned.
    fn extract_multipart_hash_key(method: &Method, uri: &hyper::Uri) -> Option<String> {
        let query = uri.query()?;

        // Check if this is a multipart operation
        let is_multipart = query == "uploads"
            || query.starts_with("uploads&")
            || query.contains("uploadId=")
            || query.contains("partNumber=");

        if !is_multipart {
            return None;
        }

        // Extract bucket/key from path
        let path = uri.path().trim_start_matches('/');
        if path.is_empty() {
            return None;
        }

        // Return the full path (bucket/key) as the hash key
        Some(path.to_string())
    }

    /// URL-encode a string component, preserving forward slashes.
    /// Encodes all characters except unreserved (A-Za-z0-9-_.~) and '/'.
    fn encode_uri_component(s: &str) -> String {
        static HEX: &[u8; 16] = b"0123456789ABCDEF";

        let mut result = String::with_capacity(s.len() + 32);
        for byte in s.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                    result.push(byte as char);
                }
                _ => {
                    result.push('%');
                    result.push(HEX[(byte >> 4) as usize] as char);
                    result.push(HEX[(byte & 0xf) as usize] as char);
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: S3Operation tests use raw path strings since we can't easily
    // construct Request<Incoming> in tests. The parsing logic is tested
    // through integration tests via the proxy.
}

//! S3 types and response structures

use serde::{Deserialize, Serialize};

/// S3 Object metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    /// Object key
    pub key: String,
    /// Object size in bytes
    pub size: u64,
    /// Last modified timestamp (optional)
    pub last_modified: Option<String>,
    /// ETag (optional)
    pub etag: Option<String>,
    /// Storage class (STANDARD, STANDARD_IA, GLACIER, etc.)
    pub storage_class: Option<String>,
}

impl S3Object {
    /// Create a new S3Object
    pub fn new(key: String, size: u64) -> Self {
        Self {
            key,
            size,
            last_modified: None,
            etag: None,
            storage_class: None,
        }
    }
}

/// Response from ListObjectsV2 operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    /// List of objects
    pub contents: Vec<S3Object>,
    /// Common prefixes (subdirectories when using delimiter)
    pub common_prefixes: Vec<String>,
    /// Whether the response is truncated
    pub is_truncated: bool,
    /// Continuation token for next request
    pub next_continuation_token: Option<String>,
    /// Prefix used in the request
    pub prefix: Option<String>,
    /// Max keys requested
    pub max_keys: Option<i32>,
    /// Key count in this response
    pub key_count: Option<i32>,
}

impl ListObjectsResponse {
    /// Create a new empty response
    pub fn new() -> Self {
        Self {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_continuation_token: None,
            prefix: None,
            max_keys: None,
            key_count: None,
        }
    }
}

impl Default for ListObjectsResponse {
    fn default() -> Self {
        Self::new()
    }
}

/// Response from DeleteObjects batch operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteObjectsResponse {
    /// List of successfully deleted objects
    pub deleted: Vec<DeletedObject>,
    /// List of errors
    pub errors: Vec<DeleteError>,
}

impl DeleteObjectsResponse {
    /// Create a new empty response
    pub fn new() -> Self {
        Self {
            deleted: Vec::new(),
            errors: Vec::new(),
        }
    }
}

impl Default for DeleteObjectsResponse {
    fn default() -> Self {
        Self::new()
    }
}

/// Deleted object information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedObject {
    /// Object key
    pub key: String,
    /// Version ID (optional)
    pub version_id: Option<String>,
    /// Delete marker (optional)
    pub delete_marker: Option<bool>,
    /// Delete marker version ID (optional)
    pub delete_marker_version_id: Option<String>,
}

impl DeletedObject {
    /// Create a new deleted object
    pub fn new(key: String) -> Self {
        Self {
            key,
            version_id: None,
            delete_marker: None,
            delete_marker_version_id: None,
        }
    }
}

/// Delete error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteError {
    /// Object key
    pub key: String,
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
}

// =============================================================================
// Multipart Upload Types
// =============================================================================

/// Response from CreateMultipartUpload operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateMultipartUploadResponse {
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Upload ID for subsequent UploadPart and CompleteMultipartUpload requests
    pub upload_id: String,
}

impl CreateMultipartUploadResponse {
    /// Create a new response
    pub fn new(bucket: String, key: String, upload_id: String) -> Self {
        Self { bucket, key, upload_id }
    }
}

/// Response from UploadPart operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPartResponse {
    /// Part number
    pub part_number: u32,
    /// ETag of the uploaded part (required for CompleteMultipartUpload)
    pub etag: String,
}

impl UploadPartResponse {
    /// Create a new response
    pub fn new(part_number: u32, etag: String) -> Self {
        Self { part_number, etag }
    }
}

/// Part information for CompleteMultipartUpload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPart {
    /// Part number (1-10000)
    pub part_number: u32,
    /// ETag returned from UploadPart
    pub etag: String,
}

impl CompletedPart {
    /// Create a new completed part
    pub fn new(part_number: u32, etag: String) -> Self {
        Self { part_number, etag }
    }
}

/// Response from CompleteMultipartUpload operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMultipartUploadResponse {
    /// Location URL of the completed object
    pub location: Option<String>,
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub key: String,
    /// ETag of the completed object
    pub etag: String,
}

impl CompleteMultipartUploadResponse {
    /// Create a new response
    pub fn new(bucket: String, key: String, etag: String) -> Self {
        Self {
            location: None,
            bucket,
            key,
            etag,
        }
    }
}

/// Configuration for multipart uploads
#[derive(Debug, Clone)]
pub struct MultipartConfig {
    /// Minimum part size in bytes (default: 5MB, S3 minimum)
    pub part_size: usize,
    /// Maximum concurrent part uploads (default: 10)
    pub concurrency: usize,
    /// Threshold file size to trigger multipart (default: 100MB)
    pub threshold: u64,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            part_size: 5 * 1024 * 1024,      // 5MB minimum per S3 spec
            concurrency: 10,                  // 10 concurrent uploads
            threshold: 100 * 1024 * 1024,     // Use multipart for files > 100MB
        }
    }
}

impl MultipartConfig {
    /// Create a new config with custom part size
    pub fn with_part_size(mut self, size: usize) -> Self {
        // Enforce S3 minimum of 5MB (except for last part)
        self.part_size = size.max(5 * 1024 * 1024);
        self
    }

    /// Create a new config with custom concurrency
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Create a new config with custom threshold
    pub fn with_threshold(mut self, threshold: u64) -> Self {
        self.threshold = threshold;
        self
    }
}

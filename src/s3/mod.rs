//! S3 client module with AWS SigV4 signing
//!
//! This module provides:
//! - AWS Signature Version 4 signing for S3 requests
//! - Async S3 operations (list, get, put, delete)
//! - Type-safe S3 response structures

pub mod client;
pub mod signer;
pub mod types;

// Re-export main types for convenience
pub use client::{S3Client, S3Error, Result};
pub use signer::S3SignerV4;
pub use types::{
    DeleteError, DeleteObjectsResponse, DeletedObject, ListObjectsResponse, S3Object,
    // Multipart upload types
    CreateMultipartUploadResponse, UploadPartResponse, CompletedPart,
    CompleteMultipartUploadResponse, MultipartConfig,
};

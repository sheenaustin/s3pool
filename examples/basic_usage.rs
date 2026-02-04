//! Basic usage example for S3Pool v2
//!
//! This example demonstrates how to use the S3 client with AWS SigV4 signing.
//!
//! Run with:
//! ```
//! cargo run --example basic_usage
//! ```

use s3pool::s3::{S3Client, S3Error};

#[tokio::main]
async fn main() -> Result<(), S3Error> {
    // Create S3 client
    let client = S3Client::new(
        "YOUR_ACCESS_KEY".to_string(),
        "YOUR_SECRET_KEY".to_string(),
        "my-bucket".to_string(),
        Some("us-east-1".to_string()),
    );

    let endpoint = "https://s3.amazonaws.com";

    println!("S3Pool v2 - Basic Usage Example");
    println!("================================\n");

    // Example 1: Put object
    println!("1. Uploading object...");
    let test_data = b"Hello, S3Pool v2!";
    let etag = client
        .put_object(endpoint, "test/example.txt", test_data)
        .await?;
    println!("   Uploaded with ETag: {}\n", etag);

    // Example 2: Get object
    println!("2. Downloading object...");
    let data = client.get_object(endpoint, "test/example.txt").await?;
    println!("   Downloaded {} bytes", data.len());
    println!("   Content: {}\n", String::from_utf8_lossy(&data));

    // Example 3: List objects
    println!("3. Listing objects with prefix 'test/'...");
    let list_response = client
        .list_objects_v2(endpoint, Some("test/"), Some(10), None)
        .await?;
    println!("   Found {} objects:", list_response.contents.len());
    for obj in list_response.contents.iter() {
        println!("   - {} ({} bytes)", obj.key, obj.size);
    }
    println!();

    // Example 4: Delete single object
    println!("4. Deleting single object...");
    client.delete_object(endpoint, "test/example.txt").await?;
    println!("   Deleted successfully\n");

    // Example 5: Batch delete multiple objects
    println!("5. Batch deleting multiple objects...");
    let keys = vec![
        "test/file1.txt".to_string(),
        "test/file2.txt".to_string(),
        "test/file3.txt".to_string(),
    ];
    let delete_response = client.delete_objects(endpoint, &keys).await?;
    println!("   Deleted {} objects", delete_response.deleted.len());
    if !delete_response.errors.is_empty() {
        println!("   Errors: {}", delete_response.errors.len());
        for error in delete_response.errors.iter() {
            println!("   - {}: {} ({})", error.key, error.message, error.code);
        }
    }

    println!("\nAll operations completed successfully!");

    Ok(())
}

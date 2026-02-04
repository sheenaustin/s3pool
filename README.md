# s3pool

Fast S3 client with connection pooling and load balancing. Single binary, two modes: **CLI** and **Proxy**.

## Features

- **4-12x faster listing** via intelligent endpoint rotation
- **Zero extra threads** in CLI mode for minimal overhead
- **Built-in load balancing** across multiple S3 backends
- **Connection pooling** with HTTP/1.1 keep-alive
- **Health monitoring** with automatic failover
- **Circuit breaker** for unhealthy backends

## Installation

```bash
cargo build --release
cp target/release/s3pool /usr/local/bin/
```

## Quick Start

Set environment variables in `.env`:

```bash
S3_POOL=https://s3-1.example.com,https://s3-2.example.com
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET=my-bucket
```

Basic commands:

```bash
# List objects
s3pool ls s3/bucket/prefix/

# Upload/download
s3pool cp file.txt s3/bucket/path/
s3pool cp s3/bucket/path/file.txt ./local.txt

# Delete
s3pool rm s3/bucket/file.txt --force

# For self-signed certs
s3pool --insecure ls s3/bucket/
```

## CLI Commands

| Command | Description | Example |
|---------|-------------|---------|
| `ls` | List objects | `s3pool ls s3/bucket/` |
| `cp` | Upload/download | `s3pool cp file.txt s3/bucket/` |
| `rm` | Remove objects | `s3pool rm s3/bucket/file.txt --force` |
| `sync` | Sync directories | `s3pool sync ./dir s3/bucket/` |
| `stat` | Object info | `s3pool stat s3/bucket/file.txt` |
| `du` | Disk usage | `s3pool du s3/bucket/prefix/` |
| `find` | Find objects | `s3pool find s3/bucket/ --name "*.log"` |
| `mb` | Make bucket | `s3pool mb s3/new-bucket` |
| `rb` | Remove bucket | `s3pool rb s3/old-bucket --force` |
| `speedtest` | Test performance | `s3pool speedtest s3/bucket/` |
| `proxy` | Run as proxy | `s3pool proxy --listen 0.0.0.0:8000` |

## Proxy Mode

Run as HTTP proxy for any S3 client:

```bash
s3pool proxy --listen 0.0.0.0:8000

# Use with AWS CLI
aws s3 ls s3://bucket/ --endpoint-url http://localhost:8000

# Use with boto3
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:8000')

# Use with any S3-compatible tool
export S3_ENDPOINT=http://localhost:8000
```

## Configuration

Priority: CLI flags > Environment variables > Config file

### Environment Variables

```bash
S3_POOL=https://s3-1.com,https://s3-2.com   # Endpoints (comma-separated)
AWS_ACCESS_KEY_ID=key                        # Access key
AWS_SECRET_ACCESS_KEY=secret                 # Secret key
AWS_REGION=us-east-1                         # Region (default: us-east-1)
S3_BUCKET=bucket                             # Default bucket
S3POOL_INSECURE_TLS=true                     # Skip cert verification
```

### Config File

Create `~/.s3pool/config.yaml`:

```yaml
endpoints:
  - https://s3-1.example.com
  - https://s3-2.example.com

access_key: your_key
secret_key: your_secret
region: us-east-1
bucket: default-bucket
```

## Performance

Tested against 30 S3 backend nodes with self-signed HTTPS:

| Operation | s3pool | Traditional Client | Improvement |
|-----------|--------|-------------------|-------------|
| List (rotated) | 2000-4400 obj/s | 290-520 obj/s | **4-12x** |
| List (single) | 290-520 obj/s | 290-520 obj/s | Same |

### Why Faster?

S3 backends throttle listing per connection. s3pool rotates endpoints for each page, avoiding throttling despite TLS handshake overhead (~50ms/connection).

### Endpoint Rotation Strategy

| Operation | Strategy |
|-----------|----------|
| `ls`, `du`, `find` | Rotate per page |
| `cp`, `rm` | Rotate per file/batch |
| `stat`, `mb`, `rb` | Single endpoint |

## Architecture

```
CLI Mode                 Proxy Mode
──────────               ────────────
current_thread           multi_thread
(0 extra threads)        (thread pool)
       │                       │
       ▼                       ▼
   ┌────────────────────────────┐
   │  Core (shared)             │
   │  • S3Client + SigV4        │
   │  • LoadBalancer            │
   │  • HealthChecker           │
   │  • ConnectionPool          │
   └────────────────────────────┘
                │
                ▼
        S3 Backend Pool
```

## Development

```bash
# Build
cargo build --release

# Test
cargo test

# Debug logging
s3pool --log-level debug ls s3/bucket/

# Profile
time s3pool ls s3/bucket/ --recursive
```

## License

MIT

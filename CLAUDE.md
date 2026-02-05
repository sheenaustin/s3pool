# s3pool Project Guide

## Project Requirements Summary

### Core Architecture
- **CLI Mode**: Single-threaded tokio runtime (`new_current_thread`) for minimal overhead
- **Proxy Mode**: Multi-threaded tokio runtime (`new_multi_thread`) for concurrency
- **Shared Core**: Common logic in `src/core/mod.rs` used by both CLI and proxy

### Load Balancing
- **Algorithms**: RoundRobin, LeastConnections, PowerOfTwo
- **Backend Health**: Health score (0-100), unhealthy when score ≤30
- **Connection Tracking**: Atomic counters for active connections per backend
- **Endpoint Rotation**: Rotate endpoints per page/request to avoid MinIO throttling

### Retry Logic Requirements
All commands that make S3 requests MUST use the shared retry logic:
1. `Core::select_untried_endpoint(&tried)` - Select backend not yet tried
2. `Core::is_connect_error(err)` - Check if error is connection-level
3. `Core::is_retryable_error(err)` - Check if error warrants retry
4. `Core::record_connect_failure(idx)` - Immediately mark backend unhealthy on connect failure
5. `Core::record_failure(idx)` - Gradual health reduction for other failures
6. `Core::with_endpoint_retry(op)` - High-level retry helper for simple operations

### Commands with Retry Logic Implemented
- `ls` - List objects with pagination
- `stat` - Object/prefix information
- `du` - Disk usage calculation
- `find` - Object search
- `cp` - Upload (simple PUT) and download
- Proxy - All S3 operations

### Commands Needing Retry Logic (TODO)
- `cp` - Multipart uploads (complex - parts bound to specific backend)
- `rm` - Recursive delete listing
- `rb` - Bucket removal listing

### Configuration (Environment Variables)
```bash
# Required
S3_POOL=https://endpoint1,https://endpoint2,...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...

# Optional - Proxy
PROXY_LISTEN=127.0.0.1:18080
LB_STRATEGY=least_connections  # round_robin, least_connections, power_of_two
LB_HEALTH_CHECK_INTERVAL=30
LB_REQUEST_TIMEOUT=30
LB_MAX_RETRIES=3

# Optional - Connection Pool
POOL_MAX_CONNECTIONS=100
POOL_MIN_CONNECTIONS=10
POOL_MAX_IDLE_TIME=90
POOL_CONNECT_TIMEOUT=5  # Fast failure detection
```

### Testing
Run `./tests.sh` for the comprehensive test suite:
- Build tests (binary exists, help works)
- Configuration tests (env vars present)
- CLI basic tests (stat, ls, du, find)
- CLI transfer tests (cp upload/download)
- Retry logic tests (backend marked unhealthy, retry succeeds)
- Load balancer tests (multiple backends used)
- Code quality tests (shared helpers exist, no duplicated retry loops)

### Code Quality Rules
1. NO duplicated retry logic - use shared helpers in `Core`
2. Connection failures must call `record_connect_failure` (immediate unhealthy)
3. All paginated operations must rotate endpoints per page
4. Use `with_endpoint_retry` for simple request-response operations
5. Use `select_untried_endpoint` for complex pagination with manual retry

---

# Coding Practices

## General Principles

- Write simple, readable code. Prefer clarity over cleverness.
- Follow the principle of least surprise — code should behave as readers expect.
- Keep functions short and focused on a single responsibility.
- Prefer composition over inheritance.
- Don't repeat yourself (DRY), but don't abstract prematurely either. Duplication is cheaper than the wrong abstraction.

## Code Style

- Use consistent naming conventions: `camelCase` for JS/TS variables and functions, `snake_case` for Python, `PascalCase` for classes and components.
- Name variables and functions descriptively. Avoid abbreviations unless they're universally understood (`id`, `url`, `config`).
- Keep lines under 100 characters when reasonable.
- Group related code together. Separate logical sections with a blank line.
- Order imports: stdlib → third-party → local, separated by blank lines.

## Functions & Methods

- Functions should do one thing well. If a function needs a comment explaining *what* it does, it should probably be split up or renamed.
- Limit function parameters to 3–4. Use an options object or config struct if you need more.
- Prefer pure functions where possible — no side effects, same input always produces same output.
- Return early to reduce nesting. Guard clauses at the top of functions are preferred over deep `if/else` trees.

## Error Handling

- Handle errors explicitly. Don't swallow exceptions silently.
- Use typed/custom errors when the caller needs to distinguish between failure modes.
- Validate inputs at system boundaries (API endpoints, CLI args, file I/O), not deep inside business logic.
- Prefer failing fast and loudly over silently producing wrong results.

## Types & Data

- Use strong typing where the language supports it. Avoid `any` in TypeScript.
- Define clear interfaces/types for data that crosses boundaries (API responses, function params, database rows).
- Prefer immutable data structures. Mutate only when performance demands it.
- Use enums or union types instead of magic strings or numbers.

## Testing

- Write tests for behavior, not implementation. Tests should survive refactors.
- Each test should be independent — no shared mutable state between tests.
- Name tests descriptively: `should return empty array when no results match filter`.
- Aim for high coverage on business logic. Don't chase 100% coverage on glue code.
- Use the Arrange → Act → Assert pattern.

## Comments & Documentation

- Code should be self-documenting. Use comments to explain *why*, not *what*.
- Delete commented-out code. That's what version control is for.
- Document public APIs, exported functions, and non-obvious design decisions.
- Keep README files up to date with setup instructions and key architecture decisions.

## Architecture & Structure

- Organize by feature or domain, not by file type (prefer `users/controller.ts` over `controllers/users.ts`).
- Keep dependencies flowing in one direction. Core business logic should not depend on frameworks or I/O.
- Use dependency injection to keep modules testable and loosely coupled.
- Separate configuration from code. Use environment variables or config files.

## Git & Version Control

- Write clear, imperative commit messages: `Add user authentication endpoint`, not `added stuff`.
- Make small, focused commits. Each commit should represent a single logical change.
- Use feature branches and pull requests for non-trivial changes.
- Don't commit secrets, credentials, or environment files.

## Performance

- Don't optimize prematurely. Measure first, then optimize the bottleneck.
- Be mindful of algorithmic complexity — avoid O(n²) when O(n) is straightforward.
- Use caching deliberately, not as a default. Every cache is a source of bugs.
- Prefer lazy loading and pagination over fetching everything upfront.

## Security

- Never trust user input. Sanitize and validate everything.
- Use parameterized queries — never concatenate strings into SQL.
- Store secrets in environment variables or a secret manager, never in code.
- Apply the principle of least privilege to all access controls.
- Keep dependencies updated. Audit for known vulnerabilities regularly.

## Code Review Checklist

Before submitting code for review, verify:

1. The code compiles/runs without errors or warnings.
2. All tests pass and new behavior has test coverage.
3. No hardcoded values that should be configurable.
4. Error cases are handled gracefully.
5. No unnecessary complexity or dead code.
6. Public interfaces are documented.
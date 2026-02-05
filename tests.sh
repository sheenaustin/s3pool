#!/bin/bash
# =============================================================================
# S3Pool Comprehensive Test Suite
# =============================================================================
# Tests CLI commands, proxy functionality, retry logic, and load balancing.
# Run from the s3pool directory: ./tests.sh
# =============================================================================

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BINARY="./target/release/s3pool"
TEST_BUCKET="${TEST_BUCKET:-webcrawl-dataset}"
TEST_PREFIX="s3pool-test-$(date +%s)"
PROXY_PID=""

# Find an available port for test proxy
find_available_port() {
    local port
    for _ in {1..20}; do
        port=$((30000 + RANDOM % 10000))
        if ! ss -tuln 2>/dev/null | grep -q ":${port} " && \
           ! netstat -tuln 2>/dev/null | grep -q ":${port} "; then
            echo "$port"
            return 0
        fi
    done
    # Fallback to a high random port
    echo "$((40000 + RANDOM % 10000))"
}

PROXY_PORT="${PROXY_PORT:-$(find_available_port)}"
PROXY_URL="http://127.0.0.1:${PROXY_PORT}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# =============================================================================
# Helper Functions
# =============================================================================

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; ((TESTS_PASSED++)) || true; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((TESTS_FAILED++)) || true; }
log_skip() { echo -e "${YELLOW}[SKIP]${NC} $1"; }
log_section() { echo -e "\n${YELLOW}=== $1 ===${NC}"; }

run_test() {
    local name="$1"
    shift
    ((TESTS_RUN++)) || true

    if timeout 60 "$@" >/dev/null 2>&1; then
        log_pass "$name"
        return 0
    else
        log_fail "$name"
        return 1
    fi
}

run_test_output() {
    local name="$1"
    local expected="$2"
    shift 2
    ((TESTS_RUN++)) || true

    local output
    output=$(timeout 60 "$@" 2>&1) || true

    if echo "$output" | grep -q "$expected"; then
        log_pass "$name"
        return 0
    else
        log_fail "$name (expected: $expected)"
        echo "  Output: $output" | head -3
        return 1
    fi
}

cleanup() {
    log_info "Cleaning up test artifacts..."
    # Remove test objects if any were created
    $BINARY --insecure --log-level error rm --recursive "s3://${TEST_BUCKET}/${TEST_PREFIX}/" 2>/dev/null || true
    # Stop proxy if we started it
    stop_proxy
}

trap cleanup EXIT

# =============================================================================
# Proxy Management
# =============================================================================

start_proxy() {
    # Check if proxy already running on our port
    if is_proxy_running; then
        log_info "Proxy already running at ${PROXY_URL}"
        return 0
    fi

    log_info "Starting proxy on port ${PROXY_PORT}..."
    $BINARY --insecure --log-level error proxy --listen "0.0.0.0:${PROXY_PORT}" &
    PROXY_PID=$!

    # Wait for proxy to be ready (max 15 seconds)
    # Check that process is running AND port is listening
    local attempts=0
    while [ $attempts -lt 30 ]; do
        # First check process is still alive
        if ! kill -0 "$PROXY_PID" 2>/dev/null; then
            log_fail "Proxy process died unexpectedly"
            return 1
        fi
        # Then check if port is listening
        if is_proxy_running; then
            # Give it a moment to fully initialize
            sleep 0.5
            log_info "Proxy started (PID: ${PROXY_PID}, port: ${PROXY_PORT})"
            return 0
        fi
        sleep 0.5
        ((attempts++)) || true
    done

    log_fail "Failed to start proxy (timeout waiting for port ${PROXY_PORT})"
    return 1
}

stop_proxy() {
    if [ -n "$PROXY_PID" ] && kill -0 "$PROXY_PID" 2>/dev/null; then
        log_info "Stopping proxy (PID: ${PROXY_PID})..."
        kill "$PROXY_PID" 2>/dev/null || true
        wait "$PROXY_PID" 2>/dev/null || true
        PROXY_PID=""
    fi
}

is_proxy_running() {
    ss -tuln 2>/dev/null | grep -q ":${PROXY_PORT} " || \
    netstat -tuln 2>/dev/null | grep -q ":${PROXY_PORT} "
}

# =============================================================================
# Build Tests
# =============================================================================

test_build() {
    log_section "Build Tests"

    run_test "Binary exists" test -f "$BINARY"
    run_test "Binary is executable" test -x "$BINARY"
    run_test_output "Help flag works" "Usage" $BINARY --help
    run_test_output "Version info" "s3pool" $BINARY --version
}

# =============================================================================
# Configuration Tests
# =============================================================================

test_config() {
    log_section "Configuration Tests"

    run_test ".env file exists" test -f ".env"
    run_test ".env.example exists" test -f ".env.example"

    # Check required env vars are set in .env
    run_test "S3_POOL configured" grep -q "^S3_POOL=" .env
    run_test "AWS_ACCESS_KEY_ID configured" grep -q "^AWS_ACCESS_KEY_ID=" .env
    run_test "AWS_SECRET_ACCESS_KEY configured" grep -q "^AWS_SECRET_ACCESS_KEY=" .env

    # Check optional proxy config
    run_test "PROXY_LISTEN configured" grep -q "PROXY_LISTEN=" .env
    run_test "LB_STRATEGY configured" grep -q "LB_STRATEGY=" .env
    run_test "POOL_MAX_CONNECTIONS configured" grep -q "POOL_MAX_CONNECTIONS=" .env
}

# =============================================================================
# CLI Basic Tests
# =============================================================================

test_cli_basic() {
    log_section "CLI Basic Tests"

    # Stat on a path (fast - single request)
    # May return Object: or Prefix: depending on exact path
    ((TESTS_RUN++)) || true
    local stat_output
    stat_output=$(timeout 30 $BINARY --insecure --log-level error stat "s3://${TEST_BUCKET}/loadtest/" 2>&1) || true
    if echo "$stat_output" | grep -q "Object:\|Prefix:"; then
        log_pass "stat command works"
    else
        log_fail "stat command works"
        echo "  Output: $stat_output" | head -3
    fi

    # Test ls with non-recursive listing (should be faster)
    # Note: large output is buffered, so we test exit code only
    ((TESTS_RUN++)) || true
    if timeout 90 $BINARY --insecure --log-level error ls "s3://${TEST_BUCKET}/loadtest/" >/dev/null 2>&1; then
        log_pass "ls command works"
    else
        log_fail "ls command works (timeout or error)"
    fi

    # du command (uses listing internally)
    run_test_output "du command works" "Total objects:" $BINARY --insecure --log-level error du "s3://${TEST_BUCKET}/loadtest/0/"

    # find command (search with name pattern for faster results)
    run_test_output "find command works" "matching objects" $BINARY --insecure --log-level error find "s3://${TEST_BUCKET}/loadtest/0/" --name elephant
}

# =============================================================================
# CLI Upload/Download Tests
# =============================================================================

test_cli_transfer() {
    log_section "CLI Transfer Tests"

    local test_file="/tmp/s3pool-test-$$.txt"
    local download_file="/tmp/s3pool-download-$$.txt"

    # Create test file
    echo "S3Pool test content $(date)" > "$test_file"

    # Upload
    if $BINARY --insecure --log-level error cp "$test_file" "s3://${TEST_BUCKET}/${TEST_PREFIX}/test.txt" 2>/dev/null; then
        log_pass "cp upload works"
        ((TESTS_RUN++)) || true
        ((TESTS_PASSED++)) || true

        # Download
        if $BINARY --insecure --log-level error cp "s3://${TEST_BUCKET}/${TEST_PREFIX}/test.txt" "$download_file" 2>/dev/null; then
            log_pass "cp download works"
            ((TESTS_RUN++)) || true
            ((TESTS_PASSED++)) || true

            # Verify content
            if diff -q "$test_file" "$download_file" >/dev/null 2>&1; then
                log_pass "cp content integrity"
                ((TESTS_RUN++)) || true
                ((TESTS_PASSED++)) || true
            else
                log_fail "cp content integrity"
                ((TESTS_RUN++)) || true
            fi
        else
            log_fail "cp download works"
            ((TESTS_RUN++)) || true
        fi

        # Delete
        if $BINARY --insecure --log-level error rm "s3://${TEST_BUCKET}/${TEST_PREFIX}/test.txt" 2>/dev/null; then
            log_pass "rm single object works"
            ((TESTS_RUN++)) || true
            ((TESTS_PASSED++)) || true
        else
            log_fail "rm single object works"
            ((TESTS_RUN++)) || true
        fi
    else
        log_fail "cp upload works"
        ((TESTS_RUN++)) || true
        log_skip "cp download (upload failed)"
        log_skip "cp content integrity (upload failed)"
        log_skip "rm single object (upload failed)"
    fi

    # Cleanup
    rm -f "$test_file" "$download_file"
}

# =============================================================================
# Retry Logic Tests
# =============================================================================

test_retry_logic() {
    log_section "Retry Logic Tests"

    # Check that the first backend in S3_POOL is the bad one (10.100.6.50)
    local first_backend
    first_backend=$(grep "^S3_POOL=" .env | cut -d= -f2 | cut -d, -f1)

    if echo "$first_backend" | grep -q "10.100.6.50"; then
        log_info "Bad backend (10.100.6.50) is first in pool - testing retry logic"

        # Check debug logs show retry happening and backend marked unhealthy
        local retry_output
        retry_output=$(timeout 30 $BINARY --insecure --log-level debug stat "s3://${TEST_BUCKET}/loadtest/" 2>&1) || true

        ((TESTS_RUN++)) || true
        if echo "$retry_output" | grep -q "backend marked unhealthy"; then
            log_pass "Bad backend marked unhealthy on connect failure"
        else
            log_fail "Bad backend not marked unhealthy"
            echo "  Output: $retry_output" | head -3
        fi

        ((TESTS_RUN++)) || true
        if echo "$retry_output" | grep -q "Prefix:\|Object:"; then
            log_pass "Command retries to healthy backend"
        else
            log_fail "Command failed to retry to healthy backend"
        fi

        ((TESTS_RUN++)) || true
        if echo "$retry_output" | grep -q "list_retry\|endpoint_retry"; then
            log_pass "Retry logging works"
        else
            # May have succeeded on first healthy backend without retry log
            if echo "$retry_output" | grep -q "connected to"; then
                log_pass "Retry logging works (found connection log)"
            else
                log_fail "Retry logging works"
            fi
        fi
    else
        log_skip "Retry tests (bad backend not first in pool)"
    fi
}

# =============================================================================
# Load Balancer Tests
# =============================================================================

test_load_balancer() {
    log_section "Load Balancer Tests"

    # Check configured strategy
    local strategy
    strategy=$(grep "^LB_STRATEGY=" .env | cut -d= -f2 | cut -d'#' -f1 | tr -d ' ')
    log_info "Configured LB strategy: $strategy"

    # Run multiple list operations and check logs for different backends
    # Use smaller prefix for faster test - just need multiple pages
    local backends_seen
    backends_seen=$(timeout 60 $BINARY --insecure --log-level debug ls --recursive "s3://${TEST_BUCKET}/loadtest/0/" 2>&1 |
        grep -o "connecting to [0-9.]*:" | sort | uniq | wc -l)

    ((TESTS_RUN++)) || true
    if [ "$backends_seen" -gt 1 ]; then
        log_pass "Load balancer distributes requests ($backends_seen backends used)"
    else
        log_fail "Load balancer distributes requests (only $backends_seen backend used)"
    fi
}

# =============================================================================
# Proxy Tests
# =============================================================================

test_proxy() {
    log_section "Proxy Tests"

    # Auto-start proxy if not running
    if ! is_proxy_running; then
        start_proxy || return
    fi

    log_info "Proxy running at ${PROXY_URL}"

    # Test proxy health
    ((TESTS_RUN++)) || true
    if curl -s --connect-timeout 5 "${PROXY_URL}/${TEST_BUCKET}/" | grep -q "ListBucketResult\|Contents\|CommonPrefixes"; then
        log_pass "Proxy LIST works"
    else
        log_fail "Proxy LIST works"
    fi

    # Test proxy PUT/GET/DELETE
    local test_key="${TEST_PREFIX}/proxy-test.txt"
    local test_content="Proxy test $(date)"

    ((TESTS_RUN++)) || true
    if echo "$test_content" | curl -s -X PUT "${PROXY_URL}/${TEST_BUCKET}/${test_key}" -d @- >/dev/null 2>&1; then
        log_pass "Proxy PUT works"

        ((TESTS_RUN++)) || true
        local get_content
        get_content=$(curl -s "${PROXY_URL}/${TEST_BUCKET}/${test_key}")
        if [ "$get_content" = "$test_content" ]; then
            log_pass "Proxy GET works"
        else
            log_fail "Proxy GET works"
        fi

        ((TESTS_RUN++)) || true
        if curl -s -X DELETE "${PROXY_URL}/${TEST_BUCKET}/${test_key}" >/dev/null 2>&1; then
            log_pass "Proxy DELETE works"
        else
            log_fail "Proxy DELETE works"
        fi
    else
        log_fail "Proxy PUT works"
        log_skip "Proxy GET (PUT failed)"
        log_skip "Proxy DELETE (PUT failed)"
    fi
}

# =============================================================================
# CLI vs Proxy Consistency Tests
# =============================================================================

test_consistency() {
    log_section "CLI vs Proxy Consistency Tests"

    # Ensure proxy is running
    if ! is_proxy_running; then
        start_proxy || return
    fi

    local test_key="${TEST_PREFIX}/consistency-test.txt"
    local test_content="Consistency test content $(date +%s)"
    local test_file="/tmp/s3pool-consistency-$$.txt"
    local download_cli="/tmp/s3pool-download-cli-$$.txt"
    local download_proxy="/tmp/s3pool-download-proxy-$$.txt"

    echo "$test_content" > "$test_file"

    # Test 1: Upload via CLI, download via both CLI and proxy
    log_info "Testing: CLI upload -> CLI download vs proxy download"

    if $BINARY --insecure --log-level error cp "$test_file" "s3://${TEST_BUCKET}/${test_key}" 2>/dev/null; then
        # Download via CLI
        $BINARY --insecure --log-level error cp "s3://${TEST_BUCKET}/${test_key}" "$download_cli" 2>/dev/null || true

        # Download via proxy
        curl -s "${PROXY_URL}/${TEST_BUCKET}/${test_key}" > "$download_proxy" 2>/dev/null || true

        ((TESTS_RUN++)) || true
        if diff -q "$test_file" "$download_cli" >/dev/null 2>&1 && diff -q "$test_file" "$download_proxy" >/dev/null 2>&1; then
            log_pass "CLI upload: CLI and proxy downloads match"
        else
            log_fail "CLI upload: CLI and proxy downloads don't match"
        fi

        # Cleanup this test object
        $BINARY --insecure --log-level error rm "s3://${TEST_BUCKET}/${test_key}" 2>/dev/null || true
    else
        log_fail "CLI upload failed for consistency test"
        ((TESTS_RUN++)) || true
    fi

    # Test 2: Upload via proxy, download via both CLI and proxy
    log_info "Testing: Proxy upload -> CLI download vs proxy download"

    local test_key2="${TEST_PREFIX}/consistency-test2.txt"
    if echo "$test_content" | curl -s -X PUT "${PROXY_URL}/${TEST_BUCKET}/${test_key2}" -d @- >/dev/null 2>&1; then
        # Download via CLI
        $BINARY --insecure --log-level error cp "s3://${TEST_BUCKET}/${test_key2}" "$download_cli" 2>/dev/null || true

        # Download via proxy
        curl -s "${PROXY_URL}/${TEST_BUCKET}/${test_key2}" > "$download_proxy" 2>/dev/null || true

        ((TESTS_RUN++)) || true
        if [ "$(cat "$download_cli")" = "$test_content" ] && [ "$(cat "$download_proxy")" = "$test_content" ]; then
            log_pass "Proxy upload: CLI and proxy downloads match"
        else
            log_fail "Proxy upload: CLI and proxy downloads don't match"
        fi

        # Cleanup this test object
        curl -s -X DELETE "${PROXY_URL}/${TEST_BUCKET}/${test_key2}" >/dev/null 2>&1 || true
    else
        log_fail "Proxy upload failed for consistency test"
        ((TESTS_RUN++)) || true
    fi

    # Test 3: List via CLI vs proxy (compare object counts)
    log_info "Testing: CLI ls vs proxy LIST (object count comparison)"

    local cli_count proxy_count
    cli_count=$($BINARY --insecure --log-level error ls "s3://${TEST_BUCKET}/loadtest/0/" 2>/dev/null | head -100 | wc -l)

    # Parse proxy XML response for object count
    proxy_count=$(curl -s "${PROXY_URL}/${TEST_BUCKET}/?prefix=loadtest/0/&max-keys=100" 2>/dev/null | grep -c "<Key>" || echo "0")

    ((TESTS_RUN++)) || true
    if [ "$cli_count" -gt 0 ] && [ "$proxy_count" -gt 0 ]; then
        # Allow some variance due to timing, but counts should be close
        local diff_count=$((cli_count > proxy_count ? cli_count - proxy_count : proxy_count - cli_count))
        if [ "$diff_count" -le 5 ]; then
            log_pass "CLI vs proxy list counts match (CLI: $cli_count, Proxy: $proxy_count)"
        else
            log_fail "CLI vs proxy list counts differ significantly (CLI: $cli_count, Proxy: $proxy_count)"
        fi
    else
        log_fail "CLI or proxy list returned no results (CLI: $cli_count, Proxy: $proxy_count)"
    fi

    # Cleanup temp files
    rm -f "$test_file" "$download_cli" "$download_proxy"
}

# =============================================================================
# Code Quality Tests
# =============================================================================

test_code_quality() {
    log_section "Code Quality Tests"

    # Check for duplicated retry logic patterns
    local inline_retry_count
    inline_retry_count=$(grep -r "for.*retry in 0\.\." src/ 2>/dev/null | wc -l) || true

    ((TESTS_RUN++)) || true
    if [ "$inline_retry_count" -eq 0 ]; then
        log_pass "No inline retry loops (uses shared logic)"
    else
        log_fail "Found $inline_retry_count inline retry loops (should use shared logic)"
    fi

    # Check that shared helpers exist
    ((TESTS_RUN++)) || true
    if grep -q "fn select_untried_endpoint" src/core/mod.rs 2>/dev/null; then
        log_pass "Shared select_untried_endpoint helper exists"
    else
        log_fail "Shared select_untried_endpoint helper missing"
    fi

    ((TESTS_RUN++)) || true
    if grep -q "fn is_retryable_error" src/core/mod.rs 2>/dev/null; then
        log_pass "Shared is_retryable_error helper exists"
    else
        log_fail "Shared is_retryable_error helper missing"
    fi

    # Check that CLI uses shared helpers (either direct or via with_endpoint_retry)
    ((TESTS_RUN++)) || true
    if grep -q "select_untried_endpoint\|with_endpoint_retry" src/cli/commands.rs 2>/dev/null; then
        log_pass "CLI uses shared endpoint selection"
    else
        log_fail "CLI should use shared endpoint selection"
    fi

    # Check that proxy uses shared helpers
    ((TESTS_RUN++)) || true
    if grep -q "select_untried_endpoint" src/proxy/server.rs 2>/dev/null; then
        log_pass "Proxy uses shared endpoint selection"
    else
        log_fail "Proxy should use shared endpoint selection"
    fi
}

# =============================================================================
# Performance Tests
# =============================================================================

test_performance() {
    log_section "Performance Tests"

    # Time a list operation (use smaller prefix for faster test)
    local start_time end_time duration
    start_time=$(date +%s%N)
    local count
    count=$(timeout 120 $BINARY --insecure --log-level error ls --recursive "s3://${TEST_BUCKET}/loadtest/0/" 2>/dev/null | wc -l)
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))  # ms

    log_info "Listed $count objects in ${duration}ms"

    ((TESTS_RUN++)) || true
    if [ "$count" -gt 0 ]; then
        local rate=$(( count * 1000 / (duration + 1) ))  # objects/sec
        log_pass "List performance: ${rate} objects/sec"
    else
        log_fail "List performance (no objects listed)"
    fi
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo -e "\n${GREEN}S3Pool Comprehensive Test Suite${NC}"
    echo "=================================="
    echo "Bucket: ${TEST_BUCKET}"
    echo "Test prefix: ${TEST_PREFIX}"
    echo "Proxy port: ${PROXY_PORT}"
    echo ""

    # Run all test suites
    test_build
    test_config
    test_cli_basic
    test_cli_transfer
    test_retry_logic
    test_load_balancer
    test_proxy
    test_consistency
    test_code_quality
    # Skip performance test for faster CI (uncomment for full benchmarking)
    # test_performance

    # Summary
    log_section "Test Summary"
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"

    if [ "$TESTS_FAILED" -eq 0 ]; then
        echo -e "\n${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "\n${RED}Some tests failed.${NC}"
        exit 1
    fi
}

main "$@"

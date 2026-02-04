#!/bin/bash
# Test script for s3pool proxy server

set -e

PROXY_URL="http://localhost:8000"
TEST_BUCKET="test-bucket"
TEST_FILE="test-file.txt"
TEST_CONTENT="Hello, S3Pool!"

echo "=== S3Pool Proxy Test Script ==="
echo ""

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

function success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

function error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if proxy is running
info "Checking if proxy is running at ${PROXY_URL}..."
if ! curl -s --connect-timeout 2 "${PROXY_URL}/" > /dev/null 2>&1; then
    error "Proxy is not running at ${PROXY_URL}"
    error "Start it with: s3pool proxy --env"
    exit 1
fi
success "Proxy is running"
echo ""

# Test HEAD bucket
info "Testing HEAD bucket: ${TEST_BUCKET}"
if curl -s -I -X HEAD "${PROXY_URL}/${TEST_BUCKET}" | grep -q "200\|404"; then
    success "HEAD bucket works"
else
    error "HEAD bucket failed"
fi
echo ""

# Test PUT object
info "Testing PUT object: ${TEST_BUCKET}/${TEST_FILE}"
echo "${TEST_CONTENT}" | curl -s -X PUT "${PROXY_URL}/${TEST_BUCKET}/${TEST_FILE}" -d @- > /dev/null
if [ $? -eq 0 ]; then
    success "PUT object works"
else
    error "PUT object failed"
fi
echo ""

# Test GET object
info "Testing GET object: ${TEST_BUCKET}/${TEST_FILE}"
RESPONSE=$(curl -s "${PROXY_URL}/${TEST_BUCKET}/${TEST_FILE}")
if [ $? -eq 0 ]; then
    success "GET object works"
    echo "Response: ${RESPONSE}"
else
    error "GET object failed"
fi
echo ""

# Test HEAD object
info "Testing HEAD object: ${TEST_BUCKET}/${TEST_FILE}"
if curl -s -I -X HEAD "${PROXY_URL}/${TEST_BUCKET}/${TEST_FILE}" | grep -q "200"; then
    success "HEAD object works"
else
    error "HEAD object failed"
fi
echo ""

# Test LIST objects
info "Testing LIST objects: ${TEST_BUCKET}/"
RESPONSE=$(curl -s "${PROXY_URL}/${TEST_BUCKET}/")
if [ $? -eq 0 ]; then
    success "LIST objects works"
    echo "Response: ${RESPONSE}"
else
    error "LIST objects failed"
fi
echo ""

# Test LIST objects with prefix
info "Testing LIST objects with prefix: ${TEST_BUCKET}/?prefix=test"
RESPONSE=$(curl -s "${PROXY_URL}/${TEST_BUCKET}/?prefix=test")
if [ $? -eq 0 ]; then
    success "LIST objects with prefix works"
    echo "Response: ${RESPONSE}"
else
    error "LIST objects with prefix failed"
fi
echo ""

# Test DELETE object
info "Testing DELETE object: ${TEST_BUCKET}/${TEST_FILE}"
curl -s -X DELETE "${PROXY_URL}/${TEST_BUCKET}/${TEST_FILE}" > /dev/null
if [ $? -eq 0 ]; then
    success "DELETE object works"
else
    error "DELETE object failed"
fi
echo ""

echo "=== Test Complete ==="

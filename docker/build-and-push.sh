#!/usr/bin/env bash
# Build and publish Zebra Docker image with versioning
#
# Usage:
#   ./docker/build-and-push.sh [DOCKER_HUB_USERNAME] [--push]
#
# Environment variables:
#   DOCKER_HUB_USERNAME - Your Docker Hub username (or pass as first argument)
#   DOCKER_HUB_PASSWORD - Your Docker Hub password/token (for pushing)
#   IMAGE_NAME          - Custom image name (default: zebrad)
#   VERSION             - Override version (default: from Cargo.toml)
#
# Examples:
#   ./docker/build-and-push.sh myusername
#   ./docker/build-and-push.sh myusername --push
#   DOCKER_HUB_USERNAME=myusername ./docker/build-and-push.sh --push

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Parse arguments
DOCKER_HUB_USERNAME="${1:-${DOCKER_HUB_USERNAME:-}}"
PUSH_IMAGE=false

if [[ "${1:-}" == "--push" ]]; then
    PUSH_IMAGE=true
    DOCKER_HUB_USERNAME="${DOCKER_HUB_USERNAME:-}"
elif [[ "${2:-}" == "--push" ]] || [[ "${1:-}" == *"--push"* ]]; then
    PUSH_IMAGE=true
fi

# Get version from Cargo.toml
VERSION="${VERSION:-$(grep -E '^version\s*=' "${PROJECT_ROOT}/zebrad/Cargo.toml" | head -1 | sed -E 's/.*version\s*=\s*"([^"]+)".*/\1/')}"
if [[ -z "${VERSION}" ]]; then
    echo -e "${RED}Error: Could not extract version from Cargo.toml${NC}" >&2
    exit 1
fi

# Get git commit hash
GIT_COMMIT=$(cd "${PROJECT_ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_TAG=$(cd "${PROJECT_ROOT}" && git describe --tags --exact-match 2>/dev/null || echo "")

# Image name
IMAGE_NAME="${IMAGE_NAME:-zebrad}"

# Build image name
if [[ -n "${DOCKER_HUB_USERNAME}" ]]; then
    IMAGE_BASE="${DOCKER_HUB_USERNAME}/${IMAGE_NAME}"
else
    IMAGE_BASE="${IMAGE_NAME}"
fi

# Build tags
TAGS=(
    "${IMAGE_BASE}:${VERSION}"
    "${IMAGE_BASE}:${VERSION}-${GIT_COMMIT}"
)

# Add 'latest' tag if this is a release tag
if [[ -n "${GIT_TAG}" ]] || [[ "${VERSION}" != *"-"* ]]; then
    TAGS+=("${IMAGE_BASE}:latest")
fi

# Add git commit tag
TAGS+=("${IMAGE_BASE}:${GIT_COMMIT}")

echo -e "${GREEN}Building Zebra Docker image${NC}"
echo -e "  Version: ${YELLOW}${VERSION}${NC}"
echo -e "  Git commit: ${YELLOW}${GIT_COMMIT}${NC}"
if [[ -n "${GIT_TAG}" ]]; then
    echo -e "  Git tag: ${YELLOW}${GIT_TAG}${NC}"
fi
echo -e "  Image: ${YELLOW}${IMAGE_BASE}${NC}"
echo -e "  Tags: ${YELLOW}${TAGS[*]}${NC}"
echo ""

# Build the image
cd "${PROJECT_ROOT}"

# Build with all tags
TAG_ARGS=()
for tag in "${TAGS[@]}"; do
    TAG_ARGS+=(--tag "${tag}")
done

echo -e "${GREEN}Building Docker image...${NC}"
docker build \
    --file "${SCRIPT_DIR}/Dockerfile" \
    --target runtime \
    "${TAG_ARGS[@]}" \
    "${PROJECT_ROOT}"

echo -e "${GREEN}✓ Build complete!${NC}"
echo ""

# List images
echo -e "${GREEN}Built images:${NC}"
for tag in "${TAGS[@]}"; do
    echo -e "  ${YELLOW}${tag}${NC}"
done
echo ""

# Push to Docker Hub if requested
if [[ "${PUSH_IMAGE}" == "true" ]]; then
    if [[ -z "${DOCKER_HUB_USERNAME}" ]]; then
        echo -e "${RED}Error: Docker Hub username required for pushing.${NC}" >&2
        echo "  Set DOCKER_HUB_USERNAME environment variable or pass as first argument" >&2
        exit 1
    fi

    echo -e "${GREEN}Pushing images to Docker Hub...${NC}"
    
    # Login if needed (check if already logged in)
    if ! docker info | grep -q "Username:"; then
        if [[ -z "${DOCKER_HUB_PASSWORD:-}" ]]; then
            echo -e "${YELLOW}Note: DOCKER_HUB_PASSWORD not set. You may need to login manually.${NC}"
            echo "  Run: docker login"
        else
            echo "Logging in to Docker Hub..."
            echo "${DOCKER_HUB_PASSWORD}" | docker login --username "${DOCKER_HUB_USERNAME}" --password-stdin
        fi
    fi

    # Push all tags
    for tag in "${TAGS[@]}"; do
        echo -e "  Pushing ${YELLOW}${tag}${NC}..."
        docker push "${tag}"
    done

    echo -e "${GREEN}✓ All images pushed successfully!${NC}"
    echo ""
    echo -e "${GREEN}You can now pull the image with:${NC}"
    echo -e "  ${YELLOW}docker pull ${TAGS[0]}${NC}"
else
    echo -e "${YELLOW}To push to Docker Hub, run with --push flag:${NC}"
    echo -e "  ${YELLOW}./docker/build-and-push.sh ${DOCKER_HUB_USERNAME:-<username>} --push${NC}"
fi


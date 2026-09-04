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

# Parse arguments without treating --push as a Docker Hub username.
docker_hub_username="${DOCKER_HUB_USERNAME:-}"
push_image=false
username_argument_seen=false

while (($# > 0)); do
    case "$1" in
        --push)
            push_image=true
            ;;
        --help|-h)
            sed -n '2,17p' "$0" | sed -E 's/^# ?//'
            exit 0
            ;;
        --*)
            echo -e "${RED}Error: Unknown option: $1${NC}" >&2
            exit 2
            ;;
        *)
            if [[ "${username_argument_seen}" == "true" ]]; then
                echo -e "${RED}Error: Only one Docker Hub username can be supplied${NC}" >&2
                exit 2
            fi
            docker_hub_username="$1"
            username_argument_seen=true
            ;;
    esac
    shift
done

# Get the binary version from Cargo.toml, then apply an optional tag override.
cargo_version="$(grep -E '^version\s*=' "${PROJECT_ROOT}/zebrad/Cargo.toml" | head -1 | sed -E 's/.*version\s*=\s*"([^"]+)".*/\1/')"
if [[ -z "${cargo_version}" ]]; then
    echo -e "${RED}Error: Could not extract version from Cargo.toml${NC}" >&2
    exit 1
fi
VERSION="${VERSION:-${cargo_version}}"

# Validate overrides as SemVer before converting them to Docker tags. Docker does not allow the
# build-metadata separator (`+`), so encode it as `_`; underscores are forbidden by SemVer, making
# this mapping collision-free (unlike mapping `+` to `-`).
numeric_identifier='(0|[1-9][0-9]*)'
alphanumeric_identifier='([0-9]*[A-Za-z-][0-9A-Za-z-]*)'
prerelease_identifier="(${numeric_identifier}|${alphanumeric_identifier})"
semver_pattern="^${numeric_identifier}\\.${numeric_identifier}\\.${numeric_identifier}(-${prerelease_identifier}(\\.${prerelease_identifier})*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?$"
if [[ ! "${VERSION}" =~ ${semver_pattern} ]]; then
    echo -e "${RED}Error: VERSION is not valid SemVer: ${VERSION}${NC}" >&2
    exit 1
fi
docker_version="${VERSION/+/_}"
if [[ ! "${docker_version}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo -e "${RED}Error: VERSION cannot be represented as a Docker tag: ${VERSION}${NC}" >&2
    exit 1
fi

# Get git commit hash
git_full_commit=$(cd "${PROJECT_ROOT}" && git rev-parse HEAD 2>/dev/null || echo "unknown")
if [[ "${git_full_commit}" == "unknown" ]]; then
    GIT_COMMIT="unknown"
else
    # A fixed prefix is stable across clones regardless of git's core.abbrev setting.
    GIT_COMMIT="${git_full_commit:0:12}"
fi
GIT_TAG=$(cd "${PROJECT_ROOT}" && git describe --tags --exact-match HEAD 2>/dev/null || echo "")

git_dirty=false
if [[ "${GIT_COMMIT}" != "unknown" ]] && [[ -n "$(cd "${PROJECT_ROOT}" && git status --porcelain --untracked-files=normal)" ]]; then
    git_dirty=true
fi

source_tag="${GIT_COMMIT}"
if [[ "${git_dirty}" == "true" ]]; then
    source_tag="${source_tag}-dirty"
fi

is_release=false
if [[ "${git_dirty}" == "false" ]] \
    && [[ "${VERSION}" == "${cargo_version}" ]] \
    && { [[ "${GIT_TAG}" == "${VERSION}" ]] || [[ "${GIT_TAG}" == "v${VERSION}" ]]; }; then
    is_release=true
fi

# Image name
IMAGE_NAME="${IMAGE_NAME:-zebrad}"

# Build image name
if [[ -n "${docker_hub_username}" ]]; then
    IMAGE_BASE="${docker_hub_username}/${IMAGE_NAME}"
else
    IMAGE_BASE="${IMAGE_NAME}"
fi

if [[ "${push_image}" == "true" ]]; then
    if [[ -z "${docker_hub_username}" ]]; then
        echo -e "${RED}Error: Docker Hub username required for pushing.${NC}" >&2
        echo "  Set DOCKER_HUB_USERNAME environment variable or pass as first argument" >&2
        exit 1
    fi

    if [[ "${GIT_COMMIT}" == "unknown" ]] || [[ "${git_dirty}" == "true" ]]; then
        echo -e "${RED}Error: Refusing to push an image without a clean, identifiable git commit.${NC}" >&2
        exit 1
    fi
fi

# Development builds only receive immutable source-specific tags. Mutable version and latest tags
# are reserved for a clean commit checked out at the matching release tag.
TAGS=(
    "${IMAGE_BASE}:${docker_version}-${source_tag}"
    "${IMAGE_BASE}:${source_tag}"
)
if [[ "${is_release}" == "true" ]]; then
    TAGS=(
        "${IMAGE_BASE}:${docker_version}"
        "${TAGS[@]}"
    )
    # Prereleases get their immutable/versioned tags, but must not replace the stable `latest` tag.
    version_without_build_metadata="${VERSION%%+*}"
    if [[ "${version_without_build_metadata}" != *-* ]]; then
        TAGS=(
            "${IMAGE_BASE}:latest"
            "${TAGS[@]}"
        )
    fi
fi

echo -e "${GREEN}Building Zebra Docker image${NC}"
echo -e "  Version: ${YELLOW}${VERSION}${NC}"
if [[ "${docker_version}" != "${VERSION}" ]]; then
    echo -e "  Docker version tag: ${YELLOW}${docker_version}${NC}"
fi
echo -e "  Git commit: ${YELLOW}${GIT_COMMIT}${NC}"
if [[ -n "${GIT_TAG}" ]]; then
    echo -e "  Git tag: ${YELLOW}${GIT_TAG}${NC}"
fi
if [[ "${git_dirty}" == "true" ]]; then
    echo -e "  Working tree: ${YELLOW}dirty${NC}"
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
    --build-arg "SHORT_SHA=${GIT_COMMIT}" \
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
if [[ "${push_image}" == "true" ]]; then
    echo -e "${GREEN}Pushing images to Docker Hub...${NC}"

    # Login if needed (check if already logged in)
    if ! docker info | grep -q "Username:"; then
        if [[ -z "${DOCKER_HUB_PASSWORD:-}" ]]; then
            echo -e "${YELLOW}Note: DOCKER_HUB_PASSWORD not set. You may need to login manually.${NC}"
            echo "  Run: docker login"
        else
            echo "Logging in to Docker Hub..."
            echo "${DOCKER_HUB_PASSWORD}" | docker login --username "${docker_hub_username}" --password-stdin
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
    echo -e "  ${YELLOW}./docker/build-and-push.sh ${docker_hub_username:-<username>} --push${NC}"
fi

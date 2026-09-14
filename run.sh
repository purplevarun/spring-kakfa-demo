#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf 'Usage: %s {inventory|notification|order}\n' "${0##*/}"
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 2
fi

case "$1" in
    inventory|inventory-service)
        service=inventory
        port=4001
        ;;
    notification|notification-service)
        service=notification
        port=4002
        ;;
    order|order-service)
        service=order
        port=4003
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        printf 'Unknown service: %s\n' "$1" >&2
        usage >&2
        exit 2
        ;;
esac

if ! command -v docker >/dev/null 2>&1; then
    printf 'Docker is not installed or is not on PATH.\n' >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    printf 'Docker is not available. Start Docker and check your Docker context.\n' >&2
    exit 1
fi

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
service_name="${service}-service"
image="${service_name}:latest"

printf 'Building %s...\n' "$service_name"
docker build --tag "$image" "$project_dir/$service_name"

printf 'Starting %s on port %s...\n' "$service_name" "$port"
exec docker run --rm --init --name "$service_name" \
    --publish "$port:$port" \
    "$image"

#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf 'Usage: %s [all|order|inventory|notification|frontend]\n' "${0##*/}"
    printf 'With no argument, builds and starts the complete stack.\n'
}

if [[ $# -gt 1 ]]; then
    usage >&2
    exit 2
fi

case "${1:-all}" in
    all)
        service=""
        ;;
    inventory|inventory-service)
        service=inventory-service
        ;;
    notification|notification-service)
        service=notification-service
        ;;
    order|order-service)
        service=order-service
        ;;
    frontend)
        service=frontend
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

if ! docker compose version >/dev/null 2>&1; then
    printf 'Docker Compose is required.\n' >&2
    exit 1
fi

printf 'Starting %s with Docker Compose...\n' "${service:-all services}"
if [[ -z "$service" ]]; then
    exec docker compose --file "$project_dir/docker-compose.yml" up --build
else
    exec docker compose --file "$project_dir/docker-compose.yml" up --build "$service"
fi

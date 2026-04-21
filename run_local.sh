#!/usr/bin/env bash
# Convenience wrapper to run the Kafka visualizer against local Kafka.
#
# Usage:
#   ./run_local.sh [options]
#
# All arguments are forwarded to kafka-event-visualizer.sh
#
# Examples:
#   ./run_local.sh
#   ./run_local.sh --filter ORDER
#   ./run_local.sh --since-minutes 30
#   ./run_local.sh --bootstrap kafka.local:9092 --topic my-topic

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$SCRIPT_DIR/kafka-event-visualizer.sh" "$@"

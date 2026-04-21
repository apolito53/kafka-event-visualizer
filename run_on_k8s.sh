#!/usr/bin/env bash
# Convenience wrapper to run the Kafka visualizer against Kubernetes Kafka cluster.
#
# Usage:
#   ./run_on_k8s.sh [options]
#
# All arguments are forwarded to kafka-event-visualizer-k8s.sh
#
# Examples:
#   ./run_on_k8s.sh --topic my-events --bootstrap kafka:9092
#   ./run_on_k8s.sh --topic my-events --bootstrap kafka:9092 --filter ORDER
#   ./run_on_k8s.sh --topic my-events --bootstrap kafka:9092 --since-minutes 30

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$SCRIPT_DIR/kafka-event-visualizer-k8s.sh" "$@"

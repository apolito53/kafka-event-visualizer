#!/usr/bin/env bash
# Launches the Kafka Event Bus Visualizer against a Kubernetes Kafka cluster.
#
# Architecture: a lightweight bridge script runs inside a temporary pod,
# consuming Kafka and streaming JSON lines to stdout. An HTTP server in the
# pod handles on-demand history requests. Both outputs pipe to the visualizer
# running locally in --stdin mode, so Rich renders directly on your terminal.
#
# Usage:
#   ./kafka-event-visualizer-k8s.sh [options]
#
# Options:
#   --topic TOPIC              Kafka topic. If omitted, choose from a topic picker.
#   --bootstrap BOOTSTRAP      Kafka bootstrap servers inside the cluster
#                               (default: confluent-platform-cp-kafka:9092)
#   --since-minutes N          Load history from N minutes ago
#   --history-count N          Number of recent events to preload (default: 200)
#   --filter PATTERN           Filter events by type pattern
#   --window-size N            Max events in memory (default: 2000)
#
# Examples:
#   ./kafka-event-visualizer-k8s.sh
#   ./kafka-event-visualizer-k8s.sh --filter ORDER
#   ./kafka-event-visualizer-k8s.sh --since-minutes 30
#   ./kafka-event-visualizer-k8s.sh --topic my-events --bootstrap kafka:9092
#
# The temporary pod is automatically cleaned up on exit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VISUALIZER_SCRIPT="$SCRIPT_DIR/kafka-event-visualizer.sh"
POD_NAME="kafka-visualizer-$$"
KAFKA_BOOTSTRAP="confluent-platform-cp-kafka:9092"
HTTP_PORT=18080

# Defaults
TOPIC=""
SINCE_MINUTES=""
HISTORY_COUNT="200"
FILTER=""
WINDOW_SIZE="2000"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --topic) TOPIC="$2"; shift 2 ;;
        --bootstrap) KAFKA_BOOTSTRAP="$2"; shift 2 ;;
        --since-minutes) SINCE_MINUTES="$2"; shift 2 ;;
        --history-count) HISTORY_COUNT="$2"; shift 2 ;;
        --filter) FILTER="$2"; shift 2 ;;
        --window-size) WINDOW_SIZE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ ! -f "$VISUALIZER_SCRIPT" ]; then
    echo "ERROR: kafka-event-visualizer.sh not found at $VISUALIZER_SCRIPT"
    exit 1
fi

pick_topic() {
    local topics=()
    local selected=""

    echo "Fetching topic metadata from $KAFKA_BOOTSTRAP..." >&2
    if ! mapfile -t topics < <(
        kubectl exec -i "$POD_NAME" -- python3 - "$KAFKA_BOOTSTRAP" <<'PY'
import sys
from kafka import KafkaConsumer

bootstrap = sys.argv[1]
consumer = KafkaConsumer(bootstrap_servers=bootstrap, consumer_timeout_ms=1000)
try:
    for topic in sorted(t for t in consumer.topics() if not t.startswith("__")):
        print(topic)
finally:
    consumer.close()
PY
    ); then
        echo "ERROR: failed to fetch Kafka topic metadata from $KAFKA_BOOTSTRAP" >&2
        return 1
    fi

    if [ "${#topics[@]}" -eq 0 ]; then
        echo "ERROR: no observable Kafka topics found from $KAFKA_BOOTSTRAP" >&2
        return 1
    fi

    if [ -t 0 ] && [ -t 1 ] && command -v whiptail >/dev/null 2>&1; then
        local menu_items=()
        local menu_height
        local dialog_height
        local dialog_width=96
        local i

        menu_height="${#topics[@]}"
        if [ "$menu_height" -gt 18 ]; then
            menu_height=18
        fi
        dialog_height=$((menu_height + 9))

        for i in "${!topics[@]}"; do
            menu_items+=("$((i + 1))" "${topics[$i]}")
        done

        if selected=$(whiptail \
            --title "Kafka Topic Picker" \
            --ok-button "Launch" \
            --cancel-button "Cancel" \
            --menu "Choose a topic from $KAFKA_BOOTSTRAP:" \
            "$dialog_height" "$dialog_width" "$menu_height" \
            "${menu_items[@]}" \
            3>&1 1>&2 2>&3); then
            TOPIC="${topics[$((selected - 1))]}"
            return 0
        fi

        echo "Topic selection canceled." >&2
        return 1
    fi

    echo "" >&2
    echo "Select a Kafka topic:" >&2
    local i
    for i in "${!topics[@]}"; do
        printf '  %3d) %s\n' "$((i + 1))" "${topics[$i]}" >&2
    done
    echo "" >&2

    while true; do
        read -r -p "Topic number or exact topic name: " selected
        if [[ "$selected" =~ ^[0-9]+$ ]] && [ "$selected" -ge 1 ] && [ "$selected" -le "${#topics[@]}" ]; then
            TOPIC="${topics[$((selected - 1))]}"
            return 0
        fi
        for i in "${!topics[@]}"; do
            if [ "${topics[$i]}" = "$selected" ]; then
                TOPIC="$selected"
                return 0
            fi
        done
        echo "Invalid selection. Enter a number from 1-${#topics[@]} or an exact topic name." >&2
    done
}

cleanup() {
    if [ -n "${PF_PID:-}" ]; then
        kill "$PF_PID" 2>/dev/null || true
    fi
    echo "" >&2
    echo "Cleaning up pod $POD_NAME..." >&2
    kubectl delete pod "$POD_NAME" --grace-period=0 --force 2>/dev/null || true
}
trap cleanup EXIT

echo "Creating temporary pod $POD_NAME..." >&2
kubectl run "$POD_NAME" \
    --image=python:3.11-slim \
    --restart=Never \
    --command -- sleep 3600 >/dev/null

echo "Waiting for pod to be ready..." >&2
kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s >/dev/null

echo "Installing kafka-python..." >&2
kubectl exec "$POD_NAME" -- pip install --quiet --disable-pip-version-check --root-user-action=ignore kafka-python >/dev/null

if [ -z "$TOPIC" ]; then
    pick_topic
fi

# Build bridge args
BRIDGE_ARGS="'$KAFKA_BOOTSTRAP', '$TOPIC', $HISTORY_COUNT"
if [ -n "$SINCE_MINUTES" ]; then
    BRIDGE_ARGS="$BRIDGE_ARGS, since_minutes=$SINCE_MINUTES"
fi

echo "Starting port-forward for HTTP bridge..." >&2
kubectl port-forward "pod/$POD_NAME" "$HTTP_PORT:8080" >/dev/null 2>&1 &
PF_PID=$!
sleep 2

if ! kill -0 "$PF_PID" 2>/dev/null; then
    echo "ERROR: port-forward failed to start" >&2
    exit 1
fi

echo "Launching bridge -> visualizer..." >&2
echo "" >&2

# The bridge runs in the pod with HTTP server + Kafka consumer
kubectl exec "$POD_NAME" -- python3 -u -c "
import json, sys, time, threading
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Shared state
bootstrap = None
topic = None
output_lock = threading.Lock()
last_request_time = 0
request_in_flight = False
fetch_depth = 0

def emit(msg, default_topic, marker=None):
    val = msg.value
    if isinstance(val, dict):
        event_type = str(val.get('type', 'UNKNOWN'))
        timestamp = val.get('timestamp', datetime.now().isoformat())
    else:
        event_type = 'UNKNOWN'
        timestamp = datetime.now().isoformat()

    output = {
        'event_type': event_type,
        'timestamp': timestamp,
        'key': msg.key,
        'payload': val,
        'topic': getattr(msg, 'topic', default_topic),
        'partition': getattr(msg, 'partition', None),
        'offset': getattr(msg, 'offset', None),
        'broker_timestamp_ms': getattr(msg, 'timestamp', None),
    }
    if marker:
        output['__marker'] = marker

    with output_lock:
        try:
            print(json.dumps(output, default=str), flush=True)
        except BrokenPipeError:
            sys.exit(0)

def fetch_history(count, depth):
    try:
        hist = KafkaConsumer(
            bootstrap_servers=bootstrap,
            enable_auto_commit=False,
            key_deserializer=lambda m: int.from_bytes(m, byteorder='big', signed=True) if m and len(m) == 8 else None,
            value_deserializer=lambda m: json.loads(m.decode('utf-8', errors='replace')),
            consumer_timeout_ms=2000,
        )

        partitions = hist.partitions_for_topic(topic)
        if not partitions:
            return []

        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        hist.assign(tps)
        end_offsets = hist.end_offsets(tps)

        per_partition = max(1, count // len(tps)) * 2
        limit_offsets = {}
        for tp in tps:
            end_off = end_offsets.get(tp, 0)
            start = max(0, end_off - (per_partition * (depth + 1)))
            limit = max(0, end_off - (per_partition * depth))
            hist.seek(tp, start)
            limit_offsets[tp] = limit

        collected = []
        while True:
            records = hist.poll(timeout_ms=2000)
            if not records:
                break
            for tp_key, messages in records.items():
                limit = limit_offsets.get(tp_key, 0)
                for msg in messages:
                    if msg.offset < limit:
                        collected.append(msg)
            if all(hist.position(tp) >= limit_offsets.get(tp, 0) for tp in tps):
                break

        hist.close()
        collected.sort(key=lambda m: (getattr(m, 'timestamp', 0), getattr(m, 'offset', 0)))
        return collected[-count:] if len(collected) > count else collected
    except Exception as e:
        sys.stderr.write(f'History fetch error: {e}\n')
        sys.stderr.flush()
        return []

class HistoryHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global last_request_time, request_in_flight, fetch_depth

        if '/load-history' not in self.path:
            self.send_response(404)
            self.end_headers()
            return

        now = time.time()
        if now - last_request_time < 0.5:
            self.send_response(429)
            self.end_headers()
            return

        if request_in_flight:
            self.send_response(503)
            self.end_headers()
            return

        request_in_flight = True
        last_request_time = now

        try:
            count = 200
            if 'count=' in self.path:
                try:
                    count = int(self.path.split('count=')[1].split('&')[0])
                except:
                    pass

            fetch_depth += 1
            events = fetch_history(count, fetch_depth)
            for msg in reversed(events):
                emit(msg, topic, marker='history')

            with output_lock:
                try:
                    print(json.dumps({'__sentinel': 'HISTORY_BATCH_COMPLETE', 'count': len(events)}), flush=True)
                except BrokenPipeError:
                    sys.exit(0)

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'loaded': len(events)}).encode())
        except Exception as e:
            sys.stderr.write(f'Request handler error: {e}\n')
            sys.stderr.flush()
            self.send_response(500)
            self.end_headers()
        finally:
            request_in_flight = False

    def log_message(self, format, *args):
        pass  # Suppress HTTP logs

def run_http_server():
    server = HTTPServer(('127.0.0.1', 8080), HistoryHandler)
    server.serve_forever()

def load_initial_history(bootstrap_arg, topic_arg, history_count, since_minutes=None):
    global bootstrap, topic
    bootstrap = bootstrap_arg
    topic = topic_arg

    since_time = None
    if since_minutes is not None:
        since_time = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

    try:
        hist = KafkaConsumer(
            bootstrap_servers=bootstrap,
            enable_auto_commit=False,
            key_deserializer=lambda m: int.from_bytes(m, byteorder='big', signed=True) if m and len(m) == 8 else None,
            value_deserializer=lambda m: json.loads(m.decode('utf-8', errors='replace')),
            consumer_timeout_ms=2000,
        )

        partitions = hist.partitions_for_topic(topic)
        if partitions:
            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            hist.assign(tps)
            end_offsets = hist.end_offsets(tps)

            if since_time is not None:
                target_ms = int(since_time.timestamp() * 1000)
                offsets = hist.offsets_for_times({tp: target_ms for tp in tps})
                for tp in tps:
                    off = offsets.get(tp)
                    if off is not None and off.offset is not None:
                        hist.seek(tp, off.offset)
                    else:
                        hist.seek(tp, end_offsets.get(tp, 0))
            else:
                per_partition = max(1, history_count // len(tps)) * 2
                for tp in tps:
                    start = max(0, end_offsets.get(tp, 0) - per_partition)
                    hist.seek(tp, start)

            collected = []
            while True:
                records = hist.poll(timeout_ms=2000)
                if not records:
                    break
                for tp_key, messages in records.items():
                    for msg in messages:
                        collected.append(msg)
                if all(hist.position(tp) >= end_offsets.get(tp, 0) for tp in tps):
                    break

            collected.sort(key=lambda m: (getattr(m, 'timestamp', 0), getattr(m, 'offset', 0)))
            if since_time is None:
                collected = collected[-history_count:]

            for msg in collected:
                emit(msg, topic)

        hist.close()
    except Exception as e:
        sys.stderr.write(f'Initial history load error: {e}\n')
        sys.stderr.flush()

    try:
        print(json.dumps({'__sentinel': 'HISTORY_COMPLETE'}), flush=True)
    except BrokenPipeError:
        sys.exit(0)

    # Start live tail
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=f'visualizer-bridge-{int(time.time())}',
        key_deserializer=lambda m: int.from_bytes(m, byteorder='big', signed=True) if m and len(m) == 8 else None,
        value_deserializer=lambda m: json.loads(m.decode('utf-8', errors='replace')),
        consumer_timeout_ms=1000,
    )

    while True:
        records = consumer.poll(timeout_ms=500)
        for tp_key, messages in records.items():
            for msg in messages:
                emit(msg, topic)

# Heartbeat to detect when visualizer exits
def heartbeat():
    while True:
        time.sleep(2)
        try:
            sys.stdout.write('\\n')
            sys.stdout.flush()
        except (BrokenPipeError, IOError):
            sys.exit(0)

# Poll consumer group lag and send to visualizer
def poll_lag():
    import re
    from kafka import KafkaAdminClient
    from kafka.structs import TopicPartition

    UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    time.sleep(5)  # Wait for bootstrap/topic to be set

    while True:
        try:
            if bootstrap is None or topic is None:
                time.sleep(5)
                continue

            admin = KafkaAdminClient(bootstrap_servers=bootstrap)
            tmp = KafkaConsumer(bootstrap_servers=bootstrap, consumer_timeout_ms=1000)
            partitions = tmp.partitions_for_topic(topic)
            if not partitions:
                tmp.close()
                admin.close()
                time.sleep(5)
                continue

            tps = [TopicPartition(topic, p) for p in partitions]
            tmp.assign(tps)
            end_offsets = tmp.end_offsets(tps)
            tmp.close()

            groups = admin.list_consumer_groups()
            group_ids = [g[0] for g in groups]
            lags = {}

            for gid in group_ids:
                if 'visualizer' in gid.lower() or gid.startswith('console-consumer-'):
                    continue
                if UUID_PATTERN.match(gid):
                    continue
                try:
                    offsets = admin.list_consumer_group_offsets(gid)
                except:
                    continue
                topic_offsets = {tp: off for tp, off in offsets.items() if tp.topic == topic}
                if not topic_offsets:
                    continue
                total_lag = 0
                for tp, off_meta in topic_offsets.items():
                    end = end_offsets.get(tp, 0)
                    committed = off_meta.offset if off_meta.offset >= 0 else 0
                    total_lag += max(0, end - committed)
                lags[gid] = total_lag

            admin.close()

            try:
                print(json.dumps({'__lag_data': lags}), flush=True)
            except BrokenPipeError:
                sys.exit(0)

        except Exception as e:
            sys.stderr.write(f'Lag poll error: {e}\\n')
            sys.stderr.flush()

        time.sleep(3)

# Start background threads
http_thread = threading.Thread(target=run_http_server, daemon=True)
http_thread.start()

heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
heartbeat_thread.start()

lag_thread = threading.Thread(target=poll_lag, daemon=True)
lag_thread.start()

# Run main Kafka consumer (blocks)
load_initial_history($BRIDGE_ARGS)
" | bash "$VISUALIZER_SCRIPT" --stdin --bootstrap "$KAFKA_BOOTSTRAP" --topic "$TOPIC" --http-bridge "http://localhost:$HTTP_PORT" --window-size "$WINDOW_SIZE" ${FILTER:+--filter "$FILTER"}

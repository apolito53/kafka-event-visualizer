#!/usr/bin/env bash
# Kafka Event Bus Visualizer
# =========================
#
# Purpose
# -------
# Local terminal UI for exploring Kafka event traffic during development.
# The current shape of the tool is an event explorer with:
# - a scrollable event list
# - a payload inspector for the selected event
# - a scrollable consumer-group lag panel
# - bounded history browsing for older Kafka records
#
# What It Shows
# -------------
# - Event list: key, date, time, and event type
# - Payload panel: decoded JSON payload for the selected event
# - Consumer Group Lag: current lag by consumer group for the selected topic
#
# Important Semantics
# -------------------
# - The UI no longer shows a "Source" column because Kafka does not inherently
#   tell us which service emitted a specific record unless producers add that
#   metadata themselves.
# - FOLLOW means the list is tailing the newest events.
# - PAUSED means the visible list is frozen for browsing. New live events are
#   still consumed, but they are buffered and shown as `+N queued` until you
#   return to follow mode.
# - `Load older` uses a separate temporary consumer to fetch an older bounded
#   slice of topic history without disturbing the live tail consumer.
#
# Controls
# --------
# - `j` / `k` or Up / Down: move within pane
# - `PgUp` / `PgDn`: page up / down
# - `g` / `G`: jump to top / bottom
# - `Tab` / `Shift+Tab`: switch pane (forward/backward)
# - `f`: return to FOLLOW mode / resume tailing newest events
# - `b`: load an older history slice while browsing the event pane
# - `:`: filter by event type pattern (case-insensitive, supports wildcards)
# - `q` or `Ctrl+C`: exit
#
# History Browser Model
# ---------------------
# - The visible event list is a bounded in-memory window.
# - Live events append at the front while following.
# - When paused, the visible list stays fixed and live arrivals go into a
#   pending buffer.
# - Press `b` to fetch an older bounded slice from Kafka and append it to the
#   back of the visible history window.
# - Returning to follow mode flushes buffered live events back into the list.
#
# Common Modes
# ------------
# 1. Topic picker / generic explorer
#    ./kafka-event-visualizer.sh
#
# 2. Demo mode
#    ./kafka-event-visualizer.sh --demo
#    ./kafka-event-visualizer.sh --demo --demo-rate 25
#
# 3. Replay from a point in time
#    ./kafka-event-visualizer.sh --topic orders --since "2026-04-17 09:30:00"
#    ./kafka-event-visualizer.sh --topic orders --since-minutes 30
#
# 4. Custom broker/topic
#    ./kafka-event-visualizer.sh --bootstrap localhost:9092 --topic orders
#
# Useful Flags
# ------------
# - `--bootstrap HOST:PORT`
#   Kafka bootstrap server list. Useful with SSH tunnels.
# - `--topic TOPIC`
#   Topic to inspect. If omitted, the script opens a topic picker.
# - `--demo`
#   Run without Kafka and generate fake traffic.
# - `--demo-rate N`
#   Average number of demo events per second.
# - `--since ISO_TIMESTAMP`
#   Replay from a specific point in time.
# - `--since-minutes N`
#   Replay from N minutes ago.
# - `--window-size N`
#   Maximum number of loaded history events kept in memory.
# - `--history-batch-size N`
#   Number of older events to fetch each time `b` is pressed.
# - `--show-ephemeral-groups`
#   Include UUID-like consumer groups in the lag pane.
# - `--filter PATTERN`
#   Filter events by type pattern. Supports wildcards (e.g., order_* matches order_created, order_shipped, etc.).
#   Can be toggled interactively with `:` key.
#
# Notes
# -----
# - This is a local development tool, not production code.
# - The script is self-contained: bash preamble + embedded Python via heredoc.
# - It auto-installs `rich` and `kafka-python` if missing.
set -euo pipefail

if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 is required but not found."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
PYTHON_BIN="python3"
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

ensure_venv() {
    if [ -x "${VENV_DIR}/bin/python" ]; then
        return 0
    fi

    echo "Creating local virtual environment at ${VENV_DIR}..."
    if python3 -m venv "${VENV_DIR}" &>/dev/null; then
        return 0
    fi

    echo "ERROR: failed to create a Python virtual environment at ${VENV_DIR}."
    if [ -r /etc/os-release ]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        if [ "${ID:-}" = "ubuntu" ] || [ "${ID_LIKE:-}" = "debian" ] || [[ "${ID_LIKE:-}" == *debian* ]]; then
            echo "On ${PRETTY_NAME:-this system}, install venv support with:"
            echo "  sudo apt update"
            echo "  sudo apt install python${PYTHON_VERSION}-venv"
            exit 1
        fi
    fi

    echo "Install the Python 3 venv support package for your distro and re-run this script."
    exit 1
}

ensure_pip() {
    if "${PYTHON_BIN}" -m pip --version &>/dev/null; then
        return 0
    fi

    echo "pip is missing in the selected Python environment; attempting to bootstrap it..."
    if "${PYTHON_BIN}" -m ensurepip --upgrade &>/dev/null; then
        return 0
    fi

    echo "ERROR: pip could not be initialized for ${PYTHON_BIN}."
    exit 1
}

ensure_venv
PYTHON_BIN="${VENV_DIR}/bin/python"

MISSING=()
"${PYTHON_BIN}" -c "import rich" 2>/dev/null || MISSING+=("rich")
"${PYTHON_BIN}" -c "import kafka" 2>/dev/null || MISSING+=("kafka-python")

if [ ${#MISSING[@]} -gt 0 ]; then
    ensure_pip
    echo "Installing missing Python packages: ${MISSING[*]}"
    "${PYTHON_BIN}" -m pip install "${MISSING[@]}"
fi

exec "${PYTHON_BIN}" - "$@" << 'PYTHON_EOF'
import argparse
import json
import os
import random
import re
import select
import sys
import termios
import threading
import time
import tty
from collections import deque
from datetime import datetime, timedelta, timezone

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

SERVICE_STYLES = {}
PRODUCER_ORDER = []
CONSUMER_ORDER = []
EVENT_TO_PRODUCER = {}

PIPELINE_WIDTH = 20

DEMO_EVENT_POOL = [
    "ORDER_CREATED",
    "ORDER_UPDATED",
    "ORDER_FULFILLED",
    "PAYMENT_AUTHORIZED",
    "PAYMENT_CAPTURED",
    "INVENTORY_RESERVED",
    "INVENTORY_RELEASED",
    "USER_REGISTERED",
    "NOTIFICATION_SENT",
    "SHIPMENT_DISPATCHED",
]

DEMO_WEIGHTS = [5, 4, 2, 3, 2, 3, 1, 2, 2, 1]

DYNAMIC_COLORS = [
    "bright_yellow", "bright_cyan", "bright_magenta", "bright_green",
    "bright_blue", "bright_red", "bright_white", "cyan", "green",
    "yellow", "magenta", "blue", "red",
]

_dynamic_color_idx = 0
UUID_GROUP_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
TYPE_FIELD_CANDIDATES = (
    "type", "eventType", "event_type", "name", "eventName", "event_name",
)
TIME_FIELD_CANDIDATES = (
    "timestamp", "time", "createdAt", "created_at", "eventTime", "event_time",
    "occurredAt", "occurred_at",
)
RECENT_WINDOW_SENTINEL = "__recent_window__"


def classify_producer(event_type):
    return EVENT_TO_PRODUCER.get(event_type, None)


def classify_producer_dynamic(event_type, service_styles, producer_order,
                              producer_counts, producer_pipes):
    global _dynamic_color_idx
    prefix = event_type.split("_")[0]
    svc_name = prefix.lower()
    if svc_name not in service_styles:
        color = DYNAMIC_COLORS[_dynamic_color_idx % len(DYNAMIC_COLORS)]
        _dynamic_color_idx += 1
        icon = prefix[:2].upper()
        service_styles[svc_name] = {"color": color, "icon": icon, "label": prefix.title()}
        producer_order.append(svc_name)
        producer_counts[svc_name] = 0
        producer_pipes[svc_name] = deque([None] * PIPELINE_WIDTH, maxlen=PIPELINE_WIDTH)
    return svc_name


def get_consumers(event_type):
    return []


def svc_style(service):
    return SERVICE_STYLES.get(service, {"color": "dim", "icon": "??", "label": service})


def build_demo_payload(event_type, timestamp, key):
    producer = EVENT_TO_PRODUCER.get(event_type)
    payload = {
        "type": event_type,
        "timestamp": timestamp,
        "key": key,
    }
    if producer:
        payload["demoProducer"] = producer
    return payload


def format_payload(payload, max_width=60, max_lines=14):
    if payload is None:
        return ["No payload captured."]

    try:
        rendered = json.dumps(payload, indent=2, sort_keys=True, default=str)
    except Exception:
        rendered = str(payload)

    lines = rendered.splitlines() or ["{}"]
    clipped = []
    for raw_line in lines:
        if len(raw_line) <= max_width:
            clipped.append(raw_line)
            continue
        clipped.append(raw_line[: max_width - 3] + "...")

    if len(clipped) > max_lines:
        visible = clipped[: max_lines - 1]
        visible.append(f"... ({len(clipped) - max_lines + 1} more lines)")
        return visible

    return clipped


def parse_since_value(value):
    candidates = [
        value,
        value.replace(" ", "T"),
    ]
    parsed = None
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
            break
        except ValueError:
            continue
    if parsed is None:
        raise ValueError(
            "Invalid --since value. Use ISO format like '2026-04-17T09:30:00-04:00' "
            "or '2026-04-17 09:30:00'."
        )
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def detect_event_type(payload, configured_field=None):
    if not isinstance(payload, dict):
        return "UNKNOWN"

    field_names = []
    if configured_field:
        field_names.append(configured_field)
    field_names.extend(name for name in TYPE_FIELD_CANDIDATES if name != configured_field)

    for field_name in field_names:
        value = payload.get(field_name)
        if value not in (None, ""):
            return str(value)

    return "UNKNOWN"


def detect_event_time(payload, configured_field=None):
    now_iso = datetime.now().isoformat()
    if not isinstance(payload, dict):
        return now_iso

    field_names = []
    if configured_field:
        field_names.append(configured_field)
    field_names.extend(name for name in TIME_FIELD_CANDIDATES if name != configured_field)

    for field_name in field_names:
        value = payload.get(field_name)
        if value not in (None, ""):
            return value

    return now_iso


def render_topic_picker(console, topics, selected_index, offset):
    visible_rows = max((console.height or 24) - 8, 8)
    items = topics[offset:offset + visible_rows]

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1), expand=True)
    table.add_column("", width=2, no_wrap=True)
    table.add_column("Topic", no_wrap=False)

    for idx, topic in enumerate(items):
        absolute_idx = offset + idx
        is_selected = absolute_idx == selected_index
        row_style = "bold black on bright_white" if is_selected else ""
        marker = Text(">" if is_selected else "", style="bold black" if is_selected else "dim")
        topic_text = Text(topic, style="bold black" if is_selected else "")
        table.add_row(marker, topic_text, style=row_style)

    title = Text()
    title.append("  KAFKA TOPIC PICKER  ", style="bold white on blue")
    title.append("  Select one topic to explore  ", style="dim")
    title.append(f"  {len(topics)} topics  ", style="bold")

    return Panel(
        table,
        title=title,
        subtitle="[dim]Scroll: j/k or arrows  Select: Enter  Exit: q or Ctrl+C[/dim]",
        border_style="cyan",
    )


def pick_topic(bootstrap_servers):
    from kafka import KafkaConsumer

    console = Console()

    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, consumer_timeout_ms=1000)
        try:
            topics = sorted(topic for topic in consumer.topics() if not topic.startswith("__"))
        finally:
            consumer.close()
    except Exception as e:
        console.print(f"[bold red]Failed to fetch topic metadata:[/bold red] {e}")
        return None

    if not topics:
        console.print("[bold red]No observable topics found.[/bold red]")
        return None

    try:
        tty_handle = open("/dev/tty", "rb", buffering=0)
    except OSError as e:
        console.print(f"[bold red]Failed to open terminal input:[/bold red] {e}")
        return None

    fd = tty_handle.fileno()
    old_settings = termios.tcgetattr(fd)
    selected_index = 0
    offset = 0

    try:
        tty.setcbreak(fd)
        with Live(render_topic_picker(console, topics, selected_index, offset),
                  console=console, refresh_per_second=10, screen=True) as live:
            while True:
                visible_rows = max((console.height or 24) - 8, 8)
                if selected_index < offset:
                    offset = selected_index
                elif selected_index >= offset + visible_rows:
                    offset = selected_index - visible_rows + 1

                live.update(render_topic_picker(console, topics, selected_index, offset))

                ready, _, _ = select.select([tty_handle], [], [], 0.05)
                if not ready:
                    continue

                chars = os.read(fd, 1)
                if not chars:
                    continue
                if chars in (b"q", b"Q", b"\x03"):
                    return None
                if chars in (b"\r", b"\n"):
                    return topics[selected_index]
                if chars == b"\x1b":
                    seq = chars
                    for _ in range(4):
                        ready, _, _ = select.select([tty_handle], [], [], 0.02)
                        if not ready:
                            break
                        seq += os.read(fd, 1)
                    chars = seq

                if chars in (b"j", b"\x1b[B"):
                    selected_index = min(selected_index + 1, len(topics) - 1)
                elif chars in (b"k", b"\x1b[A"):
                    selected_index = max(selected_index - 1, 0)
                elif chars == b"\x1b[5~":
                    selected_index = max(selected_index - visible_rows, 0)
                elif chars == b"\x1b[6~":
                    selected_index = min(selected_index + visible_rows, len(topics) - 1)
                elif chars == b"g":
                    selected_index = 0
                elif chars == b"G":
                    selected_index = len(topics) - 1
    finally:
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except termios.error:
            pass
        tty_handle.close()


def render_time_picker(console, options, selected_index, custom_mode=False, custom_value="", error_message=None):
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1), expand=True)
    table.add_column("", width=2, no_wrap=True)
    table.add_column("Start Time", no_wrap=False)

    for idx, (label, _) in enumerate(options):
        is_selected = idx == selected_index and not custom_mode
        row_style = "bold black on bright_white" if is_selected else ""
        marker = Text(">" if is_selected else "", style="bold black" if is_selected else "dim")
        table.add_row(marker, Text(label, style="bold black" if is_selected else ""), style=row_style)

    body = Group(
        table,
        Text(""),
        Text("Custom timestamp", style="bold"),
        Text(custom_value or "YYYY-MM-DD HH:MM[:SS] or ISO-8601", style="bold black on bright_white" if custom_mode else "dim"),
        Text(error_message, style="bold red") if error_message else Text(""),
    )

    title = Text()
    title.append("  REPLAY START PICKER  ", style="bold white on blue")
    title.append("  Choose where to begin reading the topic  ", style="dim")

    subtitle = "[dim]Scroll: j/k or arrows  Select: Enter  Custom: c  Backspace edits  Esc leaves custom  Exit: q or Ctrl+C[/dim]"
    return Panel(body, title=title, subtitle=subtitle, border_style="magenta")


def pick_start_time():
    console = Console()
    options = [
        ("Recent 200 events + tail", RECENT_WINDOW_SENTINEL),
        ("Tail from now", None),
        ("Replay from 5 minutes ago", timedelta(minutes=5)),
        ("Replay from 15 minutes ago", timedelta(minutes=15)),
        ("Replay from 1 hour ago", timedelta(hours=1)),
        ("Replay from 6 hours ago", timedelta(hours=6)),
        ("Replay from 24 hours ago", timedelta(hours=24)),
    ]

    try:
        tty_handle = open("/dev/tty", "rb", buffering=0)
    except OSError as e:
        console.print(f"[bold red]Failed to open terminal input:[/bold red] {e}")
        return None, False

    fd = tty_handle.fileno()
    old_settings = termios.tcgetattr(fd)
    selected_index = 0
    custom_mode = False
    custom_value = ""
    error_message = None

    try:
        tty.setcbreak(fd)
        with Live(render_time_picker(console, options, selected_index, custom_mode, custom_value, error_message),
                  console=console, refresh_per_second=10, screen=True) as live:
            while True:
                live.update(render_time_picker(console, options, selected_index, custom_mode, custom_value, error_message))
                ready, _, _ = select.select([tty_handle], [], [], 0.05)
                if not ready:
                    continue

                chars = os.read(fd, 1)
                if not chars:
                    continue
                if chars in (b"q", b"Q", b"\x03"):
                    return None, True
                if chars == b"\x1b":
                    seq = chars
                    for _ in range(4):
                        ready, _, _ = select.select([tty_handle], [], [], 0.02)
                        if not ready:
                            break
                        seq += os.read(fd, 1)
                    chars = seq
                    if custom_mode and chars == b"\x1b":
                        custom_mode = False
                        error_message = None
                        continue

                if custom_mode:
                    if chars in (b"\r", b"\n"):
                        try:
                            return parse_since_value(custom_value.strip()), False
                        except ValueError as e:
                            error_message = str(e)
                    elif chars in (b"\x7f", b"\b"):
                        custom_value = custom_value[:-1]
                        error_message = None
                    elif len(chars) == 1 and chars.isprintable():
                        custom_value += chars.decode("utf-8", errors="ignore")
                        error_message = None
                    continue

                if chars in (b"\r", b"\n"):
                    delta = options[selected_index][1]
                    if delta == RECENT_WINDOW_SENTINEL:
                        return RECENT_WINDOW_SENTINEL, False
                    if delta is None:
                        return None, False
                    return datetime.now().astimezone() - delta, False
                if chars in (b"j", b"\x1b[B"):
                    selected_index = min(selected_index + 1, len(options) - 1)
                elif chars in (b"k", b"\x1b[A"):
                    selected_index = max(selected_index - 1, 0)
                elif chars == b"\x1b[5~":
                    selected_index = max(selected_index - 5, 0)
                elif chars == b"\x1b[6~":
                    selected_index = min(selected_index + 5, len(options) - 1)
                elif chars in (b"g",):
                    selected_index = 0
                elif chars in (b"G",):
                    selected_index = len(options) - 1
                elif chars in (b"c", b"C"):
                    custom_mode = True
                    error_message = None
    finally:
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except termios.error:
            pass
        tty_handle.close()


class EventBusVisualizer:
    def __init__(self, bootstrap_servers="localhost:9092", topic=None,
                 demo=False, dynamic=True, show_ephemeral_groups=False, since_time=None,
                 type_field=None, time_field=None, demo_rate=3.0,
                 history_window_size=2000, history_batch_size=200):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.demo = demo
        self.dynamic = dynamic
        self.show_ephemeral_groups = show_ephemeral_groups
        self.since_time = since_time
        self.recent_window_mode = (since_time == RECENT_WINDOW_SENTINEL)
        if self.recent_window_mode:
            self.since_time = None
        self.type_field = type_field
        self.time_field = time_field
        self.demo_rate = max(float(demo_rate), 0.1)
        self.history_window_size = max(int(history_window_size), 100)
        self.history_batch_size = max(int(history_batch_size), 50)
        self.running = True

        self.event_history = deque()
        self.pending_events = deque(maxlen=5000)
        self.loaded_event_ids = set()
        self.total_count = 0
        self.unmapped_count = 0
        self.consumer_lag = None
        self.group_lags = {}
        self.status_message = None
        self.status_level = "dim"
        self._own_group_id = f"kafka-event-visualizer-{int(time.time())}"
        self._recent_timestamps = deque(maxlen=50)
        self.selected_index = 0
        self.viewport_start = 0
        self.viewport_rows = 10
        self.queue_selected_index = 0
        self.queue_viewport_start = 0
        self.queue_viewport_rows = 8
        self.follow_mode = True
        self.focus_pane = "history"
        self.history_loading = False
        self.history_exhausted = False
        self._demo_offset = 0
        self._tty = None

        if dynamic:
            self.service_styles = dict(SERVICE_STYLES)
            self.producer_order = []
            self.producer_counts = {}
            self.producer_pipes = {}
            self.consumer_order = []
            self.consumer_counts = {}
            self.consumer_pipes = {}
        else:
            self.service_styles = SERVICE_STYLES
            self.producer_order = list(PRODUCER_ORDER)
            self.producer_counts = {s: 0 for s in PRODUCER_ORDER}
            self.producer_pipes = {s: deque([None] * PIPELINE_WIDTH, maxlen=PIPELINE_WIDTH) for s in PRODUCER_ORDER}
            self.consumer_order = list(CONSUMER_ORDER)
            self.consumer_counts = {s: 0 for s in CONSUMER_ORDER}
            self.consumer_pipes = {s: deque([None] * PIPELINE_WIDTH, maxlen=PIPELINE_WIDTH) for s in CONSUMER_ORDER}

        self.lock = threading.Lock()

    def _set_status(self, message, level="dim"):
        with self.lock:
            self.status_message = message
            self.status_level = level

    def _clear_status(self):
        with self.lock:
            self.status_message = None
            self.status_level = "dim"

    def _update_lag(self, consumer):
        try:
            partitions = consumer.assignment()
            if not partitions:
                return
            end_offsets = consumer.end_offsets(partitions)
            total_lag = 0
            for tp in partitions:
                pos = consumer.position(tp)
                end = end_offsets.get(tp, pos)
                total_lag += max(0, end - pos)
            with self.lock:
                self.consumer_lag = total_lag
        except Exception:
            pass

    def _poll_group_lags(self):
        from kafka import KafkaAdminClient, KafkaConsumer as _KC
        from kafka.structs import TopicPartition
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        except Exception:
            return

        tmp = _KC(bootstrap_servers=self.bootstrap_servers, consumer_timeout_ms=1000)
        try:
            partitions = tmp.partitions_for_topic(self.topic)
            if not partitions:
                return
            tps = [TopicPartition(self.topic, p) for p in partitions]
            tmp.assign(tps)
            end_offsets = tmp.end_offsets(tps)
        finally:
            tmp.close()

        while self.running:
            try:
                groups = admin.list_consumer_groups()
                group_ids = [g[0] for g in groups]
                lags = {}
                for gid in group_ids:
                    if gid == self._own_group_id \
                            or gid.startswith("kafka-event-visualizer-"):
                        continue
                    if not self.show_ephemeral_groups and UUID_GROUP_RE.match(gid):
                        continue
                    try:
                        offsets = admin.list_consumer_group_offsets(gid)
                    except Exception:
                        continue
                    topic_offsets = {tp: off for tp, off in offsets.items()
                                    if tp.topic == self.topic}
                    if not topic_offsets:
                        continue
                    total_lag = 0
                    for tp, off_meta in topic_offsets.items():
                        end = end_offsets.get(tp, 0)
                        committed = off_meta.offset if off_meta.offset >= 0 else 0
                        total_lag += max(0, end - committed)
                    lags[gid] = total_lag

                try:
                    tmp2 = _KC(bootstrap_servers=self.bootstrap_servers, consumer_timeout_ms=1000)
                    tmp2.assign(tps)
                    end_offsets = tmp2.end_offsets(tps)
                    tmp2.close()
                except Exception:
                    pass

                with self.lock:
                    self.group_lags = lags
            except Exception:
                pass
            time.sleep(5)

    def _decode_message(self, msg):
        val = msg.value
        if isinstance(val, dict):
            if self.dynamic:
                event_type = detect_event_type(val, self.type_field)
                ts = detect_event_time(val, self.time_field)
            else:
                raw_type = val.get(self.type_field or "type")
                event_type = str(raw_type) if raw_type not in (None, "") else "UNKNOWN"
                ts = val.get(self.time_field or "timestamp", datetime.now().isoformat())
        else:
            event_type = "UNKNOWN"
            ts = datetime.now().isoformat()
        return {
            "event_type": event_type,
            "timestamp": ts,
            "key": msg.key,
            "payload": val,
            "topic": getattr(msg, "topic", self.topic),
            "partition": getattr(msg, "partition", None),
            "offset": getattr(msg, "offset", None),
            "broker_timestamp_ms": getattr(msg, "timestamp", None),
        }

    def _load_recent_history(self, recent_count=200):
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition

        self._set_status(f"Loading recent {recent_count} events...", level="bold cyan")

        history_consumer = None
        try:
            history_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                enable_auto_commit=False,
                key_deserializer=lambda m: int.from_bytes(m, byteorder="big", signed=True) if m and len(m) == 8 else None,
                value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
                consumer_timeout_ms=1000,
            )

            partitions = history_consumer.partitions_for_topic(self.topic)
            if not partitions:
                return

            tps = [TopicPartition(self.topic, p) for p in sorted(partitions)]
            history_consumer.assign(tps)
            end_offsets = history_consumer.end_offsets(tps)
            if not end_offsets:
                return

            step = max(50, ((recent_count + len(tps) - 1) // len(tps)) * 2)
            collected = []

            while self.running:
                for tp in tps:
                    history_consumer.seek(tp, max(0, end_offsets.get(tp, 0) - step))

                pass_records = []
                while self.running:
                    records = history_consumer.poll(timeout_ms=250)
                    for tp, messages in records.items():
                        limit = end_offsets.get(tp, 0)
                        for msg in messages:
                            if msg.offset < limit:
                                pass_records.append(msg)

                    if all(history_consumer.position(tp) >= end_offsets.get(tp, 0) for tp in tps):
                        break

                collected = pass_records
                reached_start = all(max(0, end_offsets.get(tp, 0) - step) == 0 for tp in tps)
                if len(collected) >= recent_count or reached_start:
                    break
                step *= 2

            decorated = []
            for idx, msg in enumerate(collected):
                msg_ts = getattr(msg, "timestamp", None)
                sort_ts = msg_ts if isinstance(msg_ts, int) and msg_ts >= 0 else -1
                decorated.append((sort_ts, idx, msg))

            decorated.sort(key=lambda item: (item[0], item[1]), reverse=True)
            with self.lock:
                for _, _, msg in decorated[:recent_count]:
                    decoded = self._decode_message(msg)
                    producer = self._classify_event_locked(decoded["event_type"])
                    item = self._build_event_item(
                        decoded["event_type"],
                        decoded["timestamp"],
                        decoded["key"],
                        decoded["payload"],
                        producer=producer,
                        topic=decoded["topic"],
                        partition=decoded["partition"],
                        offset=decoded["offset"],
                        broker_timestamp_ms=decoded["broker_timestamp_ms"],
                    )
                    self._append_oldest_event_locked(item)
        finally:
            if history_consumer is not None:
                history_consumer.close()
            self._clear_status()

    def _oldest_loaded_offsets_locked(self):
        offsets = {}
        for item in self.event_history:
            topic = item.get("topic")
            partition = item.get("partition")
            offset = item.get("offset")
            if topic is None or partition is None or offset is None:
                continue
            key = (topic, partition)
            if key not in offsets or offset < offsets[key]:
                offsets[key] = offset
        return offsets

    def _load_older_history(self):
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition

        if self.demo:
            self._set_status("Older-history loading is unavailable in demo mode", level="bold yellow")
            with self.lock:
                self.history_loading = False
            return

        history_consumer = None
        loaded_count = 0
        try:
            history_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                enable_auto_commit=False,
                key_deserializer=lambda m: int.from_bytes(m, byteorder="big", signed=True) if m and len(m) == 8 else None,
                value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
                consumer_timeout_ms=1000,
            )

            partitions = history_consumer.partitions_for_topic(self.topic)
            if not partitions:
                self._set_status("No partitions available for this topic", level="bold red")
                return

            tps = [TopicPartition(self.topic, p) for p in sorted(partitions)]
            history_consumer.assign(tps)
            end_offsets = history_consumer.end_offsets(tps)

            with self.lock:
                oldest_offsets = self._oldest_loaded_offsets_locked()

            limit_offsets = {
                tp: oldest_offsets.get((tp.topic, tp.partition), end_offsets.get(tp, 0))
                for tp in tps
            }
            if all(limit_offsets.get(tp, 0) <= 0 for tp in tps):
                with self.lock:
                    self.history_exhausted = True
                self._set_status("Reached the beginning of available topic history", level="bold yellow")
                return

            step = max(50, ((self.history_batch_size + len(tps) - 1) // len(tps)) * 2)
            collected = []
            reached_start = False

            while self.running:
                for tp in tps:
                    history_consumer.seek(tp, max(0, limit_offsets.get(tp, 0) - step))

                pass_records = []
                while self.running:
                    records = history_consumer.poll(timeout_ms=250)
                    for tp, messages in records.items():
                        limit = limit_offsets.get(tp, 0)
                        for msg in messages:
                            if msg.offset < limit:
                                pass_records.append(msg)

                    if all(history_consumer.position(tp) >= limit_offsets.get(tp, 0) for tp in tps):
                        break

                collected = pass_records
                reached_start = all(max(0, limit_offsets.get(tp, 0) - step) == 0 for tp in tps)
                if len(collected) >= self.history_batch_size or reached_start:
                    break
                step *= 2

            decorated = []
            for idx, msg in enumerate(collected):
                msg_ts = getattr(msg, "timestamp", None)
                sort_ts = msg_ts if isinstance(msg_ts, int) and msg_ts >= 0 else -1
                decorated.append((sort_ts, getattr(msg, "partition", -1), getattr(msg, "offset", -1), idx, msg))
            decorated.sort(reverse=True)

            with self.lock:
                for _, _, _, _, msg in decorated[:self.history_batch_size]:
                    decoded = self._decode_message(msg)
                    producer = self._classify_event_locked(decoded["event_type"])
                    item = self._build_event_item(
                        decoded["event_type"],
                        decoded["timestamp"],
                        decoded["key"],
                        decoded["payload"],
                        producer=producer,
                        topic=decoded["topic"],
                        partition=decoded["partition"],
                        offset=decoded["offset"],
                        broker_timestamp_ms=decoded["broker_timestamp_ms"],
                    )
                    if self._append_oldest_event_locked(item):
                        loaded_count += 1
                if loaded_count:
                    self.history_exhausted = False
                elif reached_start:
                    self.history_exhausted = True

            if loaded_count:
                self._set_status(f"Loaded {loaded_count} older events", level="bold cyan")
            elif reached_start:
                self._set_status("Reached the beginning of available topic history", level="bold yellow")
            else:
                self._set_status("No additional older events found", level="bold yellow")
        except Exception as e:
            self._set_status(f"Older-history load error: {e}", level="bold red")
        finally:
            if history_consumer is not None:
                history_consumer.close()
            with self.lock:
                self.history_loading = False

    def _request_older_history(self):
        with self.lock:
            if self.history_loading:
                return
            if self.follow_mode:
                self.status_message = "Pause browsing before loading older history"
                self.status_level = "bold yellow"
                return
            self.history_loading = True
            self.status_message = f"Loading older {self.history_batch_size} events..."
            self.status_level = "bold cyan"
        threading.Thread(target=self._load_older_history, daemon=True).start()

    def _consume_kafka(self):
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest" if self.since_time else "latest",
                enable_auto_commit=True,
                group_id=self._own_group_id,
                key_deserializer=lambda m: int.from_bytes(m, byteorder="big", signed=True) if m and len(m) == 8 else None,
                value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
                consumer_timeout_ms=1000,
            )
        except Exception as e:
            self._set_status(f"Connection error: {e}", level="bold red")
            return

        if self.recent_window_mode:
            try:
                self._load_recent_history()
            except Exception as e:
                self._set_status(f"Recent history load error: {e}", level="bold red")
            consumer.subscribe([self.topic])
        elif self.since_time is not None:
            try:
                partitions = consumer.partitions_for_topic(self.topic)
                if partitions:
                    tps = [TopicPartition(self.topic, p) for p in sorted(partitions)]
                    consumer.assign(tps)
                    target_ms = int(self.since_time.astimezone(timezone.utc).timestamp() * 1000)
                    offsets = consumer.offsets_for_times({tp: target_ms for tp in tps})
                    end_offsets = consumer.end_offsets(tps)
                    for tp in tps:
                        off = offsets.get(tp)
                        if off is not None and off.offset is not None:
                            consumer.seek(tp, off.offset)
                        else:
                            consumer.seek(tp, end_offsets.get(tp, 0))
                else:
                    consumer.subscribe([self.topic])
            except Exception as e:
                self._set_status(f"Seek error: {e}", level="bold red")
        else:
            consumer.subscribe([self.topic])

        lag_check = 0
        while self.running:
            try:
                records = consumer.poll(timeout_ms=500)
                for tp, messages in records.items():
                    for msg in messages:
                        decoded = self._decode_message(msg)
                        self._ingest_event(
                            decoded["event_type"],
                            decoded["timestamp"],
                            decoded["key"],
                            decoded["payload"],
                            topic=decoded["topic"],
                            partition=decoded["partition"],
                            offset=decoded["offset"],
                            broker_timestamp_ms=decoded["broker_timestamp_ms"],
                        )
                lag_check += 1
                if lag_check % 4 == 0:
                    self._update_lag(consumer)
            except Exception:
                time.sleep(0.5)

        consumer.close()

    def _demo_producer(self):
        while self.running:
            delay = random.expovariate(self.demo_rate)
            time.sleep(delay)
            event_type = random.choices(DEMO_EVENT_POOL, weights=DEMO_WEIGHTS, k=1)[0]
            ts = datetime.now().isoformat()
            key = random.randint(1, 99999)
            self._demo_offset += 1
            self._ingest_event(
                event_type,
                ts,
                key,
                build_demo_payload(event_type, ts, key),
                topic=self.topic,
                partition=0,
                offset=self._demo_offset,
                broker_timestamp_ms=int(time.time() * 1000),
            )

    def _ensure_consumer(self, svc):
        if svc not in self.consumer_counts:
            self.consumer_order.append(svc)
            self.consumer_counts[svc] = 0
            self.consumer_pipes[svc] = deque([None] * PIPELINE_WIDTH, maxlen=PIPELINE_WIDTH)

    def _event_uid(self, item):
        if item is None:
            return None
        if "uid" in item:
            return item["uid"]
        topic = item.get("topic")
        partition = item.get("partition")
        offset = item.get("offset")
        if topic is not None and partition is not None and offset is not None:
            return (topic, partition, offset)
        return None

    def _history_sort_key(self, item):
        return (
            item.get("broker_timestamp_ms", -1),
            item.get("partition", -1),
            item.get("offset", -1),
        )

    def _trim_history_locked(self, drop_from="oldest"):
        while len(self.event_history) > self.history_window_size:
            if drop_from == "newest":
                removed = self.event_history.popleft()
                self.selected_index = max(0, self.selected_index - 1)
                self.viewport_start = max(0, self.viewport_start - 1)
            else:
                removed = self.event_history.pop()
            uid = self._event_uid(removed)
            if uid is not None:
                self.loaded_event_ids.discard(uid)

    def _append_newest_event_locked(self, item):
        uid = self._event_uid(item)
        if uid is not None and uid in self.loaded_event_ids:
            return False
        self.event_history.appendleft(item)
        if uid is not None:
            self.loaded_event_ids.add(uid)
        self._trim_history_locked(drop_from="oldest")
        return True

    def _append_oldest_event_locked(self, item):
        uid = self._event_uid(item)
        if uid is not None and uid in self.loaded_event_ids:
            return False
        self.event_history.append(item)
        if uid is not None:
            self.loaded_event_ids.add(uid)
        self._trim_history_locked(drop_from="newest")
        return True

    def _build_event_item(self, event_type, timestamp, key, payload=None, producer=None,
                          consumers=None, topic=None, partition=None, offset=None,
                          broker_timestamp_ms=None):
        if consumers is None:
            consumers = []

        if isinstance(timestamp, str):
            if len(timestamp) >= 10:
                display_date = timestamp[:10]
            else:
                display_date = datetime.now().strftime("%Y-%m-%d")
            if len(timestamp) > 19:
                display_ts = timestamp[11:19]
            else:
                display_ts = datetime.now().strftime("%H:%M:%S")
        else:
            display_date = datetime.now().strftime("%Y-%m-%d")
            display_ts = datetime.now().strftime("%H:%M:%S")

        uid = (topic, partition, offset) if topic is not None and partition is not None and offset is not None else None
        return {
            "uid": uid,
            "type": event_type,
            "producer": producer,
            "consumers": consumers,
            "date": display_date,
            "time": display_ts,
            "key": key,
            "payload": payload,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "broker_timestamp_ms": broker_timestamp_ms,
        }

    def _flush_pending_events_locked(self):
        if not self.pending_events:
            return
        pending = list(self.pending_events)
        self.pending_events.clear()
        for item in pending:
            self._append_newest_event_locked(item)
        self.status_message = None
        self.status_level = "dim"
        self.history_exhausted = False

    def _classify_event_locked(self, event_type):
        if self.dynamic:
            producer = classify_producer_dynamic(
                event_type, self.service_styles, self.producer_order,
                self.producer_counts, self.producer_pipes)
        else:
            producer = classify_producer(event_type)

        if producer and producer in self.service_styles:
            if producer not in self.producer_counts:
                self.producer_counts[producer] = 0
                self.producer_pipes[producer] = deque([None] * PIPELINE_WIDTH, maxlen=PIPELINE_WIDTH)
            self.producer_counts[producer] += 1
            cfg = self.service_styles[producer]
            self.producer_pipes[producer].append(cfg["icon"])
        else:
            self.unmapped_count += 1
            producer = None
        return producer

    def _ingest_event(self, event_type, timestamp, key, payload=None, topic=None,
                      partition=None, offset=None, broker_timestamp_ms=None):
        now = time.monotonic()
        with self.lock:
            self.total_count += 1
            self._recent_timestamps.append(now)
            producer = self._classify_event_locked(event_type)

            # Consumers are intentionally omitted from the current explorer-focused UI.
            # Keep the mapping and counter logic nearby so it can be re-enabled later.
            consumers = []
            # consumers = get_consumers(event_type)
            # for csvc in consumers:
            #     self._ensure_consumer(csvc)
            #     self.consumer_counts[csvc] += 1
            #     cfg = svc_style(csvc)
            #     self.consumer_pipes[csvc].append(cfg["icon"])

            item = self._build_event_item(
                event_type,
                timestamp,
                key,
                payload,
                producer=producer,
                consumers=consumers,
                topic=topic,
                partition=partition,
                offset=offset,
                broker_timestamp_ms=broker_timestamp_ms,
            )
            if self.follow_mode:
                self._append_newest_event_locked(item)
                self.status_message = None
                self.status_level = "dim"
                self.history_exhausted = False
                self.selected_index = 0
                self.viewport_start = 0
            else:
                self.pending_events.append(item)

    def _calc_eps(self):
        now = time.monotonic()
        with self.lock:
            while self._recent_timestamps and (now - self._recent_timestamps[0]) > 5.0:
                self._recent_timestamps.popleft()
            count = len(self._recent_timestamps)
        return count / 5.0 if count else 0.0

    def _tick_pipelines(self):
        with self.lock:
            for s in self.producer_order:
                pipe = self.producer_pipes.get(s)
                if pipe:
                    if len(pipe) >= PIPELINE_WIDTH:
                        pipe.popleft()
                    pipe.append(None)
            for s in self.consumer_order:
                pipe = self.consumer_pipes.get(s)
                if pipe:
                    if len(pipe) >= PIPELINE_WIDTH:
                        pipe.popleft()
                    pipe.append(None)

    def _move_selection(self, delta):
        with self.lock:
            if not self.event_history:
                self.selected_index = 0
                self.viewport_start = 0
                return
            max_index = len(self.event_history) - 1
            old_index = self.selected_index
            old_follow_mode = self.follow_mode
            self.selected_index = max(0, min(self.selected_index + delta, max_index))
            self.follow_mode = (self.selected_index == 0)
            page_size = self._page_size()
            if self.follow_mode:
                if not old_follow_mode:
                    self._flush_pending_events_locked()
                self.viewport_start = 0
            elif self.selected_index > old_index:
                if self.selected_index >= self.viewport_start + page_size:
                    self.viewport_start = self.selected_index - page_size + 1
            elif self.selected_index < old_index:
                if self.selected_index < self.viewport_start:
                    self.viewport_start = self.selected_index

            max_start = max(0, len(self.event_history) - page_size)
            self.viewport_start = max(0, min(self.viewport_start, max_start))

    def _page_size(self):
        return max(5, self.viewport_rows)

    def _queue_page_size(self):
        return max(5, self.queue_viewport_rows)

    def _set_selection(self, index):
        with self.lock:
            if not self.event_history:
                self.selected_index = 0
                self.viewport_start = 0
                self.follow_mode = True
                return
            max_index = len(self.event_history) - 1
            old_follow_mode = self.follow_mode
            self.selected_index = max(0, min(index, max_index))
            self.follow_mode = (self.selected_index == 0)
            page_size = self._page_size()
            if self.follow_mode:
                if not old_follow_mode:
                    self._flush_pending_events_locked()
                self.viewport_start = 0
            elif self.selected_index < self.viewport_start:
                self.viewport_start = self.selected_index
            elif self.selected_index >= self.viewport_start + page_size:
                self.viewport_start = self.selected_index - page_size + 1

            max_start = max(0, len(self.event_history) - page_size)
            self.viewport_start = max(0, min(self.viewport_start, max_start))

    def _toggle_follow_mode(self):
        with self.lock:
            self.follow_mode = not self.follow_mode
            if self.follow_mode:
                self._flush_pending_events_locked()
                self.selected_index = 0
                self.viewport_start = 0

    def _toggle_focus(self):
        with self.lock:
            self.focus_pane = "queue" if self.focus_pane == "history" else "history"

    def _move_queue_selection(self, delta):
        with self.lock:
            lag_items = sorted(self.group_lags.items(), key=lambda x: -x[1])
            if not lag_items:
                self.queue_selected_index = 0
                self.queue_viewport_start = 0
                return

            max_index = len(lag_items) - 1
            old_index = self.queue_selected_index
            self.queue_selected_index = max(0, min(self.queue_selected_index + delta, max_index))
            page_size = self._queue_page_size()

            if self.queue_selected_index > old_index:
                if self.queue_selected_index >= self.queue_viewport_start + page_size:
                    self.queue_viewport_start = self.queue_selected_index - page_size + 1
            elif self.queue_selected_index < old_index:
                if self.queue_selected_index < self.queue_viewport_start:
                    self.queue_viewport_start = self.queue_selected_index

            max_start = max(0, len(lag_items) - page_size)
            self.queue_viewport_start = max(0, min(self.queue_viewport_start, max_start))

    def _set_queue_selection(self, index):
        with self.lock:
            lag_items = sorted(self.group_lags.items(), key=lambda x: -x[1])
            if not lag_items:
                self.queue_selected_index = 0
                self.queue_viewport_start = 0
                return

            max_index = len(lag_items) - 1
            self.queue_selected_index = max(0, min(index, max_index))
            page_size = self._queue_page_size()
            if self.queue_selected_index < self.queue_viewport_start:
                self.queue_viewport_start = self.queue_selected_index
            elif self.queue_selected_index >= self.queue_viewport_start + page_size:
                self.queue_viewport_start = self.queue_selected_index - page_size + 1

            max_start = max(0, len(lag_items) - page_size)
            self.queue_viewport_start = max(0, min(self.queue_viewport_start, max_start))

    def _selected_item(self):
        with self.lock:
            if not self.event_history:
                return None
            idx = min(self.selected_index, len(self.event_history) - 1)
            return self.event_history[idx]

    def _input_loop(self):
        if self._tty is None:
            return
        fd = self._tty.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while self.running:
                try:
                    ready, _, _ = select.select([self._tty], [], [], 0.02)
                except (OSError, ValueError):
                    break
                if not ready:
                    continue
                try:
                    chars = os.read(fd, 1)
                except OSError:
                    break
                if not chars:
                    continue
                if chars in (b"q", b"Q", b"\x03"):
                    self.running = False
                    return
                if chars == b"\t":
                    self._toggle_focus()
                    continue
                if chars == b"\x1b":
                    seq = chars
                    for _ in range(4):
                        ready, _, _ = select.select([self._tty], [], [], 0.005)
                        if not ready:
                            break
                        seq += os.read(fd, 1)
                    chars = seq

                if chars in (b"f", b"F"):
                    self._toggle_follow_mode()
                    continue
                if chars in (b"b", b"B"):
                    with self.lock:
                        focus_pane = self.focus_pane
                    if focus_pane == "history":
                        self._request_older_history()
                    continue

                with self.lock:
                    focus_pane = self.focus_pane
                    history_len = len(self.event_history)
                    queue_len = len(self.group_lags)

                if chars in (b"j", b"\x1b[B"):
                    if focus_pane == "queue":
                        self._move_queue_selection(1)
                    else:
                        self._move_selection(1)
                elif chars in (b"k", b"\x1b[A"):
                    if focus_pane == "queue":
                        self._move_queue_selection(-1)
                    else:
                        self._move_selection(-1)
                elif chars == b"\x1b[5~":
                    if focus_pane == "queue":
                        self._move_queue_selection(-self._queue_page_size())
                    else:
                        self._move_selection(-self._page_size())
                elif chars == b"\x1b[6~":
                    if focus_pane == "queue":
                        self._move_queue_selection(self._queue_page_size())
                    else:
                        self._move_selection(self._page_size())
                elif chars in (b"g",):
                    if focus_pane == "queue":
                        self._set_queue_selection(0)
                    else:
                        self._set_selection(0)
                elif chars in (b"G",):
                    if focus_pane == "queue":
                        self._set_queue_selection(queue_len - 1)
                    else:
                        self._set_selection(history_len - 1)
        finally:
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except termios.error:
                pass

    def _render_pipe_table(self, order, counts, pipes, direction="out"):
        wide = self.console.width >= 160
        svc_width = 22 if wide else 16
        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1), expand=True)
        table.add_column("Service", width=svc_width, no_wrap=True)
        table.add_column("Pipeline", width=PIPELINE_WIDTH + 4, no_wrap=True)
        table.add_column("#", width=7, justify="right")

        with self.lock:
            for service in order:
                cfg = svc_style(service)
                color = cfg["color"]
                icon = cfg["icon"]
                count = counts.get(service, 0)
                pipe = list(pipes.get(service, []))

                if wide:
                    display_name = service
                else:
                    display_name = cfg["label"]
                label = Text(f" {icon} {display_name}", style=f"bold {color}")

                segments = []
                if direction == "out":
                    segments.append(("\u25c0\u2500", f"dim {color}"))
                    for slot in pipe[-PIPELINE_WIDTH:]:
                        if slot:
                            segments.append(("\u2588", f"bold {color}"))
                        else:
                            segments.append(("\u2500", f"dim {color}"))
                    segments.append(("\u2500\u25b6", f"dim {color}"))
                else:
                    segments.append(("\u25c0\u2500", f"dim {color}"))
                    for slot in pipe[-PIPELINE_WIDTH:]:
                        if slot:
                            segments.append(("\u2588", f"bold {color}"))
                        else:
                            segments.append(("\u2500", f"dim {color}"))
                    segments.append(("\u2500\u25b6", f"dim {color}"))

                pipe_text = Text()
                for char, style in segments:
                    pipe_text.append(char, style=style)

                count_text = Text(f"{count:,}", style=color)
                table.add_row(label, pipe_text, count_text)

        return table

    def _render_history_panel(self, height, focused=False):
        table = Table(show_header=True, header_style="bold dim", box=None, padding=(0, 1), expand=True)
        table.add_column("Key", no_wrap=True, ratio=1)
        table.add_column("Date", no_wrap=True, ratio=1)
        table.add_column("Time", no_wrap=True, ratio=1)
        table.add_column("Event", no_wrap=True, ratio=4)

        with self.lock:
            all_items = list(self.event_history)
            max_rows = max(height - 4, 10)
            self.viewport_rows = max_rows
            selected_index = min(self.selected_index, max(len(all_items) - 1, 0))
            max_start = max(0, len(all_items) - max_rows)
            self.viewport_start = max(0, min(self.viewport_start, max_start))
            if all_items:
                if selected_index < self.viewport_start:
                    self.viewport_start = selected_index
                elif selected_index >= self.viewport_start + max_rows:
                    self.viewport_start = max(0, selected_index - max_rows + 1)
            start = self.viewport_start
            items = all_items[start:start + max_rows]
            if focused and items:
                visible_selected_index = max(0, min(selected_index - start, len(items) - 1))
            else:
                visible_selected_index = -1

        for idx, item in enumerate(items):
            if "error" in item:
                row_style = "bold black on bright_white" if idx == visible_selected_index else ""
                error_cells = [
                    Text(""),
                    Text("---- -- --", style="bold red" if idx == visible_selected_index else "red"),
                    Text("--:--:--", style="bold red" if idx == visible_selected_index else "red"),
                ]
                error_cells.append(Text(f"ERROR: {item['error']}", style="bold red"))
                table.add_row(*error_cells, style=row_style)
                continue

            producer = item.get("producer")

            if producer:
                pcfg = svc_style(producer)
                pcolor = pcfg["color"]
            else:
                pcolor = "dim"

            row_style = "bold black on bright_white" if idx == visible_selected_index else ""
            key_style = "bold black" if idx == visible_selected_index else "dim"
            date_style = "bold black" if idx == visible_selected_index else "dim"
            time_style = "bold black" if idx == visible_selected_index else "dim"
            event_style = "bold black" if idx == visible_selected_index else pcolor
            row_cells = [
                Text(str(item.get("key") or ""), style=key_style),
                Text(item.get("date", "----------"), style=date_style),
                Text(item["time"], style=time_style),
            ]
            row_cells.append(Text(item["type"], style=event_style))
            table.add_row(*row_cells, style=row_style)

        return table

    def _render_queue_panel(self, focused=False):
        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1), expand=True)
        table.add_column("Consumer Group", no_wrap=False, ratio=3)
        table.add_column("Lag", justify="right", no_wrap=True, ratio=1)

        with self.lock:
            lag_items = sorted(self.group_lags.items(), key=lambda x: -x[1])
            max_rows = max(self.queue_viewport_rows, 5)
            self.queue_selected_index = max(0, min(self.queue_selected_index, max(len(lag_items) - 1, 0)))
            max_start = max(0, len(lag_items) - max_rows)
            self.queue_viewport_start = max(0, min(self.queue_viewport_start, max_start))
            if lag_items:
                if self.queue_selected_index < self.queue_viewport_start:
                    self.queue_viewport_start = self.queue_selected_index
                elif self.queue_selected_index >= self.queue_viewport_start + max_rows:
                    self.queue_viewport_start = max(0, self.queue_selected_index - max_rows + 1)
            start = self.queue_viewport_start
            visible_items = lag_items[start:start + max_rows]
            if focused and visible_items:
                visible_selected_index = max(0, min(self.queue_selected_index - start, len(visible_items) - 1))
            else:
                visible_selected_index = -1

        if not lag_items:
            table.add_row(
                Text("waiting for data..." if not self.demo else "n/a in demo mode", style="dim"),
                Text(""),
            )
            return table

        for idx, (gid, lag) in enumerate(visible_items):
            if lag == 0:
                style = "green"
            elif lag < 100:
                style = "yellow"
            elif lag < 1000:
                style = "bright_red"
            else:
                style = "bold red"
            is_selected = idx == visible_selected_index
            row_style = "bold black on bright_white" if is_selected else ""
            table.add_row(
                Text(gid, style="bold black" if is_selected else style, overflow="fold"),
                Text(f"{lag:,}", style="bold black" if is_selected else style),
                style=row_style,
            )

        return table

    def _render_payload_panel(self):
        selected = self._selected_item()
        if not selected:
            return Text("Waiting for events...", style="dim")

        if "error" in selected:
            return Text(selected["error"], style="bold red")

        width = max((self.console.width // 3) - 6, 36)
        lines = format_payload(selected.get("payload"), max_width=width)
        payload_text = Text()
        for i, line in enumerate(lines):
            if i:
                payload_text.append("\n")
            payload_text.append(line, style="dim")
        return payload_text

    def _render(self):
        eps = self._calc_eps()
        mode_parts = []
        if self.demo:
            mode_parts.append("DEMO")
            mode_parts.append(f"{self.demo_rate:.1f}/s")
        if self.dynamic:
            mode_parts.append("DYNAMIC")
        if self.show_ephemeral_groups:
            mode_parts.append("EPHEMERAL")
        if self.since_time is not None:
            mode_parts.append(f"SINCE {self.since_time.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}")
        if not self.demo:
            mode_parts.append(self.bootstrap_servers)
        mode = " | ".join(mode_parts)

        with self.lock:
            follow_mode = self.follow_mode
            status_message = self.status_message
            status_level = self.status_level
            lag = self.consumer_lag
            focus_pane = self.focus_pane
            pending_count = len(self.pending_events)
            history_loading = self.history_loading
            history_exhausted = self.history_exhausted
        paused = not follow_mode
        header_style = "bold white on dark_green" if not paused else "bold black on yellow"
        status_style = "bold green" if follow_mode else "bold black on yellow"
        title_text = "  KAFKA EVENT EXPLORER  "

        status_text = Text()
        status_text.append(title_text, style=header_style)
        status_text.append(f"  {mode}  ", style="dim")
        status_text.append(f"  {self.total_count:,} events  ", style="bold")
        status_text.append(f"  {eps:.1f} evt/s  ", style="bold green" if eps > 0 else "dim")
        status_text.append(
            "  FOLLOW  " if follow_mode else "  PAUSED  ",
            style=status_style,
        )
        if history_loading:
            status_text.append("  HISTORY LOADING  ", style="bold cyan")
        elif history_exhausted and not follow_mode:
            status_text.append("  HISTORY EXHAUSTED  ", style="bold yellow")
        if pending_count:
            status_text.append(f"  +{pending_count} queued  ", style="bold cyan")
        if lag is not None:
            if lag == 0:
                status_text.append("  lag: 0  ", style="bold green")
            elif lag < 100:
                status_text.append(f"  lag: {lag:,}  ", style="bold yellow")
            else:
                status_text.append(f"  lag: {lag:,}  ", style="bold red")
        if self.unmapped_count:
            status_text.append(f"  {self.unmapped_count} unmapped  ", style="bold red")
        if status_message:
            status_text.append(f"  {status_message}  ", style=status_level)
        status_text.append(
            f"  Focus: {'Events' if focus_pane == 'history' else 'Lag'}  ",
            style="bold cyan",
        )

        controls_text = Text()
        controls_text.append(
            "Controls: Scroll j/k or arrows  Page PgUp/PgDn  Load older b  Switch pane Tab  Follow newest f  Jump g/G  Exit q or Ctrl+C",
            style="dim",
        )
        header_content = Group(controls_text, status_text)

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=2),
            Layout(name="bottom", ratio=2),
        )
        layout["header"].update(header_content)

        layout["bottom"].split_row(
            Layout(name="log", ratio=3),
            Layout(name="detail", ratio=1),
        )

        layout["detail"].split_column(
            Layout(name="payload", ratio=3),
            Layout(name="queue", ratio=2),
        )

        log_panel = Panel(
            self._render_history_panel(
                layout["log"].size or (self.console.height if self.console.height else 40),
                focused=(focus_pane == "history"),
            ),
            title="[bold black on bright_white] Event Log [/bold black on bright_white]" if focus_pane == "history" else "[bold]Event Log[/bold]",
            subtitle="[dim]Event stream[/dim]",
            border_style="bright_white" if focus_pane == "history" else "dim",
        )
        layout["log"].update(log_panel)

        payload_panel = Panel(
            self._render_payload_panel(),
            title="[bold]Latest Event Payload[/bold]",
            subtitle="[dim]Newest event JSON[/dim]",
            border_style="magenta",
        )
        layout["payload"].update(payload_panel)

        self.queue_viewport_rows = max((layout["queue"].size or 10) - 4, 5)
        queue_panel = Panel(
            self._render_queue_panel(focused=(focus_pane == "queue")),
            title="[bold black on bright_white] Consumer Group Lag [/bold black on bright_white]" if focus_pane == "queue" else "[bold]Consumer Group Lag[/bold]",
            border_style="bright_white" if focus_pane == "queue" else "yellow",
        )
        layout["queue"].update(queue_panel)

        return layout

    def run(self):
        self.console = Console()
        input_thread = None

        if self.demo:
            thread = threading.Thread(target=self._demo_producer, daemon=True)
        else:
            thread = threading.Thread(target=self._consume_kafka, daemon=True)
            lag_thread = threading.Thread(target=self._poll_group_lags, daemon=True)
            lag_thread.start()
        thread.start()
        try:
            self._tty = open("/dev/tty", "rb", buffering=0)
        except OSError:
            self._tty = None
        if self._tty is not None:
            input_thread = threading.Thread(target=self._input_loop, daemon=True)
            input_thread.start()

        tick_count = 0
        try:
            with Live(self._render(), console=self.console, refresh_per_second=20, screen=True) as live:
                while self.running:
                    time.sleep(0.02)
                    tick_count += 1
                    if tick_count % 9 == 0:
                        self._tick_pipelines()
                    live.update(self._render())
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            if input_thread is not None:
                input_thread.join(timeout=0.2)
            if self._tty is not None:
                self._tty.close()
            self.console.print("\n[dim]Visualizer stopped.[/dim]")


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Event Visualizer - Terminal UI for exploring Kafka events in real-time",
        epilog="""
QUICK START:
  ./kafka-event-visualizer.sh              # Topic picker + recent 200 + tail
  ./kafka-event-visualizer.sh --demo       # Demo mode (no Kafka needed)
  ./kafka-event-visualizer.sh --filter order  # Filter events by type (case-insensitive, wildcard)

PANES:
  Events (left)      - Event log with type, key, timestamp
  Payload (middle)   - JSON payload of selected event
  Lag (right)        - Consumer group lag per topic

KEYBOARD CONTROLS:
  Navigation:
    j/k, ↑/↓         - Move within pane
    PgUp/PgDn        - Page up/down
    g/G              - Jump to top/bottom
    Tab / Shift+Tab  - Switch pane (forward/backward)

  Event Control:
    f                - Follow mode (tail newest)
    b                - Load older history batch
    :                - Filter by event type pattern

  Other:
    q, Ctrl+C        - Exit

EXAMPLES:
  Replay from 1 hour ago:
    ./kafka-event-visualizer.sh --topic orders --since-minutes 60

  Filter for order events:
    ./kafka-event-visualizer.sh --topic events --filter order

  Custom broker:
    ./kafka-event-visualizer.sh --bootstrap kafka.prod:9092 --topic events

  Load 5000 events into memory:
    ./kafka-event-visualizer.sh --window-size 5000
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic",
                        help="Kafka topic. Omit this to open a topic picker.")
    parser.add_argument("--demo", action="store_true", help="Run in demo mode (no Kafka needed)")
    parser.add_argument("--demo-rate", type=float, default=3.0,
                        help="Average demo events per second; implies --demo when explicitly set")
    parser.add_argument("--window-size", type=int, default=2000,
                        help="Maximum number of loaded history events to keep in memory")
    parser.add_argument("--history-batch-size", type=int, default=200,
                        help="Number of older events to fetch per history-browser request")
    parser.add_argument("--show-ephemeral-groups", action="store_true",
                        help="Show UUID-shaped ephemeral consumer groups in the lag panel")
    parser.add_argument("--since",
                        help="Replay from a specific local/ISO timestamp, e.g. '2026-04-17 09:30:00'")
    parser.add_argument("--since-minutes", type=int,
                        help="Replay from N minutes ago")
    parser.add_argument("--type-field",
                        help="JSON field to use as the event type; overrides auto-detection")
    parser.add_argument("--time-field",
                        help="JSON field to use as the event timestamp; overrides auto-detection")
    parser.add_argument("--filter",
                        help="Filter events by type pattern. Supports wildcards (e.g., order_* matches order_created, order_shipped, etc.)")
    args = parser.parse_args()
    demo_requested = args.demo or any(
        arg == "--demo-rate" or arg.startswith("--demo-rate=")
        for arg in sys.argv[1:]
    )

    if args.since and args.since_minutes is not None:
        parser.error("--since and --since-minutes cannot be used together")

    since_time = None
    if args.since:
        since_time = parse_since_value(args.since)
    elif args.since_minutes is not None:
        since_time = datetime.now().astimezone() - timedelta(minutes=args.since_minutes)

    topic = args.topic
    if not topic and not demo_requested:
        topic = pick_topic(args.bootstrap)
        if not topic:
            return
    if not demo_requested and since_time is None:
        since_time, canceled = pick_start_time()
        if canceled:
            return
    if not topic:
        topic = "demo-events"

    viz = EventBusVisualizer(
        bootstrap_servers=args.bootstrap,
        topic=topic,
        demo=demo_requested,
        dynamic=True,
        show_ephemeral_groups=args.show_ephemeral_groups,
        since_time=since_time,
        type_field=args.type_field,
        time_field=args.time_field,
        demo_rate=args.demo_rate,
        history_window_size=args.window_size,
        history_batch_size=args.history_batch_size,
    )
    viz.run()


if __name__ == "__main__":
    main()
PYTHON_EOF

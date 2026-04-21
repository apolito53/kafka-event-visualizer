# Kafka Event Visualizer

A self-contained terminal UI for exploring Kafka traffic in near real time.

The project is intentionally lightweight: one shell script, an embedded Python app, and no packaging ceremony. It is meant for local debugging, topic exploration, and quick history browsing while developing against Kafka.

## Features

- Scrollable event list with key, date, time, and event type
- Payload inspector for the selected event (scrollable)
- Consumer group lag pane with live and time-windowed peak lag views
- Event type distribution panel with selectable time windows and bar charts
- Event filtering by type pattern (case-insensitive, wildcards)
- Event muting — hide noisy event types, toggle via interactive UI
- Topic picker when no topic is provided
- Replay from a timestamp or recent lookback window
- Bounded history browser with on-demand older event loading
- Auto-load on scroll — reaching the bottom automatically fetches more history
- Responsive layout that adapts to small and large terminal windows
- Kubernetes mode — bridge pod streams events to local visualizer via pipe
- Demo mode for UI testing without Kafka
- Generic JSON event exploration with automatic event-type and time-field detection

## Quick Start

### Local Development

Run against local Kafka (default: `localhost:9092`):

```bash
./run_local.sh
```

### Kubernetes

Run against Kafka in your k8s cluster:

```bash
./run_on_k8s.sh
```

## How It Works

The entrypoint is [`kafka-event-visualizer.sh`](./kafka-event-visualizer.sh).

The shell wrapper:
- checks for `python3`
- creates a local `.venv` if needed
- installs `rich` and `kafka-python` there if missing
- runs the embedded Python application via heredoc

The UI is built with `rich`, and Kafka access uses `kafka-python`.

## Requirements

- Python 3
- Access to a Kafka broker
- A terminal with at least 40 columns and 10 rows

The script will auto-install these Python packages into a local `.venv` if needed:
- `rich`
- `kafka-python`

### WSL Requirements

If you run this from WSL, Python virtualenv support must be installed for the system Python. On Ubuntu/Debian-based WSL distros, install the matching `venv` package first:

```bash
sudo apt update
sudo apt install python3.12-venv
```

If your WSL image has a different Python minor version, replace `3.12` with the version reported by `python3 --version`.

The script creates and reuses a project-local `.venv`, so it does not need to install packages into the system Python.

If Kafka is running outside WSL, make sure the broker is reachable from the WSL environment and pass an explicit bootstrap address when needed:

```bash
./run_local.sh --bootstrap <host>:9092
```

## Usage

### Basic

```bash
# Topic picker + recent 200 events + tail
./run_local.sh

# Custom topic
./run_local.sh --topic my-custom-topic

# Different broker
./run_local.sh --bootstrap kafka.prod:9092
```

### Filtering

```bash
# Filter for order events
./run_local.sh --filter ORDER

# Wildcard pattern
./run_local.sh --filter "ORDER_*"

# Interactive filter: press `:` in the UI
```

### Time-based Replay

```bash
# Start from 1 hour ago
./run_local.sh --since-minutes 60

# Start from specific timestamp
./run_local.sh --since "2026-04-20 14:30:00"

# Load more recent history on startup (default: 200 events)
./run_local.sh --history-batch-size 500
```

### Memory Configuration

```bash
# Larger in-memory window for long sessions
./run_local.sh --window-size 5000

# Bigger history batches when pressing 'b'
./run_local.sh --history-batch-size 500
```

### Kubernetes Examples

```bash
# Basic k8s usage
./run_on_k8s.sh

# With filter
./run_on_k8s.sh --filter ORDER

# Load 30 minutes of history
./run_on_k8s.sh --since-minutes 30

# Custom topic/bootstrap
./run_on_k8s.sh --topic my-events --bootstrap kafka:9092
```

### Demo Mode

Test the UI without Kafka:

```bash
./run_local.sh --demo
./run_local.sh --demo --demo-rate 25  # 25 events/sec
```

## Controls

### Navigation

| Key | Action |
|-----|--------|
| `j` / `k` or `↑` / `↓` | Move selection / scroll pane |
| `PgUp` / `PgDn` | Page up / down |
| `g` / `G` | Jump to top / bottom |
| `Tab` / `Shift+Tab` | Cycle focus forward / backward |
| `←` / `→` | Change time window (Distribution / Lag pane) |

### Event Control

| Key | Action |
|-----|--------|
| `f` | Follow mode (tail newest events) |
| `b` | Load older history batch |
| `:` | Filter by event type pattern |
| `x` | Mute selected event type (or unmute from muted pane) |
| `X` | Clear all muted types |
| `q` or `Ctrl+C` | Exit |

### Panes

Focus cycles through: **Events** → **Payload** → **Distribution** → **Lag** → **Muted** → Events

- **Events (left)**: Scrollable event log with type, key, timestamp
- **Payload (middle top)**: JSON payload of selected event (scrollable)
- **Distribution (middle bottom)**: Event type distribution with bar chart and time window selector (`←`/`→`)
- **Lag (right)**: Consumer group lag per topic, with peak lag over selected time windows
- **Muted (sidebar)**: List of muted event types (navigate and unmute with `x`)

## Scripts

| Script | Purpose |
|--------|---------|
| `run_local.sh` | Convenience wrapper for local Kafka (calls `kafka-event-visualizer.sh`) |
| `run_on_k8s.sh` | Convenience wrapper for Kubernetes (calls `kafka-event-visualizer-k8s.sh`) |
| `kafka-event-visualizer.sh` | Core visualizer script (can be used directly) |
| `kafka-event-visualizer-k8s.sh` | Kubernetes bridge launcher (can be used directly) |

## Architecture

### Local Mode (`run_local.sh` / `kafka-event-visualizer.sh`)

Runs directly on your machine, connects to Kafka via `kafka-python`:

```
[Your Terminal]
      ↓
[kafka-event-visualizer.sh]
      ↓ kafka-python
[Kafka Broker]
```

### Kubernetes Mode (`run_on_k8s.sh` / `kafka-event-visualizer-k8s.sh`)

Runs a bridge in a temp pod, streams events to local visualizer via pipe:

```
[Your Terminal]
      ↓
[kafka-event-visualizer.sh --stdin]
      ↑ JSON lines via pipe
[kubectl exec]
      ↑
[Temp Pod: bridge script]
  - Kafka consumer → stdout
  - HTTP server (port 8080) → on-demand history
      ↓ kafka-python
[Kafka Broker Pods (in cluster)]
```

**Why a bridge pod?** Kafka advertised listeners inside k8s are often not reachable from your local machine. The bridge runs inside the cluster where it can access Kafka directly, and streams decoded JSON to the local visualizer over kubectl exec. Rich renders locally so the TUI stays responsive — no lag from rendering over a remote connection.

The temporary pod is automatically cleaned up on exit.

## Time Windows

The Distribution and Lag panes support these windows: `All`, `5m`, `30m`, `1h`, `1d`, `7d`, `30d`, `90d`, `180d`, `1y`.

Tab to the Distribution pane and use `←` / `→` to recalculate event type counts from all events currently loaded in memory, including historical events loaded on startup or through older-history browsing.

Tab to the Lag pane and use `←` / `→` to switch from live lag to peak lag over the selected window. Lag snapshots are recorded about every 5 seconds while the visualizer is running, including lag data streamed through the Kubernetes bridge.

## Flags

- `--bootstrap HOST:PORT`
  Kafka bootstrap server list. In k8s mode, defaults to `confluent-platform-cp-kafka:9092`.
- `--topic TOPIC`
  Topic to inspect. If omitted, the script opens a topic picker.
- `--demo`
  Run without Kafka and generate fake traffic.
- `--demo-rate N`
  Average number of demo events per second. Explicitly setting this also enables demo mode.
- `--since ISO_TIMESTAMP`
  Replay from a specific point in time.
- `--since-minutes N`
  Replay from N minutes ago.
- `--type-field FIELD`
  Override event-type field auto-detection.
- `--time-field FIELD`
  Override event-timestamp field auto-detection.
- `--window-size N`
  Maximum number of loaded history events kept in memory.
- `--history-batch-size N`
  Number of older events to fetch each time `b` is pressed.
- `--show-ephemeral-groups`
  Include UUID-like consumer groups in the lag pane.
- `--filter PATTERN`
  Filter events by type pattern. Supports wildcards (e.g., `ORDER_*`).
- `--stdin`
  Read events as JSON lines from stdin instead of connecting to Kafka directly. Used by the k8s launcher.
- `--http-bridge URL`
  HTTP bridge URL for on-demand history loading in stdin mode.

## Notes

- This is a local development tool, not production infrastructure.
- Kafka does not inherently know which service emitted a specific record unless producers include that metadata in headers or payloads.
- Memory footprint scales mostly with `window-size` and average decoded payload size.
- The script stores decoded payloads for the loaded history window so the payload pane remains browsable.
- The lag panel filters out consumer groups containing "visualizer", console consumers, and UUID-shaped ephemeral groups by default.

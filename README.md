# Kafka Event Visualizer

A self-contained terminal UI for exploring Kafka traffic in near real time.

The project is intentionally lightweight: one shell script, an embedded Python app, and no packaging ceremony. It is meant for local debugging, topic exploration, and quick history browsing while developing against Kafka.

## Features

- Scrollable event list with key, date, time, and event type
- Payload inspector for the selected event
- Consumer group lag pane with severity coloring
- Topic picker when no topic is provided
- Replay from a timestamp or recent lookback window
- Bounded history browser with explicit `load older` behavior
- Demo mode for UI testing without Kafka
- Generic JSON event exploration with automatic event-type and time-field detection

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
- A terminal with enough width/height to comfortably render the TUI

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
./kafka-event-visualizer.sh --bootstrap <host>:9092
```

## Usage

Basic startup with topic picker:

```bash
./kafka-event-visualizer.sh
```

Connect to a specific broker and topic:

```bash
./kafka-event-visualizer.sh --bootstrap localhost:9092 --topic orders
```

Replay from a specific local timestamp:

```bash
./kafka-event-visualizer.sh --topic orders --since "2026-04-17 09:30:00"
```

Replay from N minutes ago:

```bash
./kafka-event-visualizer.sh --topic orders --since-minutes 30
```

Run demo mode:

```bash
./kafka-event-visualizer.sh --demo
./kafka-event-visualizer.sh --demo-rate 25
```

Tune the loaded history window:

```bash
./kafka-event-visualizer.sh --topic orders --window-size 5000 --history-batch-size 500
```

## Controls

The event pane and lag pane share the same navigation model. Use `Tab` to switch focus.

- `j` / `k` or Up / Down: move selection
- `PgUp` / `PgDn`: page up / down
- `g` / `G`: jump to top / bottom
- `Tab`: switch focus between Events and Consumer Group Lag
- `f`: return to follow mode / resume tailing newest events
- `b`: load an older history slice while browsing the event pane
- `q` or `Ctrl+C`: exit

## History Browser Model

The visible event list is a bounded in-memory window.

- In `FOLLOW`, the list tails the newest events.
- When you leave the newest event, the UI enters a frozen browsing state.
- While browsing, live events continue to arrive but are buffered instead of mutating the visible list.
- Press `b` to fetch an older bounded slice of topic history.
- Returning to follow mode flushes buffered live events back into the visible window.

Older-history fetches use a separate temporary consumer so the live tail consumer is not disturbed.

## Flags

- `--bootstrap HOST:PORT`
  Kafka bootstrap server list.
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

## Notes

- This is a local development tool, not production infrastructure.
- Kafka does not inherently know which service emitted a specific record unless producers include that metadata in headers or payloads.
- Memory footprint scales mostly with `window-size` and average decoded payload size.
- The script stores decoded payloads for the loaded history window so the payload pane remains browsable.

## Future Directions

Potential improvements that fit the current model:

- better batching/coalescing for held key input
- richer payload formatting and truncation controls
- optional header inspection
- export selected history slices
- optional producer metadata display when headers or payload fields are available

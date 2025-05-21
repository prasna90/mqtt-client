# mqtt-paho-client

A Python project for working with [Sparkplug B](https://sparkplug.eclipse.org/) MQTT messages, including publishing, subscribing, and storing data in SQLite.

## Features

- **MQTT Client**: Subscribes to Sparkplug B messages and stores device/metric data in a local SQLite database (`mqtt-client.py`).
- **MQTT Publisher**: Publishes sample Sparkplug B messages for testing (`mqtt-pub.py`).
- **Database Viewer**: Command-line tool to view devices and metrics stored in the database (`db-viewer.py`).
- **Protocol Buffers**: Uses generated Sparkplug B protobuf definitions (`sparkplug_b_pb2.py`).

## Requirements

- Python 3.13+
- [paho-mqtt](https://pypi.org/project/paho-mqtt/)
- [protobuf](https://pypi.org/project/protobuf/)
- [tahu](https://pypi.org/project/tahu/)

Install dependencies with:

```sh
pip install -r requirements.txt
```

Or use the dependencies listed in `pyproject.toml`.

## Usage

### 1. Start the MQTT Client

Subscribes to Sparkplug B topics and stores data in `sparkplug_data.db`:

```sh
python mqtt-client.py
```

### 2. Publish Sample Data

Publishes Sparkplug B messages to your MQTT broker:

```sh
python mqtt-pub.py
```

### 3. View Database Contents

List all devices and metrics:

```sh
python db-viewer.py --devices --metrics
```

Show metrics for a specific device ID:

```sh
python db-viewer.py --device <DEVICE_ID>
```

## Project Structure

- `mqtt-client.py`: Main MQTT subscriber and database writer.
- `mqtt-pub.py`: Publishes test Sparkplug B messages.
- `db-viewer.py`: CLI tool to inspect the SQLite database.
- `sparkplug_b_pb2.py`: Generated protobuf code for Sparkplug B.
- `sparkplug_data.db`: SQLite database file (created at runtime).
- `pyproject.toml`: Project metadata and dependencies.

## License

MIT License 

---
*This project is for testing and development with Sparkplug B MQTT payloads and is not intended for production use without further security and robustness improvements.*
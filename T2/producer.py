"""
Task 2 / Step 2.2 - Live telemetry simulation.

Reads the Austin 'Camera Traffic Counts' CSV, converts each row to a
structured JSON event, and publishes it to the 'traffic-telemetry' topic
every 2 seconds. Messages are keyed by sensor id so records for the same
sensor always land in the same partition (of the 3 partitions).

A small shuffle buffer (--disorder) deliberately emits some events out of
chronological order, so the Flink watermarking strategy in Step 2.3 has real
out-of-order data to reconcile - mention this in your report.

    pip install kafka-python-ng
    python producer.py camera_traffic_counts.csv
    python producer.py camera_traffic_counts.csv --disorder 5 --interval 2
"""

import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "traffic-telemetry"
BOOTSTRAP = "localhost:9092"

# Column aliases -> tolerant to Socrata header variations
SENSOR_COLS = ["atd device id", "atd_device_id", "detector id", "device id",
               "kits id", "camera id", "sensor id"]
TIME_COLS   = ["read date", "read_date", "reading datetime", "curdatetime",
               "published date", "timestamp", "datetime"]
VOLUME_COLS = ["volume", "vehicle count", "count", "total volume"]
EXTRA_COLS  = ["intersection name", "direction", "movement", "speed average",
               "day of week", "time bin"]

TIME_FORMATS = ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]


def pick(cols, candidates):
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    return None


def parse_ts(raw):
    raw = str(raw).strip()
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def rows_to_events(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cols = {c.strip().lower(): c for c in reader.fieldnames}
        print(f"[detect] columns: {reader.fieldnames}")

        sensor_c = pick(cols, SENSOR_COLS)
        time_c   = pick(cols, TIME_COLS)
        vol_c    = pick(cols, VOLUME_COLS)
        if not (time_c and vol_c):
            sys.exit("[error] could not locate timestamp/volume columns - "
                     "check the printed header and extend the alias lists.")
        print(f"[detect] sensor={sensor_c}  time={time_c}  volume={vol_c}")

        for row in reader:
            ts = parse_ts(row.get(time_c, ""))
            if ts is None:
                continue
            try:
                volume = int(float(row[vol_c]))
            except (ValueError, TypeError, KeyError):
                continue
            event = {
                "sensor_id": str(row.get(sensor_c, "unknown")).strip() or "unknown",
                "event_time": ts.isoformat(),
                "event_ts_ms": int(ts.timestamp() * 1000),   # used by Flink watermarks
                "volume": volume,
            }
            for extra in EXTRA_COLS:
                if extra in cols:
                    event[extra.replace(" ", "_")] = row.get(cols[extra], "")
            yield event


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path")
    ap.add_argument("--interval", type=float, default=2.0,
                    help="seconds between messages (assignment: 2)")
    ap.add_argument("--disorder", type=int, default=4,
                    help="shuffle-buffer size to create out-of-order events (0 = off)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )

    sent, buffer = 0, []
    for event in rows_to_events(args.csv_path):
        buffer.append(event)
        if len(buffer) >= max(1, args.disorder):
            random.shuffle(buffer)               # inject bounded out-of-orderness
            for ev in buffer:
                producer.send(TOPIC, key=ev["sensor_id"], value=ev)
                sent += 1
                print(f"[{sent:>6}] sensor={ev['sensor_id']:<8} "
                      f"t={ev['event_time']} volume={ev['volume']}")
                time.sleep(args.interval)
                if args.limit and sent >= args.limit:
                    producer.flush()
                    return
            buffer = []
    for ev in buffer:
        producer.send(TOPIC, key=ev["sensor_id"], value=ev)
    producer.flush()
    print(f"[done] {sent} events published to '{TOPIC}'")


if __name__ == "__main__":
    main()

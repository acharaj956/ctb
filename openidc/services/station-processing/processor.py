"""OpenIDC station-processing service.

Consumes signed waveform frames from Kafka, verifies their PKI signature, runs
the STA/LTA detector, and writes each detection as a CSS 3.0 `arrival` row into
Postgres (plus a `wfdisc` row describing the segment). Detections are also
re-published to the `detections` topic for the Phase 2 network-association
stage to consume.

This is the OpenIDC analogue of IDC automatic station processing.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sys
import time

# Make the shared `common` package importable (it is copied to /app/common).
sys.path.insert(0, "/app")

import psycopg2
from kafka import KafkaConsumer, KafkaProducer

from common import frames
from stalta import detect_triggers, recursive_sta_lta, using_c_kernel

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s station-proc %(levelname)s %(message)s"
)
log = logging.getLogger("station-processing")

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
RAW_TOPIC = os.environ.get("RAW_TOPIC", "raw.waveforms")
DET_TOPIC = os.environ.get("DET_TOPIC", "detections")
CONTROL_TOPIC = os.environ.get("CONTROL_TOPIC", "control")
STA_SECONDS = float(os.environ.get("STA_SECONDS", "1.0"))
LTA_SECONDS = float(os.environ.get("LTA_SECONDS", "8.0"))
TRIGGER_ON = float(os.environ.get("TRIGGER_ON", "4.0"))
TRIGGER_OFF = float(os.environ.get("TRIGGER_OFF", "1.5"))
VERIFY_FRAMES = os.environ.get("VERIFY_FRAMES", "true").lower() == "true"
PKI_DIR = os.environ.get("PKI_DIR", "/pki")

# Detector thresholds are mutable at runtime via the `control` topic, so the
# dashboard can tune detector sensitivity live — a miniature control plane.
THRESHOLDS = {"on": TRIGGER_ON, "off": TRIGGER_OFF}


def apply_threshold_command(raw: bytes) -> None:
    """Validate and apply a live detector-threshold control message."""
    try:
        cmd = json.loads(raw)
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        log.warning("ignoring malformed control message: %s", exc)
        return
    if not isinstance(cmd, dict):
        log.warning("ignoring non-object control message")
        return

    try:
        if isinstance(cmd.get("trigger_on"), bool) or isinstance(cmd.get("trigger_off"), bool):
            raise ValueError
        new_on = float(cmd.get("trigger_on", THRESHOLDS["on"]))
        new_off = float(cmd.get("trigger_off", THRESHOLDS["off"]))
    except (TypeError, ValueError):
        log.warning("ignoring control message with non-numeric thresholds: %s", cmd)
        return

    if not (math.isfinite(new_on) and math.isfinite(new_off)):
        log.warning("ignoring control message with non-finite thresholds: %s", cmd)
        return
    if not (0.1 <= new_on <= 50.0 and 0.1 <= new_off <= 50.0):
        log.warning("ignoring out-of-range thresholds: on=%.2f off=%.2f", new_on, new_off)
        return
    if new_off >= new_on:
        log.warning("ignoring invalid thresholds: off %.2f must be less than on %.2f", new_off, new_on)
        return

    THRESHOLDS["on"] = new_on
    THRESHOLDS["off"] = new_off
    log.info("CONTROL thresholds updated -> on=%.2f off=%.2f",
             THRESHOLDS["on"], THRESHOLDS["off"])


def validate_config() -> None:
    if not (math.isfinite(STA_SECONDS) and STA_SECONDS > 0):
        raise SystemExit("STA_SECONDS must be positive and finite")
    if not (math.isfinite(LTA_SECONDS) and LTA_SECONDS > STA_SECONDS):
        raise SystemExit("LTA_SECONDS must be finite and greater than STA_SECONDS")
    if not (0.1 <= THRESHOLDS["on"] <= 50.0 and 0.1 <= THRESHOLDS["off"] <= 50.0):
        raise SystemExit("TRIGGER_ON/TRIGGER_OFF must be between 0.1 and 50.0")
    if THRESHOLDS["off"] >= THRESHOLDS["on"]:
        raise SystemExit("TRIGGER_OFF must be less than TRIGGER_ON")


def connect_db():
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=os.environ["PGHOST"],
                port=os.environ.get("PGPORT", "5432"),
                user=os.environ["PGUSER"],
                password=os.environ["PGPASSWORD"],
                dbname=os.environ["PGDATABASE"],
            )
            conn.autocommit = True
            log.info("connected to Postgres")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Postgres not ready (%s/30): %s", attempt + 1, exc)
            time.sleep(2)
    raise SystemExit("could not connect to Postgres")


def connect_consumer():
    for attempt in range(30):
        try:
            return KafkaConsumer(
                RAW_TOPIC,
                CONTROL_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id="station-processing",
                auto_offset_reset="latest",
                value_deserializer=lambda b: b,
                enable_auto_commit=True,
            )
        except Exception as exc:  # noqa: BLE001 - broker may not be up yet
            log.warning("Kafka not ready (%s/30): %s", attempt + 1, exc)
            time.sleep(2)
    raise SystemExit("could not connect to Kafka")


def store_wfdisc(cur, frame: frames.Frame) -> int:
    cur.execute("SELECT nextval('wfid_seq')")
    wfid = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO wfdisc (sta, chan, time, wfid, jdate, endtime, nsamp, samprate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (frame.sta, frame.chan, frame.start_time, wfid, frame.jdate,
         frame.end_time, len(frame.samples), frame.sample_rate),
    )
    return wfid


def store_arrival(cur, frame: frames.Frame, onset_time: float, snr: float) -> int:
    cur.execute("SELECT nextval('arid_seq')")
    arid = cur.fetchone()[0]
    cur.execute(
        """
        INSERT INTO arrival (sta, time, arid, jdate, chan, iphase, snr, auth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (frame.sta, onset_time, arid, frame.jdate, frame.chan, "P", snr, "OpenIDC-auto"),
    )
    return arid


def main() -> None:
    validate_config()
    log.info("STA/LTA detector kernel: %s", "C (libstalta.so)" if using_c_kernel() else "NumPy fallback")

    public_key = None
    if VERIFY_FRAMES:
        log.info("waiting for producer public key in %s ...", PKI_DIR)
        public_key = frames.wait_for_public_key(PKI_DIR)
        log.info("loaded producer public key; frame verification enabled")

    db = connect_db()
    consumer = connect_consumer()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    log.info("consuming %s ...", RAW_TOPIC)

    processed = 0
    detected = 0
    for msg in consumer:
        # Control-plane message: update detector thresholds live.
        if msg.topic == CONTROL_TOPIC:
            apply_threshold_command(msg.value)
            continue

        try:
            frame, sig = frames.Frame.from_wire(msg.value)
        except ValueError as exc:
            log.warning("REJECTED malformed frame: %s", exc)
            continue

        if VERIFY_FRAMES:
            if sig is None or not frames.verify(frame, sig, public_key):
                log.warning("REJECTED unsigned/invalid frame from %s — skipping", frame.sta)
                continue

        nsta = max(1, int(STA_SECONDS * frame.sample_rate))
        nlta = max(nsta + 1, int(LTA_SECONDS * frame.sample_rate))
        charfct = recursive_sta_lta(frame.samples, nsta, nlta)
        triggers = detect_triggers(charfct, THRESHOLDS["on"], THRESHOLDS["off"])

        processed += 1
        with db.cursor() as cur:
            wfid = store_wfdisc(cur, frame)
            for onset_sample, peak in triggers:
                onset_time = frame.start_time + onset_sample / frame.sample_rate
                arid = store_arrival(cur, frame, onset_time, peak)
                detected += 1
                producer.send(
                    DET_TOPIC,
                    {
                        "arid": arid,
                        "wfid": wfid,
                        "sta": frame.sta,
                        "chan": frame.chan,
                        "time": onset_time,
                        "snr": round(peak, 2),
                    },
                )
                log.info(
                    "DETECTION  sta=%s chan=%s snr=%.1f arid=%d",
                    frame.sta, frame.chan, peak, arid,
                )

        if processed % 20 == 0:
            log.info("processed=%d frames, detected=%d arrivals", processed, detected)


if __name__ == "__main__":
    main()

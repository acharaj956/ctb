"""OpenIDC ingestion service.

Emits continuous, PKI-signed waveform frames onto the `raw.waveforms` Kafka
topic — the OpenIDC analogue of IMS stations streaming CD-1.1 data into the IDC.

Two modes:
  * synthetic (default): generates band-limited noise with occasional injected
    seismic transients, so the downstream detector reliably fires. No network.
  * FDSN (FDSN_MODE=true): pulls real public waveform data via ObsPy. See
    docs/RUNBOOK.md. Falls back to synthetic if ObsPy is unavailable.
"""
from __future__ import annotations

import logging
import math
import os
import re
import sys
import time

import numpy as np

sys.path.insert(0, "/app")

from kafka import KafkaProducer

from common import frames, synthetic

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s ingestion %(levelname)s %(message)s"
)
log = logging.getLogger("ingestion")

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
RAW_TOPIC = os.environ.get("RAW_TOPIC", "raw.waveforms")
STATIONS = [s.strip() for s in os.environ.get("STATIONS", "ARCES,FINES").split(",")]
SAMPLE_RATE = float(os.environ.get("SAMPLE_RATE", "40"))
FRAME_SECONDS = float(os.environ.get("FRAME_SECONDS", "10"))
EVENT_PROBABILITY = float(os.environ.get("EVENT_PROBABILITY", "0.25"))
SIGN_FRAMES = os.environ.get("SIGN_FRAMES", "true").lower() == "true"
FDSN_MODE = os.environ.get("FDSN_MODE", "false").lower() == "true"
PKI_DIR = os.environ.get("PKI_DIR", "/pki")

CHANNEL = "BHZ"
STA_RE = re.compile(r"^[A-Z0-9]{1,6}$")
_rng = np.random.default_rng()


def validate_config() -> None:
    if not STATIONS or any(not STA_RE.fullmatch(sta) for sta in STATIONS):
        raise SystemExit("STATIONS must be a comma-separated list of ^[A-Z0-9]{1,6}$ station codes")
    if not (math.isfinite(SAMPLE_RATE) and SAMPLE_RATE > 0):
        raise SystemExit("SAMPLE_RATE must be positive and finite")
    if not (math.isfinite(FRAME_SECONDS) and FRAME_SECONDS > 0):
        raise SystemExit("FRAME_SECONDS must be positive and finite")
    if not (math.isfinite(EVENT_PROBABILITY) and 0.0 <= EVENT_PROBABILITY <= 1.0):
        raise SystemExit("EVENT_PROBABILITY must be between 0.0 and 1.0")


def synthetic_frame_samples() -> np.ndarray:
    """Background noise with a probabilistically injected seismic transient."""
    n = int(FRAME_SECONDS * SAMPLE_RATE)
    samples = synthetic.noise(n, _rng)

    if _rng.random() < EVENT_PROBABILITY:
        freq = float(_rng.uniform(2.0, 6.0))           # Hz
        amp = float(_rng.uniform(8.0, 20.0))           # well above the noise floor
        onset_frac = float(_rng.uniform(0.2, 0.7))
        samples, onset = synthetic.add_event(
            samples, SAMPLE_RATE, amp, freq, onset_frac=onset_frac
        )
        log.info("injected synthetic event at sample %d (f=%.1fHz)", onset, freq)

    return samples


def connect_producer() -> KafkaProducer:
    for attempt in range(30):
        try:
            return KafkaProducer(bootstrap_servers=BOOTSTRAP)
        except Exception as exc:  # noqa: BLE001 - broker may not be up yet
            log.warning("Kafka not ready (%s/30): %s", attempt + 1, exc)
            time.sleep(2)
    raise SystemExit("could not connect to Kafka")


def run_synthetic(producer: KafkaProducer, private_key) -> None:
    log.info("synthetic mode: stations=%s rate=%gHz frame=%gs", STATIONS, SAMPLE_RATE, FRAME_SECONDS)
    while True:
        now = time.time()
        for sta in STATIONS:
            frame = frames.Frame(
                sta=sta,
                chan=CHANNEL,
                start_time=now,
                sample_rate=SAMPLE_RATE,
                samples=synthetic_frame_samples().round(4).tolist(),
            )
            sig = frames.sign(frame, private_key) if private_key else None
            producer.send(RAW_TOPIC, frame.to_wire(sig))
        producer.flush()
        log.info("emitted %d frames", len(STATIONS))
        time.sleep(FRAME_SECONDS)


def run_fdsn(producer: KafkaProducer, private_key) -> None:
    try:
        from obspy import UTCDateTime
        from obspy.clients.fdsn import Client
    except ImportError:
        log.warning("ObsPy not installed; falling back to synthetic mode")
        return run_synthetic(producer, private_key)

    client = Client("IRIS")
    log.info("FDSN mode: pulling real waveforms from IRIS for %s", STATIONS)
    while True:
        end = UTCDateTime() - 300        # a few minutes of latency
        start = end - FRAME_SECONDS
        for sta in STATIONS:
            try:
                st = client.get_waveforms("IU", sta, "00", "BHZ", start, end)
                tr = st[0]
                frame = frames.Frame(
                    sta=sta, chan=tr.stats.channel,
                    start_time=tr.stats.starttime.timestamp,
                    sample_rate=float(tr.stats.sampling_rate),
                    samples=tr.data.astype(float).round(4).tolist(),
                )
                sig = frames.sign(frame, private_key) if private_key else None
                producer.send(RAW_TOPIC, frame.to_wire(sig))
            except Exception as exc:  # noqa: BLE001 - station may be unavailable
                log.warning("FDSN fetch failed for %s: %s", sta, exc)
        producer.flush()
        time.sleep(FRAME_SECONDS)


def main() -> None:
    validate_config()
    private_key = None
    if SIGN_FRAMES:
        private_key = frames.ensure_keypair(PKI_DIR)
        log.info("PKI enabled: frames signed; public key published to %s", PKI_DIR)

    producer = connect_producer()
    if FDSN_MODE:
        run_fdsn(producer, private_key)
    else:
        run_synthetic(producer, private_key)


if __name__ == "__main__":
    main()

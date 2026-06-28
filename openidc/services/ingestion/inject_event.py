"""On-demand event injector — for live demos.

Sends a single signed waveform frame containing a clear seismic transient to the
raw-waveform topic, so you can make a detection appear on command. Runs inside
the ingestion container (where the signing key and Kafka access already live):

    make inject STA=ARCES AMP=18 FREQ=4      # from the repo root
    make quiet STA=ARCES                     # noise-only frame (should NOT trigger)

or directly:

    docker compose exec ingestion python inject_event.py --sta FINES --amp 20
"""
from __future__ import annotations

import argparse
import math
import os
import re
import sys
import time

sys.path.insert(0, "/app")

from kafka import KafkaProducer

from common import frames, synthetic

STA_RE = re.compile(r"^[A-Z0-9]{1,6}$")
CHAN_RE = re.compile(r"^[A-Z0-9]{1,8}$")


def _validate_args(ap: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if not STA_RE.fullmatch(args.sta):
        ap.error("--sta must match ^[A-Z0-9]{1,6}$")
    if not CHAN_RE.fullmatch(args.chan):
        ap.error("--chan must match ^[A-Z0-9]{1,8}$")
    if not (math.isfinite(args.amp) and 0.0 <= args.amp <= 100.0):
        ap.error("--amp must be between 0 and 100")
    if not (math.isfinite(args.freq) and 0.1 <= args.freq <= 20.0):
        ap.error("--freq must be between 0.1 and 20")
    if not (math.isfinite(args.rate) and args.rate > 0):
        ap.error("--rate must be positive and finite")
    if not (math.isfinite(args.seconds) and 0 < args.seconds <= 3600):
        ap.error("--seconds must be between 0 and 3600")


def main() -> None:
    ap = argparse.ArgumentParser(description="Inject one waveform frame on demand.")
    ap.add_argument("--sta", default="ARCES", help="station code")
    ap.add_argument("--chan", default="BHZ", help="channel code")
    ap.add_argument("--amp", type=float, default=18.0, help="event amplitude (noise floor ~1.0)")
    ap.add_argument("--freq", type=float, default=4.0, help="event frequency in Hz")
    ap.add_argument("--rate", type=float, default=float(os.environ.get("SAMPLE_RATE", "40")))
    ap.add_argument("--seconds", type=float, default=float(os.environ.get("FRAME_SECONDS", "10")))
    ap.add_argument("--quiet", action="store_true", help="send a noise-only frame (no event)")
    args = ap.parse_args()
    _validate_args(ap, args)

    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    topic = os.environ.get("RAW_TOPIC", "raw.waveforms")
    pki_dir = os.environ.get("PKI_DIR", "/pki")
    sign = os.environ.get("SIGN_FRAMES", "true").lower() == "true"
    key = frames.ensure_keypair(pki_dir) if sign else None

    n = int(args.seconds * args.rate)
    samples = synthetic.noise(n)
    if not args.quiet:
        samples, onset = synthetic.add_event(samples, args.rate, args.amp, args.freq, onset_frac=0.4)

    frame = frames.Frame(
        sta=args.sta,
        chan=args.chan,
        start_time=time.time(),
        sample_rate=args.rate,
        samples=samples.round(4).tolist(),
    )
    sig = frames.sign(frame, key) if key else None

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    producer.send(topic, frame.to_wire(sig))
    producer.flush()

    kind = "quiet (noise-only)" if args.quiet else f"event (amp={args.amp}, freq={args.freq}Hz)"
    print(f"injected {kind} frame for station {args.sta} -> {topic}")


if __name__ == "__main__":
    main()

"""OpenIDC dashboard — a small FastAPI control panel for live demos.

Serves a single page that:
  * shows recent detections, polled from Postgres,
  * lets you inject an event on demand (publishes a signed frame to Kafka),
  * lets you tune the detector thresholds live (publishes to the `control`
    topic, which the station-processing service consumes and applies).

Intentionally read-mostly and dependency-light (vanilla HTML/JS, no build step)
so it runs anywhere with `docker compose up`.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time

import pika
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from kafka import KafkaProducer
from prometheus_client import make_asgi_app
from prometheus_client.core import REGISTRY, GaugeMetricFamily

sys.path.insert(0, "/app")
from common import frames, synthetic

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
RAW_TOPIC = os.environ.get("RAW_TOPIC", "raw.waveforms")
CONTROL_TOPIC = os.environ.get("CONTROL_TOPIC", "control")
SAMPLE_RATE = float(os.environ.get("SAMPLE_RATE", "40"))
FRAME_SECONDS = float(os.environ.get("FRAME_SECONDS", "10"))
SIGN_FRAMES = os.environ.get("SIGN_FRAMES", "true").lower() == "true"
PKI_DIR = os.environ.get("PKI_DIR", "/pki")
STATIONS = [s.strip() for s in os.environ.get("STATIONS", "ARCES,FINES,GERES,WRA").split(",")]
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
CONTROL_QUEUE = "control.network"

# --------------------------------------------------------------------------- #
# Input validation. Every value that reaches the database, Kafka or RabbitMQ is
# range/format checked here and rejected with HTTP 422 if it is out of bounds,
# so the control endpoints cannot be used to push malformed or hostile values
# downstream. See docs/SECURITY.md.
# --------------------------------------------------------------------------- #
STA_RE = re.compile(r"^[A-Z0-9]{1,6}$")        # CSS station codes: up to 6 alphanumerics
ALLOWED_COMMANDS = {"set_min_stations", "add_station"}


def _num(value, lo: float, hi: float, name: str) -> float:
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{name} must be a number")
    try:
        x = float(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"{name} must be a number")
    if not (lo <= x <= hi):
        raise HTTPException(status_code=422, detail=f"{name} must be between {lo} and {hi}")
    return x


def _int(value, lo: int, hi: int, name: str) -> int:
    if isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{name} must be an integer")
    try:
        x = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"{name} must be an integer")
    if not (lo <= x <= hi):
        raise HTTPException(status_code=422, detail=f"{name} must be between {lo} and {hi}")
    return x


def _sta(value) -> str:
    if not isinstance(value, str) or not STA_RE.match(value):
        raise HTTPException(status_code=422, detail="station must match ^[A-Z0-9]{1,6}$")
    return value


def _bool(value, name: str) -> bool:
    if not isinstance(value, bool):
        raise HTTPException(status_code=422, detail=f"{name} must be true or false")
    return value


HERE = os.path.dirname(__file__)
app = FastAPI(title="OpenIDC dashboard")

_producer = None
_private_key = None
_last_thresholds = {
    "on": float(os.environ.get("TRIGGER_ON", "4.0")),
    "off": float(os.environ.get("TRIGGER_OFF", "1.5")),
}
_last_network = {"min_stations": int(os.environ.get("MIN_STATIONS", "2"))}


def send_command(cmd: dict) -> None:
    """Publish a JSON control command to the RabbitMQ control queue."""
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        connection_attempts=1,
        socket_timeout=3,
        blocked_connection_timeout=3,
    )
    conn = pika.BlockingConnection(params)
    try:
        ch = conn.channel()
        ch.queue_declare(queue=CONTROL_QUEUE, durable=False)
        ch.basic_publish(exchange="", routing_key=CONTROL_QUEUE, body=json.dumps(cmd).encode())
    finally:
        conn.close()


def producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    return _producer


def private_key():
    global _private_key
    if SIGN_FRAMES and _private_key is None:
        _private_key = frames.ensure_keypair(PKI_DIR)
    return _private_key


def db():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "postgres"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "idc"),
        password=os.environ.get("PGPASSWORD", "idc"),
        dbname=os.environ.get("PGDATABASE", "openidc"),
        connect_timeout=3,
    )


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    with open(os.path.join(HERE, "index.html")) as fh:
        return fh.read()


@app.get("/api/config")
def config() -> dict:
    return {
        "stations": STATIONS,
        "sample_rate": SAMPLE_RATE,
        "thresholds": _last_thresholds,
        "network": _last_network,
    }


@app.get("/api/arrivals")
def arrivals(limit: int = 20):
    try:
        conn = db()
    except psycopg2.OperationalError:
        return JSONResponse({"rows": [], "error": "db unavailable"})
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT arid, sta, chan, iphase, round(snr::numeric,1) AS snr, "
                "to_char(onset_utc,'HH24:MI:SS') AS onset, auth "
                "FROM recent_arrivals LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"rows": rows}


@app.get("/api/stats")
def stats() -> dict:
    try:
        conn = db()
    except psycopg2.OperationalError:
        return {"total": 0, "rate_1m": 0, "per_station": [], "thresholds": _last_thresholds}
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM arrival")
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM arrival WHERE time > extract(epoch from now()) - 60"
            )
            rate = cur.fetchone()[0]
            cur.execute("SELECT sta, count(*) FROM arrival GROUP BY sta ORDER BY 2 DESC")
            per = [{"sta": r[0], "count": r[1]} for r in cur.fetchall()]
    finally:
        conn.close()
    return {"total": total, "rate_1m": rate, "per_station": per, "thresholds": _last_thresholds}


@app.get("/api/events")
def events(limit: int = 15):
    try:
        conn = db()
    except psycopg2.OperationalError:
        return JSONResponse({"rows": []})
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT orid, evid, "
                "round(lat::numeric,2) AS lat, round(lon::numeric,2) AS lon, "
                "round(depth::numeric,1) AS depth, round(mb::numeric,2) AS mb, "
                "etype, nass, to_char(to_timestamp(time),'HH24:MI:SS') AS origin_time "
                "FROM origin ORDER BY lddate DESC LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"rows": rows}


@app.post("/api/command")
def command(body: dict) -> dict:
    # Whitelist the command and rebuild it from validated fields — never forward
    # the raw request body to the broker.
    cmd = body.get("cmd")
    if cmd not in ALLOWED_COMMANDS:
        raise HTTPException(status_code=422, detail=f"unknown command; allowed: {sorted(ALLOWED_COMMANDS)}")

    if cmd == "set_min_stations":
        value = _int(body.get("value"), 1, 10, "value")
        outgoing = {"cmd": "set_min_stations", "value": value}
        _last_network["min_stations"] = value
    else:  # add_station
        outgoing = {
            "cmd": "add_station",
            "sta": _sta(body.get("sta")),
            "lat": _num(body.get("lat"), -90.0, 90.0, "lat"),
            "lon": _num(body.get("lon"), -180.0, 180.0, "lon"),
        }

    try:
        send_command(outgoing)
    except Exception as exc:  # noqa: BLE001 - surface broker errors to the UI
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=200)
    return {"ok": True, "sent": outgoing, "network": _last_network}


@app.post("/api/inject")
def inject(body: dict) -> dict:
    sta = _sta(body.get("sta", STATIONS[0]))
    amp = _num(body.get("amp", 18.0), 0.0, 100.0, "amp")
    freq = _num(body.get("freq", 4.0), 0.1, 20.0, "freq")
    quiet = _bool(body.get("quiet", False), "quiet")

    n = int(FRAME_SECONDS * SAMPLE_RATE)
    samples = synthetic.noise(n)
    if not quiet:
        samples, _ = synthetic.add_event(samples, SAMPLE_RATE, amp, freq, onset_frac=0.4)

    frame = frames.Frame(
        sta=sta, chan="BHZ", start_time=time.time(),
        sample_rate=SAMPLE_RATE, samples=samples.round(4).tolist(),
    )
    sig = frames.sign(frame, private_key()) if SIGN_FRAMES else None
    producer().send(RAW_TOPIC, frame.to_wire(sig))
    producer().flush()
    return {"ok": True, "injected": "quiet" if quiet else "event", "sta": sta}


@app.post("/api/threshold")
def threshold(body: dict) -> dict:
    on = _num(body.get("on", _last_thresholds["on"]), 0.1, 50.0, "on")
    off = _num(body.get("off", _last_thresholds["off"]), 0.1, 50.0, "off")
    if off >= on:
        raise HTTPException(status_code=422, detail="trigger_off must be less than trigger_on")
    producer().send(CONTROL_TOPIC, json.dumps({"trigger_on": on, "trigger_off": off}).encode())
    producer().flush()
    _last_thresholds.update(on=on, off=off)
    return {"ok": True, "thresholds": _last_thresholds}


# --------------------------------------------------------------------------- #
# Prometheus metrics. A custom collector queries Postgres on each scrape and
# exposes pipeline State-of-Health gauges at /metrics, which Prometheus scrapes
# and Grafana visualises. This is the "operate and monitor the software modules"
# duty from the vacancy.
# --------------------------------------------------------------------------- #
class PipelineCollector:
    def collect(self):
        up = GaugeMetricFamily("openidc_up", "1 if the results database is reachable")
        try:
            conn = db()
        except Exception:  # noqa: BLE001 - DB may be down; report up=0
            up.add_metric([], 0.0)
            yield up
            return
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM arrival")
                total = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM arrival WHERE time > extract(epoch from now()) - 60")
                rate = cur.fetchone()[0]
                cur.execute("SELECT sta, count(*) FROM arrival GROUP BY sta")
                per_station = cur.fetchall()
                cur.execute("SELECT count(*) FROM origin")
                events = cur.fetchone()[0]
                cur.execute("SELECT etype, count(*) FROM origin GROUP BY etype")
                screened = cur.fetchall()
        finally:
            conn.close()

        up.add_metric([], 1.0)
        yield up

        g = GaugeMetricFamily("openidc_arrivals_total", "Total detections (arrivals)")
        g.add_metric([], total)
        yield g

        g = GaugeMetricFamily("openidc_arrivals_rate_1m", "Detections in the last 60 seconds")
        g.add_metric([], rate)
        yield g

        g = GaugeMetricFamily("openidc_events_total", "Total located+screened events")
        g.add_metric([], events)
        yield g

        ps = GaugeMetricFamily("openidc_arrivals_per_station", "Detections per station", labels=["sta"])
        for sta, count in per_station:
            ps.add_metric([sta], count)
        yield ps

        es = GaugeMetricFamily("openidc_events_screened", "Events by screening type", labels=["etype"])
        for etype, count in screened:
            es.add_metric([etype or "unknown"], count)
        yield es


REGISTRY.register(PipelineCollector())
app.mount("/metrics", make_asgi_app())

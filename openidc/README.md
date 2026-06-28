# OpenIDC — a miniature CTBTO IDC automatic processing pipeline

> A working, event-driven seismic **detection** pipeline that mirrors the
> architecture of the CTBTO International Data Centre (IDC) Automatic Processing
> system: continuous waveform frames are streamed over **Apache Kafka**, each
> station's data is processed by an **STA/LTA detector** (C kernel called from
> Python), and detections (*arrivals*) are written into a **PostgreSQL** database
> using the real **CSS 3.0** schema used by the IDC.

This project was built to demonstrate the engineering skills required for the
**Processing Engineer (P-3)** role in the *Configuration and Data Processing
Unit, Automatic Processing Systems Section*. It is a deliberately small but
**end-to-end runnable** mirror of how the IDC ingests data from the global
sensor network, runs automatic station processing, and stores results for
analyst review and event screening.

```
 ┌───────────┐  Kafka         ┌──────────────────┐  Kafka       ┌──────────────────────┐
 │ Ingestion │  raw.waveforms │ Station Proc.    │  detections  │ Network Proc. (Java) │
 │ (Python)  │ ─────────────► │ STA/LTA (C via   │ ───────────► │ associate→locate→    │
 │ PKI-signed│                │ ctypes) → arrival│              │ screen (eq vs ex)    │
 └───────────┘                └────────┬─────────┘              └──────────┬───────────┘
                                       ▼                                    ▼
                Postgres (CSS 3.0): wfdisc, arrival, origin, assoc, event
  RabbitMQ control plane · Kafka control topic · FastAPI dashboard (http://localhost:8000)
```

## Why this mirrors the real system

| Real CTBTO IDC | OpenIDC (this repo) |
|---|---|
| Stations send continuous data in **CD-1.1** format, cryptographically signed | Ingestion emits JSON waveform frames, each **RSA-signed** (PKI); the processor verifies them |
| **Station processing** detects signals automatically (STA/LTA, DFX) | C STA/LTA detector kernel, called from the Python processing service |
| Results stored in the **CSS 3.0** relational schema | `db/schema.sql` creates `wfdisc`, `arrival`, `origin`, `assoc`, `event` |
| Pipeline moving toward a **modern streaming architecture** (SHI Re-Engineering Phase 3) | Stages decoupled over **Apache Kafka** topics; **RabbitMQ** control plane |
| Network association → location → **event screening** | **Java** network-processing service: associate → locate → screen (earthquake vs explosion-like) |

## Quick start (one command)

```bash
make up        # build + start Kafka, Postgres, ingestion, station-processing
make logs      # watch detections being written
make psql      # open a SQL shell and: SELECT sta, time, iphase, snr FROM arrival ORDER BY time DESC LIMIT 20;
make down      # stop everything
```

By default the ingestion service generates **synthetic** waveforms with injected
seismic transients (no network needed), so the detector reliably fires and you
can watch arrivals appear in the database. Set `FDSN_MODE=true` to pull **real**
public waveform data instead (see `docs/RUNBOOK.md`).

## Interactive demo

Two ways to drive the pipeline live (ideal for an interview screen-share):

**Web control panel** — open **http://localhost:8000** after `make up`:
- inject a seismic event for any station with one click and watch the detection appear,
- inject a noise-only frame to show it correctly does *not* trigger,
- drag the **trigger threshold** sliders to re-tune the detector *live* — the
  change is sent over a Kafka `control` topic and the running processor applies
  it immediately (a miniature control plane), so you can visibly change the
  detection rate while it runs.

The dashboard also shows the **Events & screening** table (network-processing
output) and a **Network processing** panel that sends commands over **RabbitMQ**
— set the minimum stations for association, or configure a brand-new station.

**Command line:**
```bash
make inject STA=FINES AMP=20 FREQ=4   # inject one event on demand
make quiet  STA=ARCES                 # inject a noise-only frame (should NOT trigger)
make boom   STA=ARCES                 # high-amplitude event -> screens as explosion-like
make watch                            # live-refreshing table of recent detections
make events                           # located + screened events (Phase 2)
make min-stations N=3                 # change association sensitivity (via RabbitMQ)
make station-add NAME=NEWS LAT=10 LON=20   # configure a new station (via RabbitMQ)
make rabbitmq                         # RabbitMQ management UI URL (guest/guest)
```

## What this demonstrates (mapped to the vacancy)

- **Apache Kafka + RabbitMQ, event-driven distributed pipeline** — stages decoupled over Kafka topics; a RabbitMQ control plane for operational commands.
- **Python, C, Java, SQL, bash** — Python services, a C detector kernel, a Java network-processing service, the CSS 3.0 schema, shell glue.
- **PostgreSQL + database interfaces** — real CSS 3.0 schema; Python writes arrivals via `psycopg2`, Java writes origins/events via JDBC.
- **Event screening** — earthquake vs. explosion-like classification (depth + mb), the core CTBT verification question.
- **Containers** — every service is Dockerised; the whole system runs via `docker-compose`.
- **PKI / security** — frames are RSA-signed and verified (mirrors signed CD-1.1 data); the dashboard API **validates and whitelists** all input and **escapes output** (XSS). Full threat model in `docs/SECURITY.md`.
- **Software testing** — `pytest` unit tests for the detector (`make test`).
- **Seismological data processing** — STA/LTA detection, network association, location, the IDC processing chain.
- **Monitoring / ITIL** — Prometheus scrapes pipeline metrics; a provisioned **Grafana** State-of-Health dashboard with alert rules (`make grafana`).
- **Perl** — a `proc2css`-style CSS flat-file export tool (`make css-export`).
- **Kubernetes** — hardened app-tier manifests in `k8s/`, with stateful infra delegated to operators.
- **Git, documentation** — clean history, architecture notes, an operational runbook, and a security threat model in `docs/`.

All three roadmap phases are built; see `docs/ROADMAP.md` for the full
requirement-to-phase mapping.

## Repository layout

```
openidc/
├── docker-compose.yml          # Kafka (KRaft) + Postgres + services
├── Makefile                    # up / down / logs / test / psql
├── db/schema.sql               # CSS 3.0 schema (wfdisc, arrival, origin, assoc, event)
├── common/                     # shared: frame model + PKI signing, synthetic waveforms
├── services/
│   ├── ingestion/              # synthetic/FDSN waveform producer (+ on-demand injector)
│   ├── station-processing/     # STA/LTA detector + arrival writer (C + Python)
│   ├── network-processing/     # Java: associate → locate → screen (Kafka + JDBC + RabbitMQ)
│   └── dashboard/              # FastAPI control panel + Prometheus /metrics
├── monitoring/                 # Prometheus config + alerts + Grafana provisioning
├── tools/                      # Perl CSS flat-file export (proc2css-style)
├── k8s/                        # hardened Kubernetes manifests (app tier)
├── scripts/watch.sh            # live detections viewer (make watch)
├── tests/                      # pytest unit tests
└── docs/                       # ARCHITECTURE, RUNBOOK, ROADMAP, SECURITY
```

> **Note on fidelity:** real CD-1.1 / IMS2.0 data is access-controlled, so this
> repo uses synthetic and public FDSN waveforms and *models* the CD-1.1 signing
> concept rather than implementing the wire protocol. Demonstrating awareness of
> the real formats is intentional.

#!/usr/bin/env bash
# Live-refreshing view of the most recent detections. Ctrl-C to stop.
# Used by `make watch`.
set -euo pipefail

while true; do
  clear
  echo "OpenIDC — recent detections (refreshing every 2s, Ctrl-C to stop)"
  echo
  docker compose exec -T postgres psql -U idc -d openidc -c \
    "SELECT arid, sta, chan, round(snr::numeric,1) AS snr, \
            to_char(onset_utc,'HH24:MI:SS') AS onset_utc, auth \
     FROM recent_arrivals LIMIT 15;"
  sleep 2
done

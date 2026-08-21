#!/usr/bin/env bash
set -euo pipefail

cd /home/sebastian/projects/market-data-warehouse/docker/ib-gateway

# Recreate the Docker IB Gateway from the current safe config:
#   TRADING_MODE=paper
#   READ_ONLY_API=no   # write-enabled API, still paper trading
#   TWOFA_TIMEOUT_ACTION=exit
#   RELOGIN_AFTER_TWOFA_TIMEOUT=no
#   Paper API host port: 127.0.0.1:4002

echo "Stopping/recreating IB Gateway from docker/ib-gateway..."
sudo docker compose down
sudo docker compose up -d --force-recreate

echo "Waiting for container to expose paper API port 4002..."
for i in {1..60}; do
  if nc -z 127.0.0.1 4002 >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [ "$i" -eq 60 ]; then
    echo "ERROR: 127.0.0.1:4002 did not open within 120s" >&2
    sudo docker compose ps >&2 || true
    exit 1
  fi
done

printf '\nRendered gateway env in container:\n'
safe_env="$(sudo docker compose exec -T ib-gateway env | grep -E '^(TRADING_MODE|READ_ONLY_API|TWOFA_TIMEOUT_ACTION|RELOGIN_AFTER_TWOFA_TIMEOUT)=' | sort)"
printf '%s\n' "$safe_env"

printf '%s\n' "$safe_env" | grep -qx 'READ_ONLY_API=no'
printf '%s\n' "$safe_env" | grep -qx 'RELOGIN_AFTER_TWOFA_TIMEOUT=no'
printf '%s\n' "$safe_env" | grep -qx 'TRADING_MODE=paper'
printf '%s\n' "$safe_env" | grep -qx 'TWOFA_TIMEOUT_ACTION=exit'

echo "Paper/write-enabled gateway config verified."

# Re-enable the paper-only trend-engine runner only after verification succeeded.
systemctl --user enable --now trend-engine-hourly-runner.service

printf '\nCurrent containers:\n'
sudo docker compose ps
printf '\nTrend runner status:\n'
systemctl --user --no-pager -l status trend-engine-hourly-runner.service | head -25

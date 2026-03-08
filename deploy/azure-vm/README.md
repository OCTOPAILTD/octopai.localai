# Azure VM Rollout Guide

## 1) Install Service Files

- Copy unit files from `deploy/azure-vm/systemd/` to `/etc/systemd/system/`.
- Reload and enable:
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable python-sql-model`
  - `sudo systemctl enable python-sql-repair-model`
  - `sudo systemctl enable python-sql-parser`

## 2) Start In Order

- `sudo systemctl start python-sql-model`
- `sudo systemctl start python-sql-repair-model`
- `sudo systemctl start python-sql-parser`

## 3) Health Checks

- Parser health: `curl http://127.0.0.1:8080/health`
- Model health:
  - `curl http://127.0.0.1:8000/v1/models`
  - `curl http://127.0.0.1:8001/v1/models`

## 4) Shadow -> Canary -> Full

- Shadow: send mirrored requests, do not consume outputs.
- Canary: route 5%-10% traffic to parser service.
- Full: ramp from 25% -> 50% -> 100% after SLO checks.

## 5) Suggested Alerts

- Parser `5xx` rate > 2%
- Empty SQL outputs > 5%
- Validation failures > 10%
- p95 latency over agreed threshold


# Docker Setup

Docker is **already installed** on this machine — verified by the audit:
```
Docker version 29.3.0, build 5927d80
Docker Compose version v5.1.0
```

## Quick start

```powershell
# From project root
docker compose build       # one-time build of api + frontend images
docker compose up -d       # start everything in detached mode
docker compose ps          # see what's running
docker compose logs -f scheduler   # tail one service
docker compose down        # stop everything
```

## Services

| Service | Port | Description |
|---|---|---|
| `api` | 8001 | FastAPI backend (`uvicorn trading_platform.api.main:app`) |
| `frontend` | 5173 | Vite dev server for the React GUI |
| `scheduler` | — | `scripts/task_scheduler.py` — runs cron-like tasks |
| `watchdog` | — | `scripts/health_watchdog.py` — health probes + Telegram alerts |

The DB at `data/polymarket/wallet_intelligence.db`, `.env`, and the
`scripts/` directory are bind-mounted into every container so changes
on the host are immediately visible.

## Verification

After `docker compose up -d`:

```powershell
# All four containers should be 'running'
docker compose ps

# API health
curl http://localhost:8001/api/system/status

# Scheduler state (should populate within ~30s)
curl http://localhost:8001/api/system/scheduler-status

# Frontend
start http://localhost:5173
```

## Fallback: PowerShell launcher (no Docker)

If you prefer to run without Docker, use the PowerShell scripts:

```powershell
.\scripts\run_all.ps1     # start everything as background jobs
.\scripts\status.ps1      # one-shot status check
.\scripts\stop_all.ps1    # stop everything
```

The PowerShell launcher uses the local `.venv` Python and `npm run dev`
directly. It auto-restarts crashed jobs every 30 seconds. Note that
the `WATCHDOG_API_URL` is set to `http://localhost:8001` (not the
Docker network alias `http://api:8001`).

## Notes

- **pmxt sidecar**: pmxt 2.26.x is a Python package, not a separate
  Node sidecar. The `api`, `scheduler`, and `watchdog` containers
  import it directly. No extra service needed.
- **`POLYMARKET_LIVE_ENABLED`**: This is intentionally NOT set in the
  Docker compose file. To go live you must edit `.env` and add the
  variable yourself, then `docker compose restart api scheduler`.
- **State persistence**: scheduler state lives in
  `data/scheduler/state.json`, watchdog state is in-memory only.

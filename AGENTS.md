# AGENTS.md

Repo-specific guidance for OpenCode sessions working on SimCompare (同传对比调试台): a React/Vite frontend + FastAPI backend that drives one or two gRPC simultaneous-translation services and compares their chunked output on a timeline.

## Critical repo conventions (do not get this wrong)

- **`node_modules/` is committed on purpose.** Windows-only team, avoids `npm install`. If you run `npm install`, do **not** commit the resulting changes — restore with `git checkout -- node_modules/`.
- **`dist/` is committed on purpose.** Production deploys via `git pull`, not a build step. After changing frontend code, run `npm run build` and commit the new `dist/` artifacts. The backend serves `dist/index.html` + `/assets` (see `server/main.py:720-741`), so a stale `dist/` means a stale prod UI.
- **No tests, lint, typecheck, or CI exist.** There is no verification command to run. Verify changes manually by booting backend + frontend (or use `scripts/grpc_probe.py` for gRPC connectivity).

## Commands

Frontend (`package.json`):
- `npm run dev` — Vite dev server on :5173. Pass `-- --host 127.0.0.1 --port 5173` to match backend CORS.
- `npm run build` — outputs to `dist/` (must be committed for prod).
- `npm run preview` — serve built `dist/`.

Backend (run from repo root, never from inside `server/`):
- `python -m uvicorn server.main:app --host 127.0.0.1 --port 8000` — dev/local
- `python3 -m uvicorn server.main:app --host 0.0.0.0 --port 8000` — Linux deploy
- Backend reads `ROOT_DIR` relative to `server/config.py`, so the working directory matters.

gRPC connectivity probe (faster than the web UI for isolating issues):
- `python scripts/grpc_probe.py --endpoint <ip:port> --audio <file.wav> --lang en|zh`
- `python scripts/grpc_probe.py --endpoint <ip:port> --silence-seconds 3 --lang en` — no audio file needed.

## Dev vs prod wiring

- **Dev = two processes.** Frontend (:5173) calls backend (:8000). There is **no Vite proxy**; instead `src/main.jsx:12` hardcodes: if `window.location.port === '5173'`, `API_BASE` = `hostname:8000`, else same origin. Override with `VITE_API_BASE`.
- **Backend CORS** (`server/main.py:43`) allows only `http://localhost:5173` and `http://127.0.0.1:5173`. Using any other dev host/port will be blocked — update the allowlist if you change the dev origin.
- **Prod = backend alone.** The FastAPI app mounts `dist/assets` and has an SPA catch-all at `/{full_path:path}` serving `dist/index.html`. `dist/` missing → index returns 404.

## Dependencies — two requirements files, trust `server/`

- Use `server/requirements.txt` (pinned: fastapi, uvicorn, python-multipart, grpcio). README installs from here.
- Root `requirements.txt` is **stale**: unpinned, and lists `paramiko` + `protobuf` — `paramiko` is not imported anywhere in `server/` or `scripts/`. Don't add it back without a real consumer.
- `protobuf` is needed only because the generated stubs in `server/grpc_info/` import it.

## Config

- `simcompare.config.json` at repo root is loaded at backend startup by `server/config.py`. Override the path with the `SIMCOMPARE_CONFIG` env var.
- Config is mutable at runtime via `PUT /api/config` — `save_config()` does an atomic temp-file + `os.replace` write, then reloads the in-memory `CONFIG` dict. Modules that imported `CONFIG` keep seeing updates because the dict is mutated in place.
- Top-level keys: `services` (`left`/`right`: `label`, `grpc_url`, `debug_log`, `debug_root`), `storage` (`upload_dir`), `runtime` (`debug_log_max_bytes`, `debug_context_before`, `debug_context_after`). Internal `_path`/`_loaded` keys are stripped on write.

## gRPC

- Bidirectional streaming on `/AsrService/createRec`, `grpc.insecure_channel` — **no TLS, token, or metadata**. If a service needs auth, patch `server/grpc_runner.py`.
- Generated protobuf stubs live in `server/grpc_info/` (`asr_pb2.py`, `asr_pb2_grpc.py`) and are committed. Imports are local (`from .grpc_info import asr_pb2_grpc`), so do not move that package.
- Direction mapping: `zh2en → lang="zh"`, `en2zh → lang="en"`.
- Audio is sent in 0.4s chunks (12800 bytes for 16kHz/mono/16-bit PCM). gRPC deadline = `max(60s, duration*2.5 + 30s)`.
- Per-run results accumulate in-memory in `RUNS[run_id]["left"|"right"]`; the frontend polls `/api/runs/{run_id}/chunks` for incremental timeline updates. State is process-local and lost on restart.

## Audio input constraints

- Preferred: **16kHz / mono / 16-bit WAV**. This is the only fully reliable path.
- MP3 is auto-converted via `ffmpeg`; if `ffmpeg` is absent the upload fails. Install ffmpeg or pre-convert.
- MP4/MOV are rejected — extract audio first.
- `*.wav`/`*.mp3` etc. are gitignored (user uploads), so do not commit test audio.

## Layout notes

- `src/main.jsx` (~880 lines) and `src/styles.css` are the **entire frontend** — single-file React app, no component split, no router. `index.html` is the Vite entry.
- `server/main.py` (~740 lines) is the entire backend (routes, run state, debug-log parsing, audio chunking). `server/grpc_runner.py` is the real gRPC adapter; `server/config.py` is config I/O.
- `mockup/` and `public/` hold design mockups and standalone demo HTML/CSS — not part of the Vite build input (Vite serves `index.html` → `src/main.jsx`).
- `scripts/` is standalone CLI tooling (only `grpc_probe.py`); not imported by the server.

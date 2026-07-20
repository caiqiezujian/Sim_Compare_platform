"""FastAPI entrypoint for the SimCompare debug platform.

The endpoint intentionally keeps the transport contract small. Drop the generated
asr_pb2.py/asr_pb2_grpc.py files next to this module to enable the real runner.
"""
import asyncio
import json
import logging
import os
import secrets
import shutil
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
DIST_DIR = ROOT_DIR / "dist"
RUNS: Dict[str, dict] = {}
UPLOADS: Dict[str, dict] = {}
logger = logging.getLogger("simcompare")

app = FastAPI(title="SimCompare API", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], allow_methods=["*"], allow_headers=["*"])


def demo_chunks():
    rows = [
        (0, 3280, "各位观众大家好，欢迎来到今天的节目。", "Hello everyone, welcome to today’s program."),
        (3280, 5190, "今天我们来聊一个有趣的话题。", "Today we are going to talk about an interesting topic."),
        (5190, 6450, "应该是到了。", "I think we’ve reached it."),
        (6450, 9360, "从这里开始，两个系统的结果出现了差异。", "From here, the two systems begin to diverge."),
        (9360, 12120, "我们可以逐句查看它们的表现。", "We can review their performance sentence by sentence."),
        (12120, 14600, "这条结果正在等待服务返回。", "Waiting for the service to return this result."),
    ]
    return [{"id": f"chunk-{i + 1:02d}", "start": start, "end": end, "asr": asr, "mt": mt, "status": "done" if i < 5 else "pending", "audio": f"chunk-{i + 1:02d}.wav", "logs": [f"audio stream {start / 1000:.2f}s — {end / 1000:.2f}s", "ASR final · hold_n=1", "MT final · latency 964ms"]} for i, (start, end, asr, mt) in enumerate(rows)]


def error_chunk(side: str, endpoint: str, message: str):
    return [{
        "id": f"{side}-grpc-error",
        "start": 0,
        "end": 0,
        "asr": f"{side.upper()} gRPC 连接失败",
        "mt": message,
        "status": "failed",
        "audio": "",
        "logs": [
            f"endpoint={endpoint}",
            message,
        ],
    }]


async def process_run(run_id: str, video_path: str, systems: list, direction: str):
    run = RUNS[run_id]
    run["status"] = "running"
    try:
        def should_stop():
            return bool(run.get("cancelled"))

        real_grpc = os.getenv("SIMCOMPARE_REAL_GRPC", "0") == "1"
        if real_grpc and systems:
            from .grpc_runner import run_grpc
            lang = "en" if direction == "en2zh" else "zh"
            active_sides = ["left", "right"] if len(systems) > 1 else ["left"]
            run["stage"] = "starting"
            run["progress"] = 1

            def update_side(side: str, rows: list):
                if should_stop():
                    return
                run[side] = rows
                run["completed_chunks"] = max(len(run.get("left", [])), len(run.get("right", [])))

            def mark_stream_started():
                if not run.get("stream_started"):
                    run["stream_started"] = True
                    run["stream_started_at"] = time.time()
                    run["stage"] = "streaming"
                    run["progress"] = max(run.get("progress", 1), 1)

            def update_audio_progress(side: str, sent_ms: int, total_ms: int):
                if should_stop():
                    return
                side_progress = run.setdefault("audio_progress", {})
                side_progress[side] = {"sent_ms": sent_ms, "total_ms": total_ms}
                available = [side_progress[name] for name in active_sides if name in side_progress]
                if not available:
                    return
                total = max((item.get("total_ms") or 0) for item in available)
                sent = min((item.get("sent_ms") or 0) for item in available)
                run["audio_sent_ms"] = sent
                run["audio_total_ms"] = total
                run["stage"] = "waiting_results" if total > 0 and sent >= total else "streaming"
                if total > 0:
                    run["progress"] = min(95, max(1, round(sent / total * 95)))

            def run_side(side: str, endpoint: str):
                try:
                    return run_grpc(
                        video_path,
                        endpoint,
                        lang,
                        None,
                        lambda rows: update_side(side, rows),
                        should_stop,
                        mark_stream_started,
                        lambda sent_ms, total_ms: update_audio_progress(side, sent_ms, total_ms),
                    )
                except Exception as exc:
                    message = str(exc)
                    rows = error_chunk(side, endpoint, message)
                    update_side(side, rows)
                    run[f"{side}_error"] = message
                    return rows

            left_task = asyncio.to_thread(
                run_side,
                "left",
                systems[0].get("url", ""),
            )
            right_task = asyncio.to_thread(
                run_side,
                "right",
                systems[1].get("url", ""),
            ) if len(systems) > 1 else None
            if right_task:
                left, right = await asyncio.gather(left_task, right_task)
            else:
                left, right = await left_task, []
            if should_stop():
                run["status"] = "cancelled"
                run["stage"] = "cancelled"
                run["left"], run["right"] = left, right
                return
            run["left"], run["right"] = left, right
            run["progress"] = 100
            run["completed_chunks"] = max(len(left), len(right))
            if run.get("left_error") or run.get("right_error"):
                errors = [run.get("left_error"), run.get("right_error")]
                run["error"] = " | ".join(error for error in errors if error)
                run["status"] = "failed" if (run.get("left_error") and (not right_task or run.get("right_error"))) else "partial_completed"
                run["stage"] = run["status"]
            else:
                run["status"] = "completed"
                run["stage"] = "completed"
            return

        # Safe local fallback while generated protobuf modules or real mode are absent.
        for index in range(1, 7):
            if should_stop():
                run["status"] = "cancelled"
                run["stage"] = "cancelled"
                return
            await asyncio.sleep(0.32)
            run["progress"] = round(index / 6 * 100)
            run["completed_chunks"] = index
        run["status"] = "completed"
        run["stage"] = "completed"
        run["systems"] = systems
        run["direction"] = direction
        run["left"] = demo_chunks()
        canary = demo_chunks()
        canary[3]["asr"] = "从这里开始，两个系统的输出有一点不同。"
        canary[3]["mt"] = "From this point, the outputs from the systems are slightly different."
        run["right"] = canary
    except Exception as exc:
        if run.get("cancelled"):
            run["status"] = "cancelled"
            run["stage"] = "cancelled"
        else:
            run["status"] = "failed"
            run["stage"] = "failed"
            run["error"] = str(exc)
            logger.exception("run %s failed", run_id)
    finally:
        if os.path.exists(video_path):
            os.remove(video_path)


@app.get("/api/health")
async def health():
    return {"ok": True, "service": "simcompare-api", "grpc_adapter": (APP_DIR / "grpc_info" / "asr_pb2_grpc.py").exists()}


@app.post("/api/uploads")
async def create_upload(video: UploadFile = File(...)):
    upload_id = f"upload_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}"
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(video.filename or ".wav").suffix)
    with temp as output:
        shutil.copyfileobj(video.file, output)
    UPLOADS[upload_id] = {"upload_id": upload_id, "path": temp.name, "filename": video.filename or Path(temp.name).name, "created_at": time.time()}
    return {"upload_id": upload_id, "filename": UPLOADS[upload_id]["filename"]}


@app.post("/api/runs")
async def create_run(background_tasks: BackgroundTasks, video: Optional[UploadFile] = File(None), upload_id: str = Form(""), systems: str = Form("[]"), direction: str = Form("zh2en")):
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}"
    upload = UPLOADS.pop(upload_id, None) if upload_id else None
    if upload:
        video_path = upload["path"]
    elif video:
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(video.filename or ".mp4").suffix)
        with temp as output:
            shutil.copyfileobj(video.file, output)
        video_path = temp.name
    else:
        return JSONResponse(status_code=400, content={"detail": "video or upload_id is required"})
    try:
        parsed_systems = json.loads(systems)
    except json.JSONDecodeError:
        parsed_systems = []
    direction = direction if direction in {"zh2en", "en2zh"} else "zh2en"
    RUNS[run_id] = {"run_id": run_id, "status": "queued", "stage": "queued", "progress": 0, "completed_chunks": 0, "audio_sent_ms": 0, "audio_total_ms": 0, "audio_progress": {}, "systems": parsed_systems, "direction": direction, "cancelled": False, "stream_started": False}
    background_tasks.add_task(process_run, run_id, video_path, parsed_systems, direction)
    return {"run_id": run_id, "status": "queued"}


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    return run


@app.post("/api/runs/{run_id}/cancel")
async def cancel_run(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    run["cancelled"] = True
    if run.get("status") in {"queued", "running"}:
        run["status"] = "cancelling"
    return {"run_id": run_id, "status": run.get("status"), "cancelled": True}


@app.get("/api/runs/{run_id}/chunks")
async def get_chunks(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    return {"left": run.get("left", []), "right": run.get("right", [])}


if (DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


@app.get("/", include_in_schema=False)
async def serve_frontend_index():
    index_file = DIST_DIR / "index.html"
    if not index_file.exists():
        return JSONResponse(status_code=404, content={"detail": "dist/index.html not found"})
    return FileResponse(index_file)


@app.get("/{full_path:path}", include_in_schema=False)
async def serve_frontend_spa(full_path: str):
    if full_path.startswith("api/"):
        return JSONResponse(status_code=404, content={"detail": "api route not found"})
    requested_file = DIST_DIR / full_path
    if requested_file.is_file():
        return FileResponse(requested_file)
    index_file = DIST_DIR / "index.html"
    if not index_file.exists():
        return JSONResponse(status_code=404, content={"detail": "dist/index.html not found"})
    return FileResponse(index_file)

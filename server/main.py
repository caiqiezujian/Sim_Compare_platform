"""FastAPI entrypoint for the SimCompare debug platform.

The endpoint intentionally keeps the transport contract small. Drop the generated
asr_pb2.py/asr_pb2_grpc.py files next to this module to enable the real runner.
"""
import asyncio
import json
import os
import secrets
import shutil
import tempfile
import time
from pathlib import Path
from typing import Dict

from fastapi import BackgroundTasks, FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

APP_DIR = Path(__file__).resolve().parent
RUNS: Dict[str, dict] = {}

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


async def process_run(run_id: str, video_path: str, systems: list, direction: str):
    run = RUNS[run_id]
    run["status"] = "running"
    try:
        real_grpc = os.getenv("SIMCOMPARE_REAL_GRPC", "0") == "1"
        if real_grpc and systems:
            from .grpc_runner import run_grpc
            lang = "en" if direction == "en2zh" else "zh"
            left = await asyncio.to_thread(run_grpc, video_path, systems[0].get("url", ""), lang)
            right = await asyncio.to_thread(run_grpc, video_path, systems[1].get("url", ""), lang) if len(systems) > 1 else left
            run["progress"] = 100
            run["completed_chunks"] = max(len(left), len(right))
            run["status"] = "completed"
            run["left"], run["right"] = left, right
            return

        # Safe local fallback while generated protobuf modules or real mode are absent.
        for index in range(1, 7):
            await asyncio.sleep(0.32)
            run["progress"] = round(index / 6 * 100)
            run["completed_chunks"] = index
        run["status"] = "completed"
        run["systems"] = systems
        run["direction"] = direction
        run["left"] = demo_chunks()
        canary = demo_chunks()
        canary[3]["asr"] = "从这里开始，两个系统的输出有一点不同。"
        canary[3]["mt"] = "From this point, the outputs from the systems are slightly different."
        run["right"] = canary
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = str(exc)
    finally:
        if os.path.exists(video_path):
            os.remove(video_path)


@app.get("/api/health")
async def health():
    return {"ok": True, "service": "simcompare-api", "grpc_adapter": (APP_DIR / "grpc_info" / "asr_pb2_grpc.py").exists()}


@app.post("/api/runs")
async def create_run(background_tasks: BackgroundTasks, video: UploadFile = File(...), systems: str = Form("[]"), direction: str = Form("zh2en")):
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}"
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(video.filename or ".mp4").suffix)
    with temp as output:
        shutil.copyfileobj(video.file, output)
    try:
        parsed_systems = json.loads(systems)
    except json.JSONDecodeError:
        parsed_systems = []
    direction = direction if direction in {"zh2en", "en2zh"} else "zh2en"
    RUNS[run_id] = {"run_id": run_id, "status": "queued", "progress": 0, "completed_chunks": 0, "systems": parsed_systems, "direction": direction}
    background_tasks.add_task(process_run, run_id, temp.name, parsed_systems, direction)
    return {"run_id": run_id, "status": "queued"}


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    return run


@app.get("/api/runs/{run_id}/chunks")
async def get_chunks(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    return {"left": run.get("left", []), "right": run.get("right", [])}

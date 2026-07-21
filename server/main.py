"""FastAPI entrypoint for the SimCompare debug platform.

The endpoint intentionally keeps the transport contract small. Drop the generated
asr_pb2.py/asr_pb2_grpc.py files next to this module to enable the real runner.
"""
import asyncio
import ast
import json
import logging
import os
import re
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

from .config import config_loaded, config_path, runtime_config, service_config, storage_config

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


def system_endpoint(system: dict):
    return (system.get("url") or system.get("grpc_url") or system.get("grpcUrl") or system.get("endpoint") or "").strip()


def debug_root_for_side(run: dict, side: str):
    side = side.lower()
    index = 0 if side == "left" else 1 if side == "right" else None
    if index is None:
        return None
    systems = run.get("systems") or []
    system = systems[index] if index < len(systems) else {}
    configured = system.get("debug_root") or system.get("debugRoot")
    env_name = "SIMCOMPARE_DEBUG_LEFT_ROOT" if side == "left" else "SIMCOMPARE_DEBUG_RIGHT_ROOT"
    service_root = service_config(side).get("debug_root") or service_config(side).get("debugRoot")
    root = configured or os.getenv(env_name) or os.getenv("SIMCOMPARE_DEBUG_ROOT") or service_root
    return Path(root).expanduser() if root else None


def debug_log_for_side(run: dict, side: str):
    side = side.lower()
    index = 0 if side == "left" else 1 if side == "right" else None
    if index is None:
        return None
    systems = run.get("systems") or []
    system = systems[index] if index < len(systems) else {}
    configured = system.get("debug_log") or system.get("debugLog")
    env_name = "SIMCOMPARE_DEBUG_LEFT_LOG" if side == "left" else "SIMCOMPARE_DEBUG_RIGHT_LOG"
    service_log = service_config(side).get("debug_log") or service_config(side).get("debugLog")
    log_path = configured or os.getenv(env_name) or os.getenv("SIMCOMPARE_DEBUG_LOG") or service_log
    return Path(log_path).expanduser() if log_path else None


def find_chunk(run: dict, side: str, chunk_id: str):
    for item in run.get(side, []) or []:
        candidates = {str(item.get("chunk_id", "")), str(item.get("id", "")), str(item.get("sn", ""))}
        if str(chunk_id) in candidates:
            return item
    return None


def create_upload_temp_file(filename: str, default_suffix: str):
    suffix = Path(filename or default_suffix).suffix or default_suffix
    upload_dir = os.getenv("SIMCOMPARE_UPLOAD_DIR") or storage_config().get("upload_dir")
    if upload_dir:
        directory = Path(upload_dir).expanduser()
        directory.mkdir(parents=True, exist_ok=True)
        return tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=directory)
    return tempfile.NamedTemporaryFile(delete=False, suffix=suffix)


def is_supported_audio(filename: str):
    return Path(filename or "").suffix.lower() in {".wav", ".wave", ".mp3"}


def split_log_records(text: str):
    records = []
    pattern = re.compile(r"(?=\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\s+\|)")
    for record in pattern.split(text):
        record = record.strip()
        if record:
            records.append(record)
    return records


def log_message(record: str):
    parts = record.split(" | ", 6)
    return parts[-1] if len(parts) >= 7 else record


def parse_float(pattern: str, text: str):
    match = re.search(pattern, text)
    return float(match.group(1)) if match else None


def parse_int(pattern: str, text: str):
    match = re.search(pattern, text)
    return int(match.group(1)) if match else None


def parse_words_from_log(message: str):
    match = re.search(r"WORD_TIMESTAMPS:\|(.+?)\|?$", message)
    if not match:
        return []
    try:
        return ast.literal_eval(match.group(1))
    except (SyntaxError, ValueError):
        return []


def parse_asr_from_log(message: str):
    if "ASR:|" not in message:
        return {}
    fields = message.split("|")
    # Current service log shape:
    # ASR:|src_text|part_or_partial|tgt_text|part_2_mt|bg_or_offset|lang|is_end|
    result = {}
    if len(fields) >= 8:
        result["src_text"] = fields[1]
        result["part_2_mt"] = fields[2]
        result["tgt_text"] = fields[3]
        result["offset"] = fields[-4]
        result["lang"] = fields[-3]
        result["is_end"] = fields[-2].lower() == "true"
    return result


def parse_debug_log(log_path: Path, conference_id: str, chunk_id: str):
    if not log_path or not log_path.exists():
        return None
    runtime = runtime_config()
    max_bytes = int(os.getenv("SIMCOMPARE_DEBUG_LOG_MAX_BYTES", str(runtime.get("debug_log_max_bytes", 32 * 1024 * 1024))))
    with log_path.open("rb") as reader:
        reader.seek(0, os.SEEK_END)
        size = reader.tell()
        reader.seek(max(0, size - max_bytes))
        text = reader.read().decode("utf-8", errors="replace")
    records = [record for record in split_log_records(text) if conference_id in record]
    if not records:
        return None

    target_index = None
    chunk_pattern = re.compile(rf"DEBUG_INFO:\s*chunk_id\s*=\s*{re.escape(str(chunk_id))}\b")
    for index in range(len(records) - 1, -1, -1):
        record = records[index]
        if chunk_pattern.search(record):
            target_index = index
            break
    if target_index is None:
        return None

    block_start = target_index
    for index in range(target_index, -1, -1):
        if "####################qwen3_asr######################" in records[index]:
            block_start = index
            break
    block_end = len(records)
    for index in range(target_index + 1, len(records)):
        if "####################qwen3_asr######################" in records[index]:
            block_end = index
            break
    if block_start == target_index and block_end == len(records):
        before = int(os.getenv("SIMCOMPARE_DEBUG_CONTEXT_BEFORE", str(runtime.get("debug_context_before", 24))))
        after = int(os.getenv("SIMCOMPARE_DEBUG_CONTEXT_AFTER", str(runtime.get("debug_context_after", 8))))
        context = records[max(0, target_index - before): min(len(records), target_index + after + 1)]
    else:
        context = records[block_start:block_end]
    messages = [log_message(record) for record in context]
    debug_info_message = log_message(records[target_index])

    debug = {
        "chunk_id": int(chunk_id) if str(chunk_id).isdigit() else chunk_id,
        "ctc_avg_prob": parse_float(r"ctc_avg_prob=([0-9.]+)", debug_info_message),
        "rtf": parse_float(r"rtf=([0-9.]+)", debug_info_message),
        "concat_wav_duration": parse_float(r"concat_wav_duration=([0-9.]+)s?", debug_info_message),
        "dur_vad": parse_float(r"dur_vad=([0-9.]+)s?", debug_info_message),
        "prefix_length": parse_int(r"prefix_length=([0-9]+)", debug_info_message),
    }

    asr_info = {}
    words = []
    for message in reversed(messages):
        if not asr_info and "ASR:|" in message:
            asr_info = parse_asr_from_log(message)
        if not words and "WORD_TIMESTAMPS:" in message:
            words = parse_words_from_log(message)
        if debug.get("concat_wav_duration") is None and "concat wave duration:" in message:
            debug["concat_wav_duration"] = parse_float(r"concat wave duration:\s*([0-9.]+)", message)
        if debug.get("dur_vad") is None and "VAD_DUR" in message:
            debug["dur_vad"] = parse_float(r"VAD_DUR-+([0-9.]+)", message)
        if debug.get("rtf") is None and "S2TT RTF" in message:
            debug["rtf"] = parse_float(r"S2TT RTF\(per wave_dur\):\s*([0-9.]+)", message)
        if debug.get("ctc_avg_prob") is None and "avg prob" in message:
            debug["ctc_avg_prob"] = parse_float(r"avg prob:\|([0-9.]+)\|", message)

    return {
        "chunk_id": int(chunk_id) if str(chunk_id).isdigit() else chunk_id,
        "conference_id": conference_id,
        "asr": {
            "src_text": asr_info.get("src_text", ""),
            "bg": None,
            "ed": None,
            "words": words,
            "lang": asr_info.get("lang", ""),
        },
        "mt": {
            "tgt_text": asr_info.get("tgt_text", ""),
            "part_2_mt": asr_info.get("part_2_mt", ""),
        },
        "debug": {key: value for key, value in debug.items() if value is not None},
        "logs": messages,
        "log_file": str(log_path),
    }


def debug_audio_path(debug_root: Path, conference_id: str, chunk_id: str, log_debug: Optional[dict] = None):
    debug_dir = debug_root / conference_id
    if log_debug and log_debug.get("audio_file"):
        candidate = debug_dir / str(log_debug["audio_file"])
        if candidate.exists():
            return candidate
    candidate = debug_dir / "audio" / f"{chunk_id}.wav"
    return candidate if candidate.exists() else None


async def process_run(run_id: str, video_path: str, systems: list, direction: str, conference_id: str):
    run = RUNS[run_id]
    run["status"] = "running"
    try:
        def should_stop():
            return bool(run.get("cancelled"))

        if systems:
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
                        conference_id=conference_id,
                    )
                except Exception as exc:
                    logger.exception(
                        "%s grpc failed endpoint=%s lang=%s conference_id=%s audio=%s",
                        side,
                        endpoint,
                        lang,
                        conference_id,
                        video_path,
                    )
                    message = f"{type(exc).__name__}: {exc}"
                    rows = error_chunk(side, endpoint, message)
                    update_side(side, rows)
                    run[f"{side}_error"] = message
                    return rows

            left_task = asyncio.to_thread(
                run_side,
                "left",
                system_endpoint(systems[0]),
            )
            right_task = asyncio.to_thread(
                run_side,
                "right",
                system_endpoint(systems[1]),
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
    return {
        "ok": True,
        "service": "simcompare-api",
        "grpc_adapter": (APP_DIR / "grpc_info" / "asr_pb2_grpc.py").exists(),
        "config_loaded": config_loaded(),
        "config_path": config_path(),
    }


@app.get("/api/config")
async def get_config():
    services = {}
    for side in ("left", "right"):
        item = service_config(side)
        services[side] = {
            "label": item.get("label") or ("系统 A" if side == "left" else "系统 B"),
            "grpc_url": item.get("grpc_url") or item.get("url") or "",
            "debug_log_configured": bool(item.get("debug_log") or item.get("debugLog")),
            "debug_root_configured": bool(item.get("debug_root") or item.get("debugRoot")),
        }
    return {
        "config_loaded": config_loaded(),
        "config_path": config_path(),
        "services": services,
        "runtime": runtime_config(),
        "storage": {
            "upload_dir_configured": bool(os.getenv("SIMCOMPARE_UPLOAD_DIR") or storage_config().get("upload_dir")),
        },
    }


@app.post("/api/uploads")
async def create_upload(video: UploadFile = File(...)):
    if not is_supported_audio(video.filename or ""):
        return JSONResponse(status_code=400, content={"detail": "only wav, wave and mp3 files are supported"})
    upload_id = f"upload_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}"
    temp = create_upload_temp_file(video.filename or ".wav", ".wav")
    with temp as output:
        shutil.copyfileobj(video.file, output)
    UPLOADS[upload_id] = {"upload_id": upload_id, "path": temp.name, "filename": video.filename or Path(temp.name).name, "created_at": time.time()}
    return {"upload_id": upload_id, "filename": UPLOADS[upload_id]["filename"]}


@app.post("/api/runs")
async def create_run(background_tasks: BackgroundTasks, video: Optional[UploadFile] = File(None), upload_id: str = Form(""), systems: str = Form("[]"), direction: str = Form("zh2en"), conference_id: str = Form("")):
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}"
    upload = UPLOADS.pop(upload_id, None) if upload_id else None
    if upload:
        video_path = upload["path"]
    elif video:
        if not is_supported_audio(video.filename or ""):
            return JSONResponse(status_code=400, content={"detail": "only wav, wave and mp3 files are supported"})
        temp = create_upload_temp_file(video.filename or ".wav", ".wav")
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
    conference_id = conference_id.strip() or f"simcompare_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}"
    RUNS[run_id] = {"run_id": run_id, "conference_id": conference_id, "status": "queued", "stage": "queued", "progress": 0, "completed_chunks": 0, "audio_sent_ms": 0, "audio_total_ms": 0, "audio_progress": {}, "systems": parsed_systems, "direction": direction, "cancelled": False, "stream_started": False}
    background_tasks.add_task(process_run, run_id, video_path, parsed_systems, direction, conference_id)
    return {"run_id": run_id, "status": "queued", "conference_id": conference_id}


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


@app.get("/api/runs/{run_id}/debug/{side}/{chunk_id}")
async def get_chunk_debug(run_id: str, side: str, chunk_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    if side not in {"left", "right"}:
        return JSONResponse(status_code=400, content={"detail": "side must be left or right"})
    chunk = find_chunk(run, side, chunk_id)
    if not chunk:
        return JSONResponse(status_code=404, content={"detail": "chunk not found"})
    conference_id = str(chunk.get("conference_id") or run.get("conference_id") or "")
    debug_root = debug_root_for_side(run, side)
    debug_log = debug_log_for_side(run, side)
    log_debug = parse_debug_log(debug_log, conference_id, chunk_id) if debug_log and conference_id else None
    audio_path = debug_audio_path(debug_root, conference_id, chunk_id, log_debug) if debug_root and conference_id else None
    raw = chunk.get("raw") or {}
    fallback = {
        "chunk_id": chunk_id,
        "conference_id": conference_id,
        "side": side,
        "asr": {
            "src_text": chunk.get("asr", ""),
            "bg": chunk.get("start", 0),
            "ed": chunk.get("end", 0),
            "words": chunk.get("words", []),
            "lang": raw.get("ident_lang", ""),
        },
        "mt": {
            "tgt_text": chunk.get("mt", ""),
            "part_2_mt": raw.get("part_2_mt", ""),
        },
        "debug": {
            "hold_n": raw.get("hold_n"),
            "asr_returned_at_ms": chunk.get("asr_returned_at"),
            "mt_returned_at_ms": chunk.get("mt_returned_at"),
        },
        "logs": chunk.get("logs", []),
        "raw": raw,
        "debug_found": bool(log_debug),
        "audio_found": bool(audio_path),
        "audio_url": f"/api/runs/{run_id}/debug/{side}/{chunk_id}/audio" if audio_path else "",
    }
    if log_debug:
        fallback.update(log_debug)
        fallback["side"] = side
        fallback["debug_found"] = True
        fallback["audio_found"] = bool(audio_path)
        fallback["audio_url"] = f"/api/runs/{run_id}/debug/{side}/{chunk_id}/audio" if audio_path else ""
        fallback.setdefault("raw", raw)
        fallback["raw"] = raw
        if not fallback.get("asr", {}).get("src_text"):
            fallback["asr"]["src_text"] = chunk.get("asr", "")
        fallback["asr"]["bg"] = chunk.get("start", fallback.get("asr", {}).get("bg"))
        fallback["asr"]["ed"] = chunk.get("end", fallback.get("asr", {}).get("ed"))
        if not fallback.get("mt", {}).get("tgt_text"):
            fallback["mt"]["tgt_text"] = chunk.get("mt", "")
    return fallback


@app.get("/api/runs/{run_id}/debug/{side}/{chunk_id}/audio")
async def get_chunk_debug_audio(run_id: str, side: str, chunk_id: str):
    run = RUNS.get(run_id)
    if not run:
        return JSONResponse(status_code=404, content={"detail": "run not found"})
    if side not in {"left", "right"}:
        return JSONResponse(status_code=400, content={"detail": "side must be left or right"})
    chunk = find_chunk(run, side, chunk_id)
    if not chunk:
        return JSONResponse(status_code=404, content={"detail": "chunk not found"})
    conference_id = str(chunk.get("conference_id") or run.get("conference_id") or "")
    debug_root = debug_root_for_side(run, side)
    debug_log = debug_log_for_side(run, side)
    log_debug = parse_debug_log(debug_log, conference_id, chunk_id) if debug_log and conference_id else None
    audio_path = debug_audio_path(debug_root, conference_id, chunk_id, log_debug) if debug_root and conference_id else None
    if not audio_path:
        return JSONResponse(status_code=404, content={"detail": "debug audio not found"})
    return FileResponse(audio_path, media_type="audio/wav", filename=audio_path.name)


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

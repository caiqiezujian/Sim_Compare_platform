"""External model API runner for SimCompare.

Supports Qwen (WebSocket real-time) and Doubao (TBD).
Each provider transcribes audio (ASR) + translates (MT),
returning chunks in the same format as grpc_runner.

Audio format matches gRPC: 16kHz / mono / 16-bit PCM.
No debug log functionality — external APIs don't expose that.
"""
import asyncio
import base64
import json
import logging
import math
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

logger = logging.getLogger("simcompare.api_runner")

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2


def _ensure_wav(path: str) -> str:
    """Convert any audio/video file to 16kHz/mono/16-bit WAV."""
    if path.lower().endswith((".wav", ".wave")):
        return path
    target = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    subprocess.run(
        ["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", str(SAMPLE_RATE), "-sample_fmt", "s16", target],
        check=True, capture_output=True,
    )
    return target


def _read_pcm(path: str) -> bytes:
    """Read raw PCM bytes from a WAV file."""
    import wave
    wav_path = _ensure_wav(path)
    with wave.open(wav_path, "rb") as reader:
        return reader.readframes(reader.getnframes())


def _make_chunk(chunk_id, sn, conference_id, start_ms, end_ms, asr_text, mt_text, logs=None):
    return {
        "id": str(chunk_id),
        "chunk_id": str(chunk_id),
        "sn": sn,
        "conference_id": conference_id,
        "start": start_ms,
        "end": end_ms,
        "asr": asr_text,
        "mt": mt_text,
        "status": "done",
        "audio": f"{chunk_id}.wav",
        "logs": logs or [],
        "debug_available": False,
    }


# ──────────────────────── Qwen (WebSocket) ────────────────────────

QWEN_API_URL = (
    "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
    "?model=qwen3.5-livetranslate-flash-realtime"
)
QWEN_CHUNK_MS = 100
QWEN_CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * QWEN_CHUNK_MS / 1000)  # 3200


def _run_qwen_sync(
    audio_file: str,
    api_key: str,
    lang: str,
    lang_to: str,
    conference_id: str,
    on_update: Optional[Callable],
    should_stop: Optional[Callable],
    on_stream_start: Optional[Callable],
    on_audio_progress: Optional[Callable],
) -> List[Dict[str, Any]]:
    """Qwen real-time translation — sync websocket-client (no asyncio nesting).

    Follows the official qwen3.5-livetranslate-flash-realtime API:
    - modalities: ["text"] → uses response.text.text (streaming) + response.text.done (final)
    - input_audio_transcription: qwen3-asr-flash-realtime → ASR source text
    - VAD mode (default): server auto-detects speech boundaries
    - text field = confirmed text (grows within a response)
    - stash field = unconfirmed prediction (can change)
    - Display: text + stash for live preview, .done for final lock
    """
    import ssl
    import threading
    import websocket as ws_client

    pcm = _read_pcm(audio_file)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / QWEN_CHUNK_BYTES)

    # State — delta segmentation approach
    # Qwen's text field is CUMULATIVE (grows monotonically).
    # We extract per-second deltas: delta = text[len(prev_text):]
    # ASR segments are the "anchor" — each ASR delta pairs with all MT deltas
    # accumulated since the previous ASR delta.
    chunks: List[Dict[str, Any]] = []
    session_finished = False
    last_event_time = time.time()
    lock = threading.Lock()
    current_sent_ms = 0

    # Delta extraction state
    prev_asr_text = ""          # cumulative text at last second boundary
    prev_mt_text = ""
    asr_current_text = ""       # latest cumulative text seen this second
    mt_current_text = ""
    asr_last_sec = -1           # integer second of last ASR event
    mt_last_sec = -1

    # Locked segments: each is (sent_ms, delta_text)
    asr_segs: List = []         # ASR delta segments (the "anchor")
    mt_segs: List = []          # MT delta segments (independent)
    asr_mt_end: List[int] = []  # asr_mt_end[i] = len(mt_segs) when ASR seg #i was locked
    # Chunk #i's MT = join(mt_segs[asr_mt_end[i-1] : asr_mt_end[i]])

    def flush_asr_delta():
        """Lock ASR delta for the previous second and pair with accumulated MT."""
        nonlocal prev_asr_text, asr_current_text
        if asr_current_text and asr_current_text != prev_asr_text:
            delta = asr_current_text[len(prev_asr_text):]
            if delta.strip():
                asr_segs.append((current_sent_ms, delta))
                asr_mt_end.append(len(mt_segs))
        prev_asr_text = asr_current_text

    def flush_mt_delta():
        """Lock MT delta for the previous second."""
        nonlocal prev_mt_text, mt_current_text
        if mt_current_text and mt_current_text != prev_mt_text:
            delta = mt_current_text[len(prev_mt_text):]
            if delta.strip():
                mt_segs.append((current_sent_ms, delta))
        prev_mt_text = mt_current_text

    def push_update():
        if on_update:
            on_update(list(chunks))

    def rebuild_chunks():
        """Rebuild chunks: pair each ASR delta with its accumulated MT deltas."""
        new_chunks = []
        for i in range(len(asr_segs)):
            asr_ms, asr_delta = asr_segs[i]
            prev_end = asr_mt_end[i - 1] if i > 0 else 0
            this_end = asr_mt_end[i]
            mt_text = "".join(d for _, d in mt_segs[prev_end:this_end])
            logs = [f"Qwen #{i+1}", f"ASR delta | sent={asr_ms}ms"]
            if this_end > prev_end:
                logs.append(f"MT segs {prev_end}..{this_end} | sent={mt_segs[prev_end][0]}ms")
            new_chunks.append(_make_chunk(i + 1, i + 1, conference_id,
                                          asr_ms, asr_ms,
                                          asr_delta, mt_text, logs))
        # Streaming chunk: un-flushed ASR delta + pending MT deltas
        asr_live = ""
        if asr_current_text and asr_current_text != prev_asr_text:
            asr_live = asr_current_text[len(prev_asr_text):]
        pending_start = asr_mt_end[-1] if asr_mt_end else 0
        mt_live = "".join(d for _, d in mt_segs[pending_start:])
        if mt_current_text and mt_current_text != prev_mt_text:
            mt_live += mt_current_text[len(prev_mt_text):]
        if asr_live.strip() or mt_live.strip():
            idx = len(asr_segs)
            new_chunks.append(_make_chunk(idx + 1, idx + 1, conference_id,
                                          current_sent_ms, current_sent_ms,
                                          asr_live, mt_live, [f"Qwen #{idx+1} (streaming)"]))
        chunks[:] = new_chunks
        push_update()

    def on_message(ws, message):
        nonlocal asr_current_text, asr_last_sec, prev_asr_text
        nonlocal mt_current_text, mt_last_sec, prev_mt_text
        nonlocal session_finished, last_event_time, current_sent_ms
        last_event_time = time.time()
        try:
            event = json.loads(message)
        except Exception:
            return
        etype = event.get("type", "")
        sec = int(current_sent_ms / 1000)

        # ASR streaming partial (cumulative text)
        if etype == "conversation.item.input_audio_transcription.text":
            text = (event.get("text") or "").strip()
            with lock:
                if asr_last_sec != -1 and sec != asr_last_sec:
                    flush_asr_delta()
                asr_last_sec = sec
                asr_current_text = text
                rebuild_chunks()

        # ASR final → force flush
        elif etype == "conversation.item.input_audio_transcription.completed":
            transcript = (event.get("transcript") or "").strip()
            if transcript:
                with lock:
                    asr_current_text = transcript
                    if asr_last_sec != -1:
                        flush_asr_delta()
                    if transcript != prev_asr_text:
                        delta = transcript[len(prev_asr_text):]
                        if delta.strip():
                            asr_segs.append((current_sent_ms, delta))
                            asr_mt_end.append(len(mt_segs))
                            prev_asr_text = transcript
                    rebuild_chunks()

        # MT streaming partial (text mode)
        elif etype == "response.text.text":
            text = (event.get("text") or "").strip()
            with lock:
                if mt_last_sec != -1 and sec != mt_last_sec:
                    flush_mt_delta()
                mt_last_sec = sec
                mt_current_text = text
                rebuild_chunks()

        # MT streaming partial (audio+text mode)
        elif etype == "response.audio_transcript.text":
            text = (event.get("text") or "").strip()
            with lock:
                if mt_last_sec != -1 and sec != mt_last_sec:
                    flush_mt_delta()
                mt_last_sec = sec
                mt_current_text = text
                rebuild_chunks()

        # MT final (text mode) → force flush
        elif etype == "response.text.done":
            text = (event.get("text") or "").strip()
            if text:
                with lock:
                    mt_current_text = text
                    if mt_last_sec != -1:
                        flush_mt_delta()
                    if text != prev_mt_text:
                        delta = text[len(prev_mt_text):]
                        if delta.strip():
                            mt_segs.append((current_sent_ms, delta))
                        prev_mt_text = text
                    rebuild_chunks()

        # MT final (audio+text mode) → force flush
        elif etype == "response.audio_transcript.done":
            text = (event.get("transcript") or event.get("text") or "").strip()
            if text:
                with lock:
                    mt_current_text = text
                    if mt_last_sec != -1:
                        flush_mt_delta()
                    if text != prev_mt_text:
                        delta = text[len(prev_mt_text):]
                        if delta.strip():
                            mt_segs.append((current_sent_ms, delta))
                        prev_mt_text = text
                    rebuild_chunks()

        elif etype == "session.finished":
            session_finished = True

        elif etype == "error":
            logger.warning("Qwen error event: %s", event)

    def on_error(ws, error):
        logger.warning("Qwen WebSocket error: %s: %s", type(error).__name__, error)

    def on_close(ws, code, reason):
        logger.info("Qwen WebSocket closed: code=%s reason=%s", code, reason)

    def on_open(ws):
        # Configure session — match probe settings
        session_config = {
            "modalities": ["text", "audio"],
            "input_audio_format": "pcm",
            "output_audio_format": "pcm",
            "translation": {"language": lang_to},
            "input_audio_transcription": {"model": "qwen3-asr-flash-realtime"},
        }
        if lang:
            session_config["input_audio_transcription"]["language"] = lang

        ws.send(json.dumps({
            "event_id": f"evt_{int(time.time()*1000)}",
            "type": "session.update",
            "session": session_config,
        }, ensure_ascii=False))

    # Create WebSocket
    ws = ws_client.WebSocketApp(
        QWEN_API_URL,
        header={"Authorization": f"Bearer {api_key}"},
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Send audio in a background thread
    def send_audio():
        nonlocal session_finished, current_sent_ms
        # Wait for session.update to be processed
        time.sleep(2)

        if on_stream_start:
            on_stream_start()

        # Send audio in 100ms chunks (real-time pace)
        for idx in range(total_chunks):
            if should_stop and should_stop():
                break
            chunk = pcm[idx * QWEN_CHUNK_BYTES:(idx + 1) * QWEN_CHUNK_BYTES]
            if not chunk:
                break
            try:
                ws.send(json.dumps({
                    "event_id": f"evt_{int(time.time()*1000)}_{idx}",
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(chunk).decode("utf-8"),
                }, ensure_ascii=False))
            except Exception as exc:
                logger.warning("Qwen send failed at chunk %d: %s", idx + 1, exc)
                break
            current_sent_ms = (idx + 1) * QWEN_CHUNK_MS
            if on_audio_progress:
                on_audio_progress(current_sent_ms, total_ms)
            if idx % 50 == 0 or idx < 3:
                logger.info("Qwen sending chunk %d/%d  sent=%dms", idx + 1, total_chunks, current_sent_ms)
            time.sleep(QWEN_CHUNK_MS / 1000.0)

        # Send 3s ending silence (helps VAD detect end of speech)
        silence = b"\x00" * QWEN_CHUNK_BYTES
        for i in range(30):
            if should_stop and should_stop():
                break
            try:
                ws.send(json.dumps({
                    "event_id": f"evt_silence_{i}",
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(silence).decode("utf-8"),
                }, ensure_ascii=False))
            except Exception:
                break
            time.sleep(0.1)

        # Send session.finish
        try:
            ws.send(json.dumps({"event_id": "evt_finish", "type": "session.finish"}))
            logger.info("Qwen session.finish sent")
        except Exception:
            pass

        # Wait for session.finished (max 60s, or 15s idle — Qwen ASR/MT final
        # results can arrive 5-10s after audio ends)
        deadline = time.time() + 60
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > 15:
                logger.warning("Qwen: 15s idle after session.finish, closing")
                break
            time.sleep(0.3)

        # Close WebSocket (this will end ws.run_forever)
        try:
            ws.close()
        except Exception:
            pass

    threading.Thread(target=send_audio, daemon=True).start()

    # Run WebSocket in main thread (blocking)
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30, ping_timeout=10)

    # Final rebuild
    with lock:
        rebuild_chunks()

    return chunks


# ──────────────────────── Doubao (WebSocket + protobuf) ────────────────────────

DOUBAO_WS_URL = "wss://openspeech.bytedance.com/api/v4/ast/v2/translate"
DOUBAO_RESOURCE_ID = "volc.service_type.10053"
DOUBAO_CHUNK_BYTES = 3200  # 100ms = 16000 * 2 * 0.1

# ──────────────────────── Huawei (WSS) ────────────────────────

# Auth params are configured per-service in the frontend (WSS URL with query params).
# Not stored in config file — passed at runtime from the sidebar system dict.

HW_CHUNK_MS = 80
HW_CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * HW_CHUNK_MS / 1000)  # 2560

# Protobuf modules (lazy import — only needed for Doubao)
_doubao_proto_ready = False

def _ensure_doubao_proto():
    """Add ast_python/python_protogen_v2 to sys.path and import protobuf."""
    global _doubao_proto_ready
    if _doubao_proto_ready:
        return
    import sys as _sys
    proto_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "ast_python", "python_protogen_v2")
    if proto_dir not in _sys.path:
        _sys.path.insert(0, proto_dir)
    _doubao_proto_ready = True


def _run_doubao(
    audio_file: str,
    api_key: str,
    lang: str,
    lang_to: str,
    conference_id: str,
    on_update: Optional[Callable],
    should_stop: Optional[Callable],
    on_stream_start: Optional[Callable],
    on_audio_progress: Optional[Callable],
) -> List[Dict[str, Any]]:
    """Doubao real-time translation via WebSocket + protobuf.

    Protocol (from doubao.py / ast_demo.py):
    - WebSocket: wss://openspeech.bytedance.com/api/v4/ast/v2/translate
    - Auth: X-Api-Key + X-Api-Resource-Id + X-Api-Connect-Id headers
    - Request: protobuf TranslateRequest (StartSession → TaskRequest×N → FinishSession)
    - Response: protobuf TranslateResponse with event type:
      - 650 SourceSubtitleStart / 651 SourceSubtitleResponse (piece) / 652 SourceSubtitleEnd (final)
      - 653 TranslationSubtitleStart / 654 TranslationSubtitleResponse (piece) / 655 TranslationSubtitleEnd (final)
    - Response.text is INCREMENTAL (a piece), must join pieces for building text
    """
    import ssl
    import threading
    import wave
    import uuid
    import websocket as ws_client

    _ensure_doubao_proto()
    from products.understanding.ast.ast_service_pb2 import TranslateRequest, TranslateResponse
    from common.events_pb2 import Type

    # Doubao expects WAV format (with header), not raw PCM.
    # Read the entire WAV file bytes (like doubao.py does).
    wav_path = _ensure_wav(audio_file)
    with open(wav_path, "rb") as f:
        wav_bytes = f.read()
    total_ms = int(len(wav_bytes) / 2 / SAMPLE_RATE * 1000)  # rough
    total_chunks = math.ceil(len(wav_bytes) / DOUBAO_CHUNK_BYTES)

    session_id = str(uuid.uuid4())
    conn_id = str(uuid.uuid4())

    # State
    chunks: List[Dict[str, Any]] = []
    asr_locked: List[str] = []     # final ASR segments (from SourceSubtitleEnd)
    mt_locked: List[str] = []      # final MT segments (from TranslationSubtitleEnd)
    asr_building_parts: List[str] = []   # current ASR pieces (from SourceSubtitleResponse)
    mt_building_parts: List[str] = []    # current MT pieces
    session_finished = False
    last_event_time = time.time()
    lock = threading.Lock()
    current_sent_ms = 0            # audio sent so far (updated by send_audio thread)
    seg_start_ms: List[int] = []  # per-segment start: sent_ms when MT Start (653) fires
    seg_end_ms: List[int] = []    # per-segment end: sent_ms when MT End (655) fires
    asr_start_ms: List[int] = []  # ASR start: sent_ms when ASR Start (650) fires (for logs)
    asr_end_ms: List[int] = []    # ASR end: sent_ms when ASR End (652) fires (for logs)
    seg_logs: List[List[str]] = []  # per-segment log lines

    def push_update():
        if on_update:
            on_update(list(chunks))

    def rebuild_chunks():
        """Rebuild chunks from locked + building state."""
        new_chunks = []
        max_locked = max(len(asr_locked), len(mt_locked))
        for i in range(max_locked):
            asr_text = asr_locked[i] if i < len(asr_locked) else ""
            mt_text = mt_locked[i] if i < len(mt_locked) else ""
            s = seg_start_ms[i] if i < len(seg_start_ms) else 0
            e = seg_end_ms[i] if i < len(seg_end_ms) else s
            logs = [f"Doubao #{i+1}"]
            if i < len(asr_start_ms):
                logs.append(f"ASR start | sent={asr_start_ms[i]}ms")
            if i < len(asr_end_ms):
                logs.append(f"ASR end | sent={asr_end_ms[i]}ms")
            if i < len(seg_start_ms):
                logs.append(f"MT start | sent={seg_start_ms[i]}ms")
            if i < len(seg_end_ms):
                logs.append(f"MT end | sent={seg_end_ms[i]}ms")
            new_chunks.append(_make_chunk(i + 1, i + 1, conference_id,
                                          s, e,
                                          asr_text, mt_text, logs))
        # Add streaming chunk (in-progress segment)
        asr_live = "".join(asr_building_parts)
        mt_live = "".join(mt_building_parts)
        if asr_live or mt_live:
            idx = max_locked
            s = seg_start_ms[idx] if idx < len(seg_start_ms) else current_sent_ms
            new_chunks.append(_make_chunk(idx + 1, idx + 1, conference_id,
                                          s, current_sent_ms,
                                          asr_live, mt_live, [f"Doubao #{idx+1} (streaming)"]))
        chunks[:] = new_chunks
        push_update()

    def build_request(event_type, audio_data=None):
        req = TranslateRequest()
        req.request_meta.SessionID = session_id
        req.event = event_type
        req.user.uid = "simcompare"
        req.user.did = "simcompare"
        req.source_audio.format = "wav"
        req.source_audio.rate = SAMPLE_RATE
        req.source_audio.bits = 16
        req.source_audio.channel = 1
        if audio_data:
            req.source_audio.binary_data = audio_data
        req.request.mode = "s2t"
        req.request.source_language = lang
        req.request.target_language = lang_to
        return req

    def on_open(ws):
        logger.info("Doubao connected, sending StartSession")
        start_req = build_request(Type.StartSession)
        ws.send(start_req.SerializeToString(), opcode=ws_client.ABNF.OPCODE_BINARY)

    def on_message(ws, message):
        nonlocal session_finished, last_event_time, current_sent_ms
        if isinstance(message, str):
            return
        last_event_time = time.time()
        resp = TranslateResponse()
        resp.ParseFromString(message)
        event_val = resp.event
        text = resp.text or ""

        with lock:
            # SessionStarted → start sending audio
            if event_val == Type.SessionStarted:
                logger.info("Doubao SessionStarted, starting audio send")
                if on_stream_start:
                    on_stream_start()

                def send_audio():
                    nonlocal current_sent_ms
                    for idx in range(total_chunks):
                        if should_stop and should_stop():
                            break
                        chunk = wav_bytes[idx * DOUBAO_CHUNK_BYTES:(idx + 1) * DOUBAO_CHUNK_BYTES]
                        if not chunk:
                            break
                        req = build_request(Type.TaskRequest, chunk)
                        try:
                            ws.send(req.SerializeToString(), opcode=ws_client.ABNF.OPCODE_BINARY)
                        except Exception as exc:
                            logger.warning("Doubao send failed at chunk %d: %s", idx + 1, exc)
                            break
                        current_sent_ms = int((idx + 1) * DOUBAO_CHUNK_BYTES / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
                        if on_audio_progress:
                            on_audio_progress(current_sent_ms, total_ms)
                        if idx % 50 == 0 or idx < 3:
                            logger.info("Doubao sending chunk %d/%d  sent=%dms", idx + 1, total_chunks, current_sent_ms)
                        time.sleep(0.1)

                    # FinishSession — send immediately after audio (like doubao.py)
                    try:
                        finish_req = build_request(Type.FinishSession)
                        ws.send(finish_req.SerializeToString(), opcode=ws_client.ABNF.OPCODE_BINARY)
                        logger.info("Doubao FinishSession sent")
                    except Exception:
                        pass

                threading.Thread(target=send_audio, daemon=True).start()

            # ASR: SourceSubtitleStart (650)
            elif event_val == 650:
                asr_building_parts.clear()
                asr_start_ms.append(current_sent_ms)
                rebuild_chunks()

            # ASR: SourceSubtitleResponse (651) — incremental piece
            elif event_val == 651:
                if text:
                    asr_building_parts.append(text)
                    rebuild_chunks()

            # ASR: SourceSubtitleEnd (652) — final
            elif event_val == 652:
                final_text = text or "".join(asr_building_parts)
                if final_text:
                    asr_locked.append(final_text)
                asr_end_ms.append(current_sent_ms)
                asr_building_parts.clear()
                rebuild_chunks()

            # MT: TranslationSubtitleStart (653)
            elif event_val == 653:
                mt_building_parts.clear()
                seg_start_ms.append(current_sent_ms)
                rebuild_chunks()

            # MT: TranslationSubtitleResponse (654) — incremental piece
            elif event_val == 654:
                if text:
                    mt_building_parts.append(text)
                    rebuild_chunks()

            # MT: TranslationSubtitleEnd (655) — final
            elif event_val == 655:
                final_text = text or "".join(mt_building_parts)
                if final_text:
                    mt_locked.append(final_text)
                seg_end_ms.append(current_sent_ms)
                mt_building_parts.clear()
                rebuild_chunks()

            elif event_val == Type.SessionFinished:
                session_finished = True
                logger.info("Doubao SessionFinished")

            elif event_val == Type.SessionFailed:
                session_finished = True
                logger.warning("Doubao SessionFailed: %s", resp.response_meta.Message)

            elif event_val == Type.UsageResponse:
                pass  # billing info, skip

    def on_error(ws, error):
        logger.warning("Doubao WebSocket error: %s: %s", type(error).__name__, error)

    def on_close(ws, code, reason):
        logger.info("Doubao WebSocket closed: code=%s reason=%s", code, reason)

    # Create WebSocket
    ws = ws_client.WebSocketApp(
        DOUBAO_WS_URL,
        header={
            "X-Api-Key": api_key,
            "X-Api-Resource-Id": DOUBAO_RESOURCE_ID,
            "X-Api-Connect-Id": conn_id,
        },
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Run WebSocket (blocking) — wait for SessionFinished, like doubao.py
    def watchdog():
        """Only force-close after 15s of silence after audio finished."""
        time.sleep(10)
        deadline = time.time() + 120
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > 15:
                logger.warning("Doubao watchdog: 15s no events, closing")
                try:
                    ws.close()
                except Exception:
                    pass
                break
            time.sleep(1)

    threading.Thread(target=watchdog, daemon=True).start()
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30, ping_timeout=10)

    # Final rebuild
    with lock:
        rebuild_chunks()

    return chunks


# ──────────────────────── Huawei (WSS) ────────────────────────

def _run_huawei(
    audio_file: str,
    api_key: str,  # unused for huawei
    lang: str,
    lang_to: str,
    conference_id: str,
    on_update: Optional[Callable],
    should_stop: Optional[Callable],
    on_stream_start: Optional[Callable],
    on_audio_progress: Optional[Callable],
    wss_url: str = "",
) -> List[Dict[str, Any]]:
    """Huawei real-time translation via WebSocket (JSON protocol).

    Protocol (from huawei-translation-api.md):
    - Connect: wss://...?X-HW-ID=...&langFrom=zh&langTo=en
    - Server pushes: {"msg":"connect","conferenceId":"xxx"}
    - Client sends audio: {"audioData":"<base64 PCM>","conferenceId":"xxx","seq":N}
      - PCM 16kHz/16-bit/mono, 80ms = 2560 bytes per packet
    - Server returns: {"msgType":"text","sn":N,"sentenceType":0/1/2,"text":"ASR","translate":"MT",...}
      - sentenceType: 0=partial, 1=final(translate may be empty), 2=smooth(translate arrives)
    - Heartbeat every 30s: {"beat":true}

    Timestamps (consistent with Doubao — MT-based):
    - chunk.start = sent_ms when first message for this sn arrives
    - chunk.end   = sent_ms when sentenceType=2 (smooth, MT arrives) fires
    """
    import ssl
    import threading
    import websocket as ws_client

    pcm = _read_pcm(audio_file)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / HW_CHUNK_BYTES)

    if not wss_url:
        raise ValueError("Huawei WSS URL not configured — please set it in the frontend service settings")
    url = wss_url

    # State
    chunks: List[Dict[str, Any]] = []
    sn_data: Dict[int, dict] = {}  # sn -> {asr, mt, start_ms, end_ms, asr_start_ms, mt_end_ms}
    server_conference_id = ""
    session_finished = False
    last_event_time = time.time()
    lock = threading.Lock()
    current_sent_ms = 0

    def push_update():
        if on_update:
            on_update(list(chunks))

    def rebuild_chunks():
        new_chunks = []
        for sn in sorted(sn_data.keys()):
            d = sn_data[sn]
            logs = [f"Huawei #{sn}"]
            if d.get("asr_start_ms") is not None:
                logs.append(f"ASR start | sent={d['asr_start_ms']}ms")
            if d.get("mt_end_ms") is not None:
                logs.append(f"MT end | sent={d['mt_end_ms']}ms")
            s = d.get("start_ms", 0)
            e = d.get("end_ms", s)
            new_chunks.append(_make_chunk(sn, sn, conference_id or server_conference_id,
                                          s, e,
                                          d.get("asr", ""), d.get("mt", ""), logs))
        chunks[:] = new_chunks
        push_update()

    def on_message(ws, message):
        nonlocal server_conference_id, session_finished, last_event_time, current_sent_ms
        if isinstance(message, bytes):
            return
        last_event_time = time.time()
        try:
            obj = json.loads(message)
        except Exception:
            return

        # Connect message
        if obj.get("msg") == "connect":
            server_conference_id = obj.get("conferenceId", "")
            logger.info("Huawei connected, conferenceId=%s", server_conference_id)
            if on_stream_start:
                on_stream_start()

            def send_audio():
                nonlocal current_sent_ms
                time.sleep(0.5)

                # Heartbeat
                def heartbeat():
                    while not session_finished:
                        time.sleep(30)
                        if not session_finished:
                            try:
                                ws.send(json.dumps({"beat": True}))
                            except Exception:
                                break

                threading.Thread(target=heartbeat, daemon=True).start()

                seq = 0
                for idx in range(total_chunks):
                    if should_stop and should_stop():
                        break
                    chunk = pcm[idx * HW_CHUNK_BYTES:(idx + 1) * HW_CHUNK_BYTES]
                    if not chunk:
                        break
                    seq += 1
                    try:
                        b64 = base64.b64encode(chunk).decode()
                        ws.send(json.dumps({
                            "audioData": b64,
                            "conferenceId": server_conference_id,
                            "seq": seq,
                        }, ensure_ascii=False))
                    except Exception as exc:
                        logger.warning("Huawei send failed at seq %d: %s", seq, exc)
                        break
                    current_sent_ms = int(seq * HW_CHUNK_MS)
                    if on_audio_progress:
                        on_audio_progress(current_sent_ms, total_ms)
                    if seq % 50 == 0 or seq <= 3:
                        logger.info("Huawei sending seq=%d/%d sent=%dms", seq, total_chunks, current_sent_ms)
                    time.sleep(HW_CHUNK_MS / 1000.0)

                logger.info("Huawei audio done, waiting for results...")

            threading.Thread(target=send_audio, daemon=True).start()
            return

        # Heartbeat ack
        if obj.get("beat") is True:
            return

        # Text message
        if obj.get("msgType") == "text":
            sn = obj.get("sn", 0)
            st = obj.get("sentenceType", 0)
            text = (obj.get("text") or "").strip()
            translate = (obj.get("translate") or "").strip()
            fluency = (obj.get("fluency") or "").strip()
            progressive = (obj.get("progressive") or "").strip()
            st_label = {0: "partial", 1: "final", 2: "smooth"}.get(st, st)
            logger.info("Huawei recv sn=%d type=%s asr=%r mt=%r fluency=%r sent=%dms",
                        sn, st_label, text, translate, fluency, current_sent_ms)

            with lock:
                d = sn_data.setdefault(sn, {
                    "asr": "", "mt": "", "start_ms": current_sent_ms,
                    "end_ms": 0, "asr_start_ms": None, "mt_end_ms": None,
                })
                if d.get("asr_start_ms") is None:
                    d["asr_start_ms"] = current_sent_ms
                    d["start_ms"] = current_sent_ms

                # type 0 (partial): update streaming ASR
                if st == 0:
                    if text:
                        d["asr"] = text
                    # Show progressive translation if available during partial
                    if progressive:
                        d["mt"] = progressive

                # type 1 (final): lock ASR (translate may be empty)
                elif st == 1:
                    if fluency:
                        d["asr"] = fluency
                    elif text:
                        d["asr"] = text
                    # type=1 may have translate, save if present
                    if translate:
                        d["mt"] = translate
                    # Record end time as fallback (type=2 may not arrive)
                    d["end_ms"] = current_sent_ms

                # type 2 (smooth): lock ASR (fluency) + MT, record end time
                elif st == 2:
                    if fluency:
                        d["asr"] = fluency
                    elif text:
                        d["asr"] = text
                    if translate:
                        d["mt"] = translate
                    d["mt_end_ms"] = current_sent_ms
                    d["end_ms"] = current_sent_ms

                rebuild_chunks()

    def on_error(ws, error):
        logger.warning("Huawei WebSocket error: %s: %s", type(error).__name__, error)

    def on_close(ws, code, reason):
        nonlocal session_finished
        session_finished = True
        logger.info("Huawei WebSocket closed: code=%s reason=%s", code, reason)

    def on_open(ws):
        logger.info("Huawei WebSocket connected")

    ws = ws_client.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Watchdog — close after 15s idle
    def watchdog():
        time.sleep(10)
        deadline = time.time() + 120
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > 15:
                logger.warning("Huawei watchdog: 15s no events, closing")
                try:
                    ws.close()
                except Exception:
                    pass
                break
            time.sleep(1)

    threading.Thread(target=watchdog, daemon=True).start()
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30, ping_timeout=10)

    # Final rebuild
    with lock:
        rebuild_chunks()

    return chunks


# ──────────────────────── OpenAI (WebSocket) ────────────────────────

OPENAI_API_URL = "wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate"
OPENAI_SAMPLE_RATE = 24000
OPENAI_CHUNK_MS = 200
OPENAI_CHUNK_BYTES = int(OPENAI_SAMPLE_RATE * BYTES_PER_SAMPLE * OPENAI_CHUNK_MS / 1000)  # 9600


def _read_pcm_24k(audio_file: str) -> bytes:
    """Read audio as 24kHz/mono/16-bit PCM (OpenAI requirement)."""
    import wave
    wav_path = _ensure_wav(audio_file)
    # Re-encode to 24kHz via ffmpeg
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    subprocess.run(
        ["ffmpeg", "-y", "-i", wav_path, "-ac", "1", "-ar", str(OPENAI_SAMPLE_RATE),
         "-sample_fmt", "s16", tmp],
        check=True, capture_output=True,
    )
    with wave.open(tmp, "rb") as r:
        data = r.readframes(r.getnframes())
    os.unlink(tmp)
    return data


def _run_openai(
    audio_file: str,
    api_key: str,
    lang: str,
    lang_to: str,
    conference_id: str,
    on_update: Optional[Callable],
    should_stop: Optional[Callable],
    on_stream_start: Optional[Callable],
    on_audio_progress: Optional[Callable],
) -> List[Dict[str, Any]]:
    """OpenAI GPT-Realtime-Translate via WebSocket.

    Protocol:
    - Connect: wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate
    - Auth: Authorization: Bearer <key>, OpenAI-Safety-Identifier header
    - session.update: audio.input.transcription.model=gpt-realtime-whisper, audio.output.language=target
    - Send audio: 24kHz/mono/16-bit, 200ms chunks, base64 in session.input_audio_buffer.append
    - ASR events: session.input_transcript.delta (incremental) / .done (final)
    - MT events: session.output_transcript.delta (incremental) / .done (final)
    - Close: session.close → session.closed

    Text is delta-incremental (each event gives only the new text, not cumulative).
    Segmentation: per-second delta collection, ASR as anchor, MT paired like Qwen.
    """
    import ssl
    import threading
    import websocket as ws_client

    pcm = _read_pcm_24k(audio_file)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / OPENAI_SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / OPENAI_CHUNK_BYTES)

    # State — delta segmentation (same approach as Qwen probe2)
    chunks: List[Dict[str, Any]] = []
    session_finished = False
    last_event_time = time.time()
    lock = threading.Lock()
    current_sent_ms = 0

    # Delta buffers — OpenAI sends incremental deltas, we accumulate per second
    asr_delta_buffer = ""   # accumulated ASR delta text this second
    mt_delta_buffer = ""    # accumulated MT delta text this second
    asr_last_sec = -1
    mt_last_sec = -1

    # Locked segments: each is (sent_ms, text)
    asr_segs: List = []
    mt_segs: List = []
    asr_mt_end: List[int] = []  # asr_mt_end[i] = len(mt_segs) when ASR seg #i locked

    # Streaming state for live display
    asr_live_buffer = ""   # current un-flushed ASR delta
    mt_live_buffer = ""    # current un-flushed MT delta

    def push_update():
        if on_update:
            on_update(list(chunks))

    def flush_asr_delta():
        nonlocal asr_delta_buffer, asr_live_buffer
        if asr_delta_buffer.strip():
            asr_segs.append((current_sent_ms, asr_delta_buffer))
            asr_mt_end.append(len(mt_segs))
        asr_delta_buffer = ""

    def flush_mt_delta():
        nonlocal mt_delta_buffer, mt_live_buffer
        if mt_delta_buffer.strip():
            mt_segs.append((current_sent_ms, mt_delta_buffer))
        mt_delta_buffer = ""

    def rebuild_chunks():
        new_chunks = []
        for i in range(len(asr_segs)):
            asr_ms, asr_delta = asr_segs[i]
            prev_end = asr_mt_end[i - 1] if i > 0 else 0
            this_end = asr_mt_end[i]
            mt_text = "".join(d for _, d in mt_segs[prev_end:this_end])
            logs = [f"OpenAI #{i+1}", f"ASR delta | sent={asr_ms}ms"]
            if this_end > prev_end:
                logs.append(f"MT segs {prev_end}..{this_end}")
            new_chunks.append(_make_chunk(i + 1, i + 1, conference_id,
                                          asr_ms, asr_ms,
                                          asr_delta, mt_text, logs))
        # Streaming chunk
        if asr_delta_buffer.strip() or mt_delta_buffer.strip():
            idx = len(asr_segs)
            pending_start = asr_mt_end[-1] if asr_mt_end else 0
            mt_live = "".join(d for _, d in mt_segs[pending_start:]) + mt_delta_buffer
            new_chunks.append(_make_chunk(idx + 1, idx + 1, conference_id,
                                          current_sent_ms, current_sent_ms,
                                          asr_delta_buffer, mt_live,
                                          [f"OpenAI #{idx+1} (streaming)"]))
        chunks[:] = new_chunks
        push_update()

    def on_message(ws, message):
        nonlocal asr_delta_buffer, asr_last_sec, mt_delta_buffer, mt_last_sec
        nonlocal session_finished, last_event_time, current_sent_ms
        if isinstance(message, bytes):
            return
        last_event_time = time.time()
        try:
            event = json.loads(message)
        except Exception:
            return
        etype = event.get("type", "")
        sec = int(current_sent_ms / 1000)

        # ASR delta (incremental text)
        if etype in ("session.input_transcript.delta", "session.input_audio_transcription.delta",
                     "conversation.item.input_audio_transcription.delta"):
            text = _extract_openai_text(event)
            if text:
                with lock:
                    if asr_last_sec != -1 and sec != asr_last_sec:
                        flush_asr_delta()
                    asr_last_sec = sec
                    asr_delta_buffer += text
                    rebuild_chunks()

        # ASR final
        elif etype in ("session.input_transcript.done", "session.input_audio_transcription.done",
                       "conversation.item.input_audio_transcription.completed"):
            text = _extract_openai_text(event)
            if text:
                with lock:
                    asr_delta_buffer += text
                    flush_asr_delta()
                    rebuild_chunks()

        # MT delta (incremental translation)
        elif etype in ("session.output_transcript.delta", "session.output_transcription.delta"):
            text = _extract_openai_text(event)
            if text:
                with lock:
                    if mt_last_sec != -1 and sec != mt_last_sec:
                        flush_mt_delta()
                    mt_last_sec = sec
                    mt_delta_buffer += text
                    rebuild_chunks()

        # MT final
        elif etype in ("session.output_transcript.done", "session.output_transcription.done"):
            text = _extract_openai_text(event)
            if text:
                with lock:
                    mt_delta_buffer += text
                    flush_mt_delta()
                    rebuild_chunks()

        elif etype == "session.closed":
            session_finished = True
            logger.info("OpenAI session.closed received")

        elif etype == "error":
            logger.warning("OpenAI error event: %s", event)

    def _extract_openai_text(event):
        """Extract text from OpenAI event — delta/done formats."""
        for key in ("delta", "transcript", "text", "output_text"):
            value = event.get(key)
            if isinstance(value, str) and value:
                return value
        item = event.get("item")
        if isinstance(item, dict):
            for key in ("delta", "transcript", "text"):
                value = item.get(key)
                if isinstance(value, str) and value:
                    return value
            content = item.get("content")
            if isinstance(content, list):
                texts = []
                for c in content:
                    if isinstance(c, dict):
                        for key in ("transcript", "text"):
                            value = c.get(key)
                            if isinstance(value, str) and value:
                                texts.append(value)
                if texts:
                    return "".join(texts)
        return ""

    def on_error(ws, error):
        logger.warning("OpenAI WebSocket error: %s: %s", type(error).__name__, error)

    def on_close(ws, code, reason):
        nonlocal session_finished
        session_finished = True
        logger.info("OpenAI WebSocket closed: code=%s reason=%s", code, reason)

    def on_open(ws):
        logger.info("OpenAI WebSocket connected")
        # Send session.update
        session_update = {
            "type": "session.update",
            "session": {
                "audio": {
                    "input": {
                        "transcription": {
                            "model": "gpt-realtime-whisper"
                        },
                        "noise_reduction": None
                    },
                    "output": {
                        "language": lang_to
                    }
                }
            }
        }
        ws.send(json.dumps(session_update, ensure_ascii=False))
        logger.info("OpenAI session.update sent, target_language=%s", lang_to)

        # Start sending audio in background
        def send_audio():
            nonlocal current_sent_ms
            time.sleep(0.5)

            if on_stream_start:
                on_stream_start()

            for idx in range(total_chunks):
                if should_stop and should_stop():
                    break
                chunk = pcm[idx * OPENAI_CHUNK_BYTES:(idx + 1) * OPENAI_CHUNK_BYTES]
                if not chunk:
                    break
                try:
                    audio_b64 = base64.b64encode(chunk).decode("ascii")
                    ws.send(json.dumps({
                        "type": "session.input_audio_buffer.append",
                        "audio": audio_b64,
                    }, ensure_ascii=False))
                except Exception as exc:
                    logger.warning("OpenAI send failed at chunk %d: %s", idx + 1, exc)
                    break
                current_sent_ms = int((idx + 1) * OPENAI_CHUNK_MS)
                if on_audio_progress:
                    on_audio_progress(current_sent_ms, total_ms)
                if idx % 50 == 0 or idx < 3:
                    logger.info("OpenAI sending chunk %d/%d sent=%dms", idx + 1, total_chunks, current_sent_ms)
                time.sleep(OPENAI_CHUNK_MS / 1000.0)

            # Send 3s trailing silence
            silence = b"\x00" * OPENAI_CHUNK_BYTES
            for i in range(15):  # 15 * 200ms = 3s
                if should_stop and should_stop():
                    break
                try:
                    audio_b64 = base64.b64encode(silence).decode("ascii")
                    ws.send(json.dumps({
                        "type": "session.input_audio_buffer.append",
                        "audio": audio_b64,
                    }, ensure_ascii=False))
                except Exception:
                    break
                time.sleep(0.2)

            # Wait a bit for last events
            time.sleep(2)

            # Send session.close
            try:
                ws.send(json.dumps({"type": "session.close"}))
                logger.info("OpenAI session.close sent")
            except Exception:
                pass

            # Wait for session.closed (max 30s, or 10s idle)
            deadline = time.time() + 30
            while time.time() < deadline:
                if session_finished:
                    break
                if time.time() - last_event_time > 10:
                    logger.warning("OpenAI: 10s idle after session.close, closing")
                    break
                time.sleep(0.3)

            try:
                ws.close()
            except Exception:
                pass

        threading.Thread(target=send_audio, daemon=True).start()

    ws = ws_client.WebSocketApp(
        OPENAI_API_URL,
        header=[
            f"Authorization: Bearer {api_key}",
            "OpenAI-Safety-Identifier: simcompare",
        ],
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Watchdog
    def watchdog():
        time.sleep(10)
        deadline = time.time() + 180
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > 25:
                logger.warning("OpenAI watchdog: 25s no events, closing")
                try:
                    ws.close()
                except Exception:
                    pass
                break
            time.sleep(1)

    threading.Thread(target=watchdog, daemon=True).start()

    sslopt = {"cert_reqs": ssl.CERT_NONE}
    ws.run_forever(sslopt=sslopt, ping_interval=20, ping_timeout=10)

    # Final rebuild
    with lock:
        # Flush any remaining deltas
        if asr_delta_buffer.strip():
            flush_asr_delta()
        if mt_delta_buffer.strip():
            flush_mt_delta()
        rebuild_chunks()

    return chunks


# ──────────────────────── Gemini (google-genai SDK) ────────────────────────

GEMINI_MODEL = "gemini-3.5-live-translate-preview"
GEMINI_CHUNK_MS = 100
GEMINI_CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * GEMINI_CHUNK_MS / 1000)  # 3200


def _run_gemini(
    audio_file: str,
    api_key: str,
    lang: str,
    lang_to: str,
    conference_id: str,
    on_update: Optional[Callable],
    should_stop: Optional[Callable],
    on_stream_start: Optional[Callable],
    on_audio_progress: Optional[Callable],
) -> List[Dict[str, Any]]:
    """Gemini Live Translate via google-genai SDK.

    Uses asyncio internally, wrapped in a thread for sync compatibility.
    Text is cumulative (like Qwen) — delta extraction per second.
    ASR is anchor, MT paired same as Qwen probe2.

    Proxy: set HTTP_PROXY/HTTPS_PROXY env vars before starting backend.
    SSL: skipped (proxy environment).
    """
    import asyncio
    import ssl
    import inspect
    import threading

    pcm = _read_pcm(audio_file)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / GEMINI_CHUNK_BYTES)

    # Map lang_to to Gemini language codes
    gemini_lang_to = lang_to
    if lang_to == "zh":
        gemini_lang_to = "zh-Hans"

    # Shared state between async and sync worlds
    chunks: List[Dict[str, Any]] = []
    session_finished = False
    last_event_time = time.time()
    lock = threading.Lock()
    current_sent_ms = 0

    # Delta extraction state (same as Qwen)
    prev_asr_text = ""
    prev_mt_text = ""
    asr_current_text = ""
    mt_current_text = ""
    asr_last_sec = -1
    mt_last_sec = -1
    asr_segs: List = []
    mt_segs: List = []
    asr_mt_end: List[int] = []
    asr_parts: List[str] = []
    mt_parts: List[str] = []

    def push_update():
        if on_update:
            on_update(list(chunks))

    def flush_asr_delta(sent_ms):
        nonlocal prev_asr_text, asr_current_text
        if asr_current_text and asr_current_text != prev_asr_text:
            delta = asr_current_text[len(prev_asr_text):]
            if delta.strip():
                asr_segs.append((sent_ms, delta))
                asr_mt_end.append(len(mt_segs))
        prev_asr_text = asr_current_text

    def flush_mt_delta(sent_ms):
        nonlocal prev_mt_text, mt_current_text
        if mt_current_text and mt_current_text != prev_mt_text:
            delta = mt_current_text[len(prev_mt_text):]
            if delta.strip():
                mt_segs.append((sent_ms, delta))
        prev_mt_text = mt_current_text

    def rebuild_chunks():
        new_chunks = []
        for i in range(len(asr_segs)):
            asr_ms, asr_delta = asr_segs[i]
            prev_end = asr_mt_end[i - 1] if i > 0 else 0
            this_end = asr_mt_end[i]
            mt_text = "".join(d for _, d in mt_segs[prev_end:this_end])
            logs = [f"Gemini #{i+1}", f"ASR delta | sent={asr_ms}ms"]
            if this_end > prev_end:
                logs.append(f"MT segs {prev_end}..{this_end}")
            new_chunks.append(_make_chunk(i + 1, i + 1, conference_id,
                                          asr_ms, asr_ms,
                                          asr_delta, mt_text, logs))
        # Streaming chunk
        if asr_current_text and asr_current_text != prev_asr_text:
            asr_live = asr_current_text[len(prev_asr_text):]
        else:
            asr_live = ""
        pending_start = asr_mt_end[-1] if asr_mt_end else 0
        mt_live = "".join(d for _, d in mt_segs[pending_start:])
        if mt_current_text and mt_current_text != prev_mt_text:
            mt_live += mt_current_text[len(prev_mt_text):]
        if asr_live.strip() or mt_live.strip():
            idx = len(asr_segs)
            new_chunks.append(_make_chunk(idx + 1, idx + 1, conference_id,
                                          current_sent_ms, current_sent_ms,
                                          asr_live, mt_live,
                                          [f"Gemini #{idx+1} (streaming)"]))
        chunks[:] = new_chunks
        push_update()

    def append_text_smart(parts, text):
        if not text:
            return
        current = "".join(parts)
        if not current:
            parts.append(text)
            return
        if current.endswith(text):
            return
        if text.startswith(current):
            parts.clear()
            parts.append(text)
            return
        parts.append(text)

    def _build_client():
        # Set proxy env before importing
        proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY") or ""
        if proxy:
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                        "http_proxy", "https_proxy", "all_proxy"):
                os.environ[key] = proxy
            os.environ["NO_PROXY"] = ""
            os.environ["no_proxy"] = ""

        from google import genai
        from google.genai import types

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        async_client_args = {
            "ssl": ssl_ctx,
            "open_timeout": 120,
            "ping_interval": 20,
            "ping_timeout": 20,
        }

        # Try adding proxy to websockets connect
        try:
            from websockets.asyncio.client import connect as ws_connect
            sig = inspect.signature(ws_connect)
            if proxy and "proxy" in sig.parameters:
                async_client_args["proxy"] = proxy
        except Exception:
            pass

        http_options = types.HttpOptions(
            api_version="v1beta",
            async_client_args=async_client_args,
        )

        return genai.Client(api_key=api_key, http_options=http_options)

    def _build_config(target_lang):
        from google.genai import types
        config = types.LiveConnectConfig.model_construct(
            response_modalities=[types.Modality.AUDIO],
            input_audio_transcription=types.AudioTranscriptionConfig(),
            output_audio_transcription=types.AudioTranscriptionConfig(),
        )
        config.__pydantic_extra__ = {
            "translation_config": {
                "echo_target_language": True,
                "target_language_code": target_lang,
            }
        }
        return config

    async def _run_async():
        nonlocal prev_asr_text, asr_current_text, asr_last_sec
        nonlocal prev_mt_text, mt_current_text, mt_last_sec
        nonlocal session_finished, last_event_time, current_sent_ms

        client = _build_client()
        config = _build_config(gemini_lang_to)

        logger.info("Gemini connecting, model=%s target=%s", GEMINI_MODEL, gemini_lang_to)

        async with client.aio.live.connect(model=GEMINI_MODEL, config=config) as session:
            logger.info("Gemini connected OK")

            if on_stream_start:
                on_stream_start()

            async def send_audio():
                nonlocal current_sent_ms
                for idx in range(total_chunks):
                    if should_stop and should_stop():
                        break
                    chunk = pcm[idx * GEMINI_CHUNK_BYTES:(idx + 1) * GEMINI_CHUNK_BYTES]
                    if not chunk:
                        break
                    try:
                        from google.genai import types as gtypes
                        await session.send_realtime_input(
                            audio=gtypes.Blob(
                                data=chunk,
                                mime_type=f"audio/pcm;rate={SAMPLE_RATE}",
                            )
                        )
                    except Exception as exc:
                        logger.warning("Gemini send failed at chunk %d: %s", idx + 1, exc)
                        break
                    current_sent_ms = (idx + 1) * GEMINI_CHUNK_MS
                    if on_audio_progress:
                        on_audio_progress(current_sent_ms, total_ms)
                    if idx % 50 == 0 or idx < 3:
                        logger.info("Gemini sending chunk %d/%d sent=%dms", idx + 1, total_chunks, current_sent_ms)
                    await asyncio.sleep(GEMINI_CHUNK_MS / 1000.0)

                # Send audio_stream_end
                try:
                    await session.send_realtime_input(audio_stream_end=True)
                    logger.info("Gemini audio_stream_end sent")
                except Exception as exc:
                    logger.warning("Gemini audio_stream_end failed: %s", exc)

                # Wait for tail output
                deadline = time.time() + 15
                while time.time() < deadline:
                    if session_finished:
                        break
                    if time.time() - last_event_time > 4:
                        logger.warning("Gemini: 4s idle, stopping")
                        break
                    await asyncio.sleep(0.2)

            async def receive_responses():
                nonlocal prev_asr_text, asr_current_text, asr_last_sec
                nonlocal prev_mt_text, mt_current_text, mt_last_sec
                nonlocal session_finished, last_event_time, current_sent_ms

                try:
                    async for response in session.receive():
                        server_content = getattr(response, "server_content", None)
                        if not server_content:
                            continue

                        last_event_time = time.time()
                        sec = int(current_sent_ms / 1000)

                        # ASR
                        input_transcription = getattr(server_content, "input_transcription", None)
                        if input_transcription and getattr(input_transcription, "text", None):
                            text = input_transcription.text
                            with lock:
                                append_text_smart(asr_parts, text)
                                asr_current_text = "".join(asr_parts)
                                if asr_last_sec != -1 and sec != asr_last_sec:
                                    flush_asr_delta(current_sent_ms)
                                asr_last_sec = sec
                                rebuild_chunks()

                        # MT
                        output_transcription = getattr(server_content, "output_transcription", None)
                        if output_transcription and getattr(output_transcription, "text", None):
                            text = output_transcription.text
                            with lock:
                                append_text_smart(mt_parts, text)
                                mt_current_text = "".join(mt_parts)
                                if mt_last_sec != -1 and sec != mt_last_sec:
                                    flush_mt_delta(current_sent_ms)
                                mt_last_sec = sec
                                rebuild_chunks()

                        # turn_complete
                        if getattr(server_content, "turn_complete", False):
                            logger.info("Gemini turn_complete received")
                            session_finished = True
                            break

                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.warning("Gemini receive error: %s: %s", type(exc).__name__, exc)

            send_task = asyncio.create_task(send_audio())
            recv_task = asyncio.create_task(receive_responses())

            await send_task
            if not recv_task.done():
                await asyncio.sleep(2)
                recv_task.cancel()
                try:
                    await recv_task
                except asyncio.CancelledError:
                    pass

        # Final flush
        with lock:
            if asr_current_text != prev_asr_text:
                flush_asr_delta(current_sent_ms)
            if mt_current_text != prev_mt_text:
                flush_mt_delta(current_sent_ms)
            rebuild_chunks()

    # Run asyncio in a thread
    def run_in_thread():
        try:
            asyncio.run(_run_async())
        except Exception as exc:
            logger.error("Gemini async error: %s: %s", type(exc).__name__, exc)

    t = threading.Thread(target=run_in_thread, daemon=True)
    t.start()
    t.join(timeout=300)  # max 5 minutes

    return chunks


# ──────────────────────── Dispatcher ────────────────────────

def run_api(
    audio_file: str,
    provider: str,
    api_key: str,
    lang: str = "zh",
    lang_to: str = "en",
    conference_id: str = "",
    on_update: Optional[Callable] = None,
    should_stop: Optional[Callable] = None,
    on_stream_start: Optional[Callable] = None,
    on_audio_progress: Optional[Callable] = None,
    wss_url: str = "",
) -> List[Dict[str, Any]]:
    """Run an external model API and return chunks like grpc_runner."""
    provider = provider.lower()
    if provider == "qwen":
        return _run_qwen_sync(audio_file, api_key, lang, lang_to, conference_id,
                              on_update, should_stop, on_stream_start, on_audio_progress)
    elif provider == "doubao":
        return _run_doubao(audio_file, api_key, lang, lang_to, conference_id,
                           on_update, should_stop, on_stream_start, on_audio_progress)
    elif provider == "huawei":
        return _run_huawei(audio_file, api_key, lang, lang_to, conference_id,
                           on_update, should_stop, on_stream_start, on_audio_progress,
                           wss_url=wss_url)
    elif provider == "openai":
        return _run_openai(audio_file, api_key, lang, lang_to, conference_id,
                           on_update, should_stop, on_stream_start, on_audio_progress)
    elif provider == "gemini":
        return _run_gemini(audio_file, api_key, lang, lang_to, conference_id,
                           on_update, should_stop, on_stream_start, on_audio_progress)
    else:
        raise ValueError(f"Unknown provider: {provider}")

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

    # State
    chunks: List[Dict[str, Any]] = []
    # Per-response streaming state: text grows, stash changes
    # When .done arrives, lock the full text, reset for next response
    asr_locked: List[str] = []   # completed ASR segments
    mt_locked: List[str] = []    # completed MT segments
    asr_streaming = ""           # current response's text (confirmed)
    asr_stash = ""               # current response's stash (prediction)
    mt_streaming = ""
    mt_stash = ""
    session_finished = False
    last_event_time = time.time()
    lock = threading.Lock()
    current_sent_ms = 0            # audio sent so far (updated by send_audio thread)
    seg_start_ms: List[int] = []  # per-segment start: sent_ms when first ASR text arrives
    seg_end_ms: List[int] = []    # per-segment end: sent_ms when MT .done fires

    def push_update():
        if on_update:
            on_update(list(chunks))

    def rebuild_chunks():
        """Rebuild the chunks list from locked + streaming state."""
        new_chunks = []
        max_locked = max(len(asr_locked), len(mt_locked))
        for i in range(max_locked):
            asr_text = asr_locked[i] if i < len(asr_locked) else ""
            mt_text = mt_locked[i] if i < len(mt_locked) else ""
            s = seg_start_ms[i] if i < len(seg_start_ms) else 0
            e = seg_end_ms[i] if i < len(seg_end_ms) else s
            new_chunks.append(_make_chunk(i + 1, i + 1, conference_id,
                                          s, e,
                                          asr_text, mt_text, [f"Qwen #{i+1}"]))
        # Add streaming chunk — only use confirmed text, no stash
        asr_live = asr_streaming
        mt_live = mt_streaming
        if asr_live or mt_live:
            idx = max_locked
            s = seg_start_ms[idx] if idx < len(seg_start_ms) else current_sent_ms
            new_chunks.append(_make_chunk(idx + 1, idx + 1, conference_id,
                                          s, current_sent_ms,
                                          asr_live, mt_live, [f"Qwen #{idx+1} (streaming)"]))
        chunks[:] = new_chunks
        push_update()

    def on_message(ws, message):
        nonlocal asr_streaming, asr_stash, mt_streaming, mt_stash, session_finished, last_event_time, current_sent_ms
        last_event_time = time.time()
        try:
            event = json.loads(message)
        except Exception:
            return
        etype = event.get("type", "")

        # ASR streaming: only use confirmed text (no stash — stash is prediction, changes wildly)
        if etype == "conversation.item.input_audio_transcription.text":
            with lock:
                if not asr_streaming:
                    # First text for this segment — record start
                    seg_start_ms.append(current_sent_ms)
                asr_streaming = (event.get("text") or "").strip()
                rebuild_chunks()

        # ASR final: lock segment, reset streaming
        elif etype == "conversation.item.input_audio_transcription.completed":
            transcript = (event.get("transcript") or "").strip()
            if transcript:
                with lock:
                    asr_locked.append(transcript)
                    asr_streaming = ""
                    asr_stash = ""
                    rebuild_chunks()

        # MT streaming (text mode): text (confirmed) + stash (prediction)
        elif etype == "response.text.text":
            with lock:
                mt_streaming = (event.get("text") or "").strip()
                mt_stash = (event.get("stash") or "").strip()
                rebuild_chunks()

        # MT streaming (audio+text mode)
        elif etype == "response.audio_transcript.text":
            with lock:
                mt_streaming = (event.get("text") or "").strip()
                mt_stash = (event.get("stash") or "").strip()
                rebuild_chunks()

        # MT final (text mode)
        elif etype == "response.text.done":
            text = (event.get("text") or "").strip()
            if text:
                with lock:
                    mt_locked.append(text)
                    mt_streaming = ""
                    mt_stash = ""
                    seg_end_ms.append(current_sent_ms)
                    rebuild_chunks()

        # MT final (audio+text mode)
        elif etype == "response.audio_transcript.done":
            text = (event.get("transcript") or event.get("text") or "").strip()
            if text:
                with lock:
                    mt_locked.append(text)
                    mt_streaming = ""
                    mt_stash = ""
                    seg_end_ms.append(current_sent_ms)
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

        # Send 1s ending silence (helps VAD detect end of speech)
        silence = b"\x00" * QWEN_CHUNK_BYTES
        for i in range(10):
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

        # Wait for session.finished (max 30s, or 5s idle)
        deadline = time.time() + 30
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > 5:
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
    else:
        raise ValueError(f"Unknown provider: {provider}")

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
    """Qwen real-time translation via WebSocket (sync wrapper)."""
    return asyncio.run(_run_qwen_async(
        audio_file, api_key, lang, lang_to, conference_id,
        on_update, should_stop, on_stream_start, on_audio_progress,
    ))


async def _run_qwen_async(
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
    import websockets

    pcm = _read_pcm(audio_file)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / QWEN_CHUNK_BYTES)

    # State
    source_segments: List[str] = []
    mt_segments: List[str] = []
    chunks: List[Dict[str, Any]] = []
    mt_index = 0  # index of next MT segment to pair
    session_finished = False
    last_event_time = time.time()
    # Streaming partial text — store only the NEW portion (not full cumulative)
    current_asr_partial = ""  # incremental text for current in-progress ASR segment
    current_mt_partial = ""   # incremental text for current in-progress MT segment
    last_asr_full = ""  # last full cumulative ASR text from Qwen (for diff)
    last_mt_full = ""   # last full cumulative MT text from Qwen (for diff)

    def push_update():
        if on_update:
            on_update(list(chunks))

    def _extract_new(old_full, new_full):
        """Extract the new portion from a cumulative partial text.
        
        Qwen sends the full cumulative text each time (e.g. 'First' → 'First of' → 'First of all').
        We need to find what's new compared to last time.
        If new_full starts with old_full, just take the suffix.
        Otherwise (text was corrected), find the common prefix and take the rest.
        """
        if not old_full:
            return new_full
        if new_full.startswith(old_full):
            return new_full[len(old_full):]
        # Find common prefix length
        common = 0
        for i in range(min(len(old_full), len(new_full))):
            if old_full[i] != new_full[i]:
                break
            common = i + 1
        # Take everything after the common prefix as "new"
        return new_full[common:]

    def try_pair():
        """Pair ASR segments with MT segments by index, push chunks."""
        nonlocal mt_index, current_asr_partial, current_mt_partial
        while mt_index < len(source_segments) and mt_index < len(mt_segments):
            idx = mt_index
            asr_text = source_segments[idx]
            mt_text = mt_segments[idx]
            start_ms = int(idx * 2000)
            end_ms = int((idx + 1) * 2000)
            chunk = _make_chunk(idx + 1, idx + 1, conference_id, start_ms, end_ms,
                                asr_text, mt_text, [f"Qwen #{idx+1}"])
            if idx < len(chunks):
                chunks[idx] = chunk
            else:
                chunks.append(chunk)
            mt_index += 1
            push_update()

    def push_partial():
        """Push current partial text as the last chunk (streaming, not locked)."""
        nonlocal last_asr_full, last_mt_full
        idx = max(len(source_segments), len(mt_segments), len(chunks))
        
        asr_text = current_asr_partial
        mt_text = current_mt_partial
        
        if not asr_text and not mt_text:
            return
        chunk = _make_chunk(idx + 1, idx + 1, conference_id,
                            int(idx * 2000), int((idx + 1) * 2000),
                            asr_text, mt_text, [f"Qwen #{idx+1} (streaming)"])
        if idx < len(chunks):
            chunks[idx] = chunk
        else:
            chunks.append(chunk)
        push_update()

    def update_asr_partial(full_text):
        """Update ASR partial from Qwen's cumulative text, extracting only new portion."""
        nonlocal current_asr_partial, last_asr_full
        new_portion = _extract_new(last_asr_full, full_text)
        if new_portion:
            current_asr_partial += new_portion
        last_asr_full = full_text
        push_partial()

    def update_mt_partial(full_text, is_delta=False, delta=""):
        """Update MT partial from Qwen's cumulative/delta text."""
        nonlocal current_mt_partial, last_mt_full
        if is_delta and delta:
            current_mt_partial += delta
        else:
            new_portion = _extract_new(last_mt_full, full_text)
            if new_portion:
                current_mt_partial += new_portion
            last_mt_full = full_text
        push_partial()

    # Connect
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        ws = await websockets.connect(
            QWEN_API_URL,
            additional_headers=headers,
            ping_interval=None,
            close_timeout=3,
            open_timeout=30,
            max_size=None,
        )
    except TypeError:
        ws = await websockets.connect(
            QWEN_API_URL,
            extra_headers=headers,
            ping_interval=None,
            close_timeout=3,
            open_timeout=30,
            max_size=None,
        )

    try:
        # Configure session — match qwen_demo.py settings
        session_config = {
            "modalities": ["text", "audio"],
            "input_audio_format": "pcm",
            "output_audio_format": "pcm",
            "translation": {"language": lang_to},
            "input_audio_transcription": {"model": "qwen3-asr-flash-realtime"},
        }
        if lang:
            session_config["input_audio_transcription"]["language"] = lang

        await ws.send(json.dumps({
            "event_id": f"evt_{int(time.time()*1000)}",
            "type": "session.update",
            "session": session_config,
        }, ensure_ascii=False))
        await asyncio.sleep(0.8)

        if on_stream_start:
            on_stream_start()

        # Start receiver task
        async def receive_loop():
            nonlocal session_finished, last_event_time, current_asr_partial, current_mt_partial, last_asr_full, last_mt_full
            try:
                async for message in ws:
                    last_event_time = time.time()
                    try:
                        event = json.loads(message)
                    except Exception:
                        continue
                    etype = event.get("type", "")

                    # ASR streaming partial — Qwen sends cumulative text, we extract new portion
                    if etype == "conversation.item.input_audio_transcription.text":
                        text = (event.get("text") or "")
                        stash = (event.get("stash") or "")
                        full = (text + stash).strip()
                        if full:
                            update_asr_partial(full)

                    # ASR completed — lock this segment, reset partial state
                    elif etype == "conversation.item.input_audio_transcription.completed":
                        transcript = (event.get("transcript") or "").strip()
                        if transcript:
                            source_segments.append(transcript)
                            current_asr_partial = ""
                            last_asr_full = ""
                            if chunks:
                                chunks[-1]["asr"] = transcript
                            try_pair()
                            push_update()

                    # MT streaming partial (text+stash mode — cumulative)
                    elif etype == "response.audio_transcript.text":
                        text = (event.get("text") or "")
                        stash = (event.get("stash") or "")
                        full = (text + stash).strip()
                        if full:
                            update_mt_partial(full)

                    # MT streaming delta (incremental, not cumulative)
                    elif etype == "response.audio_transcript.delta":
                        delta = (event.get("delta") or "").strip()
                        if delta:
                            update_mt_partial("", is_delta=True, delta=delta)

                    # MT completed (primary path)
                    elif etype == "response.audio_transcript.done":
                        text = (event.get("transcript") or event.get("text") or "").strip()
                        if text:
                            mt_segments.append(text)
                            current_mt_partial = ""
                            last_mt_full = ""
                            if chunks:
                                chunks[-1]["mt"] = text
                            try_pair()
                            push_update()

                    # MT streaming delta (fallback text path)
                    elif etype == "response.text.delta":
                        delta = (event.get("delta") or "").strip()
                        if delta:
                            update_mt_partial("", is_delta=True, delta=delta)

                    # MT completed (fallback path)
                    elif etype == "response.text.done":
                        text = (event.get("text") or "").strip()
                        if text:
                            mt_segments.append(text)
                            current_mt_partial = ""
                            last_mt_full = ""
                            if chunks:
                                chunks[-1]["mt"] = text
                            try_pair()
                            push_update()

                    elif etype == "session.finished":
                        session_finished = True

                    elif etype == "error":
                        logger.warning("Qwen error event: %s", event)

            except Exception as exc:
                logger.warning("Qwen receiver stopped: %s", exc)

        receiver = asyncio.create_task(receive_loop())

        # Send audio in chunks
        offset = 0
        sent_ms = 0
        for idx in range(total_chunks):
            if should_stop and should_stop():
                break
            chunk = pcm[offset:offset + QWEN_CHUNK_BYTES]
            if not chunk:
                break
            await ws.send(json.dumps({
                "event_id": f"evt_{int(time.time()*1000)}_{idx}",
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(chunk).decode("utf-8"),
            }, ensure_ascii=False))
            offset += QWEN_CHUNK_BYTES
            sent_ms = int(offset / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
            if on_audio_progress:
                on_audio_progress(sent_ms, total_ms)
            if idx % 50 == 0 or idx < 3:
                logger.info("Qwen sending chunk %d/%d  sent=%dms", idx + 1, total_chunks, sent_ms)
            await asyncio.sleep(QWEN_CHUNK_MS / 1000.0)

        # Send ending silence + finish
        silence = b"\x00" * QWEN_CHUNK_BYTES * 20  # 2s silence
        for i in range(20):
            await ws.send(json.dumps({
                "event_id": f"evt_silence_{i}",
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(silence[i * QWEN_CHUNK_BYTES:(i+1) * QWEN_CHUNK_BYTES]).decode("utf-8"),
            }, ensure_ascii=False))
            await asyncio.sleep(0.1)

        await ws.send(json.dumps({"event_id": "evt_finish", "type": "session.finish"}))
        logger.info("Qwen session.finish sent, waiting for results...")

        # Wait for session.finished or timeout
        deadline = time.time() + 60
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > 15:
                break
            await asyncio.sleep(0.5)

        receiver.cancel()
        try:
            await receiver
        except asyncio.CancelledError:
            pass

        # Final pairing pass
        try_pair()

        # If we have ASR but no MT, still create chunks
        for i in range(len(source_segments)):
            if i >= len(chunks):
                asr_text = source_segments[i]
                mt_text = mt_segments[i] if i < len(mt_segments) else ""
                chunk = _make_chunk(i + 1, i + 1, conference_id,
                                    int(i * 2000), int((i + 1) * 2000),
                                    asr_text, mt_text, [f"Qwen #{i+1}"])
                chunks.append(chunk)
        push_update()

    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return chunks


# ──────────────────────── Doubao (TBD) ────────────────────────

def _run_doubao(audio_file, api_key, lang, lang_to, conference_id, on_update, should_stop, on_stream_start, on_audio_progress):
    """Doubao: TBD — needs Volcano Engine SDK / API docs."""
    if on_stream_start:
        on_stream_start()
    chunk = _make_chunk(1, 1, conference_id, 0, 0,
                        "豆包同传服务待接入",
                        "Doubao (Seed LiveInterpret 2.0) 需要火山引擎 SDK,待后续接入",
                        ["Doubao: 待接入"])
    chunks = [chunk]
    if on_update:
        on_update(chunks)
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
) -> List[Dict[str, Any]]:
    """Run an external model API and return chunks like grpc_runner."""
    provider = provider.lower()
    if provider == "qwen":
        return _run_qwen_sync(audio_file, api_key, lang, lang_to, conference_id,
                              on_update, should_stop, on_stream_start, on_audio_progress)
    elif provider == "doubao":
        return _run_doubao(audio_file, api_key, lang, lang_to, conference_id,
                           on_update, should_stop, on_stream_start, on_audio_progress)
    else:
        raise ValueError(f"Unknown provider: {provider}")

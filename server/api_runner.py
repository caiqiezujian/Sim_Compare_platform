"""External model API runner for SimCompare.

Supports OpenAI, Gemini, Qwen, and Doubao APIs.
Each provider transcribes audio (ASR) + translates (MT),
returning chunks in the same format as grpc_runner.

Audio format matches gRPC: 16kHz / mono / 16-bit PCM.
No debug log functionality — external APIs don't expose that.
"""
import base64
import json
import re
import subprocess
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional

import requests

SAMPLE_RATE = 16000
_TIMEOUT = 300


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


def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


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


def _parse_json_array(text: str) -> list:
    """Extract a JSON array from text that might have markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            return json.loads(match.group())
    return []


# ──────────────────────── OpenAI ────────────────────────

def _run_openai(audio_file, api_key, lang, lang_to, conference_id, on_update, should_stop, on_stream_start, on_audio_progress):
    """OpenAI: Whisper ASR (with timestamps) + GPT-4o-mini MT (per segment)."""
    headers = {"Authorization": f"Bearer {api_key}"}
    lang_name = {"zh": "Chinese", "en": "English"}.get(lang, lang)
    target_name = {"zh": "Chinese", "en": "English"}.get(lang_to, lang_to)

    # Step 1: ASR — Whisper returns segments with timestamps
    with open(audio_file, "rb") as f:
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers=headers,
            files={"file": f},
            data={"model": "whisper-1", "response_format": "verbose_json", "language": lang},
            timeout=_TIMEOUT,
        )
    resp.raise_for_status()
    asr_data = resp.json()
    segments = asr_data.get("segments", [])
    total_ms = int(float(asr_data.get("duration", 0)) * 1000)

    if on_stream_start:
        on_stream_start()

    chunks = []
    for i, seg in enumerate(segments):
        if should_stop and should_stop():
            break
        asr_text = seg["text"].strip()
        start_ms = int(seg["start"] * 1000)
        end_ms = int(seg["end"] * 1000)

        if on_audio_progress:
            on_audio_progress(end_ms, total_ms)

        # Step 2: MT — translate each segment
        mt_text = ""
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={**headers, "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": f"Translate from {lang_name} to {target_name}. Output only the translation."},
                        {"role": "user", "content": asr_text},
                    ],
                    "temperature": 0,
                },
                timeout=60,
            )
            r.raise_for_status()
            mt_text = r.json()["choices"][0]["message"]["content"].strip()
        except Exception as exc:
            mt_text = f"[MT error: {exc}]"

        chunk = _make_chunk(i + 1, i + 1, conference_id, start_ms, end_ms, asr_text, mt_text,
                            [f"OpenAI Whisper #{i+1}", "GPT-4o-mini MT"])
        chunks.append(chunk)
        if on_update:
            on_update(list(chunks))

    return chunks


# ──────────────────────── Gemini ────────────────────────

def _run_gemini(audio_file, api_key, lang, lang_to, conference_id, on_update, should_stop, on_stream_start, on_audio_progress):
    """Gemini 1.5 Flash: audio input, returns ASR + MT as JSON in one call."""
    wav_path = _ensure_wav(audio_file)
    audio_b64 = base64.b64encode(_read_file_bytes(wav_path)).decode()
    lang_name = {"zh": "Chinese", "en": "English"}.get(lang, lang)
    target_name = {"zh": "Chinese", "en": "English"}.get(lang_to, lang_to)

    prompt = (
        f"Transcribe this audio in {lang_name} and translate each sentence to {target_name}. "
        f'Return ONLY a JSON array: [{{"asr":"transcription","mt":"translation","start":0.0,"end":2.5}}] '
        f"Split by sentences. Include timestamps in seconds."
    )

    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}",
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": "audio/wav", "data": audio_b64}},
            ]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
        },
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    segments = _parse_json_array(text)

    if on_stream_start:
        on_stream_start()

    chunks = []
    for i, seg in enumerate(segments):
        if should_stop and should_stop():
            break
        asr_text = str(seg.get("asr", "")).strip()
        mt_text = str(seg.get("mt", "")).strip()
        start_ms = int(float(seg.get("start", 0)) * 1000)
        end_ms = int(float(seg.get("end", 0)) * 1000)
        if on_audio_progress:
            on_audio_progress(end_ms, end_ms)
        chunk = _make_chunk(i + 1, i + 1, conference_id, start_ms, end_ms, asr_text, mt_text,
                            [f"Gemini 1.5 Flash #{i+1}"])
        chunks.append(chunk)
        if on_update:
            on_update(list(chunks))

    return chunks


# ──────────────────────── Qwen ────────────────────────

def _run_qwen(audio_file, api_key, lang, lang_to, conference_id, on_update, should_stop, on_stream_start, on_audio_progress):
    """Qwen-Audio-Turbo via DashScope: audio input, returns ASR + MT in one call."""
    wav_path = _ensure_wav(audio_file)
    audio_b64 = base64.b64encode(_read_file_bytes(wav_path)).decode()
    lang_name = {"zh": "中文", "en": "English"}.get(lang, lang)
    target_name = {"zh": "中文", "en": "English"}.get(lang_to, lang_to)

    prompt = (
        f'请转录这段{lang_name}音频并翻译为{target_name}。'
        f'返回JSON数组:[{{"asr":"转录文本","mt":"翻译文本","start":0.0,"end":2.5}}]，按句子分割，包含时间戳(秒)。'
    )

    resp = requests.post(
        "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": "qwen-audio-turbo",
            "input": {"messages": [{"role": "user", "content": [
                {"audio": f"data:audio/wav;base64,{audio_b64}"},
                {"text": prompt},
            ]}]},
        },
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    text = data["output"]["choices"][0]["message"]["content"]
    segments = _parse_json_array(text)

    if on_stream_start:
        on_stream_start()

    chunks = []
    for i, seg in enumerate(segments):
        if should_stop and should_stop():
            break
        asr_text = str(seg.get("asr", "")).strip()
        mt_text = str(seg.get("mt", "")).strip()
        start_ms = int(float(seg.get("start", 0)) * 1000)
        end_ms = int(float(seg.get("end", 0)) * 1000)
        if on_audio_progress:
            on_audio_progress(end_ms, end_ms)
        chunk = _make_chunk(i + 1, i + 1, conference_id, start_ms, end_ms, asr_text, mt_text,
                            [f"Qwen-Audio-Turbo #{i+1}"])
        chunks.append(chunk)
        if on_update:
            on_update(list(chunks))

    return chunks


# ──────────────────────── Doubao ────────────────────────

def _run_doubao(audio_file, api_key, lang, lang_to, conference_id, on_update, should_stop, on_stream_start, on_audio_progress):
    """Doubao: Volcano Engine Ark API (OpenAI-compatible).

    Doubao LLM 不直接支持音频输入，需要分两步：
    1. 用支持音频的模型（如 doubao-1.5-vision-pro）做 ASR
    2. 用 doubao-pro 做 MT
    如果 Ark API 不支持音频，返回提示信息。
    """
    wav_path = _ensure_wav(audio_file)
    audio_b64 = base64.b64encode(_read_file_bytes(wav_path)).decode()
    lang_name = {"zh": "Chinese", "en": "English"}.get(lang, lang)
    target_name = {"zh": "Chinese", "en": "English"}.get(lang_to, lang_to)

    # Try using Ark API with audio input (OpenAI-compatible format)
    # Some Doubao models support audio via input_audio type
    try:
        resp = requests.post(
            "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "doubao-1.5-pro-32k-250115",
                "messages": [
                    {"role": "system", "content": (
                        f"You are a simultaneous interpreter. Transcribe the audio in {lang_name} "
                        f"and translate to {target_name}. "
                        f'Return ONLY a JSON array: [{{"asr":"text","mt":"text","start":0.0,"end":2.5}}]'
                    )},
                    {"role": "user", "content": [
                        {"type": "text", "text": "Transcribe and translate this audio."},
                        {"type": "input_audio", "input_audio": {"data": f"data:audio/wav;base64,{audio_b64}"}},
                    ]},
                ],
                "temperature": 0,
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        segments = _parse_json_array(text)
    except Exception:
        # Fallback: Doubao doesn't support audio via Ark API
        # Need separate Volcano Engine ASR (requires app_id + access_token)
        if on_stream_start:
            on_stream_start()
        chunk = _make_chunk(1, 1, conference_id, 0, 0,
                            "Doubao ASR 需要火山引擎语音识别服务",
                            "豆包 LLM 不支持音频输入，需配置火山引擎 ASR (app_id + access_token) 后才能使用",
                            ["Doubao: Ark API 不支持音频输入，需要单独配置 ASR"])
        chunks = [chunk]
        if on_update:
            on_update(chunks)
        return chunks

    if on_stream_start:
        on_stream_start()

    chunks = []
    for i, seg in enumerate(segments):
        if should_stop and should_stop():
            break
        asr_text = str(seg.get("asr", "")).strip()
        mt_text = str(seg.get("mt", "")).strip()
        start_ms = int(float(seg.get("start", 0)) * 1000)
        end_ms = int(float(seg.get("end", 0)) * 1000)
        if on_audio_progress:
            on_audio_progress(end_ms, end_ms)
        chunk = _make_chunk(i + 1, i + 1, conference_id, start_ms, end_ms, asr_text, mt_text,
                            [f"Doubao #{i+1}"])
        chunks.append(chunk)
        if on_update:
            on_update(list(chunks))

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
    runners = {
        "openai": _run_openai,
        "gemini": _run_gemini,
        "qwen": _run_qwen,
        "doubao": _run_doubao,
    }
    runner = runners.get(provider)
    if not runner:
        raise ValueError(f"Unknown provider: {provider}")
    return runner(audio_file, api_key, lang, lang_to, conference_id,
                  on_update, should_stop, on_stream_start, on_audio_progress)

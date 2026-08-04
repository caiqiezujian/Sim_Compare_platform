"""Gemini Live Translate probe — delta segmentation test.

Goal: verify per-second delta segmentation for timeline display.
Gemini's text is CUMULATIVE (like Qwen) — we extract delta = text[len(prev):].

Protocol:
  - SDK: google-genai (asyncio)
  - client.aio.live.connect(model=gemini-3.5-live-translate-preview, config=...)
  - session.send_realtime_input(audio=Blob) per 100ms chunk
  - session.send_realtime_input(audio_stream_end=True) after audio
  - Receive: response.server_content with input_transcription / output_transcription / turn_complete

Usage:
    python scripts/gemini_probe.py --audio test.wav --lang zh --lang-to en

Environment:
    Set GEMINI_API_KEY or pass --api-key
    Proxy: set HTTP_PROXY / HTTPS_PROXY before running
"""
import argparse
import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
import time

# ─── Set proxy env BEFORE importing google-genai ───
PROXY_URL = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
os.environ["HTTP_PROXY"] = PROXY_URL
os.environ["HTTPS_PROXY"] = PROXY_URL
os.environ["ALL_PROXY"] = PROXY_URL
os.environ["http_proxy"] = PROXY_URL
os.environ["https_proxy"] = PROXY_URL
os.environ["NO_PROXY"] = ""
os.environ["no_proxy"] = ""

from google import genai
from google.genai import types

# ─── Defaults ───
MODEL = "gemini-3.5-live-translate-preview"
SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2
CHANNELS = 1
CHUNK_MS = 100
CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS * CHUNK_MS / 1000)  # 3200

DRAIN_SECONDS_AFTER_SEND = 15.0
IDLE_SECONDS_AFTER_SEND = 4.0


def get_api_key():
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        key = os.environ.get("GOOGLE_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "No GEMINI_API_KEY found.\n"
            "PowerShell: $env:GEMINI_API_KEY='...'\n"
            "Or pass: --api-key ..."
        )
    return key


def read_pcm_16k(path):
    """Convert any audio to 16kHz/mono/16-bit PCM."""
    import wave
    if path.lower().endswith((".wav", ".wave")):
        with wave.open(path, "rb") as r:
            if r.getframerate() == SAMPLE_RATE and r.getnchannels() == 1 and r.getsampwidth() == 2:
                return r.readframes(r.getnframes())
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    subprocess.run(
        ["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", str(SAMPLE_RATE),
         "-sample_fmt", "s16", tmp],
        check=True, capture_output=True,
    )
    with wave.open(tmp, "rb") as r:
        data = r.readframes(r.getnframes())
    os.unlink(tmp)
    return data


def append_text_smart(parts, text):
    """Smart append: handles cumulative text (from Gemini.py)."""
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


# ─── Probe state ───
start_time = time.time()

# Delta extraction state (same as Qwen probe2)
prev_asr_text = ""
prev_mt_text = ""
asr_current_text = ""
mt_current_text = ""
asr_last_sec = -1
mt_last_sec = -1

asr_segs = []   # (sent_ms, delta_text)
mt_segs = []
asr_mt_end = []  # asr_mt_end[i] = len(mt_segs) when ASR seg #i locked

# Track full text for reconstruction
asr_parts = []
mt_parts = []

session_finished = False
last_useful_content_time = time.time()

# Event stats
event_counts = {}
first_asr_time = None
first_mt_time = None


def flush_asr_delta(sent_ms):
    global prev_asr_text, asr_current_text
    if asr_current_text and asr_current_text != prev_asr_text:
        delta = asr_current_text[len(prev_asr_text):]
        if delta.strip():
            asr_segs.append((sent_ms, delta))
            asr_mt_end.append(len(mt_segs))
            print(f"  [ASR SEG #{len(asr_segs)}] t={int(sent_ms/1000)}s  delta={delta!r}")
    prev_asr_text = asr_current_text


def flush_mt_delta(sent_ms):
    global prev_mt_text, mt_current_text
    if mt_current_text and mt_current_text != prev_mt_text:
        delta = mt_current_text[len(prev_mt_text):]
        if delta.strip():
            mt_segs.append((sent_ms, delta))
            print(f"  [MT  SEG #{len(mt_segs)}] t={int(sent_ms/1000)}s  delta={delta!r}")
    prev_mt_text = mt_current_text


def print_timeline():
    """Print final delta segmentation and pairing table."""
    print("\n" + "=" * 80)
    print("DELTA SEGMENTATION SUMMARY")
    print("=" * 80)

    print(f"\nEvent counts: {json.dumps(event_counts, indent=2)}")
    if first_asr_time:
        print(f"First ASR text at: {first_asr_time - start_time:.1f}s")
    if first_mt_time:
        print(f"First MT  text at: {first_mt_time - start_time:.1f}s")

    print(f"\nASR segments: {len(asr_segs)}")
    for i, (ms, delta) in enumerate(asr_segs):
        print(f"  #{i+1:3d}  t={int(ms/1000):4d}s  {delta!r}")

    print(f"\nMT segments: {len(mt_segs)}")
    for i, (ms, delta) in enumerate(mt_segs):
        print(f"  #{i+1:3d}  t={int(ms/1000):4d}s  {delta!r}")

    # Pairing table
    print("\n" + "-" * 80)
    print("TIMELINE PAIRING (ASR + MT by segment index)")
    print("-" * 80)
    max_segs = max(len(asr_segs), len(mt_segs))
    print(f"{'#':>4}  {'ASR time':>8}  {'MT range':>10}  {'ASR delta':<40}  {'MT delta'}")
    for i in range(max_segs):
        asr_ms, asr_delta = asr_segs[i] if i < len(asr_segs) else ("", "")
        prev_end = asr_mt_end[i - 1] if i > 0 and i - 1 < len(asr_mt_end) else 0
        this_end = asr_mt_end[i] if i < len(asr_mt_end) else len(mt_segs)
        mt_text = "".join(d for _, d in mt_segs[prev_end:this_end])
        if not mt_text and i >= len(asr_segs):
            mt_ms, mt_delta = mt_segs[i] if i < len(mt_segs) else ("", "")
            mt_text = mt_delta
            asr_ms = ""
        asr_short = (asr_delta[:38] + "..") if len(asr_delta) > 40 else asr_delta
        mt_short = (mt_text[:38] + "..") if len(mt_text) > 40 else mt_text
        print(f"{i+1:>4}  {str(asr_ms):>8}  {str(prev_end)}..{str(this_end):>4}  {asr_short:<40}  {mt_short}")

    # Reconstruction check
    print("\n" + "-" * 80)
    print("RECONSTRUCTION CHECK")
    print("-" * 80)
    asr_reconstructed = "".join(d for _, d in asr_segs)
    mt_reconstructed = "".join(d for _, d in mt_segs)
    print(f"ASR reconstructed ({len(asr_reconstructed)} chars):")
    print(f"  {asr_reconstructed!r}")
    print(f"MT  reconstructed ({len(mt_reconstructed)} chars):")
    print(f"  {mt_reconstructed!r}")
    print(f"\nTotal: {len(asr_segs)} ASR segments, {len(mt_segs)} MT segments")
    print("[probe] done.")


def build_client(api_key):
    import ssl
    import inspect

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
        if "proxy" in sig.parameters:
            async_client_args["proxy"] = PROXY_URL
    except Exception:
        pass

    http_options = types.HttpOptions(
        api_version="v1beta",
        async_client_args=async_client_args,
    )

    return genai.Client(
        api_key=api_key,
        http_options=http_options,
    )


def build_live_config(target_language):
    """Build LiveConnectConfig — uses model_construct to bypass Pydantic validation
    for translation_config (not available in google-genai 1.47.0 / Python 3.9)."""
    config = types.LiveConnectConfig.model_construct(
        response_modalities=[types.Modality.AUDIO],
        input_audio_transcription=types.AudioTranscriptionConfig(),
        output_audio_transcription=types.AudioTranscriptionConfig(),
    )
    # Set extra field via __pydantic_extra__ — SDK will serialize it to the API
    config.__pydantic_extra__ = {
        "translation_config": {
            "echo_target_language": True,
            "target_language_code": target_language,
        }
    }
    return config


async def run_probe(audio_path, target_language, api_key):
    global prev_asr_text, asr_current_text, asr_last_sec
    global prev_mt_text, mt_current_text, mt_last_sec
    global session_finished, last_useful_content_time, start_time
    global first_asr_time, first_mt_time, event_counts

    print(f"[probe] converting audio to 16kHz PCM...")
    pcm = read_pcm_16k(audio_path)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = (len(pcm) + CHUNK_BYTES - 1) // CHUNK_BYTES

    print(f"[probe] duration: {total_ms}ms  chunks: {total_chunks} ({CHUNK_MS}ms each)")
    print(f"[probe] target_language: {target_language}")
    print(f"[probe] model: {MODEL}")
    print(f"[probe] proxy: {PROXY_URL}")
    print(f"[probe] API key: {api_key[:6]}...{api_key[-4:]}")

    client = build_client(api_key)
    config = build_live_config(target_language)

    print(f"[probe] connecting to Gemini Live...")

    async with client.aio.live.connect(model=MODEL, config=config) as session:
        print(f"[probe] connected OK!")

        current_sent_ms = 0
        start_time = time.time()

        async def send_audio():
            nonlocal current_sent_ms
            global last_useful_content_time

            for idx in range(total_chunks):
                if session_finished:
                    break
                chunk = pcm[idx * CHUNK_BYTES:(idx + 1) * CHUNK_BYTES]
                if not chunk:
                    break
                try:
                    await session.send_realtime_input(
                        audio=types.Blob(
                            data=chunk,
                            mime_type=f"audio/pcm;rate={SAMPLE_RATE}",
                        )
                    )
                except Exception as exc:
                    print(f"[send] failed at chunk {idx+1}: {exc}")
                    break
                current_sent_ms = (idx + 1) * CHUNK_MS
                if idx % 50 == 0 or idx < 3:
                    print(f"[send] chunk {idx+1}/{total_chunks}  sent={current_sent_ms}ms")
                await asyncio.sleep(CHUNK_MS / 1000.0)

            # Send audio_stream_end
            try:
                await session.send_realtime_input(audio_stream_end=True)
                print("[send] audio_stream_end=True sent")
            except Exception as exc:
                print(f"[WARNING] audio_stream_end failed: {exc}")

            # Wait for tail outputs
            print(f"[probe] waiting up to {DRAIN_SECONDS_AFTER_SEND}s for tail output...")
            wait_start = time.time()
            while True:
                if time.time() - wait_start >= DRAIN_SECONDS_AFTER_SEND:
                    print("[probe] drain timeout reached")
                    break
                idle = time.time() - (last_useful_content_time or wait_start)
                if time.time() - start_time > total_ms / 1000 and idle >= IDLE_SECONDS_AFTER_SEND:
                    print(f"[probe] {IDLE_SECONDS_AFTER_SEND}s idle, stopping")
                    break
                await asyncio.sleep(0.2)

        async def receive_responses():
            global prev_asr_text, asr_current_text, asr_last_sec
            global prev_mt_text, mt_current_text, mt_last_sec
            global session_finished, last_useful_content_time
            global first_asr_time, first_mt_time, event_counts

            try:
                async for response in session.receive():
                    server_content = getattr(response, "server_content", None)
                    if not server_content:
                        continue

                    now = time.time()
                    etype = "server_content"
                    event_counts[etype] = event_counts.get(etype, 0) + 1
                    sec = int(current_sent_ms / 1000) if 'current_sent_ms' in dir() else int((now - start_time))

                    # Input transcription (ASR)
                    input_transcription = getattr(server_content, "input_transcription", None)
                    if input_transcription and getattr(input_transcription, "text", None):
                        text = input_transcription.text
                        if first_asr_time is None:
                            first_asr_time = time.time()
                            print(f"[probe] *** FIRST ASR at {first_asr_time - start_time:.1f}s ***")

                        # Use smart append to get cumulative text
                        append_text_smart(asr_parts, text)
                        asr_current_text = "".join(asr_parts)

                        # Delta extraction (per second)
                        if asr_last_sec != -1 and sec != asr_last_sec:
                            flush_asr_delta(current_sent_ms if 'current_sent_ms' in dir() else int((now - start_time) * 1000))
                        asr_last_sec = sec

                        last_useful_content_time = now
                        print(f"[recv {now-start_time:.1f}s] ASR: {text!r}")

                    # Output transcription (MT)
                    output_transcription = getattr(server_content, "output_transcription", None)
                    if output_transcription and getattr(output_transcription, "text", None):
                        text = output_transcription.text
                        if first_mt_time is None:
                            first_mt_time = time.time()
                            print(f"[probe] *** FIRST MT at {first_mt_time - start_time:.1f}s ***")

                        append_text_smart(mt_parts, text)
                        mt_current_text = "".join(mt_parts)

                        if mt_last_sec != -1 and sec != mt_last_sec:
                            flush_mt_delta(current_sent_ms if 'current_sent_ms' in dir() else int((now - start_time) * 1000))
                        mt_last_sec = sec

                        last_useful_content_time = now
                        print(f"[recv {now-start_time:.1f}s] MT: {text!r}")

                    # Check turn_complete
                    if getattr(server_content, "turn_complete", False):
                        print(f"[probe] turn_complete received at {now-start_time:.1f}s")
                        session_finished = True
                        break

            except asyncio.CancelledError:
                pass
            except Exception as exc:
                print(f"[ERROR] receive: {type(exc).__name__}: {exc}")

        # Run send and receive concurrently
        send_task = asyncio.create_task(send_audio())
        recv_task = asyncio.create_task(receive_responses())

        await send_task

        # Wait a bit more for receive to finish
        if not recv_task.done():
            await asyncio.sleep(2)
            recv_task.cancel()
            try:
                await recv_task
            except asyncio.CancelledError:
                pass

    # Final flush
    if asr_current_text != prev_asr_text:
        flush_asr_delta(0)
    if mt_current_text != prev_mt_text:
        flush_mt_delta(0)

    print_timeline()


def main():
    parser = argparse.ArgumentParser(description="Gemini Live Translate probe")
    parser.add_argument("--audio", required=True, help="Audio file (wav/mp3/etc)")
    parser.add_argument("--lang-to", default="en", help="Target language code (en/zh-Hans/ja/ko)")
    parser.add_argument("--api-key", default="", help="Gemini API key (or set GEMINI_API_KEY env)")
    args = parser.parse_args()

    if not os.path.exists(args.audio):
        print(f"ERROR: file not found: {args.audio}")
        sys.exit(1)

    if args.api_key:
        os.environ["GEMINI_API_KEY"] = args.api_key

    api_key = get_api_key()

    print(f"[probe] Gemini Live Translate probe")
    print(f"[probe] audio: {args.audio}")
    print(f"[probe] lang-to: {args.lang_to}")

    asyncio.run(run_probe(args.audio, args.lang_to, api_key))


if __name__ == "__main__":
    main()

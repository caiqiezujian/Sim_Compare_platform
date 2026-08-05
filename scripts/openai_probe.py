"""OpenAI GPT-Realtime-Translate probe — delta segmentation test.

Goal: verify per-second delta segmentation for timeline display.
OpenAI sends INCREMENTAL deltas (each event = only new text), unlike
Qwen's cumulative text. This makes segmentation simpler.

Protocol:
  - Connect: wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate
  - Auth: Authorization: Bearer <key>, OpenAI-Safety-Identifier header
  - session.update: input.transcription.model=gpt-realtime-whisper, output.language=target
  - Send audio: 24kHz/mono/16-bit, 200ms chunks, base64
  - ASR: session.input_transcript.delta (incremental) / .done (final)
  - MT:  session.output_transcript.delta (incremental) / .done (final)
  - Close: session.close → session.closed

Segmentation:
  - Per-second: accumulate delta text, on second-change lock as a segment
  - ASR is the anchor — each ASR segment pairs with all MT deltas accumulated
    since the previous ASR segment (same as Qwen probe2)

Output:
  - All terminal output is simultaneously saved to scripts/openai_probe_output.txt
  - Overwritten each run

Usage:
    python scripts/openai_probe.py --audio test.wav --lang zh --lang-to en
    python scripts/openai_probe.py --audio test.wav --lang en --lang-to zh --api-key sk-...

Environment:
    Set OPENAI_API_KEY or pass --api-key
    Proxy: set HTTP_PROXY / HTTPS_PROXY (default http://127.0.0.1:7897)
"""
import argparse
import base64
import json
import math
import os
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import websocket

# ─── Tee logger: write to both stdout and file ───
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_PATH = SCRIPT_DIR / "openai_probe_output.txt"

class TeeLogger:
    """Redirect stdout/stderr to both console and a log file."""
    def __init__(self, log_path):
        self.log_path = log_path
        self.log_file = open(log_path, "w", encoding="utf-8")
        self.stdout = sys.stdout
        self.stderr = sys.stderr
        sys.stdout = self
        sys.stderr = self

    def write(self, data):
        self.stdout.write(data)
        self.log_file.write(data)
        self.log_file.flush()

    def flush(self):
        self.stdout.flush()
        self.log_file.flush()

    def close(self):
        sys.stdout = self.stdout
        sys.stderr = self.stderr
        self.log_file.close()

    def isatty(self):
        return False


# ─── Defaults ───
API_URL = "wss://api.openai.com/v1/realtime/translations?model=gpt-realtime-translate"
SAFETY_IDENTIFIER = "simcompare-openai-probe"

SAMPLE_RATE = 24000
BYTES_PER_SAMPLE = 2
CHANNELS = 1
CHUNK_MS = 200
CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS * CHUNK_MS / 1000)  # 9600

TRAILING_SILENCE_SECONDS = 3.0
POST_AUDIO_WAIT_SECONDS = 2.0
DRAIN_TIMEOUT_SECONDS = 30.0
DRAIN_IDLE_SECONDS = 10.0

PROXY_HOST = os.environ.get("HTTP_PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("HTTP_PROXY_PORT", "7897"))


def get_api_key():
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "No OPENAI_API_KEY found.\n"
            "PowerShell: $env:OPENAI_API_KEY='sk-...'\n"
            "Or pass: --api-key sk-..."
        )
    return key


def read_pcm_24k(path):
    """Convert any audio to 24kHz/mono/16-bit PCM."""
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


def extract_text(event):
    """Extract text from OpenAI event — supports delta/done/item formats."""
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


# ─── Probe state ───
start_time = time.time()
current_sent_ms = 0

asr_delta_buffer = ""
mt_delta_buffer = ""
asr_last_sec = -1
mt_last_sec = -1

asr_segs = []
mt_segs = []
asr_mt_end = []

session_finished = False
last_event_time = time.time()

event_counts = {}
asr_delta_count = 0
mt_delta_count = 0
asr_done_count = 0
mt_done_count = 0

first_asr_time = None
first_mt_time = None


def flush_asr_delta():
    global asr_delta_buffer
    if asr_delta_buffer.strip():
        asr_segs.append((current_sent_ms, asr_delta_buffer))
        asr_mt_end.append(len(mt_segs))
        print(f"  [ASR SEG #{len(asr_segs)}] t={int(current_sent_ms/1000)}s  delta={asr_delta_buffer!r}")
    asr_delta_buffer = ""


def flush_mt_delta():
    global mt_delta_buffer
    if mt_delta_buffer.strip():
        mt_segs.append((current_sent_ms, mt_delta_buffer))
        print(f"  [MT  SEG #{len(mt_segs)}] t={int(current_sent_ms/1000)}s  delta={mt_delta_buffer!r}")
    mt_delta_buffer = ""


def on_open(ws):
    print("[probe] connected OK!")

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
                    "language": TARGET_LANGUAGE
                }
            }
        }
    }
    print(f"[send] session.update: {json.dumps(session_update, ensure_ascii=False)}")
    ws.send(json.dumps(session_update, ensure_ascii=False))

    def send_audio():
        global current_sent_ms, start_time
        time.sleep(0.5)
        start_time = time.time()

        for idx in range(TOTAL_CHUNKS):
            if session_finished:
                break
            chunk = PCM_DATA[idx * CHUNK_BYTES:(idx + 1) * CHUNK_BYTES]
            if not chunk:
                break
            try:
                audio_b64 = base64.b64encode(chunk).decode("ascii")
                ws.send(json.dumps({
                    "type": "session.input_audio_buffer.append",
                    "audio": audio_b64,
                }, ensure_ascii=False))
            except Exception as exc:
                print(f"[send] failed at chunk {idx+1}: {exc}")
                break
            current_sent_ms = (idx + 1) * CHUNK_MS
            if idx % 25 == 0 or idx < 3:
                print(f"[send] chunk {idx+1}/{TOTAL_CHUNKS}  sent={current_sent_ms}ms")
            time.sleep(CHUNK_MS / 1000.0)

        # Trailing silence
        silence = b"\x00" * CHUNK_BYTES
        n_silence = int(TRAILING_SILENCE_SECONDS * 1000 / CHUNK_MS)
        print(f"[send] trailing silence: {n_silence} chunks ({TRAILING_SILENCE_SECONDS}s)")
        for i in range(n_silence):
            if session_finished:
                break
            try:
                audio_b64 = base64.b64encode(silence).decode("ascii")
                ws.send(json.dumps({
                    "type": "session.input_audio_buffer.append",
                    "audio": audio_b64,
                }, ensure_ascii=False))
            except Exception:
                break
            time.sleep(CHUNK_MS / 1000.0)

        print(f"[send] audio done, waiting {POST_AUDIO_WAIT_SECONDS}s before close...")
        time.sleep(POST_AUDIO_WAIT_SECONDS)

        try:
            ws.send(json.dumps({"type": "session.close"}))
            print("[send] session.close sent")
        except Exception:
            pass

        deadline = time.time() + DRAIN_TIMEOUT_SECONDS
        while time.time() < deadline:
            if session_finished:
                break
            if time.time() - last_event_time > DRAIN_IDLE_SECONDS:
                print(f"[probe] {DRAIN_IDLE_SECONDS}s idle after close, stopping")
                break
            time.sleep(0.3)

        try:
            ws.close()
        except Exception:
            pass

    threading.Thread(target=send_audio, daemon=True).start()


def on_message(ws, message):
    global asr_delta_buffer, asr_last_sec, mt_delta_buffer, mt_last_sec
    global session_finished, last_event_time, current_sent_ms
    global asr_delta_count, mt_delta_count, asr_done_count, mt_done_count
    global first_asr_time, first_mt_time

    if isinstance(message, bytes):
        return

    last_event_time = time.time()
    try:
        event = json.loads(message)
    except Exception:
        return

    etype = event.get("type", "")
    event_counts[etype] = event_counts.get(etype, 0) + 1
    sec = int(current_sent_ms / 1000)

    # ASR delta
    if etype in ("session.input_transcript.delta", "session.input_audio_transcription.delta",
                 "conversation.item.input_audio_transcription.delta"):
        text = extract_text(event)
        if text:
            asr_delta_count += 1
            if first_asr_time is None:
                first_asr_time = time.time() - start_time
                print(f"[probe] *** FIRST ASR at {first_asr_time:.1f}s ***")
            if asr_last_sec != -1 and sec != asr_last_sec:
                flush_asr_delta()
            asr_last_sec = sec
            asr_delta_buffer += text
            print(f"[recv {time.time()-start_time:.1f}s] ASR delta: {text!r}")

    # ASR done
    elif etype in ("session.input_transcript.done", "session.input_audio_transcription.done",
                   "conversation.item.input_audio_transcription.completed"):
        text = extract_text(event)
        if text:
            asr_done_count += 1
            asr_delta_buffer += text
            flush_asr_delta()
            print(f"[recv {time.time()-start_time:.1f}s] ASR done: {text!r}")

    # MT delta
    elif etype in ("session.output_transcript.delta", "session.output_transcription.delta"):
        text = extract_text(event)
        if text:
            mt_delta_count += 1
            if first_mt_time is None:
                first_mt_time = time.time() - start_time
                print(f"[probe] *** FIRST MT at {first_mt_time:.1f}s ***")
            if mt_last_sec != -1 and sec != mt_last_sec:
                flush_mt_delta()
            mt_last_sec = sec
            mt_delta_buffer += text
            print(f"[recv {time.time()-start_time:.1f}s] MT delta: {text!r}", end="", flush=True)

    # MT done
    elif etype in ("session.output_transcript.done", "session.output_transcription.done"):
        text = extract_text(event)
        if text:
            mt_done_count += 1
            mt_delta_buffer += text
            flush_mt_delta()
            print(f"\n[recv {time.time()-start_time:.1f}s] MT done: {text!r}")

    elif etype == "session.closed":
        session_finished = True
        print(f"[recv {time.time()-start_time:.1f}s] session.closed")

    elif etype == "error":
        print(f"[recv] ERROR: {json.dumps(event, ensure_ascii=False)[:300]}")

    elif etype == "session.created":
        print(f"[recv {time.time()-start_time:.1f}s] session.created")
    elif etype == "session.updated":
        print(f"[recv {time.time()-start_time:.1f}s] session.updated")

    elif etype in {
        "input_audio_buffer.speech_started", "input_audio_buffer.speech_stopped",
        "input_audio_buffer.committed",
    }:
        pass

    else:
        print(f"[recv] UNKNOWN: {etype} {json.dumps(event, ensure_ascii=False)[:200]}")


def on_error(ws, error):
    print(f"[error] {type(error).__name__}: {error}")


def on_close(ws, code, reason):
    print(f"\n[close] code={code} reason={reason}")
    print_timeline()


def print_timeline():
    """Print final delta segmentation and pairing table."""
    print("\n" + "=" * 80)
    print("DELTA SEGMENTATION SUMMARY")
    print("=" * 80)

    print(f"\nEvent counts: {json.dumps(event_counts, indent=2)}")
    print(f"ASR delta events: {asr_delta_count}, done events: {asr_done_count}")
    print(f"MT  delta events: {mt_delta_count}, done events: {mt_done_count}")
    if first_asr_time:
        print(f"First ASR text at: {first_asr_time:.1f}s")
    if first_mt_time:
        print(f"First MT  text at: {first_mt_time:.1f}s")
    if first_asr_time and first_mt_time:
        delay = first_mt_time - first_asr_time
        print(f"MT started {'before' if delay < 0 else 'after'} ASR by {abs(delay):.1f}s")

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


# ─── Globals set in main() ───
AUDIO_PATH = ""
PCM_DATA = b""
TOTAL_MS = 0
TOTAL_CHUNKS = 0
SOURCE_LANG = "zh"
TARGET_LANGUAGE = "en"


def main():
    global AUDIO_PATH, PCM_DATA, TOTAL_MS, TOTAL_CHUNKS, SOURCE_LANG, TARGET_LANGUAGE

    parser = argparse.ArgumentParser(description="OpenAI GPT-Realtime-Translate probe")
    parser.add_argument("--audio", required=True, help="Audio file (wav/mp3/etc)")
    parser.add_argument("--lang", default="zh", help="Source language (record only)")
    parser.add_argument("--lang-to", default="en", help="Target language code (en/zh/ja/ko/es/fr/de)")
    parser.add_argument("--api-key", default="", help="OpenAI API key (or set OPENAI_API_KEY env)")
    args = parser.parse_args()

    if not os.path.exists(args.audio):
        print(f"ERROR: file not found: {args.audio}")
        sys.exit(1)

    if args.api_key:
        os.environ["OPENAI_API_KEY"] = args.api_key

    AUDIO_PATH = args.audio
    SOURCE_LANG = args.lang
    TARGET_LANGUAGE = args.lang_to

    print(f"[probe] OpenAI GPT-Realtime-Translate probe")
    print(f"[probe] audio: {AUDIO_PATH}")
    print(f"[probe] lang={SOURCE_LANG} -> {TARGET_LANGUAGE}")
    print(f"[probe] sample_rate={SAMPLE_RATE}Hz  chunk_ms={CHUNK_MS}")

    print("[probe] converting audio to 24kHz PCM...")
    PCM_DATA = read_pcm_24k(AUDIO_PATH)
    TOTAL_MS = int(len(PCM_DATA) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    TOTAL_CHUNKS = (len(PCM_DATA) + CHUNK_BYTES - 1) // CHUNK_BYTES

    print(f"[probe] duration: {TOTAL_MS}ms  chunks: {TOTAL_CHUNKS}")

    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
    if proxy:
        print(f"[probe] using proxy env: {proxy}")
    print(f"[probe] ws proxy: {PROXY_HOST}:{PROXY_PORT}")

    api_key = get_api_key()
    print(f"[probe] API key: {api_key[:6]}...{api_key[-4:]}")
    print(f"[probe] connecting to {API_URL[:60]}...")
    print(f"[probe] log file: {LOG_PATH}")

    ws = websocket.WebSocketApp(
        API_URL,
        header=[
            f"Authorization: Bearer {api_key}",
            f"OpenAI-Safety-Identifier: {SAFETY_IDENTIFIER}",
        ],
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
    ws.run_forever(
        sslopt=sslopt,
        http_proxy_host=PROXY_HOST,
        http_proxy_port=PROXY_PORT,
        proxy_type="http",
        ping_interval=20,
        ping_timeout=10,
    )


if __name__ == "__main__":
    tee = TeeLogger(LOG_PATH)
    try:
        main()
    finally:
        tee.close()

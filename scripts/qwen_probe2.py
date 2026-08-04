"""Qwen real-time translation probe v2 — delta segmentation test.

Goal: verify that per-second delta extraction produces clean ASR/MT segments
suitable for timeline display (like Doubao's Start/Response/End pattern).

Algorithm:
  - Qwen's `text` field is CUMULATIVE (grows monotonically, never rewrites)
  - Each second, extract delta = text[len(prev_text):] as a new segment
  - ASR and MT segments are extracted independently, then paired by index
  - .completed / .done events force a final flush

Usage:
    python scripts/qwen_probe2.py --audio test.wav --lang zh --lang-to en
"""
import argparse
import base64
import json
import math
import os
import subprocess
import sys
import threading
import time

import websocket

API_KEY = "sk-33c1d03f05744da1bfb55e7aae3c6f28"
API_URL = (
    "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
    "?model=qwen3.5-livetranslate-flash-realtime"
)

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2
CHUNK_MS = 100
CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * CHUNK_MS / 1000)  # 3200


def read_pcm(path):
    import wave
    if path.lower().endswith((".wav", ".wave")):
        with wave.open(path, "rb") as r:
            return r.readframes(r.getnframes())
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    subprocess.run(["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", str(SAMPLE_RATE),
                    "-sample_fmt", "s16", tmp], check=True, capture_output=True)
    with wave.open(tmp, "rb") as r:
        data = r.readframes(r.getnframes())
    os.unlink(tmp)
    return data


# ─── Delta segmentation state ───
# ASR and MT each have independent delta tracking.
# Per-second: collect the latest `text`, and on second-change, lock the delta.

start_time = time.time()
current_sent_ms = 0

# ASR delta state
asr_prev_text = ""           # text locked at previous second boundary
asr_current_text = ""        # latest text seen this second
asr_last_sec = -1            # integer second of last ASR event
asr_segments = []            # list of (sec, delta_text)

# MT delta state
mt_prev_text = ""
mt_current_text = ""
mt_last_sec = -1
mt_segments = []

# Track whether we've seen any text at all (for start-time logging)
asr_first_text_time = None
mt_first_text_time = None

session_finished = False


def flush_asr(sec):
    """Lock the delta for the previous second and append to segments."""
    global asr_prev_text, asr_current_text
    if asr_current_text and asr_current_text != asr_prev_text:
        delta = asr_current_text[len(asr_prev_text):]
        if delta.strip():
            asr_segments.append((sec, delta))
            print(f"  [ASR SEG #{len(asr_segments)}] t={sec}s  delta={delta!r}")
    asr_prev_text = asr_current_text


def flush_mt(sec):
    """Lock the delta for the previous second and append to segments."""
    global mt_prev_text, mt_current_text
    if mt_current_text and mt_current_text != mt_prev_text:
        delta = mt_current_text[len(mt_prev_text):]
        if delta.strip():
            mt_segments.append((sec, delta))
            print(f"  [MT  SEG #{len(mt_segments)}] t={sec}s  delta={delta!r}")
    mt_prev_text = mt_current_text


def on_open(ws):
    print("[probe2] connected OK!")
    session_config = {
        "modalities": ["text", "audio"],
        "input_audio_format": "pcm",
        "output_audio_format": "pcm",
        "translation": {"language": "en"},
        "input_audio_transcription": {"model": "qwen3-asr-flash-realtime"},
    }
    session_config["input_audio_transcription"]["language"] = "zh"

    config_msg = json.dumps({
        "event_id": f"evt_{int(time.time()*1000)}",
        "type": "session.update",
        "session": session_config,
    }, ensure_ascii=False)
    print(f"[send] session.update: {json.dumps(session_config, ensure_ascii=False)}")
    ws.send(config_msg)


def on_message(ws, message):
    global asr_current_text, asr_last_sec, asr_prev_text
    global mt_current_text, mt_last_sec, mt_prev_text
    global asr_first_text_time, mt_first_text_time
    global session_finished

    try:
        event = json.loads(message)
    except Exception:
        return

    etype = event.get("type", "")
    elapsed = time.time() - start_time
    sec = int(elapsed)

    # ── ASR streaming partial (cumulative text + tentative stash) ──
    if etype == "conversation.item.input_audio_transcription.text":
        text = event.get("text", "")
        if asr_first_text_time is None and text:
            asr_first_text_time = elapsed
            print(f"[probe2] *** FIRST ASR TEXT at {elapsed:.1f}s ***")

        # Second changed → flush previous second's delta
        if asr_last_sec != -1 and sec != asr_last_sec:
            flush_asr(asr_last_sec)
        asr_last_sec = sec
        asr_current_text = text

    # ── ASR final → force flush ──
    elif etype == "conversation.item.input_audio_transcription.completed":
        transcript = (event.get("transcript") or "").strip()
        print(f"[probe2] ASR FINAL at {elapsed:.1f}s: {transcript!r}")
        # Flush any pending delta using the final transcript
        if transcript:
            asr_current_text = transcript
            if asr_last_sec != -1:
                flush_asr(sec)
            # Also check if there's a remaining delta after flush
            if transcript != asr_prev_text:
                delta = transcript[len(asr_prev_text):]
                if delta.strip():
                    asr_segments.append((sec, delta))
                    print(f"  [ASR SEG #{len(asr_segments)}] t={sec}s  delta(final)={delta!r}")
                asr_prev_text = transcript

    # ── MT streaming partial (text mode) ──
    elif etype == "response.text.text":
        text = event.get("text", "")
        if mt_first_text_time is None and text:
            mt_first_text_time = elapsed
            print(f"[probe2] *** FIRST MT TEXT at {elapsed:.1f}s ***")

        if mt_last_sec != -1 and sec != mt_last_sec:
            flush_mt(mt_last_sec)
        mt_last_sec = sec
        mt_current_text = text

    # ── MT streaming partial (audio+text mode) ──
    elif etype == "response.audio_transcript.text":
        text = event.get("text", "")
        if mt_first_text_time is None and text:
            mt_first_text_time = elapsed
            print(f"[probe2] *** FIRST MT_audio TEXT at {elapsed:.1f}s ***")

        if mt_last_sec != -1 and sec != mt_last_sec:
            flush_mt(mt_last_sec)
        mt_last_sec = sec
        mt_current_text = text

    # ── MT final (text mode) → force flush ──
    elif etype == "response.text.done":
        text = (event.get("text") or "").strip()
        print(f"[probe2] MT TEXT.DONE at {elapsed:.1f}s: {text!r}")
        if text:
            mt_current_text = text
            if mt_last_sec != -1:
                flush_mt(sec)
            if text != mt_prev_text:
                delta = text[len(mt_prev_text):]
                if delta.strip():
                    mt_segments.append((sec, delta))
                    print(f"  [MT  SEG #{len(mt_segments)}] t={sec}s  delta(final)={delta!r}")
                mt_prev_text = text

    # ── MT final (audio+text mode) → force flush ──
    elif etype == "response.audio_transcript.done":
        text = (event.get("transcript") or event.get("text") or "").strip()
        print(f"[probe2] MT FINAL at {elapsed:.1f}s: {text!r}")
        if text:
            mt_current_text = text
            if mt_last_sec != -1:
                flush_mt(sec)
            if text != mt_prev_text:
                delta = text[len(mt_prev_text):]
                if delta.strip():
                    mt_segments.append((sec, delta))
                    print(f"  [MT  SEG #{len(mt_segments)}] t={sec}s  delta(final)={delta!r}")
                mt_prev_text = text

    elif etype == "session.finished":
        session_finished = True
        print(f"[probe2] SESSION FINISHED at {elapsed:.1f}s")

    elif etype == "error":
        print(f"[probe2] ERROR: {json.dumps(event, ensure_ascii=False)[:300]}")

    # Silently skip noisy lifecycle events
    elif etype in {
        "session.created", "session.updated",
        "input_audio_buffer.speech_started", "input_audio_buffer.speech_stopped",
        "input_audio_buffer.committed", "response.created",
        "response.output_item.added", "response.output_item.done",
        "response.content_part.added", "response.content_part.done",
        "response.audio.done", "response.audio.delta",
        "response.done", "conversation.item.created",
    }:
        pass

    else:
        print(f"[probe2] UNKNOWN: {etype}")


def on_error(ws, error):
    print(f"[error] {type(error).__name__}: {error}")


def on_close(ws, code, reason):
    print(f"\n[close] code={code} reason={reason}")
    print_timeline()


def print_timeline():
    """Print the final delta segmentation and pairing table."""
    print("\n" + "=" * 80)
    print("DELTA SEGMENTATION SUMMARY")
    print("=" * 80)

    print(f"\nASR segments: {len(asr_segments)}")
    for i, (sec, delta) in enumerate(asr_segments):
        print(f"  #{i+1:3d}  t={sec:4d}s  {delta!r}")

    print(f"\nMT segments: {len(mt_segments)}")
    for i, (sec, delta) in enumerate(mt_segments):
        print(f"  #{i+1:3d}  t={sec:4d}s  {delta!r}")

    # Pairing table (like Doubao's ASR+MT per chunk)
    print("\n" + "-" * 80)
    print("TIMELINE PAIRING (ASR + MT by segment index)")
    print("-" * 80)
    max_segs = max(len(asr_segments), len(mt_segments))
    print(f"{'#':>4}  {'ASR time':>8}  {'MT time':>8}  {'ASR delta':<40}  {'MT delta'}")
    for i in range(max_segs):
        asr_sec, asr_delta = asr_segments[i] if i < len(asr_segments) else ("", "")
        mt_sec, mt_delta = mt_segments[i] if i < len(mt_segments) else ("", "")
        asr_short = (asr_delta[:38] + "..") if len(asr_delta) > 40 else asr_delta
        mt_short = (mt_delta[:38] + "..") if len(mt_delta) > 40 else mt_delta
        print(f"{i+1:>4}  {str(asr_sec):>8}s  {str(mt_sec):>8}s  {asr_short:<40}  {mt_short}")

    # Full text reconstruction check
    print("\n" + "-" * 80)
    print("RECONSTRUCTION CHECK (sum of deltas == final text?)")
    print("-" * 80)
    asr_reconstructed = "".join(d for _, d in asr_segments)
    mt_reconstructed = "".join(d for _, d in mt_segments)
    print(f"ASR reconstructed ({len(asr_reconstructed)} chars): {asr_reconstructed!r}")
    print(f"MT  reconstructed ({len(mt_reconstructed)} chars): {mt_reconstructed!r}")

    if asr_first_text_time:
        print(f"\nFirst ASR text at: {asr_first_text_time:.1f}s")
    if mt_first_text_time:
        print(f"First MT  text at: {mt_first_text_time:.1f}s")
    if asr_first_text_time and mt_first_text_time:
        delay = asr_first_text_time - mt_first_text_time
        print(f"MT started {'before' if delay > 0 else 'after'} ASR by {abs(delay):.1f}s")

    print(f"\nTotal: {len(asr_segments)} ASR segments, {len(mt_segments)} MT segments")
    print("[probe2] done.")


def main():
    global start_time, current_sent_ms

    parser = argparse.ArgumentParser(description="Qwen delta segmentation probe")
    parser.add_argument("--audio", required=True, help="Audio file (wav/mp3/etc)")
    parser.add_argument("--lang", default="zh", help="Source language")
    parser.add_argument("--lang-to", default="en", help="Target language")
    args = parser.parse_args()

    if not os.path.exists(args.audio):
        print(f"ERROR: file not found: {args.audio}")
        sys.exit(1)

    pcm = read_pcm(args.audio)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / CHUNK_BYTES)

    print(f"[probe2] audio: {args.audio}")
    print(f"[probe2] duration: {total_ms}ms  chunks: {total_chunks} (100ms each)")
    print(f"[probe2] lang={args.lang} -> {args.lang_to}")
    print(f"[probe2] connecting to {API_URL[:60]}...")

    ws = websocket.WebSocketApp(
        API_URL,
        header={"Authorization": f"Bearer {API_KEY}"},
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Send audio in a background thread
    def send_audio():
        global start_time, current_sent_ms
        time.sleep(2)  # wait for session.update
        start_time = time.time()

        for idx in range(total_chunks):
            if session_finished:
                break
            chunk = pcm[idx * CHUNK_BYTES:(idx + 1) * CHUNK_BYTES]
            if not chunk:
                break
            try:
                ws.send(json.dumps({
                    "event_id": f"evt_{int(time.time()*1000)}_{idx}",
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(chunk).decode("utf-8"),
                }, ensure_ascii=False))
                current_sent_ms = (idx + 1) * CHUNK_MS
                if idx % 50 == 0 or idx < 3:
                    print(f"[send] chunk {idx+1}/{total_chunks}  sent={current_sent_ms}ms")
            except Exception as exc:
                print(f"[send] failed at chunk {idx+1}: {exc}")
                break
            time.sleep(CHUNK_MS / 1000.0)

        print(f"[send] audio DONE, sending 2s silence...")
        silence = b"\x00" * CHUNK_BYTES
        for i in range(20):
            if session_finished:
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

        print(f"[send] session.finish")
        try:
            ws.send(json.dumps({"event_id": "evt_finish", "type": "session.finish"}))
        except Exception:
            pass

        # Wait for final results
        time.sleep(30)
        try:
            ws.close()
        except Exception:
            pass

    threading.Thread(target=send_audio, daemon=True).start()

    ws.run_forever(sslopt={"cert_reqs": 0})


if __name__ == "__main__":
    main()

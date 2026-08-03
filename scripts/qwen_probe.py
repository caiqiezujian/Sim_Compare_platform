"""Qwen real-time translation probe (sync websocket-client version).

Tests the qwen3.5-livetranslate-flash-realtime WebSocket API and prints
every event received, so we can verify streaming behavior.

Usage:
    python scripts/qwen_probe.py --audio test.wav --lang zh --lang-to en
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

event_counts = {}
first_asr_time = None
first_mt_time = None
start_time = time.time()


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


def on_open(ws):
    print("[probe] connected OK!")


def on_message(ws, message):
    global first_asr_time, first_mt_time
    try:
        event = json.loads(message)
    except Exception:
        print(f"[recv] NON-JSON: {message[:100]}")
        return

    etype = event.get("type", "")
    event_counts[etype] = event_counts.get(etype, 0) + 1
    elapsed = time.time() - start_time

    # ASR streaming partial — print text and stash SEPARATELY to understand their behavior
    if etype == "conversation.item.input_audio_transcription.text":
        text = event.get("text", "")
        stash = event.get("stash", "")
        print(f"[recv {elapsed:.1f}s] ASR text={text!r} stash={stash!r}")

    # ASR final
    elif etype == "conversation.item.input_audio_transcription.completed":
        transcript = (event.get("transcript") or "").strip()
        if first_asr_time is None:
            first_asr_time = elapsed
            print(f"[recv] *** FIRST ASR at {elapsed:.1f}s ***")
        print(f"[recv {elapsed:.1f}s] ASR FINAL: {transcript!r}")

    # MT streaming partial (text mode) — print text and stash SEPARATELY
    elif etype == "response.text.text":
        text = event.get("text", "")
        stash = event.get("stash", "")
        print(f"[recv {elapsed:.1f}s] MT text={text!r} stash={stash!r}")

    # MT streaming partial (audio+text mode)
    elif etype == "response.audio_transcript.text":
        text = event.get("text", "")
        stash = event.get("stash", "")
        print(f"[recv {elapsed:.1f}s] MT_audio text={text!r} stash={stash!r}")

    # MT final (primary)
    elif etype == "response.audio_transcript.done":
        text = (event.get("transcript") or event.get("text") or "").strip()
        if first_mt_time is None:
            first_mt_time = elapsed
            print(f"[recv] *** FIRST MT at {elapsed:.1f}s ***")
        print(f"[recv {elapsed:.1f}s] MT FINAL: {text!r}")

    # MT fallback delta
    elif etype == "response.text.delta":
        delta = (event.get("delta") or "").strip()
        print(f"[recv {elapsed:.1f}s] MT TEXT.DELTA: {delta!r}")

    # MT fallback final
    elif etype == "response.text.done":
        text = (event.get("text") or "").strip()
        if first_mt_time is None:
            first_mt_time = elapsed
            print(f"[recv] *** FIRST MT at {elapsed:.1f}s ***")
        print(f"[recv {elapsed:.1f}s] MT TEXT.DONE: {text!r}")

    elif etype == "session.finished":
        print(f"[recv] SESSION FINISHED")

    elif etype == "error":
        print(f"[recv] ERROR: {json.dumps(event, ensure_ascii=False)[:300]}")

    elif etype == "response.done":
        usage = event.get("response", {}).get("usage", {})
        if usage:
            print(f"[recv] response.done usage: {json.dumps(usage, ensure_ascii=False)}")

    elif etype in {
        "session.created", "session.updated",
        "input_audio_buffer.speech_started", "input_audio_buffer.speech_stopped",
        "input_audio_buffer.committed", "response.created",
        "response.output_item.added", "response.output_item.done",
        "response.content_part.added", "response.content_part.done",
        "response.audio.done", "response.audio.delta",
    }:
        pass  # silently skip noisy events

    else:
        print(f"[recv] UNKNOWN: {etype} {json.dumps(event, ensure_ascii=False)[:200]}")


def on_error(ws, error):
    print(f"[error] {type(error).__name__}: {error}")


def on_close(ws, code, reason):
    global first_asr_time, first_mt_time
    print(f"\n[close] code={code} reason={reason}")
    print(f"\n[probe] === SUMMARY ===")
    print(f"[probe] event counts: {json.dumps(event_counts, ensure_ascii=False, indent=2)}")
    if first_asr_time:
        print(f"[probe] first ASR result at: {first_asr_time:.1f}s")
    if first_mt_time:
        print(f"[probe] first MT result at: {first_mt_time:.1f}s")
    if first_asr_time and first_mt_time:
        print(f"[probe] ASR→MT delay: {first_mt_time - first_asr_time:.1f}s")
    print(f"[probe] done.")


def main():
    global start_time
    parser = argparse.ArgumentParser(description="Qwen real-time translation probe")
    parser.add_argument("--audio", required=True, help="Audio file (wav/mp3/etc)")
    parser.add_argument("--lang", default="zh", help="Source language (zh/en/ja/ko/ru/th/ar)")
    parser.add_argument("--lang-to", default="en", help="Target language")
    args = parser.parse_args()

    if not os.path.exists(args.audio):
        print(f"ERROR: file not found: {args.audio}")
        sys.exit(1)

    pcm = read_pcm(args.audio)
    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
    total_chunks = math.ceil(len(pcm) / CHUNK_BYTES)

    print(f"[probe] audio: {args.audio}")
    print(f"[probe] duration: {total_ms}ms  chunks: {total_chunks} (100ms each)")
    print(f"[probe] lang={args.lang} -> {args.lang_to}")
    print(f"[probe] connecting to {API_URL[:60]}...")

    ws = websocket.WebSocketApp(
        API_URL,
        header={"Authorization": f"Bearer {API_KEY}"},
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Configure session on open
    def on_open_wrapper(ws):
        print("[probe] connected OK!")
        session_config = {
            "modalities": ["text", "audio"],
            "input_audio_format": "pcm",
            "output_audio_format": "pcm",
            "translation": {"language": args.lang_to},
            "input_audio_transcription": {"model": "qwen3-asr-flash-realtime"},
        }
        if args.lang:
            session_config["input_audio_transcription"]["language"] = args.lang

        config_msg = json.dumps({
            "event_id": f"evt_{int(time.time()*1000)}",
            "type": "session.update",
            "session": session_config,
        }, ensure_ascii=False)
        print(f"[send] session.update: {json.dumps(session_config, ensure_ascii=False)}")
        ws.send(config_msg)

    ws.on_open = on_open_wrapper

    # Send audio in a background thread
    def send_audio():
        global start_time
        time.sleep(2)  # wait for session.update to be processed
        start_time = time.time()

        for idx in range(total_chunks):
            chunk = pcm[idx * CHUNK_BYTES:(idx + 1) * CHUNK_BYTES]
            if not chunk:
                break
            try:
                ws.send(json.dumps({
                    "event_id": f"evt_{int(time.time()*1000)}_{idx}",
                    "type": "input_audio_buffer.append",
                    "audio": base64.b64encode(chunk).decode("utf-8"),
                }, ensure_ascii=False))
                if idx % 50 == 0 or idx < 3:
                    sent_ms = (idx + 1) * CHUNK_MS
                    print(f"[send] chunk {idx+1}/{total_chunks}  sent={sent_ms}ms")
            except Exception as exc:
                print(f"[send] failed at chunk {idx+1}: {exc}")
                break
            time.sleep(CHUNK_MS / 1000.0)

        print(f"[send] audio DONE, sending 2s silence...")
        silence = b"\x00" * CHUNK_BYTES
        for i in range(20):
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

        # Wait for results
        time.sleep(30)

    threading.Thread(target=send_audio, daemon=True).start()

    ws.run_forever(sslopt={"cert_reqs": 0})


if __name__ == "__main__":
    main()

"""Doubao (ByteDance) AST v2 translation probe.

Tests the Doubao real-time translation WebSocket API and prints
every event received, so we can verify streaming behavior.

Protocol: protobuf over WebSocket (NOT JSON).
Requires python_protogen_v2 (compiled with protoc 3.19.1).

Usage:
    python scripts/doubao_probe.py --audio test.wav --lang zh --lang-to en
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import wave
from pathlib import Path

import websocket  # websocket-client (sync, not websockets)

# Add python_protogen_v2 to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTOGEN_DIR = os.path.join(SCRIPT_DIR, "ast_python", "python_protogen_v2")
if PROTOGEN_DIR not in sys.path:
    sys.path.insert(0, PROTOGEN_DIR)

try:
    from products.understanding.ast.ast_service_pb2 import TranslateRequest, TranslateResponse
    from common.events_pb2 import Type
    from google.protobuf.json_format import MessageToDict
except Exception as e:
    print(f"ERROR: Failed to import protobuf modules: {e}")
    print(f"  Looked in: {PROTOGEN_DIR}")
    sys.exit(1)

API_KEY = "4d396124-e651-441d-839f-056d585dfbfb"
WS_URL = "wss://openspeech.bytedance.com/api/v4/ast/v2/translate"
RESOURCE_ID = "volc.service_type.10053"

SAMPLE_RATE = 16000
CHUNK_SIZE = 3200

EVENT_NAMES = {}
for name in dir(Type):
    if not name.startswith("_"):
        val = getattr(Type, name)
        if isinstance(val, int):
            EVENT_NAMES[val] = name

MANUAL_EVENTS = {
    650: "SourceSubtitleStart",
    651: "SourceSubtitleResponse",
    652: "SourceSubtitleEnd",
    653: "TranslationSubtitleStart",
    654: "TranslationSubtitleResponse",
    655: "TranslationSubtitleEnd",
}


def get_event_name(event_val):
    if event_val in MANUAL_EVENTS:
        return MANUAL_EVENTS[event_val]
    return EVENT_NAMES.get(event_val, f"Unknown_{event_val}")


def read_pcm(path):
    if path.lower().endswith((".wav", ".wave")):
        with wave.open(path, "rb") as r:
            return r.readframes(r.getnframes())
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    subprocess.run(["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", str(SAMPLE_RATE),
                    "-sample_fmt", "s16", tmp], check=True, capture_output=True)
    with wave.open(tmp, "rb") as r:
        data = r.readframes(r.getnframes())
    os.unlink(tmp)
    return data


def build_request(session_id, event_type, audio_data=None, lang="zh", lang_to="en"):
    req = TranslateRequest()
    req.request_meta.SessionID = session_id
    req.event = event_type
    req.user.uid = "simcompare_probe"
    req.user.did = "simcompare_probe"
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


def main():
    parser = argparse.ArgumentParser(description="Doubao AST v2 translation probe")
    parser.add_argument("--audio", required=True)
    parser.add_argument("--lang", default="zh")
    parser.add_argument("--lang-to", default="en")
    args = parser.parse_args()

    if not os.path.exists(args.audio):
        print(f"ERROR: file not found: {args.audio}")
        sys.exit(1)

    pcm = read_pcm(args.audio)
    total_chunks = (len(pcm) + CHUNK_SIZE - 1) // CHUNK_SIZE
    total_ms = len(pcm) // 2 * 1000 // SAMPLE_RATE
    lang, lang_to = args.lang, args.lang_to

    print(f"[probe] audio: {args.audio}")
    print(f"[probe] duration: {total_ms}ms  chunks: {total_chunks}")
    print(f"[probe] lang={lang} -> {lang_to}")

    conn_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    headers = {
        "X-Api-Key": API_KEY,
        "X-Api-Resource-Id": RESOURCE_ID,
        "X-Api-Connect-Id": conn_id,
    }

    print(f"[probe] connecting to {WS_URL[:50]}...")

    start_time = time.time()
    event_counts = {}
    first_asr_time = [None]
    first_mt_time = [None]
    current_source_parts = []
    current_translation_parts = []
    source_finals = []
    translation_finals = []
    session_done = [False]

    def on_open(ws):
        print(f"[probe] connected!")
        # StartSession
        start_req = build_request(session_id, Type.StartSession, None, lang, lang_to)
        data = start_req.SerializeToString()
        print(f"[send] StartSession ({len(data)} bytes protobuf)")
        ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)

    def on_message(ws, message):
        if isinstance(message, str):
            print(f"[recv] TEXT (unexpected): {message[:200]}")
            return

        # It's binary — parse protobuf
        resp = TranslateResponse()
        resp.ParseFromString(message)
        event_val = resp.event
        event_name = get_event_name(event_val)
        event_counts[event_name] = event_counts.get(event_name, 0) + 1
        elapsed = time.time() - start_time
        text = resp.text or ""

        # Check SessionStarted first
        if event_val == Type.SessionStarted:
            print(f"[recv {elapsed:.1f}s] SessionStarted — starting audio send")
            # Start sending audio in background
            def send_audio():
                for idx in range(total_chunks):
                    chunk = pcm[idx * CHUNK_SIZE:(idx + 1) * CHUNK_SIZE]
                    if not chunk:
                        break
                    req = build_request(session_id, Type.TaskRequest, chunk, lang, lang_to)
                    data = req.SerializeToString()
                    try:
                        ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)
                    except Exception as exc:
                        print(f"[send] failed at chunk {idx+1}: {exc}")
                        break
                    if idx % 50 == 0 or idx < 3:
                        print(f"[send] chunk {idx+1}/{total_chunks}  {len(chunk)}B pcm  {len(data)}B proto")
                    time.sleep(0.1)

                # FinishSession
                finish_req = build_request(session_id, Type.FinishSession, None, lang, lang_to)
                try:
                    ws.send(finish_req.SerializeToString(), opcode=websocket.ABNF.OPCODE_BINARY)
                    print(f"[send] FinishSession")
                except Exception:
                    pass

            threading.Thread(target=send_audio, daemon=True).start()
            return

        # Source subtitle events
        if event_val == 650:  # SourceSubtitleStart
            current_source_parts.clear()
            print(f"\n[recv {elapsed:.1f}s] === ASR SEGMENT START ===")

        elif event_val == 651:  # SourceSubtitleResponse
            if text:
                current_source_parts.append(text)
                building = "".join(current_source_parts)
                if first_asr_time[0] is None:
                    first_asr_time[0] = elapsed
                    print(f"[recv] *** FIRST ASR at {elapsed:.1f}s ***")
                print(f"[recv {elapsed:.1f}s] ASR BUILDING: {building!r}")

        elif event_val == 652:  # SourceSubtitleEnd
            final_text = text or "".join(current_source_parts)
            if final_text:
                source_finals.append(final_text)
            print(f"[recv {elapsed:.1f}s] ASR FINAL: {final_text!r}")
            current_source_parts.clear()

        # Translation subtitle events
        elif event_val == 653:  # TranslationSubtitleStart
            current_translation_parts.clear()
            print(f"\n[recv {elapsed:.1f}s] === MT SEGMENT START ===")

        elif event_val == 654:  # TranslationSubtitleResponse
            if text:
                current_translation_parts.append(text)
                building = "".join(current_translation_parts)
                if first_mt_time[0] is None:
                    first_mt_time[0] = elapsed
                    print(f"[recv] *** FIRST MT at {elapsed:.1f}s ***")
                print(f"[recv {elapsed:.1f}s] MT BUILDING: {building!r}")

        elif event_val == 655:  # TranslationSubtitleEnd
            final_text = text or "".join(current_translation_parts)
            if final_text:
                translation_finals.append(final_text)
            print(f"[recv {elapsed:.1f}s] MT FINAL: {final_text!r}")
            current_translation_parts.clear()

        elif event_val == Type.SessionFinished:
            print(f"\n[recv] SESSION FINISHED")
            session_done[0] = True

        elif event_val == Type.SessionFailed:
            print(f"\n[recv] SESSION FAILED")
            session_done[0] = True

        elif event_val == Type.UsageResponse:
            try:
                d = MessageToDict(resp)
                print(f"[recv {elapsed:.1f}s] UsageResponse: {json.dumps(d, ensure_ascii=False)[:200]}")
            except Exception:
                print(f"[recv {elapsed:.1f}s] UsageResponse")

        else:
            print(f"[recv {elapsed:.1f}s] {event_name} (event={event_val}): {text[:100]!r}")

    def on_error(ws, error):
        print(f"[error] {type(error).__name__}: {error}")

    def on_close(ws, code, reason):
        print(f"\n[close] code={code} reason={reason}")
        print(f"\n[probe] === SUMMARY ===")
        print(f"[probe] event counts: {json.dumps(event_counts, ensure_ascii=False, indent=2)}")
        if first_asr_time[0]:
            print(f"[probe] first ASR at: {first_asr_time[0]:.1f}s")
        if first_mt_time[0]:
            print(f"[probe] first MT at: {first_mt_time[0]:.1f}s")
        print(f"[probe] ASR segments: {len(source_finals)}")
        print(f"[probe] MT segments: {len(translation_finals)}")
        for i in range(min(len(source_finals), len(translation_finals))):
            print(f"[probe] pair {i+1}:")
            print(f"  ASR: {source_finals[i][:100]}")
            print(f"  MT:  {translation_finals[i][:100]}")
        print(f"[probe] done.")

    ws = websocket.WebSocketApp(
        WS_URL,
        header=headers,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(sslopt={"cert_reqs": 0}, ping_timeout=10)


if __name__ == "__main__":
    main()

"""WebSocket ASR/MT probe for Huawei real-time translation API.

Protocol (from huawei-translation-api.md):
1. Connect: wss://apigw-beta.huawei.com/ws/apiAsr/plug/audioTranslate?X-HW-ID=...&langFrom=zh&langTo=en
2. Server pushes: {"msg":"connect","conferenceId":"xxx"}
3. Client sends audio: {"audioData":"<base64 PCM>","conferenceId":"xxx","seq":1}
   - PCM 16kHz/16-bit/mono, 80ms = 2560 bytes per packet
4. Server returns: {"msgType":"text","sn":1,"sentenceType":0/1/2,"text":"ASR","translate":"MT",...}
   - sentenceType: 0=partial, 1=final(translate may be empty), 2=smooth(translate arrives)
5. Heartbeat every 30s: {"beat":true}

Usage:
    python scripts/ws_probe.py --url "wss://apigw-beta.huawei.com/ws/apiAsr/plug/audioTranslate?X-HW-ID=...&langFrom=zh&langTo=en"
    python scripts/ws_probe.py --url "wss://..." --audio test.wav
    python scripts/ws_probe.py --url "wss://..." --silence-seconds 10
"""
import argparse
import base64
import json
import math
import ssl
import threading
import time
import wave

import websocket

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2
# 80ms = 16000 * 2 * 0.08 = 2560 bytes (per API doc)
CHUNK_MS = 80
CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * CHUNK_MS / 1000)  # 2560

received_messages = []


def on_message(ws, message):
    if isinstance(message, bytes):
        print(f"[recv] BINARY {len(message)} bytes: {message[:80]}...")
    else:
        received_messages.append(message)
        try:
            obj = json.loads(message)
            msg_type = obj.get("msg") or obj.get("msgType") or obj.get("beat")
            if msg_type is True:
                print(f"[recv] heartbeat: {message[:80]}")
            elif obj.get("msgType") == "text":
                sn = obj.get("sn", "?")
                st = obj.get("sentenceType", "?")
                st_label = {0: "partial", 1: "final", 2: "smooth"}.get(st, st)
                text = obj.get("text", "")
                tr = obj.get("translate", "")
                fluency = obj.get("fluency", "")
                print(f"[recv] sn={sn} type={st_label} asr={text!r} mt={tr!r} fluency={fluency!r}")
            elif obj.get("msg") == "connect":
                print(f"[recv] CONNECT conferenceId={obj.get('conferenceId')}")
            else:
                print(f"[recv] JSON: {json.dumps(obj, ensure_ascii=False)[:300]}")
        except (json.JSONDecodeError, TypeError):
            print(f"[recv] TEXT: {message[:300]}")


def on_error(ws, error):
    print(f"[error] {type(error).__name__}: {error}")


def on_close(ws, code, reason):
    print(f"\n[close] code={code} reason={reason}")
    print(f"[summary] total messages received: {len(received_messages)}")


def on_open(ws):
    print("[probe] connected OK!")


def main():
    parser = argparse.ArgumentParser(description="Huawei WSS translation probe")
    parser.add_argument("--url", required=True)
    parser.add_argument("--audio", default="")
    parser.add_argument("--silence-seconds", type=float, default=10)
    args = parser.parse_args()

    url = args.url
    print(f"[probe] connecting to {url[:80]}...")
    print(f"[probe] audio chunk: {CHUNK_MS}ms = {CHUNK_BYTES} bytes")

    pcm_data = b""
    if args.audio:
        with wave.open(args.audio, "rb") as reader:
            assert reader.getframerate() == SAMPLE_RATE, f"expected {SAMPLE_RATE}Hz"
            assert reader.getsampwidth() == 2, "expected 16-bit"
            assert reader.getnchannels() == 1, "expected mono"
            pcm_data = reader.readframes(reader.getnframes())
        total_ms = len(pcm_data) // 2 * 1000 // SAMPLE_RATE
        total_chunks = math.ceil(len(pcm_data) / CHUNK_BYTES)
        print(f"[probe] audio: {args.audio}  duration={total_ms}ms  chunks={total_chunks}")
    else:
        pcm_data = b"\x00" * CHUNK_BYTES * int(args.silence_seconds / (CHUNK_MS / 1000))
        total_chunks = len(pcm_data) // CHUNK_BYTES
        print(f"[probe] silence: {args.silence_seconds}s  chunks={total_chunks}")

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    conference_id = [None]
    seq = [0]
    stopped = [False]

    def send_audio():
        # Wait for connect message with conferenceId
        for _ in range(50):
            if conference_id[0]:
                break
            time.sleep(0.1)

        if not conference_id[0]:
            print("[send] ERROR: no conferenceId received, cannot send audio")
            return

        print(f"[send] conferenceId={conference_id[0]}, starting audio stream...")

        # Start heartbeat
        def heartbeat():
            while not stopped[0]:
                time.sleep(30)
                if not stopped[0]:
                    try:
                        ws.send(json.dumps({"beat": True}))
                        print("[heartbeat] sent")
                    except Exception:
                        break

        threading.Thread(target=heartbeat, daemon=True).start()

        # Send audio in 80ms chunks
        offset = 0
        while offset < len(pcm_data) and not stopped[0]:
            chunk = pcm_data[offset:offset + CHUNK_BYTES]
            seq[0] += 1
            try:
                b64 = base64.b64encode(chunk).decode()
                msg = json.dumps({
                    "audioData": b64,
                    "conferenceId": conference_id[0],
                    "seq": seq[0],
                })
                ws.send(msg)
                if seq[0] % 50 == 0 or seq[0] <= 3:
                    print(f"[send] seq={seq[0]}/{total_chunks}  {len(chunk)}B pcm  {len(msg)}B json")
            except Exception as exc:
                print(f"[send] failed at seq={seq[0]}: {exc}")
                break
            offset += CHUNK_BYTES
            time.sleep(CHUNK_MS / 1000.0)  # 80ms per chunk

        print(f"[send] audio DONE  total seq={seq[0]}")
        # Wait for final results
        time.sleep(5)
        stopped[0] = True
        ws.close()

    # Intercept connect message to grab conferenceId
    original_on_message = ws.on_message

    def intercept_message(ws, message):
        if isinstance(message, str):
            try:
                obj = json.loads(message)
                if obj.get("msg") == "connect" and obj.get("conferenceId"):
                    conference_id[0] = obj["conferenceId"]
            except Exception:
                pass
        original_on_message(ws, message)

    ws.on_message = intercept_message

    threading.Thread(target=send_audio, daemon=True).start()

    ws.run_forever(
        sslopt={"cert_reqs": ssl.CERT_NONE},
        ping_interval=30,
    )

    stopped[0] = True
    print("[probe] done.")


if __name__ == "__main__":
    main()

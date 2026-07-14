"""Optional adapter for the streaming protocol in the supplied Python snippet.

This module is deliberately dependency-light at import time. The generated protobuf
modules are loaded only when a real run is requested, so the UI can still start in
demo mode on a machine that has not received the .proto build artifacts yet.
"""
import json
import math
import os
import secrets
import subprocess
import tempfile
import time
import wave
from pathlib import Path

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2
SEND_SAMPLES = 400 * 16
SEND_BYTES = SEND_SAMPLES * BYTES_PER_SAMPLE


def _audio_samples(path: str):
    source = path
    temp_wav = None
    if Path(path).suffix.lower() not in {".wav", ".wave"}:
        temp_wav = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
        try:
            subprocess.run(["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", str(SAMPLE_RATE), temp_wav], check=True, capture_output=True)
            source = temp_wav
        except (OSError, subprocess.CalledProcessError) as exc:
            if temp_wav and os.path.exists(temp_wav):
                os.remove(temp_wav)
            raise RuntimeError("视频转音频需要 ffmpeg；也可以先上传 16kHz WAV 文件") from exc
    with wave.open(source, "rb") as wav:
        channels = wav.getnchannels()
        sample_width = wav.getsampwidth()
        rate = wav.getframerate()
        frame_count = wav.getnframes()
        frames = wav.readframes(frame_count)
    if channels != 1 or sample_width != 2 or rate != SAMPLE_RATE:
        raise RuntimeError("WAV 输入需要是 16kHz、16-bit、mono")
    if temp_wav and os.path.exists(temp_wav):
        os.remove(temp_wav)
    return frames, frame_count / float(rate)


def _send_audio(audio_file: str, lang: str, AsrRequest):
    sid = int(time.time() * 1e6) + secrets.randbelow(1000)
    conference_id = str(secrets.randbits(32))
    yield AsrRequest(sessionParam={"sid": f"Beijing-TSC-test-{sid}", "lang": lang, "latency": "long", "userinfo": json.dumps({"conferenceType": "Caption", "conferenceId": conference_id, "source": 2, "talkId": str(secrets.randbits(32)), "userId": "simcompare", "save": 1})})
    pcm, _ = _audio_samples(audio_file)
    for index in range(math.ceil(len(pcm) / SEND_BYTES)):
        yield AsrRequest(samples=pcm[index * SEND_BYTES:(index + 1) * SEND_BYTES])
        time.sleep(0.4)
    yield AsrRequest(endFlag=True)


def _sorted_chunks(chunks: dict):
    return sorted(chunks.values(), key=lambda row: (row["start"], row["end"], row["id"]))


def run_grpc(audio_file: str, endpoint: str, lang: str = "zh", timeout=None, on_update=None):
    """Run one endpoint and normalize its responses to the UI chunk contract."""
    import grpc
    from .grpc_info import asr_pb2_grpc
    from .grpc_info.asr_pb2 import AsrRequest

    _, duration = _audio_samples(audio_file)
    deadline = timeout or max(60, int(duration * 2.5 + 30))
    chunks = {}
    started = time.monotonic()
    with grpc.insecure_channel(endpoint) as channel:
        stub = asr_pb2_grpc.AsrServiceStub(channel)
        for response in stub.createRec(_send_audio(audio_file, lang, AsrRequest), timeout=deadline):
            payload = getattr(response, "data", None)
            if not payload:
                continue
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            result = json.loads(payload)
            key = str(result.get("sn") or result.get("bg") or len(chunks) + 1)
            item = chunks.setdefault(key, {"id": f"chunk-{len(chunks) + 1:02d}", "start": result.get("bg", 0), "end": result.get("ed", 0), "asr": "", "mt": "", "status": "done", "audio": Path(audio_file).name, "logs": []})
            item["start"] = result.get("bg", item["start"])
            item["end"] = result.get("ed", item["end"])
            received_at = int((time.monotonic() - started) * 1000)
            if result.get("type") == 1 and result.get("ws"):
                item["asr"] = result["ws"][0]["cw"][0].get("w", "")
                item["asr_time"] = received_at
                item["words"] = result.get("words", [])
                item["logs"].append(f"ASR final · hold_n={result.get('hold_n', 0)} · returned_at={received_at}ms")
            if result.get("part_2_mt"):
                item["mt"] = result["part_2_mt"]
            if result.get("mt_type") == 1 and result.get("mt_ws"):
                item["mt"] = result["mt_ws"][0]["cw"][0].get("w", item["mt"])
                item["mt_time"] = item.get("asr_time", received_at)
                item["logs"].append(f"MT final · returned_at={received_at}ms")
            item["logs"].insert(0, f"grpc response · sn={result.get('sn', '-')}")
            item["raw"] = result
            if on_update:
                on_update(_sorted_chunks(chunks))
    if not chunks:
        raise RuntimeError(f"gRPC 服务未返回有效 data: {endpoint}")
    return _sorted_chunks(chunks)

"""gRPC adapter for the SimCompare streaming ASR/S2TT service."""
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


def _convert_with_ffmpeg(path: str):
    target = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", str(SAMPLE_RATE), "-sample_fmt", "s16", target],
            check=True,
            capture_output=True,
        )
        return target
    except FileNotFoundError as exc:
        if os.path.exists(target):
            os.remove(target)
        raise RuntimeError("ffmpeg not found. Upload 16kHz / mono / 16-bit WAV, or install ffmpeg on the server.") from exc
    except subprocess.CalledProcessError as exc:
        if os.path.exists(target):
            os.remove(target)
        detail = exc.stderr.decode("utf-8", errors="ignore")[-1000:]
        raise RuntimeError(f"ffmpeg conversion failed: {detail}") from exc


def _read_wav(path: str):
    with wave.open(path, "rb") as wav:
        channels = wav.getnchannels()
        sample_width = wav.getsampwidth()
        rate = wav.getframerate()
        frame_count = wav.getnframes()
        frames = wav.readframes(frame_count)
    return frames, frame_count / float(rate), channels, sample_width, rate


def _audio_samples(path: str):
    source = path
    generated = None
    if Path(path).suffix.lower() not in {".wav", ".wave"}:
        generated = _convert_with_ffmpeg(path)
        source = generated
    try:
        frames, duration, channels, sample_width, rate = _read_wav(source)
        if channels == 1 and sample_width == BYTES_PER_SAMPLE and rate == SAMPLE_RATE:
            return frames, duration
    finally:
        if generated and os.path.exists(generated):
            os.remove(generated)

    converted = _convert_with_ffmpeg(path)
    try:
        frames, duration, channels, sample_width, rate = _read_wav(converted)
        if channels != 1 or sample_width != BYTES_PER_SAMPLE or rate != SAMPLE_RATE:
            raise RuntimeError(
                f"audio must be 16kHz / mono / 16-bit WAV, got channels={channels}, "
                f"sample_width={sample_width}, sample_rate={rate}"
            )
        return frames, duration
    finally:
        if os.path.exists(converted):
            os.remove(converted)


def _send_audio(pcm: bytes, lang: str, AsrRequest, conference_id: str, should_stop=None, on_stream_start=None, on_audio_progress=None):
    conference_id = conference_id or f"simcompare-{int(time.time())}-{secrets.token_hex(2)}"
    session_param = {
        "sid": conference_id,
        "lang": lang,
        "latency": "long",
        "userinfo": json.dumps(
            {
                "conferenceType": "Caption",
                "conferenceId": conference_id,
                "source": 2,
                "talkId": str(secrets.randbits(32)),
                "userId": "simcompare",
                "save": 1,
            },
            ensure_ascii=False,
        ),
    }
    yield AsrRequest(sessionParam=session_param)

    total_ms = int(len(pcm) / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000) if pcm else 0
    total_chunks = math.ceil(len(pcm) / SEND_BYTES)
    for index in range(total_chunks):
        if should_stop and should_stop():
            break
        if index == 0 and on_stream_start:
            on_stream_start()
        if on_audio_progress:
            sent_bytes = min((index + 1) * SEND_BYTES, len(pcm))
            sent_ms = int(sent_bytes / BYTES_PER_SAMPLE / SAMPLE_RATE * 1000)
            on_audio_progress(sent_ms, total_ms)
        yield AsrRequest(samples=pcm[index * SEND_BYTES:(index + 1) * SEND_BYTES])
        time.sleep(0.4)

    yield AsrRequest(endFlag=True)


def _sorted_chunks(chunks: dict):
    return sorted(chunks.values(), key=lambda row: (row["start"], row["end"], row["id"]))


def _result_key(result: dict, fallback_index: int):
    sn = result.get("sn")
    if sn is not None:
        return f"sn:{sn}"
    return f"idx:{fallback_index}"


def _first_text(result: dict, field: str, default: str = ""):
    try:
        return result[field][0]["cw"][0].get("w", default)
    except (KeyError, IndexError, TypeError, AttributeError):
        return default


def run_grpc(
    audio_file: str,
    endpoint: str,
    lang: str = "zh",
    timeout=None,
    on_update=None,
    should_stop=None,
    on_stream_start=None,
    on_audio_progress=None,
    conference_id: str = "",
    ready_timeout: int = 8,
):
    """Run one gRPC endpoint and normalize responses to the UI chunk contract."""
    import grpc

    from .grpc_info import asr_pb2_grpc
    from .grpc_info.asr_pb2 import AsrRequest

    endpoint = (endpoint or "").strip()
    if not endpoint:
        raise RuntimeError("empty gRPC endpoint")

    pcm, duration = _audio_samples(audio_file)
    deadline = timeout or max(60, int(duration * 2.5 + 30))
    chunks = {}
    started = time.monotonic()
    channel_options = (("grpc.enable_http_proxy", 0),)

    with grpc.insecure_channel(endpoint, options=channel_options) as channel:
        grpc.channel_ready_future(channel).result(timeout=ready_timeout)
        stub = asr_pb2_grpc.AsrServiceStub(channel)
        for response in stub.createRec(
            _send_audio(pcm, lang, AsrRequest, conference_id, should_stop, on_stream_start, on_audio_progress),
            timeout=deadline,
        ):
            if should_stop and should_stop():
                break

            payload = getattr(response, "data", None)
            if not payload:
                continue
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="replace")
            try:
                result = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"gRPC returned non-JSON data: {payload[:500]}") from exc

            key = _result_key(result, len(chunks) + 1)
            sn = result.get("sn")
            chunk_id = str(sn) if sn is not None else f"idx-{len(chunks) + 1}"
            result_conference_id = result.get("conference_id") or conference_id
            item = chunks.setdefault(
                key,
                {
                    "id": chunk_id,
                    "chunk_id": chunk_id,
                    "sn": sn,
                    "conference_id": result_conference_id,
                    "start": result.get("bg", 0),
                    "end": result.get("ed", 0),
                    "asr": "",
                    "mt": "",
                    "status": "done",
                    "audio": f"{chunk_id}.wav",
                    "logs": [],
                },
            )
            item["chunk_id"] = chunk_id
            item["sn"] = sn
            item["conference_id"] = result_conference_id
            item["start"] = result.get("bg", item["start"])
            item["end"] = result.get("ed", item["end"])
            item["debug_available"] = bool(result_conference_id and sn is not None)

            received_at = int((time.monotonic() - started) * 1000)
            if result.get("type") == 1 and result.get("ws"):
                item["asr"] = _first_text(result, "ws")
                item["asr_time"] = item["end"]
                item["asr_end_time"] = item["end"]
                item["asr_returned_at"] = received_at
                item["words"] = result.get("words", [])
                item["logs"].append(f"ASR final | hold_n={result.get('hold_n', 0)} | returned_at={received_at}ms")
            if result.get("part_2_mt"):
                item["mt"] = result["part_2_mt"]
            if result.get("mt_type") == 1 and result.get("mt_ws"):
                item["mt"] = _first_text(result, "mt_ws", item["mt"])
                item["mt_time"] = item.get("asr_end_time", item["end"])
                item["mt_returned_at"] = received_at
                item["logs"].append(f"MT final | returned_at={received_at}ms")
            item["logs"].insert(0, f"grpc response | sn={result.get('sn', '-')}")
            item["raw"] = result
            if on_update:
                on_update(_sorted_chunks(chunks))

    if should_stop and should_stop():
        return _sorted_chunks(chunks)
    if not chunks:
        raise RuntimeError(f"gRPC returned no valid data: endpoint={endpoint}, duration={duration:.2f}s, lang={lang}")
    return _sorted_chunks(chunks)

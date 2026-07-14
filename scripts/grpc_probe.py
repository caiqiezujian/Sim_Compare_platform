"""Smoke-test the S2TT gRPC service without starting the web frontend/backend.

Examples:
  python scripts/grpc_probe.py --endpoint 10.185.1.71:16552 --audio sample.wav --lang zh
  python scripts/grpc_probe.py --endpoint 10.185.1.71:16552 --silence-seconds 3
"""
import argparse
import json
import math
import secrets
import subprocess
import sys
import tempfile
import time
import wave
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.grpc_info import asr_pb2_grpc
from server.grpc_info.asr_pb2 import AsrRequest

SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
CHANNELS = 1
DEFAULT_CHUNK_BYTES = 400 * 16


def log(message):
    print(message, flush=True)


def make_silence_wav(seconds):
    path = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    frames = b"\x00\x00" * int(SAMPLE_RATE * seconds)
    with wave.open(path, "wb") as wav:
        wav.setnchannels(CHANNELS)
        wav.setsampwidth(SAMPLE_WIDTH)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(frames)
    return path


def convert_with_ffmpeg(source):
    target = tempfile.NamedTemporaryFile(delete=False, suffix=".wav").name
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", source, "-ac", "1", "-ar", str(SAMPLE_RATE), "-sample_fmt", "s16", target],
            check=True,
            capture_output=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg not found. Please provide 16kHz / mono / 16-bit WAV.") from exc
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode("utf-8", errors="ignore")[-1000:]
        raise RuntimeError(f"ffmpeg conversion failed: {detail}") from exc
    return target


def load_pcm(audio_path):
    source = Path(audio_path)
    temp_path = None
    if source.suffix.lower() not in {".wav", ".wave"}:
        temp_path = convert_with_ffmpeg(str(source))
        source = Path(temp_path)

    with wave.open(str(source), "rb") as wav:
        channels = wav.getnchannels()
        sample_width = wav.getsampwidth()
        sample_rate = wav.getframerate()
        frame_count = wav.getnframes()
        frames = wav.readframes(frame_count)

    if channels != CHANNELS or sample_width != SAMPLE_WIDTH or sample_rate != SAMPLE_RATE:
        if temp_path is None:
            temp_path = convert_with_ffmpeg(str(source))
            return load_pcm(temp_path)
        raise RuntimeError(
            f"audio must be 16kHz / mono / 16-bit WAV, got channels={channels}, "
            f"sample_width={sample_width}, sample_rate={sample_rate}"
        )

    duration = frame_count / float(sample_rate)
    return frames, duration, temp_path


def build_requests(audio_path, lang, chunk_bytes, sleep_seconds):
    sid = int(time.time() * 1e6) + secrets.randbelow(1000)
    conference_id = str(secrets.randbits(32))
    session_param = {
        "sid": f"Beijing-TSC-test-{sid}",
        "lang": lang,
        "latency": "long",
        "userinfo": json.dumps(
            {
                "conferenceType": "Caption",
                "conferenceId": conference_id,
                "source": 2,
                "talkId": str(secrets.randbits(32)),
                "userId": "grpc-probe",
                "save": 1,
            },
            ensure_ascii=False,
        ),
    }
    log(f"[send] session sid={session_param['sid']} lang={lang} conferenceId={conference_id}")
    yield AsrRequest(sessionParam=session_param)

    pcm, duration, _ = load_pcm(audio_path)
    total_chunks = math.ceil(len(pcm) / chunk_bytes)
    log(f"[audio] {audio_path} bytes={len(pcm)} duration={duration:.2f}s chunks={total_chunks}")

    for index in range(total_chunks):
        chunk = pcm[index * chunk_bytes : (index + 1) * chunk_bytes]
        log(f"[send] audio chunk {index + 1}/{total_chunks} bytes={len(chunk)}")
        yield AsrRequest(samples=chunk)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    log("[send] endFlag=True")
    yield AsrRequest(endFlag=True)


def parse_data(data):
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    if not data:
        return None, None
    try:
        return data, json.loads(data)
    except json.JSONDecodeError:
        return data, None


def extract_text(result):
    asr = ""
    mt = ""
    if isinstance(result, dict):
        try:
            if result.get("ws"):
                asr = result["ws"][0]["cw"][0].get("w", "")
        except (KeyError, IndexError, TypeError):
            pass
        try:
            if result.get("mt_ws"):
                mt = result["mt_ws"][0]["cw"][0].get("w", "")
        except (KeyError, IndexError, TypeError):
            pass
    return asr, mt


def run_probe(args):
    audio_path = args.audio or make_silence_wav(args.silence_seconds)
    generated_audio = args.audio is None
    log(f"[config] endpoint={args.endpoint} lang={args.lang} timeout={args.timeout}s")

    try:
        with grpc.insecure_channel(args.endpoint) as channel:
            if not args.skip_ready_check:
                log(f"[check] waiting for channel ready, timeout={args.ready_timeout}s")
                grpc.channel_ready_future(channel).result(timeout=args.ready_timeout)
                log("[check] channel ready")

            stub = asr_pb2_grpc.AsrServiceStub(channel)
            started = time.monotonic()
            count = 0
            for response in stub.createRec(
                build_requests(audio_path, args.lang, args.chunk_bytes, args.sleep),
                timeout=args.timeout,
            ):
                count += 1
                elapsed = time.monotonic() - started
                raw, result = parse_data(getattr(response, "data", None))
                log(
                    f"[recv #{count} +{elapsed:.2f}s] status={getattr(response, 'status', None)} "
                    f"endFlag={getattr(response, 'endFlag', None)} message={getattr(response, 'message', '')!r}"
                )
                if raw:
                    log(f"[raw] {raw[:1200]}")
                if result is not None:
                    asr, mt = extract_text(result)
                    log(
                        "[json] "
                        f"sn={result.get('sn')} type={result.get('type')} "
                        f"bg={result.get('bg')} ed={result.get('ed')} "
                        f"mt_type={result.get('mt_type')} hold_n={result.get('hold_n')}"
                    )
                    if asr:
                        log(f"[asr] {asr}")
                    if mt:
                        log(f"[mt] {mt}")
                if args.max_responses and count >= args.max_responses:
                    log(f"[stop] reached --max-responses={args.max_responses}")
                    break

            if count == 0:
                raise RuntimeError("stream ended without any response")
            log(f"[done] received {count} response(s)")
    except grpc.FutureTimeoutError:
        log("[error] channel_ready timeout. Endpoint is unreachable or not a gRPC server.")
        return 2
    except grpc.RpcError as exc:
        log(f"[error] grpc code={exc.code()} details={exc.details()}")
        return 3
    except Exception as exc:
        log(f"[error] {type(exc).__name__}: {exc}")
        return 1
    finally:
        if generated_audio:
            Path(audio_path).unlink(missing_ok=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Probe the streaming S2TT gRPC service.")
    parser.add_argument("--endpoint", default="10.185.1.71:16552", help="gRPC host:port")
    parser.add_argument("--audio", help="Audio/video file. WAV is preferred.")
    parser.add_argument("--lang", default="zh", choices=["zh", "en"], help="Service language parameter")
    parser.add_argument("--timeout", type=int, default=60, help="RPC deadline in seconds")
    parser.add_argument("--ready-timeout", type=int, default=8, help="Channel ready timeout in seconds")
    parser.add_argument("--max-responses", type=int, default=20, help="Stop after N responses; 0 means no limit")
    parser.add_argument("--chunk-bytes", type=int, default=DEFAULT_CHUNK_BYTES, help="Bytes per audio chunk")
    parser.add_argument("--sleep", type=float, default=0.4, help="Sleep seconds between chunks")
    parser.add_argument("--silence-seconds", type=float, default=3.0, help="Silence duration when --audio is omitted")
    parser.add_argument("--skip-ready-check", action="store_true", help="Skip grpc.channel_ready_future check")
    raise SystemExit(run_probe(parser.parse_args()))


if __name__ == "__main__":
    main()

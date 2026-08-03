# -*- coding: utf-8 -*-
"""
批量调用 qwen3.5-livetranslate-flash-realtime 翻译文件夹下的音频文件。

核心逻辑：
1. 输入参数只需要给一个音频文件夹 INPUT_MEDIA_DIR。
2. 自动扫描文件夹下所有音频文件。
3. 每个音频文件单独建立 WebSocket 会话，逐个调用实时翻译接口。
4. ffmpeg 自动转成 16kHz / mono / PCM。
5. 按实时速度分块发送音频。
6. 开启 modalities=["text", "audio"]，主收 response.audio_transcript.done 作为译文。
7. 最终只输出一个 JSONL 文件。
8. JSONL 每行只保留：
   - idx
   - source_language
   - target_language
   - source_text
   - translation
9. idx 直接使用音频文件名，例如 Dataflow_001.wav。
"""

import os
import json
import time
import base64
import asyncio
import subprocess
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import quote
import websockets


DASHSCOPE_API_KEY = "sk-33c1d03f05744da1bfb55e7aae3c6f28"

# 输入音频文件夹：程序会依次处理这个文件夹下的所有音频
INPUT_MEDIA_DIR = "/data/yb/Code/xiaoyi_data/KO/backup"

# 最终只输出这一个 JSONL 文件
OUTPUT_JSONL_PATH = "/data/yb/Code/xiaoyi_data/KO/qwen/second.jsonl"

FFMPEG_PATH = "ffmpeg"

# 中文 zh，英语 en，日语 ja，韩语 ko，俄语 ru，泰语 th，阿拉伯语 ar
SOURCE_LANGUAGE: Optional[str] = "ko"
TARGET_LANGUAGE = "zh"

ENABLE_SOURCE_ASR = True

# 关键：这里必须 True，这样会走 response.audio_transcript.done 路径
ENABLE_TRANSLATION_AUDIO = False

# 每次发送多少毫秒音频
CHUNK_MS = 100

# 建议 True，模拟实时发送
REALTIME_SEND = True

# 发送速度倍率。1.0 最稳
SEND_SPEED_FACTOR = 1.0

# 文件结束后先补一点静音，再发送 session.finish
ENDING_SILENCE_SEC = 2.0

# 发送 session.finish 后最多等待多久
WAIT_SESSION_FINISHED_MAX_SEC = 180

# 如果一直没收到 session.finished，则用 idle 兜底
DRAIN_IDLE_SEC = 15

# 是否递归扫描子文件夹
RECURSIVE_SCAN = False

# 支持的音频后缀
SUPPORTED_AUDIO_SUFFIXES = {
    ".wav",
    ".mp3",
    ".m4a",
    ".aac",
    ".flac",
    ".ogg",
    ".opus",
    ".wma",
    ".mp4",
    ".mov",
    ".mkv",
    ".avi",
}

# 打印控制
PRINT_STREAMING_TEXT = False
PRINT_FINAL_SEGMENTS = True
PRINT_RESPONSE_DONE = False
PRINT_UNKNOWN_EVENTS = False
PRINT_AUDIO_DELTA_PROGRESS = False

API_URL = (
    "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
    "?model=qwen3.5-livetranslate-flash-realtime"
)


# ============================================================
# 工具函数
# ============================================================

def make_event_id() -> str:
    return f"event_{int(time.time() * 1000)}"


def check_api_key(api_key: str):
    if not api_key:
        raise ValueError("DASHSCOPE_API_KEY 不能为空")

    if api_key in {"sk-", "sk-**", "sk-***", "sk-xxxxxxxxxxxxxxxxxxxxxxxx"}:
        raise ValueError("请先把 DASHSCOPE_API_KEY 改成你自己的真实 API Key")


def check_input_dir(path: str):
    if not path:
        raise ValueError("INPUT_MEDIA_DIR 不能为空")

    if not os.path.exists(path):
        raise FileNotFoundError(f"输入文件夹不存在：{path}")

    if not os.path.isdir(path):
        raise ValueError(f"INPUT_MEDIA_DIR 不是文件夹：{path}")


def build_ffmpeg_command(input_path: str) -> List[str]:
    """
    转成 raw PCM：
    - 16000 Hz
    - mono
    - signed 16-bit little-endian
    """
    return [
        FFMPEG_PATH,
        "-hide_banner",
        "-loglevel", "error",
        "-i", input_path,
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "pipe:1",
    ]


def ensure_parent_dir(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def normalize_segment (text: str) -> str:
    return (text or "").strip()


def append_no_adjacent_duplicate(items: List[str], text: str):
    """
    只去掉相邻完全重复，避免服务端偶发重复 done。
    不做全局去重，防止误删真实重复内容。
    """
    text = normalize_segment(text)

    if not text:
        return

    if items and items[-1].strip() == text:
        return

    items.append(text)


def join_segments_by_language(segments: List[str], language: Optional[str]) -> str:
    clean_segments = [normalize_segment(x) for x in segments if normalize_segment(x)]

    if not clean_segments:
        return ""

    no_space_langs = {"zh", "ja", "th"}

    if language in no_space_langs:
        return "".join(clean_segments).strip()

    return " ".join(clean_segments).strip()


def safe_json_dumps(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False)


def scan_media_files(input_dir: str) -> List[Path]:
    root = Path(input_dir)

    if RECURSIVE_SCAN:
        candidates = [p for p in root.rglob("*") if p.is_file()]
    else:
        candidates = [p for p in root.iterdir() if p.is_file()]

    media_files = [
        p for p in candidates
        if p.suffix.lower() in SUPPORTED_AUDIO_SUFFIXES
    ]

    media_files.sort(key=lambda x: x.name.lower())
    return media_files


def init_output_jsonl(path: str):
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        pass


def append_record_to_jsonl(path: str, record: Dict[str, Any]):
    with open(path, "a", encoding="utf-8") as f:
        f.write(safe_json_dumps(record) + "\n")
        f.flush()


# ============================================================
# 客户端
# ============================================================

class OfflineLiveTranslateClient:
    def __init__(
        self,
        api_key: str,
        input_media_path: str,
        source_language: Optional[str],
        target_language: str,
        enable_source_asr: bool = True,
        enable_translation_audio: bool = True,
    ):
        check_api_key(api_key)

        self.api_key = api_key
        self.input_media_path = input_media_path
        self.source_language = source_language
        self.target_language = target_language
        self.enable_source_asr = enable_source_asr
        self.enable_translation_audio = enable_translation_audio

        self.ws = None
        self.is_connected = False
        self.stop_sending = False
        self.disconnect_reason = ""

        self.source_segments: List[str] = []

        # 主路径：text + audio 模式下的译文字幕
        self.audio_transcript_segments: List[str] = []

        # 兜底路径：response.text.done
        self.text_done_segments: List[str] = []

        self.start_wall_time = time.time()
        self.last_event_wall_time = time.time()

        self.total_sent_bytes = 0
        self.total_sent_audio_sec = 0.0

        self.first_asr_time = None
        self.first_translation_time = None

        self.current_response_text_parts: List[str] = []
        self.current_audio_transcript_parts: List[str] = []
        self.current_asr_stream_preview = ""

        self.session_finish_sent = False
        self.session_finished_received = False

        self.response_done_count = 0
        self.asr_completed_count = 0
        self.audio_transcript_done_count = 0
        self.text_done_count = 0

        self.audio_delta_count = 0
        self.audio_delta_total_bytes = 0

    # --------------------------------------------------------
    # WebSocket
    # --------------------------------------------------------

    async def connect(self):
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }

        try:
            self.ws = await websockets.connect(
                API_URL,
                additional_headers=headers,
                ping_interval=None,
                close_timeout=3,
                open_timeout=30,
                max_size=None,
            )
        except TypeError:
            self.ws = await websockets.connect(
                API_URL,
                extra_headers=headers,
                ping_interval=None,
                close_timeout=3,
                open_timeout=30,
                max_size=None,
            )

        self.is_connected = True
        self.stop_sending = False
        self.disconnect_reason = ""

        print(f"[INFO] WebSocket 已连接：{API_URL}")

        await self.configure_session()
        await asyncio.sleep(0.8)

    async def configure_session(self):
        modalities = ["text", "audio"] if self.enable_translation_audio else ["text"]

        session = {
            "modalities": modalities,
            "input_audio_format": "pcm",
            "output_audio_format": "pcm",
            "translation": {
                "language": self.target_language
            }
        }

        if self.enable_source_asr:
            asr_config = {
                "model": "qwen3-asr-flash-realtime"
            }

            if self.source_language:
                asr_config["language"] = self.source_language

            session["input_audio_transcription"] = asr_config

        config = {
            "event_id": make_event_id(),
            "type": "session.update",
            "session": session
        }

        print("[INFO] 发送会话配置：")
        print(json.dumps(config, ensure_ascii=False, indent=2))

        await self.ws.send(json.dumps(config, ensure_ascii=False))

    # --------------------------------------------------------
    # 音频发送
    # --------------------------------------------------------

    async def send_audio_chunk(self, pcm_bytes: bytes) -> bool:
        if not self.is_connected or self.stop_sending:
            return False

        if not pcm_bytes:
            return True

        event = {
            "event_id": make_event_id(),
            "type": "input_audio_buffer.append",
            "audio": base64.b64encode(pcm_bytes).decode("utf-8")
        }

        try:
            await self.ws.send(json.dumps(event, ensure_ascii=False))
            return True

        except websockets.exceptions.ConnectionClosed as e:
            self.is_connected = False
            self.stop_sending = True
            self.disconnect_reason = str(e)
            print(f"\n[WARNING] WebSocket 已断开，停止继续发送音频：{e}")
            return False

        except (asyncio.TimeoutError, TimeoutError) as e:
            self.is_connected = False
            self.stop_sending = True
            self.disconnect_reason = str(e)
            print(f"\n[WARNING] WebSocket 发送超时，停止继续发送音频：{e}")
            return False

        except OSError as e:
            self.is_connected = False
            self.stop_sending = True
            self.disconnect_reason = str(e)
            print(f"\n[WARNING] WebSocket 网络异常，停止继续发送音频：{e}")
            return False

    async def send_ending_silence(self, seconds: float):
        if seconds <= 0:
            return

        if not self.is_connected or self.stop_sending:
            print("[WARNING] 连接已断开，跳过结尾静音")
            return

        bytes_per_second = 16000 * 2
        total_bytes = int(bytes_per_second * seconds)

        chunk_bytes = int(16000 * 2 * CHUNK_MS / 1000)
        chunk_count = max(1, total_bytes // chunk_bytes)
        silence = b"\x00" * chunk_bytes

        print(f"[INFO] 发送 {seconds:.2f} 秒静音用于收尾")

        for _ in range(chunk_count):
            if not self.is_connected or self.stop_sending:
                break

            ok = await self.send_audio_chunk(silence)
            if not ok:
                break

            if REALTIME_SEND:
                await asyncio.sleep((CHUNK_MS / 1000.0) / max(SEND_SPEED_FACTOR, 0.01))

    async def send_session_finish(self):
        if not self.is_connected or self.stop_sending:
            print("[WARNING] 连接已断开，无法发送 session.finish")
            return False

        if self.session_finish_sent:
            return True

        event = {
            "event_id": make_event_id(),
            "type": "session.finish"
        }

        try:
            await self.ws.send(json.dumps(event, ensure_ascii=False))
            self.session_finish_sent = True
            self.last_event_wall_time = time.time()
            print("[INFO] 已发送 session.finish，等待 session.finished")
            return True

        except websockets.exceptions.ConnectionClosed as e:
            self.is_connected = False
            self.stop_sending = True
            self.disconnect_reason = str(e)
            print(f"[WARNING] session.finish 发送失败，连接已关闭：{e}")
            return False

        except Exception as e:
            self.disconnect_reason = str(e)
            print(f"[WARNING] session.finish 发送失败：{e}")
            return False

    async def stream_local_file(self):
        input_path = self.input_media_path

        if not os.path.exists(input_path):
            raise FileNotFoundError(f"输入文件不存在：{input_path}")

        chunk_bytes = int(16000 * 2 * CHUNK_MS / 1000)
        cmd = build_ffmpeg_command(input_path)

        print("[INFO] 启动 ffmpeg：")
        print(" ".join(f'"{x}"' if " " in x else x for x in cmd))

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        start_time = time.time()
        last_progress_bucket = -1
        eof_reached = False

        try:
            while self.is_connected and not self.stop_sending:
                pcm_chunk = await asyncio.to_thread(process.stdout.read, chunk_bytes)

                if not pcm_chunk:
                    eof_reached = True
                    break

                self.total_sent_bytes += len(pcm_chunk)
                self.total_sent_audio_sec = self.total_sent_bytes / 2 / 16000

                ok = await self.send_audio_chunk(pcm_chunk)
                if not ok:
                    print("[WARNING] 发送音频时连接已断开，停止读取后续音频")
                    break

                progress_bucket = int(self.total_sent_audio_sec // 10)
                if progress_bucket != last_progress_bucket:
                    last_progress_bucket = progress_bucket
                    print(f"[INFO] 已发送音频约 {self.total_sent_audio_sec:.2f} 秒")

                if REALTIME_SEND:
                    actual_chunk_sec = len(pcm_chunk) / 2 / 16000
                    await asyncio.sleep(actual_chunk_sec / max(SEND_SPEED_FACTOR, 0.01))

            if eof_reached:
                return_code = await asyncio.to_thread(process.wait)
                stderr_bytes = await asyncio.to_thread(process.stderr.read)
                stderr_text = stderr_bytes.decode("utf-8", errors="ignore")

                if return_code != 0:
                    raise RuntimeError(f"ffmpeg 转码失败：\n{stderr_text}")

                elapsed = time.time() - start_time

                print("[INFO] 音频发送完成")
                print(f"[INFO] 音频时长约：{self.total_sent_audio_sec:.2f} 秒")
                print(f"[INFO] 实际发送耗时：{elapsed:.2f} 秒")

                await self.send_ending_silence(ENDING_SILENCE_SEC)
                await self.send_session_finish()

            else:
                print("[WARNING] 音频未完整发送，可能是 WebSocket 已断开或程序停止发送")

        finally:
            if process.poll() is None:
                try:
                    process.kill()
                except Exception:
                    pass

    # --------------------------------------------------------
    # 服务端事件接收
    # --------------------------------------------------------

    async def receive_messages(self):
        try:
            async for message in self.ws:
                self.last_event_wall_time = time.time()

                try:
                    event = json.loads(message)
                except Exception:
                    print(f"[WARNING] 收到非 JSON 消息：{message}")
                    continue

                event_type = event.get("type", "")

                # -------------------------
                # 源语言 ASR
                # -------------------------

                if event_type == "conversation.item.input_audio_transcription.completed":
                    transcript = normalize_segment(event.get("transcript", ""))

                    if transcript:
                        if self.first_asr_time is None:
                            self.first_asr_time = time.time() - self.start_wall_time

                        append_no_adjacent_duplicate(self.source_segments, transcript)
                        self.asr_completed_count += 1

                        if PRINT_FINAL_SEGMENTS:
                            print(f"\n[源文段 {len(self.source_segments)}] {transcript}")

                elif event_type == "conversation.item.input_audio_transcription.text":
                    text = event.get("text", "")
                    stash = event.get("stash", "")
                    preview = normalize_segment((text or "") + (stash or ""))

                    if preview:
                        self.current_asr_stream_preview = preview

                        if PRINT_STREAMING_TEXT:
                            print(f"\r[识别中] {preview}", end="", flush=True)

                # -------------------------
                # 主译文路径：text + audio 模式下的字幕
                # -------------------------

                elif event_type == "response.audio_transcript.text":
                    text = event.get("text", "")
                    stash = event.get("stash", "")
                    preview = normalize_segment((text or "") + (stash or ""))

                    if preview and PRINT_STREAMING_TEXT:
                        print(f"\r[译文音频字幕流式] {preview}", end="", flush=True)

                elif event_type == "response.audio_transcript.delta":
                    delta = normalize_segment(event.get("delta", ""))

                    if delta:
                        self.current_audio_transcript_parts.append(delta)

                        if PRINT_STREAMING_TEXT:
                            print(f"\r[译文音频字幕增量] {delta}", end="", flush=True)

                elif event_type == "response.audio_transcript.done":
                    text = normalize_segment(
                        event.get("transcript", "") or event.get("text", "")
                    )

                    if not text and self.current_audio_transcript_parts:
                        text = normalize_segment("".join(self.current_audio_transcript_parts))

                    self.current_audio_transcript_parts.clear()

                    if text:
                        if self.first_translation_time is None:
                            self.first_translation_time = time.time() - self.start_wall_time

                        append_no_adjacent_duplicate(self.audio_transcript_segments, text)
                        self.audio_transcript_done_count += 1

                        if PRINT_FINAL_SEGMENTS:
                            print(f"\n[译文音频字幕段 {len(self.audio_transcript_segments)}] {text}")

                # -------------------------
                # 兜底译文路径：response.text.done
                # -------------------------

                elif event_type == "response.text.delta":
                    delta = event.get("delta", "")

                    if delta:
                        self.current_response_text_parts.append(delta)

                        if PRINT_STREAMING_TEXT:
                            print(f"\r[译文文本流式] {delta}", end="", flush=True)

                elif event_type == "response.text.done":
                    text = normalize_segment(event.get("text", ""))

                    if not text and self.current_response_text_parts:
                        text = normalize_segment("".join(self.current_response_text_parts))

                    self.current_response_text_parts.clear()

                    if text:
                        append_no_adjacent_duplicate(self.text_done_segments, text)
                        self.text_done_count += 1

                        if PRINT_FINAL_SEGMENTS:
                            print(f"\n[译文文本兜底段 {len(self.text_done_segments)}] {text}")

                # -------------------------
                # 译文音频流：只接收，不保存
                # -------------------------

                elif event_type == "response.audio.delta":
                    audio_b64 = event.get("delta", "")

                    if audio_b64:
                        try:
                            audio_bytes = base64.b64decode(audio_b64)
                        except Exception:
                            audio_bytes = b""

                        self.audio_delta_count += 1
                        self.audio_delta_total_bytes += len(audio_bytes)

                        if PRINT_AUDIO_DELTA_PROGRESS and self.audio_delta_count % 50 == 0:
                            print(
                                f"\n[译文音频流] chunks={self.audio_delta_count}, "
                                f"bytes={self.audio_delta_total_bytes}"
                            )

                # -------------------------
                # 其他事件
                # -------------------------

                elif event_type == "response.done":
                    self.response_done_count += 1

                    if PRINT_RESPONSE_DONE:
                        print("\n[INFO] response.done")

                        usage = event.get("response", {}).get("usage", {})
                        if usage:
                            print("[INFO] Token 使用情况：")
                            print(json.dumps(usage, ensure_ascii=False, indent=2))

                elif event_type == "session.finished":
                    self.session_finished_received = True
                    print("\n[INFO] 收到 session.finished，服务端会话已完整结束")

                elif event_type == "error":
                    print("\n[ERROR] 服务端返回错误：")
                    print(json.dumps(event, ensure_ascii=False, indent=2))

                elif event_type in {
                    "session.created",
                    "session.updated",
                    "input_audio_buffer.speech_started",
                    "input_audio_buffer.speech_stopped",
                    "input_audio_buffer.committed",
                    "response.created",
                    "response.output_item.added",
                    "response.content_part.added",
                    "response.content_part.done",
                    "response.output_item.done",
                    "response.audio.done",
                }:
                    pass

                else:
                    if PRINT_UNKNOWN_EVENTS:
                        print("[DEBUG_UNKNOWN_EVENT]")
                        print(json.dumps(event, ensure_ascii=False, indent=2))

        except asyncio.CancelledError:
            pass

        except websockets.exceptions.ConnectionClosed as e:
            print(f"\n[INFO] WebSocket 连接关闭：{e}")
            self.is_connected = False
            self.stop_sending = True
            self.disconnect_reason = str(e)

        except Exception as e:
            print(f"\n[ERROR] 接收消息异常：{e}")
            traceback.print_exc()
            self.is_connected = False
            self.stop_sending = True
            self.disconnect_reason = str(e)

    # --------------------------------------------------------
    # 等待服务端完整结束
    # --------------------------------------------------------

    async def wait_for_session_finished(self):
        if not self.is_connected:
            print("[WARNING] WebSocket 已断开，跳过 session.finished 等待")
            return

        print("=" * 70)
        print("[INFO] 等待服务端 session.finished")
        print(f"[INFO] 最多等待 {WAIT_SESSION_FINISHED_MAX_SEC} 秒")
        print("=" * 70)

        begin = time.time()

        while True:
            if self.session_finished_received:
                break

            if not self.is_connected:
                print("[WARNING] 等待过程中 WebSocket 已断开")
                break

            now = time.time()
            total_wait = now - begin
            idle_time = now - self.last_event_wall_time

            if total_wait >= WAIT_SESSION_FINISHED_MAX_SEC:
                print(f"[WARNING] 超过最大等待时间 {WAIT_SESSION_FINISHED_MAX_SEC} 秒，准备保存已有结果")
                break

            if self.session_finish_sent and idle_time >= DRAIN_IDLE_SEC:
                print(f"[WARNING] 已发送 session.finish，但服务端连续 {DRAIN_IDLE_SEC} 秒无新事件，准备保存已有结果")
                break

            await asyncio.sleep(0.5)

    # --------------------------------------------------------
    # 最终结果
    # --------------------------------------------------------

    def choose_translation_segments(self) -> List[str]:
        """
        优先选择 audio_transcript_segments。
        如果 audio_transcript 为空，则用 text_done_segments 兜底。
        """
        audio_text = join_segments_by_language(
            self.audio_transcript_segments,
            self.target_language,
        )

        text_done_text = join_segments_by_language(
            self.text_done_segments,
            self.target_language,
        )

        if audio_text:
            return self.audio_transcript_segments

        if text_done_text:
            return self.text_done_segments

        return []

    def build_final_record(self) -> Dict[str, Any]:
        final_source_text = join_segments_by_language(
            self.source_segments,
            self.source_language,
        )

        selected_translation_segments = self.choose_translation_segments()

        final_translation = join_segments_by_language(
            selected_translation_segments,
            self.target_language,
        )

        media_name = Path(self.input_media_path).name
        # 如果你希望 idx 不带后缀，例如 Dataflow_001，而不是 Dataflow_001.wav，
        # 把上一行改成：
        # media_name = Path(self.input_media_path).stem

        record = {
            "idx": media_name,
            "source_language": self.source_language,
            "target_language": self.target_language,
            "source_text": final_source_text,
            "translation": final_translation,
        }

        return record

    async def close(self):
        self.is_connected = False
        self.stop_sending = True

        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=3)
            except (asyncio.TimeoutError, TimeoutError):
                print("[WARNING] WebSocket close 超时，已忽略")
            except Exception as e:
                print(f"[WARNING] WebSocket close 异常，已忽略：{e}")

        print("[INFO] WebSocket 已关闭")


# ============================================================
# 单文件处理
# ============================================================

async def process_one_media_file(media_path: Path) -> Dict[str, Any]:
    print("\n" + "=" * 90)
    print(f"[INFO] 开始处理文件：{media_path}")
    print("=" * 90)

    client = OfflineLiveTranslateClient(
        api_key=DASHSCOPE_API_KEY,
        input_media_path=str(media_path),
        source_language=SOURCE_LANGUAGE,
        target_language=TARGET_LANGUAGE,
        enable_source_asr=ENABLE_SOURCE_ASR,
        enable_translation_audio=ENABLE_TRANSLATION_AUDIO,
    )

    receiver_task = None

    try:
        await client.connect()

        receiver_task = asyncio.create_task(client.receive_messages())

        await client.stream_local_file()

        await client.wait_for_session_finished()

    except Exception as e:
        print(f"\n[ERROR] 文件处理异常：{media_path}")
        print(f"[ERROR] 异常信息：{e}")
        traceback.print_exc()

    finally:
        if receiver_task:
            receiver_task.cancel()
            try:
                await receiver_task
            except asyncio.CancelledError:
                pass

        record = client.build_final_record()

        await client.close()

    print("[INFO] 当前文件处理结束")
    print(f"[INFO] idx: {record['idx']}")
    print(f"[INFO] source_text 长度: {len(record['source_text'])}")
    print(f"[INFO] translation 长度: {len(record['translation'])}")

    return record


# ============================================================
# 主函数
# ============================================================

async def main():
    check_api_key(DASHSCOPE_API_KEY)
    check_input_dir(INPUT_MEDIA_DIR)

    media_files = scan_media_files(INPUT_MEDIA_DIR)

    print("=" * 90)
    print("qwen3.5-livetranslate-flash-realtime 文件夹批量翻译")
    print("=" * 90)
    print(f"[INFO] 输入文件夹：{INPUT_MEDIA_DIR}")
    print(f"[INFO] 输出 JSONL：{OUTPUT_JSONL_PATH}")
    print(f"[INFO] 源语言：{SOURCE_LANGUAGE}")
    print(f"[INFO] 目标语言：{TARGET_LANGUAGE}")
    print(f"[INFO] 扫描到音频/媒体文件数量：{len(media_files)}")
    print("=" * 90)

    if not media_files:
        print("[WARNING] 没有扫描到可处理的音频/媒体文件")
        return

    for i, path in enumerate(media_files, start=1):
        print(f"[INFO] 待处理 {i}/{len(media_files)}：{path.name}")

    init_output_jsonl(OUTPUT_JSONL_PATH)

    success_count = 0

    for i, media_path in enumerate(media_files, start=1):
        print("\n" + "#" * 90)
        print(f"[INFO] 批处理进度：{i}/{len(media_files)}")
        print("#" * 90)

        record = await process_one_media_file(media_path)

        append_record_to_jsonl(OUTPUT_JSONL_PATH, record)

        if record.get("source_text") or record.get("translation"):
            success_count += 1

        print(f"[INFO] 已写入 JSONL：{OUTPUT_JSONL_PATH}")

    print("\n" + "=" * 90)
    print("[INFO] 全部处理完成")
    print(f"[INFO] 总文件数：{len(media_files)}")
    print(f"[INFO] 有结果文件数：{success_count}")
    print(f"[INFO] 输出 JSONL：{OUTPUT_JSONL_PATH}")
    print("=" * 90)


if __name__ == "__main__":
    asyncio.run(main())
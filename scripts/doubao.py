# -*- coding: utf-8 -*-
"""
代码名称：doubao_ast_s2t_batch_readable_stream.py

豆包语音同声传译 2.0 / AST / S2T 批量调用脚本

功能：
    1. 输入一个音频文件夹
    2. 对文件夹下所有音频依次调用豆包同声传译
    3. 最终输出一个 jsonl 文件：
       {"vid": "xxx.wav", "Doubao_asr": "...", "Doubao_trans": "..."}
    4. 额外保存每个音频的底层流式事件日志 jsonl
    5. 额外保存每个音频的可读时间线日志 jsonl
    6. 终端打印更容易理解的过程：
       - 原文是如何逐步生成的
       - 原文片段最终结果
       - 译文是如何逐步生成的
       - 译文片段最终结果
       - 原文片段和译文片段的顺序配对

依赖：
    pip install websockets protobuf

系统依赖：
    ffmpeg

目录要求：
    当前脚本同级目录下必须有官方 demo 的 python_protogen 文件夹。
"""

import asyncio
import uuid
import os
import sys
import re
import time
import json
import shutil
import logging
import subprocess
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import websockets
from google.protobuf.json_format import MessageToDict


# ============================================================
# 1. 只需要改这里
# ============================================================

API_KEY = "4d396124-e651-441d-839f-056d585dfbfb"

# 输入音频文件夹
INPUT_AUDIO_DIR = "/data/yb/Code/Ddoubao_realtime/backup/Byte_en2zh_second"

# 最终输出 jsonl：只保留 vid / Doubao_asr / Doubao_trans
OUTPUT_JSONL_PATH = "/data/yb/Code/Ddoubao_realtime/backup/Byte_en2zh_second/Byte_en2zh_second.jsonl"

# 底层流式事件日志目录：每个音频一个 jsonl
RAW_STREAM_EVENT_LOG_DIR = "/data/yb/Code/Ddoubao_realtime/backup/Byte_en2zh_second/doubao_raw_stream_event_logs"

# 可读过程日志目录：每个音频一个 jsonl
READABLE_TIMELINE_LOG_DIR = "/data/yb/Code/Ddoubao_realtime/backup/Byte_en2zh_second/doubao_readable_timeline_logs"

# 语言方向
SOURCE_LANGUAGE = "en"
TARGET_LANGUAGE = "zh"

# s2t = 语音 -> 翻译文本
MODE = "s2t"

WS_URL = "wss://openspeech.bytedance.com/api/v4/ast/v2/translate"
RESOURCE_ID = "volc.service_type.10053"

# 100ms chunk：16000 * 2 * 0.1 = 3200 bytes
CHUNK_SIZE = 3200
CHUNK_SLEEP_SEC = 0.1

# 支持的音频/视频后缀
AUDIO_EXTENSIONS = {
    ".wav", ".mp3", ".m4a", ".aac", ".flac", ".ogg", ".opus",
    ".mp4", ".mov", ".mkv", ".avi", ".webm"
}

# 如果输出 jsonl 已存在，是否跳过已经处理过的 vid
SKIP_EXISTING = True

# 是否保存底层流式事件
SAVE_RAW_STREAM_EVENTS = True

# 是否保存可读时间线
SAVE_READABLE_TIMELINE = True

# 是否在终端打印可读过程
PRINT_READABLE_TIMELINE = True

# 是否打印 building 过程。
# True 会看到“原文/译文如何一步步变长”。
# False 只打印最终片段和配对结果。
PRINT_BUILDING_PROCESS = True

# 是否记录发送 chunk 的过程。
# 音频很多或很长时，打开会产生很多日志。
LOG_SEND_CHUNKS = True

# 是否保存 response raw_dict
SAVE_RAW_DICT = True

# raw_dict 中 data 字段可能较大，默认去掉
DROP_RAW_DATA_FIELD = True


# ============================================================
# 2. 导入官方 protobuf
# ============================================================

current_dir = os.path.dirname(os.path.abspath(__file__))
protogen_dir = os.path.join(current_dir, "python_protogen")

if protogen_dir not in sys.path:
    sys.path.append(protogen_dir)

try:
    from products.understanding.ast.ast_service_pb2 import (
        TranslateRequest,
        TranslateResponse,
    )
    from common.events_pb2 import Type
except Exception as e:
    raise RuntimeError(
        "导入官方 protobuf 失败。\n"
        "请确认 python_protogen 文件夹和当前脚本在同一级目录。\n"
        f"当前查找路径：{protogen_dir}\n"
        f"原始错误：{repr(e)}"
    )


# ============================================================
# 3. 数据结构
# ============================================================

@dataclass
class Config:
    ws_url: str
    api_key: str
    resource_id: str


@dataclass
class Audio:
    binary_data: Optional[bytes] = None


@dataclass
class TranslateRequestData:
    session_id: str
    event: str
    source_audio: Optional[Audio] = None


@dataclass
class TranslateResponseData:
    event: int
    text: str
    data: bytes
    message: str
    raw_dict: Dict[str, Any]


# ============================================================
# 4. 基础工具
# ============================================================

def check_config() -> None:
    if not API_KEY or API_KEY.strip() in {"--", "你的新版API_KEY写在这里"}:
        raise RuntimeError("请先填写 API_KEY。")

    input_dir = Path(INPUT_AUDIO_DIR)
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"输入音频文件夹不存在：{input_dir}")

    if shutil.which("ffmpeg") is None:
        raise RuntimeError("没有找到 ffmpeg，请先安装 ffmpeg，并确保 ffmpeg 在 PATH 中。")

    if SOURCE_LANGUAGE == TARGET_LANGUAGE:
        logging.warning("SOURCE_LANGUAGE 和 TARGET_LANGUAGE 相同，请确认是否设置正确。")


def iter_audio_files(input_dir: str) -> List[Path]:
    root = Path(input_dir)
    files = []

    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in AUDIO_EXTENSIONS:
            files.append(p)

    return sorted(files, key=lambda x: str(x).lower())


def load_existing_vids(jsonl_path: str) -> set:
    path = Path(jsonl_path)
    if not path.exists():
        return set()

    vids = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                vid = obj.get("vid")
                if vid:
                    vids.add(vid)
            except Exception:
                continue

    return vids


def append_jsonl(row: Dict[str, Any], output_jsonl_path: str) -> None:
    output_path = Path(output_jsonl_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def get_type_value(name: str, fallback: int) -> int:
    try:
        return int(getattr(Type, name))
    except Exception:
        return int(fallback)


def get_event_name(event_value: int) -> str:
    try:
        return Type.Name(event_value)
    except Exception:
        pass

    manual_map = {
        get_type_value("SourceSubtitleStart", 650): "SourceSubtitleStart",
        get_type_value("SourceSubtitleResponse", 651): "SourceSubtitleResponse",
        get_type_value("SourceSubtitleEnd", 652): "SourceSubtitleEnd",
        get_type_value("TranslationSubtitleStart", 653): "TranslationSubtitleStart",
        get_type_value("TranslationSubtitleResponse", 654): "TranslationSubtitleResponse",
        get_type_value("TranslationSubtitleEnd", 655): "TranslationSubtitleEnd",
    }

    return manual_map.get(int(event_value), f"UnknownEvent_{event_value}")


def safe_message_to_dict(pb_msg) -> Dict[str, Any]:
    try:
        d = MessageToDict(pb_msg, preserving_proto_field_name=True)
    except TypeError:
        d = MessageToDict(pb_msg)
    except Exception:
        d = {}

    if DROP_RAW_DATA_FIELD and isinstance(d, dict):
        d.pop("data", None)

    return d


def build_http_headers(conf: Config, conn_id: str) -> Dict[str, str]:
    return {
        "X-Api-Key": conf.api_key.strip(),
        "X-Api-Resource-Id": conf.resource_id,
        "X-Api-Connect-Id": conn_id,
    }


async def ws_connect(url: str, headers: Dict[str, str]):
    try:
        return await websockets.connect(
            url,
            additional_headers=headers,
            max_size=1000000000,
            ping_interval=None,
        )
    except TypeError:
        return await websockets.connect(
            url,
            extra_headers=headers,
            max_size=1000000000,
            ping_interval=None,
        )


def get_ws_response_header(ws, key: str) -> Optional[str]:
    response = getattr(ws, "response", None)
    if response is not None:
        headers = getattr(response, "headers", None)
        if headers is not None:
            try:
                return headers.get(key)
            except Exception:
                pass

    headers = getattr(ws, "response_headers", None)
    if headers is not None:
        try:
            return headers.get(key)
        except Exception:
            pass

    return None


def convert_to_16k_mono_wav(input_path: str, output_wav_path: str) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        input_path,
        "-vn",
        "-acodec",
        "pcm_s16le",
        "-ac",
        "1",
        "-ar",
        "16000",
        output_wav_path,
    ]

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="ignore")
        raise RuntimeError(f"ffmpeg 转码失败：{input_path}\n{err}")


async def read_audio_chunks(audio_path: str, chunk_size: int) -> List[bytes]:
    chunks = []
    with open(audio_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
    return chunks


def make_safe_log_filename(name: str, suffix: str) -> str:
    base = Path(name).name
    base = re.sub(r'[\\/:*?"<>|\s]+', "_", base)
    base = base.strip("_")
    if not base:
        base = f"audio_{uuid.uuid4().hex}"
    return base + suffix


def normalize_console_text(text: str) -> str:
    return (text or "").replace("\n", "\\n")


# ============================================================
# 5. 日志器
# ============================================================

class JsonlLogger:
    def __init__(self, vid: str, log_dir: str, suffix: str, enabled: bool = True):
        self.vid = vid
        self.enabled = enabled
        self.seq = 0
        self.start_time = time.time()

        Path(log_dir).mkdir(parents=True, exist_ok=True)
        self.log_path = Path(log_dir) / make_safe_log_filename(vid, suffix)

    def log(self, row: Dict[str, Any]) -> None:
        if not self.enabled:
            return

        self.seq += 1

        full_row = {
            "vid": self.vid,
            "seq": self.seq,
            "time_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "relative_sec": round(time.time() - self.start_time, 6),
        }
        full_row.update(row)

        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(full_row, ensure_ascii=False) + "\n")


def console_print(title: str = "", text: str = "", force: bool = False) -> None:
    if not PRINT_READABLE_TIMELINE and not force:
        return

    if text:
        print(f"{title} {normalize_console_text(text)}", flush=True)
    else:
        print(title, flush=True)


def readable_log_and_print(
    readable_logger: JsonlLogger,
    kind: str,
    title: str,
    text: str = "",
    extra: Optional[Dict[str, Any]] = None,
    print_to_console: bool = True,
) -> None:
    row = {
        "kind": kind,
        "title": title,
        "text": text,
    }

    if extra:
        row.update(extra)

    readable_logger.log(row)

    if print_to_console:
        console_print(title, text)


# ============================================================
# 6. 请求与响应
# ============================================================

async def send_request(ws, request: TranslateRequestData) -> None:
    request_data = TranslateRequest()

    request_data.request_meta.SessionID = request.session_id

    if request.event == "Type_StartSession":
        request_data.event = Type.StartSession
    elif request.event == "Type_TaskRequest":
        request_data.event = Type.TaskRequest
    elif request.event == "Type_FinishSession":
        request_data.event = Type.FinishSession
    else:
        raise ValueError(f"未知 event：{request.event}")

    request_data.user.uid = "ast_py_client"
    request_data.user.did = "ast_py_client"

    request_data.source_audio.format = "wav"
    request_data.source_audio.rate = 16000
    request_data.source_audio.bits = 16
    request_data.source_audio.channel = 1

    if request.source_audio and request.source_audio.binary_data:
        request_data.source_audio.binary_data = request.source_audio.binary_data

    request_data.request.mode = MODE
    request_data.request.source_language = SOURCE_LANGUAGE
    request_data.request.target_language = TARGET_LANGUAGE

    await ws.send(request_data.SerializeToString())


async def receive_message(ws) -> TranslateResponseData:
    response = await ws.recv()

    response_data = TranslateResponse()
    response_data.ParseFromString(response)

    event = int(response_data.event)
    text = response_data.text or ""

    message = ""
    try:
        message = response_data.response_meta.Message
    except Exception:
        pass

    raw_dict = safe_message_to_dict(response_data)

    return TranslateResponseData(
        event=event,
        text=text,
        data=response_data.data,
        message=message,
        raw_dict=raw_dict,
    )


def log_send_event(
    raw_logger: JsonlLogger,
    session_id: str,
    event_name: str,
    chunk_index: Optional[int] = None,
    chunk_bytes: Optional[int] = None,
    total_chunks: Optional[int] = None,
) -> None:
    if event_name == "Type_TaskRequest" and not LOG_SEND_CHUNKS:
        return

    row = {
        "direction": "send",
        "session_id": session_id,
        "event_name": event_name,
    }

    if chunk_index is not None:
        row["chunk_index"] = chunk_index
    if chunk_bytes is not None:
        row["chunk_bytes"] = chunk_bytes
    if total_chunks is not None:
        row["total_chunks"] = total_chunks

    raw_logger.log(row)


def log_recv_event(
    raw_logger: JsonlLogger,
    session_id: str,
    resp: TranslateResponseData,
    tag: Optional[str] = None,
) -> None:
    row = {
        "direction": "recv",
        "session_id": session_id,
        "event": resp.event,
        "event_name": get_event_name(resp.event),
        "text": resp.text,
        "message": resp.message,
    }

    if tag:
        row["tag"] = tag

    if SAVE_RAW_DICT:
        row["raw_dict"] = resp.raw_dict

    raw_logger.log(row)


# ============================================================
# 7. 核心调用：单音频 -> row
# ============================================================

async def doubao_translate_one_audio(conf: Config, audio_path: str) -> Dict[str, str]:
    temp_wav_path = None
    conn = None

    vid = Path(audio_path).name

    raw_logger = JsonlLogger(
        vid=vid,
        log_dir=RAW_STREAM_EVENT_LOG_DIR,
        suffix=".raw_stream_events.jsonl",
        enabled=SAVE_RAW_STREAM_EVENTS,
    )

    readable_logger = JsonlLogger(
        vid=vid,
        log_dir=READABLE_TIMELINE_LOG_DIR,
        suffix=".readable_timeline.jsonl",
        enabled=SAVE_READABLE_TIMELINE,
    )

    try:
        raw_logger.log({
            "direction": "local",
            "event_name": "BeginAudio",
            "audio_path": audio_path,
        })

        readable_log_and_print(
            readable_logger,
            kind="begin_audio",
            title=f"\n========== 开始处理音频：{vid} ==========",
            text="",
        )

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            temp_wav_path = tmp.name

        raw_logger.log({
            "direction": "local",
            "event_name": "BeginFfmpegConvert",
            "input_path": audio_path,
            "temp_wav_path": temp_wav_path,
        })

        convert_to_16k_mono_wav(audio_path, temp_wav_path)

        raw_logger.log({
            "direction": "local",
            "event_name": "EndFfmpegConvert",
            "temp_wav_path": temp_wav_path,
            "temp_wav_size_bytes": os.path.getsize(temp_wav_path) if os.path.exists(temp_wav_path) else None,
        })

        audio_chunks = await read_audio_chunks(temp_wav_path, CHUNK_SIZE)

        raw_logger.log({
            "direction": "local",
            "event_name": "AudioChunkPrepared",
            "chunk_size": CHUNK_SIZE,
            "chunk_count": len(audio_chunks),
        })

        readable_log_and_print(
            readable_logger,
            kind="audio_prepared",
            title=f"[{vid}][音频切块完成]",
            text=f"chunk_count={len(audio_chunks)}, chunk_size={CHUNK_SIZE}",
        )

        if not audio_chunks:
            raise RuntimeError("音频切块为空，请检查输入音频。")

        conn_id = str(uuid.uuid4())
        headers = build_http_headers(conf, conn_id)

        raw_logger.log({
            "direction": "local",
            "event_name": "BeginWebSocketConnect",
            "conn_id": conn_id,
            "ws_url": conf.ws_url,
        })

        conn = await ws_connect(conf.ws_url, headers)
        log_id = get_ws_response_header(conn, "X-Tt-Logid")

        logging.info(f"Connected. X-Tt-Logid={log_id}")

        raw_logger.log({
            "direction": "local",
            "event_name": "WebSocketConnected",
            "conn_id": conn_id,
            "x_tt_logid": log_id,
        })

        session_id = str(uuid.uuid4())

        start_request = TranslateRequestData(
            session_id=session_id,
            event="Type_StartSession",
            source_audio=Audio(),
        )

        log_send_event(
            raw_logger=raw_logger,
            session_id=session_id,
            event_name="Type_StartSession",
        )

        await send_request(conn, start_request)

        resp = await receive_message(conn)
        log_recv_event(
            raw_logger=raw_logger,
            session_id=session_id,
            resp=resp,
            tag="StartSessionResponse",
        )

        if resp.event != Type.SessionStarted:
            raise RuntimeError(
                f"StartSession 失败：event={resp.event}, "
                f"event_name={get_event_name(resp.event)}, "
                f"message={resp.message}, logid={log_id}"
            )

        readable_log_and_print(
            readable_logger,
            kind="session_started",
            title=f"[{vid}][SessionStarted]",
            text=f"session_id={session_id}, logid={log_id}",
        )

        # 用于最终 jsonl 的拼接
        source_response_parts: List[str] = []
        translation_response_parts: List[str] = []

        source_end_segments: List[str] = []
        translation_end_segments: List[str] = []

        # 用于可读过程展示
        current_source_parts: List[str] = []
        current_translation_parts: List[str] = []

        source_final_segments: List[str] = []
        translation_final_segments: List[str] = []

        source_seg_idx = 0
        translation_seg_idx = 0
        paired_print_idx = 0

        stop_sender = asyncio.Event()

        async def send_audio_chunks():
            try:
                total_chunks = len(audio_chunks)

                for chunk_idx, chunk in enumerate(audio_chunks, start=1):
                    if stop_sender.is_set():
                        break

                    chunk_request = TranslateRequestData(
                        session_id=session_id,
                        event="Type_TaskRequest",
                        source_audio=Audio(binary_data=chunk),
                    )

                    log_send_event(
                        raw_logger=raw_logger,
                        session_id=session_id,
                        event_name="Type_TaskRequest",
                        chunk_index=chunk_idx,
                        chunk_bytes=len(chunk),
                        total_chunks=total_chunks,
                    )

                    await send_request(conn, chunk_request)
                    await asyncio.sleep(CHUNK_SLEEP_SEC)

                if not stop_sender.is_set():
                    finish_request = TranslateRequestData(
                        session_id=session_id,
                        event="Type_FinishSession",
                        source_audio=Audio(),
                    )

                    log_send_event(
                        raw_logger=raw_logger,
                        session_id=session_id,
                        event_name="Type_FinishSession",
                    )

                    await send_request(conn, finish_request)

            except websockets.ConnectionClosed:
                stop_sender.set()
                raw_logger.log({
                    "direction": "local",
                    "event_name": "SenderConnectionClosed",
                    "session_id": session_id,
                })
            except Exception as e:
                stop_sender.set()
                raw_logger.log({
                    "direction": "local",
                    "event_name": "SenderException",
                    "session_id": session_id,
                    "error": repr(e),
                })
                logging.error(f"发送音频失败：{repr(e)}")

        sender_task = asyncio.create_task(send_audio_chunks())

        source_start_event = get_type_value("SourceSubtitleStart", 650)
        source_resp_event = get_type_value("SourceSubtitleResponse", 651)
        source_end_event = get_type_value("SourceSubtitleEnd", 652)

        translation_start_event = get_type_value("TranslationSubtitleStart", 653)
        translation_resp_event = get_type_value("TranslationSubtitleResponse", 654)
        translation_end_event = get_type_value("TranslationSubtitleEnd", 655)

        try:
            while True:
                resp = await receive_message(conn)

                tag = get_event_name(resp.event)

                # ====================================================
                # 1. 原文片段开始
                # ====================================================
                if resp.event == source_start_event:
                    tag = "SourceSubtitleStart"

                    current_source_parts = []

                    readable_log_and_print(
                        readable_logger,
                        kind="source_start",
                        title=f"\n========== [{vid}] 原文片段开始 ==========",
                        text="",
                        extra={
                            "event": resp.event,
                            "event_name": tag,
                        },
                        print_to_console=True,
                    )

                # ====================================================
                # 2. 原文中间过程：SourceSubtitleResponse
                # ====================================================
                elif resp.event == source_resp_event:
                    tag = "SourceSubtitleResponse"

                    if resp.text:
                        source_response_parts.append(resp.text)
                        current_source_parts.append(resp.text)

                        current_source_text = "".join(current_source_parts)

                        readable_logger.log({
                            "kind": "source_building",
                            "event": resp.event,
                            "event_name": tag,
                            "piece": resp.text,
                            "building_text": current_source_text,
                        })

                        if PRINT_READABLE_TIMELINE and PRINT_BUILDING_PROCESS:
                            console_print(
                                f"[{vid}][原文生成中]",
                                current_source_text,
                            )

                # ====================================================
                # 3. 原文最终片段：SourceSubtitleEnd
                # ====================================================
                elif resp.event == source_end_event:
                    tag = "SourceSubtitleEnd"

                    if resp.text:
                        final_source_text = resp.text
                    else:
                        final_source_text = "".join(current_source_parts)

                    if final_source_text:
                        source_end_segments.append(final_source_text)
                        source_final_segments.append(final_source_text)

                    source_seg_idx += 1

                    readable_log_and_print(
                        readable_logger,
                        kind="source_final",
                        title=f"\n========== [{vid}] 原文片段 {source_seg_idx:03d} 完成 ==========",
                        text="",
                        extra={
                            "event": resp.event,
                            "event_name": tag,
                            "segment_index": source_seg_idx,
                        },
                    )

                    readable_log_and_print(
                        readable_logger,
                        kind="source_final_text",
                        title="[ASR_FINAL]",
                        text=final_source_text,
                        extra={
                            "segment_index": source_seg_idx,
                        },
                    )

                    current_source_parts = []

                # ====================================================
                # 4. 译文片段开始
                # ====================================================
                elif resp.event == translation_start_event:
                    tag = "TranslationSubtitleStart"

                    current_translation_parts = []

                    readable_log_and_print(
                        readable_logger,
                        kind="translation_start",
                        title=f"\n========== [{vid}] 译文片段开始 ==========",
                        text="",
                        extra={
                            "event": resp.event,
                            "event_name": tag,
                        },
                        print_to_console=True,
                    )

                # ====================================================
                # 5. 译文中间过程：TranslationSubtitleResponse
                # ====================================================
                elif resp.event == translation_resp_event:
                    tag = "TranslationSubtitleResponse"

                    if resp.text:
                        translation_response_parts.append(resp.text)
                        current_translation_parts.append(resp.text)

                        current_translation_text = "".join(current_translation_parts)

                        readable_logger.log({
                            "kind": "translation_building",
                            "event": resp.event,
                            "event_name": tag,
                            "piece": resp.text,
                            "building_text": current_translation_text,
                        })

                        if PRINT_READABLE_TIMELINE and PRINT_BUILDING_PROCESS:
                            console_print(
                                f"[{vid}][译文生成中]",
                                current_translation_text,
                            )

                # ====================================================
                # 6. 译文最终片段：TranslationSubtitleEnd
                # ====================================================
                elif resp.event == translation_end_event:
                    tag = "TranslationSubtitleEnd"

                    if resp.text:
                        final_translation_text = resp.text
                    else:
                        final_translation_text = "".join(current_translation_parts)

                    if final_translation_text:
                        translation_end_segments.append(final_translation_text)
                        translation_final_segments.append(final_translation_text)

                    translation_seg_idx += 1

                    readable_log_and_print(
                        readable_logger,
                        kind="translation_final",
                        title=f"\n========== [{vid}] 译文片段 {translation_seg_idx:03d} 完成 ==========",
                        text="",
                        extra={
                            "event": resp.event,
                            "event_name": tag,
                            "segment_index": translation_seg_idx,
                        },
                    )

                    readable_log_and_print(
                        readable_logger,
                        kind="translation_final_text",
                        title="[TRANS_FINAL]",
                        text=final_translation_text,
                        extra={
                            "segment_index": translation_seg_idx,
                        },
                    )

                    current_translation_parts = []

                # ====================================================
                # 7. 其他事件
                # ====================================================
                elif resp.event == Type.UsageResponse:
                    tag = "UsageResponse"

                elif resp.event == Type.SessionFinished:
                    tag = "SessionFinished"

                elif resp.event == Type.SessionFailed:
                    tag = "SessionFailed"

                elif resp.event == Type.SessionCanceled:
                    tag = "SessionCanceled"

                else:
                    tag = get_event_name(resp.event)

                    if resp.text:
                        readable_log_and_print(
                            readable_logger,
                            kind="other_text_event",
                            title=f"[{vid}][{tag}]",
                            text=resp.text,
                            extra={
                                "event": resp.event,
                                "event_name": tag,
                            },
                        )

                # ====================================================
                # 8. 底层事件一定保存
                # ====================================================
                log_recv_event(
                    raw_logger=raw_logger,
                    session_id=session_id,
                    resp=resp,
                    tag=tag,
                )

                # ====================================================
                # 9. 尝试打印原文-译文片段配对
                # ====================================================
                while paired_print_idx < min(len(source_final_segments), len(translation_final_segments)):
                    src = source_final_segments[paired_print_idx]
                    tgt = translation_final_segments[paired_print_idx]

                    readable_log_and_print(
                        readable_logger,
                        kind="pair",
                        title=f"\n========== [{vid}] 片段配对 {paired_print_idx + 1:03d} ==========",
                        text="",
                        extra={
                            "pair_index": paired_print_idx + 1,
                            "src": src,
                            "tgt": tgt,
                        },
                    )

                    readable_log_and_print(
                        readable_logger,
                        kind="pair_src",
                        title="SRC:",
                        text=src,
                        extra={
                            "pair_index": paired_print_idx + 1,
                        },
                    )

                    readable_log_and_print(
                        readable_logger,
                        kind="pair_tgt",
                        title="TGT:",
                        text=tgt,
                        extra={
                            "pair_index": paired_print_idx + 1,
                        },
                    )

                    readable_log_and_print(
                        readable_logger,
                        kind="pair_end",
                        title="=" * 100,
                        text="",
                        extra={
                            "pair_index": paired_print_idx + 1,
                        },
                    )

                    paired_print_idx += 1

                # ====================================================
                # 10. 失败 / 结束处理
                # ====================================================
                if resp.event == Type.SessionFailed or resp.event == Type.SessionCanceled:
                    stop_sender.set()
                    raise RuntimeError(
                        f"SessionFailed/SessionCanceled："
                        f"event={resp.event}, "
                        f"event_name={get_event_name(resp.event)}, "
                        f"message={resp.message}, "
                        f"logid={log_id}"
                    )

                if resp.event == Type.SessionFinished:
                    stop_sender.set()
                    break

        finally:
            stop_sender.set()

            try:
                await sender_task
            except Exception as e:
                raw_logger.log({
                    "direction": "local",
                    "event_name": "SenderTaskJoinException",
                    "session_id": session_id,
                    "error": repr(e),
                })

        # ============================================================
        # 11. 最终结果拼接
        # ============================================================

        if source_end_segments:
            source_text = "".join(source_end_segments).strip()
            source_strategy = "SourceSubtitleEnd"
        else:
            source_text = "".join(source_response_parts).strip()
            source_strategy = "SourceSubtitleResponseFallback"

        if translation_end_segments:
            translation_text = "".join(translation_end_segments).strip()
            translation_strategy = "TranslationSubtitleEnd"
        else:
            translation_text = "".join(translation_response_parts).strip()
            translation_strategy = "TranslationSubtitleResponseFallback"

        raw_logger.log({
            "direction": "local",
            "event_name": "FinalTextMerged",
            "source_strategy": source_strategy,
            "translation_strategy": translation_strategy,
            "source_response_parts_count": len(source_response_parts),
            "source_end_segments_count": len(source_end_segments),
            "translation_response_parts_count": len(translation_response_parts),
            "translation_end_segments_count": len(translation_end_segments),
            "Doubao_asr": source_text,
            "Doubao_trans": translation_text,
        })

        readable_log_and_print(
            readable_logger,
            kind="final_result",
            title=f"\n========== [{vid}] 最终结果 ==========",
            text="",
            extra={
                "source_strategy": source_strategy,
                "translation_strategy": translation_strategy,
                "Doubao_asr": source_text,
                "Doubao_trans": translation_text,
            },
        )

        readable_log_and_print(
            readable_logger,
            kind="final_asr",
            title="[FINAL_ASR]",
            text=source_text,
        )

        readable_log_and_print(
            readable_logger,
            kind="final_translation",
            title="[FINAL_TRANS]",
            text=translation_text,
        )

        return {
            "vid": vid,
            "Doubao_asr": source_text,
            "Doubao_trans": translation_text,
        }

    except Exception as e:
        raw_logger.log({
            "direction": "local",
            "event_name": "AudioProcessException",
            "audio_path": audio_path,
            "error": repr(e),
        })

        readable_log_and_print(
            readable_logger,
            kind="error",
            title=f"[{vid}][ERROR]",
            text=repr(e),
            print_to_console=True,
        )

        raise

    finally:
        if conn is not None:
            try:
                await conn.close()
                raw_logger.log({
                    "direction": "local",
                    "event_name": "WebSocketClosed",
                })
            except Exception as e:
                raw_logger.log({
                    "direction": "local",
                    "event_name": "WebSocketCloseException",
                    "error": repr(e),
                })

        if temp_wav_path and os.path.exists(temp_wav_path):
            try:
                os.remove(temp_wav_path)
                raw_logger.log({
                    "direction": "local",
                    "event_name": "TempWavRemoved",
                    "temp_wav_path": temp_wav_path,
                })
            except Exception as e:
                raw_logger.log({
                    "direction": "local",
                    "event_name": "TempWavRemoveException",
                    "temp_wav_path": temp_wav_path,
                    "error": repr(e),
                })


# ============================================================
# 8. 批量处理
# ============================================================

async def main():
    check_config()

    audio_files = iter_audio_files(INPUT_AUDIO_DIR)

    if not audio_files:
        raise RuntimeError(f"输入文件夹下没有找到音频文件：{INPUT_AUDIO_DIR}")

    existing_vids = load_existing_vids(OUTPUT_JSONL_PATH) if SKIP_EXISTING else set()

    conf = Config(
        ws_url=WS_URL,
        api_key=API_KEY,
        resource_id=RESOURCE_ID,
    )

    logging.info(f"共找到音频文件：{len(audio_files)}")
    logging.info(f"最终结果 JSONL：{OUTPUT_JSONL_PATH}")
    logging.info(f"底层流式事件日志目录：{RAW_STREAM_EVENT_LOG_DIR}")
    logging.info(f"可读过程日志目录：{READABLE_TIMELINE_LOG_DIR}")
    logging.info(f"是否打印可读过程：{PRINT_READABLE_TIMELINE}")
    logging.info(f"是否打印 building 过程：{PRINT_BUILDING_PROCESS}")
    logging.info(f"是否保存底层流式事件：{SAVE_RAW_STREAM_EVENTS}")
    logging.info(f"是否保存可读时间线：{SAVE_READABLE_TIMELINE}")

    success_count = 0
    fail_count = 0
    skip_count = 0

    start_all = time.time()

    for idx, audio_path in enumerate(audio_files, start=1):
        vid = audio_path.name

        if SKIP_EXISTING and vid in existing_vids:
            skip_count += 1
            logging.info(f"[{idx}/{len(audio_files)}] 跳过已存在：{vid}")
            continue

        logging.info(f"[{idx}/{len(audio_files)}] 开始处理：{audio_path}")

        start = time.time()

        try:
            row = await doubao_translate_one_audio(
                conf=conf,
                audio_path=str(audio_path),
            )

            append_jsonl(row, OUTPUT_JSONL_PATH)
            existing_vids.add(vid)

            success_count += 1
            logging.info(
                f"[{idx}/{len(audio_files)}] 完成：{vid}，"
                f"耗时 {time.time() - start:.3f}s"
            )

        except Exception as e:
            fail_count += 1
            logging.error(f"[{idx}/{len(audio_files)}] 失败：{vid}，错误：{repr(e)}")

            # 失败时也写一行，保证最终 jsonl 和音频列表可对齐。
            # 最终文件仍然只保留用户要求的三个字段。
            row = {
                "vid": vid,
                "Doubao_asr": "",
                "Doubao_trans": "",
            }
            append_jsonl(row, OUTPUT_JSONL_PATH)
            existing_vids.add(vid)

    logging.info("========== 批量处理完成 ==========")
    logging.info(f"成功：{success_count}")
    logging.info(f"失败：{fail_count}")
    logging.info(f"跳过：{skip_count}")
    logging.info(f"总耗时：{time.time() - start_all:.3f}s")
    logging.info(f"最终结果文件：{OUTPUT_JSONL_PATH}")
    logging.info(f"底层流式事件日志目录：{RAW_STREAM_EVENT_LOG_DIR}")
    logging.info(f"可读过程日志目录：{READABLE_TIMELINE_LOG_DIR}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    asyncio.run(main())
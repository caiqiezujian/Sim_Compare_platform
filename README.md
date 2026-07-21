# SimCompare 同传对比调试台

SimCompare 是一个用于调试同传 S2TT / gRPC 服务的本地 Web 平台。它支持上传音频或视频，调用一个或两个 gRPC 服务，并在时间轴上查看每个返回包里的 ASR、翻译、日志和 chunk 音频信息。

当前版本的时间轴按 gRPC 返回包组织：时间轴位置使用 ASR 音频结束时间 `ed`。如果同一个返回包里同时包含 ASR 和 MT，前端会把它们展示在同一个 chunk 卡片里。因为当前 MT 没有独立时间戳，所以暂不单独拆成 MT 时间轴事件。

真实 gRPC 模式下，后端会边收到服务返回边更新任务缓存，前端轮询时会增量刷新时间轴，不需要等整段音频全部跑完才看到结果。点击前端“重置”会取消当前后端任务并停止继续轮询旧 run。

## 项目结构

```text
D:\Sim_Compare_platform
├─ src\                 前端 React / Vite
├─ server\              后端 FastAPI
├─ server\grpc_info\    gRPC protobuf/stub 文件
├─ server\grpc_runner.py 真实 gRPC 调用适配器
├─ scripts\grpc_probe.py 后端直连 gRPC 测试脚本
└─ README.md
```

## 首次安装

在 PowerShell 进入项目目录：

```powershell
cd D:\Sim_Compare_platform
```

安装前端依赖：

```powershell
npm install
```

安装后端依赖：

```powershell
pip install -r server\requirements.txt
```

## 启动方式

需要开两个 PowerShell 窗口。

先编辑项目根目录的 `simcompare.config.json`。这里放默认 gRPC 地址、debug 日志文件和 concat 音频根目录：

```json
{
  "services": {
    "left": {
      "label": "系统 A",
      "grpc_url": "10.185.1.71:16552",
      "debug_log": "/data/path/to/left/grpc.log",
      "debug_root": "/data/path/to/left/shells/debug"
    },
    "right": {
      "label": "系统 B",
      "grpc_url": "",
      "debug_log": "/data/path/to/right/grpc.log",
      "debug_root": "/data/path/to/right/shells/debug"
    }
  },
  "storage": {
    "upload_dir": "/data/simcompare/uploads"
  }
}
```

窗口 1：启动后端。页面会自动读取配置里的默认 gRPC 地址；你也可以在页面上临时手动改地址。

```powershell
cd D:\Sim_Compare_platform
python -m uvicorn server.main:app --host 127.0.0.1 --port 8000
```

Linux 服务器上通常这样启动：

```bash
cd /data/yb/Sim_Compare_platform
python3 -m uvicorn server.main:app --host 0.0.0.0 --port 8000
```

如果你不想使用项目根目录的配置文件，可以用 `SIMCOMPARE_CONFIG` 指向另一个配置文件：

```powershell
cd D:\Sim_Compare_platform
$env:SIMCOMPARE_CONFIG="D:\path\to\simcompare.config.json"
python -m uvicorn server.main:app --host 127.0.0.1 --port 8000
```

窗口 2：启动前端：

```powershell
cd D:\Sim_Compare_platform
npm run dev -- --host 127.0.0.1 --port 5173
```

浏览器打开：

```text
http://127.0.0.1:5173/
```

## 使用流程

1. 在页面里填写 gRPC 地址。
2. 选择 `zh2en` 或 `en2zh`。
3. 上传音频或视频。
4. 点击“开始对比”。
5. 在时间轴里点击 chunk 卡片查看日志和音频信息。

目前你可以先只测一个服务：

```text
A: 10.185.1.71:16552
B: 留空
```

两个服务都可用时：

```text
A: 服务A的 ip:port
B: 服务B的 ip:port
```

## 音频要求

最稳的输入格式：

```text
16kHz / mono / 16-bit WAV
```

上传 MP4 / MOV 时，后端会尝试用 `ffmpeg` 转成 16kHz mono WAV。如果本机没有 `ffmpeg`，建议先手动准备 WAV 文件。

后端发送音频时按 0.4 秒一个 chunk 发送。对于 16kHz / mono / 16-bit PCM，一个 0.4 秒 chunk 是：

```text
16000 samples/s * 0.4s * 2 bytes = 12800 bytes
```

gRPC deadline 会根据音频时长动态计算：

```text
max(60 秒, 音频时长 * 2.5 + 30 秒)
```

## gRPC 协议

当前服务使用双向流接口：

```text
/AsrService/createRec
```

后端通过：

```python
grpc.insecure_channel(endpoint)
```

连接服务。也就是说当前默认不带 TLS、token、metadata。如果服务需要鉴权，需要在 `server/grpc_runner.py` 里补 metadata 或安全 channel。

protobuf/stub 已放在：

```text
server\grpc_info\asr_pb2.py
server\grpc_info\asr_pb2_grpc.py
```

方向参数映射：

```text
zh2en -> lang = "zh"
en2zh -> lang = "en"
```

## 后端直连测试

如果前端不确定是否有问题，可以先用后端测试脚本直接打 gRPC 服务：

```powershell
cd D:\Sim_Compare_platform
python scripts\grpc_probe.py --endpoint 10.185.1.71:16552 --audio Dataflow_001.wav --lang en
```

没有音频时，可以先用静音包测试连通性：

```powershell
python scripts\grpc_probe.py --endpoint 10.185.1.71:16552 --silence-seconds 3 --lang en
```

脚本会打印：

```text
[check] channel ready
[send] session ...
[send] audio chunk ...
[recv #...] ...
[raw] ...
[json] ...
[asr] ...
[mt] ...
[error] ...
```

如果 `grpc_probe.py` 能收到 `[raw]`、`[asr]`、`[mt]`，说明 gRPC 服务、协议和音频链路基本是通的。此时如果 Web 页面异常，优先看 FastAPI 任务状态、前端轮询和返回数据映射。

## 返回包映射

当前重点适配这种返回结构：

```python
{
    "sn": 2,
    "type": 1,
    "bg": 5190,
    "ed": 6450,
    "ws": [{"cw": [{"w": "应该是到了。"}]}],
    "words": [{"word": "应", "start": 5190, "end": 5370}],
    "hold_n": 1,
    "ident_lang": "zh",
    "part_2_mt": "我讲。",
    "mt_type": 1,
    "mt_ws": [{"cw": [{"w": "Even solving this scientific problem..."}]}]
}
```

前端字段映射：

```text
sn/bg/ed -> chunk key 来源；sn=0、bg=0 都是有效值
bg / ed  -> ASR 时间范围；时间轴按 ed 定位
ws       -> ASR 文本
words    -> 字级时间戳
mt_ws    -> 翻译文本
hold_n   -> debug log
raw      -> 原始 JSON
```

后端同时保留两类时间：

```text
asr_end_time / asr_time      ASR 音频结束时间，来自 ed，用于时间轴
asr_returned_at             后端收到该 gRPC 包的相对时间，用于 debug
mt_returned_at              后端收到最终 MT 的相对时间，用于 debug
```

当前 MT 没有独立时间戳，所以 MT 会和同一个返回包里的 ASR 合并展示。

运行中返回的 chunk 会被增量写入：

```text
RUNS[run_id]["left"]
RUNS[run_id]["right"]
```

前端会持续请求 `/api/runs/{run_id}/chunks` 刷新时间轴。

## API

```text
GET  /api/health
GET  /api/config
POST /api/runs
GET  /api/runs/{run_id}
POST /api/runs/{run_id}/cancel
GET  /api/runs/{run_id}/chunks
GET  /api/runs/{run_id}/debug/{side}/{chunk_id}
GET  /api/runs/{run_id}/debug/{side}/{chunk_id}/audio
```

`POST /api/runs` 表单字段：

```text
video      上传的音频或视频文件
systems    前端传入的 gRPC 地址列表
direction  zh2en 或 en2zh
conference_id  会议 ID，会作为 gRPC sid 和 userinfo.conferenceId 传入
```

## 停止服务

在前端和后端两个 PowerShell 窗口里分别按：

```text
Ctrl + C
```

如果出现：

```text
Terminate batch job (Y/N)?
```

输入：

```text
Y
```

## 常见问题

如果页面能打开但没有真实请求 gRPC，先检查前端是否填写了 gRPC 服务地址：

```powershell
curl http://127.0.0.1:8000/api/config
```

如果上传 WAV 后报格式错误，优先确认是否为：

```text
16kHz / mono / 16-bit
```

如果只配置了一个服务，建议先把 B 地址留空，避免误请求旧地址。

如果页面一直显示“运行中”，先用 `scripts\grpc_probe.py` 直连同一个音频文件。如果 probe 也卡住，问题在 gRPC 服务或音频流；如果 probe 正常，检查后端窗口里 `POST /api/runs` 后的异常日志。

如果服务需要 TLS、token 或 metadata，当前代码还没有接入，需要先补后端连接参数。

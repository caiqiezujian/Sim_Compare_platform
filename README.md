# SimCompare 同传对比调试台

SimCompare 是一个用于调试同传 S2TT / gRPC 服务的本地 Web 平台。它支持上传音频或视频，调用一个或两个 gRPC 服务，并在时间轴上查看每个返回包里的 ASR、翻译、日志和 chunk 音频信息。

当前版本的时间轴按 gRPC 返回包组织：如果同一个返回包里同时包含 ASR 和 MT，前端会把它们展示在同一个 chunk 卡片里。因为当前 MT 没有独立时间戳，所以暂不单独拆成 MT 时间轴事件。

## 项目结构

```text
D:\Sim_Compare_platform
├─ src\                 前端 React / Vite
├─ server\              后端 FastAPI
├─ server\grpc_info\    gRPC protobuf/stub 文件
├─ server\grpc_runner.py 真实 gRPC 调用适配器
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

窗口 1：启动后端。如果要真实调用 gRPC：

```powershell
cd D:\Sim_Compare_platform
$env:SIMCOMPARE_REAL_GRPC="1"
python -m uvicorn server.main:app --host 127.0.0.1 --port 8000
```

如果只是看界面和 demo 数据，不真实请求 gRPC：

```powershell
cd D:\Sim_Compare_platform
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
sn       -> chunk 编号来源
bg / ed  -> ASR 时间范围
ws       -> ASR 文本
words    -> 字级时间戳
mt_ws    -> 翻译文本
hold_n   -> debug log
raw      -> 原始 JSON
```

当前 MT 没有独立时间戳，所以 MT 会和同一个返回包里的 ASR 合并展示。

## API

```text
GET  /api/health
POST /api/runs
GET  /api/runs/{run_id}
GET  /api/runs/{run_id}/chunks
```

`POST /api/runs` 表单字段：

```text
video      上传的音频或视频文件
systems    前端传入的 gRPC 地址列表
direction  zh2en 或 en2zh
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

如果页面能打开但没有真实请求 gRPC，检查后端是否用真实模式启动：

```powershell
$env:SIMCOMPARE_REAL_GRPC="1"
```

如果上传 WAV 后报格式错误，优先确认是否为：

```text
16kHz / mono / 16-bit
```

如果只配置了一个服务，建议先把 B 地址留空，避免误请求旧地址。

如果服务需要 TLS、token 或 metadata，当前代码还没有接入，需要先补后端连接参数。

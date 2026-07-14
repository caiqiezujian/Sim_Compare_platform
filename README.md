# SimCompare · 同传对比调试台

一个用于并行查看两套 S2TT / gRPC 服务结果的全栈调试平台第一版。

## 启动

前端：

```powershell
npm install
npm run dev
```

后端（另开终端）：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r server\requirements.txt
uvicorn server.main:app --reload --port 8000
```

直接打开 `http://localhost:5173` 即可。未选择视频时界面使用内置演示数据，方便先验收时间轴与调试器交互。

## 接入真实 gRPC

将生成的 `asr_pb2.py` 与 `asr_pb2_grpc.py` 放到 `server/` 下，然后在 `server/main.py` 的 `process_run` 中替换 `demo_chunks()` 为实际 runner。你提供的流式协议字段已映射到前端需要的 `start / end / asr / mt / logs / audio` 结构；`part_2_mt` 对应翻译文本，`words` 对应词级时间戳，`hold_n` 与原始响应可以继续写入 logs。

当前仓库已经内置了可选适配器。准备好 protobuf 生成文件后，在 Windows PowerShell 中运行：

```powershell
$env:SIMCOMPARE_REAL_GRPC = "1"
uvicorn server.main:app --reload --port 8000
```

真实媒体调用需要 `ffmpeg`；上传 16kHz / 16-bit / mono WAV 可不经过视频转音频步骤。

## API

- `GET /api/health`：服务健康检查
- `POST /api/runs`：上传媒体并创建对比任务，字段 `video`、`systems`
- `GET /api/runs/{run_id}`：查询任务状态与进度
- `GET /api/runs/{run_id}/chunks`：查询左右两套结果

# 华为实时语音翻译 WebSocket 接口文档

> 基于 Rokid AR/XR 翻译应用 `TranslationWsClient` 实现整理，供其他项目同步接入。
> 本文只聚焦同传翻译（ASR 识别 + MT 译文），不含 TTS 语音合成。

## 1. 接口概述

华为实时语音翻译接口基于 WebSocket 实现双向通信：客户端持续上传 PCM 音频流，服务端实时返回 ASR 识别结果 + MT 译文。

| 项目 | 说明 |
|------|------|
| **协议** | WebSocket（`wss://`） |
| **方向** | 双向：客户端上行音频/控制信令，服务端下行识别/翻译 |
| **音频输入** | PCM 16kHz / 16-bit / 单声道 |
| **支持语向** | 中→英（`zh`→`en`）、英→中（`en`→`zh`） |

## 2. 接口地址与鉴权参数

### 2.1 连接 URL

```
wss://apigw-beta.huawei.com/ws/apiAsr/plug/audioTranslate
```

### 2.2 Query 参数（拼在 URL 上）

| 参数 | 说明 | 示例值 |
|------|------|--------|
| `X-HW-ID` | 华为 API 网关鉴权 ID | `com.huawei.mt` |
| `X-HW-APPKEY` | 华为 API 网关密钥（URL 编码） | `zmyLcM2kfpUlN%2B99o0HncQ%3D%3D` |
| `appid` | 应用标识 | `aiglasses.huawei.com` |
| `token` | 会议/应用 token | ***** |
| `langFrom` | 源语言代码 | `zh` |
| `langTo` | 目标语言代码 | `en` |

### 2.3 完整连接 URL 示例

```
wss://apigw-beta.huawei.com/ws/apiAsr/plug/audioTranslate
  ?X-HW-ID=com.huawei.mt
  &X-HW-APPKEY=zmyLcM2kfpUlN%2B99o0HncQ%3D%3D
  &appid=aiglasses.huawei.com
  &token=****
  &langFrom=zh
  &langTo=en
```

> ⚠️ `X-HW-APPKEY` 的值是经过 URL 编码的（`%2B` = `+`，`%3D` = `=`）。直接拼接即可，**不要再次编码**。

## 3. 通信协议

### 3.1 连接建立

客户端发起 WebSocket 连接后，服务端返回连接确认消息，其中包含 `conferenceId`（会议 ID）：

```json
{
  "msg": "connect",
  "conferenceId": "7795803227"
}
```

**`conferenceId` 必须保存**——后续所有音频上行包都需要携带它。

### 3.2 客户端 → 服务端：音频数据

PCM 音频按 **80ms 一包（2560 字节）** 累积后发送，Base64 编码：

```json
{
  "audioData": "<base64 编码的 PCM 字节>",
  "conferenceId": "7795803227",
  "seq": 1
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `audioData` | string | Base64 编码的 PCM 音频字节（16kHz/16bit/mono） |
| `conferenceId` | string | 连接时服务端返回的会议 ID |
| `seq` | int | 音频包序号，从 1 递增，用于服务端排序 |

> **音频包大小**：80ms = 16000 Hz × 2 字节 × 0.08s = **2560 字节**。建议保持此节奏，过大（如 >12KB）会触发 WebSocket 1009 关闭码。

### 3.3 客户端 → 服务端：心跳

每 30 秒发送一次心跳，保持连接：

```json
{ "beat": true }
```

服务端会回 `{ "beat": true }` 作为应答。

### 3.4 客户端 → 服务端：切换语向

运行中动态切换翻译方向（无需重新建连）：

```json
{
  "change_lang": true,
  "from": "en",
  "to": "zh"
}
```

### 3.5 服务端 → 客户端：识别 + 翻译结果

服务端返回 `msgType: "text"` 的消息，包含 ASR 识别与 MT 译文：

```json
{
  "msgType": "text",
  "sn": 10,
  "sentenceType": 2,
  "text": "我的确看到了",
  "translate": " I do see",
  "fluency": "我的确看到了",
  "progressive": "",
  "part2Mt": "一些比较。",
  "translateContext": " I do see",
  "from": "zh",
  "to": "en",
  "conferenceId": "7795803227",
  "isFluency": true,
  "end": false
}
```

**关键字段说明：**

| 字段 | 说明 |
|------|------|
| `sn` | 句子序号（sentence number），用于同一句的去重/合并 |
| `sentenceType` | **0** = 过程态（partial，识别中）；**1** = 终态（final，句子确认）；**2** = 顺滑态（smooth，优化后的最终结果） |
| `text` | ASR 识别的源语言原文 |
| `translate` | MT 译文。⚠️ **注意时序**：终态(type=1)时译文常为空，译文在顺滑态(type=2)才到达 |
| `fluency` | 顺滑后的原文（纠正了识别口误等） |
| `progressive` | 渐进式译文（部分场景使用） |
| `part2Mt` | 下一句的预翻译提示 |
| `translateContext` | 译文的上下文修正版本 |
| `from` / `to` | 实际翻译方向 |
| `isFluency` | 是否为顺滑态结果 |

> **⚠️ 译文时序陷阱**：同一 `sn` 会先收到 `sentenceType=1`（终态，原文确认但 `translate` 可能为空），稍后再收到 `sentenceType=2`（顺滑态，`translate` 才有值）。**实现时必须按 `sn` 合并多次结果**，不能假设终态一定带译文。

## 4. 调用流程

```
┌──────────┐                                    ┌──────────┐
│  Client  │                                    │  Server  │
└────┬─────┘                                    └────┬─────┘
     │  1. WebSocket connect (带鉴权参数+langFrom/langTo)
     │ ───────────────────────────────────────────► │
     │                                               │
     │  2. {msg:"connect", conferenceId:"xxx"}       │
     │ ◄───────────────────────────────────────────  │
     │                                               │
     │  3. {audioData:"...", conferenceId, seq:1}   │
     │ ───────────────────────────────────────────► │
     │  4. {msgType:"text", sn:1, sentenceType:0...}│ (过程态识别)
     │ ◄───────────────────────────────────────────  │
     │  5. {audioData:"...", conferenceId, seq:2}   │
     │ ───────────────────────────────────────────► │
     │  6. {msgType:"text", sn:1, sentenceType:1...}│ (终态, 译文可能为空)
     │ ◄───────────────────────────────────────────  │
     │  7. {msgType:"text", sn:1, sentenceType:2...}│ (顺滑态, 译文到达)
     │ ◄───────────────────────────────────────────  │
     │                                               │
     │  8. {beat:true} (每30s心跳)                  │
     │ ──────────────────────────────────────────►  │
     │ ◄──────────────────────────────────────────   │
```

## 5. Kotlin 调用代码示例

### 5.1 依赖

```kotlin
// build.gradle.kts
implementation("com.squareup.okhttp3:okhttp:4.12.0")
implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
```

### 5.2 数据模型

```kotlin
/** 一条翻译结果。 */
data class TranslationResult(
    val sn: Int,              // 句子序号
    val sentenceType: Int,    // 0=过程 1=终态 2=顺滑态
    val originalText: String, // 源语言原文
    val translatedText: String, // 译文（顺滑态才有值）
    val from: String,
    val to: String
)
```

### 5.3 WebSocket 客户端

```kotlin
import android.util.Base64
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import okhttp3.*
import org.json.JSONObject
import java.util.concurrent.TimeUnit

class TranslationWsClient {

    companion object {
        private const val HEARTBEAT_INTERVAL_MS = 30_000L
        // PCM 16kHz 16bit mono: 16000 * 2 = 32000 bytes/sec → 80ms = 2560 bytes
        private const val AUDIO_BUFFER_SIZE = 2560

        private const val BASE_URL = "wss://apigw-beta.huawei.com/ws/apiAsr/plug/audioTranslate"
        private const val HW_ID = "com.huawei.mt"
        private const val HW_APPKEY = "zmyLcM2kfpUlN%2B99o0HncQ%3D%3D"
        private const val APP_ID = "aiglasses.huawei.com"
        private const val TOKEN = "*******"
    }

    private val client = OkHttpClient.Builder()
        .readTimeout(0, TimeUnit.MILLISECONDS)   // 长连接不超时
        .pingInterval(30, TimeUnit.SECONDS)      // OkHttp 层 ping
        .build()

    private var webSocket: WebSocket? = null
    private var conferenceId: String? = null
    private var seq = 0
    private var langFrom = "zh"
    private var langTo = "en"

    // 80ms 音频累积缓冲
    private val audioBuffer = ByteArray(AUDIO_BUFFER_SIZE)
    private var audioBufferPos = 0

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var heartbeatJob: Job? = null

    // 对外暴露的结果流（UI collect 即可）
    private val _results = MutableSharedFlow<TranslationResult>(extraBufferCapacity = 64)
    val results: SharedFlow<TranslationResult> = _results.asSharedFlow()

    private val _connectionState = MutableSharedFlow<ConnectionState>(extraBufferCapacity = 16)
    val connectionState: SharedFlow<ConnectionState> = _connectionState.asSharedFlow()

    enum class ConnectionState { DISCONNECTED, CONNECTING, CONNECTED }

    /** 建立连接。 */
    fun connect(langFrom: String = "zh", langTo: String = "en") {
        this.langFrom = langFrom
        this.langTo = langTo
        this.seq = 0
        this.conferenceId = null
        this.audioBufferPos = 0
        _connectionState.tryEmit(ConnectionState.CONNECTING)

        val url = "$BASE_URL?X-HW-ID=$HW_ID&X-HW-APPKEY=$HW_APPKEY" +
            "&appid=$APP_ID&token=$TOKEN&langFrom=$langFrom&langTo=$langTo"
        val request = Request.Builder().url(url).build()
        webSocket = client.newWebSocket(request, wsListener)
    }

    /** 断开连接。 */
    fun disconnect() {
        heartbeatJob?.cancel()
        webSocket?.close(1000, "client disconnect")
        webSocket = null
        conferenceId = null
        _connectionState.tryEmit(ConnectionState.DISCONNECTED)
    }

    /** 切换语向（无需重连）。 */
    fun switchLanguage(from: String, to: String) {
        langFrom = from; langTo = to
        val json = JSONObject().apply {
            put("change_lang", true); put("from", from); put("to", to)
        }
        webSocket?.send(json.toString())
    }

    /**
     * 喂入 PCM 音频字节（来自麦克风回调）。
     * 内部按 80ms(2560字节) 累积，满包后 Base64 发送。
     */
    fun feedAudio(data: ByteArray, offset: Int, length: Int) {
        if (conferenceId == null) return   // 连接确认前丢弃
        var remaining = length
        var srcPos = offset
        while (remaining > 0) {
            val chunk = minOf(remaining, AUDIO_BUFFER_SIZE - audioBufferPos)
            System.arraycopy(data, srcPos, audioBuffer, audioBufferPos, chunk)
            audioBufferPos += chunk
            srcPos += chunk
            remaining -= chunk
            if (audioBufferPos >= AUDIO_BUFFER_SIZE) {
                sendAudioPacket(audioBuffer, audioBufferPos)
                audioBufferPos = 0
            }
        }
    }

    private fun sendAudioPacket(data: ByteArray, length: Int) {
        val confId = conferenceId ?: return
        seq++
        val b64 = Base64.encodeToString(data, 0, length, Base64.NO_WRAP)
        val json = JSONObject().apply {
            put("audioData", b64)
            put("conferenceId", confId)
            put("seq", seq)
        }
        webSocket?.send(json.toString())
    }

    private val wsListener = object : WebSocketListener() {
        override fun onMessage(text: String) {
            val json = JSONObject(text)
            when {
                json.optString("msg") == "connect" -> {
                    conferenceId = json.getString("conferenceId")
                    _connectionState.tryEmit(ConnectionState.CONNECTED)
                    startHeartbeat()
                }
                json.optString("msgType") == "text" -> {
                    _results.tryEmit(TranslationResult(
                        sn = json.getInt("sn"),
                        sentenceType = json.getInt("sentenceType"),
                        originalText = json.optString("text", ""),
                        translatedText = json.optString("translate", ""),
                        from = json.optString("from", ""),
                        to = json.optString("to", "")
                    ))
                }
                json.optBoolean("beat", false) -> { /* 心跳应答 */ }
            }
        }

        override fun onFailure(t: Throwable, response: Response?) {
            _connectionState.tryEmit(ConnectionState.DISCONNECTED)
            // 此处可加重连逻辑
        }
    }

    private fun startHeartbeat() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            while (isActive) {
                delay(HEARTBEAT_INTERVAL_MS)
                webSocket?.send(JSONObject().put("beat", true).toString())
            }
        }
    }
}
```

### 5.4 UI 层使用示例

```kotlin
val ws = TranslationWsClient()

// 收集识别结果
scope.launch {
    ws.results.collect { result ->
        // 同一 sn 会收到多次：先 type=1（终态，原文确认），再 type=2（顺滑态，译文到达）
        // 需按 sn 合并：用 type=2 的 translate 填充 type=1 时为空的译文
        println("sn=${result.sn} type=${result.sentenceType} " +
            "src=${result.originalText} dst=${result.translatedText}")
    }
}

// 建立连接
ws.connect(langFrom = "zh", langTo = "en")

// 喂麦克风音频（16kHz/16bit/mono PCM）
// 如来自 AudioRecord 回调：
ws.feedAudio(pcmBytes, 0, pcmBytes.size)

// 结束
ws.disconnect()
```

## 6. 注意事项

| # | 注意点 |
|---|--------|
| 1 | **音频格式必须是 PCM 16kHz/16-bit/单声道**，其他采样率会导致识别异常或 WebSocket 1009 关闭 |
| 2 | **音频包按 80ms（2560 字节）累积发送**，单包过大（>12KB）会触发 1009 |
| 3 | **译文时序**：终态(type=1)常不带译文，译文在顺滑态(type=2)到达，需按 `sn` 合并多次结果 |
| 4 | **`conferenceId` 必须保存**，所有音频上行包都需携带；连接确认前喂入的音频会被丢弃 |
| 5 | **心跳 30s 一次**，否则连接可能被服务端断开 |
| 6 | `X-HW-APPKEY` 已 URL 编码，直接拼接 URL，不要二次编码 |

## 7. 语言代码

| 代码 | 语言 |
|------|------|
| `zh` | 中文 |
| `en` | 英文 |

> 当前仅支持 `zh`↔`en` 双向。其他语向需确认服务端是否开放。

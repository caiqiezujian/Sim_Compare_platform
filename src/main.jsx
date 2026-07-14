import { StrictMode, useEffect, useMemo, useRef, useState } from 'react'
import { createRoot } from 'react-dom/client'
import {
  AudioLines, Check, ChevronDown, CircleHelp, Clock3, Copy, FileAudio,
  FileVideo, FolderOpen, GitCompareArrows, Headphones, History, Info, Languages,
  Link2, ListFilter, LoaderCircle, Logs, Menu, Minus, Pause, Play, Plus,
  RotateCcw, Search, Settings2, SlidersHorizontal, Sparkles, TerminalSquare,
  UploadCloud, Volume2, X, Zap
} from 'lucide-react'
import './styles.css'

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

const initialSystems = [
  { id: 'system-a', label: '线上稳定版', name: 'S2TT · Stable', url: '10.185.1.62:7860', language: 'zh → en', color: 'cyan', enabled: true },
  { id: 'system-b', label: '候选实验版', name: 'S2TT · Canary', url: '10.185.1.63:7860', language: 'zh → en', color: 'violet', enabled: true },
]

const demoItems = [
  { id: 'chunk-01', start: 0, end: 3280, status: 'done', asr: '各位观众大家好，欢迎来到今天的节目。', mt: 'Hello everyone, welcome to today’s program.', audio: 'chunk-01.wav', logs: ['request opened · sid=Beijing-TSC-test-7812', 'audio stream 0.0s — 3.28s', 'ASR final · hold_n=1', 'MT final · latency 842ms'] },
  { id: 'chunk-02', start: 3280, end: 5190, status: 'done', asr: '今天我们来聊一个有趣的话题。', mt: 'Today we are going to talk about an interesting topic.', audio: 'chunk-02.wav', logs: ['audio stream 3.28s — 5.19s', 'ASR final · hold_n=0', 'MT final · latency 906ms'] },
  { id: 'chunk-03', start: 5190, end: 6450, status: 'done', asr: '应该是到了。', mt: 'I think we’ve reached it.', audio: 'chunk-03.wav', logs: ['audio stream 5.19s — 6.45s', 'ASR final · hold_n=1', 'MT final · latency 1,024ms', 'debug snapshot saved'] },
  { id: 'chunk-04', start: 6450, end: 9360, status: 'done', asr: '从这里开始，两个系统的结果出现了差异。', mt: 'From here, the two systems begin to diverge.', audio: 'chunk-04.wav', logs: ['audio stream 6.45s — 9.36s', 'ASR final · hold_n=1', 'MT final · latency 1,188ms'] },
  { id: 'chunk-05', start: 9360, end: 12120, status: 'done', asr: '我们可以逐句查看它们的表现。', mt: 'We can review their performance sentence by sentence.', audio: 'chunk-05.wav', logs: ['audio stream 9.36s — 12.12s', 'ASR final · hold_n=0', 'MT final · latency 997ms'] },
  { id: 'chunk-06', start: 12120, end: 14600, status: 'pending', asr: '这条结果正在等待服务返回。', mt: 'Waiting for the service to return this result.', audio: 'chunk-06.wav', logs: ['audio stream 12.12s — 14.60s', 'stream still open'] },
]

function formatTime(ms) {
  const seconds = Math.floor(ms / 1000)
  const minutes = Math.floor(seconds / 60)
  return `${String(minutes).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}.${String(ms % 1000).padStart(3, '0')}`
}

function eventStamp(item, side, index) {
  const explicit = item.asrTime ?? item.asr_time ?? item.asrReturnedAt ?? item.asr_returned_at
  if (Number.isFinite(Number(explicit))) return Number(explicit)
  const sideOffset = side === 'right' ? 260 : 0
  return (item.start || 0) + sideOffset + (index % 3) * 70
}

function buildTimelineEvents(leftItems, rightItems, query) {
  const q = query.trim().toLowerCase()
  const rows = []
  const append = (side, items) => {
    items.forEach((item, index) => {
      if (q && !`${item.asr} ${item.mt}`.toLowerCase().includes(q)) return
      rows.push({
        id: `${side}-${item.id}`,
        side,
        kind: 'bundle',
        label: 'ASR + MT',
        item,
        chunkIndex: index + 1,
        stamp: eventStamp(item, side, index),
      })
    })
  }
  append('left', leftItems)
  append('right', rightItems)
  return rows.sort((a, b) => a.stamp - b.stamp || a.chunkIndex - b.chunkIndex || a.side.localeCompare(b.side))
}

function chunkDelay(stamp, item) {
  return Math.max(0, stamp - (item.start || 0))
}

function App() {
  const [systems, setSystems] = useState(initialSystems)
  const [selectedSystem, setSelectedSystem] = useState('system-a')
  const [direction, setDirection] = useState('zh2en')
  const [video, setVideo] = useState(null)
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(68)
  const [activeTab, setActiveTab] = useState('compare')
  const [selectedChunk, setSelectedChunk] = useState('chunk-03')
  const [selectedSide, setSelectedSide] = useState('left')
  const [query, setQuery] = useState('')
  const [showSystemModal, setShowSystemModal] = useState(false)
  const [toast, setToast] = useState('')
  const [serverState, setServerState] = useState('demo')
  const [serverRunId, setServerRunId] = useState(null)
  const [leftItems, setLeftItems] = useState(demoItems)
  const [rightItems, setRightItems] = useState(demoItems.map((item, i) => ({ ...item, asr: i === 3 ? '从这里开始，两个系统的输出有一点不同。' : item.asr, mt: i === 3 ? 'From this point, the outputs from the systems are slightly different.' : item.mt })))
  const fileInputRef = useRef(null)

  const timelineEvents = useMemo(() => buildTimelineEvents(leftItems, rightItems, query), [leftItems, rightItems, query])

  useEffect(() => {
    if (!running || serverRunId) return undefined
    const timer = setInterval(() => setProgress(value => value >= 100 ? (setRunning(false), 100) : value + 4), 480)
    return () => clearInterval(timer)
  }, [running, serverRunId])

  useEffect(() => {
    if (!serverRunId) return undefined
    let disposed = false
    const poll = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/runs/${serverRunId}`)
        if (!response.ok) throw new Error('status')
        const run = await response.json()
        if (disposed) return
        setProgress(run.progress || 0)
        if (run.status === 'completed') {
          const chunksResponse = await fetch(`${API_BASE}/api/runs/${serverRunId}/chunks`)
          const chunks = await chunksResponse.json()
          if (chunks.left?.length) setLeftItems(chunks.left)
          if (chunks.right?.length) setRightItems(chunks.right)
          setRunning(false)
          setServerRunId(null)
          notify('gRPC 对比任务完成')
        } else if (run.status === 'failed') {
          setRunning(false)
          setServerRunId(null)
          notify('任务执行失败，已保留当前结果')
        }
      } catch {
        if (!disposed) {
          setRunning(false)
          setServerRunId(null)
          setServerState('offline')
          notify('任务状态读取失败，已保留当前结果')
        }
      }
    }
    poll()
    const timer = window.setInterval(poll, 520)
    return () => { disposed = true; window.clearInterval(timer) }
  }, [serverRunId])

  const selectedLeft = leftItems.find(item => item.id === selectedChunk) || leftItems[0]
  const selectedRight = rightItems.find(item => item.id === selectedChunk) || rightItems[0]
  const selected = selectedSide === 'left' ? selectedLeft : selectedRight

  const notify = (message) => {
    setToast(message)
    window.setTimeout(() => setToast(''), 2400)
  }

  const addSystem = (event) => {
    event.preventDefault()
    const form = new FormData(event.currentTarget)
    const url = String(form.get('url') || '').trim()
    const label = String(form.get('label') || '').trim() || '自定义服务'
    if (!url) return
    const next = { id: `system-${Date.now()}`, label, name: 'Custom endpoint', url, language: 'zh → en', color: 'amber', enabled: true }
    setSystems(items => [...items, next])
    setSelectedSystem(next.id)
    setShowSystemModal(false)
    notify('服务地址已加入对比')
  }

  const updateSystemUrl = (id, url) => {
    setSystems(items => items.map(system => system.id === id ? { ...system, url } : system))
  }

  const startRun = async () => {
    setServerRunId(null)
    setRunning(true)
    setProgress(8)
    if (!video) {
      notify('当前使用演示视频，可直接查看交互')
      return
    }
    try {
      const enabled = systems.slice(0, 2).filter(system => system.enabled && system.url.trim())
      const form = new FormData()
      form.append('video', video)
      form.append('systems', JSON.stringify(enabled))
      form.append('direction', direction)
      const response = await fetch(`${API_BASE}/api/runs`, { method: 'POST', body: form })
      if (!response.ok) throw new Error('server')
      const data = await response.json()
      setServerState('connected')
      setServerRunId(data.run_id)
      notify(`任务 ${data.run_id} 已启动`)
    } catch {
      setServerState('offline')
      notify('后端未连接，已切换为本地演示模式')
    }
  }

  const handleFile = event => {
    const file = event.target.files?.[0]
    if (file) { setVideo(file); setProgress(0); notify(`已选择 ${file.name}`) }
  }

  const copyValue = value => { navigator.clipboard?.writeText(value); notify('已复制服务地址') }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand"><div className="brand-mark"><GitCompareArrows size={19} /></div><div><div className="brand-name">SIM<span>COMPARE</span></div><div className="brand-sub">同传结果调试台</div></div></div>
        <div className="topbar-center"><div className="live-dot" /> <span>LOCAL WORKSPACE</span><i /> <span className={serverState === 'connected' ? 'server-online' : ''}>{serverState === 'demo' ? '演示数据已加载' : serverState === 'connected' ? 'API CONNECTED' : 'API OFFLINE'}</span></div>
        <div className="topbar-actions"><button className="icon-button" title="帮助"><CircleHelp size={18} /></button><button className="icon-button" title="设置"><Settings2 size={18} /></button><div className="avatar">LY</div></div>
      </header>

      <aside className="sidebar">
        <div className="side-label">WORKSPACE</div>
        <button className={`nav-item ${activeTab === 'compare' ? 'active' : ''}`} onClick={() => setActiveTab('compare')}><GitCompareArrows size={17} /> 对比调试</button>
        <button className={`nav-item ${activeTab === 'history' ? 'active' : ''}`} onClick={() => setActiveTab('history')}><History size={17} /> 任务历史 <span className="nav-count">12</span></button>
        <div className="side-label service-label">SERVICES</div>
        {systems.map((system, index) => <button key={system.id} className={`service-item ${selectedSystem === system.id ? 'selected' : ''}`} onClick={() => setSelectedSystem(system.id)}><span className={`service-dot ${system.color}`} /><span className="service-copy"><strong>{system.label}</strong><small>{system.url}</small></span><span className={`status-pip ${index === 1 ? 'warning' : ''}`} /></button>)}
        <button className="add-service" onClick={() => setShowSystemModal(true)}><Plus size={15} /> 添加服务地址</button>
        <div className="sidebar-foot"><span>v0.1.0</span><span>API <span className="api-dot" /></span></div>
      </aside>

      <main className="main-content">
        <section className="page-heading"><div><div className="eyebrow"><span>DEBUG SESSION</span><span className="slash">/</span><span>NEW COMPARISON</span></div><h1>同传对比 <em>debug</em></h1><p>并行观察转录与翻译结果，快速定位差异产生的时间点。</p></div><div className="heading-actions"><button className="ghost-button" onClick={() => { setVideo(null); setProgress(0); setLeftItems(demoItems); setRightItems(demoItems); notify('已重置当前任务') }}><RotateCcw size={15} /> 重置</button><button className="primary-button" onClick={startRun} disabled={running}><span className="button-glow" />{running ? <LoaderCircle className="spin" size={16} /> : <Play size={15} fill="currentColor" />} {running ? '运行中…' : '开始对比'}</button></div></section>

        <section className="control-grid">
          <div className="control-card source-card"><div className="card-top"><div className="card-title"><span className="step-number">01</span><div><h3>选择媒体文件</h3><p>上传视频，后端会按音频 chunk 发送至服务。</p></div></div><FileVideo size={20} className="muted-icon" /></div><input ref={fileInputRef} type="file" accept="video/*,audio/*" hidden onChange={handleFile} /><button className={`dropzone ${video ? 'has-file' : ''}`} onClick={() => fileInputRef.current?.click()}><div className="upload-icon">{video ? <FileAudio size={22} /> : <UploadCloud size={22} />}</div><div><strong>{video ? video.name : '拖拽文件到这里，或点击选择'}</strong><small>{video ? `${(video.size / 1024 / 1024).toFixed(1)} MB · 已就绪` : '支持 MP4 / MOV / WAV · 最大 2 GB'}</small></div><ChevronDown size={16} className="drop-chevron" /></button></div>
          <div className="control-card service-card"><div className="card-top"><div className="card-title"><span className="step-number">02</span><div><h3>配置对比服务</h3><p>直接输入两个 gRPC 地址，同时发起流式调用。</p></div></div><Link2 size={20} className="muted-icon" /></div><div className="service-selects">{systems.slice(0, 2).map((system, index) => <div className="endpoint-row" key={system.id}><span className={`endpoint-tag ${system.color}`}>{index === 0 ? 'A' : 'B'}</span><label className="endpoint-input"><span>{system.label}</span><input value={system.url} onChange={event => updateSystemUrl(system.id, event.target.value)} onFocus={() => setSelectedSystem(system.id)} placeholder="127.0.0.1:7860" spellCheck="false" /></label><button className="copy-button" title="复制地址" onClick={() => copyValue(system.url)}><Copy size={14} /></button></div>)}</div><div className="service-actions"><button type="button" className={`direction-switch ${direction === 'en2zh' ? 'is-right' : ''}`} aria-label="切换翻译方向" onClick={() => setDirection(value => value === 'zh2en' ? 'en2zh' : 'zh2en')}><span className="direction-thumb" /><span className="direction-option">zh2en</span><span className="direction-option">en2zh</span></button><button className="inline-add" onClick={() => setShowSystemModal(true)}><Plus size={14} /> 添加备用地址</button></div></div>
        </section>

        <section className="run-bar"><div className="run-info"><div className="run-status"><span className={running ? 'pulse-dot' : 'complete-dot'} /> {running ? 'STREAMING' : 'READY'}</div><div className="run-divider" /><span className="file-name"><FileVideo size={14} /> {video?.name || 'demo_interview_zh.mp4'}</span><span className="run-meta">· {direction} · 00:14.600 · 16 kHz mono</span></div><div className="progress-wrap"><span>{Math.min(progress, 100)}%</span><div className="progress-track"><div className="progress-value" style={{ width: `${progress}%` }} /></div><span className="progress-label">{running ? 'processing' : '6 / 6 chunks'}</span></div></section>

        <section className="comparison-panel"><div className="panel-heading"><div><div className="section-kicker"><span className="kicker-line" /> TIMELINE OUTPUT</div><h2>结果时间轴</h2></div><div className="panel-tools"><div className="search-box"><Search size={15} /><input value={query} onChange={event => setQuery(event.target.value)} placeholder="搜索转录或翻译…" /></div><button className="small-tool"><ListFilter size={15} /> 筛选</button><button className="small-tool icon-only"><SlidersHorizontal size={15} /></button></div></div><div className="timeline-header"><div className="group left"><span className="system-badge cyan">A</span><span className="label">{systems[0]?.label || '系统 A'}</span><span className="url">{systems[0]?.url || '未配置'}</span><span className="lat">ASR time</span></div><div className="spacer" /><div className="axis-label">ASR TIME / BUNDLE</div><div className="spacer" /><div className="group right"><span className="lat">ASR time</span><span className="url">{systems[1]?.url || '未配置'}</span><span className="label">{systems[1]?.label || '系统 B'}</span><span className="system-badge violet">B</span></div></div><div className="timeline-list">{timelineEvents.map((event, index) => <TimelineEventRow key={event.id} event={event} isLast={index === timelineEvents.length - 1} selectedChunk={selectedChunk} selectedSide={selectedSide} onSelect={(chunkId, side) => { setSelectedChunk(chunkId); setSelectedSide(side) }} />)}</div>{timelineEvents.length === 0 && <div className="empty-search">没有找到匹配结果</div>}<div className="timeline-footer"><span><span className="footer-dot cyan" /> A · {leftItems.length} chunks</span><span><span className="footer-dot violet" /> B · {rightItems.length} chunks</span><span className="footer-note"><Info size={13} /> 当前按 ASR 时间定位；翻译没有独立时间戳时合并展示在同一个 chunk 卡片内</span></div></section>

        <section className="inspector-panel"><div className="inspector-heading"><div className="inspector-title"><div className="inspect-icon"><Logs size={17} /></div><div><div className="section-kicker"><span className="kicker-line" /> INSPECTOR</div><h2>Chunk 调试详情 <span>#{selectedChunk.replace('chunk-', '')}</span></h2></div></div><div className="inspect-actions"><span className="time-chip"><Clock3 size={13} /> {formatTime(selected?.start || 0)} — {formatTime(selected?.end || 0)}</span><button className="small-tool"><TerminalSquare size={14} /> 原始 JSON</button></div></div><div className="inspector-grid"><div className="debug-log"><div className="subhead"><span>DEBUG LOG</span><span className="log-live"><span className="mini-live" /> STREAM LOG</span></div><div className="log-window">{(selected?.logs || []).map((log, index) => <div className="log-line" key={log}><span className="log-time">{formatTime((selected?.start || 0) + index * 184)}</span><span className={`log-level ${index === 2 ? 'accent' : ''}`}>{index === 2 ? 'RESULT' : 'INFO'}</span><span>{log}</span></div>)}<div className="log-cursor">_</div></div></div><div className="audio-debug"><div className="subhead"><span>CHUNK AUDIO</span><span className="audio-format">WAV · 16 kHz</span></div><div className="audio-file"><div className="audio-symbol"><AudioLines size={18} /></div><div><strong>{selected?.audio || 'chunk-03.wav'}</strong><small>{((selected?.end - selected?.start || 1260) / 1000).toFixed(2)}s · mono · 40.3 KB</small></div><button className="play-circle" onClick={() => notify('音频预览已加入播放队列')}><Play size={15} fill="currentColor" /></button></div><div className="waveform">{Array.from({ length: 52 }, (_, i) => <span key={i} style={{ height: `${18 + ((i * 17 + (selected?.start || 0) / 10) % 30)}%` }} />)}</div><div className="audio-controls"><button className="audio-play" onClick={() => notify('音频预览已加入播放队列')}><Play size={13} fill="currentColor" /> 试听 chunk</button><span>{formatTime(selected?.start || 0)}</span><span>{formatTime(selected?.end || 0)}</span><Volume2 size={14} /></div></div></div></section>
      </main>
      {toast && <div className="toast"><Check size={15} /> {toast}</div>}
      {showSystemModal && <div className="modal-backdrop" onMouseDown={() => setShowSystemModal(false)}><div className="modal" onMouseDown={event => event.stopPropagation()}><div className="modal-head"><div><div className="section-kicker"><span className="kicker-line" /> NEW ENDPOINT</div><h2>添加 gRPC 服务</h2></div><button className="close-button" onClick={() => setShowSystemModal(false)}><X size={17} /></button></div><form onSubmit={addSystem}><label>服务名称<input name="label" placeholder="例如：我的本地测试版" autoFocus /></label><label>gRPC 地址<input name="url" placeholder="127.0.0.1:7860" required /></label><div className="modal-hint"><Info size={14} /> 支持 host:port 格式。后端会将此地址传入流式调用。</div><div className="modal-actions"><button type="button" className="ghost-button" onClick={() => setShowSystemModal(false)}>取消</button><button className="primary-button" type="submit"><Plus size={15} /> 添加地址</button></div></form></div></div>}
    </div>
  )
}

function TimelineEventRow({ event, isLast, selectedChunk, selectedSide, onSelect }) {
  if (!event) return null
  const { side, item, chunkIndex, stamp } = event
  const isSelected = selectedChunk === item.id && selectedSide === side
  const asrReady = Boolean(item.asr)
  const mtReady = Boolean(item.mt)
  const delay = Math.max(0, stamp - (item.start || 0))
  const card = (
    <div className={`result-cell ${side}-result bundle-result ${isSelected ? 'focus' : ''}`} onClick={() => onSelect(item.id, side)}>
      <div className="result-head">
        <div className="result-tags">
          <span className="result-tag asr">ASR</span>
          <span className="result-tag mt">MT</span>
          <span className="result-time">+{(delay / 1000).toFixed(2)}s</span>
        </div>
        <span className={`result-state ${asrReady ? 'final' : 'pending'}`}>
          {asrReady ? <><Check size={12} /> final</> : <><LoaderCircle size={12} className="spin" /> 等待</>}
        </span>
      </div>
      <div className="bundle-lines">
        <div className="bundle-line asr-line">
          <span className="bundle-label"><AudioLines size={12} /> ASR</span>
          {asrReady ? <p className="result-text asr-text">{item.asr}</p> : <div className="result-placeholder"><LoaderCircle size={14} className="spin" /><span>ASR 尚未返回</span></div>}
        </div>
        <div className="bundle-line mt-line">
          <span className="bundle-label"><Languages size={12} /> MT</span>
          {mtReady ? <p className="result-text mt-text">{item.mt}</p> : <div className="result-placeholder"><span>翻译随本次 ASR 包展示，暂无独立时间戳</span></div>}
        </div>
      </div>
      <div className="event-meta"><span>CHUNK {String(chunkIndex).padStart(2, '0')}</span><span>{formatTime(item.start || 0)} — {formatTime(item.end || 0)}</span></div>
    </div>
  )
  return (
    <div className={`timeline-row event-row ${side}-only bundle-event ${isSelected ? 'selected' : ''}`}>
      <div className="timeline-side-slot">{side === 'left' ? card : null}</div>
      <div className="center-spine">
        <span className="event-time">{formatTime(stamp)}</span>
        <span className={`spine-dot bundle ${side} ${!asrReady ? 'pending' : ''}`} />
        {!isLast && <i />}
      </div>
      <div className="timeline-side-slot">{side === 'right' ? card : null}</div>
    </div>
  )
}

createRoot(document.getElementById('root')).render(<StrictMode><App /></StrictMode>)

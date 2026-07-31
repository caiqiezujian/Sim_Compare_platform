import { StrictMode, useEffect, useMemo, useRef, useState } from 'react'
import { createRoot } from 'react-dom/client'
import {
  AudioLines, Check, ChevronDown, CircleHelp, Clock3, Copy, FileAudio,
  FolderOpen, Gauge, GitCompareArrows, Headphones, History, Info, Languages,
  Link2, ListFilter, LoaderCircle, Logs, Maximize2, Menu, Minus, Pause, Pencil, Play, Plus,
  RotateCcw, Search, Settings2, SlidersHorizontal, Sparkles, TerminalSquare,
  UploadCloud, Volume2, VolumeX, X, Zap
} from 'lucide-react'
import './styles.css'

const API_BASE = import.meta.env.VITE_API_BASE || (window.location.port === '5173' ? `${window.location.protocol}//${window.location.hostname}:8000` : window.location.origin)
const TIMELINE_TOP_PADDING = 64
const TIMELINE_BOTTOM_PADDING = 180
const TIMELINE_PX_PER_SECOND = 84
const TIMELINE_CARD_ANCHOR_Y = 58
const TIMELINE_ROW_GAP = 14
const TIMELINE_ROW_BASE_HEIGHT = 132

const initialSystems = [
  { id: 'system-a', label: '线上稳定版', name: 'S2TT · Stable', url: '10.185.1.71:16552', type: 'grpc', language: 'zh → en', color: 'cyan', enabled: true },
  { id: 'system-b', label: '候选实验版', name: 'S2TT · Canary', url: '', type: 'grpc', language: 'zh → en', color: 'violet', enabled: true },
]

const maskKey = (key) => {
  if (!key) return ''
  if (key.length <= 8) return '****'
  return key.slice(0, 3) + '****' + key.slice(-4)
}

const SERVICE_TYPE_LABEL = { grpc: 'gRPC', doubao: '豆包', qwen: 'Qwen', openai: 'OpenAI', gemini: 'Gemini' }

const serviceDisplay = (system) => {
  if (!system) return '未配置'
  const t = system.type || 'grpc'
  if (t === 'grpc') return system.url || '未配置'
  return (system.api_key || system.has_api_key) ? (maskKey(system.api_key) || 'Key 已配置') : '未配置 Key'
}

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
  const explicit = item.asrEndTime ?? item.asr_end_time ?? item.end ?? item.ed ?? item.asr_time
  if (Number.isFinite(Number(explicit))) return Number(explicit)
  return Number(item.start || 0)
}

function buildTimelineRows(leftItems, rightItems, query) {
  const q = query.trim().toLowerCase()
  const grouped = new Map()
  const append = (side, items) => {
    items.forEach((item, index) => {
      if (q && !`${item.asr} ${item.mt}`.toLowerCase().includes(q)) return
      const stamp = eventStamp(item, side, index)
      const key = String(stamp)
      if (!grouped.has(key)) {
        grouped.set(key, { id: `time-${key}`, stamp, left: [], right: [] })
      }
      grouped.get(key)[side].push({
        id: `${side}-${item.id}`,
        side,
        kind: 'bundle',
        label: 'ASR + MT',
        item,
        chunkIndex: index + 1,
        stamp,
      })
    })
  }
  append('left', leftItems)
  append('right', rightItems)
  return Array.from(grouped.values())
    .map(row => ({
      ...row,
      left: row.left.sort((a, b) => a.chunkIndex - b.chunkIndex),
      right: row.right.sort((a, b) => a.chunkIndex - b.chunkIndex),
    }))
    .sort((a, b) => a.stamp - b.stamp)
}

function buildTimelineLayout(rows) {
  if (!rows.length) return { rows: [], height: 360 }
  const maxStamp = Math.max(...rows.map(row => Number(row.stamp) || 0), 0)
  // 累加 row.top:每个 row 高度按 max(left cell 数, right cell 数) 自适应,
  // row 之间固定 gap。媒体时间只作为参考显示在中间 spine,不再作为像素级定位,
  // 避免稠密 chunk(cell 高度 > 时间间距)时重叠。
  let cursor = TIMELINE_TOP_PADDING
  const positionedRows = rows.map(row => {
    const cellRows = Math.max(row.left.length, row.right.length)
    const rowHeight = TIMELINE_ROW_BASE_HEIGHT + Math.max(0, cellRows - 1) * 36
    const positioned = { ...row, top: cursor, height: rowHeight }
    cursor += rowHeight + TIMELINE_ROW_GAP
    return positioned
  })
  return {
    rows: positionedRows,
    height: Math.max(360, cursor + TIMELINE_BOTTOM_PADDING - TIMELINE_ROW_GAP),
  }
}

function App() {
  const [systems, setSystems] = useState(initialSystems)
  const [selectedSystem, setSelectedSystem] = useState('system-a')
  // Compare slots reference one entry from `systems` by id.  A/B used to be
  // pinned to systems[0]/systems[1] which forced every other service into
  // "read-only" territory; now any sidebar service can be promoted into a
  // slot by clicking its A/B chip.
  const [slotA, setSlotA] = useState('system-a')
  const [slotB, setSlotB] = useState('system-b')
  const [direction, setDirection] = useState('zh2en')
  const [conferenceId, setConferenceId] = useState(`my_test_${Date.now()}`)
  const [video, setVideo] = useState(null)
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState(0)
  const [runStage, setRunStage] = useState('idle')
  const [activeTab, setActiveTab] = useState('compare')
  const [activeRoute, setActiveRoute] = useState('compare')
  const [selectedChunk, setSelectedChunk] = useState('')
  const [selectedSide, setSelectedSide] = useState('left')
  const [query, setQuery] = useState('')
  const [showSystemModal, setShowSystemModal] = useState(false)
  const [newServiceType, setNewServiceType] = useState('grpc')
  const [newServiceGroup, setNewServiceGroup] = useState(null)
  const [expandedGroups, setExpandedGroups] = useState({ grpc: true, api: true })
  const [editingService, setEditingService] = useState(null)
  const [editingType, setEditingType] = useState('grpc')
  const [toast, setToast] = useState('')
  const [serverState, setServerState] = useState('offline')
  const [serverRunId, setServerRunId] = useState(null)
  const [lastRunId, setLastRunId] = useState(null)
  const [runHistory, setRunHistory] = useState([])
  const [historyLoading, setHistoryLoading] = useState(false)
  const [mediaUrl, setMediaUrl] = useState('')
  const [mediaMuted, setMediaMuted] = useState(false)
  const [mediaPlaying, setMediaPlaying] = useState(false)
  const [mediaArmedRunId, setMediaArmedRunId] = useState(null)
  const [uploadId, setUploadId] = useState('')
  // When new chunks stream in, the timeline keeps growing.  If the user
  // already scrolled away from the bottom (e.g. to read the inspector) we
  // respect that and stop auto-following.  They can re-enable via the chip.
  const [timelineFollow, setTimelineFollow] = useState(true)
  const [timelineFullscreen, setTimelineFullscreen] = useState(false)
  const [uploadState, setUploadState] = useState('idle')
  const [uploadProgress, setUploadProgress] = useState(0)
  const [uploadError, setUploadError] = useState('')
  const [debugInfo, setDebugInfo] = useState(null)
  const [debugLoading, setDebugLoading] = useState(false)
  const [leftItems, setLeftItems] = useState([])
  const [rightItems, setRightItems] = useState([])
  const timelineListRef = useRef(null)
  const fullscreenListRef = useRef(null)
  const fileInputRef = useRef(null)
  const mediaRef = useRef(null)
  const handleTimelineScroll = event => {
    const el = event.currentTarget
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight
    // Stick-to-bottom: keep following while near the bottom; the moment the
    // user scrolls up to read a chunk, stop yanking them down.  Scrolling
    // back to the bottom re-enables follow.  No toggle needed.
    setTimelineFollow(distanceFromBottom <= 24)
  }
  useEffect(() => {
    if (!timelineFollow) return
    for (const el of [timelineListRef.current, fullscreenListRef.current]) {
      if (el) el.scrollTop = el.scrollHeight
    }
  }, [leftItems, rightItems, timelineFollow, timelineFullscreen])

  const timelineRows = useMemo(() => buildTimelineRows(leftItems, rightItems, query), [leftItems, rightItems, query])
  const timelineLayout = useMemo(() => buildTimelineLayout(timelineRows), [timelineRows])

  useEffect(() => {
    let disposed = false
    fetch(`${API_BASE}/api/config`)
      .then(response => response.ok ? response.json() : null)
      .then(config => {
        if (disposed || !config?.services) return
        setServerState('connected')
        const configured = [config.services.left, config.services.right]
        setSystems(items => items.map((item, index) => {
          const service = configured[index] || {}
          return {
            ...item,
            label: service.label || item.label,
            url: service.grpc_url || item.url,
            type: service.type || 'grpc',
            has_api_key: service.has_api_key || false,
          }
        }))
      })
      .catch(() => setServerState('offline'))
    return () => { disposed = true }
  }, [])

  useEffect(() => {
    if (!video) {
      setMediaUrl('')
      setMediaPlaying(false)
      return undefined
    }
    const url = URL.createObjectURL(video)
    setMediaUrl(url)
    setMediaPlaying(false)
    return () => URL.revokeObjectURL(url)
  }, [video])

  useEffect(() => {
    if (mediaRef.current) mediaRef.current.muted = mediaMuted
  }, [mediaMuted])

  useEffect(() => {
    if (!mediaRef.current || !mediaUrl) return
    mediaRef.current.pause()
    mediaRef.current.currentTime = 0
    mediaRef.current.muted = mediaMuted
    mediaRef.current.load()
    setMediaPlaying(false)
  }, [mediaUrl])

  useEffect(() => {
    if (!serverRunId && mediaArmedRunId) setMediaArmedRunId(null)
  }, [serverRunId, mediaArmedRunId])

  useEffect(() => {
    const debugRunId = serverRunId || lastRunId
    if (!debugRunId || !selectedChunk) {
      setDebugInfo(null)
      return undefined
    }
    const sideItems = selectedSide === 'left' ? leftItems : rightItems
    const item = sideItems.find(row => row.id === selectedChunk)
    const chunkId = item?.chunk_id ?? item?.sn ?? item?.id
    if (chunkId === undefined || chunkId === null || String(chunkId).startsWith('chunk-')) {
      setDebugInfo(null)
      return undefined
    }
    let disposed = false
    setDebugLoading(true)
    fetch(`${API_BASE}/api/runs/${debugRunId}/debug/${selectedSide}/${chunkId}`)
      .then(response => response.ok ? response.json() : null)
      .then(data => { if (!disposed) setDebugInfo(data) })
      .catch(() => { if (!disposed) setDebugInfo(null) })
      .finally(() => { if (!disposed) setDebugLoading(false) })
    return () => { disposed = true }
  }, [serverRunId, lastRunId, selectedChunk, selectedSide, leftItems, rightItems])

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
        setRunStage(run.stage || run.status || 'running')
        if (run.stream_started && mediaArmedRunId === serverRunId) {
          setMediaArmedRunId(null)
          playSourceMediaFromStart()
        }
        const chunksResponse = await fetch(`${API_BASE}/api/runs/${serverRunId}/chunks`)
        if (chunksResponse.ok) {
          const chunks = await chunksResponse.json()
          if (Array.isArray(chunks.left)) setLeftItems(chunks.left)
          if (Array.isArray(chunks.right)) setRightItems(chunks.right)
        }
        if (run.status === 'completed') {
          setRunning(false)
          setServerRunId(null)
          notify('gRPC 对比任务完成')
        } else if (run.status === 'cancelled') {
          setRunning(false)
          setServerRunId(null)
          notify('任务已停止')
        } else if (run.status === 'partial_completed') {
          setRunning(false)
          setServerRunId(null)
          notify(`部分服务失败：${run.error || '请查看错误卡片'}`)
        } else if (run.status === 'failed') {
          setRunning(false)
          setServerRunId(null)
          notify(`任务执行失败：${run.error || '请查看后端窗口日志'}`)
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
  }, [serverRunId, mediaArmedRunId, mediaMuted])

  const selectedLeft = leftItems.find(item => item.id === selectedChunk) || leftItems[0]
  const selectedRight = rightItems.find(item => item.id === selectedChunk) || rightItems[0]
  const selected = selectedSide === 'left' ? selectedLeft : selectedRight
  const selectedDebug = debugInfo || {}
  const selectedDebugMetrics = selectedDebug.debug || {}
  const selectedDebugAsr = selectedDebug.asr || {}
  const selectedDebugMt = selectedDebug.mt || {}
  const selectedChunkId = selected?.chunk_id ?? selected?.sn ?? selected?.id ?? ''
  const debugAudioUrl = selectedDebug.audio_url ? `${API_BASE}${selectedDebug.audio_url}` : ''
  const inspectorLogs = [
    selectedDebugAsr.src_text ? { label: 'ASR', text: selectedDebugAsr.src_text } : selected?.asr ? { label: 'ASR', text: selected.asr } : null,
    selectedDebugMt.tgt_text ? { label: 'MT', text: selectedDebugMt.tgt_text } : selected?.mt ? { label: 'MT', text: selected.mt } : null,
    ...(Array.isArray(selectedDebug.logs) ? selectedDebug.logs : selected?.logs || []),
    selectedDebug.debug_found === false ? 'debug log not found' : '',
    selectedDebug.audio_found === false ? 'debug audio not found' : '',
    selectedDebugMetrics.rtf !== undefined ? `RTF=${selectedDebugMetrics.rtf}` : '',
    selectedDebugMetrics.ctc_avg_prob !== undefined ? `CTC=${selectedDebugMetrics.ctc_avg_prob}` : '',
    selectedDebugMetrics.dur_vad !== undefined ? `VAD=${selectedDebugMetrics.dur_vad}s` : '',
    selectedDebugMetrics.concat_wav_duration !== undefined ? `concat=${selectedDebugMetrics.concat_wav_duration}s` : '',
  ].filter(Boolean)
  const progressLabel = running ? runStage : progress >= 100 ? 'completed' : 'idle'
  const mediaStateLabel = mediaPlaying ? 'playing' : uploadState === 'uploading' ? 'loading' : uploadState === 'failed' ? 'load failed' : mediaUrl ? 'ready' : 'no media'
  const uploadProgressWidth = uploadState === 'failed' ? 100 : uploadProgress
  const audioPanelMode = uploadState === 'uploading' || uploadState === 'ready' || uploadState === 'failed' ? 'upload' : 'run'
  const audioPanelStatus = uploadState === 'uploading' ? 'UPLOADING' : uploadState === 'ready' ? 'UPLOAD READY' : uploadState === 'failed' ? 'UPLOAD FAILED' : running ? 'STREAMING' : 'READY'
  const audioPanelProgress = audioPanelMode === 'upload' ? uploadProgressWidth : running ? progress : 0
  const audioPanelProgressText = uploadState === 'uploading' ? `${uploadProgress}%` : uploadState === 'ready' ? '100%' : uploadState === 'failed' ? 'error' : running ? `${Math.min(progress, 100)}%` : '0%'
  const audioPanelLabel = uploadState === 'uploading' ? 'uploading audio' : uploadState === 'ready' ? 'audio ready' : uploadState === 'failed' ? 'upload failed' : progressLabel

  const notify = (message) => {
    setToast(message)
    window.setTimeout(() => setToast(''), 2400)
  }

  const playSourceMediaFromStart = async () => {
    if (!mediaRef.current) return
    try {
      mediaRef.current.currentTime = 0
      mediaRef.current.muted = mediaMuted
      await mediaRef.current.play()
      setMediaPlaying(true)
    } catch {
      setMediaPlaying(false)
      notify('原音频自动播放失败，请检查浏览器播放权限')
    }
  }

  const unlockSourceMediaPlayback = async () => {
    if (!mediaRef.current) return
    try {
      mediaRef.current.currentTime = 0
      mediaRef.current.muted = true
      await mediaRef.current.play()
      mediaRef.current.pause()
      mediaRef.current.currentTime = 0
      mediaRef.current.muted = mediaMuted
      setMediaPlaying(false)
    } catch {
      mediaRef.current.pause()
      mediaRef.current.currentTime = 0
      mediaRef.current.muted = mediaMuted
      setMediaPlaying(false)
    }
  }

  const uploadSelectedFile = async (file) => {
    setUploadId('')
    setUploadState('uploading')
    try {
      const form = new FormData()
      form.append('video', file)
      const response = await fetch(`${API_BASE}/api/uploads`, { method: 'POST', body: form })
      if (!response.ok) throw new Error('upload')
      const data = await response.json()
      setUploadId(data.upload_id)
      setUploadState('ready')
      setServerState('connected')
    } catch {
      setUploadState('failed')
      setServerState('offline')
      notify('音频加载到后端失败，点击开始时会尝试直接上传')
    }
  }
  const uploadSelectedFileWithProgress = async (file) => {
    setUploadId('')
    setUploadState('uploading')
    setUploadProgress(0)
    setUploadError('')
    try {
      const data = await new Promise((resolve, reject) => {
        const form = new FormData()
        form.append('video', file)
        const xhr = new XMLHttpRequest()
        xhr.open('POST', `${API_BASE}/api/uploads`)
        xhr.upload.onprogress = event => {
          if (!event.lengthComputable) return
          const value = Math.max(1, Math.min(99, Math.round((event.loaded / event.total) * 100)))
          setUploadProgress(value)
        }
        xhr.onload = () => {
          let payload = {}
          try {
            payload = JSON.parse(xhr.responseText || '{}')
          } catch {
            payload = {}
          }
          if (xhr.status >= 200 && xhr.status < 300) resolve(payload)
          else reject(new Error(payload.detail || `upload failed: ${xhr.status}`))
        }
        xhr.onerror = () => reject(new Error('upload network error'))
        xhr.onabort = () => reject(new Error('upload aborted'))
        xhr.send(form)
      })
      setUploadId(data.upload_id)
      setUploadProgress(100)
      setUploadState('ready')
      setServerState('connected')
    } catch (error) {
      setUploadState('failed')
      setUploadProgress(0)
      setUploadError(error?.message || 'upload failed')
      setServerState('offline')
      notify('音频上传失败，请检查格式或网络')
    }
  }

  const addSystem = (event) => {
    event.preventDefault()
    const form = new FormData(event.currentTarget)
    const label = String(form.get('label') || '').trim() || '自定义服务'
    const type = newServiceType
    const url = type === 'grpc' ? String(form.get('url') || '').trim() : ''
    const api_key = type !== 'grpc' ? String(form.get('api_key') || '').trim() : ''
    const debug_log = type === 'grpc' ? String(form.get('debug_log') || '').trim() : ''
    const debug_root = type === 'grpc' ? String(form.get('debug_root') || '').trim() : ''
    if (type === 'grpc' && !url) return
    if (type !== 'grpc' && !api_key) return
    const palette = ['amber', 'green', 'pink', 'teal']
    const next = { id: `system-${Date.now()}`, label, name: 'Custom endpoint', url, type, api_key, has_api_key: !!api_key, language: 'zh → en', color: palette[systems.length % palette.length], enabled: true, debug_log, debug_root }
    setSystems(items => [...items, next])
    setSelectedSystem(next.id)
    setShowSystemModal(false)
    setNewServiceType('grpc')
    setNewServiceGroup(null)
    notify(type === 'grpc' ? 'gRPC 服务已加入对比' : `${SERVICE_TYPE_LABEL[type] || type} 服务已加入对比`)
  }

  const updateSystemUrl = (id, url) => {
    setSystems(items => items.map(system => system.id === id ? { ...system, url } : system))
  }

  // Persist the two compare slots (left + right) to the backend config file.
  // We always send a complete payload so the server can atomically rewrite
  // the file; missing slots fall back to empty so the user can clearly see
  // "no endpoint configured yet" instead of an old URL hanging around.
  const persistSlots = async (leftEntry, rightEntry) => {
    const payload = {
      services: {
        left: {
          label: leftEntry.label || '',
          type: leftEntry.type || 'grpc',
          grpc_url: leftEntry.url || '',
          api_key: leftEntry.api_key || '',
          debug_log: leftEntry.debug_log || '',
          debug_root: leftEntry.debug_root || '',
        },
        right: {
          label: rightEntry.label || '',
          type: rightEntry.type || 'grpc',
          grpc_url: rightEntry.url || '',
          api_key: rightEntry.api_key || '',
          debug_log: rightEntry.debug_log || '',
          debug_root: rightEntry.debug_root || '',
        },
      },
    }
    const response = await fetch(`${API_BASE}/api/config`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    if (!response.ok) {
      const detail = await response.text()
      throw new Error(detail || `HTTP ${response.status}`)
    }
  }

  const openEditService = (system) => {
    const isSlot = system.id === 'system-a' || system.id === 'system-b'
    const side = system.id === 'system-a' ? 'left' : system.id === 'system-b' ? 'right' : null
    const stype = system.type || 'grpc'
    setEditingType(stype)
    setEditingService({
      system,
      isSlot,
      side,
      label: system.label || '',
      type: stype,
      url: system.url || '',
      api_key: '',
      has_api_key: system.has_api_key || !!system.api_key,
      debug_log: system.debug_log || '',
      debug_root: system.debug_root || '',
    })
  }

  const submitEditService = async (event) => {
    event.preventDefault()
    if (!editingService) return
    const form = new FormData(event.currentTarget)
    const label = String(form.get('label') || '').trim() || '自定义服务'
    const type = String(form.get('type') || 'grpc')
    const url = type === 'grpc' ? String(form.get('url') || '').trim() : ''
    const api_key = type !== 'grpc' ? String(form.get('api_key') || '').trim() : ''
    const debug_log = type === 'grpc' ? String(form.get('debug_log') || '').trim() : ''
    const debug_root = type === 'grpc' ? String(form.get('debug_root') || '').trim() : ''
    const { system, isSlot, side } = editingService

    const updated = { label, type, url, api_key, has_api_key: api_key ? true : editingService.has_api_key, debug_log, debug_root }
    let snapshot = null
    setSystems(items => {
      snapshot = items
      return items.map(item => item.id === system.id ? { ...item, ...updated, api_key: api_key || item.api_key } : item)
    })

    if (isSlot) {
      const other = side === 'left' ? systems[1] : systems[0]
      try {
        const otherEntry = { label: other?.label || '', type: other?.type || 'grpc', url: other?.url || '', api_key: '', debug_log: other?.debug_log || '', debug_root: other?.debug_root || '' }
        const leftEntry = side === 'left' ? updated : otherEntry
        const rightEntry = side === 'right' ? updated : otherEntry
        await persistSlots(leftEntry, rightEntry)
        notify('服务配置已保存到 config')
      } catch (err) {
        if (snapshot) setSystems(snapshot)
        notify(`保存失败：${err.message || err}`)
      }
    } else {
      notify('服务信息已更新（仅本地，未持久化到 config）')
    }

    setEditingService(null)
  }

  const deleteCustomService = (system) => {
    if (system.id === 'system-a' || system.id === 'system-b') return
    setSystems(items => items.filter(item => item.id !== system.id))
    if (selectedSystem === system.id) setSelectedSystem('system-a')
    if (slotA === system.id) setSlotA('system-a')
    if (slotB === system.id) setSlotB('system-b')
    notify('已删除自定义服务')
  }

  // Toggle a service into / out of a compare slot.  Each side accepts exactly
  // one service; clicking the same chip again clears the slot back to the
  // default (system-a / system-b) so the user always has a working pair.
  const assignToSlot = (serviceId, slot) => {
    if (slot === 'A') {
      setSlotA(current => current === serviceId ? 'system-a' : serviceId)
    } else {
      setSlotB(current => current === serviceId ? 'system-b' : serviceId)
    }
  }

  const slotService = (slot) => {
    const id = slot === 'A' ? slotA : slotB
    return systems.find(item => item.id === id) || systems[slot === 'A' ? 0 : 1] || systems[0]
  }

  const cancelServerRun = async (runId) => {
    if (!runId) return
    try {
      await fetch(`${API_BASE}/api/runs/${runId}/cancel`, { method: 'POST' })
    } catch {
      // The UI should still stop polling even if the backend is already gone.
    }
  }

  const resetRun = () => {
    const activeRunId = serverRunId
    if (mediaRef.current) {
      mediaRef.current.pause()
      mediaRef.current.currentTime = 0
    }
    setMediaPlaying(false)
    setMediaArmedRunId(null)
    setRunning(false)
    setServerRunId(null)
    setLastRunId(null)
    setProgress(0)
    setRunStage('idle')
    setUploadId('')
    setUploadState('idle')
    setUploadProgress(0)
    setUploadError('')
    setVideo(null)
    setLeftItems([])
    setRightItems([])
    setDebugInfo(null)
    setDebugLoading(false)
    setSelectedChunk('')
    setSelectedSide('left')
    if (fileInputRef.current) fileInputRef.current.value = ''
    cancelServerRun(activeRunId)
    notify(activeRunId ? '已停止并重置当前任务' : '已重置当前任务')
  }

  const startRun = async () => {
    if (serverRunId) {
      await cancelServerRun(serverRunId)
    }
    setServerRunId(null)
    setRunning(true)
    setRunStage('uploading')
    setProgress(1)
    if (!video) {
      notify('当前使用演示音频，可直接查看交互')
      setRunning(false)
      setRunStage('idle')
      return
    }
    try {
      const leftSystem = slotService('A')
      const rightSystem = slotService('B')
      const enabled = [leftSystem, rightSystem].filter(system => system && system.enabled && (
        (system.type || 'grpc') === 'grpc' ? system.url.trim() : (system.api_key || system.has_api_key)
      ))
      if (!enabled.length) {
        notify('请至少配置一个服务（gRPC 地址或 API Key）')
        if (mediaRef.current) {
          mediaRef.current.pause()
          mediaRef.current.currentTime = 0
        }
        setMediaPlaying(false)
        setRunning(false)
        setRunStage('idle')
        setProgress(0)
        return
      }
      if (uploadState === 'uploading') {
        notify('音频还在加载到后端，请稍后开始')
        setRunning(false)
        setRunStage('idle')
        setProgress(0)
        return
      }
      await unlockSourceMediaPlayback()
      setLeftItems([])
      setRightItems([])
      setSelectedSide('left')
      const form = new FormData()
      if (uploadId) form.append('upload_id', uploadId)
      else form.append('video', video)
      form.append('systems', JSON.stringify(enabled))
      form.append('direction', direction)
      form.append('conference_id', conferenceId.trim())
      const response = await fetch(`${API_BASE}/api/runs`, { method: 'POST', body: form })
      if (!response.ok) throw new Error('server')
      const data = await response.json()
      setServerState('connected')
      setServerRunId(data.run_id)
      setLastRunId(data.run_id)
      setRunStage('queued')
      if (uploadId) {
        setUploadId('')
        setUploadState('consumed')
      }
      setMediaArmedRunId(data.run_id)
      notify(`任务 ${data.run_id} 已启动`)
    } catch {
      if (mediaRef.current) {
        mediaRef.current.pause()
        mediaRef.current.currentTime = 0
      }
      setMediaPlaying(false)
      setRunStage('failed')
      setServerState('offline')
      notify('后端未连接，已切换为本地演示模式')
    }
  }

  const fetchRunHistory = async () => {
    setHistoryLoading(true)
    try {
      const res = await fetch(`${API_BASE}/api/runs`)
      if (!res.ok) throw new Error('history')
      const data = await res.json()
      setRunHistory(Array.isArray(data.runs) ? data.runs : [])
    } catch {
      setRunHistory([])
    } finally {
      setHistoryLoading(false)
    }
  }
  useEffect(() => { fetchRunHistory() }, [activeTab])

  const loadHistoryRun = async runId => {
    try {
      const [runRes, chunksRes] = await Promise.all([
        fetch(`${API_BASE}/api/runs/${runId}`),
        fetch(`${API_BASE}/api/runs/${runId}/chunks`),
      ])
      const run = runRes.ok ? await runRes.json() : {}
      const chunks = chunksRes.ok ? await chunksRes.json() : {}
      const isActive = !['completed', 'cancelled', 'partial_completed', 'failed'].includes(run.status)
      setLeftItems(Array.isArray(chunks.left) ? chunks.left : [])
      setRightItems(Array.isArray(chunks.right) ? chunks.right : [])
      setLastRunId(runId)
      setSelectedChunk('')
      setSelectedSide('left')
      setDebugInfo(null)
      setActiveTab('compare')
      if (isActive) {
        setServerRunId(runId)
        setRunning(true)
        setRunStage(run.stage || run.status || 'running')
        notify(`已恢复实时任务 ${runId}`)
      } else {
        setServerRunId(null)
        setRunning(false)
        setProgress(run.progress || 0)
        setRunStage(run.stage || run.status || 'idle')
        notify(`已加载任务 ${runId}`)
      }
    } catch {
      notify('加载历史任务失败')
    }
  }

  const goToCompare = () => {
    setActiveRoute('compare')
    setActiveTab('compare')
    const activeRun = runHistory.find(r => r && !['completed', 'cancelled', 'partial_completed', 'failed'].includes(r.status))
    if (activeRun && activeRun.run_id !== serverRunId) {
      setLeftItems([])
      setRightItems([])
      setServerRunId(activeRun.run_id)
      setRunning(true)
      setLastRunId(activeRun.run_id)
      setRunStage(activeRun.stage || activeRun.status || 'running')
      notify(`已恢复实时任务 ${activeRun.run_id}`)
    }
  }

  const handleFile = event => {
    const file = event.target.files?.[0]
    if (file) {
      setVideo(file)
      setProgress(0)
      setUploadProgress(0)
      setUploadError('')
      setRunStage('idle')
      setMediaArmedRunId(null)
      setLeftItems([])
      setRightItems([])
      setDebugInfo(null)
      setDebugLoading(false)
      setSelectedChunk('')
      setSelectedSide('left')
      notify(`已选择 ${file.name}`)
      uploadSelectedFileWithProgress(file)
    }
  }

  const playDebugAudio = async () => {
    if (!debugAudioUrl) {
      notify('debug 音频未找到')
      return
    }
    try {
      await new Audio(debugAudioUrl).play()
    } catch {
      notify('debug 音频播放失败')
    }
  }

  const toggleMediaMute = () => {
    setMediaMuted(value => !value)
  }

  const copyValue = value => { navigator.clipboard?.writeText(value); notify('已复制服务地址') }

  const runStatusBar = (
    <div className={`run-bar source-run-bar ${uploadState === 'failed' ? 'failed' : ''}`}>
      <div className="source-run-top">
        <div className="run-status"><span className={running || uploadState === 'uploading' ? 'pulse-dot' : 'complete-dot'} /> {audioPanelStatus}</div>
        <span className="file-name"><FileAudio size={14} /> {video?.name || 'sample_audio.wav'}</span>
        <span className="media-state">{mediaStateLabel}</span>
      </div>
      <div className="source-run-bottom">
        <span className="run-meta">{direction} · 原音同步播放</span>
        <button className={`media-mute ${mediaMuted ? 'muted' : ''}`} type="button" onClick={toggleMediaMute} disabled={!mediaUrl}>{mediaMuted ? <VolumeX size={14} /> : <Volume2 size={14} />} {mediaMuted ? '静音' : '原音'}</button>
        <div className="progress-wrap">
          <span>{audioPanelProgressText}</span>
          <div className="progress-track"><div className="progress-value" style={{ width: `${audioPanelProgress}%` }} /></div>
          <span className="progress-label">{audioPanelLabel}</span>
        </div>
      </div>
      {uploadError && <div className="source-run-error">{uploadError}</div>}
    </div>
  )

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand"><div className="brand-mark"><GitCompareArrows size={19} /></div><div><div className="brand-name">SIM<span>COMPARE</span></div><div className="brand-sub">同传结果调试台</div></div></div>
        <div className="topbar-center"><div className="live-dot" /> <span>LOCAL WORKSPACE</span><i /> <span className={serverState === 'connected' ? 'server-online' : ''}>{serverState === 'connected' ? 'API CONNECTED' : 'API OFFLINE'}</span></div>
        <div className="topbar-actions"><button className="icon-button" title="帮助"><CircleHelp size={18} /></button><button className="icon-button" title="设置"><Settings2 size={18} /></button><div className="avatar">LY</div></div>
      </header>

      <aside className="sidebar">
        <div className="route-switch">
          <button className={`route-item ${activeRoute === 'compare' ? 'active' : ''}`} onClick={() => setActiveRoute('compare')}><GitCompareArrows size={16} /> 同传对比调试</button>
          <button className={`route-item ${activeRoute === 'evaluation' ? 'active' : ''}`} onClick={() => setActiveRoute('evaluation')}><Gauge size={16} /> 同传质量评估</button>
        </div>
        {activeRoute === 'compare' && (<>
        <div className="side-label">WORKSPACE</div>
        <button className={`nav-item ${activeTab === 'compare' ? 'active' : ''}`} onClick={goToCompare}><GitCompareArrows size={17} /> 对比调试</button>
        <button className={`nav-item ${activeTab === 'history' ? 'active' : ''}`} onClick={() => setActiveTab('history')}><History size={17} /> 任务历史 <span className="nav-count">{runHistory.length}</span></button>
        </>)}
        {activeRoute === 'evaluation' && (<>
        <div className="side-label">EVALUATION</div>
        <button className="nav-item active"><Gauge size={17} /> 质量评估</button>
        </>)}
        <div className="side-label service-label">SERVICES</div>
        {(() => {
          const groups = [
            { key: 'grpc', title: '我们的系统', list: systems.filter(s => (s.type || 'grpc') === 'grpc') },
            { key: 'api', title: '外部模型', list: systems.filter(s => (s.type || 'grpc') !== 'grpc') },
          ]
          return groups.map(group => {
            const expanded = expandedGroups[group.key]
            return (
              <div className={`service-folder ${expanded ? 'expanded' : ''}`} key={group.key}>
                <button className="service-folder-head" onClick={() => setExpandedGroups(g => ({ ...g, [group.key]: !g[group.key] }))}>
                  <span className="service-folder-title">
                    <strong>{group.title}</strong>
                    <small>{group.list.length}</small>
                  </span>
                  <ChevronDown size={15} className={`service-folder-chevron ${expanded ? 'up' : ''}`} />
                </button>
                {expanded && (
                  <div className="service-folder-body">
                    {group.list.length === 0 && <div className="service-folder-empty">暂无服务</div>}
                    {group.list.map(system => {
                      const isSlot = system.id === 'system-a' || system.id === 'system-b'
                      const inSlotA = slotA === system.id
                      const inSlotB = slotB === system.id
                      return (
                        <div
                          key={system.id}
                          role="button"
                          tabIndex={0}
                          className={`service-item ${selectedSystem === system.id ? 'selected' : ''} ${inSlotA ? 'in-slot-a' : ''} ${inSlotB ? 'in-slot-b' : ''}`}
                          onClick={() => setSelectedSystem(system.id)}
                          onDoubleClick={() => openEditService(system)}
                          title="单击选中 · 双击编辑"
                        >
                          <span className={`service-dot ${system.color}`} />
                          <span className="service-copy">
                            <strong>{system.label}{system.type && system.type !== 'grpc' ? <span className="service-type-tag">{SERVICE_TYPE_LABEL[system.type] || system.type}</span> : null}</strong>
                            <small>{serviceDisplay(system)}</small>
                          </span>
                          <div className="slot-chips">
                            <button className={`slot-chip ${inSlotA ? 'active cyan' : ''}`} title={inSlotA ? '取消 A 槽位' : '分配到 A 槽位'} onClick={event => { event.stopPropagation(); assignToSlot(system.id, 'A') }}>A</button>
                            <button className={`slot-chip ${inSlotB ? 'active violet' : ''}`} title={inSlotB ? '取消 B 槽位' : '分配到 B 槽位'} onClick={event => { event.stopPropagation(); assignToSlot(system.id, 'B') }}>B</button>
                          </div>
                          <div className="service-actions">
                            <button className="service-icon-btn" title="编辑" onClick={event => { event.stopPropagation(); openEditService(system) }}><Pencil size={12} /></button>
                            <button className={`service-icon-btn danger ${isSlot ? 'placeholder' : ''}`} title={isSlot ? '系统服务不可删除' : '删除'} disabled={isSlot} onClick={event => { if (!isSlot) { event.stopPropagation(); deleteCustomService(system) } }}><X size={12} /></button>
                          </div>
                        </div>
                      )
                    })}
                    <button className="service-folder-add" onClick={() => { setShowSystemModal(true); setNewServiceGroup(group.key); setNewServiceType(group.key === 'grpc' ? 'grpc' : 'doubao') }}><Plus size={13} /> 添加{group.title}</button>
                  </div>
                )}
              </div>
            )
          })
        })()}
        <div className="sidebar-foot"><span>v0.1.0</span><span>API <span className="api-dot" /></span></div>
      </aside>

      <main className="main-content">
        {activeRoute === 'compare' && activeTab === 'compare' && (<>
        <section className="page-heading"><div><div className="eyebrow"><span>DEBUG SESSION</span><span className="slash">/</span><span>NEW COMPARISON</span></div><h1>同传对比 <em>debug</em></h1><p>并行观察转录与翻译结果，快速定位差异产生的时间点。</p></div><div className="heading-actions"><button className="ghost-button" onClick={resetRun}><RotateCcw size={15} /> 重置</button><button className="primary-button" onClick={startRun} disabled={running}><span className="button-glow" />{running ? <LoaderCircle className="spin" size={16} /> : <Play size={15} fill="currentColor" />} {running ? '运行中…' : '开始对比'}</button></div></section>

        <section className="control-grid">
          <div className="control-card source-card"><div className="card-top"><div className="card-title"><span className="step-number">01</span><div><h3>选择音视频文件</h3><p>上传 WAV / MP3 或视频（MP4/MOV 等），后端会抽音并按音频 chunk 发送至服务。</p></div></div><FileAudio size={20} className="muted-icon" /></div><input ref={fileInputRef} type="file" accept=".wav,.wave,.mp3,.mp4,.mov,.m4a,.m4v,.webm,.mkv,.avi,.flv,audio/wav,audio/mpeg,video/mp4,video/quicktime,video/webm" hidden onChange={handleFile} /><button className={`dropzone ${video ? 'has-file' : ''}`} onClick={() => fileInputRef.current?.click()}><div className="upload-icon">{video ? <FileAudio size={22} /> : <UploadCloud size={22} />}</div><div><strong>{video ? video.name : '拖拽文件到这里，或点击选择'}</strong><small>{video ? `${(video.size / 1024 / 1024).toFixed(1)} MB · 已就绪` : '支持 WAV / MP3 / MP4 / MOV · 最大 2 GB'}</small></div><ChevronDown size={16} className="drop-chevron" /></button>{runStatusBar}</div>
          <div className="control-card service-card"><div className="card-top"><div className="card-title"><span className="step-number">02</span><div><div className="service-title-row"><h3>配置对比服务</h3><button type="button" className={`direction-switch ${direction === 'en2zh' ? 'is-right' : ''}`} aria-label="切换翻译方向" onClick={() => setDirection(value => value === 'zh2en' ? 'en2zh' : 'zh2en')}><span className="direction-thumb" /><span className="direction-option">zh2en</span><span className="direction-option">en2zh</span></button></div><p>选择两个同传服务（gRPC 或 API 型）进行对比，同时发起流式调用。</p></div></div><Link2 size={20} className="muted-icon" /></div><div className="service-selects">{['A', 'B'].map(slot => { const system = slotService(slot); if (!system) return null; const meta = serviceDisplay(system); return <div className="endpoint-row" key={slot}><span className={`endpoint-tag ${slot === 'A' ? 'cyan' : 'violet'}`}>{slot}</span><label className="endpoint-input"><span>{system.label}{system.type && system.type !== 'grpc' ? ` · ${SERVICE_TYPE_LABEL[system.type] || system.type}` : ''}</span><input value={meta} readOnly onFocus={() => setSelectedSystem(system.id)} placeholder="未配置" spellCheck="false" /></label><button className="copy-button" title="编辑服务" onClick={() => openEditService(system)}><Pencil size={14} /></button></div> })}</div><div className="service-bottom-row"><label className="conference-input service-conference-input"><span>conference_id</span><input value={conferenceId} onChange={event => setConferenceId(event.target.value)} placeholder="my_test_001" spellCheck="false" /></label><button className="inline-add" onClick={() => setShowSystemModal(true)}><Plus size={14} /> 添加服务</button></div><div className="service-conference-hint">{'作为 gRPC sid 和 userinfo.conferenceId 传入，用于定位 debug/{conference_id}/audio/{sn}.wav'}</div></div>
        </section>

        <section className="comparison-panel"><div className="panel-heading"><div><div className="section-kicker"><span className="kicker-line" /> TIMELINE OUTPUT</div><h2>结果时间轴</h2></div><div className="panel-tools"><div className="search-box"><Search size={15} /><input value={query} onChange={event => setQuery(event.target.value)} placeholder="搜索转录或翻译…" /></div><button className="small-tool"><ListFilter size={15} /> 筛选</button><button className="small-tool icon-only"><SlidersHorizontal size={15} /></button><button className="small-tool icon-only" title="全屏时间轴" onClick={() => setTimelineFullscreen(true)}><Maximize2 size={15} /></button></div></div><div className="timeline-header"><div className="group left"><span className="system-badge cyan">A</span><span className="label">{slotService('A')?.label || '系统 A'}</span><span className="url">{serviceDisplay(slotService('A'))}</span><span className="lat">ASR end</span></div><div className="spacer" /><div className="axis-label">ABSOLUTE ASR END TIME</div><div className="spacer" /><div className="group right"><span className="lat">ASR end</span><span className="url">{serviceDisplay(slotService('B'))}</span><span className="label">{slotService('B')?.label || '系统 B'}</span><span className="system-badge violet">B</span></div></div><div ref={timelineListRef} className="timeline-list absolute-timeline" onScroll={handleTimelineScroll}><div className="absolute-axis" /><div style={{ position: 'relative', height: `${timelineLayout.height}px`, minHeight: '100%' }}>{timelineLayout.rows.map((row, index) => <TimelineEventRow key={row.id} row={row} isLast={index === timelineLayout.rows.length - 1} selectedChunk={selectedChunk} selectedSide={selectedSide} onSelect={(chunkId, side) => { setSelectedChunk(chunkId); setSelectedSide(side) }} style={{ '--row-top': `${row.top}px`, '--row-height': `${row.height}px` }} />)}</div></div>{timelineLayout.rows.length === 0 && <div className="empty-search">没有找到匹配结果</div>}<div className="timeline-footer"><span><span className="footer-dot cyan" /> A · {leftItems.length} chunks</span><span><span className="footer-dot violet" /> B · {rightItems.length} chunks</span><span className="footer-note"><Info size={13} /> 当前按绝对 ASR 结束时间排布；左右结果各自落在自己的时间点上</span></div></section>

        <section className="inspector-panel"><div className="inspector-heading"><div className="inspector-title"><div className="inspect-icon"><Logs size={17} /></div><div><div className="section-kicker"><span className="kicker-line" /> INSPECTOR</div><h2>Chunk 调试详情 <span>#{String(selectedChunkId).replace('chunk-', '')}</span></h2></div></div><div className="inspect-actions"><span className="time-chip"><Clock3 size={13} /> {formatTime(selected?.start || 0)} — {formatTime(selected?.end || 0)}</span><button className="small-tool"><TerminalSquare size={14} /> {debugLoading ? '读取中' : selectedDebug.debug_found ? '服务 debug' : '原始 JSON'}</button></div></div><div className="inspector-grid"><div className="debug-log"><div className="subhead"><span>DEBUG LOG</span><span className="log-live"><span className="mini-live" /> {debugLoading ? 'LOADING DEBUG' : selectedDebug.debug_found ? 'SERVICE DEBUG' : 'STREAM LOG'}</span></div><div className="log-window">{inspectorLogs.map((log, index) => <div className="log-line" key={index}><span className="log-time">{formatTime((selected?.start || 0) + index * 184)}</span><span className={`log-level ${index >= Math.max(0, inspectorLogs.length - 4) ? 'accent' : ''}`}>{index >= Math.max(0, inspectorLogs.length - 4) ? 'DEBUG' : 'INFO'}</span><span>{typeof log === 'string' ? log : (log && log.label ? <>{log.label}: {log.text}</> : JSON.stringify(log))}</span></div>)}<div className="log-cursor">_</div></div></div><div className="audio-debug"><div className="subhead"><span>CONCAT AUDIO</span><span className="audio-format">{selectedDebug.audio_found ? 'DEBUG WAV' : 'WAV · 16 kHz'}</span></div><div className="audio-file"><div className="audio-symbol"><AudioLines size={18} /></div><div><strong>{selectedDebug.audio_file || selected?.audio || `${selectedChunkId || 'chunk'}.wav`}</strong><small>conference={selectedDebug.conference_id || selected?.conference_id || conferenceId} · sn={selectedChunkId || '-'}</small></div><button className="play-circle" onClick={playDebugAudio} disabled={!debugAudioUrl}><Play size={15} fill="currentColor" /></button></div><div className="waveform">{Array.from({ length: 52 }, (_, i) => <span key={i} style={{ height: `${18 + ((i * 17 + (selected?.start || 0) / 10) % 30)}%` }} />)}</div><div className="debug-metrics"><span>RTF {selectedDebugMetrics.rtf ?? '-'}</span><span>CTC {selectedDebugMetrics.ctc_avg_prob ?? '-'}</span><span>VAD {selectedDebugMetrics.dur_vad ?? '-'}</span><span>concat {selectedDebugMetrics.concat_wav_duration ?? '-'}</span></div><div className="audio-controls"><button className="audio-play" onClick={playDebugAudio} disabled={!debugAudioUrl}><Play size={13} fill="currentColor" /> 播放 concat 音频</button><span>{formatTime(selectedDebugAsr.bg ?? selected?.start ?? 0)}</span><span>{formatTime(selectedDebugAsr.ed ?? selected?.end ?? 0)}</span><Volume2 size={14} /></div></div></div></section>
        </>)}
        {activeRoute === 'compare' && activeTab === 'history' && (
          <section className="history-panel">
            <div className="panel-heading"><div><div className="section-kicker"><span className="kicker-line" /> HISTORY</div><h2>任务历史</h2></div><div className="panel-tools"><button className="small-tool" onClick={fetchRunHistory}><RotateCcw size={14} /> 刷新</button></div></div>
            <div className="history-list">
              {historyLoading && runHistory.length === 0 && <div className="empty-search">加载中…</div>}
              {!historyLoading && runHistory.length === 0 && <div className="empty-search">暂无任务历史（后端重启后会清空）</div>}
              {runHistory.map(run => (
                <div key={run.run_id} className="history-row" onClick={() => loadHistoryRun(run.run_id)}>
                  <span className={`status-dot status-${run.status || 'unknown'}`}>{run.status || 'unknown'}</span>
                  <div className="history-main">
                    <div className="history-title">{run.conference_id || run.run_id}</div>
                    <div className="history-meta">
                      <span>{run.created_at ? new Date(run.created_at * 1000).toLocaleString() : '-'}</span>
                      <span className="sep">·</span>
                      <span>{run.direction}</span>
                      <span className="sep">·</span>
                      <span>A {run.left_count} / B {run.right_count} chunks</span>
                      <span className="sep">·</span>
                      <span>{run.progress || 0}%</span>
                    </div>
                    <div className="history-services">
                      {(run.systems || []).map((s, i) => (
                        <span key={i} className={`history-svc ${i === 0 ? 'cyan' : 'violet'}`}><span className="badge">{i === 0 ? 'A' : 'B'}</span> {s.label || '系统'} · {s.url}</span>
                      ))}
                    </div>
                  </div>
                  <Play size={16} className="history-go" />
                </div>
              ))}
            </div>
          </section>
        )}
        {activeRoute === 'evaluation' && (
          <section className="placeholder-panel">
            <div className="panel-heading"><div><div className="section-kicker"><span className="kicker-line" /> EVALUATION</div><h2>同传翻译质量评估</h2></div></div>
            <div className="placeholder-body">
              <Gauge size={28} />
              <strong>功能开发中</strong>
              <p>这里将提供同传翻译结果的自动化质量评估指标（如 BLEU、字错率、延迟、流畅度等），敬请期待。</p>
            </div>
          </section>
        )}
      </main>
      {toast && <div className="toast"><Check size={15} /> {toast}</div>}
      {timelineFullscreen && (
        <div className="timeline-fullscreen-overlay" onMouseDown={() => setTimelineFullscreen(false)}>
          <div className="timeline-fullscreen-inner" onMouseDown={event => event.stopPropagation()}>
            <div className="panel-heading">
              <div><div className="section-kicker"><span className="kicker-line" /> TIMELINE OUTPUT</div><h2>结果时间轴 · 全屏</h2></div>
              <div className="panel-tools">
                <div className="search-box"><Search size={15} /><input value={query} onChange={event => setQuery(event.target.value)} placeholder="搜索转录或翻译…" /></div>
                <button className="small-tool icon-only" title="关闭全屏" onClick={() => setTimelineFullscreen(false)}><X size={16} /></button>
              </div>
            </div>
            <div className="timeline-header"><div className="group left"><span className="system-badge cyan">A</span><span className="label">{slotService('A')?.label || '系统 A'}</span><span className="url">{serviceDisplay(slotService('A'))}</span><span className="lat">ASR end</span></div><div className="spacer" /><div className="axis-label">ABSOLUTE ASR END TIME</div><div className="spacer" /><div className="group right"><span className="lat">ASR end</span><span className="url">{serviceDisplay(slotService('B'))}</span><span className="label">{slotService('B')?.label || '系统 B'}</span><span className="system-badge violet">B</span></div></div>
            <div ref={fullscreenListRef} className="timeline-list absolute-timeline is-fullscreen" onScroll={handleTimelineScroll}><div className="absolute-axis" /><div style={{ position: 'relative', height: `${timelineLayout.height}px`, minHeight: '100%' }}>{timelineLayout.rows.map((row, index) => <TimelineEventRow key={row.id} row={row} isLast={index === timelineLayout.rows.length - 1} selectedChunk={selectedChunk} selectedSide={selectedSide} onSelect={(chunkId, side) => { setSelectedChunk(chunkId); setSelectedSide(side) }} style={{ '--row-top': `${row.top}px`, '--row-height': `${row.height}px` }} />)}</div></div>
            {timelineLayout.rows.length === 0 && <div className="empty-search">没有找到匹配结果</div>}
            <div className="timeline-footer"><span><span className="footer-dot cyan" /> A · {leftItems.length} chunks</span><span><span className="footer-dot violet" /> B · {rightItems.length} chunks</span><span className="footer-note"><Info size={13} /> 当前按绝对 ASR 结束时间排布；左右结果各自落在自己的时间点上</span></div>
          </div>
        </div>
      )}
      {mediaUrl && <video ref={mediaRef} src={mediaUrl} preload="auto" playsInline className="source-media-player" onEnded={() => setMediaPlaying(false)} onPause={() => setMediaPlaying(false)} onPlay={() => setMediaPlaying(true)} />}
      {showSystemModal && <div className="modal-backdrop" onMouseDown={() => setShowSystemModal(false)}><div className="modal" onMouseDown={event => event.stopPropagation()}><div className="modal-head"><div><div className="section-kicker"><span className="kicker-line" /> NEW ENDPOINT</div><h2>{newServiceGroup === 'grpc' ? '添加 gRPC 服务' : newServiceGroup === 'api' ? '添加外部模型' : '添加服务'}</h2></div><button className="close-button" onClick={() => setShowSystemModal(false)}><X size={17} /></button></div><form onSubmit={addSystem}><label>服务名称<input name="label" placeholder="例如：豆包同传 / 我的本地测试版" autoFocus /></label>{newServiceGroup === null && <label>服务类型<select value={newServiceType} onChange={event => setNewServiceType(event.target.value)}><option value="grpc">gRPC（内部同传系统）</option><option value="doubao">豆包 Doubao（API）</option><option value="qwen">通义千问 Qwen（API）</option></select></label>}{newServiceGroup === 'api' && <label>模型类型<select value={newServiceType} onChange={event => setNewServiceType(event.target.value)}><option value="doubao">Doubao · 豆包</option><option value="qwen">Qwen · 通义千问</option><option value="openai">OpenAI</option><option value="gemini">Gemini</option></select></label>}{newServiceType === 'grpc' ? <><label>gRPC 地址<input name="url" placeholder="127.0.0.1:7860" required /></label><details className="modal-advanced"><summary>debug 路径（可选）</summary><label>debug_log<input name="debug_log" placeholder="/var/log/qwen3_asr.log" spellCheck="false" /></label><label>debug_root<input name="debug_root" placeholder="/data/debug" spellCheck="false" /></label></details></> : <label>API Key<input name="api_key" type="password" placeholder="sk-..." required autoComplete="off" /></label>}<div className="modal-hint"><Info size={14} /> {newServiceType === 'grpc' ? '支持 host:port 格式。后端会将此地址传入流式调用。' : 'API Key 仅存储在服务端 config，不会在前端明文展示。API 型服务的调用适配器尚未接入，配置后暂不可运行对比。'}</div><div className="modal-actions"><button type="button" className="ghost-button" onClick={() => setShowSystemModal(false)}>取消</button><button className="primary-button" type="submit"><Plus size={15} /> 添加</button></div></form></div></div>}
      {editingService && <div className="modal-backdrop" onMouseDown={() => setEditingService(null)}><div className="modal" onMouseDown={event => event.stopPropagation()}><div className="modal-head"><div><div className="section-kicker"><span className="kicker-line" /> EDIT ENDPOINT</div><h2>编辑服务{editingService.isSlot ? `（${editingService.side === 'left' ? 'A' : 'B'} 槽位 · 同步到 config）` : '（仅本地）'}</h2></div><button className="close-button" onClick={() => setEditingService(null)}><X size={17} /></button></div><form onSubmit={submitEditService}><label>服务名称<input name="label" defaultValue={editingService.label} autoFocus required /></label><label>服务类型<select name="type" value={editingType} onChange={event => setEditingType(event.target.value)}><option value="grpc">gRPC（内部同传系统）</option><option value="doubao">豆包 Doubao（API）</option><option value="qwen">通义千问 Qwen（API）</option></select></label>{editingType === 'grpc' ? <><label>gRPC 地址<input name="url" defaultValue={editingService.url} placeholder="127.0.0.1:7860" required spellCheck="false" /></label><details className="modal-advanced"><summary>debug 路径（可选）</summary><label>debug_log<input name="debug_log" defaultValue={editingService.debug_log} placeholder="/var/log/qwen3_asr.log" spellCheck="false" /></label><label>debug_root<input name="debug_root" defaultValue={editingService.debug_root} placeholder="/data/debug" spellCheck="false" /></label></details></> : <label>API Key<input name="api_key" type="password" placeholder={editingService.has_api_key ? '已配置，留空保持不变' : 'sk-...'} autoComplete="off" /></label>}<div className="modal-hint"><Info size={14} /> {editingService.isSlot ? '保存后会写回 simcompare.config.json，团队其它成员 git pull 后即可看到。' : '这只是 sidebar 里的一个便签，不会持久化。'}</div><div className="modal-actions"><button type="button" className="ghost-button" onClick={() => setEditingService(null)}>取消</button><button className="primary-button" type="submit"><Check size={15} /> 保存</button></div></form></div></div>}
    </div>
  )
}

function TimelineResultCard({ event, selectedChunk, selectedSide, onSelect }) {
  if (!event) return null
  const { side, item, chunkIndex, stamp } = event
  const isSelected = selectedChunk === item.id && selectedSide === side
  const asrReady = Boolean(item.asr)
  const mtReady = Boolean(item.mt)
  return (
    <div className={`result-cell ${side}-result bundle-result ${isSelected ? 'focus' : ''}`} style={{ transform: `translateY(-${TIMELINE_CARD_ANCHOR_Y}px)` }} onClick={() => onSelect(item.id, side)}>
      <div className="result-head">
        <div className="result-tags">
          <span className="result-tag asr">ASR</span>
          <span className="result-tag mt">MT</span>
          <span className="result-time">end {formatTime(stamp)}</span>
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
          {mtReady ? <p className="result-text mt-text">{item.mt}</p> : null}
        </div>
      </div>
      <div className="event-meta"><span>CHUNK {String(chunkIndex).padStart(2, '0')}</span><span>{formatTime(item.start || 0)} — {formatTime(item.end || 0)}</span></div>
    </div>
  )
}

function TimelineEventRow({ row, isLast, selectedChunk, selectedSide, onSelect, style }) {
  if (!row) return null
  const events = [...row.left, ...row.right]
  const isSelected = events.some(event => selectedChunk === event.item.id && selectedSide === event.side)
  const hasPending = events.some(event => !event.item.asr)
  const dotSide = row.left.length && row.right.length ? 'bundle' : row.right.length ? 'right' : 'left'
  return (
    <div className={`timeline-row event-row time-row ${isSelected ? 'selected' : ''}`} style={style}>
      <div className="timeline-side-slot">
        {row.left.map(event => <TimelineResultCard key={event.id} event={event} selectedChunk={selectedChunk} selectedSide={selectedSide} onSelect={onSelect} />)}
      </div>
      <div className="center-spine">
        <span className="event-time">{formatTime(row.stamp)}</span>
        <span className={`spine-dot ${dotSide} ${hasPending ? 'pending' : ''}`} />
        {!isLast && <i />}
      </div>
      <div className="timeline-side-slot">
        {row.right.map(event => <TimelineResultCard key={event.id} event={event} selectedChunk={selectedChunk} selectedSide={selectedSide} onSelect={onSelect} />)}
      </div>
    </div>
  )
}

createRoot(document.getElementById('root')).render(<StrictMode><App /></StrictMode>)

// Dev-only: surface service-item layout so we can spot real alignment issues
// from the browser console / via the ?probe=1 overlay without leaving the page.
if (import.meta.env.DEV) {
  setTimeout(() => {
    const items = document.querySelectorAll('.service-item')
    const lines = [`items: ${items.length}`]
    items.forEach((row, i) => {
      const r = row.getBoundingClientRect()
      const chips = row.querySelector('.slot-chips')?.getBoundingClientRect()
      const labels = row.querySelector('strong')?.textContent || ''
      lines.push(
        `[${i}] "${labels}"  h=${r.height.toFixed(1)}  chips-top=${chips ? (chips.top - r.top).toFixed(1) : 'NA'}`
      )
    })
    if (window.location.search.includes('probe=1')) {
      const pre = document.createElement('pre')
      pre.style.cssText = 'position:fixed;bottom:0;left:0;right:0;background:white;color:#000;padding:8px;z-index:9999;font:11px monospace;white-space:pre-wrap;border-top:2px solid red;'
      pre.textContent = lines.join('\n')
      document.body.appendChild(pre)
    }
    console.log('[probe]', lines.join('\n'))
  }, 1500)
}

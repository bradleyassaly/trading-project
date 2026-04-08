import { useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'

function relTime(ts) {
  if (!ts) return '?'
  const d = (Date.now() / 1000) - ts
  if (d < 60) return `${Math.round(d)}s ago`
  if (d < 3600) return `${Math.floor(d / 60)}m ago`
  if (d < 86400) return `${Math.floor(d / 3600)}h ago`
  return `${Math.floor(d / 86400)}d ago`
}

function relFuture(ts) {
  if (!ts) return '?'
  const d = ts - Date.now() / 1000
  if (d < 0) return 'overdue'
  if (d < 3600) return `in ${Math.floor(d / 60)}m`
  return `in ${Math.floor(d / 3600)}h ${Math.floor((d % 3600) / 60)}m`
}

function HealthBadge({ health }) {
  const styles = {
    healthy: 'bg-accent-green/20 text-accent-green',
    degraded: 'bg-yellow-900/60 text-yellow-300',
    critical: 'bg-accent-red/20 text-accent-red',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${styles[health] || 'bg-gray-700 text-gray-400'}`}>
      {(health || 'unknown').toUpperCase()}
    </span>
  )
}

function StepRow({ name, step }) {
  if (!step) return (
    <tr><td className="py-0.5 pr-2 text-gray-600">
      <span className="inline-block w-2 h-2 rounded-full bg-gray-700 mr-1.5" />{name}
    </td><td className="text-gray-700">—</td><td /><td /></tr>
  )
  const dot = step.status === 'ok' ? 'bg-accent-green' : step.status === 'failed' ? 'bg-accent-red' : 'bg-gray-600'
  return (
    <>
      <tr className="text-[10px]">
        <td className="py-0.5 pr-2 text-gray-400">
          <span className={`inline-block w-2 h-2 rounded-full ${dot} mr-1.5`} />{name}
        </td>
        <td className="pr-2 text-gray-500">{step.status?.toUpperCase()}</td>
        <td className="pr-2 text-gray-600">{step.duration_seconds}s</td>
        <td className="text-gray-600 truncate max-w-[200px]">{step.detail || ''}</td>
      </tr>
      {step.error && (
        <tr><td colSpan={4} className="pl-5 text-accent-red text-[9px] pb-1">{step.error}</td></tr>
      )}
    </>
  )
}

function PipelineCard({ data }) {
  const p = data?.pipeline || {}
  const steps = p.steps || {}
  const badge = p.last_run_success === true ? 'OK' : p.last_run_success === false ? 'FAILED' : p.overdue ? 'OVERDUE' : 'UNKNOWN'
  const badgeCls = badge === 'OK' ? 'bg-accent-green/20 text-accent-green' : badge === 'FAILED' ? 'bg-accent-red/20 text-accent-red' : 'bg-yellow-900/60 text-yellow-300'

  const [running, setRunning] = useState(false)
  const triggerRun = async () => {
    setRunning(true)
    try { await api.runPipeline() } catch (e) { console.error(e) }
    setTimeout(() => setRunning(false), 5000)
  }

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-gray-300">Daily Intelligence</h3>
        <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${badgeCls}`}>{badge}</span>
      </div>
      <div className="grid grid-cols-3 gap-2 text-[10px] text-gray-500 mb-3">
        <div>Last run: <span className="text-gray-300">{p.last_run_at ? relTime(p.last_run_at) : 'never'}</span></div>
        <div>Duration: <span className="text-gray-300">{p.last_run_duration ? `${p.last_run_duration}s` : '—'}</span></div>
        <div>Next: <span className="text-gray-300">{p.next_run_at ? relFuture(p.next_run_at) : '—'}</span></div>
      </div>
      <table className="w-full text-[10px] mb-3">
        <tbody>
          <StepRow name="data-fetch" step={steps.data_fetch} />
          <StepRow name="profile-rebuild" step={steps.profile_rebuild} />
          <StepRow name="classify-buckets" step={steps.classify_buckets} />
          <StepRow name="build-leaderboard" step={steps.build_leaderboard} />
          <StepRow name="refresh-universe" step={steps.refresh_universe} />
        </tbody>
      </table>
      <button onClick={triggerRun} disabled={running}
        className="text-[10px] px-3 py-1 rounded bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 disabled:opacity-50">
        {running ? 'Running...' : 'Run now'}
      </button>
    </div>
  )
}

function MonitorCard({ data }) {
  const m = data?.monitor || {}
  const running = m.running
  const badge = running ? 'RUNNING' : 'DOWN'
  const badgeCls = running ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
  const wsStale = m.websocket_stale

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-gray-300">Live Monitor</h3>
        <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${badgeCls}`}>{badge}</span>
      </div>
      <div className="space-y-1.5 text-[10px]">
        <div className="flex items-center gap-2">
          <span className={`w-2 h-2 rounded-full ${m.websocket_connected ? 'bg-accent-green' : 'bg-accent-red'}`} />
          <span className="text-gray-400">WebSocket: {m.websocket_connected ? 'Connected' : 'Disconnected'}</span>
          {wsStale && <span className="text-yellow-400">(stale {m.websocket_age_seconds}s)</span>}
        </div>
        <div className="text-gray-500">Signals today: <span className="text-gray-300">{m.signals_fired_today || 0}</span></div>
        <div className="text-gray-500">Restarts: <span className="text-gray-300">{m.restarts_today || 0}</span></div>
        <div className="text-gray-500">Uptime: <span className="text-gray-300">{m.uptime_seconds ? `${Math.floor(m.uptime_seconds / 3600)}h ${Math.floor((m.uptime_seconds % 3600) / 60)}m` : '—'}</span></div>
      </div>
    </div>
  )
}

function FilesCard({ data }) {
  const f = data?.files || {}
  const rows = [
    { name: 'market_universe.json', age: f.market_universe_age_hours, stale: f.market_universe_stale },
    { name: 'leaderboard', age: f.leaderboard_age_hours, stale: f.leaderboard_stale, extra: `v${f.leaderboard_version || 0}` },
  ]

  return (
    <div className="card">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Data Files</h3>
      <table className="w-full text-[10px]">
        <thead>
          <tr className="border-b border-surface-border text-gray-500">
            <th className="pb-1 text-left">File</th><th className="pb-1 text-right">Age</th><th className="pb-1 text-right">Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.name} className="text-gray-400">
              <td className="py-0.5">{r.name} {r.extra ? <span className="text-gray-600">({r.extra})</span> : null}</td>
              <td className="py-0.5 text-right">{r.age != null ? `${r.age.toFixed(1)}h` : '—'}</td>
              <td className="py-0.5 text-right">
                <span className={`px-1 py-0.5 rounded text-[8px] font-bold ${r.stale ? 'bg-yellow-900/60 text-yellow-300' : r.age != null ? 'bg-accent-green/20 text-accent-green' : 'bg-gray-700 text-gray-500'}`}>
                  {r.stale ? 'STALE' : r.age != null ? 'OK' : 'MISSING'}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function TelegramCard({ data }) {
  const alerts = data?.alerts || {}
  const configured = alerts.telegram_configured
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState(null)

  const sendTest = async () => {
    setTesting(true)
    setTestResult(null)
    try {
      const res = await fetch('/api/alerts/telegram-test', { method: 'POST' })
      const d = await res.json()
      setTestResult(d.success ? 'sent' : `failed: ${d.error}`)
    } catch (e) { setTestResult(`error: ${e.message}`) }
    setTesting(false)
  }

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-sm font-medium text-gray-300">Telegram Alerts</h3>
        <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${configured ? 'bg-accent-green/20 text-accent-green' : 'bg-gray-700 text-gray-500'}`}>
          {configured ? 'CONFIGURED' : 'NOT CONFIGURED'}
        </span>
      </div>
      {!configured ? (
        <div className="text-[10px] text-gray-500 space-y-1">
          <p>1. Message @BotFather on Telegram, create bot, copy token</p>
          <p>2. Message @userinfobot to get chat_id</p>
          <p>3. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env</p>
          <p>4. Restart API server</p>
        </div>
      ) : (
        <div className="space-y-2">
          <button onClick={sendTest} disabled={testing}
            className="text-[10px] px-3 py-1 rounded bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 disabled:opacity-50">
            {testing ? 'Sending...' : 'Send Test Message'}
          </button>
          {testResult && (
            <p className={`text-[10px] ${testResult === 'sent' ? 'text-accent-green' : 'text-accent-red'}`}>
              {testResult === 'sent' ? 'Message sent!' : testResult}
            </p>
          )}
        </div>
      )}
    </div>
  )
}

function RunsCard({ runs }) {
  const [expanded, setExpanded] = useState(null)
  if (!runs?.length) return (
    <div className="card">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Recent Runs</h3>
      <p className="text-[10px] text-gray-600">No pipeline runs recorded yet</p>
    </div>
  )

  return (
    <div className="card">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Recent Runs</h3>
      <div className="space-y-0.5">
        {runs.slice(0, 10).map((r, i) => (
          <div key={r.run_id || i} className="text-[10px] cursor-pointer hover:bg-surface-hover rounded px-1 py-0.5"
               onClick={() => setExpanded(expanded === i ? null : i)}>
            <div className="flex items-center gap-2">
              <span className="text-gray-500">{r.started_at ? relTime(r.started_at) : '?'}</span>
              <span className="text-gray-600">{r.duration_seconds}s</span>
              <span className="text-gray-600">{r.steps_completed}/{r.total_steps}</span>
              <span className={r.success ? 'text-accent-green' : 'text-accent-red'}>{r.success ? 'OK' : 'FAILED'}</span>
            </div>
            {expanded === i && r.error && (
              <p className="text-accent-red pl-4 mt-0.5">{r.error}</p>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

export default function PipelineMonitor() {
  const { data: status } = useApi(api.pipelineStatus, 30_000)
  const { data: runs } = useApi(api.pipelineRuns, 60_000)

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Pipeline Monitor</span></p>
          <h1 className="text-lg font-semibold text-gray-200">Pipeline Monitor</h1>
        </div>
        <HealthBadge health={status?.overall_health} />
      </div>

      <div className="flex gap-4">
        {/* Left: Component Status */}
        <div className="flex-[58] space-y-4">
          <PipelineCard data={status} />
          <MonitorCard data={status} />
          <FilesCard data={status} />
        </div>

        {/* Right: Alerts + Runs */}
        <div className="flex-[42] space-y-4">
          <TelegramCard data={status} />
          <RunsCard runs={runs} />
        </div>
      </div>
    </div>
  )
}

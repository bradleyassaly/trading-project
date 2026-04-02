import { useCallback, useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function DrawdownGauge({ value }) {
  // value is a fraction (e.g. -0.12 = 12% drawdown)
  if (value == null) {
    return (
      <div className="flex flex-col items-center justify-center w-32 h-32">
        <span className="text-gray-600 text-xs">No data</span>
      </div>
    )
  }

  const pct = Math.abs(Number(value)) * 100
  const capped = Math.min(pct, 30)
  const danger = pct >= 15
  const color = pct >= 20 ? '#ff4757' : pct >= 15 ? '#ffd32a' : '#00d084'

  // SVG circular gauge
  const R = 54
  const circumference = 2 * Math.PI * R
  const sweep = (capped / 30) * (circumference * 0.75)  // 270° arc

  return (
    <div className="flex flex-col items-center">
      <svg width="140" height="140" viewBox="0 0 140 140">
        {/* Track */}
        <circle
          cx="70" cy="70" r={R}
          fill="none" stroke="#2a2d3e" strokeWidth="10"
          strokeDasharray={`${circumference * 0.75} ${circumference * 0.25}`}
          strokeDashoffset={circumference * 0.125}
          strokeLinecap="round"
        />
        {/* Fill */}
        <circle
          cx="70" cy="70" r={R}
          fill="none" stroke={color} strokeWidth="10"
          strokeDasharray={`${sweep} ${circumference - sweep}`}
          strokeDashoffset={circumference * 0.125}
          strokeLinecap="round"
          style={{ transition: 'stroke-dasharray 0.5s, stroke 0.3s' }}
        />
        <text x="70" y="65" textAnchor="middle" fill={color} fontSize="20" fontWeight="bold" fontFamily="monospace">
          {pct.toFixed(1)}%
        </text>
        <text x="70" y="82" textAnchor="middle" fill="#6b7280" fontSize="10">
          drawdown
        </text>
        {danger && (
          <text x="70" y="96" textAnchor="middle" fill="#ffd32a" fontSize="9">
            ⚠ RED ZONE
          </text>
        )}
      </svg>
    </div>
  )
}

function DecisionTimeline({ entries }) {
  if (!entries || entries.length === 0) return <EmptyState title="No loop decisions" icon="⊛" />

  return (
    <div className="relative pl-6 space-y-4">
      {/* Vertical line */}
      <div className="absolute left-2 top-2 bottom-2 w-px bg-surface-border" />

      {[...entries].reverse().map((entry, i) => {
        const action = entry.action ?? entry.trigger ?? 'event'
        const ts = String(entry.timestamp ?? '').slice(0, 19).replace('T', ' ')
        const outcome = entry.outcome ?? entry.action_taken
        const isFirst = i === 0

        return (
          <div key={i} className="relative flex gap-3">
            {/* Dot */}
            <div className={`absolute -left-4 mt-1 w-2.5 h-2.5 rounded-full border-2 ${
              isFirst
                ? 'bg-accent-blue border-accent-blue'
                : 'bg-surface border-surface-border'
            }`} />

            <div className="space-y-0.5">
              <div className="flex items-center gap-2">
                <span className="text-accent-blue text-xs font-medium">{action}</span>
                <span className="text-gray-600 text-[10px] font-mono">{ts}</span>
              </div>
              {entry.reasoning && (
                <p className="text-xs text-gray-400">{String(entry.reasoning).slice(0, 120)}</p>
              )}
              {outcome && (
                <p className="text-[10px] text-gray-600">→ {String(outcome).slice(0, 80)}</p>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function AlertLog({ entries }) {
  const alerts = entries?.filter(e => e.alert || e.warning || e.error) ?? []
  if (alerts.length === 0) {
    return <p className="text-xs text-gray-600">No alerts.</p>
  }
  return (
    <ul className="space-y-1 text-xs">
      {alerts.map((a, i) => (
        <li key={i} className="flex gap-3">
          <span className="text-gray-600 font-mono">{String(a.timestamp ?? '').slice(11, 19)}</span>
          <span className="text-accent-yellow">{a.alert ?? a.warning ?? a.error}</span>
        </li>
      ))}
    </ul>
  )
}

const STATE_LABELS = {
  running:        'Running',
  stopped:        'Paused',
  trigger_pending:'Waiting for trigger',
  unknown:        'Unknown',
}

function Breadcrumb({ page }) {
  return (
    <p className="text-xs text-gray-600 mb-1">
      Trading Platform <span className="mx-1">›</span>
      <span className="text-gray-400">{page}</span>
    </p>
  )
}

export default function Control() {
  const [feedback, setFeedback] = useState(null)
  const [busy, setBusy] = useState(false)

  const statusFetcher = useCallback(() => api.systemStatus(), [])
  const pnlFetcher = useCallback(() => api.pnlSummary(), [])
  const decisionsFetcher = useCallback(() => api.loopDecisions(), [])

  const { data: status, refresh: refreshStatus } = useApi(statusFetcher, 10_000)
  const { data: pnl } = useApi(pnlFetcher, 30_000)
  const { data: decisions, loading: decisionsLoading } = useApi(decisionsFetcher, 15_000)

  const loopState = status?.loop_state ?? 'unknown'
  const isRunning = loopState === 'running'
  const isPending = loopState === 'trigger_pending'
  const stateLabel = STATE_LABELS[loopState] ?? loopState

  async function doControl(action) {
    setBusy(true)
    setFeedback(null)
    try {
      const result = await api.loopControl(action)
      setFeedback({ ok: result.success, msg: result.message })
      await refreshStatus()
    } catch (err) {
      setFeedback({ ok: false, msg: err.message })
    } finally {
      setBusy(false)
    }
  }

  const stateColor =
    isRunning  ? 'text-accent-green border-accent-green' :
    loopState === 'stopped' ? 'text-accent-red border-accent-red' :
    'text-accent-yellow border-accent-yellow'

  const stateBg =
    isRunning  ? 'bg-green-900/20' :
    loopState === 'stopped' ? 'bg-red-900/20' : 'bg-yellow-900/20'

  // Pulsing border only when trigger_pending
  const pulseClass = isPending ? 'animate-pulse' : ''

  return (
    <div className="p-6 space-y-6">
      <div>
        <Breadcrumb page="Loop Control" />
        <h1 className="text-lg font-semibold text-gray-200">Autonomous Loop Control</h1>
      </div>

      {/* Status + Gauge row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Large status */}
        <div className={`card col-span-1 flex flex-col items-center justify-center py-8 border-2 ${stateColor} ${stateBg} ${pulseClass}`}>
          <div className={`text-4xl font-bold tracking-wider mb-2 ${stateColor.split(' ')[0]}`}>
            {stateLabel.toUpperCase()}
          </div>
          <p className="text-xs text-gray-500">autonomous loop</p>
          {status?.last_run_timestamp && (
            <p className="text-xs text-gray-600 mt-2">
              Last run: {String(status.last_run_timestamp).slice(0, 19).replace('T', ' ')}
            </p>
          )}
        </div>

        {/* Drawdown gauge */}
        <div className="card col-span-1 flex flex-col items-center justify-center">
          <DrawdownGauge value={pnl?.max_drawdown} />
          <p className="text-xs text-gray-500 mt-1 text-center">
            Red zone: &gt;15%<br />
            Circuit breaker: &gt;20%
          </p>
        </div>

        {/* Control buttons */}
        <div className="card col-span-1 space-y-4">
          <p className="text-sm font-medium text-gray-400">Loop Controls</p>

          <div className="space-y-2">
            <button
              onClick={() => doControl('pause')}
              disabled={busy || !isRunning}
              className={`w-full btn-danger ${(busy || !isRunning) ? 'opacity-40 cursor-not-allowed' : ''}`}
            >
              ⏸ Pause Loop
            </button>
            <button
              onClick={() => doControl('resume')}
              disabled={busy || isRunning}
              className={`w-full btn-success ${(busy || isRunning) ? 'opacity-40 cursor-not-allowed' : ''}`}
            >
              ▶ Resume Loop
            </button>
            <button
              onClick={() => doControl('trigger_now')}
              disabled={busy}
              className={`w-full btn-primary ${busy ? 'opacity-40 cursor-not-allowed' : ''}`}
            >
              ⚡ Trigger Now
            </button>
          </div>

          {feedback && (
            <div className={`text-xs rounded-md px-3 py-2 ${
              feedback.ok ? 'bg-green-900/30 text-accent-green border border-green-800/40'
                          : 'bg-red-900/30 text-accent-red border border-red-800/40'
            }`}>
              {feedback.msg}
            </div>
          )}
        </div>
      </div>

      {/* Timeline + Alerts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Decision Timeline</h2>
          {decisionsLoading ? (
            <LoadingSkeleton rows={5} />
          ) : (
            <DecisionTimeline entries={decisions?.data} />
          )}
        </div>

        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Alert Log</h2>
          {decisionsLoading ? (
            <LoadingSkeleton rows={3} />
          ) : decisions?.available === false ? (
            <EmptyState title={decisions.reason} icon="⊛" />
          ) : (
            <AlertLog entries={decisions?.data} />
          )}
        </div>
      </div>
    </div>
  )
}

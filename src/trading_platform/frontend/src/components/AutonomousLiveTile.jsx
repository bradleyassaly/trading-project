import { useEffect, useState } from 'react'
import { useApi } from '../hooks/useApi'
import { api } from '../api/client'

// One-shot answer to "are we trading live yet, and if not — what's
// the binding gate?". Surfaces:
//   - Master switch + emergency-stop status
//   - Real-money trades fired in the last 30 days
//   - Top binding gate on the live surface (with a category breakdown
//     drilled in via /api/funnel/decisions/breakdown when gate is
//     LIVE_CAT_GATE — that's the most common blocker)
//   - Promotable signals and bankroll
export default function AutonomousLiveTile() {
  const { data: ladder } = useApi(api.ladderStatus, 60_000)
  const { data: readiness } = useApi(api.systemReadiness, 30_000)
  const { data: funnel } = useApi(() => api.decisionFunnel(24), 60_000)
  const [breakdown, setBreakdown] = useState(null)

  const liveGates = (funnel?.by_gate || []).filter(g => g.surface === 'live')
  const topLiveGate = liveGates[0]

  useEffect(() => {
    if (!topLiveGate?.gate) {
      setBreakdown(null)
      return
    }
    let cancelled = false
    api.decisionFunnelBreakdown(topLiveGate.gate, {
      surface: 'live', groupBy: 'category', limit: 8,
    }).then(r => { if (!cancelled) setBreakdown(r) })
      .catch(() => { if (!cancelled) setBreakdown(null) })
    return () => { cancelled = true }
  }, [topLiveGate?.gate])

  const live = ladder?.live || {}
  const realFired = live.submitted_30d || 0
  const bankroll = ladder?.bankroll_usd
  const promotable = ladder?.promotable_signals || []
  const status = readiness?.status || '—'
  const blockers = readiness?.blockers || []
  const warnings = readiness?.warnings || []

  // Master traffic light
  const trafficLight = realFired > 0
    ? { color: 'bg-accent-green', label: 'LIVE — autonomous trading active', tone: 'text-accent-green' }
    : status === 'READY'
      ? { color: 'bg-accent-yellow animate-pulse', label: 'ARMED — waiting for first signal to clear gates', tone: 'text-accent-yellow' }
      : { color: 'bg-accent-red', label: `BLOCKED — ${blockers[0] || 'system not ready'}`, tone: 'text-accent-red' }

  return (
    <div className="card border border-surface-border">
      <div className="flex items-center gap-3 mb-3">
        <span className={`inline-block w-3 h-3 rounded-full ${trafficLight.color}`} />
        <div className="flex-1">
          <h2 className="text-sm font-medium text-gray-200">Autonomous Live Trading</h2>
          <p className={`text-[11px] ${trafficLight.tone}`}>{trafficLight.label}</p>
        </div>
        <div className="text-right">
          <div className="text-[9px] text-gray-500">REAL TRADES (30d)</div>
          <div className={`text-lg font-mono font-bold ${realFired > 0 ? 'text-accent-green' : 'text-gray-500'}`}>
            {realFired}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-2 mb-3 text-[10px]">
        <div className="bg-surface-hover rounded p-2">
          <div className="text-[9px] text-gray-500">BANKROLL</div>
          <div className="font-mono text-gray-200">${bankroll != null ? bankroll.toLocaleString() : '—'}</div>
        </div>
        <div className="bg-surface-hover rounded p-2">
          <div className="text-[9px] text-gray-500">SYSTEM STATUS</div>
          <div className={`font-bold ${
            status === 'READY' ? 'text-accent-green' :
            status === 'DEGRADED' ? 'text-accent-yellow' : 'text-accent-red'
          }`}>{status}</div>
          {warnings.length > 0 && (
            <div className="text-[9px] text-accent-yellow mt-0.5">{warnings.length} warn</div>
          )}
        </div>
        <div className="bg-surface-hover rounded p-2">
          <div className="text-[9px] text-gray-500">LIVE ATTEMPTS (30d)</div>
          <div className="font-mono text-gray-200">{live.attempts_30d || 0}</div>
          <div className="text-[9px] text-gray-600">{live.closed_30d || 0} closed</div>
        </div>
        <div className="bg-surface-hover rounded p-2">
          <div className="text-[9px] text-gray-500">LIVE WIN RATE</div>
          <div className="font-mono text-gray-200">
            {live.wr_30d != null ? `${(live.wr_30d * 100).toFixed(0)}%` : '—'}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <div className="bg-surface-hover rounded p-2">
          <div className="text-[9px] text-gray-500 mb-1">PROMOTABLE SIGNALS</div>
          {promotable.length === 0 ? (
            <p className="text-[10px] text-gray-600">None yet — Bayesian gate hasn't cleared any signal.</p>
          ) : (
            <div className="space-y-0.5">
              {promotable.map(s => (
                <div key={s.signal_type} className="flex justify-between text-[10px]">
                  <span className="text-gray-300">{s.signal_type}</span>
                  <span className="font-mono text-gray-500">
                    n={s.n} · WR <span className="text-accent-green">{(s.wr * 100).toFixed(0)}%</span>
                    {' · '}
                    P {s.p_acc_ge_55 != null ? s.p_acc_ge_55.toFixed(2) : '—'}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="bg-surface-hover rounded p-2">
          <div className="flex items-baseline justify-between mb-1">
            <div className="text-[9px] text-gray-500">TOP BLOCKING GATE (LIVE · 24h)</div>
            {topLiveGate && (
              <span className="text-[9px] text-accent-red">
                {topLiveGate.rejected} rejected
              </span>
            )}
          </div>
          {!topLiveGate ? (
            <p className="text-[10px] text-gray-600">No live signals reached the executor.</p>
          ) : (
            <>
              <div className="font-mono text-[11px] text-gray-200">{topLiveGate.gate}</div>
              {breakdown?.buckets?.length > 0 && (
                <div className="mt-1 space-y-0.5 max-h-[88px] overflow-auto pr-1">
                  {breakdown.buckets.slice(0, 6).map(b => (
                    <div key={b.bucket} className="flex justify-between text-[9px]">
                      <span className="text-gray-400">{b.bucket}</span>
                      <span className="font-mono text-accent-red">{b.rejected}</span>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {blockers.length > 0 && (
        <div className="mt-2 p-2 rounded bg-accent-red/10 border border-accent-red/40 text-[10px] text-accent-red">
          <span className="font-semibold">Hard blockers:</span> {blockers.join(' · ')}
        </div>
      )}
    </div>
  )
}

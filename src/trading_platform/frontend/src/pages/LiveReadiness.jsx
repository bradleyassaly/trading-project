import { useState, useEffect } from 'react'
import { api } from '../api/client'
import LoadingSkeleton from '../components/LoadingSkeleton'

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`
  return `$${n.toFixed(0)}`
}

function fmtTimeAgo(ts) {
  if (!ts) return '—'
  const sec = Math.floor(Date.now() / 1000 - ts)
  if (sec < 60) return `${sec}s ago`
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`
  return `${Math.floor(sec / 86400)}d ago`
}

function fmtSignalName(s) {
  return (s || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

function StatusChip({ ok, label, off = 'Off' }) {
  return (
    <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${
      ok ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
    }`}>
      {ok ? '✓' : '✗'} {label}{!ok && off ? ` — ${off}` : ''}
    </span>
  )
}

export default function LiveReadiness() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [trades, setTrades] = useState([])
  const [dryRunResult, setDryRunResult] = useState(null)
  const [busy, setBusy] = useState(false)

  const refresh = async () => {
    setLoading(true)
    try {
      const [r, t] = await Promise.all([
        api.liveReadiness(),
        api.liveTrades(20),
      ])
      setData(r)
      setTrades(t.trades || [])
    } catch {
      setData(null)
    }
    setLoading(false)
  }

  useEffect(() => { refresh() }, [])

  const runDryRun = async (signalType) => {
    setBusy(true)
    setDryRunResult(null)
    try {
      const r = await api.liveTestDryRun(signalType)
      setDryRunResult(r)
      refresh()
    } catch (e) {
      setDryRunResult({ error: e.message })
    }
    setBusy(false)
  }

  const emergencyStop = async () => {
    if (!window.confirm('Activate emergency stop? All live trading will halt.')) return
    setBusy(true)
    try {
      await api.liveEmergencyStop({ reason: 'manual via GUI' })
      refresh()
    } catch {}
    setBusy(false)
  }

  const clearStop = async () => {
    setBusy(true)
    try {
      await api.liveClearStop()
      refresh()
    } catch {}
    setBusy(false)
  }

  if (loading && !data) return <div className="p-6"><LoadingSkeleton rows={6} /></div>
  if (!data || !data.available) return <div className="p-6 text-xs text-gray-500">Live readiness unavailable.</div>

  const sg = data.system_gates || {}
  const limits = data.kill_switch_limits || {}
  const signalGates = data.signal_gates || []

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Live Readiness</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Live Trading Readiness</h1>
      </div>

      {/* Master switch banner */}
      {!sg.master_switch && (
        <div className="card bg-accent-yellow/10 border-accent-yellow/40">
          <div className="text-[12px] text-accent-yellow font-medium">
            ⚠ Live trading is DISABLED
          </div>
          <div className="text-[10px] text-gray-400 mt-1">
            Set <code className="text-accent-blue">POLYMARKET_LIVE_ENABLED=1</code> in <code>.env</code> to enable.
            Even with the env var set, the kill switch must clear all gates per signal.
          </div>
        </div>
      )}

      {/* Emergency stop banner */}
      {data.emergency_stopped && (
        <div className="card bg-accent-red/10 border-accent-red/40">
          <div className="text-[12px] text-accent-red font-bold">🛑 EMERGENCY STOP ACTIVE</div>
          <div className="text-[10px] text-gray-400 mt-1">{data.stop_reason}</div>
          <button
            onClick={clearStop}
            disabled={busy}
            className="mt-2 px-3 py-1 rounded text-[10px] font-medium bg-accent-yellow/20 text-accent-yellow hover:bg-accent-yellow/30 disabled:opacity-50"
          >
            Clear Emergency Stop
          </button>
        </div>
      )}

      {/* System gates */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-300 mb-2">System Gates</h2>
        <div className="flex flex-wrap gap-2">
          <StatusChip ok={sg.master_switch}        label="POLYMARKET_LIVE_ENABLED" off="Not set" />
          <StatusChip ok={sg.clob_reachable}       label="CLOB Reachable" off="Unreachable" />
          <StatusChip ok={sg.clob_configured}      label="CLOB Credentials" off="Not configured" />
          <StatusChip ok={sg.wallet_set}           label="Wallet Address" off="Not set" />
          <StatusChip ok={sg.py_clob_client}       label="py-clob-client" off="Not installed" />
          <StatusChip ok={sg.emergency_stop_clear} label="Emergency Stop Clear" off="STOPPED" />
        </div>
        <div className="mt-3 flex items-center gap-2">
          <button
            onClick={() => runDryRun('price_velocity')}
            disabled={busy}
            className="px-3 py-1 rounded text-[10px] font-medium bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 disabled:opacity-50"
          >
            🧪 Test Dry Run (price_velocity)
          </button>
          <button
            onClick={emergencyStop}
            disabled={busy || data.emergency_stopped}
            className="px-3 py-1 rounded text-[10px] font-medium bg-accent-red/20 text-accent-red hover:bg-accent-red/30 disabled:opacity-50"
          >
            🛑 Emergency Stop
          </button>
          <button
            onClick={refresh}
            disabled={busy}
            className="px-3 py-1 rounded text-[10px] font-medium bg-surface-card text-gray-400 hover:bg-surface-hover disabled:opacity-50"
          >
            🔄 Refresh
          </button>
          <span className="text-[10px] text-gray-500 ml-auto">
            {data.n_ready} of {signalGates.length} signals ready · {sg.live_trades_executed || 0} live trades executed
          </span>
        </div>
        {dryRunResult && (
          <div className="mt-2 text-[10px] font-mono text-gray-400 bg-surface-bg p-2 rounded">
            <div>success: {String(dryRunResult.success)}</div>
            <div>mode: {dryRunResult.mode || '—'}</div>
            <div>size_usd: ${dryRunResult.size_usd ?? '—'}</div>
            {dryRunResult.reason && <div className="text-accent-yellow">reason: {dryRunResult.reason}</div>}
          </div>
        )}
      </div>

      {/* Signal readiness table */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-300 mb-2">Signal Readiness</h2>
        {signalGates.length === 0 ? (
          <p className="text-[10px] text-gray-500">No signals have resolved trades yet.</p>
        ) : (
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-1">Signal</th>
                <th className="pb-1 text-right">Resolved</th>
                <th className="pb-1 text-right">EV</th>
                <th className="pb-1 text-right">Win Rate</th>
                <th className="pb-1 text-right">Kelly Size</th>
                <th className="pb-1 text-center">Sample</th>
                <th className="pb-1 text-center">EV+</th>
                <th className="pb-1 text-center">WR</th>
                <th className="pb-1">Ready</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {signalGates.map(s => {
                const g = s.gates || {}
                const cell = (ok) => ok ? <span className="text-accent-green">✓</span> : <span className="text-accent-red">✗</span>
                return (
                  <tr key={s.signal_type} className={s.ready ? 'bg-accent-green/5' : ''}>
                    <td className="py-1.5 text-gray-300">{fmtSignalName(s.signal_type)}</td>
                    <td className="py-1.5 text-right font-mono text-gray-400">{s.n_resolved}</td>
                    <td className={`py-1.5 text-right font-mono ${(s.ev || 0) > 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {s.ev != null ? `${s.ev >= 0 ? '+' : ''}${(s.ev * 100).toFixed(1)}%` : '—'}
                    </td>
                    <td className={`py-1.5 text-right font-mono ${
                      s.win_rate == null ? 'text-gray-500' :
                      s.win_rate >= 0.55 ? 'text-accent-green' :
                      s.win_rate < 0.45 ? 'text-accent-red' : 'text-accent-yellow'
                    }`}>
                      {s.win_rate != null ? `${(s.win_rate * 100).toFixed(0)}%` : '—'}
                    </td>
                    <td className="py-1.5 text-right font-mono text-gray-300">
                      {s.kelly_recommended_usd != null ? fmtUsd(s.kelly_recommended_usd) : '—'}
                    </td>
                    <td className="py-1.5 text-center">{cell(g.sample_size)}</td>
                    <td className="py-1.5 text-center">{cell(g.ev_positive)}</td>
                    <td className="py-1.5 text-center">{cell(g.win_rate_ok)}</td>
                    <td className="py-1.5">
                      <span className={`px-1.5 py-0.5 rounded text-[9px] ${
                        s.ready ? 'bg-accent-green/20 text-accent-green' : 'bg-surface-bg text-gray-500'
                      }`}>{s.ready ? 'READY' : 'BLOCKED'}</span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Kill switch limits */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-300 mb-2">Kill Switch Limits</h2>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[10px]">
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Max Trade</div>
            <div className="text-gray-200 font-mono">{fmtUsd(limits.max_trade_usd)}</div>
          </div>
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Max Daily Loss</div>
            <div className="text-gray-200 font-mono">{((limits.max_daily_loss_pct || 0) * 100).toFixed(0)}%</div>
          </div>
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Max Open</div>
            <div className="text-gray-200 font-mono">{limits.max_open_positions}</div>
          </div>
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Min Win Rate</div>
            <div className="text-gray-200 font-mono">{((limits.min_win_rate || 0) * 100).toFixed(0)}%</div>
          </div>
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Min Sample (hard)</div>
            <div className="text-gray-200 font-mono">{limits.min_resolved_hard}</div>
          </div>
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Preferred Sample</div>
            <div className="text-gray-200 font-mono">{limits.preferred_min_resolved}</div>
          </div>
          <div className="bg-surface-card rounded p-2">
            <div className="text-gray-500">Bankroll</div>
            <div className="text-gray-200 font-mono">{fmtUsd(limits.bankroll)}</div>
          </div>
        </div>
        <p className="text-[9px] text-gray-600 mt-2">
          Limits are constants in <code>kill_switch.py</code>. Edit <code>KillSwitch.MAX_TRADE_USD</code> etc. to change.
        </p>
      </div>

      {/* Live trade audit log */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-300 mb-2">Live Trade Audit ({trades.length})</h2>
        {trades.length === 0 ? (
          <p className="text-[10px] text-gray-500">
            No live trades attempted yet. Dry runs and real submissions will appear here.
          </p>
        ) : (
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-1">Time</th>
                <th className="pb-1">Signal</th>
                <th className="pb-1">Direction</th>
                <th className="pb-1 text-right">Size</th>
                <th className="pb-1 text-right">Entry</th>
                <th className="pb-1">Status</th>
                <th className="pb-1">Mode</th>
                <th className="pb-1">Market</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {trades.map(t => (
                <tr key={t.id}>
                  <td className="py-1 text-gray-500">{fmtTimeAgo(t.attempted_at)}</td>
                  <td className="py-1 text-gray-300">{t.signal_type}</td>
                  <td className="py-1 text-gray-400">{t.direction}</td>
                  <td className="py-1 text-right font-mono text-gray-300">{fmtUsd(t.size_usd)}</td>
                  <td className="py-1 text-right font-mono text-gray-300">{t.entry_price != null ? (t.entry_price * 100).toFixed(0) + '%' : '—'}</td>
                  <td className="py-1">
                    <span className={`px-1 py-0.5 rounded text-[9px] ${
                      t.status === 'submitted' || t.status === 'live' || t.status === 'matched' ? 'bg-accent-green/20 text-accent-green' :
                      t.status === 'blocked' ? 'bg-accent-red/20 text-accent-red' :
                      t.status === 'dry_run' ? 'bg-accent-blue/20 text-accent-blue' :
                      'bg-surface-bg text-gray-500'
                    }`}>{t.status || '—'}</span>
                  </td>
                  <td className="py-1 text-gray-500">{t.dry_run ? 'DRY' : 'LIVE'}</td>
                  <td className="py-1 text-gray-400 truncate max-w-[260px]">{t.question}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

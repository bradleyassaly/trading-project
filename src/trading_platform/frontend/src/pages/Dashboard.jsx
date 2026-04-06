import { useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'
import { SignalBadge, TierBadge } from '../components/Badges'

function relTime(ts) {
  if (!ts) return '?'
  const d = (Date.now() / 1000) - (typeof ts === 'string' ? new Date(ts).getTime() / 1000 : ts)
  if (d < 60) return '<1m'
  if (d < 3600) return `${Math.floor(d / 60)}m`
  if (d < 86400) return `${Math.floor(d / 3600)}h`
  return `${Math.floor(d / 86400)}d`
}

// ── Status Bar ──────────────────────────────────────────────────────────────
function StatusBar({ status }) {
  const connected = status?.connected
  const markets = status?.markets_subscribed || 0
  const t1 = status?.tier1_wallets || 0
  const t2 = status?.tier2_wallets || 0
  const signals = status?.signals_today || 0
  const lastTs = status?.last_event_ts || 0
  const lastAge = lastTs ? relTime(lastTs) : '—'

  return (
    <div className="bg-surface-card border border-surface-border rounded px-4 py-2 flex items-center justify-between text-xs">
      <div className="flex items-center gap-2">
        <span className={`inline-block w-2 h-2 rounded-full ${connected ? 'bg-accent-green animate-pulse' : 'bg-accent-red'}`} />
        <span className="text-gray-400">
          WebSocket: <span className={connected ? 'text-accent-green' : 'text-accent-red'}>{connected ? 'CONNECTED' : 'DOWN'}</span>
          <span className="text-gray-600 ml-2">| {markets} markets</span>
        </span>
      </div>
      <span className="text-gray-500">{t1} tier1 · {t2} tier2 wallets watched</span>
      <span className="text-gray-500">{signals} signals today · last event {lastAge}</span>
    </div>
  )
}

// ── Stat Cards ──────────────────────────────────────────────────────────────
function StatCards({ whaleFeed, health, status }) {
  const alerts = whaleFeed?.data ?? []
  const todayStart = Math.floor(new Date().setUTCHours(0, 0, 0, 0) / 1000)
  const todayAlerts = alerts.filter(a => (a.fired_at || 0) >= todayStart)
  const t1Today = todayAlerts.filter(a => a.tier === 1).length
  const t2Today = todayAlerts.filter(a => a.tier === 2).length

  const lr = health?.live_readiness || {}
  const gates = [lr.gate_1_resolved_trades, lr.gate_2_categories_with_edge, lr.gate_3_max_drawdown, lr.gate_4_human_approval, lr.gate_5_capital_allocated]
  const passed = gates.filter(g => g?.passed).length

  const paper = health?.paper_trading || {}

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">WHALE DETECTIONS TODAY</p>
        <p className="text-xl font-bold text-gray-200">{todayAlerts.length}</p>
        <p className="text-[9px] text-gray-500">tier1: {t1Today} · tier2: {t2Today}</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">SIGNALS FIRED TODAY</p>
        <p className="text-xl font-bold text-gray-200">{status?.signals_today || 0}</p>
        <p className="text-[9px] text-gray-500">building data</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">PAPER P&L</p>
        <p className={`text-xl font-bold font-mono ${(paper.bankroll_current || 500) >= 500 ? 'text-accent-green' : 'text-accent-red'}`}>
          ${(paper.bankroll_current || 500).toFixed(0)}
        </p>
        <p className="text-[9px] text-gray-500">{paper.open_positions || 0} positions open</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">LIVE READINESS</p>
        <p className="text-xl font-bold text-gray-200">{passed} / 5 gates</p>
        <div className="w-full bg-gray-800 rounded-full h-1.5 mt-1">
          <div className="h-1.5 rounded-full bg-accent-blue" style={{ width: `${(passed / 5) * 100}%` }} />
        </div>
        <Link to="/live" className="text-[9px] text-accent-blue hover:underline mt-1 inline-block">View gates</Link>
      </div>
    </div>
  )
}

// ── Whale Feed ──────────────────────────────────────────────────────────────
function WhaleFeed({ data, loading }) {
  const [expanded, setExpanded] = useState(null)
  const alerts = data?.data ?? []

  if (loading && !data) return <LoadingSkeleton rows={6} />

  if (!alerts.length) return (
    <EmptyState
      title="No whale activity detected"
      message={`Monitoring markets across 9 categories`}
    />
  )

  return (
    <div className="space-y-1.5">
      {alerts.slice(0, 20).map((a, i) => {
        const isBuy = a.side === 'BUY'
        return (
          <div key={`${a.wallet}-${a.fired_at}-${i}`}
            className={`bg-surface-card border border-surface-border rounded cursor-pointer hover:bg-surface-hover border-l-[3px] ${isBuy ? 'border-accent-green' : 'border-accent-red'}`}
            onClick={() => setExpanded(expanded === i ? null : i)}
          >
            <div className="px-3 py-2">
              <div className="flex items-center justify-between mb-0.5">
                <div className="flex items-center gap-2">
                  <TierBadge tier={a.tier} />
                  <span className="font-mono text-[10px] text-gray-400">{a.wallet}</span>
                </div>
                <span className="text-[10px] text-gray-600">{a.time_ago}</span>
              </div>
              <div className="flex items-center gap-2 mb-0.5">
                <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${isBuy ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'}`}>
                  {a.side}
                </span>
                <span className="text-xs text-gray-300 truncate max-w-[400px]">{a.question || '—'}</span>
                <span className="px-1.5 py-0.5 rounded bg-surface-hover text-[9px] text-gray-400">{a.category}</span>
              </div>
              {a.signal_fired
                ? <span className="text-[10px] text-accent-green">→ Signal fired · conf {a.directional_win_rate ? (a.directional_win_rate * 100).toFixed(0) + '%' : '—'}</span>
                : <span className="text-[10px] text-gray-600">→ monitoring</span>
              }
            </div>
            {expanded === i && (
              <div className="px-3 pb-2 border-t border-surface-border text-[10px] text-gray-500 space-y-0.5">
                <p>Full: {a.question}</p>
                <p>Wallet: {a.wallet_full || a.wallet}</p>
                {a.directional_win_rate != null && <p>Win rate: {(a.directional_win_rate * 100).toFixed(1)}%</p>}
                {a.size != null && <p>Size: ${Number(a.size).toFixed(0)} · Price: {Number(a.price || 0).toFixed(3)}</p>}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ── Signal Performance Table ────────────────────────────────────────────────
function SignalPerfTable({ data }) {
  const types = data?.by_type ?? []

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-2">Signal Performance</h2>
      <table className="w-full text-[10px]">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-1 pr-2">Signal</th>
            <th className="pb-1 pr-2 text-right">Fired</th>
            <th className="pb-1 pr-2 text-right">WR</th>
            <th className="pb-1">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {types.map(t => {
            const statusCls = {
              active: 'bg-accent-green/20 text-accent-green',
              building: 'bg-gray-700 text-gray-400',
              monitoring: 'bg-yellow-900/60 text-yellow-300',
              underperforming: 'bg-accent-red/20 text-accent-red',
            }[t.status] || 'bg-gray-700 text-gray-400'
            return (
              <tr key={t.signal_type} className="hover:bg-surface-hover">
                <td className="py-1 pr-2"><SignalBadge type={t.signal_type} /></td>
                <td className="py-1 pr-2 text-right text-gray-400">{t.fired || 0}</td>
                <td className="py-1 pr-2 text-right font-mono text-gray-500">{t.win_rate != null ? `${(t.win_rate * 100).toFixed(0)}%` : '—'}</td>
                <td className="py-1"><span className={`px-1.5 py-0.5 rounded text-[8px] font-semibold ${statusCls}`}>{t.status}</span></td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ── Main Dashboard ──────────────────────────────────────────────────────────
export default function Dashboard() {
  const { data: status } = useApi(api.polymarketSubscriptionStatus, 30_000)
  const { data: whaleFeed, loading: feedL } = useApi(api.polymarketWhaleFeed, 10_000)
  const { data: health } = useApi(api.intelligenceHealth, 60_000)
  const { data: sigPerf } = useApi(api.signalsPerformance, 60_000)

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Command Center</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Command Center</h1>
      </div>

      <StatusBar status={status} />
      <StatCards whaleFeed={whaleFeed} health={health} status={status} />

      <div className="flex gap-4">
        {/* Left: Whale Feed */}
        <div className="flex-[65] space-y-2">
          <div className="flex items-center gap-2 mb-1">
            <h2 className="text-sm font-medium text-gray-300">Whale Activity</h2>
            <span className="flex items-center gap-1 text-[9px] text-gray-500">
              <span className="inline-block w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" />
              live · 10s
            </span>
          </div>
          <WhaleFeed data={whaleFeed} loading={feedL} />
        </div>

        {/* Right: Signal Intelligence */}
        <div className="flex-[35] space-y-4">
          <div className="card">
            <SignalPerfTable data={sigPerf} />
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-2">Recent Alerts</h2>
            {!(whaleFeed?.data?.length) ? (
              <p className="text-[10px] text-gray-600">Alerts appear when watched wallets trade</p>
            ) : (
              <div className="space-y-1">
                {(whaleFeed.data || []).slice(0, 5).map((a, i) => (
                  <div key={i} className="flex items-center gap-1.5 text-[10px] py-0.5">
                    <TierBadge tier={a.tier} />
                    <span className="font-mono text-[9px] text-gray-500">{a.wallet}</span>
                    <span className="px-1 py-0.5 rounded bg-surface-hover text-[8px] text-gray-500">{a.category}</span>
                    <span className="text-gray-600 ml-auto">{a.time_ago}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

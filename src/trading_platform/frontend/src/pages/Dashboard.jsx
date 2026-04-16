import { useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'
import { SignalBadge, TierBadge } from '../components/Badges'
import AccuracySparkline from '../components/charts/AccuracySparkline'
import CumulativePnLChart from '../components/charts/CumulativePnLChart'
import { fmtUsd, fmtPct, fmtRelTime } from '../lib/format'

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
function StatCards({ whaleFeed, health, status, bankroll, cb, universe, leaderboard }) {
  const alerts = whaleFeed?.data ?? []
  const todayStart = Math.floor(new Date().setUTCHours(0, 0, 0, 0) / 1000)
  const todayAlerts = alerts.filter(a => (a.fired_at || 0) >= todayStart)
  const t1Today = todayAlerts.filter(a => a.tier === 1).length
  const t2Today = todayAlerts.filter(a => a.tier === 2).length

  const lr = health?.live_readiness || {}
  const gates = [lr.gate_1_resolved_trades, lr.gate_2_categories_with_edge, lr.gate_3_max_drawdown, lr.gate_4_human_approval, lr.gate_5_capital_allocated]
  const passed = gates.filter(g => g?.passed).length

  // Wallet counts. universe-stats exposes total_wallets across the
  // entire profiles table (~16k); the winners endpoint returns the
  // tier-tagged leaderboard subset and IS the right source for tier
  // breakdown. The smart-money/leaderboard endpoint does NOT include
  // the tier field — that was the bug. Now we read from winners.
  const totalWallets = universe?.total_wallets ?? 0
  const winnerRows = leaderboard?.data ?? []
  const tier1h = winnerRows.filter(r => r.tier === 'tier1h').length
  const tier1 = winnerRows.filter(r => r.tier === 'tier1').length
  const tier2 = winnerRows.filter(r => r.tier === 'tier2').length
  const lbTotal = tier1h + tier1 + tier2

  // Bankroll — paper/bankroll endpoint gives current equity vs starting
  const equity = bankroll?.current_equity ?? bankroll?.bankroll ?? 10000
  const starting = bankroll?.starting_capital ?? 10000
  const equityPnl = equity - starting

  // Drawdown gauge from circuit breaker
  const ddPct = (cb?.current_drawdown_pct ?? 0) * 100
  const ddMax = (cb?.max_drawdown_pct ?? 0.20) * 100
  const ddColor = ddPct < ddMax * 0.5 ? 'text-accent-green' : ddPct < ddMax * 0.75 ? 'text-yellow-400' : 'text-accent-red'
  const halted = cb?.is_halted

  return (
    <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">TRACKED WALLETS</p>
        <p className="text-xl font-bold text-gray-200">{totalWallets.toLocaleString()}</p>
        <p className="text-[9px] text-gray-500">leaderboard {lbTotal} · t1h:{tier1h} t1:{tier1} t2:{tier2}</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">PAPER EQUITY</p>
        <p className={`text-xl font-bold font-mono ${equityPnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
          ${equity.toLocaleString(undefined, {maximumFractionDigits: 0})}
        </p>
        <p className="text-[9px] text-gray-500">{equityPnl >= 0 ? '+' : ''}${equityPnl.toFixed(0)} since start</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">WHALE ALERTS TODAY</p>
        <p className="text-xl font-bold text-gray-200">{todayAlerts.length}</p>
        <p className="text-[9px] text-gray-500">tier1:{t1Today} · tier2:{t2Today}</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">DRAWDOWN</p>
        <p className={`text-xl font-bold font-mono ${ddColor}`}>{ddPct.toFixed(1)}%</p>
        <div className="w-full bg-gray-800 rounded-full h-1.5 mt-1">
          <div className={`h-1.5 rounded-full ${ddPct < ddMax * 0.75 ? 'bg-accent-blue' : 'bg-accent-red'}`} style={{ width: `${Math.min(100, (ddPct / ddMax) * 100)}%` }} />
        </div>
        <p className="text-[9px] text-gray-500">{halted ? '🛑 HALTED' : `of ${ddMax.toFixed(0)}% max`}</p>
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

// Synthetic "wallets" from technical scanners — not real wallet activity.
const SYNTHETIC_WALLETS = new Set(['velocity_detector', 'order_book_monitor'])

// Sports markets are high-noise in feeds. Filter from display (not from trading).
const SPORTS_PATTERNS = ['vs.', 'vs ', 'O/U ', 'Spread:', 'NBA', 'NFL', 'NHL', 'MLB',
  'Premier League', 'La Liga', 'Serie A', 'Bundesliga', 'Champions League',
  'UFC ', 'ATP ', 'WTA ', 'Winner -', 'winner -']
function isSportsMarket(item) {
  if (!item) return false
  const cat = String(item.category || '').toLowerCase()
  if (cat === 'sports' || cat === 'soccer' || cat === 'basketball') return true
  const q = String(item.question || item.market_title || '')
  return SPORTS_PATTERNS.some(p => q.includes(p))
}

// ── Whale Feed ──────────────────────────────────────────────────────────────
function WhaleFeed({ data, loading }) {
  const [expanded, setExpanded] = useState(null)
  const alerts = (data?.data ?? []).filter(a => !SYNTHETIC_WALLETS.has(a.wallet) && !isSportsMarket(a))

  if (loading && !data) return <LoadingSkeleton rows={6} />

  if (!alerts.length) {
    const marketsMsg = data?.markets_subscribed
      ? `Monitoring ${data.markets_subscribed} markets`
      : 'Monitoring markets'
    return (
      <EmptyState
        title="No whale activity detected"
        message={marketsMsg}
      />
    )
  }

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

// Signal types fired by the wallet pipeline (real watched-wallet trades).
const WHALE_SIG_TYPES = new Set([
  'wallet_reversal', 'cascade', 'oversized_bet', 'accumulation',
  'convergence', 'specialist_entry', 'pre_deadline_surge',
  'whale_entry', 'whale_exit', 'no_position_entry', 'position_reduction',
])
// Signal types fired by technical scanners (no wallet basis).
const TECH_SIG_TYPES = new Set(['price_velocity', 'market_maker_flip'])

// ── Signal Performance Table ────────────────────────────────────────────────
function SignalPerfTable({ data, status }) {
  const types = data?.by_type ?? []
  const whaleRows = types.filter(t => WHALE_SIG_TYPES.has(t.signal_type))
  const techRows = types.filter(t => TECH_SIG_TYPES.has(t.signal_type))
  const otherRows = types.filter(t => !WHALE_SIG_TYPES.has(t.signal_type) && !TECH_SIG_TYPES.has(t.signal_type))

  const renderRow = (t) => {
    const statusCls = {
      live: 'bg-accent-green/20 text-accent-green',
      active: 'bg-accent-green/20 text-accent-green',
      building: 'bg-gray-700 text-gray-400',
      monitoring: 'bg-yellow-900/60 text-yellow-300',
      weak: 'bg-yellow-900/60 text-yellow-300',
      underperforming: 'bg-accent-red/20 text-accent-red',
      disabled: 'bg-gray-800 text-gray-600',
    }[t.status] || 'bg-gray-700 text-gray-400'
    return (
      <tr key={t.signal_type} className="hover:bg-surface-hover">
        <td className="py-1 pr-2"><SignalBadge type={t.signal_type} /></td>
        <td className="py-1 pr-2 text-right text-gray-400">{t.fired || 0}</td>
        <td className="py-1 pr-2 text-right font-mono text-gray-500">{t.fired > 0 && t.win_rate != null ? `${(t.win_rate * 100).toFixed(0)}%` : '—'}</td>
        <td className="py-1"><span className={`px-1.5 py-0.5 rounded text-[8px] font-semibold ${statusCls}`}>{t.status}</span></td>
      </tr>
    )
  }

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-2">🧠 Wallet Signal Performance</h2>
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
          {whaleRows.length ? whaleRows.map(renderRow) : (
            <tr><td colSpan={4} className="py-2 text-[10px] text-gray-600 text-center">
              No whale signals fired yet{status?.markets_subscribed ? ` — live-collect monitoring ${status.markets_subscribed} markets / ${status.watched_wallets ?? 0} wallets` : ''}.
            </td></tr>
          )}
          {otherRows.map(renderRow)}
        </tbody>
      </table>
      {techRows.length > 0 && (
        <details className="mt-3">
          <summary className="text-[10px] text-gray-600 cursor-pointer hover:text-gray-400">📊 Technical Scanners ({techRows.length})</summary>
          <table className="w-full text-[10px] mt-2 opacity-70">
            <tbody className="divide-y divide-surface-border">{techRows.map(renderRow)}</tbody>
          </table>
        </details>
      )}
    </div>
  )
}

// ── Thesis Scorecard ────────────────────────────────────────────────────────
function ThesisScorecard({ data, history }) {
  // Support both /api/thesis/claims (scorecard, claim_1, ...)
  // and /api/thesis/scorecard (thesis_scorecard, claim_1_persistence, ...)
  const sc = data?.thesis_scorecard ?? data?.scorecard ?? {}
  const c1 = data?.claim_1_persistence ?? data?.claim_1 ?? {}
  const c2 = data?.claim_2_category ?? data?.claim_2 ?? {}
  const c3 = data?.claim_3_identification ?? data?.claim_3 ?? {}
  const c4 = data?.claim_4_timing ?? data?.claim_4 ?? {}
  const c5 = data?.claim_5_execution ?? data?.claim_5 ?? {}

  const total = sc.total_hypotheses ?? 0
  const correct = sc.correct ?? 0
  const accuracy = sc.accuracy
  const target = 0.70
  const sample_target = 50
  const accPct = accuracy != null ? Math.round(accuracy * 100) : null
  const accBarPct = accuracy != null ? Math.min(100, accuracy * 100) : 0

  // Verdict color
  const verdict = sc.verdict || ''
  const verdictColor =
    verdict.startsWith('✅') ? 'text-accent-green' :
    verdict.startsWith('🟡') ? 'text-yellow-400' :
    verdict.startsWith('⚠') ? 'text-yellow-500' :
    verdict.startsWith('🔴') ? 'text-accent-red' :
    'text-gray-400'

  // Claim cards
  const claims = [
    {
      n: 1, label: 'Persistent Edge',
      status: c1.persistence === 'confirmed' ? '✅' : c1.persistence === 'degrading' ? '🔴' : '🟡',
      detail: c1.clean_cohort_wr != null
        ? `Cohort WR ${(c1.clean_cohort_wr * 100).toFixed(1)}% (${(c1.clean_cohort_trades || 0).toLocaleString()} trades)`
        : 'No data',
      sub: c1.rolling_30d_wr != null ? `30d: ${(c1.rolling_30d_wr * 100).toFixed(0)}%` : '',
    },
    {
      n: 2, label: 'Category-Specific',
      status: (c2.categories_with_edge || 0) >= 3 ? '✅' : '🟡',
      detail: `${c2.categories_with_edge || 0} categories with edge`,
      sub: c2.strongest_category ? `top: ${c2.strongest_category}` : '',
    },
    {
      n: 3, label: 'Identification',
      status: (c3.distinct_copyable_wallets || 0) >= 20 ? '✅' : '🟡',
      detail: `${c3.distinct_copyable_wallets || 0} copyable wallets`,
      sub: `${c3.copyable_combos || 0}/${c3.total_scored || 0} combos (${Math.round((c3.selectivity || 0) * 100)}% selective)`,
    },
    {
      n: 4, label: 'Real-Time Copy',
      status: (c4.alpha_gated_trades || 0) > 0 ? '🔄' : '⏳',
      detail: `${c4.alpha_gated_trades || 0} alpha-gated trades`,
      sub: `${c4.signals_skipped || 0} signals skipped`,
    },
    {
      n: 5, label: 'Transaction Costs',
      status: '⏳',
      detail: 'Not yet tested',
      sub: 'requires live trading',
    },
  ]

  return (
    <div className="space-y-3">
      {/* Hero scorecard */}
      <div className="bg-surface-card border border-surface-border rounded-lg p-5">
        <div className="flex items-baseline justify-between mb-2">
          <h2 className="text-xs uppercase tracking-wide text-gray-500">Hypothesis Accuracy</h2>
          <span className="text-[10px] text-gray-600">target: {Math.round(target * 100)}% on {sample_target}+ trades</span>
        </div>
        <div className="flex items-end gap-4 mb-2">
          <div className="flex items-baseline gap-3">
            <span className="text-4xl font-bold font-mono text-gray-200">
              {accPct != null ? `${accPct}%` : '—'}
            </span>
            <span className="text-sm text-gray-500">
              ({correct}/{total} correct)
            </span>
          </div>
          {/* 60-day accuracy trend — see THESIS.md for the target line. */}
          <div className="flex-1 min-w-[120px]">
            <AccuracySparkline snapshots={history?.snapshots} target={target} />
          </div>
        </div>
        <div className="w-full bg-gray-800 rounded-full h-2 mb-3 relative">
          <div
            className={`h-2 rounded-full transition-all ${accuracy != null && accuracy >= target ? 'bg-accent-green' : 'bg-accent-blue'}`}
            style={{ width: `${accBarPct}%` }}
          />
          {/* Target line at 70% */}
          <div className="absolute top-0 h-2 border-l border-dashed border-gray-500" style={{ left: `${target * 100}%` }} />
        </div>
        <div className="flex items-baseline gap-4">
          <span className={`text-sm font-semibold ${verdictColor}`}>{verdict || '—'}</span>
          {sc.trend && (
            <span className="text-[10px] text-gray-500">trend: {sc.trend}</span>
          )}
          {sc.recent_20_accuracy != null && sc.prior_20_accuracy != null && (
            <span className="text-[10px] text-gray-500">
              recent 20: {Math.round(sc.recent_20_accuracy * 100)}% · prior 20: {Math.round(sc.prior_20_accuracy * 100)}%
            </span>
          )}
        </div>
      </div>

      {/* Five claim cards */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-2">
        {claims.map(c => (
          <div key={c.n} className="bg-surface-card rounded p-2.5 border border-surface-border">
            <div className="flex items-center gap-1.5 mb-1">
              <span className="text-base">{c.status}</span>
              <span className="text-[9px] uppercase tracking-wide text-gray-500">Claim {c.n}</span>
            </div>
            <p className="text-[11px] font-semibold text-gray-300 mb-1">{c.label}</p>
            <p className="text-[10px] text-gray-400 leading-snug">{c.detail}</p>
            {c.sub && <p className="text-[9px] text-gray-600 mt-0.5">{c.sub}</p>}
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Recent Hypotheses ───────────────────────────────────────────────────────
// Shows the last few thesis-driven paper trades. Resolved hypotheses are
// tagged ✅/❌ so the operator can eyeball whether the framework is
// being validated in real time.
function PipelineFunnelTile({ data }) {
  if (!data || !data.available) {
    return <div className="text-[10px] text-gray-600">Loading funnel…</div>
  }
  const t = data.totals || {}
  const fires = t.signals_fired || 0
  const placed = t.paper_trades_placed || 0
  const conv = Math.round((t.conversion_rate || 0) * 100)
  const convColor = conv >= 2 ? 'text-accent-green' : conv >= 1 ? 'text-accent-orange' : 'text-accent-red'
  const bySignal = (data.by_signal || []).slice(0, 6)
  const byCat = (data.by_category || []).slice(0, 6)
  return (
    <div>
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-medium text-gray-300">Signal → Paper-trade Funnel ({data.window_hours}h)</h2>
        <span className={`text-[10px] ${convColor}`}>
          {fires.toLocaleString()} fires → {placed} placed ({conv}%)
        </span>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-[10px]">
        <div>
          <div className="text-gray-500 mb-1">By signal_type</div>
          {bySignal.map((r) => (
            <div key={r.signal_type} className="flex justify-between py-0.5">
              <span className="text-gray-400">{r.signal_type}</span>
              <span className="text-gray-500">
                {r.fires} → <span className={r.placed ? 'text-accent-green' : 'text-gray-600'}>{r.placed}</span>
              </span>
            </div>
          ))}
        </div>
        <div>
          <div className="text-gray-500 mb-1">By category</div>
          {byCat.map((r) => (
            <div key={r.category} className="flex justify-between py-0.5">
              <span className="text-gray-400">{r.category}</span>
              <span className="text-gray-500">
                {r.fires} → <span className={r.placed ? 'text-accent-green' : 'text-gray-600'}>{r.placed}</span>
              </span>
            </div>
          ))}
        </div>
      </div>
      <div className="mt-2 pt-2 border-t border-surface-border text-[9px] text-gray-600">
        Open: {t.paper_trades_currently_open} · Resolved in window: {t.paper_trades_resolved_in_window}
      </div>
    </div>
  )
}

function LivePositionsTile({ data }) {
  const positions = (data?.positions || [])
  if (!positions.length) {
    return (
      <div>
        <h2 className="text-sm font-medium text-gray-300 mb-2">Live Positions</h2>
        <div className="text-[10px] text-gray-600">No open live positions. Will populate when a signal graduates from paper → live.</div>
      </div>
    )
  }
  return (
    <div>
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-medium text-gray-300">Live Positions</h2>
        <span className="text-[10px] text-accent-orange">{positions.length} open · monitor every 5 min</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-[10px]">
          <thead className="text-gray-500">
            <tr>
              <th className="text-left pb-1">Signal</th>
              <th className="text-left pb-1">Side</th>
              <th className="text-right pb-1">Fill</th>
              <th className="text-right pb-1">Mark</th>
              <th className="text-right pb-1">Stake</th>
              <th className="text-right pb-1">Unrlz</th>
              <th className="text-left pb-1 pl-2">Market</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {positions.map((p) => {
              const unr = Number(p.unrealized_pnl || 0)
              const mark = Number(p.last_mark_price || p.fill_price || 0)
              const cls = unr > 0 ? 'text-accent-green' : unr < 0 ? 'text-accent-red' : 'text-gray-500'
              return (
                <tr key={p.id} className="align-top">
                  <td className="py-1 text-gray-400">{p.signal_type || 'n/a'}</td>
                  <td className="py-1 text-gray-400">{p.side}</td>
                  <td className="py-1 text-right text-gray-500">{Number(p.fill_price || 0).toFixed(3)}</td>
                  <td className="py-1 text-right text-gray-500">{mark.toFixed(3)}</td>
                  <td className="py-1 text-right text-gray-500">${Number(p.size_usd || 0).toFixed(2)}</td>
                  <td className={`py-1 text-right ${cls}`}>{unr >= 0 ? '+' : ''}${unr.toFixed(2)}</td>
                  <td className="py-1 pl-2 text-gray-500 truncate max-w-[200px]" title={p.question}>{p.question}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function TradeJournalTile({ data }) {
  const trades = (data?.trades || []).slice(0, 10)
  if (!trades.length) {
    return (
      <div>
        <h2 className="text-sm font-medium text-gray-300 mb-2">Recent Trades — Gate Values</h2>
        <div className="text-[10px] text-gray-600">Awaiting paper trades with entry_context.</div>
      </div>
    )
  }
  return (
    <div>
      <h2 className="text-sm font-medium text-gray-300 mb-2">Recent Trades — Gate Values (Latest {trades.length})</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-[10px]">
          <thead className="text-gray-500">
            <tr>
              <th className="text-left pb-1">Signal</th>
              <th className="text-left pb-1">Cat</th>
              <th className="text-right pb-1">Entry</th>
              <th className="text-right pb-1">Conf</th>
              <th className="text-right pb-1">Fusion</th>
              <th className="text-right pb-1">Alpha</th>
              <th className="text-right pb-1">PnL</th>
              <th className="text-left pb-1 pl-2">Outcome</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {trades.map((t) => {
              const pnl = t.realized_pnl
              const cls = pnl > 0 ? 'text-accent-green' : pnl < 0 ? 'text-accent-red' : 'text-gray-500'
              return (
                <tr key={t.id}>
                  <td className="py-0.5 text-gray-400">{t.signal_type}</td>
                  <td className="py-0.5 text-gray-500">{t.category || '-'}</td>
                  <td className="py-0.5 text-right text-gray-500">{Number(t.entry_price || 0).toFixed(3)}</td>
                  <td className="py-0.5 text-right text-gray-500">{((t.confidence || 0) * 100).toFixed(0)}%</td>
                  <td className="py-0.5 text-right text-gray-500">{Number(t.fusion_score || 0).toFixed(2)}</td>
                  <td className="py-0.5 text-right text-gray-500">{Number(t.alpha_score || 0).toFixed(2)}</td>
                  <td className={`py-0.5 text-right ${cls}`}>
                    {pnl !== null && pnl !== undefined ? `${pnl >= 0 ? '+' : ''}$${Number(pnl).toFixed(0)}` : '-'}
                  </td>
                  <td className="py-0.5 pl-2 text-gray-500">{t.exit_reason || t.outcome || 'open'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function ExitAnalyzerTile({ data }) {
  const sigs = data?.signals || []
  if (!sigs.length) {
    return (
      <div>
        <h2 className="text-sm font-medium text-gray-300 mb-2">Exit Analyzer</h2>
        <div className="text-[10px] text-gray-600">
          No signal has ≥{data?.min_sample ?? 10} resolved trades with MAE/MFE yet.
          Recommendations will appear as data accumulates.
        </div>
      </div>
    )
  }
  return (
    <div>
      <h2 className="text-sm font-medium text-gray-300 mb-2">Exit Analyzer — Recommended TP/SL</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-[10px]">
          <thead className="text-gray-500">
            <tr>
              <th className="text-left pb-1">Signal</th>
              <th className="text-right pb-1">N</th>
              <th className="text-right pb-1">WR</th>
              <th className="text-right pb-1">Avg MFE</th>
              <th className="text-right pb-1">Avg MAE</th>
              <th className="text-right pb-1">Suggested TP</th>
              <th className="text-right pb-1">Tightest SL</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {sigs.map((s) => (
              <tr key={s.signal_type}>
                <td className="py-0.5 text-gray-400">{s.signal_type}</td>
                <td className="py-0.5 text-right text-gray-500">{s.n}</td>
                <td className="py-0.5 text-right text-gray-500">{((s.wr || 0) * 100).toFixed(0)}%</td>
                <td className="py-0.5 text-right text-accent-green">+{((s.avg_mfe || 0) * 100).toFixed(1)}%</td>
                <td className="py-0.5 text-right text-accent-red">{((s.avg_mae || 0) * 100).toFixed(1)}%</td>
                <td className="py-0.5 text-right text-accent-orange">
                  {s.recommended_tp !== null ? `+${(s.recommended_tp * 100).toFixed(0)}%` : '-'}
                </td>
                <td className="py-0.5 text-right text-accent-orange">
                  {s.recommended_sl !== null ? `${(s.recommended_sl * 100).toFixed(0)}%` : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function LiveReadinessTile({ data }) {
  if (!data || !data.available) {
    return <div className="text-[10px] text-gray-600">Loading readiness…</div>
  }
  const gates = data.system_gates || {}
  const gateEntries = [
    ['master_switch', 'Master'],
    ['clob_reachable', 'CLOB'],
    ['clob_configured', 'CLOB cfg'],
    ['wallet_set', 'Wallet'],
    ['py_clob_client', 'clob_sdk'],
    ['emergency_stop_clear', 'E-stop'],
  ]
  const totalGates = gateEntries.length
  const passed = gateEntries.filter(([k]) => gates[k]).length
  const sigGates = data.signal_gates || []
  const minResolved = data.kill_switch_limits?.min_resolved_hard ?? 15
  const ready = data.n_ready ?? 0
  return (
    <div>
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-medium text-gray-300">Live Readiness</h2>
        <span className="text-[10px] text-gray-600">{passed}/{totalGates} gates · {ready} signal(s) ready</span>
      </div>
      <div className="flex flex-wrap gap-1.5 mb-3">
        {gateEntries.map(([k, label]) => (
          <span key={k}
            className={`px-1.5 py-0.5 rounded text-[9px] ${gates[k] ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'}`}
            title={`${label}: ${gates[k] ? 'OK' : 'fail'}`}>
            {gates[k] ? '●' : '○'} {label}
          </span>
        ))}
      </div>
      {sigGates.length ? (
        <div className="space-y-1">
          {sigGates.slice(0, 5).map((s) => {
            const n = s.n_resolved || 0
            const pct = Math.min(100, (n / minResolved) * 100)
            const wrOk = s.gates?.win_rate_ok
            const evOk = s.gates?.ev_positive
            return (
              <div key={s.signal_type} className="text-[10px]">
                <div className="flex justify-between mb-0.5">
                  <span className="text-gray-400">{s.signal_type}</span>
                  <span className="text-gray-500">
                    {n}/{minResolved} · WR {((s.win_rate ?? 0) * 100).toFixed(0)}%
                    {' '}
                    <span className={evOk ? 'text-accent-green' : 'text-accent-red'} title="EV gate">EV</span>
                    {' '}
                    <span className={wrOk ? 'text-accent-green' : 'text-accent-red'} title="WR gate">WR</span>
                  </span>
                </div>
                <div className="h-1 bg-surface-hover rounded overflow-hidden">
                  <div className={`h-full ${s.ready ? 'bg-accent-green' : 'bg-accent-blue'}`} style={{ width: `${pct}%` }} />
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <div className="text-[10px] text-gray-600">No signals have fired yet.</div>
      )}
    </div>
  )
}

function RecentHypotheses({ data }) {
  const rows = data?.hypotheses ?? []
  if (!data?.available) {
    return <p className="text-[10px] text-gray-600">Hypotheses endpoint unavailable</p>
  }
  if (!rows.length) {
    return <p className="text-[10px] text-gray-600">No hypotheses recorded yet — fire a paper trade to populate.</p>
  }
  return (
    <div className="space-y-1 max-h-[220px] overflow-auto pr-1">
      {rows.map((h) => {
        const resolved = h.hypothesis_correct != null
        const correct = h.hypothesis_correct === 1
        const status = !resolved
          ? <span className="text-[9px] text-gray-500">open</span>
          : correct
            ? <span className="text-[9px] text-accent-green">✓</span>
            : <span className="text-[9px] text-accent-red">✗</span>
        return (
          <div
            key={h.id}
            className="flex items-start gap-2 text-[10px] py-1 border-b border-surface-border/60 last:border-0"
          >
            <span className="w-3 mt-0.5">{status}</span>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-1.5 mb-0.5">
                <span className={`px-1 py-0.5 rounded text-[8px] font-bold ${h.direction === 'BUY' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'}`}>
                  {h.direction}
                </span>
                <span className="px-1 py-0.5 rounded bg-surface-hover text-[8px] text-gray-400">{h.category || '—'}</span>
                <span className="font-mono text-[9px] text-gray-500">α {h.alpha_score != null ? h.alpha_score.toFixed(2) : '—'}</span>
                <span className="text-gray-600 ml-auto">{fmtRelTime(h.created_at)}</span>
              </div>
              <p className="text-gray-400 truncate">{h.market_question || '—'}</p>
              <p className="text-gray-600 text-[9px] truncate">{h.thesis || ''}</p>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Main Dashboard ──────────────────────────────────────────────────────────
export default function Dashboard() {
  const { data: status } = useApi(api.polymarketSubscriptionStatus, 30_000)
  const { data: whaleFeed, loading: feedL } = useApi(api.polymarketWhaleFeed, 30_000)
  const { data: health } = useApi(api.intelligenceHealth, 60_000)
  const { data: sigPerf } = useApi(api.signalsPerformance, 60_000)
  // Live data sources added when the post-validation pipeline came online.
  const { data: bankroll } = useApi(api.paperBankroll, 30_000)
  const { data: universe } = useApi(api.smartMoneyUniverseStats, 60_000)
  // Use the winners endpoint not the leaderboard endpoint — winners
  // returns rows tagged with `tier` (tier1h/tier1/tier2), leaderboard
  // does not. See StatCards for the rationale.
  const { data: leaderboard } = useApi(() => api.smartMoneyWinners('all'), 60_000)
  // Circuit breaker status — endpoint added in the e2e validation session.
  const { data: cb } = useApi(() => fetch('/api/circuit-breaker/status').then(r => r.ok ? r.json() : null), 30_000)
  // Thesis claims — lightweight endpoint that returns scorecard + 5 claims.
  // Uses /api/thesis/claims (fast) instead of /api/thesis/scorecard (slow,
  // includes daily_metrics which adds heavy DB queries and can timeout).
  const { data: thesis } = useApi(() => fetch('/api/thesis/claims').then(r => r.ok ? r.json() : null), 60_000)
  // Daily snapshots — drives the sparkline next to the hero accuracy number.
  const { data: thesisHistory } = useApi(() => fetch('/api/thesis/history?days=60').then(r => r.ok ? r.json() : null), 60_000)
  // Paper equity curve + recent hypotheses for the new dashboard panels.
  const { data: equity } = useApi(api.equityCurve, 60_000)
  const { data: hypotheses } = useApi(() => fetch('/api/hypotheses/recent?limit=15').then(r => r.ok ? r.json() : null), 30_000)
  const { data: readiness } = useApi(() => fetch('/api/live/readiness').then(r => r.ok ? r.json() : null), 30_000)
  const { data: funnel } = useApi(() => api.pipelineFunnel(24), 60_000)
  const { data: livePositions } = useApi(() => api.livePositions(), 30_000)
  const { data: tradeJournalData } = useApi(() => api.tradeJournal(15), 60_000)
  const { data: exitAnalysis } = useApi(() => api.exitAnalyzer(5), 120_000)

  return (
    <div className="p-3 md:p-6 space-y-3 md:space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Command Center</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Command Center</h1>
      </div>

      <ThesisScorecard data={thesis} history={thesisHistory} />

      <StatusBar status={status} />
      <StatCards whaleFeed={whaleFeed} health={health} status={status} bankroll={bankroll} cb={cb} universe={universe} leaderboard={leaderboard} />

      {/* P&L curve + Recent Hypotheses — answers "are hypothesis trades
          actually compounding?" alongside "what did we just bet on?". */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <div className="lg:col-span-3 card">
          <div className="flex items-baseline justify-between mb-2">
            <h2 className="text-sm font-medium text-gray-300">Cumulative Paper P&amp;L</h2>
            <span className="text-[10px] text-gray-600">starting bankroll dashed</span>
          </div>
          <CumulativePnLChart
            rows={equity?.data}
            starting={bankroll?.starting_capital ?? 10000}
          />
        </div>
        <div className="lg:col-span-2 card">
          <LiveReadinessTile data={readiness} />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <div className="lg:col-span-3 card">
          <PipelineFunnelTile data={funnel} />
        </div>
        <div className="lg:col-span-2 card">
          <h2 className="text-sm font-medium text-gray-300 mb-2">Recent Hypotheses</h2>
          <RecentHypotheses data={hypotheses} />
        </div>
      </div>

      <div className="card">
        <LivePositionsTile data={livePositions} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <div className="lg:col-span-3 card">
          <TradeJournalTile data={tradeJournalData} />
        </div>
        <div className="lg:col-span-2 card">
          <ExitAnalyzerTile data={exitAnalysis} />
        </div>
      </div>

      <div className="flex flex-col md:flex-row gap-3 md:gap-4">
        {/* Left: Whale Feed */}
        <div className="md:flex-[65] space-y-2 min-w-0">
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
        <div className="md:flex-[35] space-y-3 md:space-y-4 min-w-0">
          <div className="card">
            <SignalPerfTable data={sigPerf} status={status} />
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-2">Recent Alerts</h2>
            {!(whaleFeed?.data?.length) ? (
              <p className="text-[10px] text-gray-600">Alerts appear when watched wallets trade</p>
            ) : (
              <div className="space-y-1">
                {(whaleFeed.data || []).filter(a => !SYNTHETIC_WALLETS.has(a.wallet) && !isSportsMarket(a)).slice(0, 5).map((a, i) => (
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

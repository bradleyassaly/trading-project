import { useState, useEffect, useCallback } from 'react'
import { useApi } from '../hooks/useApi'
import { api } from '../api/client'
import LoadingSkeleton from '../components/LoadingSkeleton'
import CumulativePnLChart from '../components/charts/CumulativePnLChart'
import { fmtUsd, fmtPct, fmtRelTime } from '../lib/format'

const TABS = [
  { id: 'portfolio', label: 'Portfolio' },
  { id: 'signals', label: 'Signal Scorecards' },
  { id: 'open', label: 'Open Positions' },
  { id: 'closed', label: 'Closed Trades' },
  { id: 'postmortem', label: 'Post-Mortem' },
]

function Metric({ label, value, sub, color }) {
  return (
    <div className="bg-surface-card rounded-lg p-3">
      <p className="text-[10px] text-gray-500 mb-1">{label}</p>
      <p className={`text-xl font-bold font-mono ${color || 'text-gray-200'}`}>{value}</p>
      {sub && <p className="text-[9px] text-gray-600 mt-0.5">{sub}</p>}
    </div>
  )
}

function ExitBadge({ reason }) {
  const styles = {
    resolution: 'bg-accent-blue/20 text-accent-blue',
    stop_loss: 'bg-accent-red/20 text-accent-red',
    take_profit: 'bg-accent-green/20 text-accent-green',
    time_decay: 'bg-gray-700 text-gray-400',
    whale_exit: 'bg-purple-500/20 text-purple-400',
    expired: 'bg-gray-800 text-gray-600',
  }
  return (
    <span className={`px-1.5 py-0.5 rounded text-[8px] font-semibold ${styles[reason] || 'bg-gray-700 text-gray-400'}`}>
      {(reason || 'resolution').replace('_', ' ')}
    </span>
  )
}

// ── Portfolio Tab ────────────────────────────────────────────────────────────
function PortfolioTab({ summary, equity }) {
  if (!summary?.available) return <p className="text-xs text-gray-500">Loading portfolio data...</p>

  const wr = summary.win_rate
  const wrColor = wr >= 0.55 ? 'text-accent-green' : wr >= 0.45 ? 'text-yellow-400' : 'text-accent-red'
  const pnl = summary.realized_pnl || 0
  const pnlColor = pnl >= 0 ? 'text-accent-green' : 'text-accent-red'

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <Metric label="REALIZED P&L" value={fmtUsd(pnl, { signed: true })} color={pnlColor} sub={`${summary.closed_trades} closed trades`} />
        <Metric label="DEPLOYED" value={fmtUsd(summary.deployed)} sub={`${summary.open_positions} open positions`} />
        <Metric label="WIN RATE" value={wr != null ? fmtPct(wr) : '--'} color={wrColor} sub={`${summary.wins}/${summary.closed_trades} wins`} />
        <Metric label="TOTAL TRADES" value={summary.total_trades} sub={`${summary.open_positions} open / ${summary.closed_trades} closed`} />
      </div>
      <div className="card">
        <h3 className="text-sm font-medium text-gray-300 mb-2">Equity Curve</h3>
        <CumulativePnLChart
          rows={equity?.data}
          starting={summary.starting_bankroll || 10000}
          height={220}
        />
      </div>
    </div>
  )
}

// ── Signal Scorecards Tab ───────────────────────────────────────────────────
function SignalsTab({ signals }) {
  const data = signals?.data || []
  if (!data.length) return <p className="text-xs text-gray-500">No signal data yet</p>

  return (
    <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
      {data.map(s => {
        const wr = s.win_rate
        const wrColor = wr >= 0.55 ? 'text-accent-green' : wr >= 0.45 ? 'text-yellow-400' : wr != null ? 'text-accent-red' : 'text-gray-600'
        return (
          <div key={s.signal_type} className="bg-surface-card rounded-lg p-3">
            <p className="text-[10px] text-gray-500 mb-1">{(s.signal_type || '').replace(/_/g, ' ')}</p>
            <div className="flex items-baseline gap-2 mb-1">
              <span className={`text-lg font-bold font-mono ${wrColor}`}>
                {wr != null ? fmtPct(wr) : '--'}
              </span>
              <span className="text-[10px] text-gray-500">WR</span>
            </div>
            <div className="grid grid-cols-2 gap-x-3 text-[10px] text-gray-400">
              <span>Trades: {s.total}</span>
              <span>Open: {s.open}</span>
              <span>Avg return: {s.avg_return_pct != null ? `${(s.avg_return_pct * 100).toFixed(1)}%` : '--'}</span>
              <span>PF: {s.profit_factor ?? '--'}</span>
              <span>Hold: {s.avg_hold_days != null ? `${s.avg_hold_days.toFixed(1)}d` : '--'}</span>
              <span>Wins: {s.wins}/{s.closed}</span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Open Positions Tab ──────────────────────────────────────────────────────
function OpenTab({ trades, onCheckExits }) {
  const open = (trades?.positions || trades?.data || []).filter(t => !t.exit_ts)
  if (!open.length) return <p className="text-xs text-gray-500">No open positions</p>

  return (
    <div className="space-y-3">
      <div className="flex gap-2">
        <button onClick={onCheckExits} className="px-3 py-1 rounded text-[10px] bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30">
          Check Exits Now
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-[10px]">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-1 pr-2">Market</th>
              <th className="pb-1 pr-2">Side</th>
              <th className="pb-1 pr-2 text-right">Entry</th>
              <th className="pb-1 pr-2 text-right">Mark</th>
              <th className="pb-1 pr-2 text-right">Unreal. P&L</th>
              <th className="pb-1 pr-2 text-right">Size</th>
              <th className="pb-1 pr-2">Signal</th>
              <th className="pb-1 pr-2">Age</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {open.map(t => {
              const entry = t.entry_price || 0
              const mark = t.last_mark_price || entry
              const unreal = t.unrealized_pnl || 0
              const pnlColor = unreal >= 0 ? 'text-accent-green' : 'text-accent-red'
              const ageDays = t.entry_ts ? ((Date.now() / 1000 - t.entry_ts) / 86400).toFixed(1) : '?'
              return (
                <tr key={t.id} className={`hover:bg-surface-hover ${unreal < 0 ? 'bg-accent-red/5' : ''}`}>
                  <td className="py-1 pr-2 text-gray-300 truncate max-w-[200px]">{t.question || t.condition_id?.slice(0, 16)}</td>
                  <td className={`py-1 pr-2 font-bold ${t.side === 'YES' || t.side === 'BUY' ? 'text-accent-green' : 'text-accent-red'}`}>{t.side}</td>
                  <td className="py-1 pr-2 text-right font-mono text-gray-400">{entry.toFixed(3)}</td>
                  <td className="py-1 pr-2 text-right font-mono text-gray-300">{mark.toFixed(3)}</td>
                  <td className={`py-1 pr-2 text-right font-mono ${pnlColor}`}>${unreal.toFixed(0)}</td>
                  <td className="py-1 pr-2 text-right font-mono text-gray-400">${(t.size_usd || 0).toFixed(0)}</td>
                  <td className="py-1 pr-2 text-gray-500">{t.signal_type}</td>
                  <td className="py-1 pr-2 text-gray-600">{ageDays}d</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Closed Trades Tab ───────────────────────────────────────────────────────
function ClosedTab({ trades }) {
  const closed = (trades?.positions || trades?.data || []).filter(t => t.exit_ts)
  if (!closed.length) return <p className="text-xs text-gray-500">No closed trades yet</p>

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[10px]">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-1 pr-2">Market</th>
            <th className="pb-1 pr-2">Side</th>
            <th className="pb-1 pr-2 text-right">Entry</th>
            <th className="pb-1 pr-2 text-right">Exit</th>
            <th className="pb-1 pr-2 text-right">P&L</th>
            <th className="pb-1 pr-2 text-right">Return</th>
            <th className="pb-1 pr-2">Signal</th>
            <th className="pb-1 pr-2">Exit Reason</th>
            <th className="pb-1 pr-2">Outcome</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {closed.slice(0, 100).map((t, i) => {
            const pnl = t.realized_pnl || 0
            return (
              <tr key={t.id || i} className="hover:bg-surface-hover">
                <td className="py-1 pr-2 text-gray-300 truncate max-w-[200px]">{t.question || '—'}</td>
                <td className={`py-1 pr-2 ${t.side === 'YES' || t.side === 'BUY' ? 'text-accent-green' : 'text-accent-red'}`}>{t.side}</td>
                <td className="py-1 pr-2 text-right font-mono text-gray-400">{(t.entry_price || 0).toFixed(3)}</td>
                <td className="py-1 pr-2 text-right font-mono text-gray-400">{(t.exit_price || 0).toFixed(3)}</td>
                <td className={`py-1 pr-2 text-right font-mono ${pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>${pnl.toFixed(0)}</td>
                <td className={`py-1 pr-2 text-right font-mono ${(t.return_pct || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {t.return_pct != null ? `${(t.return_pct * 100).toFixed(1)}%` : '--'}
                </td>
                <td className="py-1 pr-2 text-gray-500">{t.signal_type}</td>
                <td className="py-1 pr-2"><ExitBadge reason={t.exit_reason || (t.outcome ? 'resolution' : null)} /></td>
                <td className="py-1 pr-2">
                  {t.outcome && (
                    <span className={`px-1 py-0.5 rounded text-[8px] font-bold ${t.outcome === 'win' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'}`}>
                      {t.outcome}
                    </span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ── Post-Mortem Tab ─────────────────────────────────────────────────────────
function PostMortemTab({ exits, categories, wallets }) {
  return (
    <div className="space-y-4">
      {/* Exit Attribution */}
      <div className="card">
        <h3 className="text-sm font-medium text-gray-300 mb-2">Exit Attribution</h3>
        {(exits?.data || []).length ? (
          <div className="space-y-1">
            {exits.data.map(e => (
              <div key={e.exit_reason} className="flex items-center gap-3 text-[10px] py-1">
                <ExitBadge reason={e.exit_reason} />
                <span className="text-gray-400">{e.total} trades</span>
                <span className={`font-mono ${(e.total_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  ${(e.total_pnl || 0).toFixed(0)}
                </span>
                {e.avg_return != null && <span className="text-gray-500">avg: {(e.avg_return * 100).toFixed(1)}%</span>}
              </div>
            ))}
          </div>
        ) : <p className="text-[10px] text-gray-600">No exit data yet</p>}
      </div>

      {/* Category P&L */}
      <div className="card">
        <h3 className="text-sm font-medium text-gray-300 mb-2">Category P&L</h3>
        {(categories?.data || []).length ? (
          <div className="space-y-1">
            {categories.data.map(c => (
              <div key={c.category} className="flex items-center gap-3 text-[10px] py-1">
                <span className="text-gray-400 w-20 capitalize">{c.category}</span>
                <span className="text-gray-400">{c.total} trades</span>
                <span className={`font-mono ${(c.total_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  ${(c.total_pnl || 0).toFixed(0)}
                </span>
              </div>
            ))}
          </div>
        ) : <p className="text-[10px] text-gray-600">No category data yet</p>}
      </div>

      {/* Wallet Performance */}
      <div className="card">
        <h3 className="text-sm font-medium text-gray-300 mb-2">Wallet Copy Performance</h3>
        {(wallets?.data || []).length ? (
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-1 pr-2">Wallet</th>
                <th className="pb-1 pr-2 text-right">Trades</th>
                <th className="pb-1 pr-2 text-right">WR</th>
                <th className="pb-1 pr-2 text-right">Net P&L</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {wallets.data.slice(0, 20).map(w => (
                <tr key={w.wallet} className="hover:bg-surface-hover">
                  <td className="py-1 pr-2 font-mono text-accent-blue">{(w.wallet || '').slice(0, 12)}...</td>
                  <td className="py-1 pr-2 text-right text-gray-400">{w.closed}</td>
                  <td className="py-1 pr-2 text-right font-mono">{w.win_rate != null ? fmtPct(w.win_rate) : '--'}</td>
                  <td className={`py-1 pr-2 text-right font-mono ${(w.total_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                    ${(w.total_pnl || 0).toFixed(0)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <p className="text-[10px] text-gray-600">No wallet data yet</p>}
      </div>
    </div>
  )
}

// ── Main Page ───────────────────────────────────────────────────────────────
export default function Paper() {
  const [tab, setTab] = useState('portfolio')

  // Fetch all data via the new analytics endpoints
  const { data: summary } = useApi(() => fetch('/api/paper/analytics/summary').then(r => r.ok ? r.json() : null), 60_000)
  const { data: signals } = useApi(() => fetch('/api/paper/analytics/signals').then(r => r.ok ? r.json() : null), 60_000)
  const { data: categories } = useApi(() => fetch('/api/paper/analytics/categories').then(r => r.ok ? r.json() : null), 60_000)
  const { data: walletsData } = useApi(() => fetch('/api/paper/analytics/wallets').then(r => r.ok ? r.json() : null), 60_000)
  const { data: exits } = useApi(() => fetch('/api/paper/analytics/exits').then(r => r.ok ? r.json() : null), 60_000)
  const { data: equity } = useApi(() => fetch('/api/paper/equity-curve').then(r => r.ok ? r.json() : null), 60_000)
  const { data: trades } = useApi(api.paperDashboard, 30_000)

  const handleCheckExits = () => {
    fetch('/api/paper/check-exits', { method: 'POST' })
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d) alert(`Checked ${d.checked} positions, exited ${d.exited}`) })
      .catch(() => {})
  }

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          Trading Platform &gt;{' '}
          <span className="text-gray-400">Paper Trading</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200">Paper Trading</h1>
        <p className="text-xs text-gray-500 mt-0.5">Are hypothesis trades profitable? — Track thesis-driven paper trade performance</p>
      </div>

      {/* Go-Live Progress */}
      {summary?.available && (
        <div className="bg-surface-card rounded-lg p-3">
          <div className="flex items-center justify-between mb-2">
            <p className="text-[10px] text-gray-500">Path to Live</p>
            <span className="text-[10px] text-gray-500">{summary.closed_trades || 0} / 50 resolved</span>
          </div>
          <div className="w-full bg-gray-800 rounded-full h-2">
            <div
              className={`h-2 rounded-full ${(summary.closed_trades || 0) >= 50 ? 'bg-accent-green' : 'bg-accent-blue'}`}
              style={{ width: `${Math.min(100, ((summary.closed_trades || 0) / 50) * 100)}%` }}
            />
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-surface-border">
        {TABS.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-3 py-1.5 text-xs font-medium transition-colors ${
              tab === t.id
                ? 'text-accent-blue border-b-2 border-accent-blue -mb-px'
                : 'text-gray-500 hover:text-gray-300'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'portfolio' && <PortfolioTab summary={summary} equity={equity} />}
      {tab === 'signals' && <SignalsTab signals={signals} />}
      {tab === 'open' && <OpenTab trades={trades} onCheckExits={handleCheckExits} />}
      {tab === 'closed' && <ClosedTab trades={trades} />}
      {tab === 'postmortem' && <PostMortemTab exits={exits} categories={categories} wallets={walletsData} />}
    </div>
  )
}

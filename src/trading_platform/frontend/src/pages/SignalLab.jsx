import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import LoadingSkeleton from '../components/LoadingSkeleton'

const TABS = [
  { id: 'performance', label: '📊 Performance' },
  { id: 'positions',   label: '📋 Open Positions' },
  { id: 'backtest',    label: '🔬 Backtest' },
]

const SIGNAL_COLORS = {
  cascade:            '#f59e0b',
  whale_entry:        '#06b6d4',
  whale_exit:         '#f43f5e',
  oversized_bet:      '#f97316',
  market_maker_flip:  '#8b5cf6',
  price_velocity:     '#ef4444',
  convergence:        '#10b981',
  accumulation:       '#a855f7',
  wallet_reversal:    '#ec4899',
  specialist_entry:   '#84cc16',
  pre_deadline_surge: '#fbbf24',
  position_reduction: '#6366f1',
  no_position_entry:  '#f43f5e',
}

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `${n < 0 ? '-' : ''}$${(Math.abs(n) / 1e6).toFixed(2)}M`
  if (Math.abs(n) >= 1e3) return `${n < 0 ? '-' : ''}$${(Math.abs(n) / 1e3).toFixed(1)}K`
  return `${n < 0 ? '-' : ''}$${Math.abs(n).toFixed(0)}`
}

function fmtSignedUsd(n) {
  if (n == null) return '—'
  return (n >= 0 ? '+' : '') + fmtUsd(n)
}

function fmtDate(ts) {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

function fmtSignalName(s) {
  return (s || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

// ── Mini sparkline SVG (40×20) for equity curve ──
function Sparkline({ points, color }) {
  if (!points || points.length < 2) return <span className="text-gray-700">—</span>
  const w = 60, h = 20, pad = 1
  const ys = points.map(p => p.pnl)
  const minY = Math.min(...ys), maxY = Math.max(...ys)
  const range = maxY - minY || 1
  const step = (w - 2 * pad) / (points.length - 1)
  const path = points.map((p, i) => {
    const x = pad + i * step
    const y = h - pad - ((p.pnl - minY) / range) * (h - 2 * pad)
    return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`
  }).join(' ')
  const isUp = points[points.length - 1].pnl >= points[0].pnl
  return (
    <svg width={w} height={h} className="inline-block">
      <path d={path} fill="none" stroke={color || (isUp ? '#22c55e' : '#ef4444')} strokeWidth="1.2" />
    </svg>
  )
}

// ── Tab 1: Performance ──
function PerformanceTab() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [resolving, setResolving] = useState(false)
  const [resolveResult, setResolveResult] = useState(null)
  const [expanded, setExpanded] = useState(null)

  const load = () => {
    setLoading(true)
    api.signalsPerformance()
      .then(d => setData(d))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  const checkResolutions = async () => {
    setResolving(true)
    setResolveResult(null)
    try {
      const r = await api.paperCheckResolutions()
      setResolveResult(r)
      // Reload performance after resolution check
      load()
    } catch (e) {
      setResolveResult({ error: e.message })
    }
    setResolving(false)
  }

  if (loading && !data) return <LoadingSkeleton rows={6} />
  if (!data || !data.available) return <p className="text-xs text-gray-500">Signal performance unavailable.</p>

  const portfolio = data.portfolio || {}
  const byType = data.by_type || []

  return (
    <div className="space-y-4">
      {/* Portfolio summary */}
      <div className="card">
        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-[11px]">
          <div>
            <span className="text-gray-500">Bankroll:</span>{' '}
            <span className="text-gray-200 font-mono">{fmtUsd(portfolio.bankroll)}</span>
          </div>
          <div>
            <span className="text-gray-500">Deployed:</span>{' '}
            <span className="text-gray-200 font-mono">{fmtUsd(portfolio.total_deployed)}</span>
            {' '}
            <span className="text-gray-600">({portfolio.total_open || 0} open)</span>
          </div>
          <div>
            <span className="text-gray-500">Resolved:</span>{' '}
            <span className="text-gray-200 font-mono">{portfolio.total_resolved || 0}</span>
          </div>
          <div>
            <span className="text-gray-500">Win Rate:</span>{' '}
            <span className={
              portfolio.overall_win_rate == null ? 'text-gray-500' :
              portfolio.overall_win_rate >= 0.55 ? 'text-accent-green font-mono' :
              portfolio.overall_win_rate < 0.45 ? 'text-accent-red font-mono' :
              'text-accent-yellow font-mono'
            }>
              {portfolio.overall_win_rate != null ? `${(portfolio.overall_win_rate * 100).toFixed(1)}%` : '—'}
            </span>
          </div>
          <div>
            <span className="text-gray-500">Total P&L:</span>{' '}
            <span className={`font-mono font-bold ${(portfolio.total_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
              {fmtSignedUsd(portfolio.total_pnl)}
            </span>
            {' '}
            <span className="text-gray-600">({portfolio.pnl_pct >= 0 ? '+' : ''}{portfolio.pnl_pct}%)</span>
          </div>
          <button
            onClick={checkResolutions}
            disabled={resolving}
            className="ml-auto px-3 py-1 rounded text-[10px] font-medium bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 disabled:opacity-50"
          >
            {resolving ? 'Checking…' : '🔄 Check Resolutions'}
          </button>
        </div>
        {resolveResult && (
          <div className="text-[10px] text-gray-500 mt-2">
            {resolveResult.error
              ? `Error: ${resolveResult.error}`
              : `Checked ${resolveResult.checked} open trades, resolved ${resolveResult.resolved}.`}
          </div>
        )}
      </div>

      {/* Signal type table */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-300 mb-2">Signal Performance</h2>
        <table className="w-full text-[10px]">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-1">Signal</th>
              <th className="pb-1 text-right">Trades</th>
              <th className="pb-1 text-right">Open</th>
              <th className="pb-1 text-right">WR</th>
              <th className="pb-1 text-right">Avg Win</th>
              <th className="pb-1 text-right">Avg Loss</th>
              <th className="pb-1 text-right">PF</th>
              <th className="pb-1 text-right">Max DD</th>
              <th className="pb-1 text-right">Cum P&L</th>
              <th className="pb-1 text-center">Equity</th>
              <th className="pb-1">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {byType.map(s => {
              const color = SIGNAL_COLORS[s.signal_type] || '#6b7280'
              const isExpanded = expanded === s.signal_type
              const wrColor =
                s.win_rate == null ? 'text-gray-500' :
                s.win_rate >= 0.55 ? 'text-accent-green' :
                s.win_rate < 0.45 ? 'text-accent-red' :
                'text-accent-yellow'
              const pfColor =
                s.profit_factor == null ? 'text-gray-500' :
                s.profit_factor >= 1.5 ? 'text-accent-green' :
                s.profit_factor < 1.0 ? 'text-accent-red' :
                'text-accent-yellow'
              return (
                <>
                  <tr
                    key={s.signal_type}
                    onClick={() => setExpanded(isExpanded ? null : s.signal_type)}
                    className={`hover:bg-surface-hover cursor-pointer ${isExpanded ? 'bg-surface-hover' : ''}`}
                  >
                    <td className="py-1.5 text-gray-300">
                      <span className="inline-block w-2 h-2 rounded-full mr-1.5" style={{ background: color }} />
                      {fmtSignalName(s.signal_type)}
                    </td>
                    <td className={`py-1.5 text-right font-mono ${s.n_resolved === 0 ? 'text-gray-600' : 'text-gray-300'}`}>
                      {s.n_resolved}
                    </td>
                    <td className={`py-1.5 text-right font-mono ${s.n_open > 0 ? 'text-accent-yellow' : 'text-gray-600'}`}>
                      {s.n_open}
                    </td>
                    <td className={`py-1.5 text-right font-mono ${wrColor}`}>
                      {s.win_rate != null ? `${(s.win_rate * 100).toFixed(0)}%` : '—'}
                    </td>
                    <td className="py-1.5 text-right font-mono text-accent-green">{s.avg_win != null ? fmtSignedUsd(s.avg_win) : '—'}</td>
                    <td className="py-1.5 text-right font-mono text-accent-red">{s.avg_loss != null ? fmtSignedUsd(s.avg_loss) : '—'}</td>
                    <td className={`py-1.5 text-right font-mono ${pfColor}`}>
                      {s.profit_factor != null ? `${s.profit_factor}x` : '—'}
                    </td>
                    <td className="py-1.5 text-right font-mono text-accent-red">{s.max_drawdown != null ? fmtUsd(s.max_drawdown) : '—'}</td>
                    <td className={`py-1.5 text-right font-mono font-bold ${s.cum_pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {fmtSignedUsd(s.cum_pnl)}
                    </td>
                    <td className="py-1.5 text-center">
                      <Sparkline points={s.equity_curve} color={color} />
                    </td>
                    <td className="py-1.5">
                      <span className={`px-1.5 py-0.5 rounded text-[9px] ${
                        s.status === 'mature' ? 'bg-accent-green/20 text-accent-green' :
                        s.status === 'active' ? 'bg-accent-blue/20 text-accent-blue' :
                        s.status === 'building' ? 'bg-accent-yellow/20 text-accent-yellow' :
                        'bg-surface-bg text-gray-600'
                      }`}>{s.status}</span>
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr key={`${s.signal_type}-exp`} className="bg-surface-bg">
                      <td colSpan="11" className="py-3 px-4">
                        <div className="grid grid-cols-3 gap-3 text-[10px] mb-3">
                          <div>
                            <div className="text-gray-500">Allocated</div>
                            <div className="text-gray-200 font-mono">{fmtUsd(s.allocated)}</div>
                          </div>
                          <div>
                            <div className="text-gray-500">Avg Return</div>
                            <div className="text-gray-200 font-mono">
                              {s.avg_return_pct != null ? `${s.avg_return_pct >= 0 ? '+' : ''}${s.avg_return_pct}%` : '—'}
                            </div>
                          </div>
                          <div>
                            <div className="text-gray-500">Wins / Losses</div>
                            <div className="text-gray-200 font-mono">{s.wins} / {s.losses}</div>
                          </div>
                        </div>
                        {s.recent_trades && s.recent_trades.length > 0 && (
                          <div>
                            <div className="text-[10px] text-gray-500 mb-1">Last {s.recent_trades.length} resolved trades</div>
                            <table className="w-full text-[10px]">
                              <thead>
                                <tr className="text-gray-600 border-b border-surface-border text-left">
                                  <th className="pb-1">Date</th>
                                  <th className="pb-1">Market</th>
                                  <th className="pb-1">Side</th>
                                  <th className="pb-1 text-right">Entry</th>
                                  <th className="pb-1 text-right">Exit</th>
                                  <th className="pb-1 text-right">Size</th>
                                  <th className="pb-1 text-right">P&L</th>
                                  <th className="pb-1 text-right">Return</th>
                                </tr>
                              </thead>
                              <tbody className="divide-y divide-surface-border">
                                {s.recent_trades.map((t, i) => (
                                  <tr key={i}>
                                    <td className="py-1 text-gray-500">{fmtDate(t.exit_ts)}</td>
                                    <td className="py-1 text-gray-300 truncate max-w-[260px]">{t.question}</td>
                                    <td className="py-1 text-gray-400">{t.side}</td>
                                    <td className="py-1 text-right text-gray-400 font-mono">{(t.entry_price * 100).toFixed(0)}%</td>
                                    <td className="py-1 text-right text-gray-400 font-mono">{(t.exit_price * 100).toFixed(0)}%</td>
                                    <td className="py-1 text-right text-gray-400 font-mono">{fmtUsd(t.size_usd)}</td>
                                    <td className={`py-1 text-right font-mono ${t.pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                                      {fmtSignedUsd(t.pnl)}
                                    </td>
                                    <td className={`py-1 text-right font-mono ${t.return_pct >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                                      {t.return_pct >= 0 ? '+' : ''}{t.return_pct}%
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </>
              )
            })}
          </tbody>
        </table>
        {byType.every(s => s.n_resolved === 0) && (
          <p className="text-[11px] text-gray-500 mt-3">
            No resolved trades yet. Click <span className="text-accent-blue">Check Resolutions</span> or wait for active markets to settle.
          </p>
        )}
      </div>
    </div>
  )
}

// ── Tab 2: Open Positions ──
function PositionsTab() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.paperPositionsEnriched()
      .then(d => setData(d))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <LoadingSkeleton rows={5} />
  if (!data || !data.available) return <p className="text-xs text-gray-500">Positions unavailable.</p>

  const positions = data.positions || []
  if (positions.length === 0) return <p className="text-xs text-gray-500">No open positions.</p>

  const sorted = [...positions].sort((a, b) => (b.unrealized_pnl || 0) - (a.unrealized_pnl || 0))
  const totalDeployed = positions.reduce((s, p) => s + (p.size_usd || 0), 0)
  const best = sorted[0]
  const worst = sorted[sorted.length - 1]
  const now = Math.floor(Date.now() / 1000)
  const avgHoldDays = positions.reduce((s, p) => s + (now - (p.entry_ts || now)), 0) / positions.length / 86400

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-4 gap-3">
        <div className="bg-surface-card rounded p-3">
          <p className="text-[10px] text-gray-500">DEPLOYED</p>
          <p className="text-base font-bold font-mono text-gray-200">{fmtUsd(totalDeployed)}</p>
          <p className="text-[9px] text-gray-500">across {positions.length} markets</p>
        </div>
        <div className="bg-surface-card rounded p-3">
          <p className="text-[10px] text-gray-500">BEST</p>
          <p className={`text-base font-bold font-mono ${(best?.unrealized_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
            {fmtSignedUsd(best?.unrealized_pnl)}
          </p>
          <p className="text-[9px] text-gray-500 truncate">{(best?.question || '').slice(0, 30)}</p>
        </div>
        <div className="bg-surface-card rounded p-3">
          <p className="text-[10px] text-gray-500">WORST</p>
          <p className={`text-base font-bold font-mono ${(worst?.unrealized_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
            {fmtSignedUsd(worst?.unrealized_pnl)}
          </p>
          <p className="text-[9px] text-gray-500 truncate">{(worst?.question || '').slice(0, 30)}</p>
        </div>
        <div className="bg-surface-card rounded p-3">
          <p className="text-[10px] text-gray-500">AVG HOLD</p>
          <p className="text-base font-bold font-mono text-gray-200">{avgHoldDays.toFixed(1)}d</p>
          <p className="text-[9px] text-gray-500">unrealized {fmtSignedUsd(data.unrealized_pnl_total)}</p>
        </div>
      </div>

      <div className="card">
        <table className="w-full text-[10px]">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-1">Signal</th>
              <th className="pb-1">Market</th>
              <th className="pb-1">Side</th>
              <th className="pb-1 text-right">Size</th>
              <th className="pb-1 text-right">Entry</th>
              <th className="pb-1 text-right">Current</th>
              <th className="pb-1 text-right">P&L</th>
              <th className="pb-1 text-right">%</th>
              <th className="pb-1 text-right">Days</th>
              <th className="pb-1"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {sorted.map(p => {
              const color = SIGNAL_COLORS[p.signal_type] || '#6b7280'
              const days = ((now - (p.entry_ts || now)) / 86400).toFixed(1)
              return (
                <tr key={p.id} className="hover:bg-surface-hover">
                  <td className="py-1">
                    <span className="inline-block w-2 h-2 rounded-full mr-1" style={{ background: color }} />
                    <span className="text-gray-400 text-[9px]">{p.signal_type}</span>
                  </td>
                  <td className="py-1 text-gray-300 truncate max-w-[260px]">{p.question}</td>
                  <td className="py-1 text-gray-400">{p.side}</td>
                  <td className="py-1 text-right font-mono text-gray-400">{fmtUsd(p.size_usd)}</td>
                  <td className="py-1 text-right font-mono text-gray-400">{p.entry_price != null ? (p.entry_price * 100).toFixed(0) + '%' : '—'}</td>
                  <td className="py-1 text-right font-mono text-gray-200">{p.current_price != null ? (p.current_price * 100).toFixed(0) + '%' : '—'}</td>
                  <td className={`py-1 text-right font-mono ${(p.unrealized_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                    {p.unrealized_pnl != null ? fmtSignedUsd(p.unrealized_pnl) : '—'}
                  </td>
                  <td className={`py-1 text-right font-mono ${(p.unrealized_pct || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                    {p.unrealized_pct != null ? `${p.unrealized_pct >= 0 ? '+' : ''}${p.unrealized_pct.toFixed(0)}%` : '—'}
                  </td>
                  <td className="py-1 text-right text-gray-500">{days}d</td>
                  <td className="py-1">
                    <Link
                      to={`/monitor`}
                      className="text-[9px] text-accent-blue hover:underline"
                      title="View in Market Monitor"
                    >View</Link>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Tab 3: Backtest ──
function BacktestTab() {
  return (
    <div className="card">
      <p className="text-xs text-gray-300 mb-2">
        Strategy backtests live in the <Link to="/backtest" className="text-accent-blue hover:underline">Backtest page</Link>.
      </p>
      <ul className="text-[11px] text-gray-500 space-y-1.5 mt-3">
        <li>• <span className="text-gray-300">Single Wallet</span> — copy-trade backtest for any tier1/tier1h wallet</li>
        <li>• <span className="text-gray-300">Market Intelligence</span> — multi-wallet convergence analysis per market</li>
        <li>• <span className="text-gray-300">Convergence Backtest</span> — wait-for-N-wallets strategy with by-wallet-count breakdown</li>
      </ul>
      <p className="text-[10px] text-gray-600 mt-4">
        Velocity and order-book signal backtests will be added here once enough resolved trades have accumulated.
      </p>
    </div>
  )
}

// ── Top-level page ──
export default function SignalLab() {
  const [tab, setTab] = useState('performance')

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Signal Lab</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Signal Lab</h1>
      </div>

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

      {tab === 'performance' && <PerformanceTab />}
      {tab === 'positions'   && <PositionsTab />}
      {tab === 'backtest'    && <BacktestTab />}
    </div>
  )
}

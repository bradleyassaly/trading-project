import { useState, useEffect } from 'react'
import { api } from '../api/client'
import LoadingSkeleton from '../components/LoadingSkeleton'

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `${n < 0 ? '-' : ''}$${(Math.abs(n) / 1e6).toFixed(2)}M`
  if (Math.abs(n) >= 1e3) return `${n < 0 ? '-' : ''}$${(Math.abs(n) / 1e3).toFixed(1)}K`
  return `${n < 0 ? '-' : ''}$${Math.abs(n).toFixed(0)}`
}

function fmtTs(ts) {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

function MonthlyBarChart({ byMonth }) {
  const months = Object.keys(byMonth).sort()
  if (!months.length) return <p className="text-[10px] text-gray-600">No monthly data</p>
  const w = 400, h = 160, pad = 30
  const values = months.map(m => byMonth[m].pnl)
  const minV = Math.min(0, ...values)
  const maxV = Math.max(0, ...values)
  const range = maxV - minV || 1
  const barW = (w - 2 * pad) / months.length * 0.7
  const xs = i => pad + i * (w - 2 * pad) / months.length
  const yZero = h - pad - ((0 - minV) / range) * (h - 2 * pad)

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height: 160 }}>
      <line x1={pad} y1={yZero} x2={w - pad} y2={yZero} stroke="#4b5563" strokeWidth="0.5" />
      {months.map((m, i) => {
        const v = values[i]
        const yEnd = h - pad - ((v - minV) / range) * (h - 2 * pad)
        const barH = Math.abs(yEnd - yZero)
        const yTop = v >= 0 ? yEnd : yZero
        return (
          <g key={m}>
            <rect x={xs(i) + 2} y={yTop} width={barW} height={barH || 1} fill={v >= 0 ? '#00d68f' : '#ef4444'} />
            {i % Math.max(Math.floor(months.length / 5), 1) === 0 && (
              <text x={xs(i) + barW / 2} y={h - pad + 12} fontSize="8" fill="#6b7280" textAnchor="middle">{m.slice(2)}</text>
            )}
          </g>
        )
      })}
      <text x={pad} y={pad - 8} fontSize="9" fill="#6b7280">{fmtUsd(maxV)}</text>
      <text x={pad} y={h - pad + 24} fontSize="9" fill="#6b7280">{fmtUsd(minV)}</text>
    </svg>
  )
}

function MarketChart({ data, onClose }) {
  if (!data) return null
  const ph = data.price_history || []
  const buys = data.wallet_entries || []
  const sells = data.wallet_exits || []
  const summary = data.position_summary || {}
  const ourEntry = data.backtest_entry

  const w = 800, h = 320, pad = 40
  let svg = null
  if (ph.length >= 2) {
    const allTs = ph.map(p => p.t)
    const minT = Math.min(...allTs)
    const maxT = Math.max(...allTs)
    const tRange = maxT - minT || 1
    const xs = t => pad + ((t - minT) / tRange) * (w - 2 * pad)
    const ys = p => h - pad - p * (h - 2 * pad)

    const path = ph.map((pt, i) => `${i === 0 ? 'M' : 'L'} ${xs(pt.t)} ${ys(pt.p)}`).join(' ')

    svg = (
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height: 320 }}>
        {/* y-axis labels */}
        {[0, 0.25, 0.5, 0.75, 1].map(p => (
          <g key={p}>
            <line x1={pad} y1={ys(p)} x2={w - pad} y2={ys(p)} stroke="#1f2937" strokeWidth="0.5" />
            <text x={pad - 4} y={ys(p) + 3} fontSize="9" fill="#6b7280" textAnchor="end">{(p * 100).toFixed(0)}%</text>
          </g>
        ))}
        {/* price line */}
        <path d={path} fill="none" stroke="#3b82f6" strokeWidth="1.5" />
        {/* avg entry dashed line */}
        {summary.avg_entry_price && (
          <line x1={pad} y1={ys(summary.avg_entry_price)} x2={w - pad} y2={ys(summary.avg_entry_price)}
                stroke="#fbbf24" strokeWidth="1" strokeDasharray="4 4" />
        )}
        {/* wallet buys: green up triangles */}
        {buys.map((b, i) => (
          <g key={`buy-${i}`}>
            <polygon points={`${xs(b.ts)},${ys(b.price) - 6} ${xs(b.ts) - 5},${ys(b.price) + 2} ${xs(b.ts) + 5},${ys(b.price) + 2}`}
                     fill="#00d68f" />
          </g>
        ))}
        {/* wallet sells: red down triangles */}
        {sells.map((s, i) => (
          <polygon key={`sell-${i}`}
                   points={`${xs(s.ts)},${ys(s.price) + 6} ${xs(s.ts) - 5},${ys(s.price) - 2} ${xs(s.ts) + 5},${ys(s.price) - 2}`}
                   fill="#ef4444" />
        ))}
        {/* our backtest entry: blue triangle */}
        {ourEntry && (
          <polygon points={`${xs(ourEntry.ts)},${ys(ourEntry.price) - 8} ${xs(ourEntry.ts) - 6},${ys(ourEntry.price) + 4} ${xs(ourEntry.ts) + 6},${ys(ourEntry.price) + 4}`}
                   fill="#3b82f6" stroke="white" strokeWidth="1" />
        )}
      </svg>
    )
  }

  return (
    <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-surface-bg border border-surface-border rounded-lg max-w-5xl w-full p-4" onClick={e => e.stopPropagation()}>
        <div className="flex items-start justify-between mb-3">
          <div>
            <h3 className="text-sm font-medium text-gray-200">{summary.question || 'Market'}</h3>
            <p className="text-[10px] text-gray-500">{summary.category} · entered {fmtTs(summary.first_entry_ts)}</p>
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-300 text-xl">&times;</button>
        </div>
        {ph.length < 2 ? (
          <p className="text-xs text-gray-500 p-8 text-center">No price history available for this market</p>
        ) : svg}
        <div className="grid grid-cols-4 gap-2 mt-3 text-[10px]">
          <div className="bg-surface-card rounded p-2">
            <p className="text-gray-500">Wallet Entry</p>
            <p className="font-mono text-gray-200">{summary.avg_entry_price ? (summary.avg_entry_price * 100).toFixed(1) + '%' : '—'}</p>
          </div>
          <div className="bg-surface-card rounded p-2">
            <p className="text-gray-500">Our Entry</p>
            <p className="font-mono text-accent-blue">{ourEntry ? (ourEntry.price * 100).toFixed(1) + '%' : '—'}</p>
          </div>
          <div className="bg-surface-card rounded p-2">
            <p className="text-gray-500">Resolution</p>
            <p className="font-mono text-gray-200">{summary.resolution_price != null ? (summary.resolution_price * 100).toFixed(0) + '%' : '—'}</p>
          </div>
          <div className="bg-surface-card rounded p-2">
            <p className="text-gray-500">Wallet PnL</p>
            <p className={`font-mono ${(summary.realized_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
              {fmtUsd(summary.realized_pnl)}
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}

// 10-color palette for per-wallet markers in MarketIntelligence chart
const WALLET_COLORS = [
  '#3b82f6', '#00d68f', '#fbbf24', '#ef4444', '#a855f7',
  '#06b6d4', '#f97316', '#ec4899', '#84cc16', '#14b8a6',
]

function MarketIntelligenceChart({ data }) {
  if (!data) return null
  const ph = data.price_history || []
  const wallets = data.wallet_activity || []
  const conv = data.convergence_signal || {}

  const w = 800, h = 350, pad = 40
  if (ph.length < 2) {
    return <p className="text-xs text-gray-500 p-8 text-center">No price history available for this market</p>
  }
  const allTs = ph.map(p => p.t)
  const minT = Math.min(...allTs)
  const maxT = Math.max(...allTs)
  const tRange = maxT - minT || 1
  const xs = t => pad + ((t - minT) / tRange) * (w - 2 * pad)
  const ys = p => h - pad - p * (h - 2 * pad)
  const path = ph.map((pt, i) => `${i === 0 ? 'M' : 'L'} ${xs(pt.t)} ${ys(pt.p)}`).join(' ')

  // Compute max fill notional for proportional triangle sizing
  const allFills = []
  wallets.forEach(wlt => {
    (wlt.entry_fills || []).forEach(f => allFills.push(f))
    ;(wlt.exit_fills || []).forEach(f => allFills.push(f))
  })
  const maxNotional = allFills.reduce((m, f) => Math.max(m, f.notional_usdc || 0), 0) || 1
  const sizeFor = (n) => 4 + Math.sqrt((n || 0) / maxNotional) * 8  // 4-12px

  // Convergence highlight band: from 1st to convergence ts of majority side
  const majSide = data.aggregated?.consensus_side
  const inside = wallets.filter(w => (w.side || 'YES').toUpperCase() === majSide)
  const convStartTs = inside.length >= 2 ? Math.min(...inside.map(w => w.first_entry_ts || 0)) : null

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height: 350 }}>
      <defs>
        <linearGradient id="bgGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#00d68f" stopOpacity="0.05" />
          <stop offset="50%" stopColor="#000000" stopOpacity="0" />
          <stop offset="100%" stopColor="#ef4444" stopOpacity="0.05" />
        </linearGradient>
      </defs>
      <rect x={pad} y={pad} width={w - 2 * pad} height={h - 2 * pad} fill="url(#bgGrad)" />
      {[0, 0.25, 0.5, 0.75, 1].map(p => (
        <g key={p}>
          <line x1={pad} y1={ys(p)} x2={w - pad} y2={ys(p)} stroke="#1f2937" strokeWidth="0.5" />
          <text x={pad - 4} y={ys(p) + 3} fontSize="9" fill="#6b7280" textAnchor="end">{(p * 100).toFixed(0)}%</text>
        </g>
      ))}
      {conv.fired && conv.first_convergence_ts && convStartTs && (
        <rect x={xs(convStartTs)} y={pad}
              width={Math.max(2, xs(conv.first_convergence_ts) - xs(convStartTs))}
              height={h - 2 * pad} fill="#fbbf24" opacity="0.08" />
      )}
      {data.market_resolved && data.resolution_price != null && (
        <>
          <line x1={pad} y1={ys(data.resolution_price)} x2={w - pad} y2={ys(data.resolution_price)}
                stroke="#fbbf24" strokeWidth="1" strokeDasharray="4 4" />
          <text x={w - pad + 4} y={ys(data.resolution_price) + 3} fontSize="9" fill="#fbbf24">
            {(data.resolution_price * 100).toFixed(0)}%
          </text>
        </>
      )}
      <path d={path} fill="none" stroke="#14b8a6" strokeWidth="1.5" />

      {/* Per-wallet fill markers — type-aware:
            BUY_YES  → filled green up-triangle
            BUY_NO   → filled red  up-triangle
            SELL_YES → outlined green down-triangle
            SELL_NO  → outlined red   down-triangle */}
      {wallets.map((wlt, i) => {
        const label = wlt.pseudonym && !wlt.pseudonym.startsWith('0x')
          ? wlt.pseudonym : (wlt.wallet || '').slice(0, 8)
        const allF = [...(wlt.entry_fills || []), ...(wlt.exit_fills || [])]
        return allF.map((f, fi) => {
          const x = xs(f.ts)
          const y = ys(f.price || 0.5)
          const sz = sizeFor(f.notional_usdc)
          const isBuy = (f.type || '').startsWith('BUY')
          const isNo = (f.type || '').endsWith('_NO')
          const color = isNo ? '#ef4444' : '#00d68f'
          const fill = isBuy ? color : 'none'
          const stroke = color
          // up triangle for BUY, down triangle for SELL
          const points = isBuy
            ? `${x},${y - sz} ${x - sz},${y + sz * 0.7} ${x + sz},${y + sz * 0.7}`
            : `${x},${y + sz} ${x - sz},${y - sz * 0.7} ${x + sz},${y - sz * 0.7}`
          const verb = isBuy ? 'BOUGHT' : 'SOLD'
          const tok = isNo ? 'NO' : 'YES'
          const tip = `${label} ${verb} ${Math.round(f.size).toLocaleString()} ${tok} @ ${(f.price * 100).toFixed(1)}% ($${Math.round(f.notional_usdc).toLocaleString()})`
          return (
            <polygon key={`${wlt.wallet}-${fi}`} points={points}
                     fill={fill} stroke={stroke} strokeWidth="1">
              <title>{tip}</title>
            </polygon>
          )
        })
      })}

      {/* Convergence marker */}
      {conv.fired && conv.first_convergence_ts && conv.convergence_price != null && (
        <g>
          <line x1={xs(conv.first_convergence_ts)} y1={pad} x2={xs(conv.first_convergence_ts)} y2={h - pad}
                stroke="#3b82f6" strokeWidth="1" strokeDasharray="2 2" />
          <polygon points={`${xs(conv.first_convergence_ts)},${ys(conv.convergence_price) - 6}
                            ${xs(conv.first_convergence_ts) + 6},${ys(conv.convergence_price)}
                            ${xs(conv.first_convergence_ts)},${ys(conv.convergence_price) + 6}
                            ${xs(conv.first_convergence_ts) - 6},${ys(conv.convergence_price)}`}
                   fill="#3b82f6" stroke="white" strokeWidth="1" />
          <text x={xs(conv.first_convergence_ts) + 8} y={pad + 12} fontSize="9" fill="#3b82f6">
            Convergence: {conv.wallet_count} wallets
          </text>
        </g>
      )}
    </svg>
  )
}

function MarketIntelligence() {
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [selectedCid, setSelectedCid] = useState(null)
  const [marketData, setMarketData] = useState(null)
  const [marketLoading, setMarketLoading] = useState(false)
  const [convConfig, setConvConfig] = useState({
    min_wallets: 2,
    delay_seconds: 86400,
    slippage_pct: 0.02,
    starting_bankroll: 10000,
    stake_per_trade_pct: 0.02,
    wallet_tier: 'tier1',
    start_date: '2024-01-01',
    end_date: new Date().toISOString().slice(0, 10),
  })
  const [convResult, setConvResult] = useState(null)
  const [convLoading, setConvLoading] = useState(false)

  // Initial load: top whale-activity markets
  useEffect(() => {
    setSearchLoading(true)
    api.marketsSearch({ has_whale_activity: true, limit: 20 })
      .then(d => setSearchResults(d.results || []))
      .catch(() => setSearchResults([]))
      .finally(() => setSearchLoading(false))
  }, [])

  const runSearch = async () => {
    setSearchLoading(true)
    try {
      const d = await api.marketsSearch({ q: searchQuery, has_whale_activity: true, limit: 50 })
      setSearchResults(d.results || [])
    } catch {
      setSearchResults([])
    }
    setSearchLoading(false)
  }

  const selectMarket = async (cid) => {
    setSelectedCid(cid)
    setMarketData(null)
    setMarketLoading(true)
    try {
      const d = await api.marketIntelligence(cid)
      setMarketData(d)
    } catch (e) {
      setMarketData({ available: false, error: e.message })
    }
    setMarketLoading(false)
  }

  const runConvergenceBacktest = async () => {
    setConvLoading(true)
    setConvResult(null)
    try {
      const d = await api.backtestConvergence(convConfig)
      setConvResult(d)
    } catch (e) {
      setConvResult({ error: e.message, total_trades: 0 })
    }
    setConvLoading(false)
  }

  return (
    <div className="space-y-4">
      {/* Search bar */}
      <div className="card space-y-3">
        <div className="flex gap-2">
          <input
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && runSearch()}
            placeholder="Search markets by question..."
            className="flex-1 bg-surface-card border border-surface-border rounded px-3 py-2 text-xs text-gray-300"
          />
          <button onClick={runSearch}
                  className="px-4 py-2 rounded text-xs font-medium bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30">
            Search
          </button>
        </div>
        {searchLoading && <p className="text-[10px] text-gray-500">Searching...</p>}
        {!searchLoading && searchResults.length > 0 && !selectedCid && (
          <div className="max-h-60 overflow-auto divide-y divide-surface-border">
            {searchResults.map(m => (
              <div key={m.condition_id}
                   onClick={() => selectMarket(m.condition_id)}
                   className="py-1.5 px-2 hover:bg-surface-hover cursor-pointer flex items-center justify-between gap-3">
                <span className="text-[11px] text-gray-300 truncate flex-1">{m.question}</span>
                <span className="text-[10px] text-gray-500 whitespace-nowrap">
                  <span className="text-accent-blue">{m.whale_count}</span> whales · {m.category || '—'}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Selected market intelligence */}
      {selectedCid && (
        <>
          <div className="flex items-center justify-between">
            <button onClick={() => { setSelectedCid(null); setMarketData(null); setConvResult(null) }}
                    className="text-[10px] text-gray-500 hover:text-gray-300">← Back to search</button>
          </div>

          {marketLoading && <LoadingSkeleton rows={4} />}
          {marketData && marketData.available && (
            <>
              <div className="card">
                <div className="mb-2">
                  <h2 className="text-sm font-medium text-gray-200">{marketData.question}</h2>
                  <div className="flex gap-2 mt-1 text-[10px]">
                    <span className="px-1.5 py-0.5 rounded bg-surface-card text-gray-400 capitalize">{marketData.category || 'unknown'}</span>
                    <span className={`px-1.5 py-0.5 rounded ${marketData.market_resolved ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-blue/20 text-accent-blue'}`}>
                      {marketData.market_resolved ? 'resolved' : 'active'}
                    </span>
                    <span className="px-1.5 py-0.5 rounded bg-surface-card text-gray-400">
                      {marketData.wallet_activity.length} whales
                    </span>
                    <span className={`px-1.5 py-0.5 rounded ${
                      marketData.aggregated.consensus_side === 'YES' ? 'bg-accent-green/20 text-accent-green' :
                      marketData.aggregated.consensus_side === 'NO' ? 'bg-accent-red/20 text-accent-red' :
                      'bg-surface-card text-gray-400'
                    }`}>
                      consensus: {marketData.aggregated.consensus_side}
                    </span>
                  </div>
                </div>
                <MarketIntelligenceChart data={marketData} />
              </div>

              {/* Wallet activity table */}
              <div className="card">
                <h3 className="text-sm font-medium text-gray-300 mb-2">Wallet Activity</h3>
                <table className="w-full text-[10px]">
                  <thead>
                    <tr className="border-b border-surface-border text-gray-500 text-left">
                      <th className="pb-1 w-8">#</th>
                      <th className="pb-1">Wallet</th>
                      <th className="pb-1">Tier</th>
                      <th className="pb-1">Side</th>
                      <th className="pb-1 text-right">Entry</th>
                      <th className="pb-1">Date</th>
                      <th className="pb-1 text-right">Position</th>
                      <th className="pb-1 text-right">% Sold</th>
                      <th className="pb-1 text-right">Hedge</th>
                      <th className="pb-1">Status</th>
                      <th className="pb-1 text-right">PnL</th>
                      <th className="pb-1">vs Consensus</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-surface-border">
                    {marketData.wallet_activity.map((w, i) => {
                      const color = WALLET_COLORS[i % WALLET_COLORS.length]
                      const isMaj = (w.side || '').toUpperCase() === marketData.aggregated.consensus_side
                      const label = w.pseudonym && !w.pseudonym.startsWith('0x') ? w.pseudonym : (w.wallet || '').slice(0, 12)
                      const pctSold = (w.pct_sold || 0) * 100
                      const hedgePct = (w.hedge_ratio || 0) * 100
                      return (
                        <tr key={w.wallet} className={(w.side || '').toUpperCase() === 'NO' ? 'bg-accent-red/5' : 'bg-accent-green/5'}>
                          <td className="py-1"><span className="inline-block w-2 h-2 rounded-full" style={{ background: color }} /></td>
                          <td className="py-1 text-gray-300">{label}</td>
                          <td className="py-1 text-gray-500">{w.tier}</td>
                          <td className="py-1 text-gray-400">{w.side}</td>
                          <td className="py-1 text-right text-gray-300">{w.avg_entry_price != null ? (w.avg_entry_price * 100).toFixed(1) + '%' : '—'}</td>
                          <td className="py-1 text-gray-500">{fmtTs(w.first_entry_ts)}</td>
                          <td className="py-1 text-right font-mono text-gray-300">{fmtUsd(w.net_position_usdc || w.total_cost_usdc)}</td>
                          <td className="py-1 text-right text-gray-400">{pctSold > 0 ? pctSold.toFixed(0) + '%' : '—'}</td>
                          <td className={`py-1 text-right ${w.is_hedged ? 'text-accent-yellow' : 'text-gray-600'}`}>
                            {w.is_hedged ? `~${hedgePct.toFixed(0)}%` : '—'}
                          </td>
                          <td className={`py-1 ${
                            w.status === 'Closed' ? 'text-gray-500' :
                            w.status === 'Partially Closed' ? 'text-accent-yellow' :
                            'text-accent-green'
                          }`}>{w.status}</td>
                          <td className={`py-1 text-right font-mono ${(w.realized_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                            {fmtUsd(w.realized_pnl)}
                          </td>
                          <td className={`py-1 ${isMaj ? 'text-accent-green' : 'text-accent-red'}`}>
                            {isMaj ? '✓ With' : '✗ Against'}
                          </td>
                        </tr>
                      )
                    })}
                    {/* Aggregated row */}
                    <tr className="border-t-2 border-surface-border bg-surface-card">
                      <td colSpan="2" className="py-1 font-medium text-gray-300">CONSENSUS</td>
                      <td className="py-1 text-gray-400">—</td>
                      <td className="py-1 text-gray-300">{marketData.aggregated.consensus_side}</td>
                      <td className="py-1 text-right text-gray-300">{marketData.aggregated.avg_majority_entry != null ? (marketData.aggregated.avg_majority_entry * 100).toFixed(1) + '%' : '—'}</td>
                      <td className="py-1 text-gray-500">{fmtTs(marketData.aggregated.first_entry_ts)}</td>
                      <td className="py-1 text-right font-mono text-gray-300">{fmtUsd(marketData.aggregated.total_volume_usdc)}</td>
                      <td colSpan="3" className="py-1"></td>
                      <td className="py-1"></td>
                      <td className="py-1 text-gray-500">{(marketData.aggregated.consensus_strength * 100).toFixed(0)}%</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </>
          )}
          {marketData && marketData.available === false && (
            <p className="text-xs text-accent-red">Failed to load market: {marketData.reason || marketData.error}</p>
          )}
        </>
      )}

      {/* Convergence backtest panel (always visible) */}
      <div className="card">
        <h3 className="text-sm font-medium text-gray-300 mb-2">Convergence Signal Backtest</h3>
        <div className="flex flex-wrap items-end gap-3 mb-3">
          <div>
            <label className="text-[10px] text-gray-500 block">Min wallets</label>
            <select value={convConfig.min_wallets}
                    onChange={e => setConvConfig({ ...convConfig, min_wallets: parseInt(e.target.value) })}
                    className="bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300">
              {[2, 3, 4, 5].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
          <div>
            <label className="text-[10px] text-gray-500 block">Delay</label>
            <select value={convConfig.delay_seconds}
                    onChange={e => setConvConfig({ ...convConfig, delay_seconds: parseInt(e.target.value) })}
                    className="bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300">
              <option value={3600}>1h</option>
              <option value={21600}>6h</option>
              <option value={86400}>24h</option>
              <option value={259200}>3d</option>
            </select>
          </div>
          <div>
            <label className="text-[10px] text-gray-500 block">Tier</label>
            <select value={convConfig.wallet_tier}
                    onChange={e => setConvConfig({ ...convConfig, wallet_tier: e.target.value })}
                    className="bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300">
              <option value="tier1h">tier1h only</option>
              <option value="tier1">tier1 + tier1h</option>
            </select>
          </div>
          <div>
            <label className="text-[10px] text-gray-500 block">Slippage %</label>
            <input type="number" step="0.01" value={convConfig.slippage_pct * 100}
                   onChange={e => setConvConfig({ ...convConfig, slippage_pct: (parseFloat(e.target.value) || 0) / 100 })}
                   className="w-20 bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
          </div>
          <button onClick={runConvergenceBacktest} disabled={convLoading}
                  className="px-3 py-1 rounded text-xs font-medium bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 disabled:opacity-50">
            {convLoading ? 'Running...' : '▶ Run'}
          </button>
        </div>

        {convResult && convResult.total_trades > 0 && (
          <>
            <div className="grid grid-cols-4 gap-2 mb-3">
              <div className="bg-surface-card rounded p-2">
                <p className="text-[10px] text-gray-500">SIGNALS</p>
                <p className="text-lg font-bold font-mono text-gray-200">{convResult.total_trades}</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[10px] text-gray-500">WIN RATE</p>
                <p className="text-lg font-bold font-mono text-gray-200">{(convResult.win_rate * 100).toFixed(0)}%</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[10px] text-gray-500">TOTAL P&L</p>
                <p className={`text-lg font-bold font-mono ${convResult.total_pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {convResult.total_pnl >= 0 ? '+' : ''}{fmtUsd(convResult.total_pnl)}
                </p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[10px] text-gray-500">AVG WALLETS</p>
                <p className="text-lg font-bold font-mono text-gray-200">{convResult.convergence_stats?.avg_wallets_per_signal ?? '—'}</p>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <h4 className="text-[11px] text-gray-400 mb-1">Monthly P&L</h4>
                <MonthlyBarChart byMonth={convResult.by_month || {}} />
              </div>
              <div>
                <h4 className="text-[11px] text-gray-400 mb-1">By Wallet Count</h4>
                <table className="w-full text-[10px]">
                  <thead>
                    <tr className="text-gray-500 text-left border-b border-surface-border">
                      <th className="pb-1">Wallets</th>
                      <th className="pb-1 text-right">Trades</th>
                      <th className="pb-1 text-right">WR</th>
                      <th className="pb-1 text-right">PnL</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-surface-border">
                    {Object.entries(convResult.convergence_stats?.by_wallet_count || {})
                      .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
                      .map(([wc, v]) => (
                        <tr key={wc}>
                          <td className="py-1 text-gray-400">{wc}+</td>
                          <td className="py-1 text-right">{v.trades}</td>
                          <td className="py-1 text-right">{(v.win_rate * 100).toFixed(0)}%</td>
                          <td className={`py-1 text-right font-mono ${v.pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                            {v.pnl >= 0 ? '+' : ''}{fmtUsd(v.pnl)}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
        {convResult && convResult.total_trades === 0 && (
          <p className="text-[10px] text-gray-500">
            No convergence signals matched. Lower min_wallets or expand date range.
          </p>
        )}
      </div>
    </div>
  )
}

export default function Backtest() {
  const [tab, setTab] = useState('single')
  const [wallets, setWallets] = useState([])
  const [config, setConfig] = useState({
    wallet: '',
    start_date: '2025-10-01',
    end_date: new Date().toISOString().slice(0, 10),
    delay_seconds: 300,
    slippage_pct: 0.02,
    starting_bankroll: 10000,
    stake_per_trade_pct: 0.02,
  })
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [selectedTrade, setSelectedTrade] = useState(null)
  const [marketView, setMarketView] = useState(null)

  // Load wallet list once
  useEffect(() => {
    fetch('/api/smart-money/winners?window=all')
      .then(r => r.json())
      .then(d => {
        const list = (d.data || []).filter(w => w.tier && w.tier !== 'unranked')
        setWallets(list)
        if (list.length && !config.wallet) {
          setConfig(c => ({ ...c, wallet: list[0].wallet }))
        }
      })
      .catch(() => {})
  }, [])

  const runBacktest = async () => {
    if (!config.wallet) return
    setLoading(true)
    setResult(null)
    try {
      const r = await api.backtestRun(config)
      setResult(r)
    } catch (e) {
      setResult({ error: e.message, total_trades: 0, trades: [] })
    }
    setLoading(false)
  }

  const openTrade = async (trade) => {
    setSelectedTrade(trade)
    try {
      const data = await api.backtestWalletMarket(config.wallet, trade.condition_id)
      setMarketView(data)
    } catch (e) {
      setMarketView({ error: e.message })
    }
  }

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Backtest</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Wallet Backtester</h1>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-surface-border">
        {[
          { id: 'single',   label: 'Single Wallet' },
          { id: 'market',   label: 'Market Intelligence' },
        ].map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
                  className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                    tab === t.id
                      ? 'text-accent-blue border-b-2 border-accent-blue -mb-px'
                      : 'text-gray-500 hover:text-gray-300'
                  }`}>
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'market' && <MarketIntelligence />}

      {tab === 'single' && (
      <div className="flex gap-4">
        {/* Left: Config */}
        <div className="w-72 card space-y-3 flex-shrink-0">
          <h2 className="text-sm font-medium text-gray-300">Configuration</h2>
          <div>
            <label className="text-[10px] text-gray-500">Wallet</label>
            <select value={config.wallet} onChange={e => setConfig({ ...config, wallet: e.target.value })}
                    className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300">
              <option value="">Select wallet...</option>
              {wallets.map(w => (
                <option key={w.wallet} value={w.wallet}>
                  {w.pseudonym && !w.pseudonym.startsWith('0x') ? w.pseudonym : w.wallet.slice(0, 12) + '...'} [{w.tier}]
                </option>
              ))}
            </select>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-[10px] text-gray-500">Start</label>
              <input type="date" value={config.start_date}
                     onChange={e => setConfig({ ...config, start_date: e.target.value })}
                     className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
            </div>
            <div>
              <label className="text-[10px] text-gray-500">End</label>
              <input type="date" value={config.end_date}
                     onChange={e => setConfig({ ...config, end_date: e.target.value })}
                     className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
            </div>
          </div>
          <div>
            <label className="text-[10px] text-gray-500">Delay (seconds)</label>
            <input type="number" value={config.delay_seconds}
                   onChange={e => setConfig({ ...config, delay_seconds: parseInt(e.target.value) || 0 })}
                   className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
          </div>
          <div>
            <label className="text-[10px] text-gray-500">Slippage (%)</label>
            <input type="number" step="0.01" value={config.slippage_pct * 100}
                   onChange={e => setConfig({ ...config, slippage_pct: (parseFloat(e.target.value) || 0) / 100 })}
                   className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
          </div>
          <div>
            <label className="text-[10px] text-gray-500">Bankroll ($)</label>
            <input type="number" value={config.starting_bankroll}
                   onChange={e => setConfig({ ...config, starting_bankroll: parseFloat(e.target.value) || 0 })}
                   className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
          </div>
          <div>
            <label className="text-[10px] text-gray-500">Stake / trade (%)</label>
            <input type="number" step="0.1" value={config.stake_per_trade_pct * 100}
                   onChange={e => setConfig({ ...config, stake_per_trade_pct: (parseFloat(e.target.value) || 0) / 100 })}
                   className="w-full bg-surface-card border border-surface-border rounded px-2 py-1 text-xs text-gray-300" />
          </div>
          <button onClick={runBacktest} disabled={loading || !config.wallet}
                  className="w-full px-3 py-2 rounded text-xs font-medium bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 disabled:opacity-50">
            {loading ? 'Running...' : '▶ Run Backtest'}
          </button>
        </div>

        {/* Right: Results */}
        <div className="flex-1 space-y-4">
          {loading && <LoadingSkeleton rows={6} />}
          {result?.error && <p className="text-xs text-accent-red">{result.error}</p>}
          {result && !result.error && result.total_trades > 0 && (
            <>
              <div className="grid grid-cols-4 gap-3">
                <div className="bg-surface-card rounded-lg p-3">
                  <p className="text-[10px] text-gray-500">TOTAL P&L</p>
                  <p className={`text-xl font-bold font-mono ${(result.total_pnl || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                    {(result.total_pnl || 0) >= 0 ? '+' : ''}{fmtUsd(result.total_pnl)}
                  </p>
                  <p className="text-[9px] text-gray-500">{result.total_pnl_pct}%</p>
                </div>
                <div className="bg-surface-card rounded-lg p-3">
                  <p className="text-[10px] text-gray-500">WIN RATE</p>
                  <p className="text-xl font-bold font-mono text-gray-200">{(result.win_rate * 100).toFixed(0)}%</p>
                  <p className="text-[9px] text-gray-500">{result.winning_trades}/{result.total_trades}</p>
                </div>
                <div className="bg-surface-card rounded-lg p-3">
                  <p className="text-[10px] text-gray-500">MAX DRAWDOWN</p>
                  <p className="text-xl font-bold font-mono text-gray-200">{(result.max_drawdown * 100).toFixed(1)}%</p>
                  <p className="text-[9px] text-gray-500">Sharpe: {result.sharpe_ratio ?? '—'}</p>
                </div>
                <div className="bg-surface-card rounded-lg p-3">
                  <p className="text-[10px] text-gray-500">ALPHA CAPTURE</p>
                  <p className="text-xl font-bold font-mono text-gray-200">{result.alpha_capture_pct != null ? `${result.alpha_capture_pct}%` : '—'}</p>
                  <p className="text-[9px] text-gray-500">
                    vs wallet {fmtUsd(result.wallet_pnl_in_period)}
                    {result.wallet_pnl_source === 'pm_authoritative' && (
                      <span className="text-accent-blue ml-1" title="Polymarket authoritative pm_pnl">PM</span>
                    )}
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="card">
                  <h2 className="text-sm font-medium text-gray-300 mb-2">Monthly P&L</h2>
                  <MonthlyBarChart byMonth={result.by_month || {}} />
                </div>
                <div className="card">
                  <h2 className="text-sm font-medium text-gray-300 mb-2">By Category</h2>
                  <table className="w-full text-[10px]">
                    <thead>
                      <tr className="border-b border-surface-border text-gray-500 text-left">
                        <th className="pb-1">Category</th>
                        <th className="pb-1 text-right">Trades</th>
                        <th className="pb-1 text-right">WR</th>
                        <th className="pb-1 text-right">PnL</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-surface-border">
                      {Object.entries(result.by_category || {}).map(([cat, v]) => (
                        <tr key={cat}>
                          <td className="py-1 capitalize text-gray-400">{cat}</td>
                          <td className="py-1 text-right">{v.trades}</td>
                          <td className="py-1 text-right">{v.trades > 0 ? `${(v.wins / v.trades * 100).toFixed(0)}%` : '—'}</td>
                          <td className={`py-1 text-right font-mono ${v.pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                            {v.pnl >= 0 ? '+' : ''}{fmtUsd(v.pnl)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card">
                <h2 className="text-sm font-medium text-gray-300 mb-2">Trade History ({result.trades.length})</h2>
                <table className="w-full text-[10px]">
                  <thead>
                    <tr className="border-b border-surface-border text-gray-500 text-left">
                      <th className="pb-1">Market</th>
                      <th className="pb-1">Side</th>
                      <th className="pb-1 text-right">Wallet</th>
                      <th className="pb-1 text-right">Our Entry</th>
                      <th className="pb-1 text-right">Exit</th>
                      <th className="pb-1 text-right">Stake</th>
                      <th className="pb-1 text-right">PnL</th>
                      <th className="pb-1">W/L</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-surface-border">
                    {result.trades.map((t, i) => (
                      <tr key={i} className="hover:bg-surface-hover cursor-pointer" onClick={() => openTrade(t)}>
                        <td className="py-1 truncate max-w-[280px] text-gray-400">{t.question}</td>
                        <td className="py-1 text-gray-500">{t.side}</td>
                        <td className="py-1 text-right text-gray-500">{(t.wallet_entry_price * 100).toFixed(0)}%</td>
                        <td className="py-1 text-right text-gray-300">{(t.our_entry_price * 100).toFixed(0)}%</td>
                        <td className="py-1 text-right text-gray-300">{(t.our_exit_price * 100).toFixed(0)}%</td>
                        <td className="py-1 text-right font-mono text-gray-400">{fmtUsd(t.stake)}</td>
                        <td className={`py-1 text-right font-mono ${t.pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                          {t.pnl >= 0 ? '+' : ''}{fmtUsd(t.pnl)}
                        </td>
                        <td className={`py-1 font-bold ${t.won ? 'text-accent-green' : 'text-accent-red'}`}>{t.won ? 'W' : 'L'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
          {result && !result.error && result.total_trades === 0 && (
            <p className="text-xs text-gray-500">No resolved positions in date range. Try expanding the date range or run history collection first.</p>
          )}
        </div>
      </div>
      )}

      {marketView && <MarketChart data={marketView} onClose={() => { setMarketView(null); setSelectedTrade(null) }} />}
    </div>
  )
}

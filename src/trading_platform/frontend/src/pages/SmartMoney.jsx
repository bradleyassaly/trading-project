import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

const TYPE_STYLES = {
  directional: { bg: 'bg-green-900/60', text: 'text-green-300', label: 'Dir' },
  market_maker: { bg: 'bg-yellow-900/60', text: 'text-yellow-300', label: 'MM' },
  arb_bot: { bg: 'bg-gray-800', text: 'text-gray-500', label: 'Bot' },
  unknown: { bg: 'bg-gray-800', text: 'text-gray-400', label: '?' },
}

function truncAddr(addr) {
  if (!addr || addr.length < 12) return addr || '?'
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`
}

function relativeTime(ts) {
  if (!ts) return '?'
  const now = Date.now() / 1000
  const t = typeof ts === 'string' ? new Date(ts).getTime() / 1000 : ts
  const diff = now - t
  if (diff < 60) return '<1m'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function capitalize(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : '' }

function getBestDomain(wallet) {
  if (wallet.best_domain && wallet.best_domain !== 'other') {
    return capitalize(wallet.best_domain)
  }
  try {
    const cats = typeof wallet.category_trades === 'string'
      ? JSON.parse(wallet.category_trades)
      : (wallet.category_trades || {})
    const filtered = Object.entries(cats)
      .filter(([k]) => k !== 'other')
      .sort(([, a], [, b]) => b - a)
    return filtered.length > 0 ? capitalize(filtered[0][0]) : 'Other'
  } catch { return '--' }
}

function TypeBadge({ type }) {
  const s = TYPE_STYLES[type] || TYPE_STYLES.unknown
  return <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${s.bg} ${s.text}`}>{s.label}</span>
}

function WrCell({ value }) {
  if (value == null) return <span className="text-gray-600">--</span>
  const pct = (value * 100).toFixed(0)
  const cls = value > 0.5 ? 'text-accent-green' : value < 0.4 ? 'text-accent-red' : 'text-gray-400'
  return <span className={`font-mono ${cls}`}>{pct}%</span>
}

// ── Actionable Signals Strip ────────────────────────────────────────────────
function ActionableStrip() {
  const { data, loading } = useApi(api.smartMoneyActionableSignals, 300_000)
  const signals = data?.data ?? []

  if (loading && !data) return <div className="h-16 animate-pulse bg-surface-card rounded-lg" />

  if (!signals.length) return (
    <div className="bg-surface-card rounded-lg px-4 py-3 text-xs text-gray-500">
      No multi-wallet convergence signals -- signals appear when 2+ directional wallets hold same position
    </div>
  )

  return (
    <div className="flex gap-3 overflow-x-auto pb-1">
      {signals.slice(0, 10).map((s, i) => (
        <div key={i} className={`flex-shrink-0 bg-surface-card rounded-lg p-3 w-56 border-l-2 ${
          s.direction === 'YES' ? 'border-accent-green' : 'border-accent-red'
        }`}>
          <p className="text-xs text-gray-300 truncate mb-1">{(s.market_title || s.token_id || '').slice(0, 50)}</p>
          <div className="flex items-center gap-2 text-[10px]">
            <span className={`px-1 py-0.5 rounded font-bold ${
              s.direction === 'YES' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
            }`}>{s.direction}</span>
            <span className="text-gray-400">${Number(s.weighted_net_volume || 0).toLocaleString()}</span>
            <span className="text-gray-500">{s.directional_wallet_count}w</span>
            {s.hours_since_first_entry != null && <span className="text-gray-600">{Math.round(s.hours_since_first_entry)}h</span>}
          </div>
          <div className="mt-1.5 w-full bg-gray-800 rounded-full h-1">
            <div className={`h-1 rounded-full ${s.direction === 'YES' ? 'bg-accent-green' : 'bg-accent-red'}`}
                 style={{ width: `${Math.min(100, (s.confidence || 0) * 100)}%` }} />
          </div>
        </div>
      ))}
    </div>
  )
}

// ── Leaderboard Tab ─────────────────────────────────────────────────────────
function LeaderboardTab({ onSelectWallet }) {
  const { data: lb, loading: lbL } = useApi(api.smartMoneyLeaderboard, 600_000)
  const { data: fallback } = useApi(api.smartMoneyWallets, 600_000)
  const useNew = lb?.available && lb?.data?.length > 0
  const rows = useNew ? lb.data : (fallback?.data ?? [])

  if (lbL && !lb) return <LoadingSkeleton rows={10} />
  if (!rows.length) return <EmptyState title="No wallet profiles" message="Run: trading-cli data polymarket goldsky-wallet-profiles" />

  // Trust API order (equity_score DESC, bots last) — no client-side re-sort

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-2 w-8">#</th>
            <th className="pb-2 pr-3">Wallet</th>
            <th className="pb-2 pr-3">Type</th>
            <th className="pb-2 pr-3 text-right">Eq.Score</th>
            <th className="pb-2 pr-3 text-right" title="Directional win rate (excludes tweet-count and sports markets)">Dir WR</th>
            <th className="pb-2 pr-3 text-right">Net P&L</th>
            <th className="pb-2 pr-3 text-right" title="Profit factor: total wins / total losses">P.Factor</th>
            <th className="pb-2 pr-3">Domain</th>
            <th className="pb-2 pr-3 text-right">Trades</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {rows.slice(0, 50).map((w, i) => {
            const wt = w.wallet_type || 'unknown'
            const isBot = wt === 'arb_bot' || wt === 'market_maker'
            const eq = w.equity_score
            const eqCls = eq > 0.3 ? 'text-accent-green' : eq > 0.1 ? 'text-yellow-400' : 'text-gray-600'
            const domain = getBestDomain(w)
            const pnl = w.net_pnl_usdc
            const pf = w.profit_factor
            const pfCls = pf > 1.5 ? 'text-accent-green' : pf > 1 ? 'text-yellow-400' : pf > 0 ? 'text-accent-red' : 'text-gray-600'
            const vt = w.volume_tier
            const vtBadge = vt === 'whale' ? ' W' : vt === 'active' ? ' A' : ''
            return (
              <tr key={w.wallet} className={`hover:bg-surface-hover cursor-pointer ${isBot ? 'opacity-40' : ''}`}
                  onClick={() => onSelectWallet(w.wallet)}>
                <td className="py-1.5 pr-2 text-gray-600">{i + 1}</td>
                <td className="py-1.5 pr-3 font-mono text-[10px] text-accent-blue">
                  {truncAddr(w.wallet)}
                </td>
                <td className="py-1.5 pr-3">
                  <TypeBadge type={wt} />
                  {vtBadge && <span className="ml-1 text-[8px] text-gray-500">{vtBadge}</span>}
                </td>
                <td className={`py-1.5 pr-3 text-right font-mono text-[10px] ${eqCls}`}>
                  {eq > 0 ? eq.toFixed(3) : <span className="text-gray-700">--</span>}
                </td>
                <td className="py-1.5 pr-3 text-right" title="Directional win rate (excludes tweet-count and sports)"><WrCell value={w.directional_win_rate} /></td>
                <td className={`py-1.5 pr-3 text-right font-mono text-[10px] ${pnl > 0 ? 'text-accent-green' : pnl < 0 ? 'text-accent-red' : 'text-gray-600'}`}>
                  {pnl != null ? `${pnl >= 0 ? '+' : ''}$${Math.abs(pnl).toLocaleString(undefined, {maximumFractionDigits: 0})}` : '--'}
                </td>
                <td className={`py-1.5 pr-3 text-right font-mono text-[10px] ${pfCls}`}>
                  {pf > 0 ? pf.toFixed(1) : '--'}
                </td>
                <td className="py-1.5 pr-3 text-gray-400 text-[10px]">
                  {domain}
                </td>
                <td className="py-1.5 pr-3 text-right text-gray-400">
                  {w.resolved_trades ?? w.uncertain_early_trades ?? '-'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ── Open Positions Tab ──────────────────────────────────────────────────────
function OpenPositionsTab() {
  const { data, loading } = useApi(api.smartMoneyOpenPositions, 300_000)
  const rows = data?.data ?? []

  if (loading && !data) return <LoadingSkeleton rows={8} />
  if (data?.available === false) return <EmptyState title="No open positions" message="Run: trading-cli data polymarket compute-open-positions" />
  if (!rows.length) return <EmptyState title="No open positions found" />

  // Group by token_id, aggregate
  const grouped = {}
  rows.forEach(r => {
    const k = r.token_id
    if (!grouped[k]) grouped[k] = { ...r, wallets: new Set(), count: 0 }
    grouped[k].wallets.add(r.wallet)
    grouped[k].count += 1
    if ((r.wallet_edge || 0) > (grouped[k].wallet_edge || 0)) {
      grouped[k].top_wallet = r.wallet
      grouped[k].wallet_edge = r.wallet_edge
    }
  })
  const markets = Object.values(grouped).sort((a, b) => (b.net_amount_usdc || 0) - (a.net_amount_usdc || 0))

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Market</th>
            <th className="pb-2 pr-3 text-center">Side</th>
            <th className="pb-2 pr-3 text-right">Net $</th>
            <th className="pb-2 pr-3 text-right">Wallets</th>
            <th className="pb-2 pr-3">Top Wallet</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {markets.slice(0, 40).map((m, i) => {
            const wc = m.wallets?.size || m.count
            return (
              <tr key={i} className={`hover:bg-surface-hover ${wc > 1 ? 'border-l-2 border-accent-green' : ''}`}>
                <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[250px]">
                  {m.market_question || (m.token_id || '').slice(0, 24)}
                </td>
                <td className="py-1.5 pr-3 text-center">
                  <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${
                    m.net_side === 'YES' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
                  }`}>{m.net_side}</span>
                </td>
                <td className="py-1.5 pr-3 text-right font-mono">${Number(m.net_amount_usdc || 0).toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right text-gray-400">{wc}</td>
                <td className="py-1.5 pr-3 font-mono text-[10px] text-gray-500">{truncAddr(m.top_wallet || m.wallet)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ── Alerts Tab ──────────────────────────────────────────────────────────────
// ── Winners Tab ─────────────────────────────────────────────────────────
const _BUCKET_ABBR = {
  concentrated_whale: 'Whale',
  contrarian_whale: 'Contrarian',
  arb_bot: 'Bot',
  market_maker: 'MM',
  directional: 'Dir',
  domain_specialist: 'Specialist',
  portfolio_diversifier: 'Diversifier',
  volume_trader: 'Volume',
  position_builder: 'Builder',
  noise: 'Noise',
  unknown: '-',
}

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(0)}K`
  return `$${Math.round(n)}`
}

function TierPill({ tier }) {
  if (tier === 'tier1h') return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-amber-500/20 text-amber-300" title="High-conviction whale ($500K+ PnL)">T1★</span>
  if (tier === 'tier1') return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-accent-green/20 text-accent-green">T1</span>
  if (tier === 'tier2') return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-blue-500/20 text-blue-300">T2</span>
  return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-gray-800 text-gray-500">-</span>
}

function SourceBadge({ source }) {
  if (source === 'polymarket_30d') return <span className="px-1 py-0.5 rounded text-[8px] font-bold bg-purple-500/20 text-purple-300" title="Verified by Polymarket monthly leaderboard">PM 30d</span>
  if (source === 'polymarket_7d') return <span className="px-1 py-0.5 rounded text-[8px] font-bold bg-purple-500/20 text-purple-300" title="Verified by Polymarket weekly leaderboard">PM 7d</span>
  if (source === 'polymarket_all') return <span className="px-1 py-0.5 rounded text-[8px] font-bold bg-purple-500/20 text-purple-300" title="Verified by Polymarket all-time leaderboard">PM All</span>
  return null
}

function ColoredWr({ value }) {
  if (value == null) return <span className="text-gray-600 font-mono">--</span>
  const pct = value * 100
  const cls = pct >= 60 ? 'text-accent-green' : pct >= 50 ? 'text-yellow-400' : 'text-accent-red'
  return <span className={`font-mono ${cls}`}>{pct.toFixed(0)}%</span>
}

function WinnersTab({ onSelectWallet }) {
  const [window, setWindow] = useState('all')
  const fetcher = useCallback(() => api.smartMoneyWinners(window), [window])
  const { data, loading } = useApi(fetcher, 300_000)
  const rawRows = data?.data ?? []

  // Filter state
  const [tierFilter, setTierFilter] = useState('all')
  const [catFilter, setCatFilter] = useState('all')
  const [bucketFilter, setBucketFilter] = useState('all')
  const [sortField, setSortField] = useState('conviction_score')
  const [sortAsc, setSortAsc] = useState(false)
  const [search, setSearch] = useState('')

  const periods = [
    { key: 'today', label: 'Today' },
    { key: 'weekly', label: 'Weekly' },
    { key: 'monthly', label: 'Monthly' },
    { key: 'all', label: 'All Time' },
  ]

  // Build category and bucket option lists from data
  const allCategories = ['all', ...new Set(rawRows.map(r => r.primary_category).filter(Boolean))]
  const allBuckets = ['all', ...new Set(rawRows.map(r => r.wallet_bucket).filter(Boolean))]

  // Apply filters
  let rows = rawRows
  if (tierFilter !== 'all') rows = rows.filter(r => r.tier === tierFilter)
  if (catFilter !== 'all') rows = rows.filter(r => r.primary_category === catFilter)
  if (bucketFilter !== 'all') rows = rows.filter(r => r.wallet_bucket === bucketFilter)
  if (search) {
    const s = search.toLowerCase()
    rows = rows.filter(r => (r.wallet || '').toLowerCase().startsWith(s))
  }

  // Sort
  rows = [...rows].sort((a, b) => {
    const av = a[sortField] ?? -Infinity
    const bv = b[sortField] ?? -Infinity
    return sortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1)
  })

  const headerClick = (field) => {
    if (sortField === field) setSortAsc(!sortAsc)
    else { setSortField(field); setSortAsc(false) }
  }

  const sortIcon = (field) => sortField === field ? (sortAsc ? ' ▲' : ' ▼') : ''

  return (
    <div>
      <div className="flex gap-1 mb-3">
        {periods.map(p => (
          <button key={p.key}
            className={`px-3 py-1 text-[10px] rounded ${window === p.key ? 'bg-accent-blue/20 text-accent-blue' : 'text-gray-500 hover:text-gray-300'}`}
            onClick={() => setWindow(p.key)}>{p.label}</button>
        ))}
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2 mb-3 text-[10px]">
        <select value={tierFilter} onChange={e => setTierFilter(e.target.value)}
                className="bg-surface-card border border-surface-border rounded px-2 py-1 text-gray-300">
          <option value="all">All Tiers</option>
          <option value="tier1h">High-Conviction (T1★)</option>
          <option value="tier1">Statistical (T1)</option>
          <option value="tier2">Tier 2</option>
          <option value="unranked">Unranked</option>
        </select>
        <select value={catFilter} onChange={e => setCatFilter(e.target.value)}
                className="bg-surface-card border border-surface-border rounded px-2 py-1 text-gray-300 capitalize">
          {allCategories.map(c => <option key={c} value={c}>{c === 'all' ? 'All Categories' : c}</option>)}
        </select>
        <select value={bucketFilter} onChange={e => setBucketFilter(e.target.value)}
                className="bg-surface-card border border-surface-border rounded px-2 py-1 text-gray-300">
          {allBuckets.map(b => <option key={b} value={b}>{b === 'all' ? 'All Buckets' : (_BUCKET_ABBR[b] || b)}</option>)}
        </select>
        <span className="text-gray-600">Sort:</span>
        <select value={sortField} onChange={e => setSortField(e.target.value)}
                className="bg-surface-card border border-surface-border rounded px-2 py-1 text-gray-300">
          <option value="conviction_score">Conviction</option>
          <option value="net_pnl_usdc">Net PnL</option>
          <option value="directional_win_rate">Win Rate</option>
          <option value="rolling_20_wr">Rolling WR</option>
          <option value="volume">Volume</option>
          <option value="trades">Resolved</option>
        </select>
        <input
          type="text"
          placeholder="Search address..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-surface-card border border-surface-border rounded px-2 py-1 text-gray-300 flex-1 max-w-[200px]"
        />
        <span className="text-gray-600 text-[9px]">{rows.length} of {rawRows.length}</span>
      </div>

      {loading && !data ? <LoadingSkeleton rows={8} /> :
       !rows.length ? <EmptyState title="No wallets match filters" message="Adjust filter criteria" /> : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-2 pr-2 w-8">#</th>
                <th className="pb-2 pr-3">Wallet</th>
                <th className="pb-2 pr-2">Tier</th>
                <th className="pb-2 pr-3 text-right cursor-pointer hover:text-gray-300" onClick={() => headerClick('net_pnl_usdc')}>Net PnL{sortIcon('net_pnl_usdc')}</th>
                <th className="pb-2 pr-3 text-right cursor-pointer hover:text-gray-300" onClick={() => headerClick('volume')}>Volume{sortIcon('volume')}</th>
                <th className="pb-2 pr-3 text-right cursor-pointer hover:text-gray-300" onClick={() => headerClick('directional_win_rate')}>Dir WR{sortIcon('directional_win_rate')}</th>
                <th className="pb-2 pr-3 text-right cursor-pointer hover:text-gray-300" onClick={() => headerClick('rolling_20_wr')}>Roll WR{sortIcon('rolling_20_wr')}</th>
                <th className="pb-2 pr-3 text-right cursor-pointer hover:text-gray-300" onClick={() => headerClick('conviction_score')}>Conv{sortIcon('conviction_score')}</th>
                <th className="pb-2 pr-3">Category</th>
                <th className="pb-2 pr-3">Bucket</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {rows.slice(0, 50).map((w, i) => {
                const pnl = w.display_pnl ?? w.pm_pnl ?? w.net_pnl_usdc ?? w.profit ?? 0
                return (
                  <tr key={w.wallet} className="hover:bg-surface-hover cursor-pointer"
                      onClick={() => onSelectWallet?.(w.wallet)}>
                    <td className="py-1.5 pr-2 text-gray-600">{i + 1}</td>
                    <td className="py-1.5 pr-3 text-[10px]">
                      <div className="flex items-center gap-1">
                        {w.pseudonym && !w.pseudonym.startsWith('0x') ? (
                          <span className="text-accent-blue font-medium">{w.pseudonym}</span>
                        ) : (
                          <span className="font-mono text-accent-blue">{truncAddr(w.wallet)}</span>
                        )}
                      </div>
                    </td>
                    <td className="py-1.5 pr-2">
                      <div className="flex items-center gap-1">
                        <TierPill tier={w.tier} />
                        <SourceBadge source={w.leaderboard_source} />
                      </div>
                    </td>
                    <td className={`py-1.5 pr-3 text-right font-mono font-bold ${pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {pnl >= 0 ? '+' : ''}{fmtUsd(pnl)}
                    </td>
                    <td className="py-1.5 pr-3 text-right font-mono text-gray-400">{fmtUsd(w.volume || w.total_volume_usdc)}</td>
                    <td className="py-1.5 pr-3 text-right"><ColoredWr value={w.directional_win_rate} /></td>
                    <td className="py-1.5 pr-3 text-right"><ColoredWr value={w.rolling_20_wr} /></td>
                    <td className="py-1.5 pr-3 text-right font-mono text-gray-500 text-[10px]">{w.conviction_score?.toFixed(3) ?? '—'}</td>
                    <td className="py-1.5 pr-3">
                      <span className="px-1 py-0.5 rounded bg-surface-hover text-[9px] text-gray-400 capitalize">{w.primary_category || '—'}</span>
                    </td>
                    <td className="py-1.5 pr-3 text-[10px] text-gray-500">{_BUCKET_ABBR[w.wallet_bucket] || '-'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <p className="text-[9px] text-gray-600 mt-3">
            Net PnL from non-archived resolved trades. Conviction = directional WR × log(resolved + 1) × type multiplier.
          </p>
        </div>
      )}
    </div>
  )
}

function AlertsTab() {
  const { data, loading } = useApi(useCallback(() => api.smartMoneyAlerts({ limit: 100 }), []), 120_000)
  const raw = data?.data ?? []
  const alerts = raw.filter(a => a.wallet !== '0xaaa' && a.wallet !== '0xbbb')

  if (loading && !data) return <LoadingSkeleton rows={6} />
  if (!alerts.length) return (
    <EmptyState title="No alerts yet" message="Monitor generates these automatically as smart wallets trade" />
  )

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Time</th>
            <th className="pb-2 pr-3">Wallet</th>
            <th className="pb-2 pr-3">Market</th>
            <th className="pb-2 pr-3">Side</th>
            <th className="pb-2 pr-3 text-right">Amount</th>
            <th className="pb-2 pr-3 text-right">Edge</th>
            <th className="pb-2 pr-3">Tier</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {alerts.slice(0, 50).map((a, i) => (
            <tr key={i} className="hover:bg-surface-hover">
              <td className="py-1.5 pr-3 text-gray-500">{relativeTime(a.ts || a.detected_at)}</td>
              <td className="py-1.5 pr-3 font-mono text-[10px] text-accent-blue">{truncAddr(a.wallet)}</td>
              <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[200px]">{a.market_question || a.market_title || a.token_id?.slice(0, 20)}</td>
              <td className={`py-1.5 pr-3 font-bold ${a.side === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>{a.side}</td>
              <td className="py-1.5 pr-3 text-right font-mono">${Number(a.amount_usdc || a.size || 0).toLocaleString()}</td>
              <td className="py-1.5 pr-3 text-right font-mono text-accent-green">
                {a.wallet_edge != null ? `${(a.wallet_edge * 100).toFixed(0)}%` : '-'}
              </td>
              <td className="py-1.5 pr-3">
                <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${
                  a.tier === 1 ? 'bg-accent-green/20 text-accent-green' : 'bg-yellow-900/60 text-yellow-300'
                }`}>T{a.tier}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Live Signals Tab (preserved from original) ──────────────────────────────
function LiveSignalsTab() {
  const { data: sigData, loading: sigL } = useApi(api.smartMoneySignals, 60_000)
  const { data: mirData, loading: mirL } = useApi(api.smartMoneyMirror, 30_000)
  const signals = sigData?.data ?? []
  const mirror = mirData?.data ?? []

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-xs font-medium text-gray-500 mb-3">Live Market Signals</h3>
        {sigL && !sigData ? <LoadingSkeleton rows={4} /> :
         !signals.length ? <p className="text-xs text-gray-500">No live signals</p> : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-2 pr-3">Token</th><th className="pb-2 pr-3">Dir</th>
                <th className="pb-2 pr-3 text-right">Weighted $</th><th className="pb-2 pr-3 text-right">Conf</th>
                <th className="pb-2 pr-3 text-right">Edge</th><th className="pb-2 pr-3 text-right">Trades</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {signals.slice(0, 20).map((s, i) => (
                <tr key={i} className="hover:bg-surface-hover">
                  <td className="py-1.5 pr-3 font-mono text-[10px] text-gray-400">{s.token_id}</td>
                  <td className={`py-1.5 pr-3 font-bold ${s.direction === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>{s.direction}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">${Number(s.weighted_net_volume || 0).toLocaleString()}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{((s.confidence || 0) * 100).toFixed(0)}%</td>
                  <td className="py-1.5 pr-3 text-right font-mono text-accent-green">{((s.top_wallet_edge || 0) * 100).toFixed(1)}%</td>
                  <td className="py-1.5 pr-3 text-right text-gray-400">{s.smart_trade_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="border-t border-surface-border pt-4">
        <div className="flex items-center gap-2 mb-3">
          <h3 className="text-xs font-medium text-gray-500">Mirror Feed</h3>
          <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent-green/20 text-accent-green">
            <span className="w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" /> LIVE
          </span>
        </div>
        {mirL && !mirData ? <LoadingSkeleton rows={3} /> :
         !mirror.length ? <p className="text-xs text-gray-500">No mirror signals in last 90 min</p> : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-2 pr-3">Time</th><th className="pb-2 pr-3">Wallet</th>
                <th className="pb-2 pr-3">Market</th><th className="pb-2 pr-3">Dir</th>
                <th className="pb-2 pr-3 text-right">Amount</th><th className="pb-2 pr-3 text-right">Edge</th>
                <th className="pb-2 pr-3">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {mirror.slice(0, 15).map((s, i) => (
                <tr key={i} className={`hover:bg-surface-hover ${s.tradeable ? 'bg-accent-green/5' : ''}`}>
                  <td className="py-1.5 pr-3 text-gray-400">{(s.minutes_since_fill ?? 999) < 60 ? `${Math.round(s.minutes_since_fill)}m` : `${(s.minutes_since_fill / 60).toFixed(1)}h`}</td>
                  <td className="py-1.5 pr-3 font-mono text-[10px] text-accent-blue">{s.wallet}</td>
                  <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[160px]">{s.question || s.token_id || 'untracked'}</td>
                  <td className={`py-1.5 pr-3 font-bold ${s.direction === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>{s.direction}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">${Number(s.fill_amount || 0).toLocaleString()}</td>
                  <td className="py-1.5 pr-3 text-right font-mono text-accent-green">{((s.wallet_edge || 0) * 100).toFixed(0)}%</td>
                  <td className="py-1.5 pr-3">{s.tradeable
                    ? <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent-green/20 text-accent-green">TRADE</span>
                    : <span className="text-[9px] text-gray-600">illiquid</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

// ── Wallet Detail Panel ─────────────────────────────────────────────────────
function fmtUsd2(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`
  return `$${Math.round(n)}`
}
function fmtPct2(n) { return n != null ? `${(n * 100).toFixed(1)}%` : '—' }

function MiniPnlChart({ history }) {
  if (!history || history.length < 3) return null
  const w = 320, h = 80, pad = 8
  const minV = Math.min(0, ...history.map(p => p.cumulative))
  const maxV = Math.max(0, ...history.map(p => p.cumulative))
  const range = maxV - minV || 1
  const xs = i => pad + ((w - 2 * pad) * i) / Math.max(history.length - 1, 1)
  const ys = v => h - pad - ((v - minV) / range) * (h - 2 * pad)
  const path = history.map((p, i) => `${i === 0 ? 'M' : 'L'} ${xs(i)} ${ys(p.cumulative)}`).join(' ')
  const final = history[history.length - 1].cumulative
  const color = final >= 0 ? '#00d68f' : '#ef4444'
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height: 80 }}>
      <line x1={pad} y1={ys(0)} x2={w - pad} y2={ys(0)} stroke="#374151" strokeWidth="0.5" strokeDasharray="2 2" />
      <path d={path} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  )
}

function CategoryBars2({ breakdown }) {
  if (!breakdown || Object.keys(breakdown).length === 0) {
    return <p className="text-[9px] text-gray-600">No category data</p>
  }
  const entries = Object.entries(breakdown).sort((a, b) => b[1] - a[1]).slice(0, 5)
  return (
    <div className="space-y-1">
      {entries.map(([cat, pct]) => (
        <div key={cat} className="flex items-center gap-2 text-[9px]">
          <span className="text-gray-400 w-16 capitalize truncate">{cat}</span>
          <div className="flex-1 bg-gray-800 rounded-full h-1.5 overflow-hidden">
            <div className="h-1.5 bg-accent-blue rounded-full" style={{ width: `${pct * 100}%` }} />
          </div>
          <span className="text-gray-500 w-8 text-right">{(pct * 100).toFixed(0)}%</span>
        </div>
      ))}
    </div>
  )
}

function WalletPanel({ address, onClose }) {
  const [detail, setDetail] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!address) return
    setLoading(true)
    fetch(`/api/wallets/${address}`)
      .then(r => r.json())
      .then(d => { setDetail(d); setLoading(false) })
      .catch(() => { setDetail(null); setLoading(false) })
  }, [address])

  // Close on ESC
  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  const d = detail || {}
  const tier = d.tier || 'unranked'
  const tierNum = tier === 'tier1' ? 1 : tier === 'tier2' ? 2 : null
  const trades = d.recent_trades || []

  return (
    <>
      <div className="fixed inset-0 bg-black/40 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 bottom-0 w-[40%] min-w-[380px] bg-surface-bg border-l border-surface-border z-50 overflow-y-auto p-4">
        {loading ? <LoadingSkeleton rows={8} /> : !detail?.available ? (
          <div>
            <button onClick={onClose} className="text-gray-500 hover:text-gray-300">&times; Close</button>
            <p className="text-sm text-gray-500 mt-4">{detail?.reason || 'Wallet profile not found'}</p>
          </div>
        ) : (
          <div className="space-y-3">
            {/* Header */}
            <div className="flex items-center justify-between">
              <div className="flex flex-col flex-1 truncate">
                {d.pseudonym && !d.pseudonym.startsWith('0x') && (
                  <span className="text-sm font-medium text-accent-blue truncate">{d.pseudonym}</span>
                )}
                <span className="font-mono text-[10px] text-gray-500 truncate">{address}</span>
              </div>
              <button onClick={onClose} className="text-gray-500 hover:text-gray-300 text-lg leading-none ml-2">&times;</button>
            </div>

            {/* Identity badges */}
            <div className="flex items-center gap-1.5 flex-wrap">
              {tierNum ? <TierPill tier={tier} /> : <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-gray-800 text-gray-500">UNRANKED</span>}
              <SourceBadge source={d.leaderboard_source} />
              {d.wallet_bucket && (
                <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-surface-card text-gray-400">
                  {_BUCKET_ABBR[d.wallet_bucket] || d.wallet_bucket}
                </span>
              )}
              {d.primary_category && d.primary_category !== 'unknown' && (
                <span className="px-1.5 py-0.5 rounded text-[9px] bg-surface-card text-gray-400 capitalize">{d.primary_category}</span>
              )}
            </div>

            {/* KPI row 1: Core */}
            <div className="grid grid-cols-3 gap-2">
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">DIR WR</p>
                <p className="text-sm font-bold font-mono text-gray-200">{fmtPct2(d.directional_win_rate)}</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">ROLL WR (20)</p>
                <p className="text-sm font-bold font-mono text-gray-200">{fmtPct2(d.rolling_20_wr)}</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">CONVICTION</p>
                <p className="text-sm font-bold font-mono text-gray-200">{d.conviction_score?.toFixed(3) ?? '—'}</p>
              </div>
            </div>

            {/* KPI row 2: Performance */}
            <div className="grid grid-cols-3 gap-2">
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">VOLUME</p>
                <p className="text-sm font-bold font-mono text-gray-200">{fmtUsd2(d.total_volume_usdc)}</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">NET PnL</p>
                <p className={`text-sm font-bold font-mono ${(d.display_pnl || d.net_pnl_usdc || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {(d.display_pnl || d.net_pnl_usdc || 0) >= 0 ? '+' : ''}{fmtUsd2(d.display_pnl || d.net_pnl_usdc)}
                </p>
                <p className="text-[8px] text-gray-600">
                  {d.display_pnl_source === 'polymarket' ? 'Polymarket' : 'estimated (partial data)'}
                </p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">PROFIT FACTOR</p>
                <p className="text-sm font-bold font-mono text-gray-200">
                  {d.profit_factor != null ? `${d.profit_factor.toFixed(1)}x` : '—'}
                </p>
              </div>
            </div>

            {/* KPI row 3: Detail */}
            <div className="grid grid-cols-3 gap-2">
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">RESOLVED</p>
                <p className="text-sm font-bold font-mono text-gray-200">{d.resolved_trades ?? '—'}</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">AVG WIN</p>
                <p className="text-sm font-bold font-mono text-accent-green">+{fmtUsd2(d.avg_win_size_usdc)}</p>
              </div>
              <div className="bg-surface-card rounded p-2">
                <p className="text-[9px] text-gray-500">AVG LOSS</p>
                <p className="text-sm font-bold font-mono text-accent-red">-{fmtUsd2(d.avg_loss_size_usdc)}</p>
              </div>
            </div>

            {/* Performance Trend */}
            {(d.pnl_30d != null || d.pnl_trend != null) && (
              <div className="bg-surface-card rounded p-2 space-y-1">
                <p className="text-[9px] text-gray-500">PERFORMANCE TREND</p>
                <div className="grid grid-cols-4 gap-1 text-[10px]">
                  <div>
                    <p className="text-[8px] text-gray-600">7d</p>
                    <p className={`font-mono font-bold ${(d.pnl_7d || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {(d.pnl_7d || 0) >= 0 ? '+' : ''}{fmtUsd2(d.pnl_7d)}
                    </p>
                  </div>
                  <div>
                    <p className="text-[8px] text-gray-600">30d</p>
                    <p className={`font-mono font-bold ${(d.pnl_30d || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {(d.pnl_30d || 0) >= 0 ? '+' : ''}{fmtUsd2(d.pnl_30d)}
                    </p>
                  </div>
                  <div>
                    <p className="text-[8px] text-gray-600">90d</p>
                    <p className={`font-mono font-bold ${(d.pnl_90d || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {(d.pnl_90d || 0) >= 0 ? '+' : ''}{fmtUsd2(d.pnl_90d)}
                    </p>
                  </div>
                  <div>
                    <p className="text-[8px] text-gray-600">trend</p>
                    {d.pnl_trend != null ? (
                      d.pnl_trend > 1.2 ? <p className="text-accent-green font-bold">↑ {d.pnl_trend.toFixed(1)}x</p> :
                      d.pnl_trend < 0 ? <p className="text-accent-red font-bold">↓ rev</p> :
                      d.pnl_trend < 0.8 ? <p className="text-yellow-400 font-bold">↓ {d.pnl_trend.toFixed(1)}x</p> :
                      <p className="text-gray-400">→ {d.pnl_trend.toFixed(1)}x</p>
                    ) : <p className="text-gray-600">—</p>}
                  </div>
                </div>
                {d.pnl_consistency != null && (
                  <p className="text-[9px] text-gray-500">{(d.pnl_consistency * 100).toFixed(0)}% of windows profitable</p>
                )}
              </div>
            )}

            {/* Risk Profile */}
            {(d.sharpe_ratio != null || d.max_drawdown_pct != null) && (
              <div className="bg-surface-card rounded p-2 space-y-1">
                <p className="text-[9px] text-gray-500">RISK PROFILE</p>
                <div className="grid grid-cols-3 gap-1">
                  <div>
                    <p className="text-[8px] text-gray-600">Sharpe</p>
                    <p className={`text-sm font-bold font-mono ${
                      (d.sharpe_ratio || 0) > 1 ? 'text-accent-green' :
                      (d.sharpe_ratio || 0) > 0.5 ? 'text-yellow-400' : 'text-accent-red'
                    }`}>{d.sharpe_ratio != null ? d.sharpe_ratio.toFixed(2) : '—'}</p>
                  </div>
                  <div>
                    <p className="text-[8px] text-gray-600">Sortino</p>
                    <p className="text-sm font-bold font-mono text-gray-200">{d.sortino_ratio != null ? d.sortino_ratio.toFixed(2) : '—'}</p>
                  </div>
                  <div>
                    <p className="text-[8px] text-gray-600">Max DD</p>
                    <p className={`text-sm font-bold font-mono ${
                      (d.max_drawdown_pct || 0) < 0.1 ? 'text-accent-green' :
                      (d.max_drawdown_pct || 0) < 0.2 ? 'text-yellow-400' : 'text-accent-red'
                    }`}>{d.max_drawdown_pct != null ? `${(d.max_drawdown_pct * 100).toFixed(0)}%` : '—'}</p>
                  </div>
                </div>
                {(d.win_streak_max || d.loss_streak_max) && (
                  <p className="text-[9px] text-gray-500">
                    Streaks: <span className="text-accent-green">{d.win_streak_max}W</span> / <span className="text-accent-red">{d.loss_streak_max}L</span>
                  </p>
                )}
              </div>
            )}

            {/* Edge & Sizing */}
            {(d.expected_value != null || d.kelly_fraction != null) && (
              <div className="bg-surface-card rounded p-2 space-y-1">
                <p className="text-[9px] text-gray-500">EDGE & SIZING</p>
                <div className="grid grid-cols-2 gap-1">
                  <div>
                    <p className="text-[8px] text-gray-600">Expected Value</p>
                    <p className={`text-sm font-bold font-mono ${(d.expected_value || 0) > 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {(d.expected_value || 0) > 0 ? '+' : ''}${(d.expected_value || 0).toFixed(0)}/trade
                    </p>
                  </div>
                  <div>
                    <p className="text-[8px] text-gray-600">Kelly Fraction</p>
                    <p className="text-sm font-bold font-mono text-gray-200">{d.kelly_fraction != null ? `${(d.kelly_fraction * 100).toFixed(1)}%` : '—'}</p>
                  </div>
                </div>
                {/* EV by category */}
                {(d.ev_politics || d.ev_sports || d.ev_crypto || d.ev_economics) && (
                  <div className="space-y-0.5 pt-1 border-t border-surface-border">
                    {[['politics', d.ev_politics], ['sports', d.ev_sports], ['crypto', d.ev_crypto], ['economics', d.ev_economics]]
                      .filter(([, v]) => v != null)
                      .map(([cat, v]) => (
                        <div key={cat} className="flex items-center gap-2 text-[9px]">
                          <span className="text-gray-500 w-16 capitalize">{cat}</span>
                          <span className={`font-mono ${v > 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                            {v > 0 ? '+' : ''}${v.toFixed(0)}
                          </span>
                        </div>
                      ))}
                  </div>
                )}
                {d.edge_vs_market != null && (
                  <p className="text-[9px] text-gray-500">
                    Edge vs market: <span className={d.edge_vs_market > 0 ? 'text-accent-green' : 'text-accent-red'}>
                      {(d.edge_vs_market * 100).toFixed(1)}%
                    </span>
                  </p>
                )}
                {d.avg_hours_before_resolution != null && (
                  <p className="text-[9px] text-gray-500">
                    Avg {d.avg_hours_before_resolution.toFixed(0)}h before close · {((d.early_entry_rate || 0) * 100).toFixed(0)}% early entries
                  </p>
                )}
              </div>
            )}

            {/* Category breakdown */}
            <div className="bg-surface-card rounded p-2">
              <p className="text-[9px] text-gray-500 mb-1.5">CATEGORY BREAKDOWN</p>
              <CategoryBars2 breakdown={d.category_breakdown} />
            </div>

            {/* PnL chart */}
            <div className="bg-surface-card rounded p-2">
              <p className="text-[9px] text-gray-500 mb-1">CUMULATIVE P&L</p>
              {(d.pnl_history || []).length >= 3 ? (
                <MiniPnlChart history={d.pnl_history} />
              ) : (
                <p className="text-[9px] text-gray-600">{(d.pnl_history || []).length} resolved trades — chart available after 3+</p>
              )}
            </div>

            {/* Recent trades */}
            <div>
              <p className="text-[10px] text-gray-500 mb-1.5 font-medium">Recent Trades ({trades.length})</p>
              {!trades.length ? <p className="text-[9px] text-gray-600">No trade history</p> : (
                <div className="space-y-0.5">
                  {trades.slice(0, 15).map((t, i) => {
                    const won = t.won
                    return (
                      <div key={i}
                           className={`flex items-center gap-1.5 text-[10px] py-0.5 px-1 border-l-2 ${
                             won === true ? 'border-accent-green' :
                             won === false ? 'border-accent-red' :
                             'border-gray-700'
                           }`}>
                        <span className={`px-1 py-0.5 rounded text-[8px] font-bold ${
                          t.side === 'BUY' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
                        }`}>{t.side}</span>
                        <span className="text-gray-400 truncate flex-1">{t.question || '—'}</span>
                        {t.resolved && t.pnl != null ? (
                          <span className={`font-mono text-[9px] ${won ? 'text-accent-green' : 'text-accent-red'}`}>
                            {won ? 'W' : 'L'} {t.pnl >= 0 ? '+' : ''}{fmtUsd2(t.pnl)}
                          </span>
                        ) : (
                          <span className="text-gray-600 text-[9px]">OPEN</span>
                        )}
                      </div>
                    )
                  })}
                </div>
              )}
            </div>

            {/* Signals triggered */}
            {d.signals_triggered && Object.keys(d.signals_triggered).length > 0 && (
              <div>
                <p className="text-[10px] text-gray-500 mb-1.5 font-medium">Signals Triggered</p>
                <div className="flex flex-wrap gap-1">
                  {Object.entries(d.signals_triggered).map(([sig, n]) => (
                    <span key={sig} className="px-1.5 py-0.5 rounded text-[9px] bg-surface-card text-gray-400">
                      {sig} ×{n}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </>
  )
}

// ── Main Page ───────────────────────────────────────────────────────────────
export default function SmartMoney() {
  const [activeTab, setActiveTab] = useState('winners')
  const [selectedWallet, setSelectedWallet] = useState(null)

  const tabs = [
    { key: 'winners', label: 'Winners' },
    { key: 'leaderboard', label: 'Leaderboard' },
    { key: 'positions', label: 'Open Positions' },
    { key: 'alerts', label: 'Alerts' },
    { key: 'live', label: 'Live Signals' },
  ]

  return (
    <div className="p-6 space-y-5">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          Trading Platform <span className="mx-1">&gt;</span>
          <span className="text-gray-400">Wallet Intel</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200">Wallet Intelligence</h1>
        <p className="text-xs text-gray-500 mt-0.5">Track and analyze Polymarket's top directional traders</p>
      </div>

      <ActionableStrip />

      <div className="flex gap-1 border-b border-surface-border">
        {tabs.map(t => (
          <button
            key={t.key}
            className={`px-4 py-2 text-xs font-medium rounded-t-md transition-colors ${
              activeTab === t.key
                ? 'bg-surface-card text-accent-blue border-b-2 border-accent-blue'
                : 'text-gray-500 hover:text-gray-300'
            }`}
            onClick={() => setActiveTab(t.key)}
          >{t.label}</button>
        ))}
      </div>

      <div className="card min-h-[400px]">
        {activeTab === 'leaderboard' && <LeaderboardTab onSelectWallet={setSelectedWallet} />}
        {activeTab === 'winners' && <WinnersTab onSelectWallet={setSelectedWallet} />}
        {activeTab === 'positions' && <OpenPositionsTab />}
        {activeTab === 'alerts' && <AlertsTab />}
        {activeTab === 'live' && <LiveSignalsTab />}
      </div>

      {selectedWallet && (
        <WalletPanel address={selectedWallet} onClose={() => setSelectedWallet(null)} />
      )}
    </div>
  )
}

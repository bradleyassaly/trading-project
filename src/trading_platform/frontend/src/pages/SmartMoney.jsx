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
function WinnersTab({ onSelectWallet }) {
  const [window, setWindow] = useState('all')
  const fetcher = useCallback(() => api.smartMoneyWinners(window), [window])
  const { data, loading } = useApi(fetcher, 300_000)
  const rows = data?.data ?? []

  const periods = [
    { key: 'today', label: 'Today' },
    { key: 'weekly', label: 'Weekly' },
    { key: 'monthly', label: 'Monthly' },
    { key: 'all', label: 'All Time' },
  ]

  return (
    <div>
      <div className="flex gap-1 mb-4">
        {periods.map(p => (
          <button key={p.key}
            className={`px-3 py-1 text-[10px] rounded ${window === p.key ? 'bg-accent-blue/20 text-accent-blue' : 'text-gray-500 hover:text-gray-300'}`}
            onClick={() => setWindow(p.key)}>{p.label}</button>
        ))}
      </div>

      {loading && !data ? <LoadingSkeleton rows={8} /> :
       !rows.length ? <EmptyState title={`No profit data for ${window} window`} message="Run: enrich-trade-resolution + rebuild-wallet-profiles" /> : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-2 pr-2 w-8">#</th>
                <th className="pb-2 pr-3">Wallet</th>
                <th className="pb-2 pr-3 text-right">Profit</th>
                <th className="pb-2 pr-3 text-right">Volume</th>
                <th className="pb-2 pr-3 text-right">Win Rate</th>
                <th className="pb-2 pr-3">Domain</th>
                <th className="pb-2 pr-3">Type</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {rows.slice(0, 50).map((w, i) => {
                const profit = w.profit || 0
                const domain = getBestDomain(w)
                const name = w.notes || truncAddr(w.wallet)
                return (
                  <tr key={w.wallet} className="hover:bg-surface-hover cursor-pointer"
                      onClick={() => onSelectWallet?.(w.wallet)}>
                    <td className="py-1.5 pr-2 text-gray-600">{i + 1}</td>
                    <td className="py-1.5 pr-3 font-mono text-[10px] text-accent-blue">
                      {name}
                    </td>
                    <td className={`py-1.5 pr-3 text-right font-mono font-bold ${profit >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                      {profit >= 0 ? '+' : ''}${Math.abs(profit).toLocaleString(undefined, {maximumFractionDigits: 0})}
                    </td>
                    <td className="py-1.5 pr-3 text-right font-mono text-gray-400">
                      ${Number(w.volume || 0).toLocaleString(undefined, {maximumFractionDigits: 0})}
                    </td>
                    <td className="py-1.5 pr-3 text-right">
                      <WrCell value={w.directional_win_rate} />
                    </td>
                    <td className="py-1.5 pr-3 text-gray-400 text-[10px]">{domain}</td>
                    <td className="py-1.5 pr-3"><TypeBadge type={w.wallet_type || 'unknown'} /></td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <p className="text-[9px] text-gray-600 mt-3">
            Profit reflects trades in our database only. Historical profits before tracking began are not included. Open positions not included until market resolves.
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
function WalletPanel({ address, onClose }) {
  const [detail, setDetail] = useState(null)
  const [positions, setPositions] = useState(null)
  const [trades, setTrades] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!address) return
    setLoading(true)
    Promise.all([
      api.smartMoneyWalletDetail(address).catch(() => null),
      api.smartMoneyWalletPositions(address).catch(() => null),
      api.smartMoneyWalletTrades(address, 1).catch(() => null),
    ]).then(([d, p, t]) => {
      setDetail(d)
      setPositions(p)
      setTrades(t)
      setLoading(false)
    })
  }, [address])

  // Close on ESC
  useEffect(() => {
    const handler = e => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  const profile = detail?.profile || {}
  const wr = profile.directional_win_rate ?? profile.win_rate ?? profile.early_win_rate
  const posRows = positions?.data ?? detail?.open_positions ?? []
  const tradeRows = trades?.data ?? detail?.resolved_trades ?? []

  return (
    <>
      <div className="fixed inset-0 bg-black/40 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 bottom-0 w-[38%] min-w-[340px] bg-surface-bg border-l border-surface-border z-50 overflow-y-auto p-5">
        {loading ? <LoadingSkeleton rows={8} /> : (
          <div className="space-y-5">
            {/* Header */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm text-accent-blue">{truncAddr(address)}</span>
                <TypeBadge type={profile.wallet_type} />
              </div>
              <button onClick={onClose} className="text-gray-500 hover:text-gray-300 text-lg leading-none">&times;</button>
            </div>

            {/* Stats grid */}
            <div className="grid grid-cols-3 gap-2">
              {[
                ['Win Rate', wr != null ? `${(wr * 100).toFixed(0)}%` : '--'],
                ['Dir WR', profile.directional_win_rate != null ? `${(profile.directional_win_rate * 100).toFixed(0)}%` : '--'],
                ['Domain', (profile.best_domain || '--').slice(0, 10)],
                ['Avg Entry', profile.avg_hours_before_close != null ? `${Math.round(profile.avg_hours_before_close)}h` : '--'],
                ['Volume', `$${Number(profile.total_volume_usdc || 0).toLocaleString()}`],
                ['Fills', profile.total_fills ?? profile.resolved_trades ?? '--'],
              ].map(([label, val]) => (
                <div key={label} className="bg-surface-card rounded p-2">
                  <p className="text-[9px] text-gray-500">{label}</p>
                  <p className="text-xs font-mono text-gray-200">{val}</p>
                </div>
              ))}
            </div>

            {/* Positions */}
            <div>
              <p className="text-[10px] text-gray-500 mb-1.5 font-medium">Open Positions ({posRows.length})</p>
              {posRows.length > 0 ? (
                <div className="space-y-0.5">
                  {posRows.slice(0, 10).map((p, i) => (
                    <div key={i} className="flex justify-between text-[10px] py-0.5">
                      <span className="text-gray-400 truncate max-w-[55%]">{p.title || p.market_question || (p.token_id || p.asset || '').slice(0, 24)}</span>
                      <span className={`font-mono ${(p.cash_pnl ?? p.net_amount_usdc ?? 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                        {p.net_side || p.outcome || ''} ${Number(Math.abs(p.net_amount_usdc || p.current_value || p.size || 0)).toFixed(0)}
                      </span>
                    </div>
                  ))}
                </div>
              ) : <p className="text-[10px] text-gray-600">No open positions</p>}
            </div>

            {/* Trades */}
            <div>
              <p className="text-[10px] text-gray-500 mb-1.5 font-medium">Recent Trades ({tradeRows.length})</p>
              {tradeRows.length > 0 ? (
                <div className="space-y-0.5">
                  {tradeRows.slice(0, 15).map((t, i) => (
                    <div key={i} className="flex items-center gap-1.5 text-[10px] py-0.5">
                      <span className={`px-1 py-0.5 rounded text-[8px] font-bold ${
                        (t.side || t.direction) === 'BUY' || (t.side || t.direction) === 'YES'
                          ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
                      }`}>{t.side || t.direction}</span>
                      <span className="text-gray-400 truncate flex-1">{(t.title || t.token_id || '').slice(0, 40)}</span>
                      <span className="font-mono text-gray-300">${Number(t.size || t.amount_usdc || 0).toFixed(0)}</span>
                    </div>
                  ))}
                </div>
              ) : <p className="text-[10px] text-gray-600">No recent trades</p>}
            </div>
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

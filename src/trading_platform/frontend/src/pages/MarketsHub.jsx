import { useCallback, useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function PolymarketTab() {
  const { data: mkts, loading: mktsLoading } = useApi(api.polymarketLiveMarkets, 15_000)
  const { data: signals } = useApi(api.smartMoneySignals, 60_000)

  const allMarkets = mkts?.data ?? []
  const smartSignals = signals?.data ?? []

  // Build lookup by yes_token_id (the field that matches smart money token_id)
  const smartMap = {}
  smartSignals.forEach(s => { smartMap[s.token_id] = s })

  // Filter: hide markets with >60 days to close or no end date
  const markets = allMarkets.filter(m => {
    if (!m.end_date_iso) return false  // no end date = distant future, exclude
    const daysLeft = Math.ceil((new Date(m.end_date_iso) - Date.now()) / 86400000)
    return daysLeft > 0 && daysLeft <= 60
  })

  // Sort: markets with smart money signals first, then by volume
  const sorted = [...markets].sort((a, b) => {
    const aSig = smartMap[a.yes_token_id] || smartMap[a.market_id]
    const bSig = smartMap[b.yes_token_id] || smartMap[b.market_id]
    const aEdge = aSig?.top_wallet_edge ?? 0
    const bEdge = bSig?.top_wallet_edge ?? 0
    if (aEdge !== bEdge) return bEdge - aEdge
    return (b.volume || 0) - (a.volume || 0)
  })

  if (mktsLoading && !mkts) return <LoadingSkeleton rows={6} />
  if (!sorted.length) return <EmptyState title="No live Polymarket markets" />

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Question</th>
            <th className="pb-2 pr-3 text-right">Price</th>
            <th className="pb-2 pr-3 text-right">Days Left</th>
            <th className="pb-2 pr-3 text-center">Smart $</th>
            <th className="pb-2 pr-3 text-center">Direction</th>
            <th className="pb-2 pr-3 text-right">Edge</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {sorted.slice(0, 40).map(m => {
            // Match by yes_token_id first, fall back to market_id
            const sig = smartMap[m.yes_token_id] || smartMap[m.market_id] || {}
            const daysLeft = m.end_date_iso ? Math.ceil((new Date(m.end_date_iso) - Date.now()) / 86400000) : null
            const hasSmart = !!sig.direction
            return (
              <tr key={m.market_id} className={`hover:bg-surface-hover ${hasSmart ? 'bg-accent-green/5' : ''}`}>
                <td className="py-1.5 pr-3 text-gray-300 text-xs max-w-sm truncate">{m.question || m.market_id}</td>
                <td className={`py-1.5 pr-3 text-right font-mono ${(m.yes_price || 0) >= 50 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {m.yes_price?.toFixed(1)}%
                </td>
                <td className="py-1.5 pr-3 text-right text-gray-400">
                  {daysLeft != null ? `${daysLeft}d` : '-'}
                </td>
                <td className="py-1.5 pr-3 text-center font-mono text-gray-400">
                  {sig.weighted_net_volume ? `$${Math.abs(sig.weighted_net_volume).toLocaleString()}` : '-'}
                </td>
                <td className="py-1.5 pr-3 text-center">
                  {sig.direction ? (
                    <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${
                      sig.direction === 'YES' ? 'bg-accent-green/20 text-accent-green' :
                      sig.direction === 'NO' ? 'bg-accent-red/20 text-accent-red' :
                      'bg-gray-700 text-gray-400'
                    }`}>{sig.direction}</span>
                  ) : '-'}
                </td>
                <td className="py-1.5 pr-3 text-right font-mono text-accent-green">
                  {sig.top_wallet_edge ? `${(sig.top_wallet_edge * 100).toFixed(0)}%` : '-'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
      <MirrorFeed />
    </div>
  )
}

function MirrorFeed() {
  const { data, loading } = useApi(api.smartMoneyMirror, 60_000)
  const signals = data?.data ?? []

  return (
    <div className="mt-6 pt-6 border-t border-surface-border">
      <div className="flex items-center gap-2 mb-4">
        <h3 className="text-sm font-medium text-gray-400">Top Wallet Activity</h3>
        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent-green/20 text-accent-green">
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" />
          LIVE
        </span>
        <span className="text-[10px] text-gray-600 ml-2">last 90 min, $100+ fills</span>
      </div>

      {loading && !data ? <LoadingSkeleton rows={3} /> :
       !signals.length ? (
        <p className="text-xs text-gray-500 py-4">No mirror signals in last 90 minutes ($100+ fills)</p>
       ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-2 pr-3">Time</th>
                <th className="pb-2 pr-3">Wallet</th>
                <th className="pb-2 pr-3">Market</th>
                <th className="pb-2 pr-3">Dir</th>
                <th className="pb-2 pr-3 text-right">Amount</th>
                <th className="pb-2 pr-3 text-right">Price</th>
                <th className="pb-2 pr-3 text-right">Edge</th>
                <th className="pb-2 pr-3">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {signals.slice(0, 20).map((s, i) => {
                const mins = s.minutes_since_fill ?? 999
                const rowClass = s.tradeable
                  ? (mins < 30 ? 'bg-accent-green/10' : 'bg-accent-yellow/5')
                  : ''
                return (
                  <tr key={i} className={`hover:bg-surface-hover ${rowClass}`}>
                    <td className="py-1.5 pr-3 text-gray-400">
                      {mins < 60 ? `${mins.toFixed(0)}m` : `${(mins / 60).toFixed(1)}h`}
                    </td>
                    <td className="py-1.5 pr-3 font-mono text-accent-blue text-[10px]">{s.wallet}</td>
                    <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[180px]">
                      {s.question || s.token_id || 'not tracked'}
                    </td>
                    <td className={`py-1.5 pr-3 font-bold ${s.direction === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>
                      {s.direction}
                    </td>
                    <td className="py-1.5 pr-3 text-right font-mono">${Number(s.fill_amount || 0).toLocaleString()}</td>
                    <td className="py-1.5 pr-3 text-right font-mono">
                      {s.current_price != null ? `${s.current_price}%` : 'n/a'}
                    </td>
                    <td className="py-1.5 pr-3 text-right font-mono text-accent-green">
                      {(s.wallet_edge * 100).toFixed(0)}%
                    </td>
                    <td className="py-1.5 pr-3">
                      {s.tradeable ? (
                        <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent-green/20 text-accent-green">TRADE</span>
                      ) : (
                        <span className="text-[9px] text-gray-600">{s.question ? 'illiquid' : 'untracked'}</span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function KalshiTab() {
  const { data, loading } = useApi(api.kalshiMarkets, 60_000)
  const markets = data?.data ?? []

  if (loading && !data) return <LoadingSkeleton rows={6} />
  if (data?.available === false) return <EmptyState title="No Kalshi market data" />
  if (!markets.length) return <EmptyState title="No Kalshi markets loaded" />

  const SIGS = ['calibration_drift_z', 'volume_spike', 'tension']

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Ticker</th>
            <th className="pb-2 pr-3">Title</th>
            <th className="pb-2 pr-3 text-right">Yes</th>
            <th className="pb-2 pr-3 text-right">Days</th>
            {SIGS.map(s => <th key={s} className="pb-2 pr-2 text-right text-[9px]">{s.replace(/_/g, ' ').slice(0, 10)}</th>)}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {markets.slice(0, 40).map(m => (
            <tr key={m.ticker} className="hover:bg-surface-hover">
              <td className="py-1.5 pr-3 font-mono text-accent-blue text-[10px]">{m.ticker}</td>
              <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[200px]">{m.title || m.ticker}</td>
              <td className="py-1.5 pr-3 text-right font-mono">{m.yes_price != null ? `${Number(m.yes_price).toFixed(0)}c` : '-'}</td>
              <td className="py-1.5 pr-3 text-right text-gray-400">{m.days_to_close != null ? Number(m.days_to_close).toFixed(0) : '-'}</td>
              {SIGS.map(s => {
                const v = m.signals?.[s]
                const n = v != null ? Number(v) : null
                const cls = n == null ? 'text-gray-600' : Math.abs(n) > 1.5 ? (n > 0 ? 'text-accent-green' : 'text-accent-red') : 'text-gray-400'
                return <td key={s} className={`py-1.5 pr-2 text-right font-mono text-[10px] ${cls}`}>{n != null ? n.toFixed(2) : '-'}</td>
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function CrossPlatformTab() {
  const { data: kalshi } = useApi(api.kalshiMarkets, 60_000)
  const { data: poly } = useApi(api.polymarketLiveMarkets, 15_000)

  const kalshiMarkets = kalshi?.data ?? []
  const polyMarkets = poly?.data ?? []

  // Simple keyword matching for cross-platform pairs
  const KEYWORDS = {
    'fed': ['fed', 'fomc', 'interest rate', 'rate cut', 'rate hike'],
    'cpi': ['cpi', 'inflation', 'consumer price'],
    'gdp': ['gdp', 'growth'],
    'jobs': ['jobs', 'payroll', 'unemployment', 'nfp'],
  }

  const pairs = []
  for (const km of kalshiMarkets.slice(0, 50)) {
    const kTitle = (km.title || km.ticker || '').toLowerCase()
    for (const [category, words] of Object.entries(KEYWORDS)) {
      if (!words.some(w => kTitle.includes(w))) continue
      for (const pm of polyMarkets.slice(0, 50)) {
        const pQ = (pm.question || '').toLowerCase()
        if (words.some(w => pQ.includes(w))) {
          const kPrice = km.yes_price != null ? Number(km.yes_price) : null
          const pPrice = pm.yes_price != null ? Number(pm.yes_price) : null
          if (kPrice != null && pPrice != null) {
            pairs.push({
              category, kalshi_ticker: km.ticker, kalshi_title: km.title,
              poly_question: pm.question, kalshi_price: kPrice, poly_price: pPrice,
              divergence: pPrice - kPrice,
            })
          }
        }
      }
    }
  }

  // Deduplicate and sort by divergence
  const seen = new Set()
  const unique = pairs.filter(p => {
    const key = `${p.kalshi_ticker}-${p.poly_question?.slice(0, 20)}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  }).sort((a, b) => Math.abs(b.divergence) - Math.abs(a.divergence))

  if (!unique.length) return <EmptyState title="No cross-platform matches found" message="Matching Kalshi and Polymarket markets by keyword" />

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Category</th>
            <th className="pb-2 pr-3">Kalshi</th>
            <th className="pb-2 pr-3">Polymarket</th>
            <th className="pb-2 pr-3 text-right">K Price</th>
            <th className="pb-2 pr-3 text-right">P Price</th>
            <th className="pb-2 pr-3 text-right">Gap</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {unique.slice(0, 20).map((p, i) => {
            const gapColor = Math.abs(p.divergence) > 5 ? 'text-accent-red font-bold' :
                             Math.abs(p.divergence) > 3 ? 'text-accent-yellow' : 'text-gray-400'
            return (
              <tr key={i} className="hover:bg-surface-hover">
                <td className="py-1.5 pr-3 text-gray-500 uppercase text-[9px]">{p.category}</td>
                <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[180px]">{p.kalshi_title || p.kalshi_ticker}</td>
                <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[180px]">{p.poly_question}</td>
                <td className="py-1.5 pr-3 text-right font-mono">{p.kalshi_price.toFixed(0)}c</td>
                <td className="py-1.5 pr-3 text-right font-mono">{p.poly_price.toFixed(1)}%</td>
                <td className={`py-1.5 pr-3 text-right font-mono ${gapColor}`}>
                  {p.divergence > 0 ? '+' : ''}{p.divergence.toFixed(1)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export default function MarketsHub() {
  const [tab, setTab] = useState('polymarket')

  const tabs = [
    { key: 'polymarket', label: 'Polymarket + Smart Money' },
    { key: 'kalshi', label: 'Kalshi Signals' },
    { key: 'cross', label: 'Cross-Platform' },
  ]

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          Trading Platform <span className="mx-1">></span>
          <span className="text-gray-400">Markets</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200">Markets Hub</h1>
      </div>

      <div className="flex gap-1 border-b border-surface-border pb-0">
        {tabs.map(t => (
          <button
            key={t.key}
            className={`px-4 py-2 text-xs font-medium rounded-t-md transition-colors ${
              tab === t.key ? 'bg-surface-card text-accent-blue border-b-2 border-accent-blue' : 'text-gray-500 hover:text-gray-300'
            }`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="card">
        {tab === 'polymarket' && <PolymarketTab />}
        {tab === 'kalshi' && <KalshiTab />}
        {tab === 'cross' && <CrossPlatformTab />}
      </div>
    </div>
  )
}

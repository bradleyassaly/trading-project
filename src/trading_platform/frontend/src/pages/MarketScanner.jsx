import { useState, useCallback } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'
import { BucketBadge, SignalBadge, TierBadge } from '../components/Badges'

function truncAddr(addr) {
  if (!addr || addr.length < 12) return addr || '?'
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`
}

export default function MarketScanner() {
  const { data: mkts, loading: mktsL } = useApi(api.polymarketLiveMarkets, 15_000)
  const { data: signals } = useApi(api.smartMoneySignals, 60_000)
  const { data: positions } = useApi(api.smartMoneyOpenPositions, 300_000)
  const [selected, setSelected] = useState(null)

  const allMarkets = mkts?.data ?? []
  const smartSignals = signals?.data ?? []
  const openPos = positions?.data ?? []

  // Build smart money overlay by token_id
  const smartMap = {}
  smartSignals.forEach(s => { smartMap[s.token_id] = s })

  // Build position overlay by token_id
  const posMap = {}
  openPos.forEach(p => {
    const k = p.token_id
    if (!posMap[k]) posMap[k] = { total: 0, wallets: 0, direction: p.net_side, top_wallet: p.wallet, top_edge: p.wallet_edge || 0 }
    posMap[k].total += (p.net_amount_usdc || 0)
    posMap[k].wallets++
    if ((p.wallet_edge || 0) > posMap[k].top_edge) {
      posMap[k].top_wallet = p.wallet
      posMap[k].top_edge = p.wallet_edge
    }
  })

  // Filter and sort markets
  const markets = allMarkets.filter(m => {
    if (!m.end_date_iso) return false
    const daysLeft = Math.ceil((new Date(m.end_date_iso) - Date.now()) / 86400000)
    return daysLeft > 0 && daysLeft <= 60
  }).sort((a, b) => {
    const aS = (posMap[a.yes_token_id]?.total || 0) + (smartMap[a.yes_token_id] ? 1000 : 0)
    const bS = (posMap[b.yes_token_id]?.total || 0) + (smartMap[b.yes_token_id] ? 1000 : 0)
    return bS - aS
  })

  if (mktsL && !mkts) return <div className="p-6"><LoadingSkeleton rows={10} /></div>

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Market Scanner</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Market Scanner</h1>
        <p className="text-xs text-gray-500 mt-0.5">Active Polymarket markets with smart money overlay</p>
      </div>

      {!markets.length ? <EmptyState title="No active markets" message="Start the live collector to populate markets" /> : (
        <div className="flex gap-4">
          <div className={`card overflow-x-auto ${selected ? 'flex-1' : 'w-full'}`}>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-surface-border text-gray-500 text-left">
                  <th className="pb-2 pr-3">Market</th>
                  <th className="pb-2 pr-3 text-right">Price</th>
                  <th className="pb-2 pr-3 text-right">Days</th>
                  <th className="pb-2 pr-3 text-right">Smart $</th>
                  <th className="pb-2 pr-3 text-center">Dir</th>
                  <th className="pb-2 pr-3 text-right">Wallets</th>
                  <th className="pb-2 pr-3">Signal</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-border">
                {markets.slice(0, 50).map(m => {
                  const sig = smartMap[m.yes_token_id] || smartMap[m.market_id] || {}
                  const pos = posMap[m.yes_token_id] || {}
                  const daysLeft = m.end_date_iso ? Math.ceil((new Date(m.end_date_iso) - Date.now()) / 86400000) : null
                  const hasSmart = pos.total > 0 || sig.direction
                  return (
                    <tr key={m.market_id}
                        className={`hover:bg-surface-hover cursor-pointer ${hasSmart ? 'border-l-2 border-accent-green' : ''}`}
                        onClick={() => setSelected(m)}>
                      <td className="py-1.5 pr-3 text-gray-300 text-xs max-w-[250px] truncate">{m.question || m.market_id}</td>
                      <td className={`py-1.5 pr-3 text-right font-mono ${(m.yes_price || 0) >= 50 ? 'text-accent-green' : 'text-accent-red'}`}>
                        {m.yes_price?.toFixed(1)}%
                      </td>
                      <td className="py-1.5 pr-3 text-right text-gray-400">{daysLeft != null ? `${daysLeft}d` : '-'}</td>
                      <td className="py-1.5 pr-3 text-right font-mono text-gray-400">
                        {pos.total > 0 ? `$${pos.total.toLocaleString(undefined, {maximumFractionDigits: 0})}` : sig.weighted_net_volume ? `$${Math.abs(sig.weighted_net_volume).toLocaleString()}` : '-'}
                      </td>
                      <td className="py-1.5 pr-3 text-center">
                        {(pos.direction || sig.direction) ? (
                          <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${
                            (pos.direction || sig.direction) === 'YES' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
                          }`}>{pos.direction || sig.direction}</span>
                        ) : '-'}
                      </td>
                      <td className="py-1.5 pr-3 text-right text-gray-400">{pos.wallets || '-'}</td>
                      <td className="py-1.5 pr-3">
                        {sig.confidence ? <span className="text-[9px] text-gray-400">{((sig.confidence || 0) * 100).toFixed(0)}%</span> : '-'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {selected && (
            <div className="card w-[320px] flex-shrink-0">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-xs font-medium text-gray-300 truncate max-w-[250px]">{selected.question}</h3>
                <button onClick={() => setSelected(null)} className="text-gray-500 hover:text-gray-300">&times;</button>
              </div>
              <div className="grid grid-cols-2 gap-2 mb-3">
                <div className="bg-surface-card rounded p-2">
                  <p className="text-[9px] text-gray-500">Price</p>
                  <p className="text-sm font-mono text-gray-200">{selected.yes_price?.toFixed(1)}%</p>
                </div>
                <div className="bg-surface-card rounded p-2">
                  <p className="text-[9px] text-gray-500">Volume</p>
                  <p className="text-sm font-mono text-gray-200">${Number(selected.volume || 0).toLocaleString()}</p>
                </div>
              </div>
              {(() => {
                const pos = posMap[selected.yes_token_id]
                if (!pos) return <p className="text-[10px] text-gray-500">No smart money activity</p>
                return (
                  <div>
                    <p className="text-[10px] text-gray-500 mb-1">Smart Money: {pos.wallets} wallet(s), ${pos.total.toLocaleString()}</p>
                    <p className="text-[10px] text-gray-400">Direction: <span className={pos.direction === 'YES' ? 'text-accent-green' : 'text-accent-red'}>{pos.direction}</span></p>
                    <p className="text-[10px] text-gray-400">Top wallet: {truncAddr(pos.top_wallet)}</p>
                  </div>
                )
              })()}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

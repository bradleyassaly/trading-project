import { useCallback, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine,
} from 'recharts'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

export default function WalletDetail() {
  const { address } = useParams()
  const fetcher = useCallback(() => api.smartMoneyWalletDetail(address), [address])
  const { data, loading } = useApi(fetcher)

  if (loading && !data) return <div className="p-6"><LoadingSkeleton rows={10} /></div>
  if (!data?.available) {
    return (
      <div className="p-6">
        <p className="text-sm text-gray-400">{data?.reason || 'Wallet not found'}</p>
        <Link to="/smart-money" className="text-xs text-accent-blue hover:underline mt-2 inline-block">Back to Smart Money</Link>
      </div>
    )
  }

  const profile = data.profile || {}
  const trades = data.resolved_trades || []
  const edge = Number(profile.edge || 0)
  const ewr = Number(profile.early_win_rate || 0)
  const uet = Number(profile.uncertain_early_trades || 0)
  const tier = profile.is_early_informed ? (ewr >= 0.80 ? 'Tier 1 - Mirror' : 'Tier 2 - Signal') : 'Watch Only'
  const tierColor = tier.startsWith('Tier 1') ? 'text-accent-green' : tier.startsWith('Tier 2') ? 'text-accent-yellow' : 'text-gray-500'

  // Running win rate chart
  const chartData = useMemo(() => {
    let wins = 0
    return trades.filter(t => t.won != null).map((t, i) => {
      if (t.won) wins++
      return { idx: i + 1, win_rate: wins / (i + 1) }
    })
  }, [trades])

  const wonTrades = trades.filter(t => t.won === true)
  const lostTrades = trades.filter(t => t.won === false)

  return (
    <div className="p-6 space-y-6">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          <Link to="/smart-money" className="hover:text-gray-400">Smart Money</Link>
          <span className="mx-1">></span>
          <span className="text-gray-400">Wallet Detail</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200 font-mono">{address?.slice(0, 20)}...</h1>
      </div>

      {/* Profile header */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500">Edge</p>
          <p className="text-xl font-bold font-mono text-accent-green">{(edge * 100).toFixed(1)}%</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500">Early Win Rate</p>
          <p className="text-xl font-bold font-mono">{(ewr * 100).toFixed(1)}%</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500">Sample Size</p>
          <p className="text-xl font-bold font-mono text-gray-200">{uet}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500">Volume</p>
          <p className="text-lg font-bold font-mono text-gray-200">${Number(profile.total_volume_usdc || 0).toLocaleString()}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500">Tier</p>
          <p className={`text-sm font-bold ${tierColor}`}>{tier}</p>
        </div>
      </div>

      {/* Win rate chart */}
      {chartData.length > 5 && (
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-3">Cumulative Win Rate</h2>
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={chartData} margin={{ top: 4, right: 8, left: -10, bottom: 0 }}>
              <XAxis dataKey="idx" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} />
              <YAxis domain={[0, 1]} tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false}
                     tickFormatter={v => `${(v * 100).toFixed(0)}%`} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3e', borderRadius: 6, fontSize: 11 }}
                       formatter={v => [`${(v * 100).toFixed(1)}%`, 'Win Rate']} />
              <ReferenceLine y={0.5} stroke="#4b5563" strokeDasharray="4 4" />
              <Line type="monotone" dataKey="win_rate" stroke="#00ff88" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Trade history */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-3">
          Trade History ({wonTrades.length} wins / {lostTrades.length} losses of {trades.length} total)
        </h2>
        {!trades.length ? <EmptyState title="No trade history" /> : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-surface-border text-gray-500 text-left">
                  <th className="pb-2 pr-3">Date</th>
                  <th className="pb-2 pr-3">Token</th>
                  <th className="pb-2 pr-3">Dir</th>
                  <th className="pb-2 pr-3 text-right">Amount</th>
                  <th className="pb-2 pr-3 text-right">Resolution</th>
                  <th className="pb-2 pr-3">Outcome</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-border">
                {trades.slice(0, 50).map((t, i) => (
                  <tr key={i} className="hover:bg-surface-hover">
                    <td className="py-1 pr-3 text-gray-500 text-[10px]">{String(t.timestamp || '').slice(0, 16)}</td>
                    <td className="py-1 pr-3 font-mono text-[10px] text-gray-400">{t.token_id}</td>
                    <td className={`py-1 pr-3 font-bold ${t.direction === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>{t.direction}</td>
                    <td className="py-1 pr-3 text-right font-mono">${Number(t.amount_usdc || 0).toFixed(0)}</td>
                    <td className="py-1 pr-3 text-right font-mono text-gray-400">{t.resolution_price != null ? `${t.resolution_price}` : '-'}</td>
                    <td className="py-1 pr-3">
                      {t.won === true ? <span className="text-accent-green font-bold">WIN</span> :
                       t.won === false ? <span className="text-accent-red font-bold">LOSS</span> :
                       <span className="text-gray-600">-</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

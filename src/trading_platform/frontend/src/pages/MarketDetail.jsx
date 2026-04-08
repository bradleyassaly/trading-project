import { useState, useEffect } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { TierBadge, SignalBadge } from '../components/Badges'
import LoadingSkeleton from '../components/LoadingSkeleton'

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(0)}K`
  return `$${Math.round(n)}`
}

export default function MarketDetail() {
  const { conditionId } = useParams()
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const load = async () => {
      try {
        const resp = await fetch(`/api/market/${encodeURIComponent(conditionId)}`)
        setData(await resp.json())
      } catch (e) { console.error(e) }
      setLoading(false)
    }
    load()
  }, [conditionId])

  if (loading) return <div className="p-6"><LoadingSkeleton rows={10} /></div>
  if (!data?.available) return (
    <div className="p-6">
      <button onClick={() => navigate(-1)} className="text-xs text-accent-blue hover:underline mb-4">&larr; Back</button>
      <p className="text-sm text-gray-500">{data?.reason || 'Market not found'}</p>
    </div>
  )

  const d = data
  const ws = d.whale_summary || {}
  const priceColor = d.current_price >= 0.5 ? 'text-accent-green' : 'text-accent-red'

  return (
    <div className="p-6 space-y-4">
      <button onClick={() => navigate(-1)} className="text-xs text-accent-blue hover:underline">&larr; Back</button>

      {/* Header */}
      <div>
        <h1 className="text-base font-semibold text-gray-200 mb-1">{d.question || conditionId}</h1>
        <div className="flex items-center gap-2 text-[10px]">
          <span className="px-1.5 py-0.5 rounded bg-surface-hover text-gray-400">{d.category}</span>
          {d.hours_to_close != null && d.hours_to_close > 0 && (
            <span className="text-gray-500">{d.hours_to_close.toFixed(0)}h to close</span>
          )}
          {d.hours_to_close != null && d.hours_to_close <= 0 && (
            <span className="px-1.5 py-0.5 rounded bg-accent-red/20 text-accent-red">CLOSED</span>
          )}
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">Current Price</p>
          <p className={`text-xl font-bold font-mono ${priceColor}`}>
            {d.current_price != null ? `${(d.current_price * 100).toFixed(1)}%` : '—'}
          </p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">Total Volume</p>
          <p className="text-xl font-bold font-mono text-gray-200">{fmtUsd(d.total_volume)}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">Whale Activity</p>
          <p className="text-xl font-bold text-gray-200">{(d.whale_trades || []).length} trades</p>
          <p className="text-[9px] text-gray-500">{ws.tier1_count} T1 · {ws.tier2_count} T2</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">Net Whale Direction</p>
          <p className={`text-xl font-bold ${ws.net_direction === 'BUY' ? 'text-accent-green' : ws.net_direction === 'SELL' ? 'text-accent-red' : 'text-gray-400'}`}>
            {ws.net_direction || '—'}
          </p>
        </div>
      </div>

      {/* Paper position */}
      {d.paper_position && (
        <div className="bg-surface-card rounded-lg p-3 border-l-4 border-accent-blue">
          <p className="text-[10px] text-gray-500 mb-1">Our Position</p>
          <div className="flex items-center gap-3 text-xs">
            <span className={d.paper_position.side === 'YES' || d.paper_position.side === 'BUY' ? 'text-accent-green font-bold' : 'text-accent-red font-bold'}>
              {d.paper_position.side}
            </span>
            <span className="font-mono text-gray-300">{fmtUsd(d.paper_position.size_usd)} @ {d.paper_position.entry_price?.toFixed(3)}</span>
            {d.paper_position.unrealized_pnl != null && (
              <span className={`font-mono ${d.paper_position.unrealized_pnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                {d.paper_position.unrealized_pnl >= 0 ? '+' : ''}{fmtUsd(d.paper_position.unrealized_pnl)}
              </span>
            )}
            <span className="text-gray-500">Signal: {d.paper_position.signal_type} · conf {((d.paper_position.confidence || 0) * 100).toFixed(0)}%</span>
          </div>
        </div>
      )}

      {/* Two columns */}
      <div className="flex gap-4">
        {/* Left: Whale trades */}
        <div className="flex-[60] card">
          <h2 className="text-sm font-medium text-gray-300 mb-2">All Whale Activity</h2>
          {!(d.whale_trades || []).length ? (
            <p className="text-[10px] text-gray-600">No whale activity detected on this market yet</p>
          ) : (
            <table className="w-full text-[10px]">
              <thead>
                <tr className="border-b border-surface-border text-gray-500 text-left">
                  <th className="pb-1 pr-2">Tier</th>
                  <th className="pb-1 pr-2">Wallet</th>
                  <th className="pb-1 pr-2">Side</th>
                  <th className="pb-1 pr-2 text-right">Price</th>
                  <th className="pb-1 pr-2 text-right">Size</th>
                  <th className="pb-1 pr-2 text-right">WR</th>
                  <th className="pb-1">When</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-border">
                {d.whale_trades.map((t, i) => (
                  <tr key={i} className={`hover:bg-surface-hover ${t.side === 'BUY' ? 'border-l-2 border-accent-green' : 'border-l-2 border-accent-red'}`}>
                    <td className="py-1 pr-2"><TierBadge tier={t.tier} /></td>
                    <td className="py-1 pr-2">
                      <Link to={`/smart-money/${t.wallet_full}`} className="font-mono text-accent-blue hover:underline">{t.wallet}</Link>
                    </td>
                    <td className={`py-1 pr-2 font-bold ${t.side === 'BUY' ? 'text-accent-green' : 'text-accent-red'}`}>{t.side}</td>
                    <td className="py-1 pr-2 text-right font-mono text-gray-400">{t.price?.toFixed(3)}</td>
                    <td className="py-1 pr-2 text-right font-mono text-gray-400">{fmtUsd(t.size)}</td>
                    <td className="py-1 pr-2 text-right text-gray-500">{t.directional_win_rate ? `${(t.directional_win_rate * 100).toFixed(0)}%` : '—'}</td>
                    <td className="py-1 text-gray-600">{t.time_ago}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Right: Signals */}
        <div className="flex-[40] space-y-4">
          <div className="card">
            <h2 className="text-sm font-medium text-gray-300 mb-2">Signals</h2>
            {!(d.signals || []).length ? (
              <p className="text-[10px] text-gray-600">No signals fired yet</p>
            ) : (
              <div className="space-y-1">
                {d.signals.map((s, i) => (
                  <div key={i} className="flex items-center gap-2 text-[10px] py-1">
                    <SignalBadge type={s.signal_type} />
                    <span className={s.direction === 'BUY' ? 'text-accent-green' : 'text-accent-red'}>{s.direction}</span>
                    <span className="text-gray-500">conf {s.confidence ? (s.confidence * 100).toFixed(0) + '%' : '—'}</span>
                    <span className="text-gray-600 ml-auto">{s.time_ago}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-300 mb-2">Side Distribution</h2>
            {ws.tier1_count > 0 ? (
              <div className="space-y-1.5 text-[10px]">
                <div>
                  <div className="flex justify-between text-gray-400 mb-0.5">
                    <span>T1 BUY</span><span>{fmtUsd(ws.buy_volume_tier1)}</span>
                  </div>
                  <div className="w-full bg-gray-800 rounded-full h-2">
                    <div className="h-2 rounded-full bg-accent-green" style={{
                      width: `${Math.round((ws.buy_volume_tier1 / Math.max(ws.buy_volume_tier1 + ws.sell_volume_tier1, 1)) * 100)}%`
                    }} />
                  </div>
                </div>
                <div>
                  <div className="flex justify-between text-gray-400 mb-0.5">
                    <span>T1 SELL</span><span>{fmtUsd(ws.sell_volume_tier1)}</span>
                  </div>
                  <div className="w-full bg-gray-800 rounded-full h-2">
                    <div className="h-2 rounded-full bg-accent-red" style={{
                      width: `${Math.round((ws.sell_volume_tier1 / Math.max(ws.buy_volume_tier1 + ws.sell_volume_tier1, 1)) * 100)}%`
                    }} />
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-[10px] text-gray-600">No tier-1 activity yet</p>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

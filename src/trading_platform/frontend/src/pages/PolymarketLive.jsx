import { useCallback, useEffect, useState } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function LiveBadge() {
  return (
    <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent-green/20 text-accent-green">
      <span className="inline-block w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" />
      Live
    </span>
  )
}

function formatTime(ts) {
  if (!ts) return '—'
  const num = Number(ts)
  const d = Number.isFinite(num) && num > 1e9
    ? new Date(num > 1e12 ? num : num * 1000)
    : new Date(ts)
  return Number.isNaN(d.getTime()) ? '—' : d.toLocaleTimeString()
}

function daysLeft(endDateIso) {
  if (!endDateIso) return null
  const end = new Date(endDateIso)
  if (Number.isNaN(end.getTime())) return null
  const diff = Math.ceil((end - Date.now()) / (1000 * 60 * 60 * 24))
  return diff >= 0 ? diff : 0
}

function chartTimeLabel(ts) {
  if (!ts) return ''
  const d = new Date(ts)
  if (Number.isNaN(d.getTime())) return ''
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

// ── Detail Panel ─────────────────────────────────────────────────────────────

function MarketDetailPanel({ marketId, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.polymarketMarketTicks(marketId)
      .then(d => { if (!cancelled) setData(d) })
      .catch(e => { if (!cancelled) setError(e) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [marketId])

  if (loading) {
    return (
      <div className="bg-[#1a1a2e] border-l border-surface-border p-5 w-[480px] flex-shrink-0 overflow-y-auto">
        <LoadingSkeleton rows={6} />
      </div>
    )
  }

  if (error || !data?.available) {
    return (
      <div className="bg-[#1a1a2e] border-l border-surface-border p-5 w-[480px] flex-shrink-0">
        <div className="flex justify-between items-start mb-4">
          <p className="text-sm text-gray-400">{data?.reason || 'Failed to load'}</p>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-300 text-lg leading-none">&times;</button>
        </div>
      </div>
    )
  }

  const { question, volume, end_date_iso, ticks, stats } = data
  const chartData = (ticks || []).map(t => ({
    ts: chartTimeLabel(t.timestamp),
    price: t.price,
  }))

  const priceChange = stats ? (stats.last - stats.first) : null
  const dl = daysLeft(end_date_iso)

  const notEnough = !ticks || ticks.length < 5

  return (
    <div className="bg-[#1a1a2e] border-l border-surface-border p-5 w-[480px] flex-shrink-0 overflow-y-auto">
      {/* Header */}
      <div className="flex justify-between items-start mb-4">
        <div className="flex-1 pr-3">
          <p className="text-sm text-gray-200 leading-snug mb-2">{question || marketId}</p>
          <div className="flex items-center gap-3 flex-wrap">
            <span className={`text-2xl font-bold font-mono ${stats?.last >= 50 ? 'text-accent-green' : 'text-accent-red'}`}>
              {stats?.last?.toFixed(1)}%
            </span>
            <LiveBadge />
            {dl != null && (
              <span className={`text-xs ${dl <= 7 ? 'text-accent-yellow' : 'text-gray-500'}`}>{dl}d left</span>
            )}
          </div>
        </div>
        <button onClick={onClose} className="text-gray-500 hover:text-gray-300 text-xl leading-none flex-shrink-0">&times;</button>
      </div>

      {/* Info row */}
      <div className="flex gap-4 text-xs text-gray-500 mb-4">
        <span>Vol: {volume ? Number(volume).toLocaleString() : '—'}</span>
        <span>Ticks: {stats?.tick_count?.toLocaleString() ?? '—'}</span>
        {stats?.hours_collected != null && (
          <span>{stats.hours_collected.toFixed(1)}h collected</span>
        )}
      </div>

      {/* Chart */}
      {notEnough ? (
        <div className="rounded-lg bg-surface-card p-6 text-center text-xs text-gray-500 mb-4">
          Not enough data yet — check back soon
        </div>
      ) : (
        <div className="mb-4">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={chartData} margin={{ top: 4, right: 8, left: -10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2d3e" />
              <XAxis
                dataKey="ts"
                tick={{ fill: '#6b7280', fontSize: 9 }}
                tickLine={false}
                axisLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: '#6b7280', fontSize: 9 }}
                tickLine={false}
                axisLine={false}
                domain={['auto', 'auto']}
              />
              <Tooltip
                contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3e', borderRadius: 6, fontSize: 11 }}
                cursor={{ stroke: '#2a2d3e' }}
                formatter={(val) => [`${val}%`, 'Yes Price']}
              />
              <Line
                type="monotone"
                dataKey="price"
                stroke="#00ff88"
                strokeWidth={2}
                dot={false}
                name="Yes Price"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Stats */}
      {stats && (
        <div className="grid grid-cols-3 gap-3 text-xs">
          <div className="bg-surface-card rounded-lg p-3">
            <p className="text-gray-500 mb-1">Price Range</p>
            <p className="font-mono text-gray-300">{stats.min?.toFixed(1)}% — {stats.max?.toFixed(1)}%</p>
          </div>
          <div className="bg-surface-card rounded-lg p-3">
            <p className="text-gray-500 mb-1">Change</p>
            <p className={`font-mono ${priceChange >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
              {priceChange >= 0 ? '+' : ''}{priceChange?.toFixed(1)}%
            </p>
          </div>
          <div className="bg-surface-card rounded-lg p-3">
            <p className="text-gray-500 mb-1">Ticks/hr</p>
            <p className="font-mono text-gray-300">{stats.ticks_per_hour}</p>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Main Page ────────────────────────────────────────────────────────────────

export default function PolymarketLive() {
  const { data, loading, error } = useApi(api.polymarketLiveMarkets, 15_000)
  const [selected, setSelected] = useState(null)

  if (loading && !data) return <LoadingSkeleton rows={8} />
  if (error) {
    return (
      <div className="p-6">
        <h1 className="text-lg font-semibold mb-4">Polymarket Live</h1>
        <p className="text-red-400 text-sm">Failed to load live markets: {String(error)}</p>
      </div>
    )
  }

  const markets = [...(data?.data ?? [])].sort((a, b) => {
    const da = daysLeft(a.end_date_iso) ?? 999
    const db = daysLeft(b.end_date_iso) ?? 999
    return da - db  // ascending: most urgent first
  })
  const subscribed = data?.markets_subscribed ?? 0
  const startedAt = data?.started_at

  return (
    <div className="flex h-full">
      {/* Table */}
      <div className="flex-1 p-6 overflow-auto">
        <div className="flex items-center gap-3 mb-2">
          <h1 className="text-lg font-semibold">Polymarket Live</h1>
          {subscribed > 0 && (
            <span className="text-xs text-gray-500">
              {markets.length} active / {subscribed} subscribed
            </span>
          )}
        </div>
        {startedAt && (
          <p className="text-xs text-gray-500 mb-6">
            Collecting since {new Date(startedAt).toLocaleString()}
          </p>
        )}

        {markets.length === 0 ? (
          <EmptyState
            title="No live markets"
            message="Start the live collector: trading-cli data polymarket live-collect"
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-gray-500 border-b border-surface-border">
                  <th className="pb-2 pr-3 w-12">Status</th>
                  <th className="pb-2 pr-3">Question</th>
                  <th className="pb-2 pr-3 text-right">Yes Price</th>
                  <th className="pb-2 pr-3 text-right">Days Left</th>
                  <th className="pb-2 pr-3 text-right">Ticks</th>
                  <th className="pb-2 pr-3 text-right">Last Tick</th>
                </tr>
              </thead>
              <tbody>
                {markets.map((m) => (
                  <tr
                    key={m.market_id}
                    className={`border-b border-surface-border/50 hover:bg-surface-hover transition-colors cursor-pointer
                      ${selected === m.market_id ? 'bg-accent-blue/10' : ''}`}
                    onClick={() => setSelected(prev => prev === m.market_id ? null : m.market_id)}
                  >
                    <td className="py-2 pr-3">
                      <LiveBadge />
                    </td>
                    <td className="py-2 pr-3 text-gray-300 text-xs max-w-md truncate" title={m.question || m.market_id}>
                      {m.question || m.market_id}
                    </td>
                    <td className="py-2 pr-3 text-right font-mono">
                      <span className={m.yes_price >= 50 ? 'text-accent-green' : 'text-accent-red'}>
                        {m.yes_price?.toFixed(1)}%
                      </span>
                    </td>
                    <td className="py-2 pr-3 text-right text-xs font-mono">
                      {(() => {
                        const d = daysLeft(m.end_date_iso)
                        if (d == null) return <span className="text-gray-600">—</span>
                        return <span className={d <= 7 ? 'text-accent-yellow' : 'text-gray-400'}>{d}d</span>
                      })()}
                    </td>
                    <td className="py-2 pr-3 text-right text-xs text-gray-500 font-mono">
                      {(m.tick_count ?? 0).toLocaleString()}
                    </td>
                    <td className="py-2 pr-3 text-right text-xs text-gray-500">
                      {formatTime(m.last_tick_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Detail panel */}
      {selected && (
        <MarketDetailPanel
          marketId={selected}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  )
}

import { useCallback, useState, useMemo } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

const SIGNAL_KEYS = [
  'calibration_drift_z',
  'volume_spike_z',
  'tension',
  'taker_imbalance',
  'large_order_direction',
  'base_rate_edge',
]

function signalCell(val) {
  if (val == null) return <span className="text-gray-600">—</span>
  const n = Number(val)
  const abs = Math.abs(n)
  const cls =
    abs > 1.5 ? (n > 0 ? 'tag-green' : 'tag-red') :
    abs > 0.5 ? (n > 0 ? 'text-green-400' : 'text-red-400') : 'text-gray-500'
  return <span className={cls}>{n.toFixed(2)}</span>
}

function MarketHistoryChart({ ticker }) {
  const fetcher = useCallback(() => api.kalshiMarketHistory(ticker), [ticker])
  const { data, loading } = useApi(fetcher)

  if (loading) return <LoadingSkeleton rows={2} className="my-3" />
  if (!data?.available || !data.data?.length) {
    return <p className="text-xs text-gray-500 py-3">No history available for {ticker}</p>
  }

  const chartData = data.data.map(r => ({
    ts: String(r.timestamp ?? '').slice(5, 16),
    price: r.yes_price,
    drift: r.calibration_drift_z,
  }))

  return (
    <div className="mt-3 px-2">
      <ResponsiveContainer width="100%" height={150}>
        <LineChart data={chartData} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#2a2d3e" />
          <XAxis dataKey="ts" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
          <YAxis tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} />
          <Tooltip
            contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3e', borderRadius: 6, fontSize: 11 }}
            cursor={{ stroke: '#2a2d3e' }}
          />
          <Line type="monotone" dataKey="price" stroke="#3d7fff" strokeWidth={2} dot={false} name="Yes price" />
          {chartData[0]?.drift != null && (
            <Line type="monotone" dataKey="drift" stroke="#ffd32a" strokeWidth={1} dot={false} name="Cal drift Z" strokeDasharray="4 2" />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function Markets() {
  const [search, setSearch] = useState('')
  const [minSignal, setMinSignal] = useState('')
  const [maxDays, setMaxDays] = useState('')
  const [sortKey, setSortKey] = useState('yes_price')
  const [sortDir, setSortDir] = useState('desc')
  const [expanded, setExpanded] = useState(null)

  const fetcher = useCallback(() => api.kalshiMarkets(), [])
  const { data, loading } = useApi(fetcher, 60_000)

  const markets = data?.data ?? []

  const filtered = useMemo(() => {
    let list = [...markets]

    if (search) {
      const q = search.toLowerCase()
      list = list.filter(m =>
        String(m.ticker).toLowerCase().includes(q) ||
        String(m.title ?? '').toLowerCase().includes(q)
      )
    }

    if (minSignal !== '') {
      const threshold = Number(minSignal)
      list = list.filter(m => {
        const sigs = Object.values(m.signals ?? {}).filter(v => v != null)
        return sigs.some(v => Math.abs(Number(v)) >= threshold)
      })
    }

    if (maxDays !== '') {
      const d = Number(maxDays)
      list = list.filter(m => m.days_to_close == null || Number(m.days_to_close) <= d)
    }

    list.sort((a, b) => {
      let av, bv
      if (sortKey.startsWith('sig_')) {
        const sk = sortKey.slice(4)
        av = a.signals?.[sk] ?? -Infinity
        bv = b.signals?.[sk] ?? -Infinity
      } else {
        av = a[sortKey] ?? -Infinity
        bv = b[sortKey] ?? -Infinity
      }
      return sortDir === 'asc' ? av - bv : bv - av
    })

    return list
  }, [markets, search, minSignal, maxDays, sortKey, sortDir])

  function handleSort(key) {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  function toggleRow(ticker) {
    setExpanded(prev => prev === ticker ? null : ticker)
  }

  const sortIcon = key => sortKey === key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ''

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          Trading Platform <span className="mx-1">›</span>
          <span className="text-gray-400">Kalshi Markets</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200">Kalshi Markets Browser</h1>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap gap-3 items-center">
        <input
          type="text"
          placeholder="Search ticker or title..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-surface-card border border-surface-border rounded-md px-3 py-1.5 text-xs text-gray-200 placeholder-gray-600 w-56 focus:outline-none focus:border-accent-blue"
        />
        <input
          type="number"
          placeholder="Min |signal|"
          value={minSignal}
          onChange={e => setMinSignal(e.target.value)}
          className="bg-surface-card border border-surface-border rounded-md px-3 py-1.5 text-xs text-gray-200 placeholder-gray-600 w-32 focus:outline-none focus:border-accent-blue"
          min="0" step="0.1"
        />
        <input
          type="number"
          placeholder="Max days"
          value={maxDays}
          onChange={e => setMaxDays(e.target.value)}
          className="bg-surface-card border border-surface-border rounded-md px-3 py-1.5 text-xs text-gray-200 placeholder-gray-600 w-28 focus:outline-none focus:border-accent-blue"
          min="0"
        />
        <span className="text-xs text-gray-500 ml-auto">{filtered.length} markets</span>
      </div>

      {/* Table */}
      <div className="card overflow-x-auto">
        {loading ? (
          <LoadingSkeleton rows={5} />
        ) : data?.available === false ? (
          <EmptyState
            title={data.reason || 'No market data'}
            icon="◎"
            hint="trading-cli kalshi ingest-historical"
          />
        ) : filtered.length === 0 ? (
          <EmptyState title="No markets match filters" icon="◎" />
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-2 pr-4 cursor-pointer hover:text-gray-300" onClick={() => handleSort('ticker')}>
                  Ticker{sortIcon('ticker')}
                </th>
                <th className="pb-2 pr-4">Title</th>
                <th className="pb-2 pr-4 text-right cursor-pointer hover:text-gray-300" onClick={() => handleSort('yes_price')}>
                  Yes{sortIcon('yes_price')}
                </th>
                <th className="pb-2 pr-4 text-right cursor-pointer hover:text-gray-300" onClick={() => handleSort('volume')}>
                  Vol{sortIcon('volume')}
                </th>
                <th className="pb-2 pr-4 text-right cursor-pointer hover:text-gray-300" onClick={() => handleSort('days_to_close')}>
                  Days{sortIcon('days_to_close')}
                </th>
                {SIGNAL_KEYS.map(sk => (
                  <th
                    key={sk}
                    className="pb-2 pr-3 text-right cursor-pointer hover:text-gray-300 text-[9px] uppercase tracking-wider"
                    onClick={() => handleSort('sig_' + sk)}
                  >
                    {sk.replace(/_/g, '\n').slice(0, 6)}{sortIcon('sig_' + sk)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {filtered.slice(0, 200).map(market => (
                <>
                  <tr
                    key={market.ticker}
                    className="hover:bg-surface-hover cursor-pointer transition-colors"
                    onClick={() => toggleRow(market.ticker)}
                  >
                    <td className="py-2 pr-4 font-mono text-accent-blue">
                      <span className="mr-1 text-gray-600">{expanded === market.ticker ? '▾' : '▸'}</span>
                      {market.ticker}
                    </td>
                    <td className="py-2 pr-4 text-gray-300 truncate max-w-[220px]">
                      {market.title ?? market.ticker}
                    </td>
                    <td className="py-2 pr-4 text-right font-mono text-gray-200">
                      {market.yes_price != null ? `${Number(market.yes_price).toFixed(0)}¢` : '—'}
                    </td>
                    <td className="py-2 pr-4 text-right text-gray-400">
                      {market.volume != null ? Number(market.volume).toLocaleString() : '—'}
                    </td>
                    <td className="py-2 pr-4 text-right text-gray-400">
                      {market.days_to_close != null ? Number(market.days_to_close).toFixed(0) : '—'}
                    </td>
                    {SIGNAL_KEYS.map(sk => (
                      <td key={sk} className="py-2 pr-3 text-right font-mono">
                        {signalCell(market.signals?.[sk])}
                      </td>
                    ))}
                  </tr>
                  {expanded === market.ticker && (
                    <tr key={`${market.ticker}-detail`} className="bg-surface-hover">
                      <td colSpan={6 + SIGNAL_KEYS.length} className="pb-3 px-4">
                        <MarketHistoryChart ticker={market.ticker} />
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

import { useCallback, useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function SignalGauge({ value, max = 3 }) {
  if (value == null) return <span className="text-gray-600 text-xs">n/a</span>
  const pct = Math.min(100, (Math.abs(Number(value)) / max) * 100)
  const color = Number(value) > 0 ? '#00d084' : '#ff4757'
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs">
        <span className="text-gray-500">Signal strength</span>
        <span className="font-mono" style={{ color }}>{Number(value).toFixed(3)}</span>
      </div>
      <div className="h-2 bg-surface-border rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
    </div>
  )
}

function TradeDetail({ trade }) {
  const fields = Object.entries(trade).filter(([k]) =>
    !['trade_id', 'ticker', 'signal', 'score', 'entry_price', 'status'].includes(k)
  )

  return (
    <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
      {/* Signal gauge */}
      <div className="card space-y-3">
        <p className="text-gray-400 font-medium">Signal at Entry</p>
        <SignalGauge value={trade.score ?? trade.signal_value ?? trade.entry_signal} />
        {trade.rank != null && (
          <p className="text-gray-500">
            Rank in universe: <span className="text-gray-200 font-mono">#{trade.rank}</span>
          </p>
        )}
        {(trade.base_rate ?? trade.base_rate_edge) != null && (
          <p className="text-gray-500">
            Base rate: <span className="text-gray-200 font-mono">
              {Number(trade.base_rate ?? trade.base_rate_edge).toFixed(3)}
            </span>
          </p>
        )}
      </div>

      {/* Decision chain */}
      <div className="card space-y-2">
        <p className="text-gray-400 font-medium">Decision Chain</p>
        {trade.why_selected && (
          <p className="text-gray-300">{trade.why_selected}</p>
        )}
        {trade.reasoning && (
          <p className="text-gray-300">{trade.reasoning}</p>
        )}
        {fields.slice(0, 6).map(([k, v]) => (
          <div key={k} className="flex justify-between">
            <span className="text-gray-500">{k.replace(/_/g, ' ')}</span>
            <span className="text-gray-200 font-mono truncate max-w-[120px]">
              {v == null ? '—' : typeof v === 'number' ? Number(v).toFixed(4) : String(v)}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

function TradeCard({ trade }) {
  const [open, setOpen] = useState(false)

  const idKey = ['trade_id', 'id', 'ticker'].find(k => k in trade) || Object.keys(trade)[0]
  const statusKey = ['status', 'state'].find(k => k in trade)
  const signalKey = ['signal', 'signal_family'].find(k => k in trade)

  const status = statusKey ? trade[statusKey] : null
  const statusCls =
    status === 'open' ? 'tag-green' :
    status === 'closed' ? 'tag-gray' :
    status === 'filled' ? 'tag-green' : 'tag-gray'

  return (
    <div className="card">
      <button
        className="w-full flex items-start justify-between gap-4 text-left"
        onClick={() => setOpen(o => !o)}
      >
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <span className="text-accent-blue font-mono font-medium text-sm">
              {trade.ticker ?? trade[idKey]}
            </span>
            {signalKey && (
              <span className="text-gray-500 text-xs">
                {String(trade[signalKey]).replace('KALSHI_', '')}
              </span>
            )}
            {status && <span className={statusCls}>{status}</span>}
          </div>
          <div className="flex gap-4 text-xs text-gray-500">
            {trade.entry_price != null && (
              <span>Entry: <span className="text-gray-300 font-mono">{Number(trade.entry_price).toFixed(0)}¢</span></span>
            )}
            {(trade.score ?? trade.signal_value) != null && (
              <span>Score: <span className="text-gray-300 font-mono">
                {Number(trade.score ?? trade.signal_value).toFixed(3)}
              </span></span>
            )}
            {trade.rank != null && (
              <span>Rank: <span className="text-gray-300">#{trade.rank}</span></span>
            )}
          </div>
        </div>
        <span className="text-gray-600 text-sm mt-0.5">{open ? '▾' : '▸'}</span>
      </button>
      {open && <TradeDetail trade={trade} />}
    </div>
  )
}

export default function Reasoning() {
  const [search, setSearch] = useState('')
  const fetcher = useCallback(() => api.reasoningTrades(), [])
  const { data, loading } = useApi(fetcher, 60_000)

  const trades = data?.data ?? []
  const filtered = search
    ? trades.filter(t =>
        JSON.stringify(t).toLowerCase().includes(search.toLowerCase())
      )
    : trades

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs text-gray-600 mb-1">
            Trading Platform <span className="mx-1">›</span>
            <span className="text-gray-400">Trade Reasoning</span>
          </p>
          <h1 className="text-lg font-semibold text-gray-200">Trade Reasoning Viewer</h1>
        </div>
        <input
          type="text"
          placeholder="Search trades..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="bg-surface-card border border-surface-border rounded-md px-3 py-1.5 text-xs text-gray-200 placeholder-gray-600 w-48 focus:outline-none focus:border-accent-blue"
        />
      </div>

      {loading ? (
        <div className="space-y-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="card animate-pulse h-16 bg-surface-border" />
          ))}
        </div>
      ) : data?.available === false ? (
        <EmptyState
          title={data.reason || 'No trade decisions yet'}
          icon="⊕"
          reason="Trade decisions are recorded when the autonomous loop selects candidates."
          hint="trading-cli paper run"
        />
      ) : filtered.length === 0 ? (
        <EmptyState title="No trades match" icon="⊕" />
      ) : (
        <div className="space-y-3">
          {filtered.slice(0, 50).map((trade, i) => (
            <TradeCard key={i} trade={trade} />
          ))}
          {filtered.length > 50 && (
            <p className="text-xs text-gray-600 text-center py-2">
              Showing first 50 of {filtered.length} trades
            </p>
          )}
        </div>
      )}
    </div>
  )
}

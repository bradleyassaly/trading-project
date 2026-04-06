import { useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function truncAddr(addr) {
  if (!addr || addr.length < 12) return addr || '?'
  return `${addr.slice(0, 10)}...`
}

function StatusBar({ status }) {
  if (!status) return null
  const connected = status.connected
  const markets = status.markets_subscribed || 0
  const wallets = status.watched_wallets || 0
  const t1 = status.tier1_wallets || 0
  const t2 = status.tier2_wallets || 0
  const signals = status.signals_today || 0
  const lastTs = status.last_event_ts || 0
  const age = lastTs ? Math.floor((Date.now() / 1000 - lastTs)) : 0
  const lastEvt = age < 60 ? `${age}s ago` : age < 3600 ? `${Math.floor(age / 60)}m ago` : `${Math.floor(age / 3600)}h ago`

  return (
    <div className="bg-surface-card border border-surface-border rounded px-4 py-2 flex items-center justify-between text-xs">
      <div className="flex items-center gap-2">
        <span className={`inline-block w-2 h-2 rounded-full ${connected ? 'bg-accent-green animate-pulse' : 'bg-accent-red'}`} />
        <span className="text-gray-400">WebSocket: <span className={connected ? 'text-accent-green' : 'text-accent-red'}>{connected ? 'CONNECTED' : 'DISCONNECTED'}</span></span>
      </div>
      <div className="text-gray-500">
        {markets} markets · {wallets} wallets watched ({t1} tier1 · {t2} tier2)
      </div>
      <div className="text-gray-500">
        {signals} signals today · last event {lastEvt}
      </div>
    </div>
  )
}

function WhaleCard({ alert, expanded, onToggle }) {
  const isBuy = alert.side === 'BUY'
  const borderColor = isBuy ? 'border-accent-green' : 'border-accent-red'
  const tierBg = alert.tier === 1 ? 'bg-purple-500/20 text-purple-400' : 'bg-blue-500/20 text-blue-400'
  const tierLabel = alert.tier === 1 ? 'TIER1' : 'TIER2'

  return (
    <div
      className={`bg-surface-card border border-surface-border rounded cursor-pointer hover:bg-surface-hover border-l-[3px] ${borderColor}`}
      onClick={onToggle}
    >
      <div className="px-3 py-2">
        <div className="flex items-center justify-between mb-1">
          <div className="flex items-center gap-2">
            <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${tierBg}`}>{tierLabel}</span>
            <span className="font-mono text-[10px] text-gray-400">{alert.wallet}</span>
          </div>
          <span className="text-[10px] text-gray-600">{alert.time_ago}</span>
        </div>
        <div className="flex items-center gap-2 mb-1">
          <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${isBuy ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'}`}>
            {alert.side}
          </span>
          <span className="text-xs text-gray-300 truncate max-w-[400px]">{alert.question || '—'}</span>
        </div>
        <div className="flex items-center gap-3 text-[10px]">
          <span className="px-1.5 py-0.5 rounded bg-surface-hover text-gray-400">{alert.category}</span>
          {alert.price != null && <span className="text-gray-500">price: {Number(alert.price).toFixed(2)}</span>}
          {alert.size != null && <span className="text-gray-500">size: ${Number(alert.size).toFixed(0)}</span>}
          {alert.signal_fired
            ? <span className="text-accent-green">→ Signal fired</span>
            : <span className="text-gray-600">→ Monitoring only</span>
          }
        </div>
      </div>
      {expanded && (
        <div className="px-3 pb-2 pt-1 border-t border-surface-border text-[10px] text-gray-500 space-y-0.5">
          <p>Full question: {alert.question}</p>
          <p>Wallet: {alert.wallet_full || alert.wallet}</p>
          {alert.directional_win_rate != null && <p>Win rate: {(alert.directional_win_rate * 100).toFixed(1)}%</p>}
          <p>Tier: {alert.tier}</p>
        </div>
      )}
    </div>
  )
}

function CategoryTable({ data }) {
  if (!data || !data.length) return <p className="text-xs text-gray-600">No category data yet</p>
  return (
    <table className="w-full text-[10px]">
      <thead>
        <tr className="border-b border-surface-border text-gray-500 text-left">
          <th className="pb-1 pr-2">Category</th>
          <th className="pb-1 pr-2 text-right">Signals</th>
          <th className="pb-1 pr-2 text-right">Win Rate</th>
          <th className="pb-1 text-right">P&L</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-surface-border">
        {data.map(c => {
          const wr = c.win_rate
          const resolved = c.signals_resolved || 0
          return (
            <tr key={c.category} className="text-gray-400">
              <td className="py-1 pr-2">{c.category}</td>
              <td className="py-1 pr-2 text-right">{c.signals_fired || 0}</td>
              <td className="py-1 pr-2 text-right">
                {resolved >= 5
                  ? <span className={wr >= 0.5 ? 'text-accent-green' : 'text-accent-red'}>{(wr * 100).toFixed(0)}%</span>
                  : <span className="text-gray-600">—</span>
                }
              </td>
              <td className="py-1 text-right font-mono">
                {c.total_pnl != null ? `$${Number(c.total_pnl).toFixed(0)}` : '—'}
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function SignalItem({ signal, expanded, onToggle }) {
  return (
    <div
      className={`text-[10px] py-1.5 px-2 rounded cursor-pointer hover:bg-surface-hover ${signal.executed ? 'text-accent-green' : 'text-gray-500'}`}
      onClick={onToggle}
    >
      <div className="flex items-center gap-2">
        <span className="px-1 py-0.5 rounded bg-surface-hover text-[9px] text-gray-400">{signal.signal_type}</span>
        <span className={signal.direction === 'BUY' ? 'text-accent-green' : 'text-accent-red'}>{signal.direction}</span>
        <span>·</span>
        <span>{signal.category}</span>
        <span>·</span>
        <span>conf {signal.confidence != null ? (signal.confidence * 100).toFixed(0) + '%' : '—'}</span>
        {signal.size != null && <><span>·</span><span>${Math.abs(Number(signal.size)).toFixed(0)}</span></>}
        <span className="ml-auto text-gray-600">{signal.time_ago}</span>
      </div>
      {expanded && (
        <div className="mt-1 text-gray-600">
          <p>{signal.question}</p>
          <p>Wallet: {signal.wallet}</p>
        </div>
      )}
    </div>
  )
}

export default function MarketScanner() {
  const { data: statusData } = useApi(api.polymarketSubscriptionStatus, 30_000)
  const { data: feedData, loading: feedL } = useApi(api.polymarketWhaleFeed, 10_000)
  const { data: catData } = useApi(api.polymarketCategoryPerformance, 60_000)
  const { data: sigData } = useApi(api.polymarketSignalsFeed, 10_000)
  const [expandedAlert, setExpandedAlert] = useState(null)
  const [expandedSignal, setExpandedSignal] = useState(null)

  const status = statusData || {}
  const alerts = feedData?.data ?? []
  const categories = catData?.data ?? []
  const signals = sigData?.data ?? []

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Whale Monitor</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Whale Monitor</h1>
      </div>

      <StatusBar status={status} />

      <div className="flex gap-4">
        {/* LEFT COLUMN — Live Whale Feed */}
        <div className="flex-[62] space-y-2">
          <div className="flex items-center gap-2 mb-2">
            <h2 className="text-sm font-medium text-gray-300">Live Whale Activity</h2>
            <span className="flex items-center gap-1 text-[9px] text-gray-500">
              <span className="inline-block w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" />
              live · 10s
            </span>
          </div>

          {feedL && !feedData ? (
            <LoadingSkeleton rows={6} />
          ) : !alerts.length ? (
            <EmptyState
              title={`Monitoring ${status.markets_subscribed || 0} markets across 9 categories`}
              message="Whale activity will appear here in real time"
            />
          ) : (
            <div className="space-y-1.5">
              {alerts.map((a, i) => (
                <WhaleCard
                  key={`${a.wallet}-${a.fired_at}-${i}`}
                  alert={a}
                  expanded={expandedAlert === i}
                  onToggle={() => setExpandedAlert(expandedAlert === i ? null : i)}
                />
              ))}
            </div>
          )}
        </div>

        {/* RIGHT COLUMN */}
        <div className="flex-[38] space-y-4">
          {/* Category Breakdown */}
          <div className="card">
            <h3 className="text-xs font-medium text-gray-300 mb-2">Signal Performance by Category</h3>
            <CategoryTable data={categories} />
          </div>

          {/* Signal Feed */}
          <div className="card">
            <h3 className="text-xs font-medium text-gray-300 mb-2">Signal Feed</h3>
            {!signals.length ? (
              <p className="text-[10px] text-gray-600">No signals yet</p>
            ) : (
              <div className="space-y-0.5">
                {signals.slice(0, 10).map((s, i) => (
                  <SignalItem
                    key={`${s.fired_at}-${i}`}
                    signal={s}
                    expanded={expandedSignal === i}
                    onToggle={() => setExpandedSignal(expandedSignal === i ? null : i)}
                  />
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

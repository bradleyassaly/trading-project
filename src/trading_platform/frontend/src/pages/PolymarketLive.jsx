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

export default function PolymarketLive() {
  const { data, loading, error } = useApi(api.polymarketLiveMarkets, 15_000)

  if (loading && !data) return <LoadingSkeleton rows={8} />
  if (error) {
    return (
      <div className="p-6">
        <h1 className="text-lg font-semibold mb-4">Polymarket Live</h1>
        <p className="text-red-400 text-sm">Failed to load live markets: {String(error)}</p>
      </div>
    )
  }

  const markets = data?.data ?? []

  return (
    <div className="p-6">
      <div className="flex items-center gap-3 mb-6">
        <h1 className="text-lg font-semibold">Polymarket Live</h1>
        {markets.length > 0 && (
          <span className="text-xs text-gray-500">{markets.length} markets</span>
        )}
      </div>

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
                <th className="pb-2 pr-4">Status</th>
                <th className="pb-2 pr-4">Market ID</th>
                <th className="pb-2 pr-4 text-right">Yes Price</th>
                <th className="pb-2 pr-4 text-right">Last Tick</th>
              </tr>
            </thead>
            <tbody>
              {markets.map((m) => (
                <tr
                  key={m.market_id}
                  className="border-b border-surface-border/50 hover:bg-surface-hover transition-colors"
                >
                  <td className="py-2 pr-4">
                    <LiveBadge />
                  </td>
                  <td className="py-2 pr-4 text-gray-300 font-mono text-xs">
                    {m.market_id}
                  </td>
                  <td className="py-2 pr-4 text-right font-mono">
                    <span className={m.yes_price >= 50 ? 'text-accent-green' : 'text-accent-red'}>
                      {m.yes_price?.toFixed(1)}
                    </span>
                  </td>
                  <td className="py-2 pr-4 text-right text-xs text-gray-500">
                    {m.last_tick_at ? new Date(m.last_tick_at).toLocaleTimeString() : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

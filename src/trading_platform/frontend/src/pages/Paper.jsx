import { useCallback } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function fmtTs(ts) {
  if (ts == null) return '--'
  if (typeof ts === 'number') {
    return new Date(ts * 1000).toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
    })
  }
  return String(ts).slice(0, 16)
}

function GoLiveProgress({ positions, attribution }) {
  const totalTrades = attribution?.reduce((sum, a) => sum + (a.total_trades || 0), 0) ?? 0
  const closedTrades = attribution?.reduce((sum, a) => sum + (a.closed || 0), 0) ?? 0
  const wins = attribution?.reduce((sum, a) => sum + (a.wins || 0), 0) ?? 0
  const winRate = closedTrades > 0 ? (wins / closedTrades * 100) : null
  const threshold = 50
  const pct = Math.min(100, (closedTrades / threshold) * 100)

  return (
    <div className="bg-surface-card rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <p className="text-xs text-gray-500">Path to Live</p>
        <span className="text-[10px] text-gray-500">
          {closedTrades} / {threshold} resolved
        </span>
      </div>
      <div className="w-full bg-gray-800 rounded-full h-2 mb-3">
        <div
          className={`h-2 rounded-full transition-all ${pct >= 100 ? 'bg-accent-green' : 'bg-accent-blue'}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <div className="flex gap-4 text-[10px] text-gray-400">
        <span>Win rate: <span className={`font-mono ${winRate && winRate > 55 ? 'text-accent-green' : 'text-gray-300'}`}>
          {winRate != null ? `${winRate.toFixed(1)}%` : 'N/A'}
        </span></span>
        <span>{totalTrades} trades ({closedTrades} resolved)</span>
        <span className="text-accent-green">Kill switch: Active</span>
      </div>
    </div>
  )
}

function PortfolioSummary({ data }) {
  if (!data) return null
  const startingBankroll = data.starting_bankroll || 100000
  const polymarketCash = data.polymarket_cash ?? startingBankroll
  const polymarketDeployed = data.polymarket_deployed || 0
  const polymarketOpen = data.polymarket_open_count || 0
  const realizedPnl = data.portfolio?.realized_pnl || 0
  const totalTrades = data.total_trades || 0

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div className="bg-surface-card rounded-lg p-4">
        <p className="text-xs text-gray-500 mb-1">Cash Available</p>
        <p className="text-xl font-bold font-mono text-gray-200">${polymarketCash.toLocaleString(undefined, {maximumFractionDigits: 0})}</p>
        <p className="text-[10px] text-gray-500">starting: ${startingBankroll.toLocaleString()}</p>
      </div>
      <div className="bg-surface-card rounded-lg p-4">
        <p className="text-xs text-gray-500 mb-1">Deployed</p>
        <p className="text-xl font-bold font-mono text-gray-200">${polymarketDeployed.toLocaleString(undefined, {maximumFractionDigits: 0})}</p>
        <p className="text-[10px] text-gray-500">{polymarketOpen} open positions</p>
      </div>
      <div className="bg-surface-card rounded-lg p-4">
        <p className="text-xs text-gray-500 mb-1">Realized P&L</p>
        <p className={`text-xl font-bold font-mono ${realizedPnl >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
          {realizedPnl >= 0 ? '+' : ''}${realizedPnl.toFixed(2)}
        </p>
      </div>
      <div className="bg-surface-card rounded-lg p-4">
        <p className="text-xs text-gray-500 mb-1">Total Trades</p>
        <p className="text-xl font-bold font-mono text-gray-200">{totalTrades}</p>
      </div>
    </div>
  )
}

function OpenPositions({ positions }) {
  if (!positions?.length) return <EmptyState title="No open positions" icon="$" />
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Platform</th>
            <th className="pb-2 pr-3">Market</th>
            <th className="pb-2 pr-3">Side</th>
            <th className="pb-2 pr-3 text-right">Entry</th>
            <th className="pb-2 pr-3 text-right">Size</th>
            <th className="pb-2 pr-3">Signal</th>
            <th className="pb-2 pr-3 text-right">Conf</th>
            <th className="pb-2 pr-3">Opened</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {positions.map(p => (
            <tr key={p.id} className="hover:bg-surface-hover">
              <td className="py-1.5 pr-3">
                <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${
                  p.platform === 'polymarket' ? 'bg-purple-500/20 text-purple-400' : 'bg-accent-blue/20 text-accent-blue'
                }`}>{p.platform}</span>
              </td>
              <td className="py-1.5 pr-3 font-mono text-gray-300 text-[10px]">
                {p.question || (p.ticker?.length > 20 && !p.ticker.includes(' ')
                  ? `${p.ticker.slice(0, 8)}...${p.ticker.slice(-6)}`
                  : p.ticker)}
              </td>
              <td className={`py-1.5 pr-3 font-bold ${p.side === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>{p.side}</td>
              <td className="py-1.5 pr-3 text-right font-mono">{p.entry_price?.toFixed(1)}</td>
              <td className="py-1.5 pr-3 text-right font-mono">${p.size_usd?.toFixed(2)}</td>
              <td className="py-1.5 pr-3 text-gray-400">{p.signal_type}</td>
              <td className="py-1.5 pr-3 text-right font-mono">{p.confidence?.toFixed(2)}</td>
              <td className="py-1.5 pr-3 text-gray-500 text-[10px]">{fmtTs(p.entry_ts)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Wallet Intelligence — signals fired by whale_signal_engine in
// response to a real watched wallet's trade.
const WHALE_SIGNALS = [
  'wallet_reversal', 'cascade', 'oversized_bet', 'accumulation',
  'convergence', 'specialist_entry',
  'pre_deadline_surge', 'whale_entry',
  'whale_exit', 'no_position_entry', 'position_reduction',
]

// Technical scanners — fired by velocity_detector / order_book_monitor
// without any wallet basis. These pollute fusion / Kelly calibration
// and the previous validation report flagged them as the source of
// the fictional +$4.97M PnL on degenerate cheap-NO trades.
const TECHNICAL_SIGNALS = [
  'price_velocity', 'market_maker_flip',
]

function WhaleSignalAttribution({ attribution }) {
  const byType = {}
  ;(attribution || []).forEach(a => { byType[a.signal_type] = a })

  return (
    <div className="grid grid-cols-3 md:grid-cols-5 gap-2">
      {WHALE_SIGNALS.map(sig => {
        const a = byType[sig] || {}
        const fired = a.total_trades || 0
        const closed = a.closed || 0
        const wins = a.wins || 0
        const wr = closed > 0 ? (wins / closed * 100) : null
        const wrColor = wr && wr > 55 ? 'text-accent-green' : wr && wr > 50 ? 'text-yellow-400' : wr != null ? 'text-accent-red' : 'text-gray-600'
        return (
          <div key={sig} className="bg-surface-card rounded p-2">
            <p className="text-[9px] text-gray-500 truncate">{sig.replace(/_/g, ' ')}</p>
            <p className={`text-sm font-bold font-mono ${wrColor}`}>{wr != null ? `${wr.toFixed(0)}%` : '—'}</p>
            <p className="text-[9px] text-gray-600">{fired} fired · {closed} resolved</p>
          </div>
        )
      })}
    </div>
  )
}

function SignalAttribution({ attribution }) {
  if (!attribution?.length) return <EmptyState title="No trade data" icon="%" />
  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
      {attribution.map(a => {
        const wr = a.closed > 0 ? (a.wins / a.closed * 100) : 0
        const wrColor = wr > 55 ? 'text-accent-green' : wr > 50 ? 'text-accent-yellow' : wr > 0 ? 'text-accent-red' : 'text-gray-500'
        return (
          <div key={a.signal_type} className="bg-surface-card rounded-lg p-3">
            <p className="text-xs text-gray-500 mb-1">{a.signal_type}</p>
            <p className={`text-lg font-bold font-mono ${wrColor}`}>
              {a.closed > 0 ? `${wr.toFixed(1)}%` : 'n/a'}
            </p>
            <p className="text-[10px] text-gray-500 mt-1">
              {a.total_trades} trades ({a.closed} resolved, {a.wins} wins)
              {a.avg_stake ? ` | avg $${a.avg_stake.toFixed(2)}` : ''}
            </p>
          </div>
        )
      })}
    </div>
  )
}

function RecentClosed({ trades }) {
  if (!trades?.length) return <EmptyState title="No resolved trades yet" message="Waiting for markets to close..." />
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-3">Platform</th>
            <th className="pb-2 pr-3">Market</th>
            <th className="pb-2 pr-3">Side</th>
            <th className="pb-2 pr-3">Signal</th>
            <th className="pb-2 pr-3 text-right">Entry</th>
            <th className="pb-2 pr-3 text-right">Exit</th>
            <th className="pb-2 pr-3 text-right">Return</th>
            <th className="pb-2 pr-3">Outcome</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {trades.map((t, i) => (
            <tr key={i} className="hover:bg-surface-hover">
              <td className="py-1.5 pr-3">
                <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${
                  t.platform === 'polymarket' ? 'bg-purple-500/20 text-purple-400' : 'bg-accent-blue/20 text-accent-blue'
                }`}>{t.platform}</span>
              </td>
              <td className="py-1.5 pr-3 font-mono text-[10px] text-gray-300">
                {t.question || (t.ticker?.length > 20 && !t.ticker.includes(' ')
                  ? `${t.ticker.slice(0, 8)}...${t.ticker.slice(-6)}`
                  : t.ticker)}
              </td>
              <td className={`py-1.5 pr-3 ${t.side === 'YES' ? 'text-accent-green' : 'text-accent-red'}`}>{t.side}</td>
              <td className="py-1.5 pr-3 text-gray-400">{t.signal_type}</td>
              <td className="py-1.5 pr-3 text-right font-mono">{t.entry_price?.toFixed(1)}</td>
              <td className="py-1.5 pr-3 text-right font-mono">{t.exit_price?.toFixed(1)}</td>
              <td className={`py-1.5 pr-3 text-right font-mono ${(t.return_pct || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                {t.return_pct != null ? `${(t.return_pct * 100).toFixed(1)}%` : '-'}
              </td>
              <td className="py-1.5 pr-3">
                <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${
                  t.outcome === 'win' ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-red/20 text-accent-red'
                }`}>{t.outcome}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function Paper() {
  const { data, loading } = useApi(api.paperDashboard, 30_000)

  if (loading && !data) return <div className="p-6"><LoadingSkeleton rows={8} /></div>

  const available = data?.available !== false
  const portfolio = data?.portfolio
  const positions = data?.positions ?? []
  const attribution = data?.attribution ?? []
  const recent = data?.recent_closed ?? []
  const platforms = data?.platforms ?? {}

  return (
    <div className="p-6 space-y-6">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          Trading Platform <span className="mx-1">></span>
          <span className="text-gray-400">Paper Trading</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200">Paper Trading Dashboard</h1>
      </div>

      {!available ? (
        <EmptyState title={data?.reason || "No paper trading data"} message="Run the autonomous loop to start paper trading" />
      ) : (
        <>
          <PortfolioSummary data={data} />

          <GoLiveProgress positions={positions} attribution={attribution} />

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-4">
              <span className="mr-2">🧠</span>Wallet Intelligence Signals
            </h2>
            <p className="text-[10px] text-gray-600 mb-3">Fired by the smart-money pipeline in response to watched-wallet trades.</p>
            <WhaleSignalAttribution attribution={(attribution || []).filter(a => WHALE_SIGNALS.includes(a.signal_type))} />
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-4">
              <span className="mr-2">📊</span>Technical Scanners
            </h2>
            <p className="text-[10px] text-gray-600 mb-3">
              Price-velocity and order-book-imbalance signals — no wallet basis. Historical trades placed before the fillability filter (entry_price 0.05–0.95) bias these stats; treat with care.
            </p>
            <SignalAttribution attribution={(attribution || []).filter(a => TECHNICAL_SIGNALS.includes(a.signal_type))} />
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-4">
              Open Positions ({positions.length})
            </h2>
            <OpenPositions positions={positions} />
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-4">
              Recent Resolved Trades
            </h2>
            <RecentClosed trades={recent} />
          </div>
        </>
      )}
    </div>
  )
}

import { useCallback } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

const STATE_LABELS = {
  running:        'Running',
  stopped:        'Paused',
  trigger_pending:'Waiting for next trigger',
  unknown:        'Unknown',
}

function Breadcrumb({ page }) {
  return (
    <p className="text-xs text-gray-600 mb-1">
      Trading Platform <span className="mx-1">›</span>
      <span className="text-gray-400">{page}</span>
    </p>
  )
}

function StatusStrip({ status }) {
  if (!status) return null
  const { loop_state, last_run_timestamp, active_strategy_count } = status

  const stateLabel = STATE_LABELS[loop_state] ?? loop_state

  const dotClass =
    loop_state === 'running'         ? 'bg-accent-green animate-pulse' :
    loop_state === 'stopped'         ? 'bg-accent-red'                  :
    loop_state === 'trigger_pending' ? 'bg-accent-yellow animate-pulse' :
                                       'bg-gray-500'

  const textColor =
    loop_state === 'running'         ? 'text-accent-green' :
    loop_state === 'stopped'         ? 'text-accent-red'   :
    loop_state === 'trigger_pending' ? 'text-accent-yellow': 'text-gray-400'

  const lastRunAge = last_run_timestamp
    ? (() => {
        const diff = (Date.now() - new Date(last_run_timestamp).getTime()) / 60_000
        if (diff < 60) return `${Math.round(diff)}m ago`
        return `${Math.round(diff / 60)}h ago`
      })()
    : null

  return (
    <div className="flex items-center gap-6 px-6 py-2.5 bg-surface-card border-b border-surface-border text-xs">
      <div className="flex items-center gap-2">
        <span className={`w-2 h-2 rounded-full inline-block flex-shrink-0 ${dotClass}`} />
        <span className={`font-semibold ${textColor}`}>{stateLabel}</span>
      </div>
      {lastRunAge && (
        <span className="text-gray-500">
          Last run: <span className="text-gray-300">{lastRunAge}</span>
        </span>
      )}
      {active_strategy_count != null && (
        <span className="text-gray-500">
          Strategies: <span className="text-gray-300">{active_strategy_count}</span>
        </span>
      )}
    </div>
  )
}

function PnlCard({ label, value, sub, emptyHint }) {
  const isNeg = typeof value === 'number' && value < 0
  const isEmpty = value == null

  return (
    <div className="card">
      <p className="text-xs text-gray-500 uppercase tracking-wider mb-1">{label}</p>
      {isEmpty ? (
        <div className="space-y-1">
          <p className="text-2xl font-bold font-mono text-gray-700">—</p>
          {emptyHint && (
            <p className="text-[10px] text-gray-600 leading-tight">{emptyHint}</p>
          )}
        </div>
      ) : (
        <>
          <p className={`text-2xl font-bold font-mono ${isNeg ? 'text-accent-red' : 'text-accent-green'}`}>
            {typeof value === 'number' ? value.toFixed(2) : value}
          </p>
          {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
        </>
      )}
    </div>
  )
}

function EquityCurveChart({ data }) {
  if (!data || data.length === 0) {
    return (
      <EmptyState
        title="No equity curve yet"
        icon="📈"
        hint="trading-cli paper run"
      />
    )
  }

  const equityKey = ['equity', 'portfolio_value', 'value'].find(k => k in data[0])
  const dateKey = ['date', 'timestamp', 'time'].find(k => k in data[0])

  if (!equityKey || !dateKey) {
    return <EmptyState title="Unrecognised equity curve columns" icon="📈" />
  }

  const formatted = data.map(row => ({
    date: String(row[dateKey] ?? '').slice(0, 10),
    equity: row[equityKey],
  }))

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={formatted} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#2a2d3e" />
        <XAxis
          dataKey="date"
          tick={{ fill: '#6b7280', fontSize: 10 }}
          tickLine={false}
          axisLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          tick={{ fill: '#6b7280', fontSize: 10 }}
          tickLine={false}
          axisLine={false}
          tickFormatter={v => `$${(v / 1000).toFixed(0)}k`}
        />
        <Tooltip
          contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3e', borderRadius: 6, fontSize: 12 }}
          labelStyle={{ color: '#9ca3af' }}
          formatter={v => [`$${Number(v).toFixed(2)}`, 'Equity']}
        />
        <Line
          type="monotone"
          dataKey="equity"
          stroke="#00d084"
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4, fill: '#00d084' }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

function SignalMiniTable({ data }) {
  if (!data || data.length === 0) {
    return (
      <EmptyState
        title="No signal data"
        icon="⎄"
        hint="trading-cli research kalshi-full-backtest"
      />
    )
  }

  const nameKey = ['signal_family', 'signal', 'name'].find(k => k in data[0]) || Object.keys(data[0])[0]

  const colorClass = v => {
    if (v == null) return 'text-gray-500'
    return v > 0.1 ? 'text-accent-green' : v < 0 ? 'text-accent-red' : 'text-gray-300'
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            <th className="pb-2 pr-4">Signal</th>
            <th className="pb-2 pr-4 text-right">IC</th>
            <th className="pb-2 pr-4 text-right">Win%</th>
            <th className="pb-2 pr-4 text-right">Sharpe</th>
            <th className="pb-2 text-right">Trades</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {data.slice(0, 5).map((row, i) => (
            <tr key={i} className="hover:bg-surface-hover transition-colors">
              <td className="py-1.5 pr-4 text-gray-300 truncate max-w-[160px]">
                {String(row[nameKey] ?? '—').replace('KALSHI_', '')}
              </td>
              <td className={`py-1.5 pr-4 text-right font-mono ${colorClass(row.ic)}`}>
                {row.ic != null ? Number(row.ic).toFixed(3) : '—'}
              </td>
              <td className={`py-1.5 pr-4 text-right font-mono ${colorClass(row.win_rate - 0.5)}`}>
                {row.win_rate != null ? `${(Number(row.win_rate) * 100).toFixed(1)}%` : '—'}
              </td>
              <td className={`py-1.5 pr-4 text-right font-mono ${colorClass(row.sharpe)}`}>
                {row.sharpe != null ? Number(row.sharpe).toFixed(2) : '—'}
              </td>
              <td className="py-1.5 text-right text-gray-400">
                {row.n_trades ?? row.sample_size ?? '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DecisionFeed({ data }) {
  if (!data || data.length === 0) {
    return (
      <EmptyState
        title="No loop decisions yet"
        icon="⊛"
        hint="trading-cli loop run"
      />
    )
  }

  return (
    <ul className="space-y-2">
      {data.slice(-5).reverse().map((entry, i) => (
        <li key={i} className="flex gap-3 text-xs">
          <span className="text-gray-600 flex-shrink-0 font-mono">
            {String(entry.timestamp ?? '').slice(11, 16) || '??:??'}
          </span>
          <span className="text-gray-300">
            <span className="text-accent-blue font-medium">{entry.action ?? entry.trigger ?? 'event'}</span>
            {entry.reasoning && ` — ${String(entry.reasoning).slice(0, 80)}`}
          </span>
        </li>
      ))}
    </ul>
  )
}

export default function Dashboard() {
  const statusFetcher   = useCallback(() => api.systemStatus(),        [])
  const equityFetcher   = useCallback(() => api.equityCurve(),         [])
  const pnlFetcher      = useCallback(() => api.pnlSummary(),          [])
  const signalsFetcher  = useCallback(() => api.signalsPerformance(),  [])
  const decisionsFetcher= useCallback(() => api.loopDecisions(),       [])

  const { data: status }                      = useApi(statusFetcher,    15_000)
  const { data: equity,  loading: equityL }   = useApi(equityFetcher,   30_000)
  const { data: pnl,     loading: pnlL    }   = useApi(pnlFetcher,      30_000)
  const { data: signals, loading: signalsL }  = useApi(signalsFetcher,  60_000)
  const { data: decisions, loading: decisionsL } = useApi(decisionsFetcher, 30_000)

  const noData = !pnlL && pnl?.available === false

  return (
    <div className="flex flex-col min-h-screen">
      <StatusStrip status={status} />

      <div className="p-6 space-y-6">
        <div>
          <Breadcrumb page="Command Center" />
          <h1 className="text-lg font-semibold text-gray-200">Command Center</h1>
        </div>

        {/* P&L Summary Cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {pnlL ? (
            Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="card animate-pulse h-20 bg-surface-border" />
            ))
          ) : (
            <>
              <PnlCard
                label="Today P&L"
                value={pnl?.today_pnl}
                sub="latest daily return"
                emptyHint="Start paper trading to see live P&L"
              />
              <PnlCard
                label="Total P&L"
                value={pnl?.total_pnl}
                sub="since inception"
                emptyHint="Run: trading-cli paper run"
              />
              <PnlCard
                label="Max Drawdown"
                value={pnl?.max_drawdown != null ? `${(pnl.max_drawdown * 100).toFixed(1)}%` : null}
                sub="peak-to-trough"
                emptyHint="No paper equity curve yet"
              />
              <PnlCard
                label="Sharpe"
                value={pnl?.sharpe}
                sub="annualised"
                emptyHint="No return history yet"
              />
            </>
          )}
        </div>

        {/* Equity Curve */}
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Equity Curve</h2>
          {equityL ? (
            <LoadingSkeleton rows={3} />
          ) : (
            <EquityCurveChart data={equity?.data} />
          )}
        </div>

        {/* Bottom row */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-4">Signal Performance</h2>
            {signalsL ? <LoadingSkeleton rows={4} /> : <SignalMiniTable data={signals?.data} />}
          </div>

          <div className="card">
            <h2 className="text-sm font-medium text-gray-400 mb-4">Recent Loop Decisions</h2>
            {decisionsL ? <LoadingSkeleton rows={4} /> : <DecisionFeed data={decisions?.data} />}
          </div>
        </div>
      </div>
    </div>
  )
}

import { useCallback, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

const COLS = [
  { key: 'signal_family', label: 'Signal', sortable: false },
  { key: 'n_trades', label: 'Trades', sortable: true },
  { key: 'win_rate', label: 'Win %', sortable: true },
  { key: 'realized_avg_return', label: 'Avg Return', sortable: true },
  { key: 'brier_score', label: 'Brier', sortable: true },
  { key: 'ic', label: 'IC', sortable: true },
]

function colorFor(val, neutral = 0) {
  if (val == null) return 'text-gray-500'
  return val > neutral ? 'text-accent-green' : val < neutral ? 'text-accent-red' : 'text-gray-400'
}

function PerformanceTable({ data, sortKey, sortDir, onSort }) {
  if (!data || data.length === 0) return <EmptyState title="No performance data" icon="⎄" />
  const nameKey = ['signal_family', 'signal', 'name'].find(k => k in data[0]) || COLS[0].key

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border text-gray-500 text-left">
            {COLS.map(col => (
              <th
                key={col.key}
                className={`pb-2 pr-6 ${col.sortable ? 'cursor-pointer select-none hover:text-gray-300' : ''}`}
                onClick={col.sortable ? () => onSort(col.key) : undefined}
              >
                {col.label}
                {sortKey === col.key && (
                  <span className="ml-1 text-accent-blue">{sortDir === 'asc' ? '▲' : '▼'}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {data.map((row, i) => {
            const wr = row.win_rate != null ? Number(row.win_rate) : null
            const wrPct = wr != null ? (wr <= 1 ? wr * 100 : wr) : null
            const trades = Number(row.n_trades ?? row.sample_size ?? 0)
            const avgRet = row.realized_avg_return ?? row.mean_edge
            const brier = row.brier_score ?? row.sharpe
            return (
              <tr key={i} className="hover:bg-surface-hover transition-colors">
                <td className="py-2 pr-6 text-gray-200 font-medium">
                  {String(row[nameKey] ?? '—').replace(/^kalshi_/i, '').replace('metaculus_divergence', 'crowd_divergence')}
                </td>
                <td className="py-2 pr-6 text-gray-400 font-mono">
                  {trades > 0 ? trades.toLocaleString() : <span className="text-gray-600">0</span>}
                </td>
                <td className={`py-2 pr-6 font-mono ${
                  wrPct == null ? 'text-gray-600' :
                  wrPct > 55 ? 'text-accent-green' :
                  wrPct > 50 ? 'text-accent-yellow' : 'text-accent-red'
                }`}>
                  {wrPct != null ? `${wrPct.toFixed(1)}%` : trades === 0 ? 'Needs data' : '—'}
                </td>
                <td className={`py-2 pr-6 font-mono ${colorFor(avgRet != null ? Number(avgRet) : null)}`}>
                  {avgRet != null ? Number(avgRet).toFixed(4) : '—'}
                </td>
                <td className="py-2 pr-6 font-mono text-gray-400">
                  {brier != null ? Number(brier).toFixed(4) : '—'}
                </td>
                <td className={`py-2 pr-6 font-mono ${colorFor(row.ic != null ? Number(row.ic) : null)}`}>
                  {row.ic != null ? Number(row.ic).toFixed(4) : '—'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SignalBarChart({ data, metricKey, label, neutralVal = 0 }) {
  if (!data || data.length === 0) return <EmptyState title={`No ${label} data`} icon="⎄" />
  const nameKey = ['signal_family', 'signal', 'name'].find(k => k in data[0]) || Object.keys(data[0])[0]

  const chartData = data.map(r => ({
    name: String(r[nameKey] ?? '').replace('KALSHI_', '').slice(0, 18),
    value: r[metricKey] != null ? Number(r[metricKey]) : 0,
  }))

  return (
    <ResponsiveContainer width="100%" height={180}>
      <BarChart data={chartData} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#2a2d3e" vertical={false} />
        <XAxis dataKey="name" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} />
        <YAxis tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} />
        <Tooltip
          contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3e', borderRadius: 6, fontSize: 11 }}
          cursor={{ fill: 'rgba(255,255,255,0.04)' }}
        />
        <Bar dataKey="value" name={label} radius={[3, 3, 0, 0]}>
          {chartData.map((entry, i) => (
            <Cell
              key={i}
              fill={entry.value > neutralVal ? '#00d084' : entry.value < neutralVal ? '#ff4757' : '#3d7fff'}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function CorrelationHeatmap({ signals, matrix }) {
  if (!signals || signals.length === 0 || !matrix || matrix.length === 0) {
    return <EmptyState title="No correlation data" icon="⎄" />
  }

  function cellColor(val) {
    if (val == null) return '#1a1d27'
    const v = Math.max(-1, Math.min(1, val))
    if (v > 0.7) return '#00d084'
    if (v > 0.3) return '#22c55e'
    if (v < -0.7) return '#ff4757'
    if (v < -0.3) return '#ef4444'
    return '#2a2d3e'
  }

  const shortName = s => String(s).replace('KALSHI_', '').replace('_', ' ').slice(0, 12)

  return (
    <div className="overflow-x-auto">
      <div className="inline-grid gap-0.5" style={{ gridTemplateColumns: `80px repeat(${signals.length}, 56px)` }}>
        {/* Header row */}
        <div />
        {signals.map(s => (
          <div key={s} className="text-center text-gray-500 text-[9px] truncate px-0.5 py-1">
            {shortName(s)}
          </div>
        ))}
        {/* Data rows */}
        {matrix.map((row, ri) => (
          <>
            <div key={`lbl-${ri}`} className="text-gray-500 text-[9px] truncate flex items-center pr-1">
              {shortName(signals[ri])}
            </div>
            {row.map((val, ci) => (
              <div
                key={ci}
                className="h-10 flex items-center justify-center text-[9px] font-mono rounded-sm"
                style={{ backgroundColor: cellColor(val), color: val != null && Math.abs(val) > 0.4 ? '#fff' : '#9ca3af' }}
                title={`${signals[ri]} × ${signals[ci]}: ${val != null ? val.toFixed(3) : 'n/a'}`}
              >
                {val != null ? val.toFixed(2) : '—'}
              </div>
            ))}
          </>
        ))}
      </div>
    </div>
  )
}

function BacktestResultsTable({ data }) {
  if (!data || !data.rows || data.rows.length === 0) {
    return (
      <EmptyState
        title="No backtest run yet"
        icon="⎄"
        hint="python scripts/signal_engine_backtest.py --days 60 --write"
      />
    )
  }
  const runAgo = data.run_ts
    ? Math.round((Date.now() / 1000 - data.run_ts) / 3600)
    : null
  return (
    <div>
      <div className="text-[10px] text-gray-500 mb-2">
        Last run {runAgo != null ? `${runAgo}h ago` : '—'} ·
        {' '}lookback {data.rows[0]?.lookback_days ?? '?'}d · {data.n_signal_types} signal types
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-2 pr-4">Signal</th>
              <th className="pb-2 pr-4 text-right">Fires</th>
              <th className="pb-2 pr-4 text-right">Resolved</th>
              <th className="pb-2 pr-4 text-right">WR</th>
              <th className="pb-2 pr-4 text-right">EV</th>
              <th className="pb-2 pr-4 text-right">ΣΔ</th>
              <th className="pb-2 pr-4 text-right">Live n</th>
              <th className="pb-2 pr-4">Verdict</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {data.rows.map((r, i) => {
              const ev = r.backtest_ev
              const wr = r.backtest_wr != null ? r.backtest_wr * 100 : null
              const verdict =
                r.backtest_resolved == null || r.backtest_resolved < 5 ? { label: '◌ no data', cls: 'text-gray-500' }
                : r.backtest_resolved < 20 ? { label: `? n=${r.backtest_resolved}`, cls: 'text-gray-400' }
                : ev > 0.05 ? { label: '✓ positive', cls: 'text-accent-green' }
                : ev < -0.02 ? { label: '✗ negative', cls: 'text-accent-red' }
                : { label: '◐ flat', cls: 'text-gray-400' }
              return (
                <tr key={i} className="hover:bg-surface-hover transition-colors">
                  <td className="py-2 pr-4 text-gray-200 font-medium">{r.signal_type}</td>
                  <td className="py-2 pr-4 text-right text-gray-400 font-mono">{(r.backtest_fires ?? 0).toLocaleString()}</td>
                  <td className="py-2 pr-4 text-right text-gray-400 font-mono">{(r.backtest_resolved ?? 0).toLocaleString()}</td>
                  <td className="py-2 pr-4 text-right font-mono">
                    <span className={wr != null && wr >= 52 ? 'text-accent-green' : 'text-gray-400'}>
                      {wr != null ? `${wr.toFixed(0)}%` : '—'}
                    </span>
                  </td>
                  <td className="py-2 pr-4 text-right font-mono">
                    <span className={ev == null ? 'text-gray-500' : ev > 0 ? 'text-accent-green' : 'text-accent-red'}>
                      {ev != null ? (ev > 0 ? '+' : '') + ev.toFixed(3) : '—'}
                    </span>
                  </td>
                  <td className="py-2 pr-4 text-right text-gray-400 font-mono">
                    {r.backtest_total_delta != null ? (r.backtest_total_delta >= 0 ? '+' : '') + r.backtest_total_delta.toFixed(1) : '—'}
                  </td>
                  <td className="py-2 pr-4 text-right text-gray-400 font-mono">{r.live_resolved ?? 0}</td>
                  <td className={`py-2 pr-4 text-xs ${verdict.cls}`}>{verdict.label}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function BacktestRunner() {
  const [jobId, setJobId] = useState(null)
  const [jobStatus, setJobStatus] = useState(null)
  const [polling, setPolling] = useState(false)

  async function handleRun() {
    try {
      const result = await api.researchRun()
      setJobId(result.job_id)
      setJobStatus('queued')
      setPolling(true)
      poll(result.job_id)
    } catch (err) {
      setJobStatus('error: ' + err.message)
    }
  }

  async function poll(id) {
    try {
      const s = await api.researchStatus(id)
      setJobStatus(s.status)
      if (s.status === 'running' || s.status === 'queued') {
        setTimeout(() => poll(id), 3000)
      } else {
        setPolling(false)
      }
    } catch {
      setPolling(false)
    }
  }

  const statusColor =
    jobStatus === 'complete' ? 'text-accent-green' :
    jobStatus === 'failed' ? 'text-accent-red' :
    jobStatus ? 'text-accent-yellow' : ''

  return (
    <div className="flex items-center gap-4">
      <button
        onClick={handleRun}
        disabled={polling}
        className={`btn-primary ${polling ? 'opacity-50 cursor-not-allowed' : ''}`}
      >
        {polling ? 'Running...' : 'Run Backtest'}
      </button>
      {jobStatus && (
        <span className={`text-xs font-mono ${statusColor}`}>
          {jobId && <span className="text-gray-600 mr-2">{jobId.slice(0, 8)}</span>}
          {jobStatus}
        </span>
      )}
    </div>
  )
}

export default function Signals() {
  const [sortKey, setSortKey] = useState('n_trades')
  const [sortDir, setSortDir] = useState('desc')

  const perfFetcher = useCallback(() => api.signalsPerformance(), [])
  const corrFetcher = useCallback(() => api.signalsCorrelation(), [])
  const btFetcher = useCallback(() => api.signalsBacktestResults(), [])

  const { data: perf, loading: perfLoading } = useApi(perfFetcher, 60_000)
  const { data: corr, loading: corrLoading } = useApi(corrFetcher, 120_000)
  const { data: bt, loading: btLoading } = useApi(btFetcher, 300_000)

  function handleSort(key) {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const sorted = perf?.data
    ? [...perf.data].sort((a, b) => {
        const av = a[sortKey] ?? -Infinity
        const bv = b[sortKey] ?? -Infinity
        return sortDir === 'asc' ? av - bv : bv - av
      })
    : null

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs text-gray-600 mb-1">
            Trading Platform <span className="mx-1">›</span>
            <span className="text-gray-400">Signal Research</span>
          </p>
          <h1 className="text-lg font-semibold text-gray-200">Signal Research Lab</h1>
        </div>
        <BacktestRunner />
      </div>

      {/* Backtest Results — EV per signal type from 60d replay */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-4">
          Backtest Results <span className="text-gray-600 font-normal">— signal-engine replay over historical trades</span>
        </h2>
        {btLoading ? <LoadingSkeleton rows={5} /> : <BacktestResultsTable data={bt} />}
      </div>

      {/* Performance Table */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-4">Signal Performance</h2>
        {perfLoading ? <LoadingSkeleton rows={4} /> : (
          perf?.available === false
            ? <EmptyState
                title={perf.reason || 'No backtest results yet'}
                icon="⎄"
                hint="trading-cli research kalshi-full-backtest"
              />
            : <PerformanceTable data={sorted} sortKey={sortKey} sortDir={sortDir} onSort={handleSort} />
        )}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-3">Information Coefficient (IC)</h2>
          {perfLoading ? <LoadingSkeleton rows={3} /> : (
            <SignalBarChart data={perf?.data} metricKey="ic" label="IC" />
          )}
        </div>
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-3">Win Rate</h2>
          {perfLoading ? <LoadingSkeleton rows={3} /> : (
            <SignalBarChart data={perf?.data} metricKey="win_rate" label="Win Rate" neutralVal={0.5} />
          )}
        </div>
      </div>

      {/* Correlation Heatmap */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-4">Signal Correlation</h2>
        {corrLoading ? <LoadingSkeleton rows={5} /> : (
          corr?.available === false
            ? <EmptyState
                title={corr.reason || 'No feature data for correlation'}
                icon="⎄"
                hint="trading-cli kalshi ingest-historical"
              />
            : <CorrelationHeatmap signals={corr?.signals} matrix={corr?.matrix} />
        )}
      </div>
    </div>
  )
}

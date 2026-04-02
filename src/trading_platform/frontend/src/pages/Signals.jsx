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
  { key: 'ic', label: 'IC', sortable: true },
  { key: 'win_rate', label: 'Win %', sortable: true },
  { key: 'mean_edge', label: 'Mean Edge', sortable: true },
  { key: 'sharpe', label: 'Sharpe', sortable: true },
  { key: 'n_trades', label: 'Trades', sortable: true },
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
          {data.map((row, i) => (
            <tr key={i} className="hover:bg-surface-hover transition-colors">
              <td className="py-2 pr-6 text-gray-200 font-medium">
                {String(row[nameKey] ?? '—').replace('KALSHI_', '')}
              </td>
              <td className={`py-2 pr-6 font-mono ${colorFor(row.ic)}`}>
                {row.ic != null ? Number(row.ic).toFixed(4) : '—'}
              </td>
              <td className={`py-2 pr-6 font-mono ${colorFor(Number(row.win_rate) - 0.5)}`}>
                {row.win_rate != null ? `${(Number(row.win_rate) * 100).toFixed(1)}%` : '—'}
              </td>
              <td className={`py-2 pr-6 font-mono ${colorFor(row.mean_edge)}`}>
                {row.mean_edge != null ? Number(row.mean_edge).toFixed(3) : '—'}
              </td>
              <td className={`py-2 pr-6 font-mono ${colorFor(row.sharpe)}`}>
                {row.sharpe != null ? Number(row.sharpe).toFixed(2) : '—'}
              </td>
              <td className="py-2 text-gray-400">
                {row.n_trades ?? row.sample_size ?? '—'}
              </td>
            </tr>
          ))}
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
  const [sortKey, setSortKey] = useState('ic')
  const [sortDir, setSortDir] = useState('desc')

  const perfFetcher = useCallback(() => api.signalsPerformance(), [])
  const corrFetcher = useCallback(() => api.signalsCorrelation(), [])

  const { data: perf, loading: perfLoading } = useApi(perfFetcher, 60_000)
  const { data: corr, loading: corrLoading } = useApi(corrFetcher, 120_000)

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

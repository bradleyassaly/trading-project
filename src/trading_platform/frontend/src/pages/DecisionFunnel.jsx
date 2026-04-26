import { useCallback, useState } from 'react'
import { Link } from 'react-router-dom'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'

export default function DecisionFunnel() {
  const [hours, setHours] = useState(24)
  const fetcher = useCallback(
    () => fetch(`/api/funnel/decisions?hours=${hours}`).then(r => r.ok ? r.json() : null),
    [hours],
  )
  const { data, loading } = useApi(fetcher, 60_000)

  if (loading && !data) return <div className="p-6"><LoadingSkeleton rows={10} /></div>

  const rows = data?.by_gate || []
  const paperRows = rows.filter(r => r.surface === 'paper')
  const liveRows = rows.filter(r => r.surface === 'live')
  const totalRejected = rows.reduce((s, r) => s + (r.rejected || 0), 0)
  const totalPassed = rows.reduce((s, r) => s + (r.passed || 0), 0)

  const renderTable = (title, list) => {
    if (!list.length) return (
      <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
        <h3 className="text-sm font-medium text-gray-300 mb-2">{title}</h3>
        <p className="text-[10px] text-gray-600">No decisions logged in this window.</p>
      </div>
    )
    const maxRej = Math.max(...list.map(r => r.rejected || 0))
    return (
      <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
        <h3 className="text-sm font-medium text-gray-300 mb-2">{title}</h3>
        <table className="w-full text-[11px]">
          <thead className="text-gray-500 text-[10px] uppercase">
            <tr>
              <th className="text-left py-1">Gate</th>
              <th className="text-right py-1">Passed</th>
              <th className="text-right py-1">Rejected</th>
              <th className="text-right py-1">Reject %</th>
              <th className="text-left py-1 pl-3">Volume bar</th>
            </tr>
          </thead>
          <tbody>
            {list.map((r) => {
              const total = (r.passed || 0) + (r.rejected || 0)
              const rejPct = total > 0 ? (r.rejected / total) * 100 : 0
              const barW = maxRej > 0 ? (r.rejected / maxRej) * 100 : 0
              return (
                <tr key={r.gate} className="border-t border-surface-border">
                  <td className="py-1.5 font-mono text-gray-300">{r.gate}</td>
                  <td className="py-1.5 text-right font-mono text-green-400">{r.passed || 0}</td>
                  <td className="py-1.5 text-right font-mono text-red-400">{r.rejected || 0}</td>
                  <td className="py-1.5 text-right font-mono text-gray-400">{rejPct.toFixed(0)}%</td>
                  <td className="py-1.5 pl-3">
                    <div className="bg-red-500/30 h-2 rounded" style={{ width: `${barW}%` }} />
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          <Link to="/dashboard" className="hover:text-gray-400">Command Center</Link>
          <span className="mx-1">&gt;</span>
          <span className="text-gray-400">Decision Funnel</span>
        </p>
        <h1 className="text-base font-semibold text-gray-200">Decision Funnel</h1>
        <p className="text-[10px] text-gray-500 mt-1">
          Per-gate aggregate of every signal evaluation. Reads from{' '}
          <code className="text-gray-400">decision_trace</code> table populated by paper + live executors.
        </p>
      </div>

      {/* Window selector */}
      <div className="flex items-center gap-2 text-[11px]">
        <span className="text-gray-500">Window:</span>
        {[1, 6, 24, 72, 168].map(h => (
          <button
            key={h}
            onClick={() => setHours(h)}
            className={`px-2 py-1 rounded ${
              hours === h
                ? 'bg-accent-blue text-white'
                : 'bg-surface-hover text-gray-400 hover:text-gray-200'
            }`}
          >
            {h < 24 ? `${h}h` : `${h / 24}d`}
          </button>
        ))}
      </div>

      {/* Summary */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500">TOTAL DECISIONS</p>
          <p className="text-xl font-bold font-mono text-gray-200">
            {totalPassed + totalRejected}
          </p>
        </div>
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500">PASSED</p>
          <p className="text-xl font-bold font-mono text-green-400">{totalPassed}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500">REJECTED</p>
          <p className="text-xl font-bold font-mono text-red-400">{totalRejected}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {renderTable('Paper Surface', paperRows)}
        {renderTable('Live Surface', liveRows)}
      </div>

      <div className="text-[10px] text-gray-500">
        Top rejected gate is the binding constraint for that window.
        Relaxing it (or fixing the upstream condition) is the highest-EV next move.
      </div>
    </div>
  )
}

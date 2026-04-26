import { useCallback, useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'

function fmtPct(n, dp = 1) {
  if (n == null) return '—'
  return `${(n * 100).toFixed(dp)}%`
}

export default function SignalHealth() {
  const fetcher = useCallback(
    () => fetch('/api/signal-health').then(r => r.ok ? r.json() : null),
    [],
  )
  const { data, loading } = useApi(fetcher, 60_000)
  const [sortKey, setSortKey] = useState('ic_14d')
  const [sortDir, setSortDir] = useState('desc')

  const rows = data?.signals || []
  const sorted = useMemo(() => {
    const arr = [...rows]
    arr.sort((a, b) => {
      const av = a[sortKey] ?? -Infinity
      const bv = b[sortKey] ?? -Infinity
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      return sortDir === 'desc' ? -cmp : cmp
    })
    return arr
  }, [rows, sortKey, sortDir])

  const toggleSort = (key) => {
    if (sortKey === key) setSortDir(sortDir === 'desc' ? 'asc' : 'desc')
    else { setSortKey(key); setSortDir('desc') }
  }

  if (loading && !data) return <div className="p-6"><LoadingSkeleton rows={10} /></div>

  const decayed = rows.filter(r => r.decay_flag).length
  const correlated = rows.filter(r => (r.correlation_max || 0) >= 0.7).length

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          <Link to="/dashboard" className="hover:text-gray-400">Command Center</Link>
          <span className="mx-1">&gt;</span>
          <span className="text-gray-400">Signal Health</span>
        </p>
        <h1 className="text-base font-semibold text-gray-200">Signal Health</h1>
        <p className="text-[10px] text-gray-500 mt-1">
          IC over rolling 30d / 14d windows, pairwise correlation, decay flags.
          Daily refit via <code className="text-gray-400">signal_health.py</code> task.
        </p>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500">SIGNALS TRACKED</p>
          <p className="text-xl font-bold font-mono text-gray-200">{rows.length}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500">DECAY-FLAGGED</p>
          <p className={`text-xl font-bold font-mono ${decayed ? 'text-amber-400' : 'text-gray-200'}`}>
            {decayed}
          </p>
        </div>
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500">CORRELATION REDUNDANT</p>
          <p className={`text-xl font-bold font-mono ${correlated ? 'text-amber-400' : 'text-gray-200'}`}>
            {correlated}
          </p>
        </div>
      </div>

      {/* Table */}
      <div className="bg-surface-card rounded-lg border border-surface-border overflow-hidden">
        <table className="w-full text-[11px]">
          <thead className="bg-surface-hover text-gray-500 text-[10px] uppercase">
            <tr>
              {[
                ['signal_type', 'Signal'],
                ['n_resolved_30d', 'n (30d)'],
                ['ic_30d', 'IC 30d'],
                ['ic_14d', 'IC 14d'],
                ['ic_trend', 'Trend'],
                ['correlated_with', 'Correlated'],
                ['correlation_max', 'Corr'],
                ['decay_flag', 'Decay'],
              ].map(([k, label]) => (
                <th
                  key={k}
                  className="px-3 py-2 text-left cursor-pointer hover:text-gray-300 select-none"
                  onClick={() => toggleSort(k)}
                >
                  {label}
                  {sortKey === k && (
                    <span className="ml-1">{sortDir === 'desc' ? '↓' : '↑'}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => {
              const ic = r.ic_14d ?? 0
              const icColor = ic > 0.1 ? 'text-green-400' : ic < 0 ? 'text-red-400' : 'text-gray-400'
              return (
                <tr key={r.signal_type} className="border-t border-surface-border hover:bg-surface-hover">
                  <td className="px-3 py-1.5 font-mono">{r.signal_type}</td>
                  <td className="px-3 py-1.5 font-mono text-gray-400">{r.n_resolved_30d}</td>
                  <td className="px-3 py-1.5 font-mono text-gray-300">
                    {r.ic_30d != null ? r.ic_30d.toFixed(3) : '—'}
                  </td>
                  <td className={`px-3 py-1.5 font-mono ${icColor}`}>
                    {r.ic_14d != null ? r.ic_14d.toFixed(3) : '—'}
                  </td>
                  <td className="px-3 py-1.5 text-gray-400 capitalize">{r.ic_trend || '—'}</td>
                  <td className="px-3 py-1.5 font-mono text-gray-500">
                    {r.correlated_with || '—'}
                  </td>
                  <td className={`px-3 py-1.5 font-mono ${(r.correlation_max || 0) >= 0.7 ? 'text-amber-400' : 'text-gray-500'}`}>
                    {r.correlation_max != null ? r.correlation_max.toFixed(2) : '—'}
                  </td>
                  <td className="px-3 py-1.5">
                    {r.decay_flag ? (
                      <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-red-500/20 text-red-400">
                        DECAY
                      </span>
                    ) : (
                      <span className="text-gray-600">—</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="text-[10px] text-gray-500">
        IC = Spearman rank correlation between alpha_score and realized hypothesis_correct.
        Decay = IC&lt;0 on n≥10. Correlation = same-(market, side) co-fire rate within 60s.
      </div>
    </div>
  )
}

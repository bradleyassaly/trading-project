import { useState } from 'react'
import { useApi } from '../hooks/useApi'
import { api } from '../api/client'

// Pretty labels for the 6 racing hypotheses
const LABELS = {
  h1_ob_velocity: 'OB + Velocity',
  h2_pure_velocity: 'Pure Velocity',
  h3_mean_reversion: 'Mean Reversion',
  h4_late_momentum: 'Late Momentum',
  h5_close_position: 'Close Position',
  h6_random_baseline: 'Random (null)',
}

const DESCRIPTIONS = {
  h1_ob_velocity: 'OB imbalance + 30s velocity, both directions align',
  h2_pure_velocity: '30s velocity ≥ 5%, follow direction',
  h3_mean_reversion: 'Bet AGAINST extreme prices (>0.7 or <0.3)',
  h4_late_momentum: '60s velocity ≥ 5% in [15s, 90s] window',
  h5_close_position: 'Bet whichever side leads at close',
  h6_random_baseline: 'Random direction — null hypothesis',
}

function color(value, baseline, lower_is_better = false) {
  if (value == null) return 'text-gray-500'
  if (baseline == null) return 'text-gray-300'
  const beats = lower_is_better ? value < baseline : value > baseline
  return beats ? 'text-accent-green' : 'text-accent-red'
}

export default function HypothesisRaceTile() {
  const [hours, setHours] = useState(168)
  const { data, loading } = useApi(() => api.hypothesisRace(hours), 60_000)

  if (loading && !data) {
    return <div className="text-[10px] text-gray-500">Loading hypothesis race…</div>
  }
  if (!data || data.error) {
    return (
      <div>
        <h2 className="text-sm font-medium text-gray-300 mb-1">Hypothesis Race</h2>
        <p className="text-[10px] text-accent-red">{data?.error || 'unavailable'}</p>
      </div>
    )
  }

  const baseline = data.hypotheses?.find(h => h.hypothesis === 'h6_random_baseline')
  const baseline_wr = baseline?.win_rate
  const baseline_ev = baseline?.avg_ev
  const baseline_brier = baseline?.brier
  const total_resolved = (data.hypotheses || []).reduce((s, h) => s + (h.n_resolved || 0), 0)
  const sorted = [...(data.hypotheses || [])].sort((a, b) => {
    if (a.hypothesis === 'h6_random_baseline') return 1
    if (b.hypothesis === 'h6_random_baseline') return -1
    return (b.avg_ev || 0) - (a.avg_ev || 0)
  })

  return (
    <div>
      <div className="flex items-baseline justify-between mb-2">
        <h2 className="text-sm font-medium text-gray-300">Hypothesis Race — Crypto 5-min</h2>
        <div className="flex items-center gap-2">
          <span className="text-[9px] text-gray-600">window:</span>
          {[24, 72, 168, 720].map(h => (
            <button
              key={h}
              onClick={() => setHours(h)}
              className={`px-1.5 py-0.5 rounded text-[9px] ${
                hours === h
                  ? 'bg-accent-blue/30 text-accent-blue'
                  : 'bg-surface-hover text-gray-500 hover:text-gray-300'
              }`}
            >
              {h < 24 ? `${h}h` : h === 24 ? '1d' : `${h / 24}d`}
            </button>
          ))}
        </div>
      </div>

      {total_resolved === 0 ? (
        <div className="bg-surface-hover rounded p-3">
          <p className="text-[10px] text-gray-400 mb-2">
            ⏳ No resolved trades yet across the 6 hypotheses.
          </p>
          <p className="text-[9px] text-gray-500 leading-relaxed">
            6 strategies are firing in parallel on every active 5-min crypto market
            (BTC + ETH + SOL + DOGE + BNB + XRP + Hyperliquid). After ~24-48h
            of resolution data, the leader emerges. <span className="text-accent-blue">
            h6_random_baseline</span> is the null hypothesis — anything not
            beating it has no edge.
          </p>
        </div>
      ) : (
        <>
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-1">#</th>
                <th className="pb-1">Hypothesis</th>
                <th className="pb-1 text-right">Placed</th>
                <th className="pb-1 text-right">Resolved</th>
                <th className="pb-1 text-right">WR</th>
                <th className="pb-1 text-right">EV</th>
                <th className="pb-1 text-right">Brier</th>
                <th className="pb-1 text-right">Net P&amp;L</th>
                <th className="pb-1">vs baseline</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {sorted.map((h, i) => {
                const is_baseline = h.hypothesis === 'h6_random_baseline'
                const beats_wr = h.win_rate != null && baseline_wr != null && h.win_rate > baseline_wr
                const beats_ev = h.avg_ev != null && baseline_ev != null && h.avg_ev > baseline_ev
                const beats_brier = h.brier != null && baseline_brier != null && h.brier < baseline_brier
                const verdict = is_baseline
                  ? '— baseline —'
                  : (beats_wr && beats_ev && beats_brier)
                    ? '✓ all 3'
                    : (beats_wr || beats_ev || beats_brier)
                      ? `partial (${[beats_wr && 'WR', beats_ev && 'EV', beats_brier && 'Brier'].filter(Boolean).join(', ')})`
                      : '✗ no edge'
                const verdictColor = is_baseline
                  ? 'text-gray-500'
                  : verdict === '✓ all 3'
                    ? 'text-accent-green'
                    : verdict === '✗ no edge'
                      ? 'text-accent-red'
                      : 'text-accent-yellow'
                return (
                  <tr
                    key={h.hypothesis}
                    className={`hover:bg-surface-hover ${
                      is_baseline ? 'bg-surface-hover/40' : ''
                    }`}
                    title={DESCRIPTIONS[h.hypothesis] || ''}
                  >
                    <td className="py-1 text-gray-500">{is_baseline ? '—' : i + 1}</td>
                    <td className={`py-1 ${is_baseline ? 'text-gray-500 italic' : 'text-gray-300'}`}>
                      {LABELS[h.hypothesis] || h.hypothesis}
                    </td>
                    <td className="py-1 text-right font-mono text-gray-400">{h.n_placed}</td>
                    <td className="py-1 text-right font-mono text-gray-400">{h.n_resolved}</td>
                    <td className={`py-1 text-right font-mono ${color(h.win_rate, baseline_wr)}`}>
                      {h.win_rate != null ? `${(h.win_rate * 100).toFixed(0)}%` : '—'}
                    </td>
                    <td className={`py-1 text-right font-mono ${color(h.avg_ev, baseline_ev)}`}>
                      {h.avg_ev != null ? `${(h.avg_ev * 100).toFixed(1)}%` : '—'}
                    </td>
                    <td className={`py-1 text-right font-mono ${color(h.brier, baseline_brier, true)}`}>
                      {h.brier != null ? h.brier.toFixed(3) : '—'}
                    </td>
                    <td className={`py-1 text-right font-mono ${
                      (h.net_pnl || 0) > 0 ? 'text-accent-green' : (h.net_pnl || 0) < 0 ? 'text-accent-red' : 'text-gray-500'
                    }`}>
                      {h.net_pnl != null ? `${h.net_pnl >= 0 ? '+' : ''}$${h.net_pnl.toFixed(2)}` : '—'}
                    </td>
                    <td className={`py-1 text-[9px] ${verdictColor}`}>{verdict}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <p className="text-[9px] text-gray-600 mt-2 leading-relaxed">
            Decision rule: <span className="text-gray-400">WR &gt; baseline</span> +
            {' '}<span className="text-gray-400">Brier &lt; 0.25</span> +
            {' '}<span className="text-gray-400">n_resolved ≥ 20</span> = candidate alpha.
            Hover hypothesis name for description. Updates every 60s.
          </p>
        </>
      )}
    </div>
  )
}

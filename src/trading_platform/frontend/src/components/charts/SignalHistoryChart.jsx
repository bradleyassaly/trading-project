import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { chartTheme } from '../../lib/format'

/**
 * Signal cumulative P&L history — one line per signal type over time.
 *
 * Each signal in `byType` has an `equity_curve` array of
 * { date, pnl } or similar points. We merge all curves into a single
 * time-axis chart with one coloured line per signal.
 */

const SIGNAL_COLORS = {
  convergence:        '#10b981',
  specialist_entry:   '#84cc16',
  whale_entry:        '#06b6d4',
  whale_exit:         '#f43f5e',
  cascade:            '#f59e0b',
  oversized_bet:      '#f97316',
  accumulation:       '#a855f7',
  wallet_reversal:    '#ec4899',
  pre_deadline_surge: '#fbbf24',
  market_maker_flip:  '#8b5cf6',
  price_velocity:     '#ef4444',
  no_position_entry:  '#f43f5e',
  position_reduction: '#6366f1',
}

export default function SignalHistoryChart({ byType, height = 240 }) {
  // Collect all signals that have an equity curve
  const signalsWithCurve = (byType || []).filter(
    s => s.equity_curve && s.equity_curve.length >= 2
  )

  if (!signalsWithCurve.length) {
    return (
      <div
        className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded"
        style={{ height }}
      >
        No signal history yet — need resolved trades to build curves
      </div>
    )
  }

  // Build a merged dataset: for each time index, each signal has a pnl column
  // Since curves may have different lengths, normalise by index
  const maxLen = Math.max(...signalsWithCurve.map(s => s.equity_curve.length))

  const data = []
  for (let i = 0; i < maxLen; i++) {
    const point = { idx: i }
    for (const s of signalsWithCurve) {
      const curve = s.equity_curve
      if (i < curve.length) {
        point[s.signal_type] = curve[i].pnl ?? curve[i].cumulative ?? curve[i].value ?? 0
      }
    }
    data.push(point)
  }

  return (
    <div style={{ height, width: '100%' }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 4 }}>
          <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
          <XAxis
            dataKey="idx"
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            label={{ value: 'Trade #', position: 'insideBottom', offset: -2, style: { ...chartTheme.axisFont, fontSize: 9 } }}
          />
          <YAxis
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={v => v >= 1000 ? `$${(v / 1000).toFixed(0)}K` : `$${v.toFixed(0)}`}
            width={55}
          />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={(v, name) => [`$${Number(v).toFixed(0)}`, name.replace(/_/g, ' ')]}
          />
          <Legend
            wrapperStyle={{ fontSize: 9, fontFamily: 'JetBrains Mono, monospace' }}
            formatter={v => v.replace(/_/g, ' ')}
          />
          {signalsWithCurve.map(s => (
            <Line
              key={s.signal_type}
              type="monotone"
              dataKey={s.signal_type}
              stroke={SIGNAL_COLORS[s.signal_type] || '#6b7280'}
              strokeWidth={1.2}
              dot={false}
              connectNulls
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

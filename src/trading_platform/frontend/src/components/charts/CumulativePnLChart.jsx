import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer,
} from 'recharts'
import { chartTheme, fmtUsd } from '../../lib/format'

/**
 * Cumulative paper-trading equity over time. Pairs with the thesis
 * scorecard — green when above starting bankroll, red when below.
 *
 * Accepts an array of rows from /api/pnl/equity-curve. The CSV column
 * names vary by writer (`equity` / `portfolio_value` / `value`,
 * `timestamp` / `date` / `day`), so we sniff a couple of likely keys.
 */
function pickValue(row, keys) {
  for (const k of keys) {
    const v = row?.[k]
    if (v !== undefined && v !== null && v !== '') return v
  }
  return null
}

export default function CumulativePnLChart({ rows, starting = 100000, height = 220 }) {
  const data = (rows || [])
    .map((r) => {
      const ts = pickValue(r, ['timestamp', 'date', 'day', 'time'])
      const eq = Number(pickValue(r, ['equity', 'portfolio_value', 'value']))
      if (!ts || Number.isNaN(eq)) return null
      const label = typeof ts === 'string'
        ? ts.slice(5, 10)
        : new Date(ts * 1000).toISOString().slice(5, 10)
      return { day: label, equity: eq, pnl: eq - starting }
    })
    .filter(Boolean)

  if (data.length < 2) {
    return (
      <div
        className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded"
        style={{ height }}
      >
        No paper equity history yet — fire some trades to populate this chart.
      </div>
    )
  }

  const last = data[data.length - 1].equity
  const positive = last >= starting
  const stroke = positive ? chartTheme.green : chartTheme.red
  const fillId = positive ? 'pnlGreenFill' : 'pnlRedFill'

  return (
    <div style={{ height, width: '100%' }}>
      <ResponsiveContainer>
        <AreaChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 4 }}>
          <defs>
            <linearGradient id="pnlGreenFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={chartTheme.green} stopOpacity={0.35} />
              <stop offset="100%" stopColor={chartTheme.green} stopOpacity={0} />
            </linearGradient>
            <linearGradient id="pnlRedFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={chartTheme.red} stopOpacity={0.35} />
              <stop offset="100%" stopColor={chartTheme.red} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
          <XAxis dataKey="day" tick={chartTheme.axisFont} stroke={chartTheme.axis} />
          <YAxis
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={(v) => fmtUsd(v)}
            domain={['auto', 'auto']}
            width={70}
          />
          <ReferenceLine y={starting} stroke={chartTheme.axis} strokeDasharray="2 2" />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={(v, name) => [
              name === 'equity' ? fmtUsd(v) : fmtUsd(v, { signed: true }),
              name === 'equity' ? 'equity' : 'pnl',
            ]}
          />
          <Area
            type="monotone"
            dataKey="equity"
            stroke={stroke}
            strokeWidth={1.5}
            fill={`url(#${fillId})`}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

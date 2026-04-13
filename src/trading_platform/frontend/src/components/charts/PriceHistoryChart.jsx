import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { chartTheme } from '../../lib/format'

/**
 * Market price history line chart.
 *
 * Accepts either:
 * - Array of { t, p } from CLOB /prices-history (epoch seconds + price 0..1)
 * - Array of { timestamp, open, high, low, close } from our /market/{cid}/candles
 *
 * Normalises both into { time, price } for rendering.
 */
function normalise(raw) {
  if (!raw || !raw.length) return []
  return raw.map(entry => {
    // CLOB format: {t, p}
    if (entry.t !== undefined && entry.p !== undefined) {
      const ts = Number(entry.t)
      const d = new Date(ts < 1e12 ? ts * 1000 : ts)
      return {
        time: d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
        price: Number(entry.p),
      }
    }
    // Our candle format
    const ts = entry.timestamp || entry.time || entry.date
    const price = entry.close ?? entry.price ?? entry.p ?? 0
    const d = typeof ts === 'string' ? new Date(ts) : new Date((ts < 1e12 ? ts * 1000 : ts))
    return {
      time: d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
      price: Number(price),
    }
  }).filter(p => !Number.isNaN(p.price))
}

export default function PriceHistoryChart({ data, height = 200 }) {
  const points = normalise(data)

  if (points.length < 2) {
    return (
      <div
        className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded"
        style={{ height }}
      >
        Not enough price history
      </div>
    )
  }

  const first = points[0].price
  const last = points[points.length - 1].price
  const up = last >= first
  const stroke = up ? chartTheme.green : chartTheme.red

  return (
    <div style={{ height, width: '100%' }}>
      <ResponsiveContainer>
        <LineChart data={points} margin={{ top: 8, right: 12, left: 0, bottom: 4 }}>
          <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
          <XAxis
            dataKey="time"
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
          />
          <YAxis
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={v => `${(v * 100).toFixed(0)}%`}
            domain={['auto', 'auto']}
            width={45}
          />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={v => [`${(v * 100).toFixed(1)}%`, 'Price']}
          />
          <Line
            type="monotone"
            dataKey="price"
            stroke={stroke}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

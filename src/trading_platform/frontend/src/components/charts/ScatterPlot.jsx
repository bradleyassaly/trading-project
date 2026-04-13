import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip,
  ReferenceLine, ResponsiveContainer, ZAxis,
} from 'recharts'
import { chartTheme } from '../../lib/format'

/**
 * Expected vs Actual outcome scatter plot for resolved paper trades.
 *
 * X-axis: entry confidence (0..1) — what the signal predicted
 * Y-axis: actual outcome (1 = win, 0 = loss, or pnl-based)
 *
 * Dots above the diagonal = better-than-expected outcomes.
 * Dots below = worse-than-expected.
 *
 * Accepts an array of trade objects with:
 * - confidence (or entry_price): 0..1
 * - outcome: 1 (win) or 0 (loss)
 * - pnl: optional, used to size dots
 * - signal_type: optional, for tooltip
 */
export default function ScatterPlot({ trades, height = 260 }) {
  if (!trades || trades.length < 3) {
    return (
      <div
        className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded"
        style={{ height }}
      >
        Need 3+ resolved trades for the scatter plot
      </div>
    )
  }

  const data = trades
    .filter(t => t.confidence != null || t.entry_price != null)
    .map(t => {
      const conf = t.confidence ?? t.entry_price ?? 0
      const actual = t.outcome != null ? Number(t.outcome) : (t.won === true ? 1 : t.won === false ? 0 : null)
      if (actual == null) return null
      return {
        confidence: conf,
        actual,
        pnl: Math.abs(t.pnl || t.return_pct || 0.01),
        signal: t.signal_type || '—',
        question: (t.question || '').slice(0, 50),
        isWin: actual > 0.5,
      }
    })
    .filter(Boolean)

  if (data.length < 3) {
    return (
      <div
        className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded"
        style={{ height }}
      >
        Not enough resolved trades with confidence data
      </div>
    )
  }

  const wins = data.filter(d => d.isWin)
  const losses = data.filter(d => !d.isWin)

  return (
    <div style={{ height, width: '100%' }}>
      <ResponsiveContainer>
        <ScatterChart margin={{ top: 12, right: 12, left: 0, bottom: 4 }}>
          <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
          <XAxis
            dataKey="confidence"
            name="Confidence"
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={v => `${(v * 100).toFixed(0)}%`}
            domain={[0, 1]}
            type="number"
            label={{ value: 'Entry Confidence', position: 'insideBottom', offset: -2, style: { ...chartTheme.axisFont, fontSize: 9 } }}
          />
          <YAxis
            dataKey="actual"
            name="Outcome"
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={v => v >= 0.5 ? 'WIN' : 'LOSS'}
            domain={[-0.1, 1.1]}
            width={40}
            type="number"
          />
          <ZAxis dataKey="pnl" range={[20, 80]} />
          {/* Diagonal reference: if outcome matched confidence perfectly */}
          <ReferenceLine
            segment={[{ x: 0, y: 0 }, { x: 1, y: 1 }]}
            stroke={chartTheme.axis}
            strokeDasharray="4 4"
          />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={(v, name) => {
              if (name === 'Confidence') return [`${(v * 100).toFixed(0)}%`, 'Confidence']
              if (name === 'Outcome') return [v >= 0.5 ? 'WIN' : 'LOSS', 'Outcome']
              return [v, name]
            }}
            content={({ payload }) => {
              if (!payload?.length) return null
              const d = payload[0]?.payload
              if (!d) return null
              return (
                <div style={chartTheme.tooltip.contentStyle} className="p-2 text-[10px]">
                  <p className="text-gray-300">{d.signal}</p>
                  <p className="text-gray-500">{d.question}</p>
                  <p className="text-gray-400">Conf: {(d.confidence * 100).toFixed(0)}% → {d.isWin ? 'WIN' : 'LOSS'}</p>
                </div>
              )
            }}
          />
          <Scatter name="Wins" data={wins} fill={chartTheme.green} opacity={0.7} />
          <Scatter name="Losses" data={losses} fill={chartTheme.red} opacity={0.7} />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  )
}

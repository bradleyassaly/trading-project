import { LineChart, Line, YAxis, ReferenceLine, ResponsiveContainer, Tooltip } from 'recharts'
import { chartTheme } from '../../lib/format'

/**
 * Tiny inline sparkline of hypothesis accuracy over time. Sits next to
 * the big number in the Command Center hero card so the operator can
 * see the *trend* not just the snapshot.
 *
 * Expects an array of { day, accuracy } where `accuracy` is 0..1.
 * The dashed line at 0.70 is the GO_LIVE target.
 */
export default function AccuracySparkline({ snapshots, target = 0.70, height = 48 }) {
  const data = (snapshots || [])
    .filter(s => s.accuracy != null)
    .map(s => ({ day: s.day, accuracy: s.accuracy }))

  if (data.length < 2) {
    return (
      <div
        className="text-[9px] text-gray-600 flex items-center justify-center"
        style={{ height }}
      >
        building history…
      </div>
    )
  }

  const last = data[data.length - 1].accuracy
  const stroke = last >= target ? chartTheme.green : chartTheme.blue

  return (
    <div style={{ height, width: '100%' }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 4, right: 4, left: 4, bottom: 4 }}>
          <YAxis hide domain={[0, 1]} />
          <ReferenceLine y={target} stroke={chartTheme.axis} strokeDasharray="2 2" />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={(v) => [`${(v * 100).toFixed(1)}%`, 'accuracy']}
            labelFormatter={(_, p) => p?.[0]?.payload?.day || ''}
          />
          <Line
            type="monotone"
            dataKey="accuracy"
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

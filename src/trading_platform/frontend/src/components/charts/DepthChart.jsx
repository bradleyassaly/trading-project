import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { chartTheme } from '../../lib/format'

/**
 * Order-book depth chart. Renders cumulative bid/ask depth as mirrored
 * area fills — green on the bid side, red on the ask side.
 *
 * Expects either:
 * - { bids: [[price, size], ...], asks: [[price, size], ...] } from CLOB
 * - { bids: [{price, size}, ...], asks: [{price, size}, ...] } from our API
 *
 * Both are normalised internally.
 */
function normalise(raw) {
  if (!raw || !raw.length) return []
  return raw.map(entry => {
    if (Array.isArray(entry)) return { price: Number(entry[0]), size: Number(entry[1]) }
    return { price: Number(entry.price), size: Number(entry.size || entry.quantity || 0) }
  })
}

function buildCumulative(levels, ascending) {
  const sorted = [...levels].sort((a, b) => ascending ? a.price - b.price : b.price - a.price)
  let cum = 0
  return sorted.map(l => {
    cum += l.size
    return { price: l.price, cumulative: cum }
  })
}

export default function DepthChart({ book, height = 200 }) {
  if (!book) {
    return (
      <div className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded" style={{ height }}>
        No order book data
      </div>
    )
  }

  const bids = normalise(book.bids)
  const asks = normalise(book.asks)

  if (!bids.length && !asks.length) {
    return (
      <div className="text-[10px] text-gray-600 flex items-center justify-center border border-dashed border-surface-border rounded" style={{ height }}>
        Empty order book
      </div>
    )
  }

  // Bids: cumulative descending (highest price first, accumulate downward)
  const bidCum = buildCumulative(bids, false).reverse()
  // Asks: cumulative ascending (lowest price first, accumulate upward)
  const askCum = buildCumulative(asks, true)

  // Merge into one series — bids get bidDepth, asks get askDepth
  const data = [
    ...bidCum.map(b => ({ price: b.price, bidDepth: b.cumulative, askDepth: null })),
    ...askCum.map(a => ({ price: a.price, bidDepth: null, askDepth: a.cumulative })),
  ].sort((a, b) => a.price - b.price)

  return (
    <div style={{ height, width: '100%' }}>
      <ResponsiveContainer>
        <AreaChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 4 }}>
          <defs>
            <linearGradient id="depthBidFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={chartTheme.green} stopOpacity={0.4} />
              <stop offset="100%" stopColor={chartTheme.green} stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="depthAskFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={chartTheme.red} stopOpacity={0.4} />
              <stop offset="100%" stopColor={chartTheme.red} stopOpacity={0.05} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
          <XAxis
            dataKey="price"
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={v => (v * 100).toFixed(0) + '%'}
            domain={['auto', 'auto']}
            type="number"
          />
          <YAxis
            tick={chartTheme.axisFont}
            stroke={chartTheme.axis}
            tickFormatter={v => v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v.toFixed(0)}
            width={50}
          />
          <Tooltip
            {...chartTheme.tooltip}
            formatter={(v, name) => [
              v != null ? `$${v.toLocaleString()}` : '—',
              name === 'bidDepth' ? 'Bid Depth' : 'Ask Depth',
            ]}
            labelFormatter={v => `Price: ${(v * 100).toFixed(1)}%`}
          />
          <Area
            type="stepAfter"
            dataKey="bidDepth"
            stroke={chartTheme.green}
            strokeWidth={1.5}
            fill="url(#depthBidFill)"
            connectNulls={false}
            isAnimationActive={false}
          />
          <Area
            type="stepAfter"
            dataKey="askDepth"
            stroke={chartTheme.red}
            strokeWidth={1.5}
            fill="url(#depthAskFill)"
            connectNulls={false}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

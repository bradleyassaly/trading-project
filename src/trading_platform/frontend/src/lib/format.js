/**
 * Shared formatters and chart-theme constants for the dashboard.
 *
 * Numbers should always render in monospace via tailwind's `font-mono`
 * class — these helpers just normalise the value, not the typography.
 */

export function fmtUsd(value, opts = {}) {
  if (value == null || Number.isNaN(value)) return '—'
  const { signed = false, digits = 0 } = opts
  const sign = signed && value > 0 ? '+' : ''
  const absStr = Math.abs(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
  return `${value < 0 ? '-' : sign}$${absStr}`
}

export function fmtPct(value, digits = 0) {
  if (value == null || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

export function fmtNum(value, digits = 0) {
  if (value == null || Number.isNaN(value)) return '—'
  return value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

export function fmtRelTime(ts) {
  if (!ts) return '—'
  const secs = typeof ts === 'string' ? new Date(ts).getTime() / 1000 : ts
  const d = Date.now() / 1000 - secs
  if (d < 60) return '<1m'
  if (d < 3600) return `${Math.floor(d / 60)}m`
  if (d < 86400) return `${Math.floor(d / 3600)}h`
  return `${Math.floor(d / 86400)}d`
}

export function fmtDateShort(ts) {
  if (!ts) return '—'
  const d = new Date(typeof ts === 'string' ? ts : ts * 1000)
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
}

/**
 * Recharts dark theme — drop these into <CartesianGrid/>, <XAxis/>, etc.
 * to keep the look consistent across every chart on the dashboard.
 */
export const chartTheme = {
  bg: '#0f1117',
  grid: '#222537',
  axis: '#5b6075',
  axisFont: { fontSize: 10, fontFamily: 'JetBrains Mono, monospace', fill: '#5b6075' },
  tooltip: {
    contentStyle: {
      background: '#1a1d27',
      border: '1px solid #2a2d3e',
      borderRadius: 4,
      fontSize: 11,
      fontFamily: 'JetBrains Mono, monospace',
    },
    labelStyle: { color: '#9ca3af' },
    itemStyle: { color: '#e5e7eb' },
  },
  green: '#00d084',
  red: '#ff4757',
  yellow: '#ffd32a',
  blue: '#3d7fff',
}

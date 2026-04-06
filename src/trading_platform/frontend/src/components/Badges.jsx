/**
 * Shared badge components for wallet intelligence UI.
 */

const BUCKET_STYLES = {
  concentrated_whale:    { bg: 'bg-blue-900/60',   text: 'text-blue-300',   label: 'Whale' },
  contrarian_whale:      { bg: 'bg-purple-900/60',  text: 'text-purple-300', label: 'Contrarian' },
  portfolio_diversifier: { bg: 'bg-teal-900/60',    text: 'text-teal-300',   label: 'Diversifier' },
  position_builder:      { bg: 'bg-orange-900/60',  text: 'text-orange-300', label: 'Builder' },
  domain_specialist:     { bg: 'bg-green-900/60',   text: 'text-green-300',  label: 'Specialist' },
  volume_trader:         { bg: 'bg-gray-800',        text: 'text-gray-400',   label: 'Volume' },
  directional:           { bg: 'bg-green-900/60',   text: 'text-green-300',  label: 'Dir' },
  market_maker:          { bg: 'bg-yellow-900/60',  text: 'text-yellow-300', label: 'MM' },
  arb_bot:               { bg: 'bg-gray-800',        text: 'text-gray-500',   label: 'Bot' },
  noise:                 { bg: 'bg-gray-800',        text: 'text-gray-600',   label: 'Noise' },
  unknown:               { bg: 'bg-gray-800',        text: 'text-gray-400',   label: '?' },
}

const SIGNAL_STYLES = {
  whale_entry:       { bg: 'bg-blue-900/60',   text: 'text-blue-300',   label: 'Whale' },
  convergence:       { bg: 'bg-purple-900/60',  text: 'text-purple-300', label: 'Converge' },
  contrarian:        { bg: 'bg-orange-900/60',  text: 'text-orange-300', label: 'Contrarian' },
  cross_type:        { bg: 'bg-teal-900/60',    text: 'text-teal-300',   label: 'CrossType' },
  position_building: { bg: 'bg-cyan-900/60',    text: 'text-cyan-300',   label: 'Building' },
  cascade:           { bg: 'bg-gray-700',        text: 'text-gray-300',   label: 'Cascade' },
  volume_trader:     { bg: 'bg-gray-800',        text: 'text-gray-400',   label: 'Volume' },
}

export function BucketBadge({ bucket }) {
  const s = BUCKET_STYLES[bucket] || BUCKET_STYLES.unknown
  return <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${s.bg} ${s.text}`}>{s.label}</span>
}

export function SignalBadge({ type }) {
  const s = SIGNAL_STYLES[type] || { bg: 'bg-gray-800', text: 'text-gray-400', label: type || '?' }
  return <span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${s.bg} ${s.text}`}>{s.label}</span>
}

export function TierBadge({ tier }) {
  if (tier === 1) return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-accent-red/20 text-accent-red">T1</span>
  if (tier === 2) return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-yellow-900/60 text-yellow-300">T2</span>
  return <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-gray-800 text-gray-500">T{tier}</span>
}

export function EVBadge({ ev }) {
  if (ev == null) return <span className="text-gray-600 text-[10px]">--</span>
  const cls = ev > 0.3 ? 'text-accent-green' : ev > 0 ? 'text-green-400' : 'text-accent-red'
  return <span className={`font-mono text-[10px] ${cls}`}>{ev >= 0 ? '+' : ''}{ev.toFixed(2)} EV</span>
}

export function GateBadge({ passed, label }) {
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className={passed ? 'text-accent-green' : 'text-accent-red'}>{passed ? '[OK]' : '[X]'}</span>
      <span className={passed ? 'text-gray-300' : 'text-gray-500'}>{label}</span>
    </div>
  )
}

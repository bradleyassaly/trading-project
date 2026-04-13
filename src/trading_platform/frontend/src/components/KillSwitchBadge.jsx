import { useApi } from '../hooks/useApi'
import { api } from '../api/client'

export default function KillSwitchBadge() {
  const { data } = useApi(api.killSwitch, 15_000)

  if (!data) return null

  const tripped = data.tripped
  const reason = data.reason
  const config = data.config || {}

  if (tripped) {
    return (
      <div className="flex items-center gap-2 px-2.5 py-1 rounded bg-accent-red/20 text-accent-red text-[10px] font-semibold animate-pulse">
        <span className="inline-block w-2 h-2 rounded-full bg-accent-red" />
        KILL SWITCH TRIPPED
        {reason && <span className="text-gray-400 font-normal ml-1">({reason})</span>}
      </div>
    )
  }

  return (
    <div className="flex items-center gap-2 px-2.5 py-1 rounded bg-accent-green/10 text-accent-green text-[10px]">
      <span className="inline-block w-2 h-2 rounded-full bg-accent-green" />
      Kill switch active
      <span className="text-gray-500">
        ${config.MAX_TRADE_USD || 25}/trade
      </span>
    </div>
  )
}

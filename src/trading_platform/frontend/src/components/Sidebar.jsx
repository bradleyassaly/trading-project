import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { LayoutGrid, BarChart2, Globe, Lightbulb, Settings, ChevronLeft, ChevronRight } from 'lucide-react'
import { useApi } from '../hooks/useApi'
import { api } from '../api/client'

const NAV = [
  { to: '/dashboard', label: 'Command Center',  Icon: LayoutGrid },
  { to: '/signals',   label: 'Signal Research', Icon: BarChart2  },
  { to: '/markets',   label: 'Kalshi Markets',  Icon: Globe      },
  { to: '/reasoning', label: 'Trade Reasoning', Icon: Lightbulb  },
  { to: '/control',   label: 'Loop Control',    Icon: Settings   },
]

const STATE_LABELS = {
  running:        'Running',
  stopped:        'Paused',
  trigger_pending:'Awaiting trigger',
  unknown:        'Unknown',
}

function StatusDot({ state }) {
  const base = 'inline-block w-2 h-2 rounded-full flex-shrink-0'
  if (state === 'running')         return <span className={`${base} bg-accent-green animate-pulse`} />
  if (state === 'stopped')         return <span className={`${base} bg-accent-red`} />
  if (state === 'trigger_pending') return <span className={`${base} bg-accent-yellow animate-pulse`} />
  return <span className={`${base} bg-gray-500`} />
}

export default function Sidebar() {
  const [collapsed, setCollapsed] = useState(false)
  const { data: status } = useApi(api.systemStatus, 15_000)

  const loopState = status?.loop_state ?? 'unknown'
  const stateLabel = STATE_LABELS[loopState] ?? loopState

  return (
    <aside
      className={`flex flex-col bg-surface-card border-r border-surface-border transition-all duration-200
        ${collapsed ? 'w-14' : 'w-56'} min-h-screen flex-shrink-0`}
    >
      {/* Logo */}
      <div className="flex items-center gap-2 px-3 py-4 border-b border-surface-border">
        <span className="text-accent-blue text-xl font-bold select-none">⬡</span>
        {!collapsed && (
          <span className="text-sm font-semibold tracking-wide text-gray-200 truncate">
            Trading Platform
          </span>
        )}
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="ml-auto text-gray-500 hover:text-gray-300"
          aria-label="Toggle sidebar"
        >
          {collapsed
            ? <ChevronRight size={14} />
            : <ChevronLeft  size={14} />}
        </button>
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-4 space-y-0.5 px-1.5">
        {NAV.map(({ to, label, Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-2.5 px-2.5 py-2 rounded-md text-sm transition-colors duration-100
               ${isActive
                ? 'bg-accent-blue/20 text-accent-blue'
                : 'text-gray-400 hover:bg-surface-hover hover:text-gray-200'
              }`
            }
          >
            <Icon size={15} className="flex-shrink-0" />
            {!collapsed && <span className="truncate">{label}</span>}
          </NavLink>
        ))}
      </nav>

      {/* Status strip */}
      <div className="border-t border-surface-border p-3 space-y-1.5">
        <div className="flex items-center gap-2">
          <StatusDot state={loopState} />
          {!collapsed && (
            <span className="text-xs text-gray-400 truncate">
              Loop:{' '}
              <span className={
                loopState === 'running'         ? 'text-accent-green' :
                loopState === 'stopped'         ? 'text-accent-red'   :
                loopState === 'trigger_pending' ? 'text-accent-yellow': 'text-gray-400'
              }>
                {stateLabel}
              </span>
            </span>
          )}
        </div>
        {!collapsed && status?.active_strategy_count != null && (
          <p className="text-xs text-gray-500">
            {status.active_strategy_count} active{' '}
            {status.active_strategy_count === 1 ? 'strategy' : 'strategies'}
          </p>
        )}
      </div>
    </aside>
  )
}

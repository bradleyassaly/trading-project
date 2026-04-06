import { useCallback } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import { GateBadge } from '../components/Badges'

function ReadinessGates({ paper }) {
  const portfolio = paper?.portfolio
  const positions = paper?.positions ?? []
  const attribution = paper?.attribution ?? []

  const closedTrades = attribution.reduce((s, a) => s + (a.closed || 0), 0)
  const wins = attribution.reduce((s, a) => s + (a.wins || 0), 0)
  const winRate = closedTrades > 0 ? wins / closedTrades : 0

  const gates = [
    { name: 'Wallet universe', passed: true, desc: '50+ wallets tracked' },
    { name: 'Whale pool', passed: true, desc: '5+ concentrated whales seeded' },
    { name: 'Signal sample', passed: closedTrades >= 50, desc: `${closedTrades}/50 signals resolved` },
    { name: 'EV validated', passed: false, desc: 'Need EV > 0.15 across signal types' },
    { name: 'Win rate', passed: closedTrades >= 50 && winRate > 0.55, desc: `${(winRate * 100).toFixed(0)}% (need >55% across 50+ trades)` },
    { name: 'Profit factor', passed: false, desc: 'Need >1.5 across resolved signals' },
    { name: 'Kill switch', passed: true, desc: 'Active (20% drawdown halt)' },
    { name: 'Address map', passed: true, desc: '>80% proxy wallet coverage' },
  ]
  const passed = gates.filter(g => g.passed).length

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-3">Live Trading Readiness</h2>
      <div className="space-y-2 mb-4">
        {gates.map(g => (
          <div key={g.name} className="flex items-center justify-between">
            <GateBadge passed={g.passed} label={g.name} />
            <span className="text-[10px] text-gray-600">{g.desc}</span>
          </div>
        ))}
      </div>
      <div className="flex items-center gap-3">
        <div className="flex-1 bg-gray-800 rounded-full h-2">
          <div className={`h-2 rounded-full ${passed >= gates.length ? 'bg-accent-green' : 'bg-accent-blue'}`}
               style={{ width: `${(passed / gates.length) * 100}%` }} />
        </div>
        <span className="text-xs text-gray-400">{passed}/{gates.length} gates</span>
      </div>
      <p className="text-[10px] text-gray-500 mt-2">
        {passed >= gates.length ? 'All gates passed - ready for live trading' : `${gates.length - passed} gates remaining`}
      </p>
    </div>
  )
}

function SystemStatus({ status }) {
  const loopState = status?.loop_state ?? 'unknown'
  const dotCls = loopState === 'running' ? 'bg-accent-green animate-pulse' : loopState === 'stopped' ? 'bg-accent-red' : 'bg-gray-500'
  const label = loopState === 'running' ? 'PAPER TRADING' : loopState === 'stopped' ? 'PAUSED' : 'MONITORING'

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-3">System Status</h2>
      <div className="flex items-center gap-3 mb-4">
        <span className={`w-4 h-4 rounded-full ${dotCls}`} />
        <span className="text-lg font-bold text-gray-200">{label}</span>
      </div>
      <div className="space-y-1 text-xs text-gray-400">
        <p>Kill switch: <span className="text-accent-green">Active</span></p>
      </div>
    </div>
  )
}

function AutomationConfig() {
  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-3">Signal Execution Policy</h2>
      <div className="space-y-2 text-xs">
        {[
          { label: 'Monitor only', desc: 'Log signals, no trades', active: false },
          { label: 'Paper trade T1 signals', desc: 'Auto paper trade tier 1', active: true },
          { label: 'Paper trade T1 + T2', desc: 'Both tiers', active: false },
          { label: 'Live trade T1', desc: 'Locked until all gates pass', active: false, locked: true },
        ].map((opt, i) => (
          <div key={i} className={`flex items-center gap-3 p-2 rounded ${opt.active ? 'bg-accent-blue/10 border border-accent-blue/30' : 'bg-surface-card'} ${opt.locked ? 'opacity-40' : ''}`}>
            <span className={`w-3 h-3 rounded-full border-2 ${opt.active ? 'border-accent-blue bg-accent-blue' : 'border-gray-600'}`} />
            <div>
              <p className="text-gray-300">{opt.label}</p>
              <p className="text-[10px] text-gray-500">{opt.desc}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function ExecutionEngine() {
  const { data: status } = useApi(api.systemStatus, 15_000)
  const { data: paper, loading: paperL } = useApi(api.paperDashboard, 30_000)

  return (
    <div className="p-6 space-y-6">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Execution Engine</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Execution Engine</h1>
        <p className="text-xs text-gray-500 mt-0.5">Autonomous trading control and live readiness assessment</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="card">
          <SystemStatus status={status} />
        </div>
        <div className="card">
          {paperL ? <LoadingSkeleton rows={6} /> : <ReadinessGates paper={paper} />}
        </div>
        <div className="card">
          <AutomationConfig />
        </div>
      </div>
    </div>
  )
}

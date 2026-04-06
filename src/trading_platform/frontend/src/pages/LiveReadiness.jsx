import { useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'

function GateCard({ number, title, current, required, passed, children }) {
  const pct = typeof current === 'number' && typeof required === 'number' && required > 0
    ? Math.min(100, Math.round((current / required) * 100))
    : passed ? 100 : 0

  return (
    <div className={`bg-surface-card rounded-lg p-4 border-l-4 ${passed ? 'border-accent-green' : 'border-gray-700'}`}>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span className={`w-3 h-3 rounded-full ${passed ? 'bg-accent-green' : 'bg-gray-600'}`} />
          <span className="text-sm text-gray-300">Gate {number}: {title}</span>
        </div>
        <span className={`text-xs font-bold ${passed ? 'text-accent-green' : 'text-gray-500'}`}>
          {passed ? 'MET' : 'NOT MET'}
        </span>
      </div>
      <div className="w-full bg-gray-800 rounded-full h-2 mb-2">
        <div className={`h-2 rounded-full transition-all ${passed ? 'bg-accent-green' : 'bg-accent-blue'}`} style={{ width: `${pct}%` }} />
      </div>
      <div className="flex justify-between text-[10px] text-gray-500">
        <span>Current: {current != null ? String(current) : '—'}</span>
        <span>Required: {required != null ? String(required) : '—'}</span>
      </div>
      {children}
    </div>
  )
}

export default function LiveReadiness() {
  const { data: health } = useApi(api.intelligenceHealth, 30_000)
  const { data: policyData } = useApi(api.executionPolicy, 30_000)
  const [saving, setSaving] = useState(false)

  const lr = health?.live_readiness || {}
  const intel = health?.intelligence || {}
  const monitor = health?.monitor || {}
  const policy = policyData?.policy || 'paper_t1'

  const gates = [
    { n: 1, title: 'RESOLVED PAPER TRADES', ...lr.gate_1_resolved_trades, req_label: '50' },
    { n: 2, title: 'WIN RATE', ...lr.gate_2_categories_with_edge, req_label: '> 55%' },
    { n: 3, title: 'CATEGORY EDGE', ...lr.gate_3_max_drawdown, req_label: '2 categories' },
    { n: 4, title: 'DRAWDOWN CONTROL', ...lr.gate_3_max_drawdown, req_label: '< 20%' },
    { n: 5, title: 'HUMAN APPROVAL', ...lr.gate_4_human_approval, req_label: 'Manual' },
  ]
  const passedCount = gates.filter(g => g.passed).length
  const gates14Passed = [lr.gate_1_resolved_trades, lr.gate_2_categories_with_edge, lr.gate_3_max_drawdown].every(g => g?.passed)

  const setPolicy = async (p) => {
    setSaving(true)
    try { await api.setExecutionPolicy(p) } catch (e) { console.error(e) }
    setSaving(false)
  }

  return (
    <div className="p-6 space-y-5">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Live Readiness</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Live Readiness</h1>
        <p className="text-xs text-gray-500 mt-0.5">Track progress toward autonomous live trading</p>
      </div>

      {/* System Status Row */}
      <div className="flex items-center gap-6 px-4 py-2 bg-surface-card rounded-lg text-[10px] text-gray-500">
        <span className="flex items-center gap-1.5">
          <span className={`w-2 h-2 rounded-full ${monitor.connected ? 'bg-accent-green animate-pulse' : 'bg-accent-red'}`} />
          {monitor.connected ? 'MONITORING' : 'PAUSED'}
        </span>
        <span>Kill switch: <span className="text-accent-green">Active</span></span>
        <span>Leaderboard: v{intel.leaderboard_version || 0} ({intel.leaderboard_age_hours?.toFixed(1) || '?'}h ago)</span>
      </div>

      {/* Overall Progress */}
      <div className="bg-surface-card rounded-lg p-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm font-medium text-gray-300">{passedCount} / 5 gates passed</span>
          <span className={`text-xs ${passedCount === 5 ? 'text-accent-green font-bold' : 'text-gray-500'}`}>
            {passedCount === 5 ? 'READY FOR LIVE' : 'NOT READY'}
          </span>
        </div>
        <div className="w-full bg-gray-800 rounded-full h-3">
          <div className={`h-3 rounded-full transition-all ${passedCount === 5 ? 'bg-accent-green' : 'bg-accent-blue'}`}
               style={{ width: `${(passedCount / 5) * 100}%` }} />
        </div>
      </div>

      {/* The 5 Gates */}
      <div className="space-y-3">
        <GateCard number={1} title="RESOLVED PAPER TRADES"
          current={lr.gate_1_resolved_trades?.current || 0}
          required={50}
          passed={lr.gate_1_resolved_trades?.passed} />

        <GateCard number={2} title="WIN RATE"
          current={lr.gate_2_categories_with_edge?.current || 0}
          required={2}
          passed={lr.gate_2_categories_with_edge?.passed}>
          <p className="text-[9px] text-gray-600 mt-1">2+ categories with independent edge (&gt; 52% WR, 10+ resolved)</p>
        </GateCard>

        <GateCard number={3} title="DRAWDOWN CONTROL"
          current={`${((lr.gate_3_max_drawdown?.current || 0) * 100).toFixed(1)}%`}
          required="< 20%"
          passed={lr.gate_3_max_drawdown?.passed} />

        <GateCard number={4} title="HUMAN APPROVAL"
          current={lr.gate_4_human_approval?.passed ? 'Approved' : 'Not approved'}
          required="Manual approval"
          passed={lr.gate_4_human_approval?.passed}>
          <button
            className={`mt-2 px-3 py-1.5 rounded text-xs font-medium transition-colors ${
              gates14Passed
                ? 'bg-amber-600 hover:bg-amber-500 text-white'
                : 'bg-gray-700 text-gray-500 cursor-not-allowed'
            }`}
            disabled={!gates14Passed}
            onClick={() => {
              if (confirm('Are you sure? This enables live trading with real capital.\nStart with $500 maximum. All safety gates must pass.')) {
                setPolicy('live_t1')
              }
            }}
          >
            {lr.gate_4_human_approval?.passed ? 'Approved ✓' : 'APPROVE FOR LIVE TRADING'}
          </button>
          {!gates14Passed && <p className="text-[9px] text-gray-600 mt-1">Complete gates 1-3 first</p>}
        </GateCard>

        <GateCard number={5} title="CAPITAL ALLOCATED"
          current={lr.gate_5_capital_allocated?.passed ? '$500' : '$0'}
          required="$500"
          passed={lr.gate_5_capital_allocated?.passed} />
      </div>

      {/* Execution Policy */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-300 mb-3">Execution Policy</h2>
        <div className="space-y-2">
          {[
            { value: 'monitor_only', label: 'Monitor only — log signals, no trades' },
            { value: 'paper_t1', label: 'Paper trade — tier-1 signals only' },
            { value: 'paper_t1t2', label: 'Paper trade — tier-1 + tier-2 signals' },
            { value: 'live_t1', label: 'Live trade — tier-1 only', locked: !lr.gate_4_human_approval?.passed },
          ].map(opt => (
            <label key={opt.value} className={`flex items-center gap-2 text-xs cursor-pointer ${opt.locked ? 'opacity-40 cursor-not-allowed' : ''}`}>
              <input
                type="radio"
                name="policy"
                checked={policy === opt.value}
                disabled={opt.locked || saving}
                onChange={() => setPolicy(opt.value)}
                className="accent-accent-blue"
              />
              <span className="text-gray-400">{opt.label}</span>
              {opt.locked && <span className="text-[9px] text-gray-600">[LOCKED]</span>}
            </label>
          ))}
        </div>
      </div>
    </div>
  )
}

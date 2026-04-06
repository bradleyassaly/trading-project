import { useState, useCallback } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'
import { SignalBadge, TierBadge } from '../components/Badges'

function relTime(ts) {
  if (!ts) return '?'
  const d = (Date.now()/1000) - (typeof ts==='string' ? new Date(ts).getTime()/1000 : ts)
  if (d<60) return '<1m'; if (d<3600) return `${Math.floor(d/60)}m ago`; if (d<86400) return `${Math.floor(d/3600)}h ago`; return `${Math.floor(d/86400)}d ago`
}

const STATUS_BADGE = { building:'bg-gray-700 text-gray-400', live:'bg-accent-green/20 text-accent-green', weak:'bg-yellow-900/60 text-yellow-300', off:'bg-red-900/60 text-red-400' }

function SignalPerformanceTable() {
  const { data: perf, loading: pL } = useApi(api.signalsPerformance, 300_000)
  const { data: bankroll } = useApi(api.paperBankroll, 300_000)
  const types = perf?.by_type ?? []
  const bySignal = bankroll?.by_signal ?? {}

  if (pL && !perf) return <LoadingSkeleton rows={5} />
  if (!types.length) return <EmptyState title="No signal types configured" />

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-1">Signal Type Performance</h2>
      <p className="text-[10px] text-gray-600 mb-3">Virtual $10,000 bankroll allocation and paper trading results</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-2 pr-3">Signal Type</th>
              <th className="pb-2 pr-3 text-right">Allocated</th>
              <th className="pb-2 pr-3 text-right">Per Trade</th>
              <th className="pb-2 pr-3 text-right">Fired</th>
              <th className="pb-2 pr-3 text-right">Resolved</th>
              <th className="pb-2 pr-3 text-right">Win Rate</th>
              <th className="pb-2 pr-3 text-right">Avg EV</th>
              <th className="pb-2 pr-3 text-right">P.Factor</th>
              <th className="pb-2 pr-3 text-right">P&L</th>
              <th className="pb-2 pr-3">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {types.map(t => {
              const alloc = bySignal[t.signal_type] || t
              const st = STATUS_BADGE[t.status] || STATUS_BADGE.building
              return (
                <tr key={t.signal_type} className="hover:bg-surface-hover">
                  <td className="py-1.5 pr-3"><SignalBadge type={t.signal_type} /></td>
                  <td className="py-1.5 pr-3 text-right font-mono text-gray-400">${(alloc.allocated||0).toLocaleString()}</td>
                  <td className="py-1.5 pr-3 text-right font-mono text-gray-500">${alloc.stake_per_trade||0}</td>
                  <td className="py-1.5 pr-3 text-right text-gray-300">{t.fired||0}</td>
                  <td className="py-1.5 pr-3 text-right text-gray-400">{t.resolved||0}</td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${(t.win_rate||0)>0.55?'text-accent-green':'text-gray-500'}`}>
                    {t.win_rate!=null?`${(t.win_rate*100).toFixed(0)}%`:'--'}
                  </td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${(t.avg_ev||0)>0?'text-accent-green':'text-gray-500'}`}>
                    {t.avg_ev!=null?`${t.avg_ev>=0?'+':''}${t.avg_ev.toFixed(2)}`:'--'}
                  </td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${(t.profit_factor||0)>1.5?'text-accent-green':(t.profit_factor||0)>1?'text-yellow-400':'text-gray-500'}`}>
                    {t.profit_factor!=null?t.profit_factor.toFixed(1):'--'}
                  </td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${(t.cumulative_pnl||0)>0?'text-accent-green':(t.cumulative_pnl||0)<0?'text-accent-red':'text-gray-600'}`}>
                    ${Math.abs(t.cumulative_pnl||0).toFixed(0)}
                  </td>
                  <td className="py-1.5 pr-3"><span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${st}`}>{t.status||'building'}</span></td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <p className="text-[9px] text-gray-600 mt-2">Metrics update as paper trades resolve. Next resolution: April 15, 2026</p>
    </div>
  )
}

function PaperTradeLog() {
  const { data, loading } = useApi(api.paperTrades, 120_000)
  const trades = data?.data ?? []

  if (loading && !data) return <LoadingSkeleton rows={6} />
  if (!trades.length) return <EmptyState title="No paper trades" message="Paper trades appear when signals trigger execution" />

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-3">Paper Trade Log</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-2 pr-3">Date</th>
              <th className="pb-2 pr-3">Signal</th>
              <th className="pb-2 pr-3">Market</th>
              <th className="pb-2 pr-3">Side</th>
              <th className="pb-2 pr-3 text-right">Stake</th>
              <th className="pb-2 pr-3 text-right">Entry</th>
              <th className="pb-2 pr-3">Outcome</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {trades.slice(0,30).map((t,i) => {
              const sig = t.signal_family || t.signal_type || 'legacy'
              const isLegacy = sig.startsWith('kalshi_')
              return (
                <tr key={i} className={`hover:bg-surface-hover ${isLegacy?'opacity-50':''}`}>
                  <td className="py-1.5 pr-3 text-gray-500 text-[10px]">{(t.entry_ts||'').slice(0,10)}</td>
                  <td className="py-1.5 pr-3">{isLegacy ? <span className="px-1.5 py-0.5 rounded text-[9px] bg-gray-800 text-gray-500">Legacy</span> : <SignalBadge type={sig} />}</td>
                  <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[180px]">{t.ticker}</td>
                  <td className={`py-1.5 pr-3 font-bold ${t.side==='YES'?'text-accent-green':'text-accent-red'}`}>{t.side}</td>
                  <td className="py-1.5 pr-3 text-right font-mono text-gray-400">${(t.size_usd||0).toFixed(2)}</td>
                  <td className="py-1.5 pr-3 text-right font-mono text-gray-400">{t.entry_price?.toFixed(1)}</td>
                  <td className="py-1.5 pr-3">
                    {t.outcome==='win'?<span className="text-accent-green">[OK]</span>:
                     t.outcome==='loss'?<span className="text-accent-red">[X]</span>:
                     <span className="text-gray-500">Pending</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function SignalHistory() {
  const { data, loading } = useApi(useCallback(()=>api.smartMoneyAlerts({limit:200}),[]), 120_000)
  const alerts = (data?.data??[]).filter(a => a.wallet!=='0xaaa' && a.wallet!=='0xbbb')

  if (loading && !data) return <LoadingSkeleton rows={8} />
  if (!alerts.length) return <EmptyState title="No signal history" message="Signals appear as the monitor detects smart money trades" />

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-3">Signal History</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-2 pr-3">Time</th>
              <th className="pb-2 pr-3">Type</th>
              <th className="pb-2 pr-3">Market</th>
              <th className="pb-2 pr-3">Dir</th>
              <th className="pb-2 pr-3">Tier</th>
              <th className="pb-2 pr-3 text-right">Amount</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {alerts.slice(0,50).map((a,i) => (
              <tr key={i} className="hover:bg-surface-hover">
                <td className="py-1.5 pr-3 text-gray-500">{relTime(a.ts||a.detected_at)}</td>
                <td className="py-1.5 pr-3"><SignalBadge type={a.alert_type||a.signal_type||a.wallet_type} /></td>
                <td className="py-1.5 pr-3 text-gray-300 truncate max-w-[200px]">{a.market_question||a.market_title||'Unknown'}</td>
                <td className={`py-1.5 pr-3 font-bold ${a.side==='YES'?'text-accent-green':'text-accent-red'}`}>{a.side}</td>
                <td className="py-1.5 pr-3"><TierBadge tier={a.tier} /></td>
                <td className="py-1.5 pr-3 text-right font-mono text-gray-400">${Number(a.amount_usdc||a.size||0).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function SignalMonitor() {
  return (
    <div className="p-6 space-y-6">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Signal Monitor</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Signal Monitor</h1>
        <p className="text-xs text-gray-500 mt-0.5">Track signal performance, EV, and calibration across all signal types</p>
      </div>
      <div className="card"><SignalPerformanceTable /></div>
      <div className="card"><PaperTradeLog /></div>
      <div className="card"><SignalHistory /></div>
    </div>
  )
}

import { useCallback, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'
import { SignalBadge, TierBadge } from '../components/Badges'

function truncAddr(a) { return a?.length > 12 ? `${a.slice(0,6)}...${a.slice(-4)}` : a || '?' }
function relTime(ts) {
  if (!ts) return '?'
  const d = (Date.now()/1000) - (typeof ts==='string' ? new Date(ts).getTime()/1000 : ts)
  if (d<60) return '<1m'; if (d<3600) return `${Math.floor(d/60)}m`; if (d<86400) return `${Math.floor(d/3600)}h`; return `${Math.floor(d/86400)}d`
}
function fmtUsd(n) { return n >= 1e6 ? `$${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `$${(n/1e3).toFixed(0)}K` : `$${Math.round(n)}` }

const SIG_LABELS = { whale_entry:'Whale', convergence:'Converge', contrarian_whale:'Contrarian', position_building:'Building', cross_type:'CrossType', cascade:'Cascade', domain_specialist:'Specialist' }
const STATUS_BADGE = { building:'bg-gray-700 text-gray-400', live:'bg-accent-green/20 text-accent-green', weak:'bg-yellow-900/60 text-yellow-300', off:'bg-red-900/60 text-red-400' }

// ── Section 1: Signal Feed Strip ────────────────────────────────────────────
function SignalFeed() {
  const { data } = useApi(api.smartMoneyActionableSignals, 60_000)
  const { data: stats } = useApi(api.smartMoneyUniverseStats, 300_000)
  const signals = data?.data ?? []
  const totalW = stats?.total_wallets || '...'

  if (!signals.length) return (
    <div className="bg-surface-card rounded-lg px-4 py-3 text-xs text-gray-500">
      No active signals -- monitoring {totalW} wallets for convergence
    </div>
  )
  return (
    <div className="flex gap-2 overflow-x-auto pb-1">
      {signals.slice(0,10).map((s,i) => (
        <div key={i} className={`flex-shrink-0 bg-surface-card rounded-lg p-2.5 w-52 border-l-2 ${s.direction==='YES'?'border-accent-green':'border-accent-red'}`}>
          <p className="text-[10px] text-gray-300 truncate mb-1">{(s.market_title||'').slice(0,45)}</p>
          <div className="flex items-center gap-1.5 text-[9px]">
            <span className={`px-1 py-0.5 rounded font-bold ${s.direction==='YES'?'bg-accent-green/20 text-accent-green':'bg-accent-red/20 text-accent-red'}`}>{s.direction}</span>
            <TierBadge tier={s.confidence>0.7?1:2} />
            <span className="text-gray-400">${Number(s.weighted_net_volume||0).toLocaleString()}</span>
            <span className="text-gray-600">{s.directional_wallet_count}w</span>
          </div>
        </div>
      ))}
    </div>
  )
}

// ── Section 2: Six Stat Cards ───────────────────────────────────────────────
function StatCards() {
  const { data: stats } = useApi(api.smartMoneyUniverseStats, 300_000)
  const { data: bankroll } = useApi(api.paperBankroll, 60_000)
  const { data: sigPerf } = useApi(api.signalsPerformance, 300_000)
  const { data: openPos } = useApi(api.smartMoneyOpenPositions, 300_000)

  const s = stats || {}
  const b = bankroll || {}
  const bt = s.by_type || {}
  const bTier = s.by_tier || {}

  // Best signal type
  const bestSig = (sigPerf?.by_type || []).sort((a,b) => (b.fired||0) - (a.fired||0))[0]

  // Whale positions
  const whalePos = (openPos?.data || []).filter(p => (p.net_amount_usdc||0) > 10000)
  const topPos = whalePos.sort((a,b) => (b.net_amount_usdc||0) - (a.net_amount_usdc||0))[0]

  return (
    <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">WALLET UNIVERSE</p>
        <p className="text-xl font-bold text-gray-200">{s.total_wallets ?? '--'}</p>
        <p className="text-[9px] text-gray-500">{bt.directional||0} directional  {bTier.whale||0} whales</p>
        <p className="text-[9px] text-gray-600">{fmtUsd(s.total_deployed_usdc||0)} deployed</p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">SIGNALS TODAY</p>
        <p className="text-xl font-bold text-gray-200">{s.signals_today ?? 0}</p>
        <p className="text-[9px] text-gray-500"><span className="text-accent-red">{s.tier1_today||0} T1</span>  <span className="text-yellow-400">{s.tier2_today||0} T2</span></p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">LIVE READINESS</p>
        <p className="text-xl font-bold text-gray-200">4 / 8 gates</p>
        <div className="w-full bg-gray-800 rounded-full h-1.5 mt-1"><div className="h-1.5 rounded-full bg-accent-blue" style={{width:'50%'}} /></div>
        <Link to="/engine" className="text-[9px] text-accent-blue hover:underline mt-1 inline-block">View gates</Link>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">PAPER BANKROLL</p>
        <p className="text-xl font-bold font-mono text-gray-200">${(b.total_bankroll||10000).toLocaleString()}</p>
        <p className={`text-[9px] font-mono ${(b.net_pnl||0)>=0?'text-accent-green':'text-accent-red'}`}>
          P&L: {(b.net_pnl||0)>=0?'+':''}${Math.abs(b.net_pnl||0).toFixed(2)}  {b.resolved||0} resolved
        </p>
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">BEST SIGNAL</p>
        {bestSig ? (
          <>
            <div className="flex items-center gap-1.5"><SignalBadge type={bestSig.signal_type} /><span className="text-sm font-bold text-gray-200">{bestSig.fired} fired</span></div>
            <p className="text-[9px] text-gray-500">{bestSig.resolved} resolved  Status: {bestSig.status}</p>
          </>
        ) : <p className="text-sm text-gray-600">Building sample...</p>}
      </div>
      <div className="bg-surface-card rounded-lg p-3">
        <p className="text-[10px] text-gray-500 mb-1">WHALE ACTIVITY</p>
        <p className="text-xl font-bold text-gray-200">{whalePos.length} positions</p>
        <p className="text-[9px] text-gray-500 truncate">{topPos?.market_question?.slice(0,35) || 'Loading...'}</p>
      </div>
    </div>
  )
}

// ── Section 3: Signal Performance Table ─────────────────────────────────────
function SignalPerfTable() {
  const { data: perf, loading } = useApi(api.signalsPerformance, 300_000)
  const { data: bankroll } = useApi(api.paperBankroll, 300_000)
  const types = perf?.by_type ?? []
  const bySignal = bankroll?.by_signal ?? {}

  if (loading && !perf) return <LoadingSkeleton rows={4} />

  return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-1">Signal Performance</h2>
      <p className="text-[10px] text-gray-600 mb-3">Virtual $10,000 bankroll -- paper trading results by signal type</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-surface-border text-gray-500 text-left">
              <th className="pb-2 pr-3">Signal Type</th>
              <th className="pb-2 pr-3 text-right">Alloc</th>
              <th className="pb-2 pr-3 text-right">Per Trade</th>
              <th className="pb-2 pr-3 text-right">Fired</th>
              <th className="pb-2 pr-3 text-right">Resolved</th>
              <th className="pb-2 pr-3 text-right">WR</th>
              <th className="pb-2 pr-3 text-right">EV</th>
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
                  <td className="py-1.5 pr-3 text-right font-mono text-gray-500">{t.win_rate!=null?`${(t.win_rate*100).toFixed(0)}%`:'--'}</td>
                  <td className="py-1.5 pr-3 text-right font-mono text-gray-500">{t.avg_ev!=null?`${t.avg_ev>=0?'+':''}${t.avg_ev.toFixed(2)}`:'--'}</td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${(t.cumulative_pnl||0)>0?'text-accent-green':(t.cumulative_pnl||0)<0?'text-accent-red':'text-gray-600'}`}>
                    {t.cumulative_pnl?`$${t.cumulative_pnl.toFixed(0)}`:'$0'}
                  </td>
                  <td className="py-1.5 pr-3"><span className={`px-1.5 py-0.5 rounded text-[9px] font-semibold ${st}`}>{t.status||'building'}</span></td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <p className="text-[9px] text-gray-600 mt-2">EV and profit factor update as paper trades resolve. Next resolution: April 15, 2026</p>
    </div>
  )
}

// ── Section 4 Left: Positions + PnL History ─────────────────────────────────
function PositionsPanel() {
  const [tab, setTab] = useState('positions')
  const { data: posData } = useApi(api.smartMoneyOpenPositions, 300_000)
  const { data: pnlData } = useApi(api.paperPnlHistory, 300_000)
  const positions = (posData?.data ?? []).sort((a,b) => (b.net_amount_usdc||0) - (a.net_amount_usdc||0)).slice(0,15)
  const hasHistory = pnlData?.has_data

  return (
    <div>
      <div className="flex gap-1 mb-3">
        <button className={`text-[10px] px-2 py-1 rounded ${tab==='positions'?'bg-accent-blue/20 text-accent-blue':'text-gray-500'}`} onClick={()=>setTab('positions')}>Active Positions</button>
        <button className={`text-[10px] px-2 py-1 rounded ${tab==='pnl'?'bg-accent-blue/20 text-accent-blue':'text-gray-500'}`} onClick={()=>setTab('pnl')}>P&L History</button>
      </div>

      {tab === 'positions' && (
        !positions.length ? <EmptyState title="No positions" message="Run: compute-open-positions" /> : (
          <div className="space-y-1">
            {positions.map((p,i) => {
              const name = p.market_question || `${(p.token_id||'').slice(0,8)}...${(p.token_id||'').slice(-6)}`
              return (
                <div key={i} className={`flex items-center justify-between text-[10px] py-1 ${(p.net_amount_usdc||0)>50000?'border-l-2 border-accent-green pl-2':'pl-2'}`}>
                  <div className="flex items-center gap-1.5 truncate max-w-[60%]">
                    <span className={`px-1 py-0.5 rounded text-[8px] font-bold ${p.net_side==='YES'?'bg-accent-green/20 text-accent-green':'bg-accent-red/20 text-accent-red'}`}>{p.net_side}</span>
                    <span className="text-gray-300 truncate">{name.slice(0,50)}</span>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="font-mono text-gray-400">${Number(p.net_amount_usdc||0).toLocaleString(undefined,{maximumFractionDigits:0})}</span>
                  </div>
                </div>
              )
            })}
          </div>
        )
      )}

      {tab === 'pnl' && (
        <div className="bg-surface-card rounded-lg p-4 text-center">
          <p className="text-xs text-gray-400 mb-2">P&L history builds as paper trades resolve</p>
          <div className="w-full bg-gray-800 rounded-full h-2 mb-2">
            <div className="h-2 rounded-full bg-gray-700" style={{width:'0%'}} />
          </div>
          <p className="text-[10px] text-gray-500">0/50 minimum for meaningful data</p>
          <p className="text-[10px] text-gray-600 mt-1">{pnlData?.next_resolution || 'Next resolution: April 15, 2026'}</p>
        </div>
      )}
    </div>
  )
}

// ── Section 4 Right: Alert Feed ─────────────────────────────────────────────
function AlertFeed() {
  const { data } = useApi(useCallback(()=>api.smartMoneyAlerts({limit:30}),[]), 90_000)
  const { data: stats } = useApi(api.smartMoneyUniverseStats, 300_000)
  const alerts = (data?.data??[]).filter(a => a.wallet!=='0xaaa' && a.wallet!=='0xbbb')

  if (!alerts.length) return (
    <div>
      <h2 className="text-sm font-medium text-gray-400 mb-3">Alert Feed</h2>
      <div className="text-xs text-gray-500 space-y-1">
        <p>No alerts yet.</p>
        <p>System is monitoring {stats?.total_wallets||'...'} wallets.</p>
        <p className="text-gray-600">Alerts appear when tracked wallets make significant trades.</p>
      </div>
    </div>
  )

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <h2 className="text-sm font-medium text-gray-400">Alert Feed</h2>
        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent-green/20 text-accent-green">
          <span className="w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" /> LIVE
        </span>
      </div>
      <div className="space-y-1">
        {alerts.slice(0,20).map((a,i) => (
          <div key={i} className={`flex items-center gap-1.5 text-[10px] py-1 ${a.tier===1?'border-l-2 border-accent-green pl-1':'border-l-2 border-yellow-600/50 pl-1'}`}>
            <SignalBadge type={a.alert_type||a.signal_type||a.wallet_type} />
            <span className="text-gray-500 w-6 flex-shrink-0">{relTime(a.ts||a.detected_at)}</span>
            <span className="font-mono text-accent-blue flex-shrink-0 text-[9px]">{truncAddr(a.wallet)}</span>
            <span className="text-gray-400 truncate flex-1">{(a.market_question||a.market_title||'').slice(0,35)||'Unknown market'}</span>
            <TierBadge tier={a.tier} />
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Section 5: System Health ────────────────────────────────────────────────
function SystemHealth() {
  const { data: status } = useApi(api.systemStatus, 15_000)
  const { data: stats } = useApi(api.smartMoneyUniverseStats, 300_000)
  const loop = status?.loop_state || 'unknown'
  const dotCls = loop==='running'?'bg-accent-green animate-pulse':loop==='stopped'?'bg-accent-red':'bg-gray-500'

  return (
    <div className="flex items-center gap-6 px-4 py-2 bg-surface-card rounded-lg text-[10px] text-gray-500">
      <span className="flex items-center gap-1.5"><span className={`w-2 h-2 rounded-full ${dotCls}`}/> Loop: <span className="text-gray-300">{loop}</span></span>
      <span>Wallets: <span className="text-gray-300">{stats?.total_wallets||'...'}</span> tracked</span>
      <span>Last sync: <span className="text-gray-300">{stats?.last_sync_ts?relTime(stats.last_sync_ts):'never'}</span></span>
    </div>
  )
}

// ── Main Dashboard ──────────────────────────────────────────────────────────
export default function Dashboard() {
  return (
    <div className="p-6 space-y-5">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Command Center</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Command Center</h1>
      </div>

      <SignalFeed />
      <StatCards />

      <div className="card"><SignalPerfTable /></div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <div className="card lg:col-span-3"><PositionsPanel /></div>
        <div className="card lg:col-span-2"><AlertFeed /></div>
      </div>

      <SystemHealth />
    </div>
  )
}

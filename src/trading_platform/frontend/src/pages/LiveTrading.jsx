import { useApi } from '../hooks/useApi'
import { api } from '../api/client'
import LoadingSkeleton from '../components/LoadingSkeleton'
import KillSwitchBadge from '../components/KillSwitchBadge'

function fmtUsd(v, { signed } = {}) {
  if (v == null) return '--'
  const n = Number(v)
  const s = signed && n >= 0 ? '+' : ''
  return `${s}$${Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}
function fmtPct(v) { return v != null ? `${(v * 100).toFixed(1)}%` : '--' }
function fmtTime(ts) {
  if (!ts) return '--'
  const d = new Date(ts * 1000)
  return d.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}
function pnlColor(v) {
  if (v == null) return 'text-gray-500'
  return v >= 0 ? 'text-accent-green' : 'text-accent-red'
}

function Metric({ label, value, sub, color }) {
  return (
    <div className="bg-surface-card rounded-lg p-3">
      <p className="text-[10px] text-gray-500 mb-1">{label}</p>
      <p className={`text-xl font-bold font-mono ${color || 'text-gray-200'}`}>{value}</p>
      {sub && <p className="text-[9px] text-gray-600 mt-0.5">{sub}</p>}
    </div>
  )
}

function CatBadge({ cat }) {
  const styles = {
    geopolitics: 'bg-purple-500/20 text-purple-400',
    politics: 'bg-blue-500/20 text-blue-400',
    crypto: 'bg-yellow-500/20 text-yellow-400',
    economics: 'bg-green-500/20 text-green-400',
    sports: 'bg-orange-500/20 text-orange-400',
  }
  return (
    <span className={`px-1.5 py-0.5 rounded text-[8px] font-semibold ${styles[cat] || 'bg-gray-700 text-gray-400'}`}>
      {cat || 'other'}
    </span>
  )
}

// ── System Health Bar ──────────────────────────────────────────────────────
function HealthBar() {
  const { data } = useApi(api.systemHealth, 30_000)
  if (!data) return null

  const dot = (ok) => (
    <span className={`inline-block w-1.5 h-1.5 rounded-full mr-1 ${ok ? 'bg-accent-green' : 'bg-accent-red'}`} />
  )
  const yesRate = data.resolution_yes_rate ?? 0
  const yesWarn = yesRate > 0.30

  return (
    <div className="flex items-center gap-4 text-[10px] text-gray-400 bg-surface-card rounded-lg px-3 py-2 mb-4 flex-wrap">
      <span>{dot(data.db_integrity)}DB</span>
      <span>{dot(data.pipeline_running)}Pipeline</span>
      <span>{dot(data.signals_24h > 0)}Signals: {data.signals_24h}</span>
      <span className={yesWarn ? 'text-accent-red font-bold' : ''}>
        {dot(!yesWarn)}YES rate: {(yesRate * 100).toFixed(1)}%
      </span>
      <span>{dot(data.live_enabled)}Live: {data.live_enabled ? 'ON' : 'OFF'}</span>
      <span>{dot(data.kill_switch === 'active')}
        Kill: {data.kill_switch}
      </span>
      <span className="text-gray-600">Positions: {data.live_positions}</span>
    </div>
  )
}

// ── Portfolio Summary ──────────────────────────────────────────────────────
function PortfolioSummary({ positions, history, killSwitch }) {
  const open = positions?.positions || []
  const closed = history?.trades || []
  const exposure = positions?.total_exposure || 0
  const realized = history?.total_realized_pnl || 0
  const bankroll = killSwitch?.config?.BANKROLL || 10000
  const available = bankroll - exposure + realized

  return (
    <div className="grid grid-cols-2 lg:grid-cols-5 gap-3 mb-4">
      <Metric label="BANKROLL" value={fmtUsd(bankroll)} />
      <Metric label="DEPLOYED" value={fmtUsd(exposure)} sub={`${open.length} positions`} />
      <Metric label="AVAILABLE" value={fmtUsd(Math.max(0, available))} />
      <Metric label="REALIZED P&L" value={fmtUsd(realized, { signed: true })} color={pnlColor(realized)} sub={`${closed.length} closed`} />
      <Metric label="WIN RATE" value={fmtPct(history?.win_rate)} color={history?.win_rate >= 0.52 ? 'text-accent-green' : 'text-gray-400'} />
    </div>
  )
}

// ── Open Positions Table ───────────────────────────────────────────────────
function OpenPositions({ data }) {
  const positions = data?.positions || []
  if (!positions.length) {
    return <div className="card"><p className="text-xs text-gray-500">No open live positions</p></div>
  }
  return (
    <div className="card mb-4">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Open Positions ({positions.length})</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[10px] text-gray-500 border-b border-surface-border">
              <th className="text-left py-1.5 pr-2">Market</th>
              <th className="text-left py-1.5 pr-2">Cat</th>
              <th className="text-left py-1.5 pr-2">Side</th>
              <th className="text-right py-1.5 pr-2">Entry</th>
              <th className="text-right py-1.5 pr-2">Size</th>
              <th className="text-left py-1.5 pr-2">Signal</th>
              <th className="text-right py-1.5">Opened</th>
            </tr>
          </thead>
          <tbody>
            {positions.map(p => (
              <tr key={p.id} className="border-b border-surface-border/50 hover:bg-surface-hover">
                <td className="py-1.5 pr-2 max-w-[200px] truncate" title={p.question}>{p.question || p.condition_id?.slice(0, 12)}</td>
                <td className="py-1.5 pr-2"><CatBadge cat={p.category} /></td>
                <td className="py-1.5 pr-2 font-mono">{p.side}</td>
                <td className="py-1.5 pr-2 text-right font-mono">${p.fill_price?.toFixed(3)}</td>
                <td className="py-1.5 pr-2 text-right font-mono">{fmtUsd(p.size_usd)}</td>
                <td className="py-1.5 pr-2 text-gray-400">{p.signal_type}</td>
                <td className="py-1.5 text-right text-gray-500">{fmtTime(p.submitted_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Closed Trades Table ────────────────────────────────────────────────────
function ClosedTrades({ data }) {
  const trades = data?.trades || []
  if (!trades.length) {
    return <div className="card"><p className="text-xs text-gray-500">No closed live trades yet</p></div>
  }
  return (
    <div className="card mb-4">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Recent Closed Trades ({trades.length})</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[10px] text-gray-500 border-b border-surface-border">
              <th className="text-left py-1.5 pr-2">Market</th>
              <th className="text-left py-1.5 pr-2">Cat</th>
              <th className="text-left py-1.5 pr-2">Side</th>
              <th className="text-right py-1.5 pr-2">Entry</th>
              <th className="text-right py-1.5 pr-2">Exit</th>
              <th className="text-right py-1.5 pr-2">PnL</th>
              <th className="text-left py-1.5">Result</th>
            </tr>
          </thead>
          <tbody>
            {trades.map(t => (
              <tr key={t.id} className="border-b border-surface-border/50 hover:bg-surface-hover">
                <td className="py-1.5 pr-2 max-w-[200px] truncate" title={t.question}>{t.question || t.condition_id?.slice(0, 12)}</td>
                <td className="py-1.5 pr-2"><CatBadge cat={t.category} /></td>
                <td className="py-1.5 pr-2 font-mono">{t.side}</td>
                <td className="py-1.5 pr-2 text-right font-mono">${t.fill_price?.toFixed(3)}</td>
                <td className="py-1.5 pr-2 text-right font-mono">${t.exit_price?.toFixed(3)}</td>
                <td className={`py-1.5 pr-2 text-right font-mono font-semibold ${pnlColor(t.realized_pnl)}`}>
                  {fmtUsd(t.realized_pnl, { signed: true })}
                </td>
                <td className="py-1.5">{t.outcome === 'win' ? '✅' : '❌'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Execution Quality Card ─────────────────────────────────────────────────
function ExecutionQuality() {
  const { data } = useApi(api.liveExecutionQuality, 60_000)
  if (!data || !data.total_orders) return null

  const fillColor = data.fill_rate >= 0.8 ? 'text-accent-green' : data.fill_rate >= 0.6 ? 'text-yellow-400' : 'text-accent-red'
  const slipColor = (data.avg_slippage || 0) < 0.01 ? 'text-accent-green' : (data.avg_slippage || 0) < 0.03 ? 'text-yellow-400' : 'text-accent-red'

  return (
    <div className="card mb-4">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Execution Quality</h3>
      <div className="grid grid-cols-3 gap-3">
        <Metric label="FILL RATE" value={fmtPct(data.fill_rate)} color={fillColor} sub={`${data.filled}/${data.total_orders} orders`} />
        <Metric label="AVG SLIPPAGE" value={fmtPct(data.avg_slippage)} color={slipColor} />
        <Metric label="AVG FILL TIME" value={`${(data.avg_fill_time_ms || 0).toFixed(0)}ms`} />
      </div>
    </div>
  )
}

// ── Insider Wallets Card ───────────────────────────────────────────────────
function InsiderCard() {
  const { data } = useApi(api.insidersList, 60_000)
  if (!data?.insiders?.length) return null

  return (
    <div className="card mb-4">
      <h3 className="text-sm font-medium text-gray-300 mb-2">
        Insider Wallets ({data.count})
      </h3>
      <div className="space-y-1">
        {data.insiders.map(i => (
          <div key={i.wallet} className="flex items-center gap-2 text-xs py-1 border-b border-surface-border/30">
            <span className="text-purple-400">&#128269;</span>
            <span className="w-28 truncate font-mono text-gray-300" title={i.wallet}>
              {i.wallet?.slice(0, 14)}...
            </span>
            <span className={`font-mono ${i.uncertain_accuracy >= 0.80 ? 'text-accent-green' : 'text-gray-400'}`}>
              {(i.uncertain_accuracy * 100).toFixed(0)}%
            </span>
            <span className="text-gray-500">{i.uncertain_trades} trades</span>
            <CatBadge cat={i.primary_category} />
            <span className="text-gray-600 text-[9px]">score={i.insider_score?.toFixed(3)}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Signal Performance Card ────────────────────────────────────────────────
function SignalPerformance() {
  const { data } = useApi(api.signalsPerformance, 60_000)
  if (!data?.by_type) return null

  const types = Object.entries(data.by_type).sort((a, b) => (b[1].total_resolved || 0) - (a[1].total_resolved || 0))
  return (
    <div className="card">
      <h3 className="text-sm font-medium text-gray-300 mb-2">Signal Performance (corrected data)</h3>
      <div className="space-y-1">
        {types.map(([st, d]) => {
          const live = d.live_ready?.all_pass
          const evColor = d.avg_ev > 0 ? 'text-accent-green' : d.avg_ev < 0 ? 'text-accent-red' : 'text-gray-500'
          return (
            <div key={st} className="flex items-center gap-2 text-xs py-1 border-b border-surface-border/30">
              <span className={`inline-block w-1.5 h-1.5 rounded-full ${live ? 'bg-accent-green' : 'bg-gray-600'}`} />
              <span className="w-36 truncate text-gray-300 font-mono">{st}</span>
              <span className="text-gray-500 w-12 text-right">N={d.total_resolved}</span>
              <span className="text-gray-400 w-12 text-right">{fmtPct(d.win_rate)}</span>
              <span className={`w-16 text-right font-mono ${evColor}`}>
                {d.avg_ev != null ? (d.avg_ev >= 0 ? '+' : '') + d.avg_ev.toFixed(4) : '--'}
              </span>
              <span className="text-gray-600 text-[9px]">p={d.p_value?.toFixed(3)}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Main Page ──────────────────────────────────────────────────────────────
export default function LiveTrading() {
  const { data: positions, loading: posLoad } = useApi(api.livePositions, 30_000)
  const { data: history, loading: histLoad } = useApi(api.liveHistory, 30_000)
  const { data: ksData } = useApi(api.killSwitch, 15_000)

  return (
    <div className="p-4 max-w-6xl">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-lg font-semibold text-gray-200">Live Trading</h1>
          <p className="text-[10px] text-gray-500">Real-money positions on Polymarket CLOB</p>
        </div>
        <KillSwitchBadge />
      </div>

      <HealthBar />

      {posLoad && histLoad ? (
        <LoadingSkeleton rows={4} />
      ) : (
        <>
          <PortfolioSummary positions={positions} history={history} killSwitch={ksData} />
          <OpenPositions data={positions} />
          <ClosedTrades data={history} />
          <ExecutionQuality />
          <InsiderCard />
          <SignalPerformance />
        </>
      )}
    </div>
  )
}

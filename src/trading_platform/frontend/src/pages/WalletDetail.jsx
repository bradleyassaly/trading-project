import { useCallback, useMemo } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import { TierBadge, BucketBadge, SignalBadge } from '../components/Badges'

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`
  return `$${Math.round(n)}`
}
function fmtPct(n, dp = 1) { return n != null ? `${(n * 100).toFixed(dp)}%` : '—' }
function relTime(ts) {
  if (!ts) return '?'
  const d = (Date.now() / 1000) - ts
  if (d < 86400) return `${Math.floor(d / 3600)}h ago`
  return `${Math.floor(d / 86400)}d ago`
}

function PnlChart({ history }) {
  if (!history || history.length < 3) return (
    <div className="text-[10px] text-gray-600">
      {history?.length || 0} resolved trades — chart available after 3+
    </div>
  )

  const w = 800
  const h = 150
  const pad = 30
  const points = history
  const minPnl = Math.min(0, ...points.map(p => p.cumulative))
  const maxPnl = Math.max(0, ...points.map(p => p.cumulative))
  const range = maxPnl - minPnl || 1
  const xScale = (i) => pad + ((w - 2 * pad) * i) / Math.max(points.length - 1, 1)
  const yScale = (v) => h - pad - ((v - minPnl) / range) * (h - 2 * pad)
  const path = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${xScale(i)} ${yScale(p.cumulative)}`).join(' ')
  const final = points[points.length - 1].cumulative
  const lineColor = final >= 0 ? '#00d68f' : '#ef4444'
  const zeroY = yScale(0)

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none" style={{ height: 150 }}>
      <line x1={pad} y1={zeroY} x2={w - pad} y2={zeroY} stroke="#4b5563" strokeWidth="1" strokeDasharray="3 3" />
      <path d={path} fill="none" stroke={lineColor} strokeWidth="2" />
      <text x={pad} y={pad - 8} fontSize="9" fill="#6b7280">{fmtUsd(maxPnl)}</text>
      <text x={pad} y={h - pad + 14} fontSize="9" fill="#6b7280">{fmtUsd(minPnl)}</text>
      <text x={w - pad - 60} y={pad - 8} fontSize="10" fill={lineColor} textAnchor="end">
        Final: {fmtUsd(final)}
      </text>
    </svg>
  )
}

function CategoryBars({ breakdown }) {
  if (!breakdown || Object.keys(breakdown).length === 0) {
    return <p className="text-[10px] text-gray-600">No category data</p>
  }
  const entries = Object.entries(breakdown).sort((a, b) => b[1] - a[1]).slice(0, 5)
  return (
    <div className="space-y-1.5">
      {entries.map(([cat, pct]) => (
        <div key={cat} className="flex items-center gap-2 text-[10px]">
          <span className="text-gray-400 w-20 capitalize">{cat}</span>
          <div className="flex-1 bg-gray-800 rounded-full h-2 overflow-hidden">
            <div className="h-2 bg-accent-blue rounded-full" style={{ width: `${pct * 100}%` }} />
          </div>
          <span className="text-gray-500 w-10 text-right">{(pct * 100).toFixed(0)}%</span>
        </div>
      ))}
    </div>
  )
}

export default function WalletDetail() {
  const { address } = useParams()
  const navigate = useNavigate()
  const fetcher = useCallback(() => api.smartMoneyWalletDetail(address), [address])
  const { data, loading } = useApi(fetcher)
  // Alpha scores by category — full return distribution
  const alphaFetcher = useCallback(
    () => fetch(`/api/wallet/${encodeURIComponent(address)}/expected-returns`).then(r => r.ok ? r.json() : null),
    [address],
  )
  const { data: alphaData } = useApi(alphaFetcher, 0)
  // 2026-04-25: unified profile (PM PnL + behavior + insider + z-score
  // + alpha by category) from the new /api/wallet/{addr}/profile.
  const profileFetcher = useCallback(
    () => fetch(`/api/wallet/${encodeURIComponent(address)}/profile`).then(r => r.ok ? r.json() : null),
    [address],
  )
  const { data: profile } = useApi(profileFetcher, 0)

  if (loading && !data) return <div className="p-6"><LoadingSkeleton rows={10} /></div>
  if (!data?.available) {
    // If wallet detail failed but alpha endpoint returned leaderboard data, show it
    const alphaProfile = alphaData?.profile
    return (
      <div className="p-6 space-y-4">
        <div>
          <Link to="/wallets" className="text-xs text-accent-blue hover:underline">&larr; Back to Wallet Intel</Link>
          <h1 className="text-base font-semibold text-gray-200 font-mono mt-2">{address}</h1>
        </div>
        {alphaProfile ? (
          <div className="space-y-3">
            <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
              <p className="text-[10px] text-gray-500 mb-2">LEADERBOARD PROFILE</p>
              <div className="grid grid-cols-3 gap-3 text-[10px]">
                <div><span className="text-gray-500">Tier: </span><span className="text-gray-200">{alphaProfile.tier}</span></div>
                <div><span className="text-gray-500">WR: </span><span className="text-gray-200">{alphaProfile.directional_win_rate ? `${(alphaProfile.directional_win_rate*100).toFixed(0)}%` : '—'}</span></div>
                <div><span className="text-gray-500">Resolved: </span><span className="text-gray-200">{alphaProfile.resolved_trades ?? '—'}</span></div>
                <div><span className="text-gray-500">PnL: </span><span className="text-gray-200">{alphaProfile.net_pnl_usdc ? fmtUsd(alphaProfile.net_pnl_usdc) : '—'}</span></div>
                <div><span className="text-gray-500">Volume: </span><span className="text-gray-200">{alphaProfile.total_volume_usdc ? fmtUsd(alphaProfile.total_volume_usdc) : '—'}</span></div>
                <div><span className="text-gray-500">Source: </span><span className="text-gray-200">{alphaProfile.leaderboard_source || '—'}</span></div>
              </div>
            </div>
            {alphaData.note && (
              <p className="text-[10px] text-gray-500">{alphaData.note}</p>
            )}
          </div>
        ) : (
          <p className="text-sm text-gray-500">{data?.reason || 'Wallet profile not available. This wallet may not have enough resolved trades for analysis.'}</p>
        )}
      </div>
    )
  }

  const tier = data.tier || 'unranked'
  const tierNum = tier === 'tier1' || tier === 'tier1h' ? 1 : tier === 'tier2' ? 2 : null
  const isTier1h = tier === 'tier1h'
  const trades = data.recent_trades || []

  return (
    <div className="p-6 space-y-4">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          <Link to="/wallets" className="hover:text-gray-400">Wallet Intel</Link>
          <span className="mx-1">&gt;</span>
          <span className="text-gray-400">Wallet Detail</span>
        </p>
        <h1 className="text-base font-semibold text-gray-200 font-mono">{address}</h1>
      </div>

      {/* Row 1: Identity badges */}
      <div className="flex items-center gap-2">
        {isTier1h ? (
          <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-300" title="High-conviction whale">T1★ HIGH-CONVICTION</span>
        ) : tierNum ? <TierBadge tier={tierNum} /> : (
          <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-gray-800 text-gray-500">UNRANKED</span>
        )}
        {data.wallet_bucket && <BucketBadge bucket={data.wallet_bucket} />}
        {data.primary_category && (
          <span className="px-1.5 py-0.5 rounded text-[9px] bg-surface-hover text-gray-400 capitalize">{data.primary_category}</span>
        )}
        {data.last_trade_ts && (
          <span className="text-[9px] text-gray-600">last active {relTime(data.last_trade_ts)}</span>
        )}
      </div>

      {isTier1h && (
        <div className="bg-amber-500/10 border border-amber-500/30 rounded p-2 text-[10px] text-amber-300">
          <strong>HIGH-CONVICTION TIER</strong> — Qualified by ${(data.net_pnl_usdc / 1000).toFixed(0)}K+ PnL on large position sizes,
          not by trade count. Treated as tier-1 with 1.1× signal confidence multiplier.
        </div>
      )}

      {/* 2026-04-25: Behavioral metrics + insider + z-score panel.
          Sourced from /api/wallet/{addr}/profile which joins
          wallet_behavior_metrics + insider_wallets + wallet_category_zscore.
          Renders only when at least one of the new layers has data. */}
      {profile && (profile.behavior || profile.insider || (profile.zscore_by_category || []).length > 0) && (
        <div className="bg-surface-card rounded-lg p-3 border border-surface-border">
          <p className="text-[10px] text-gray-500 mb-2">INTELLIGENCE LAYERS</p>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 text-[10px]">
            {profile.behavior && (
              <>
                <div>
                  <span className="text-gray-500">copyable-CI: </span>
                  <span className={profile.behavior.is_copyable_ci ? 'text-green-400' : 'text-gray-400'}>
                    {profile.behavior.is_copyable_ci ? '✓ yes (95% LB > 0)' : '— no'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500">farmer flag: </span>
                  <span className={profile.behavior.is_likely_farmer ? 'text-red-400' : 'text-gray-400'}>
                    {profile.behavior.is_likely_farmer ? '⚠ flagged' : '— clean'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500">ROI mean: </span>
                  <span className="text-gray-200 font-mono">
                    {profile.behavior.roi_mean != null ? `${(profile.behavior.roi_mean * 100).toFixed(1)}%` : '—'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500">ROI 95% LB: </span>
                  <span className={(profile.behavior.roi_lower_95 || 0) > 0 ? 'text-green-400 font-mono' : 'text-gray-400 font-mono'}>
                    {profile.behavior.roi_lower_95 != null ? `${(profile.behavior.roi_lower_95 * 100).toFixed(1)}%` : '—'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500">sizing p90: </span>
                  <span className="text-gray-200 font-mono">
                    {profile.behavior.sizing_p90_pct != null ? `${(profile.behavior.sizing_p90_pct * 100).toFixed(1)}%` : '—'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500">cluster: </span>
                  <span className="text-gray-200">#{profile.behavior.cluster_id ?? '—'}</span>
                </div>
                <div>
                  <span className="text-gray-500">cat HHI: </span>
                  <span className="text-gray-200 font-mono">
                    {profile.behavior.category_hhi != null ? profile.behavior.category_hhi.toFixed(2) : '—'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500">primary: </span>
                  <span className="text-gray-200 capitalize">{profile.behavior.primary_category || '—'}</span>
                </div>
              </>
            )}
            {profile.insider && (
              <div className="col-span-2 lg:col-span-4 bg-purple-500/10 border border-purple-500/30 rounded p-2">
                <span className="text-purple-300 font-semibold">INSIDER</span>
                <span className="text-gray-400 ml-3">score {profile.insider.insider_score?.toFixed(2)}</span>
                <span className="text-gray-400 ml-3">via {profile.insider.detection_method}</span>
              </div>
            )}
            {(profile.zscore_by_category || []).filter(z => z.is_specialist_z).length > 0 && (
              <div className="col-span-2 lg:col-span-4">
                <p className="text-gray-500 mb-1">CATEGORY SPECIALTIES (z ≥ 1.0)</p>
                <div className="flex flex-wrap gap-1">
                  {profile.zscore_by_category.filter(z => z.is_specialist_z).slice(0, 8).map((z, i) => (
                    <span key={i} className="px-1.5 py-0.5 rounded text-[9px] bg-blue-500/20 text-blue-300 capitalize">
                      {z.category} z={z.z_score?.toFixed(1)} ({(z.cat_wr * 100).toFixed(0)}% WR / n={z.n_in_cat})
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Row 2: Core metrics */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">DIRECTIONAL WR</p>
          <p className="text-xl font-bold font-mono text-gray-200">{fmtPct(data.directional_win_rate)}</p>
          <p className="text-[9px] text-gray-600">all-time</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">ROLLING WR (20)</p>
          <p className="text-xl font-bold font-mono text-gray-200">{fmtPct(data.rolling_20_wr)}</p>
          <p className="text-[9px] text-gray-600">last 20 trades</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">CONVICTION</p>
          <p className="text-xl font-bold font-mono text-gray-200">{data.conviction_score?.toFixed(3) ?? '—'}</p>
          <p className="text-[9px] text-gray-600">WR × log(resolved)</p>
        </div>
      </div>

      {/* Row 3: Performance */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">TOTAL VOLUME</p>
          <p className="text-xl font-bold font-mono text-gray-200">{fmtUsd(data.total_volume_usdc)}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">NET PnL</p>
          <p className={`text-xl font-bold font-mono ${(data.net_pnl_usdc || 0) >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
            {(data.net_pnl_usdc || 0) >= 0 ? '+' : ''}{fmtUsd(data.net_pnl_usdc)}
          </p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">PROFIT FACTOR</p>
          <p className="text-xl font-bold font-mono text-gray-200">
            {data.profit_factor != null ? `${data.profit_factor.toFixed(1)}x` : '—'}
          </p>
        </div>
      </div>

      {/* Row 4: Detail */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">RESOLVED TRADES</p>
          <p className="text-xl font-bold text-gray-200">{data.resolved_trades ?? '—'}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">AVG WIN</p>
          <p className="text-xl font-bold font-mono text-accent-green">+{fmtUsd(data.avg_win_size_usdc)}</p>
        </div>
        <div className="bg-surface-card rounded-lg p-3">
          <p className="text-[10px] text-gray-500 mb-1">AVG LOSS</p>
          <p className="text-xl font-bold font-mono text-accent-red">-{fmtUsd(data.avg_loss_size_usdc)}</p>
        </div>
      </div>

      {/* Row 5: Category breakdown */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-2">Category Breakdown</h2>
        <CategoryBars breakdown={data.category_breakdown} />
      </div>

      {/* Alpha scores by category — full return distribution */}
      {alphaData?.available && alphaData.categories && Object.keys(alphaData.categories).length > 0 && (
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-2">Alpha Scores by Category</h2>
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-1 pr-2">Category</th>
                <th className="pb-1 pr-2 text-right">WR</th>
                <th className="pb-1 pr-2 text-right">Trades</th>
                <th className="pb-1 pr-2 text-right">Avg Win</th>
                <th className="pb-1 pr-2 text-right">Avg Loss</th>
                <th className="pb-1 pr-2 text-right">PF</th>
                <th className="pb-1 pr-2 text-right">Sharpe</th>
                <th className="pb-1 pr-2 text-right">Kelly</th>
                <th className="pb-1 pr-2">Streak</th>
                <th className="pb-1">Copyable</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {Object.entries(alphaData.categories)
                .sort(([,a], [,b]) => (b.copyability || 0) - (a.copyability || 0))
                .map(([cat, a]) => {
                  const wr = a.win_rate
                  const wrCls = wr >= 0.6 ? 'text-accent-green' : wr >= 0.5 ? 'text-yellow-400' : 'text-accent-red'
                  return (
                    <tr key={cat} className={`hover:bg-surface-hover ${a.is_copyable ? 'bg-accent-green/5' : ''}`}>
                      <td className="py-1.5 pr-2 capitalize text-gray-300">{cat}</td>
                      <td className={`py-1.5 pr-2 text-right font-mono ${wrCls}`}>{fmtPct(wr, 0)}</td>
                      <td className="py-1.5 pr-2 text-right font-mono text-gray-400">{a.resolved_trades}</td>
                      <td className="py-1.5 pr-2 text-right font-mono text-accent-green">
                        {a.avg_win_pnl != null ? `+${fmtUsd(a.avg_win_pnl)}` : '—'}
                      </td>
                      <td className="py-1.5 pr-2 text-right font-mono text-accent-red">
                        {a.avg_loss_pnl != null ? fmtUsd(a.avg_loss_pnl) : '—'}
                      </td>
                      <td className="py-1.5 pr-2 text-right font-mono text-gray-400">
                        {a.profit_factor != null ? `${a.profit_factor.toFixed(1)}x` : '—'}
                      </td>
                      <td className="py-1.5 pr-2 text-right font-mono text-gray-400">
                        {a.sharpe_ratio != null ? a.sharpe_ratio.toFixed(2) : '—'}
                      </td>
                      <td className="py-1.5 pr-2 text-right font-mono text-gray-400">
                        {a.kelly_fraction != null ? `${(a.kelly_fraction * 100).toFixed(1)}%` : '—'}
                      </td>
                      <td className="py-1.5 pr-2 text-gray-400">
                        {a.streak_current != null
                          ? `${a.streak_current > 0 ? 'W' : 'L'}${Math.abs(a.streak_current)}`
                          : '—'}
                        {a.streak_max_win ? ` (best: W${a.streak_max_win})` : ''}
                      </td>
                      <td className="py-1.5">
                        {a.is_copyable
                          ? <span className="px-1.5 py-0.5 rounded text-[8px] font-bold bg-accent-green/20 text-accent-green">YES</span>
                          : <span className="text-gray-600 text-[9px]">no</span>}
                      </td>
                    </tr>
                  )
                })}
            </tbody>
          </table>
        </div>
      )}

      {/* Row 6: PnL chart */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-2">Cumulative P&L</h2>
        <PnlChart history={data.pnl_history} />
        <p className="text-[9px] text-gray-600 mt-1">{(data.pnl_history || []).length} resolved trades</p>
      </div>

      {/* Signals triggered */}
      {data.signals_triggered && Object.keys(data.signals_triggered).length > 0 && (
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-2">Signals Triggered</h2>
          <div className="flex flex-wrap gap-1.5">
            {Object.entries(data.signals_triggered).map(([sig, n]) => (
              <span key={sig} className="flex items-center gap-1">
                <SignalBadge type={sig} />
                <span className="text-[9px] text-gray-500">×{n}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Recent trades */}
      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-2">Recent Trades</h2>
        {!trades.length ? <p className="text-[10px] text-gray-600">No trade history</p> : (
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-surface-border text-gray-500 text-left">
                <th className="pb-1 pr-2">Side</th>
                <th className="pb-1 pr-2">Market</th>
                <th className="pb-1 pr-2 text-right">Size</th>
                <th className="pb-1 pr-2">Outcome</th>
                <th className="pb-1">Category</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {trades.slice(0, 30).map((t, i) => {
                const won = t.won
                const isResolved = t.resolved
                const cid = t.condition_id || t.asset
                return (
                  <tr key={i}
                      className={`hover:bg-surface-hover cursor-pointer border-l-2 ${
                        won === true ? 'border-accent-green' :
                        won === false ? 'border-accent-red' :
                        'border-gray-700'
                      }`}
                      onClick={() => cid && navigate(`/market/${cid}`)}>
                    <td className={`py-1 pr-2 font-bold ${t.side === 'BUY' ? 'text-accent-green' : 'text-accent-red'}`}>{t.side}</td>
                    <td className="py-1 pr-2 text-gray-400 truncate max-w-[300px]">{t.question || '—'}</td>
                    <td className="py-1 pr-2 text-right font-mono text-gray-400">{fmtUsd(t.size)}</td>
                    <td className="py-1 pr-2">
                      {isResolved && t.pnl != null ? (
                        <span className={won ? 'text-accent-green font-bold' : 'text-accent-red font-bold'}>
                          {won ? 'WIN' : 'LOSS'} {t.pnl >= 0 ? '+' : ''}{fmtUsd(t.pnl)}
                        </span>
                      ) : <span className="text-gray-600">OPEN</span>}
                    </td>
                    <td className="py-1 text-gray-500 capitalize">{t.category || '—'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

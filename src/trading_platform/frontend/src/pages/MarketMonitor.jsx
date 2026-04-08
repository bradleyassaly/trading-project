import { useState, useEffect, useRef, useMemo } from 'react'
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  createSeriesMarkers,
} from 'lightweight-charts'
import { api } from '../api/client'
import LoadingSkeleton from '../components/LoadingSkeleton'

// 10-color palette shared with MarketIntelligence so the same wallet has
// the same colour across pages.
const WALLET_COLORS = [
  '#3b82f6', '#00d68f', '#fbbf24', '#ef4444', '#a855f7',
  '#06b6d4', '#f97316', '#ec4899', '#84cc16', '#14b8a6',
]

const SIGNAL_ICONS = {
  cascade: '⚡', whale_entry: '🐋', oversized_bet: '📈',
  market_maker_flip: '🔄', price_velocity: '💥', accumulation: '📦',
  convergence: '🤝', wallet_reversal: '🔁', specialist_entry: '🎓',
  pre_deadline_surge: '⏰', whale_exit: '🚪', position_reduction: '📉',
  no_position_entry: '🩸',
}

const INTERVALS = [
  { id: '1h', label: '1H' },
  { id: '4h', label: '4H' },
  { id: '1d', label: '1D' },
  { id: '1w', label: '1W' },
]

function fmtUsd(n) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1e6) return `${n < 0 ? '-' : ''}$${(Math.abs(n) / 1e6).toFixed(2)}M`
  if (Math.abs(n) >= 1e3) return `${n < 0 ? '-' : ''}$${(Math.abs(n) / 1e3).toFixed(1)}K`
  return `${n < 0 ? '-' : ''}$${Math.abs(n).toFixed(0)}`
}

function fmtTimeAgo(ts) {
  if (!ts) return '—'
  const sec = Math.floor(Date.now() / 1000 - ts)
  if (sec < 60) return `${sec}s ago`
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`
  return `${Math.floor(sec / 86400)}d ago`
}

function fmtDateLabel(ts, interval) {
  if (!ts) return ''
  const d = new Date(ts * 1000)
  if (interval === '1h' || interval === '4h') {
    return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} ${d.getHours()}:00`
  }
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

// ── Color helper for wallets — stable per wallet address
function colorForWallet(addr, indexCache) {
  if (!addr) return WALLET_COLORS[0]
  if (indexCache.has(addr)) return WALLET_COLORS[indexCache.get(addr) % WALLET_COLORS.length]
  const idx = indexCache.size
  indexCache.set(addr, idx)
  return WALLET_COLORS[idx % WALLET_COLORS.length]
}

const CATEGORIES = [
  { id: 'all',         label: 'All',         icon: '🌐' },
  { id: 'politics',    label: 'Politics',    icon: '🗳' },
  { id: 'crypto',      label: 'Crypto',      icon: '₿' },
  { id: 'sports',      label: 'Sports',      icon: '⚽' },
  { id: 'economics',   label: 'Economics',   icon: '📊' },
  { id: 'geopolitics', label: 'Geopolitics', icon: '🌍' },
  { id: 'culture',     label: 'Culture',     icon: '🎭' },
]

const SORTS = [
  { id: 'volume24h', label: 'Top Volume',     icon: '📈' },
  { id: 'trending',  label: 'Trending',       icon: '🔥' },
  { id: 'whale',     label: 'Whale Activity', icon: '🐋' },
  { id: 'new',       label: 'New Markets',    icon: '🆕' },
]

const CAT_ICONS = {
  politics: '🗳', crypto: '₿', sports: '⚽',
  economics: '📊', geopolitics: '🌍', culture: '🎭', other: '🔮',
}

function MarketCard({ market, onClick }) {
  const vol = market.volume_24h || 0
  const volStr =
    vol >= 1e6 ? `$${(vol / 1e6).toFixed(1)}M` :
    vol >= 1e3 ? `$${(vol / 1e3).toFixed(0)}K` :
    vol > 0    ? `$${vol.toFixed(0)}` : '—'
  const cat = market.category || 'other'
  return (
    <div
      onClick={() => onClick(market)}
      className="bg-surface-card border border-surface-border rounded-xl p-4 cursor-pointer hover:border-accent-blue/60 transition-colors"
    >
      <div className="flex items-center justify-between mb-2">
        <span className="text-[10px] text-gray-500 flex items-center gap-1 capitalize">
          <span>{CAT_ICONS[cat] || '🔮'}</span>
          <span>{cat}</span>
        </span>
        {market.whale_count > 0 && (
          <span className="text-[10px] text-accent-blue font-mono">
            🐋 {market.whale_count}
          </span>
        )}
      </div>
      <p
        className="text-[12px] text-gray-200 font-medium leading-snug mb-3"
        style={{ display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}
      >
        {market.question}
      </p>
      <div className="flex items-center justify-between gap-2">
        <span className="text-[10px] text-gray-500 font-mono">{volStr} 24h</span>
        {market.last_price !== null && market.last_price !== undefined && (
          <div className="flex items-center gap-2">
            <div className="w-16 h-1.5 bg-surface-bg rounded-full overflow-hidden">
              <div
                className="h-full bg-accent-blue rounded-full"
                style={{ width: `${Math.min(100, (market.last_price || 0) * 100)}%` }}
              />
            </div>
            <span className="text-[10px] font-mono text-gray-200 w-8 text-right">
              {((market.last_price || 0) * 100).toFixed(0)}%
            </span>
          </div>
        )}
      </div>
      {market.end_date && (
        <div className="mt-2 text-[9px] text-gray-600">
          Closes {new Date(market.end_date).toLocaleDateString()}
        </div>
      )}
    </div>
  )
}

function MarketCardSkeleton() {
  return (
    <div className="bg-surface-card border border-surface-border rounded-xl p-4 animate-pulse">
      <div className="h-3 bg-surface-bg rounded w-20 mb-3" />
      <div className="h-3 bg-surface-bg rounded w-full mb-2" />
      <div className="h-3 bg-surface-bg rounded w-3/4 mb-3" />
      <div className="h-3 bg-surface-bg rounded w-24" />
    </div>
  )
}

// ───────────────────────────────────────────────────────────────────────
// LWChart — TradingView Lightweight Charts (replaces SVG candle/volume)
// ───────────────────────────────────────────────────────────────────────
function LWChart({
  candles, interval, trades, signals, paperTrades, resolution,
  showWhales, showSignals, showPaper, livePrice,
}) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)
  const candleSeriesRef = useRef(null)
  const volumeSeriesRef = useRef(null)
  const markersRef = useRef(null)
  const resolutionLineRef = useRef(null)

  // ── Create chart once ──
  useEffect(() => {
    if (!containerRef.current) return
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 500,
      layout: {
        background: { color: '#0f172a' },
        textColor: '#94a3b8',
        fontSize: 11,
      },
      grid: {
        vertLines: { color: 'rgba(255,255,255,0.04)' },
        horzLines: { color: 'rgba(255,255,255,0.04)' },
      },
      crosshair: { mode: 1 },
      rightPriceScale: {
        borderColor: 'rgba(255,255,255,0.1)',
        scaleMargins: { top: 0.05, bottom: 0.25 },
      },
      timeScale: {
        borderColor: 'rgba(255,255,255,0.1)',
        timeVisible: true,
        secondsVisible: false,
      },
    })
    chartRef.current = chart

    candleSeriesRef.current = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    })

    volumeSeriesRef.current = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    })
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.78, bottom: 0 },
    })

    const ro = new ResizeObserver(() => {
      if (chartRef.current && containerRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth })
      }
    })
    ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      try { chart.remove() } catch {}
      chartRef.current = null
      candleSeriesRef.current = null
      volumeSeriesRef.current = null
      markersRef.current = null
      resolutionLineRef.current = null
    }
  }, [])

  // ── Update timescale options when interval changes ──
  useEffect(() => {
    if (!chartRef.current) return
    chartRef.current.timeScale().applyOptions({
      secondsVisible: interval === '1h',
    })
  }, [interval])

  // ── Push candle + volume data ──
  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current) return
    if (!candles || candles.length === 0) {
      candleSeriesRef.current.setData([])
      volumeSeriesRef.current.setData([])
      return
    }

    // LW Charts requires data ascending by time and uniquely-keyed.
    // Our `t` field is already a Unix seconds timestamp.
    const lwCandles = candles.map(c => ({
      time: c.t,
      open: c.o,
      high: c.h,
      low: c.l,
      close: c.c,
    }))
    candleSeriesRef.current.setData(lwCandles)

    const lwVolume = candles.map(c => ({
      time: c.t,
      value: c.v_total || 0,
      color: c.c >= c.o ? 'rgba(34,197,94,0.5)' : 'rgba(239,68,68,0.5)',
    }))
    volumeSeriesRef.current.setData(lwVolume)

    // Resolution price line
    if (resolutionLineRef.current) {
      try { candleSeriesRef.current.removePriceLine(resolutionLineRef.current) } catch {}
      resolutionLineRef.current = null
    }
    if (resolution != null) {
      resolutionLineRef.current = candleSeriesRef.current.createPriceLine({
        price: resolution,
        color: '#fbbf24',
        lineWidth: 1,
        lineStyle: 2, // dashed
        axisLabelVisible: true,
        title: `Resolved ${resolution >= 0.5 ? 'YES' : 'NO'}`,
      })
    }

    chartRef.current.timeScale().fitContent()
  }, [candles, resolution])

  // ── Markers: whales + signals + paper trades ──
  useEffect(() => {
    if (!candleSeriesRef.current || !candles || candles.length === 0) return
    const minT = candles[0].t
    const maxT = candles[candles.length - 1].t
    const markers = []

    // Wallet color palette — stable per address
    const walletColors = [
      '#06b6d4', '#8b5cf6', '#f59e0b', '#10b981', '#ef4444',
      '#3b82f6', '#ec4899', '#84cc16', '#f97316', '#6366f1',
    ]
    const colorMap = new Map()
    let colorIdx = 0
    const colorOf = (w) => {
      if (!w) return walletColors[0]
      if (!colorMap.has(w)) {
        colorMap.set(w, walletColors[colorIdx % walletColors.length])
        colorIdx++
      }
      return colorMap.get(w)
    }

    // Whale trades
    if (showWhales && trades) {
      const tracked = trades
        .filter(t => t.is_tracked && (t.tier === 'tier1h' || t.tier === 'tier1' || t.tier === 'tier2'))
        .filter(t => t.ts >= minT && t.ts <= maxT)
      // Sort ascending by time so colors are assigned in chronological order
      tracked.sort((a, b) => a.ts - b.ts).forEach(t => {
        const isBuy = (t.side || '').toUpperCase() === 'BUY'
        const sizeBucket = Math.max(1, Math.min(4, Math.round(Math.sqrt((t.size_usd || 0) / 500))))
        markers.push({
          time: t.ts,
          position: isBuy ? 'belowBar' : 'aboveBar',
          shape: isBuy ? 'arrowUp' : 'arrowDown',
          color: colorOf(t.wallet),
          size: sizeBucket,
          text: `${t.pseudonym && !t.pseudonym.startsWith('0x') ? t.pseudonym.slice(0, 10) : (t.wallet || '').slice(0, 6)} $${((t.size_usd || 0) / 1000).toFixed(1)}K`,
        })
      })
    }

    // Signals
    if (showSignals && signals) {
      const signalColors = {
        cascade: '#f59e0b', whale_entry: '#06b6d4', oversized_bet: '#f97316',
        market_maker_flip: '#8b5cf6', price_velocity: '#ef4444',
        convergence: '#10b981', accumulation: '#a855f7',
        wallet_reversal: '#ec4899', specialist_entry: '#84cc16',
      }
      signals.filter(s => s.ts >= minT && s.ts <= maxT).forEach(s => {
        markers.push({
          time: s.ts,
          position: 'aboveBar',
          shape: 'circle',
          color: signalColors[s.signal_type] || '#6b7280',
          size: 1,
          text: `${s.signal_type} ${((s.confidence || 0) * 100).toFixed(0)}%`,
        })
      })
    }

    // Paper trades — entry and exit markers
    if (showPaper && paperTrades) {
      paperTrades.forEach(pt => {
        if (!pt.ts || pt.ts < minT || pt.ts > maxT) return
        const isOpen = pt.exit_ts == null
        const isWin = pt.exit_price != null && pt.entry_price != null && pt.exit_price > pt.entry_price
        const color = isOpen ? '#fbbf24' : (isWin ? '#22c55e' : '#ef4444')
        markers.push({
          time: pt.ts,
          position: 'belowBar',
          shape: 'square',
          color,
          size: 1,
          text: `PAPER ${pt.side} $${(pt.size_usd || 0).toLocaleString()}`,
        })
        if (pt.exit_ts && pt.exit_ts >= minT && pt.exit_ts <= maxT) {
          markers.push({
            time: pt.exit_ts,
            position: 'aboveBar',
            shape: 'square',
            color,
            size: 1,
            text: 'EXIT',
          })
        }
      })
    }

    // LW Charts requires markers sorted ascending by time
    markers.sort((a, b) => a.time - b.time)
    if (markersRef.current) {
      markersRef.current.setMarkers(markers)
    } else {
      markersRef.current = createSeriesMarkers(candleSeriesRef.current, markers)
    }
  }, [candles, trades, signals, paperTrades, showWhales, showSignals, showPaper])

  // ── Live price update — append/update the trailing candle ──
  useEffect(() => {
    if (!candleSeriesRef.current || livePrice == null || !candles || candles.length === 0) return
    const last = candles[candles.length - 1]
    candleSeriesRef.current.update({
      time: last.t,
      open: last.o,
      high: Math.max(last.h, livePrice),
      low: Math.min(last.l, livePrice),
      close: livePrice,
    })
  }, [livePrice])

  return (
    <div ref={containerRef} className="w-full rounded-lg overflow-hidden bg-surface-bg" style={{ height: 500 }} />
  )
}
// OrderBookSidebar — live bid/ask depth + anomalies + market info
// ───────────────────────────────────────────────────────────────────────
function OrderBookSidebar({ conditionId }) {
  const [book, setBook] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const refresh = async () => {
    if (!conditionId) return
    setLoading(true)
    try {
      const d = await api.marketOrderBook(conditionId)
      if (d.available) {
        setBook(d)
        setError(null)
      } else {
        setError(d.error || 'Order book unavailable')
      }
    } catch (e) {
      setError(e.message)
    }
    setLoading(false)
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 60000)
    return () => clearInterval(id)
  }, [conditionId])

  if (!conditionId) return null
  if (loading && !book) return <div className="card text-[10px] text-gray-500">Loading order book…</div>
  if (error) return <div className="card text-[10px] text-gray-500">Order book unavailable</div>
  if (!book) return null

  const imbalance = book.imbalance_ratio || 0.5
  const imbColor = imbalance > 0.65 ? '#22c55e' : imbalance < 0.35 ? '#ef4444' : '#6b7280'
  const maxLevelUsd = Math.max(
    1,
    ...((book.bids || []).slice(0, 10).map(b => b.usd || 0)),
    ...((book.asks || []).slice(0, 10).map(a => a.usd || 0)),
  )

  const Bar = ({ side, price, usd }) => {
    const pct = Math.min(100, (usd / maxLevelUsd) * 100)
    const color = side === 'bid' ? '#22c55e' : '#ef4444'
    const isWall = usd >= 30000
    return (
      <div className="flex items-center text-[10px] gap-1 leading-tight">
        <span className="font-mono w-12 text-right text-gray-400">{price.toFixed(3)}</span>
        <div className="flex-1 relative h-3 bg-surface-bg rounded overflow-hidden">
          <div
            className="absolute inset-y-0 left-0 rounded"
            style={{
              width: `${pct}%`,
              background: color,
              opacity: 0.3,
              outline: isWall ? '1px solid #fbbf24' : 'none',
            }}
          />
        </div>
        <span className="font-mono w-10 text-right text-gray-500">{fmtUsd(usd)}</span>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {/* Imbalance meter */}
      <div className="card">
        <h4 className="text-[10px] text-gray-500 uppercase tracking-wide mb-1.5">Order Book</h4>
        <div className="text-[10px] text-gray-400 mb-1">
          Imbalance: <span style={{ color: imbColor }} className="font-mono font-bold">{(imbalance * 100).toFixed(0)}% buy</span>
        </div>
        <div className="h-1.5 bg-surface-bg rounded overflow-hidden">
          <div className="h-full" style={{ width: `${imbalance * 100}%`, background: imbColor }} />
        </div>
        <div className="text-[9px] text-gray-600 mt-1">
          {fmtUsd(book.bid_depth_usd)} bids · {fmtUsd(book.ask_depth_usd)} asks
        </div>
      </div>

      {/* Depth */}
      <div className="card space-y-0.5">
        {(book.asks || []).slice(0, 8).reverse().map((a, i) => (
          <Bar key={`a-${i}`} side="ask" price={a.price} usd={a.usd} />
        ))}
        <div className="border-y border-surface-border my-1 py-0.5 text-[10px] text-gray-300 font-mono text-center">
          mid {(book.mid_price || 0).toFixed(3)}
        </div>
        {(book.bids || []).slice(0, 8).map((b, i) => (
          <Bar key={`b-${i}`} side="bid" price={b.price} usd={b.usd} />
        ))}
      </div>

      {/* Recent large trades */}
      {book.large_trades_1h && book.large_trades_1h.length > 0 && (
        <div className="card">
          <h4 className="text-[10px] text-gray-500 uppercase tracking-wide mb-1.5">Large Trades (1h)</h4>
          {book.large_trades_1h.slice(0, 5).map((t, i) => (
            <div key={i} className="text-[10px] text-gray-400 truncate">
              <span className={t.side === 'BUY' ? 'text-accent-green' : 'text-accent-red'}>{t.side}</span>
              {' '}{fmtUsd(t.usd)} @ {(t.price * 100).toFixed(0)}¢
              <span className="text-gray-600 ml-1">{fmtTimeAgo(t.timestamp)}</span>
            </div>
          ))}
        </div>
      )}

      {/* Anomalies */}
      {book.recent_anomalies && book.recent_anomalies.length > 0 && (
        <div className="card">
          <h4 className="text-[10px] text-gray-500 uppercase tracking-wide mb-1.5">Anomalies (24h)</h4>
          {book.recent_anomalies.slice(0, 5).map((a, i) => (
            <div key={i} className="text-[10px] mb-1">
              <span className={a.severity === 'critical' ? 'text-accent-red' : 'text-accent-yellow'}>
                {a.severity === 'critical' ? '🔴' : '🟡'}
              </span>{' '}
              <span className="text-gray-300">{a.type}</span>
              <span className="text-gray-600 ml-1">{fmtTimeAgo(a.detected_at)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ───────────────────────────────────────────────────────────────────────
// MarketMonitor — top-level page
// ───────────────────────────────────────────────────────────────────────
export default function MarketMonitor() {
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [searchLoading, setSearchLoading] = useState(false)
  const [selectedMarket, setSelectedMarket] = useState(null)

  // Browse-view state
  const [activeCategory, setActiveCategory] = useState('all')
  const [activeSort, setActiveSort] = useState('volume24h')
  const [topMarkets, setTopMarkets] = useState([])
  const [topMarketsLoading, setTopMarketsLoading] = useState(false)

  const [interval, setInterval_] = useState('1d')
  const [candles, setCandles] = useState(null)
  const [trades, setTrades] = useState(null)
  const [signalsData, setSignalsData] = useState(null)
  const [orderBookForLive, setOrderBookForLive] = useState(null)
  const [loading, setLoading] = useState(false)

  const [showWhales, setShowWhales] = useState(true)
  const [showSignals, setShowSignals] = useState(true)
  const [showPaper, setShowPaper] = useState(true)
  const [showConvergence, setShowConvergence] = useState(true)
  const [liveMode, setLiveMode] = useState(false)

  const livePollRef = useRef(null)
  const debounceRef = useRef(null)

  // Browse: fetch top markets when category/sort change (only when no market selected)
  useEffect(() => {
    if (selectedMarket) return
    setTopMarketsLoading(true)
    api.topMarkets(activeSort, activeCategory === 'all' ? null : activeCategory, 20)
      .then(d => setTopMarkets(d.markets || []))
      .catch(() => setTopMarkets([]))
      .finally(() => setTopMarketsLoading(false))
  }, [activeCategory, activeSort, selectedMarket])

  // Debounced search (>=2 chars). Empty query → fall back to top markets grid.
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    if (searchQuery.length < 2) {
      setSearchResults([])
      return
    }
    debounceRef.current = setTimeout(() => {
      setSearchLoading(true)
      api.marketsSearch({
        q: searchQuery,
        has_whale_activity: true,
        category: activeCategory === 'all' ? undefined : activeCategory,
        limit: 20,
      })
        .then(d => setSearchResults(d.results || []))
        .catch(() => setSearchResults([]))
        .finally(() => setSearchLoading(false))
    }, 300)
    return () => debounceRef.current && clearTimeout(debounceRef.current)
  }, [searchQuery, activeCategory])

  // Load market data when selection or interval changes
  useEffect(() => {
    if (!selectedMarket) return
    setLoading(true)
    Promise.all([
      api.marketCandles(selectedMarket.condition_id, interval),
      api.marketFlow(selectedMarket.condition_id, 500),
      api.marketSignalsAndTrades(selectedMarket.condition_id),
    ])
      .then(([c, t, s]) => {
        setCandles(c)
        setTrades(t)
        setSignalsData(s)
      })
      .catch(() => {
        setCandles(null); setTrades(null); setSignalsData(null)
      })
      .finally(() => setLoading(false))
  }, [selectedMarket, interval])

  // Live polling
  useEffect(() => {
    if (livePollRef.current) {
      clearInterval(livePollRef.current)
      livePollRef.current = null
    }
    setOrderBookForLive(null)
    if (!liveMode || !selectedMarket) return
    const tick = async () => {
      try {
        const d = await api.marketOrderBook(selectedMarket.condition_id)
        if (d.available) {
          const mid = (d.best_bid + d.best_ask) / 2
          setOrderBookForLive({ mid, ts: Date.now() })
          // Stop polling if resolved (price at extreme)
          if (mid <= 0.005 || mid >= 0.995) {
            clearInterval(livePollRef.current)
            livePollRef.current = null
          }
        }
      } catch {}
    }
    tick()
    livePollRef.current = setInterval(tick, 30000)
    return () => livePollRef.current && clearInterval(livePollRef.current)
  }, [liveMode, selectedMarket])

  const selectMarket = (m) => {
    setSelectedMarket(m)
    setCandles(null)
    setTrades(null)
    setSignalsData(null)
  }

  // Resolution from latest signals payload
  const resolution = useMemo(() => {
    if (!signalsData || !signalsData.paper_trades) return null
    const resolved = signalsData.paper_trades.find(p => p.exit_price != null && p.outcome != null)
    return resolved ? resolved.exit_price : null
  }, [signalsData])

  return (
    <div className="p-6 space-y-3">
      <div>
        <p className="text-xs text-gray-600 mb-1">Trading Platform &gt; <span className="text-gray-400">Market Monitor</span></p>
        <h1 className="text-lg font-semibold text-gray-200">Market Monitor</h1>
      </div>

      {/* Search bar (always visible) */}
      <div className="card">
        <input
          value={searchQuery}
          onChange={e => setSearchQuery(e.target.value)}
          placeholder="🔍 Search markets…"
          className="w-full bg-surface-card border border-surface-border rounded px-3 py-2 text-xs text-gray-300"
        />
        {!selectedMarket && searchQuery.length >= 2 && (
          <div className="mt-2 max-h-60 overflow-auto divide-y divide-surface-border">
            {searchLoading && <div className="text-[10px] text-gray-500 py-1">Searching…</div>}
            {!searchLoading && searchResults.length === 0 && (
              <div className="text-[10px] text-gray-500 py-1">No matches</div>
            )}
            {searchResults.map(m => (
              <div
                key={m.condition_id}
                onClick={() => selectMarket(m)}
                className="py-1.5 px-2 hover:bg-surface-hover cursor-pointer flex items-center justify-between gap-3"
              >
                <span className="text-[11px] text-gray-300 truncate flex-1">{m.question}</span>
                <span className="text-[10px] text-gray-500 whitespace-nowrap">
                  <span className="text-accent-blue">{m.whale_count}</span> whales · {m.category || '—'}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Browse view: category tabs + sort tabs + card grid */}
      {!selectedMarket && searchQuery.length < 2 && (
        <>
          {/* Category tabs */}
          <div className="flex gap-1 overflow-x-auto pb-1">
            {CATEGORIES.map(c => (
              <button
                key={c.id}
                onClick={() => setActiveCategory(c.id)}
                className={`flex-shrink-0 px-3 py-1 text-[11px] rounded-full border whitespace-nowrap transition ${
                  activeCategory === c.id
                    ? 'border-accent-blue text-accent-blue bg-accent-blue/10'
                    : 'border-surface-border text-gray-500 hover:text-gray-300'
                }`}
              >
                <span className="mr-1">{c.icon}</span>{c.label}
              </button>
            ))}
          </div>

          {/* Sort tabs */}
          <div className="flex gap-2 flex-wrap">
            {SORTS.map(s => (
              <button
                key={s.id}
                onClick={() => setActiveSort(s.id)}
                className={`px-3 py-1.5 text-[11px] rounded border transition ${
                  activeSort === s.id
                    ? 'border-accent-blue text-accent-blue bg-accent-blue/10'
                    : 'border-surface-border text-gray-500 hover:text-gray-300'
                }`}
              >
                <span className="mr-1">{s.icon}</span>{s.label}
              </button>
            ))}
          </div>

          {/* Card grid */}
          {topMarketsLoading && (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {Array(10).fill(0).map((_, i) => <MarketCardSkeleton key={i} />)}
            </div>
          )}
          {!topMarketsLoading && topMarkets.length === 0 && (
            <p className="text-xs text-gray-500">No markets found for this category.</p>
          )}
          {!topMarketsLoading && topMarkets.length > 0 && (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {topMarkets.map(m => (
                <MarketCard key={m.condition_id} market={m} onClick={selectMarket} />
              ))}
            </div>
          )}
        </>
      )}

      {selectedMarket && (
        <>
          {/* Header */}
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1 min-w-0">
              <button
                onClick={() => { setSelectedMarket(null); setCandles(null); setTrades(null); setSignalsData(null) }}
                className="text-[10px] text-gray-500 hover:text-gray-300 mb-1"
              >← Back to search</button>
              <h2 className="text-sm font-medium text-gray-200 truncate">{selectedMarket.question}</h2>
              <div className="flex gap-2 mt-1 text-[10px]">
                <span className="px-1.5 py-0.5 rounded bg-surface-card text-gray-400 capitalize">{selectedMarket.category || 'unknown'}</span>
                <span className="px-1.5 py-0.5 rounded bg-surface-card text-gray-400">{trades?.tracked_count || 0} tracked</span>
                {liveMode && (
                  <span className="px-1.5 py-0.5 rounded bg-accent-green/20 text-accent-green flex items-center gap-1">
                    <span className="w-1.5 h-1.5 rounded-full bg-accent-green animate-pulse" />
                    Live
                    {orderBookForLive && <span className="text-[9px] opacity-70">{fmtTimeAgo(Math.floor(orderBookForLive.ts / 1000))}</span>}
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Controls */}
          <div className="card flex flex-wrap items-center gap-3">
            <div className="flex gap-1">
              {INTERVALS.map(iv => (
                <button
                  key={iv.id}
                  onClick={() => setInterval_(iv.id)}
                  className={`px-2 py-0.5 text-[10px] rounded border transition ${
                    interval === iv.id
                      ? 'border-accent-blue text-accent-blue bg-accent-blue/10'
                      : 'border-surface-border text-gray-500 hover:text-gray-300'
                  }`}
                >{iv.label}</button>
              ))}
            </div>
            <div className="h-4 w-px bg-surface-border" />
            <div className="flex flex-wrap gap-2 text-[10px]">
              <Toggle label="Whale Trades" value={showWhales} onChange={setShowWhales} />
              <Toggle label="Signals" value={showSignals} onChange={setShowSignals} />
              <Toggle label="Paper Trades" value={showPaper} onChange={setShowPaper} />
              <Toggle label="Convergence" value={showConvergence} onChange={setShowConvergence} />
              <Toggle label="Live" value={liveMode} onChange={setLiveMode} accent />
            </div>
          </div>

          <div className="grid grid-cols-12 gap-3">
            {/* Main chart + volume */}
            <div className="col-span-12 lg:col-span-9 space-y-3">
              {loading && <LoadingSkeleton rows={4} />}
              {!loading && candles && (
                <>
                  <div className="card">
                    <LWChart
                      candles={candles.candles}
                      interval={interval}
                      trades={trades?.trades || []}
                      signals={signalsData?.signals || []}
                      paperTrades={signalsData?.paper_trades || []}
                      resolution={resolution}
                      showWhales={showWhales}
                      showSignals={showSignals}
                      showPaper={showPaper}
                      livePrice={orderBookForLive?.mid}
                    />
                  </div>

                  {/* Whale trade timeline */}
                  {trades && trades.trades.length > 0 && (
                    <div className="card">
                      <h3 className="text-[11px] text-gray-400 mb-1.5">Whale Trades ({trades.tracked_count} tracked / {trades.total_count} total)</h3>
                      <div className="max-h-44 overflow-auto divide-y divide-surface-border">
                        {trades.trades.slice(0, 50).map((t, i) => {
                          const isBuy = (t.side || '').toUpperCase() === 'BUY'
                          const color = isBuy ? 'text-accent-green' : 'text-accent-red'
                          return (
                            <div key={i} className="py-1 px-1 text-[10px] flex items-center gap-2">
                              <span className={`${color} font-mono`}>{isBuy ? '▲' : '▽'}</span>
                              <span className={`${color} font-mono w-14 text-right`}>{fmtUsd(t.size_usd)}</span>
                              <span className="text-gray-500">{t.outcome}</span>
                              <span className="text-gray-500 font-mono">@ {(t.price * 100).toFixed(0)}%</span>
                              <span className="text-gray-400 truncate flex-1">{t.pseudonym && !t.pseudonym.startsWith('0x') ? t.pseudonym : (t.wallet || '').slice(0, 10)}</span>
                              {t.is_tracked && t.tier !== 'unknown' && (
                                <span className="text-[9px] text-accent-blue">[{t.tier}]</span>
                              )}
                              <span className="text-gray-600 text-[9px]">{fmtTimeAgo(t.ts)}</span>
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  )}
                </>
              )}
              {!loading && candles && candles.candles.length === 0 && (
                <div className="card text-xs text-gray-500">No price history available — run history collection for this market.</div>
              )}
            </div>

            {/* Right sidebar */}
            <div className="col-span-12 lg:col-span-3">
              <OrderBookSidebar conditionId={selectedMarket.condition_id} />
            </div>
          </div>
        </>
      )}
    </div>
  )
}

function Toggle({ label, value, onChange, accent }) {
  return (
    <button
      onClick={() => onChange(!value)}
      className={`px-2 py-0.5 rounded border transition ${
        value
          ? accent
            ? 'border-accent-green text-accent-green bg-accent-green/10'
            : 'border-accent-blue text-accent-blue bg-accent-blue/10'
          : 'border-surface-border text-gray-600 hover:text-gray-400'
      }`}
    >
      <span className="mr-1">●</span>{label}
    </button>
  )
}

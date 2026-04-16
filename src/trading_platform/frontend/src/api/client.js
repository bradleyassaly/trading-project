/**
 * API client for the FastAPI backend on port 8001.
 * In dev, Vite proxies /api → http://localhost:8001/api.
 */

const BASE = '/api'

async function get(path) {
  const resp = await fetch(`${BASE}${path}`)
  if (!resp.ok) {
    throw new Error(`GET ${path} → ${resp.status}`)
  }
  return resp.json()
}

async function post(path, body = {}) {
  const resp = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!resp.ok) {
    throw new Error(`POST ${path} → ${resp.status}`)
  }
  return resp.json()
}

export const api = {
  systemStatus: () => get('/system/status'),
  equityCurve: () => get('/pnl/equity-curve'),
  pnlSummary: () => get('/pnl/summary'),
  signalsPerformance: () => get('/signals/performance'),
  signalsCorrelation: () => get('/signals/correlation'),
  signalsBacktestResults: () => get('/signals/backtest-results'),
  kalshiMarkets: () => get('/kalshi/markets'),
  kalshiMarketHistory: (ticker) => get(`/kalshi/market/${encodeURIComponent(ticker)}/history`),
  reasoningTrades: () => get('/reasoning/trades'),
  loopDecisions: () => get('/loop/decisions'),
  loopControl: (action) => post('/loop/control', { action }),
  researchRun: () => post('/research/run'),
  researchStatus: (jobId) => get(`/research/status/${encodeURIComponent(jobId)}`),
  researchDatasets: (params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/datasets${suffix}`)
  },
  researchDatasetDetail: (datasetKey) => get(`/research/datasets/${encodeURIComponent(datasetKey)}`),
  researchDatasetRows: (datasetKey, params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item != null && item !== '') query.append(key, item)
        })
        return
      }
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/datasets/${encodeURIComponent(datasetKey)}/rows${suffix}`)
  },
  researchReplayPreview: (params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item != null && item !== '') query.append(key, item)
        })
        return
      }
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/replay/preview${suffix}`)
  },
  researchReplayConsumerPreview: (params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item != null && item !== '') query.append(key, item)
        })
        return
      }
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/replay/consumer-preview${suffix}`)
  },
  researchReplayEvaluationPreview: (params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item != null && item !== '') query.append(key, item)
        })
        return
      }
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/replay/evaluation-preview${suffix}`)
  },
  researchReplayComparisonLatest: () => get('/research/replay/comparison-latest'),
  researchReplayGatingLatest: () => get('/research/replay/gating-latest'),
  researchReplayReviewQueueLatest: () => get('/research/replay/review-queue-latest'),
  researchReplayDriftLatest: () => get('/research/replay/drift-latest'),
  researchReplayHistory: (params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item != null && item !== '') query.append(key, item)
        })
        return
      }
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/replay/history${suffix}`)
  },
  researchReplayComparisonPreview: (params = {}) => {
    const query = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item != null && item !== '') query.append(key, item)
        })
        return
      }
      if (value != null && value !== '') query.append(key, value)
    })
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return get(`/research/replay/comparison-preview${suffix}`)
  },
  registrySummary: () => get('/ops/registry-summary'),
  providerMonitoring: () => get('/ops/provider-monitoring'),
  providerHealth: () => get('/ops/provider-health'),
  providerDetail: (provider) => get(`/ops/providers/${encodeURIComponent(provider)}`),
  providerTimeline: (provider) => get(`/ops/providers/${encodeURIComponent(provider)}/timeline`),
  providerHistorySummary: (provider) => get(`/ops/providers/${encodeURIComponent(provider)}/history-summary`),
  monitoredDatasetDetail: (datasetKey) => get(`/ops/datasets/${encodeURIComponent(datasetKey)}`),
  monitoredDatasetTimeline: (datasetKey) => get(`/ops/datasets/${encodeURIComponent(datasetKey)}/timeline`),
  monitoredDatasetHistorySummary: (datasetKey) => get(`/ops/datasets/${encodeURIComponent(datasetKey)}/history-summary`),
  paperDashboard: () => get('/paper/dashboard'),
  paperPositionsEnriched: () => get('/paper/positions-enriched'),
  paperCheckResolutions: () => post('/paper/check-resolutions'),
  calibrationStatus: () => get('/calibration/status'),
  calibrationReportLatest: () => get('/calibration/report/latest'),
  calibrationReport: (date) => get(`/calibration/report${date ? `?date=${date}` : ''}`),
  calibrationGenerate: () => post('/calibration/report/generate'),
  calibrationRebalance: (dryRun = false) => post('/calibration/rebalance', { dry_run: dryRun }),
  liveReadiness: () => get('/live/readiness'),
  liveTrades: (limit = 50) => get(`/live/trades?limit=${limit}`),
  liveEmergencyStop: (body = {}) => post('/live/emergency-stop', body),
  liveClearStop: () => post('/live/clear-stop'),
  liveTestDryRun: (signalType = 'price_velocity') => post('/live/test-dry-run', { signal_type: signalType }),
  paperPortfolio: () => get('/paper/portfolio'),
  paperTrades: () => get('/paper/trades'),
  paperScan: () => get('/paper/scan'),
  polymarketLiveMarkets: () => get('/polymarket/live-markets'),
  polymarketMarketTicks: (marketId) => get(`/polymarket/market-ticks/${encodeURIComponent(marketId)}`),
  smartMoneyWallets: () => get('/smart-money/wallets'),
  smartMoneySignals: () => get('/smart-money/signals'),
  smartMoneyMirror: () => get('/smart-money/mirror'),
  smartMoneyWalletDetail: (addr) => get(`/smart-money/wallet/${encodeURIComponent(addr)}`),
  smartMoneyActionableSignals: () => get('/smart-money/actionable-signals'),
  smartMoneyLeaderboard: () => get('/smart-money/leaderboard'),
  smartMoneyAlerts: (params = {}) => {
    const q = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => { if (v != null) q.append(k, v) })
    return get(`/smart-money/alerts${q.toString() ? '?' + q.toString() : ''}`)
  },
  smartMoneyWinners: (window = 'all') => get(`/smart-money/winners?window=${window}`),
  smartMoneyOpenPositions: () => get('/smart-money/open-positions'),
  smartMoneyUniverseStats: () => get('/smart-money/universe-stats'),
  paperBankroll: () => get('/paper/bankroll'),
  pipelineFunnel: (hours = 24) => get(`/pipeline/funnel?hours=${hours}`),
  tradeJournal: (limit = 20) => get(`/trade-journal?limit=${limit}`),
  exitAnalyzer: (minSample = 10) => get(`/exit-analyzer?min_sample=${minSample}`),
  signalAttribution: (hours = 168) => get(`/signal-attribution?hours=${hours}`),
  paperPnlHistory: () => get('/paper/pnl-history'),
  signalsPerformance: () => get('/signals/performance'),
  smartMoneyWalletPositions: (addr) => get(`/smart-money/wallet/${encodeURIComponent(addr)}/positions`),
  smartMoneyWalletTrades: (addr, page = 1) => get(`/smart-money/wallet/${encodeURIComponent(addr)}/trades?page=${page}&limit=50`),
  intelligenceHealth: () => get('/system/intelligence-health'),
  backtestRun: (config) => post('/backtest/run', config),
  backtestWalletPositions: (wallet) => get(`/backtest/wallet/${encodeURIComponent(wallet)}/positions`),
  backtestPriceHistory: (cid) => get(`/backtest/market/${encodeURIComponent(cid)}/price-history`),
  backtestWalletMarket: (wallet, cid) => get(`/backtest/wallet/${encodeURIComponent(wallet)}/market/${encodeURIComponent(cid)}`),
  backtestConvergence: (config) => post('/backtest/convergence', config),
  marketIntelligence: (cid) => get(`/market/${encodeURIComponent(cid)}/intelligence`),
  topMarkets: (sort = 'volume24h', category = null, limit = 20) => {
    const q = new URLSearchParams({ sort, limit })
    if (category) q.append('category', category)
    return get(`/markets/top?${q.toString()}`)
  },
  marketCandles: (cid, interval = '1d', fromTs = null, toTs = null) => {
    const q = new URLSearchParams({ interval })
    if (fromTs != null) q.append('from_ts', fromTs)
    if (toTs != null) q.append('to_ts', toTs)
    return get(`/market/${encodeURIComponent(cid)}/candles?${q.toString()}`)
  },
  marketFlow: (cid, limit = 300) => get(`/market/${encodeURIComponent(cid)}/flow?limit=${limit}`),
  marketSignalsAndTrades: (cid) => get(`/market/${encodeURIComponent(cid)}/signals`),
  marketOrderBook: (cid) => get(`/market/${encodeURIComponent(cid)}/order-book`),
  anomalyAlerts: (params = {}) => {
    const q = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => { if (v != null && v !== '') q.append(k, v) })
    const suffix = q.toString() ? `?${q.toString()}` : ''
    return get(`/alerts/anomalies${suffix}`)
  },
  marketsSearch: (params = {}) => {
    const q = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => { if (v != null && v !== '') q.append(k, v) })
    const suffix = q.toString() ? `?${q.toString()}` : ''
    return get(`/markets/search${suffix}`)
  },
  pipelineStatus: () => get('/system/pipeline-status'),
  pipelineRuns: () => get('/system/pipeline-runs'),
  runPipeline: () => post('/system/run-pipeline'),
  executionPolicy: () => get('/system/execution-policy'),
  setExecutionPolicy: (policy) => post('/system/execution-policy', { policy }),
  polymarketWhaleFeed: () => get('/polymarket/whale-feed'),
  polymarketSubscriptionStatus: () => get('/polymarket/subscription-status'),
  polymarketSignalsFeed: () => get('/polymarket/signals-feed'),
  polymarketCategoryPerformance: () => get('/polymarket/category-performance'),
  livePositions: () => get('/live/positions'),
  liveHistory: (limit = 50) => get(`/live/history?limit=${limit}`),
  liveExecutionQuality: () => get('/live/execution-quality'),
  systemHealth: () => get('/system/health'),
  killSwitch: () => get('/system/kill-switch'),
  killSwitchTrip: (reason) => post('/system/kill-switch/trip', { reason }),
  killSwitchReset: () => post('/system/kill-switch/reset'),
  signalLiveReadiness: () => get('/signals/live-readiness'),
  insidersList: () => get('/insiders/list'),
}

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
  paperPnlHistory: () => get('/paper/pnl-history'),
  signalsPerformance: () => get('/signals/performance'),
  smartMoneyWalletPositions: (addr) => get(`/smart-money/wallet/${encodeURIComponent(addr)}/positions`),
  smartMoneyWalletTrades: (addr, page = 1) => get(`/smart-money/wallet/${encodeURIComponent(addr)}/trades?page=${page}&limit=50`),
}

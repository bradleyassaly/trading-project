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
}

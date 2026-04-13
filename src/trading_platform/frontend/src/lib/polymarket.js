/**
 * Client-side Polymarket API calls with a Map-backed 60s TTL cache.
 *
 * These hit Polymarket's public APIs directly from the browser so the
 * dashboard can show live order-book depth, price history, and user
 * profiles without routing through our backend.
 *
 * Every call is cached for 60s. Errors are swallowed and logged so a
 * flaky third-party API never crashes the dashboard.
 */

const GAMMA_BASE = 'https://gamma-api.polymarket.com'
const CLOB_BASE = 'https://clob.polymarket.com'
const TTL_MS = 60_000

/** @type {Map<string, {data: any, expiresAt: number}>} */
const cache = new Map()

/**
 * Generic cached fetch. Returns null on error (never throws).
 * @param {string} url
 * @param {string} cacheKey
 * @param {AbortSignal} [signal]
 */
async function cachedFetch(url, cacheKey, signal) {
  const now = Date.now()
  const hit = cache.get(cacheKey)
  if (hit && hit.expiresAt > now) return hit.data

  try {
    const resp = await fetch(url, { signal })
    if (!resp.ok) {
      console.warn(`[polymarket] ${url} → ${resp.status}`)
      return hit?.data ?? null // return stale if available
    }
    const data = await resp.json()
    cache.set(cacheKey, { data, expiresAt: now + TTL_MS })
    return data
  } catch (err) {
    if (err.name === 'AbortError') return null
    console.warn(`[polymarket] ${url} failed:`, err.message)
    return hit?.data ?? null
  }
}

/**
 * Polymarket user profile from Gamma API.
 * Returns { username, profileImage, proxyWallet, ... } or null.
 */
export function pmGetUser(address, signal) {
  if (!address) return Promise.resolve(null)
  const addr = address.toLowerCase()
  return cachedFetch(
    `${GAMMA_BASE}/users?address=${addr}`,
    `user:${addr}`,
    signal,
  )
}

/**
 * CLOB order book for a token.
 * Returns { bids: [[price, size], ...], asks: [[price, size], ...] } or null.
 */
export function pmGetBook(tokenId, signal) {
  if (!tokenId) return Promise.resolve(null)
  return cachedFetch(
    `${CLOB_BASE}/book?token_id=${encodeURIComponent(tokenId)}`,
    `book:${tokenId}`,
    signal,
  )
}

/**
 * CLOB price history for a market.
 * @param {string} tokenId - the CLOB token ID
 * @param {string} [interval='1d'] - e.g. '1h', '1d'
 * @param {string} [fidelity='60'] - number of data points
 */
export function pmGetPriceHistory(tokenId, interval = '1d', fidelity = '60', signal) {
  if (!tokenId) return Promise.resolve(null)
  const params = new URLSearchParams({ market: tokenId, interval, fidelity })
  return cachedFetch(
    `${CLOB_BASE}/prices-history?${params}`,
    `prices:${tokenId}:${interval}:${fidelity}`,
    signal,
  )
}

/**
 * Evict all cached entries. Useful after navigation or manual refresh.
 */
export function pmClearCache() {
  cache.clear()
}

/**
 * Evict expired entries only (call periodically if memory is a concern).
 */
export function pmPruneCache() {
  const now = Date.now()
  for (const [key, entry] of cache) {
    if (entry.expiresAt <= now) cache.delete(key)
  }
}

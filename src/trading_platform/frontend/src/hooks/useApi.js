import { useState, useEffect, useCallback, useRef } from 'react'

/**
 * Generic data-fetching hook.
 * @param {() => Promise<any>} fetcher  — API call function
 * @param {number} [pollMs]            — auto-refresh interval in milliseconds (0 = no polling)
 */
export function useApi(fetcher, pollMs = 0) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const timerRef = useRef(null)

  const load = useCallback(async () => {
    try {
      const result = await fetcher()
      setData(result)
      setError(null)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [fetcher])

  useEffect(() => {
    setLoading(true)
    load()

    if (pollMs > 0) {
      timerRef.current = setInterval(load, pollMs)
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [load, pollMs])

  return { data, loading, error, refresh: load }
}

from trading_platform.polymarket.db_connection import get_connection
conn = get_connection()
rows = conn.execute("""
    SELECT condition_id, slug, outcome_prices, volume_24h
    FROM markets
    WHERE yes_token_id IS NOT NULL
      AND end_date_iso::timestamptz > NOW() + interval '2 days'
      AND CAST(volume_24h AS FLOAT) > 5000
    ORDER BY CAST(volume_24h AS FLOAT) DESC NULLS LAST
    LIMIT 8
""").fetchall()
conn.close()
for cid, slug, prices, vol in rows:
    import json
    p = json.loads(prices) if isinstance(prices, str) else (prices or [])
    yes_p = float(p[0]) if p else 0
    print(f"  condition_id: {cid}")
    print(f"  slug:         {(slug or '')[:50]}")
    print(f"  yes_price:    {yes_p:.2f}  vol_24h: ${float(vol or 0):,.0f}")
    print()

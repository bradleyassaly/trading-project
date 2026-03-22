from __future__ import annotations

from trading_platform.config.loader import load_broker_config
from trading_platform.broker.service import resolve_broker_adapter


def cmd_broker_cancel_all(args) -> None:
    config = load_broker_config(args.broker_config)
    if getattr(args, "broker", None):
        config = config.__class__(**{**config.to_dict(), "broker_name": args.broker})
    adapter = resolve_broker_adapter(config)
    results = adapter.cancel_all_orders()
    print(f"Broker: {config.broker_name}")
    print(f"Cancelled orders: {len(results)}")
    for row in results:
        print(f"  {row.symbol or '<unknown>'}: {row.status} {row.message or ''}".rstrip())

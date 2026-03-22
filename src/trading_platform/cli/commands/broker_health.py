from __future__ import annotations

from trading_platform.config.loader import load_broker_config
from trading_platform.broker.service import resolve_broker_adapter


def cmd_broker_health(args) -> None:
    config = load_broker_config(args.broker_config)
    if getattr(args, "broker", None):
        config = config.__class__(**{**config.to_dict(), "broker_name": args.broker})
    adapter = resolve_broker_adapter(config)
    healthy, message = adapter.health_check()
    account = adapter.get_account_snapshot() if healthy else None
    print(f"Broker: {config.broker_name}")
    print(f"Healthy: {healthy}")
    print(f"Message: {message}")
    if account is not None:
        print(f"Account ID: {account.account_id}")
        print(f"Equity: {account.equity:,.2f}")

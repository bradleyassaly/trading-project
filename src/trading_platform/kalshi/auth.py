"""
Kalshi API authentication.

Kalshi uses RSA-PSS signed requests (NOT Bearer tokens).
Every authenticated request must include three headers:
  KALSHI-ACCESS-KEY        — your API Key ID (UUID)
  KALSHI-ACCESS-TIMESTAMP  — current Unix timestamp in milliseconds
  KALSHI-ACCESS-SIGNATURE  — base64-encoded RSA-PSS/SHA-256 signature

The message signed is:  {timestamp_ms}{HTTP_METHOD}{path_without_query_params}

Getting API keys
----------------
Demo:  https://demo.kalshi.com  →  Profile → API Keys → Create New API Key
Live:  https://kalshi.com       →  Profile → API Keys → Create New API Key
       (requires US KYC: government ID + SSN + bank account)

The private key is shown ONCE at creation — save it immediately.
"""
from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except ImportError:
    hashes = None  # type: ignore[assignment]
    serialization = None  # type: ignore[assignment]
    padding = None  # type: ignore[assignment]


LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
LIVE_WS_BASE = "wss://api.elections.kalshi.com"
DEMO_WS_BASE = "wss://demo-api.kalshi.co"


@dataclass(frozen=True)
class KalshiConfig:
    api_key_id: str
    private_key_pem: str  # PEM-encoded RSA private key (the full -----BEGIN... block)
    demo: bool = True

    @property
    def base_url(self) -> str:
        return DEMO_BASE_URL if self.demo else LIVE_BASE_URL

    @property
    def ws_base(self) -> str:
        return DEMO_WS_BASE if self.demo else LIVE_WS_BASE

    @classmethod
    def from_env(cls) -> "KalshiConfig":
        """
        Load config from environment variables:
          KALSHI_API_KEY_ID        — required
          KALSHI_PRIVATE_KEY_PEM   — inline PEM (use \\n for newlines in .env)
          KALSHI_PRIVATE_KEY_PATH  — path to .pem file (alternative to above)
          KALSHI_DEMO              — "true" (default) or "false"
        """
        api_key_id = os.getenv("KALSHI_API_KEY_ID")
        private_key_pem = os.getenv("KALSHI_PRIVATE_KEY_PEM")
        private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
        demo_val = os.getenv("KALSHI_DEMO", "true").strip().lower()

        if not api_key_id:
            raise ValueError("Missing required env var: KALSHI_API_KEY_ID")

        if private_key_pem:
            pem = private_key_pem.replace("\\n", "\n")
        elif private_key_path:
            with open(private_key_path) as f:
                pem = f.read()
        else:
            raise ValueError(
                "Set KALSHI_PRIVATE_KEY_PEM (inline PEM) or KALSHI_PRIVATE_KEY_PATH (file path)."
            )

        demo = demo_val in {"1", "true", "yes", "y"}
        return cls(api_key_id=api_key_id, private_key_pem=pem, demo=demo)


def _load_private_key(pem: str):
    if serialization is None:
        raise ImportError(
            "The 'cryptography' package is required for Kalshi auth.\n"
            "Install it: pip install cryptography"
        )
    return serialization.load_pem_private_key(pem.encode(), password=None)


def build_auth_headers(config: KalshiConfig, method: str, path: str) -> dict[str, str]:
    """
    Return the three Kalshi auth headers for a given HTTP method + path.

    :param config: KalshiConfig with api_key_id and private_key_pem.
    :param method: HTTP verb, e.g. "GET" or "POST".
    :param path:   URL path including query string (query is stripped before signing).
    """
    if padding is None or hashes is None:
        raise ImportError(
            "The 'cryptography' package is required for Kalshi auth.\n"
            "Install it: pip install cryptography"
        )

    timestamp_ms = str(int(time.time() * 1000))
    path_no_query = path.split("?")[0]
    message = f"{timestamp_ms}{method.upper()}{path_no_query}".encode()

    private_key = _load_private_key(config.private_key_pem)
    signature_bytes = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": config.api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature_bytes).decode(),
    }

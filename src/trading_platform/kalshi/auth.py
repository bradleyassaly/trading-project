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
from pathlib import Path
from typing import Any, Mapping

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
    private_key_pem: str | None = None  # PEM-encoded RSA private key (the full -----BEGIN... block)
    demo: bool = True
    private_key_path: str | None = None

    @property
    def base_url(self) -> str:
        return DEMO_BASE_URL if self.demo else LIVE_BASE_URL

    @property
    def ws_base(self) -> str:
        return DEMO_WS_BASE if self.demo else LIVE_WS_BASE

    def resolved_private_key_pem(self) -> str:
        return _resolve_private_key_pem(
            private_key_pem=self.private_key_pem,
            private_key_path=self.private_key_path,
            source_label="KalshiConfig",
        )

    @classmethod
    def from_mapping(
        cls,
        mapping: Mapping[str, Any] | None,
        *,
        env_fallback: bool = False,
        demo: bool | None = None,
        allow_missing: bool = False,
        source_label: str = "Kalshi config",
    ) -> "KalshiConfig | None":
        raw = dict(mapping or {})
        env_demo_val = os.getenv("KALSHI_DEMO", "true").strip().lower()
        env_demo = env_demo_val in {"1", "true", "yes", "y"}

        api_key_id = raw.get("api_key_id")
        private_key_pem = raw.get("private_key_pem")
        private_key_path = raw.get("private_key_path")

        if env_fallback:
            api_key_id = api_key_id or os.getenv("KALSHI_API_KEY_ID")
            private_key_pem = private_key_pem or os.getenv("KALSHI_PRIVATE_KEY_PEM")
            private_key_path = private_key_path or os.getenv("KALSHI_PRIVATE_KEY_PATH")

        if not api_key_id and not private_key_pem and not private_key_path:
            if allow_missing:
                return None
            raise ValueError(f"{source_label}: missing Kalshi auth. Set api_key_id and either private_key_pem or private_key_path.")

        if not api_key_id:
            raise ValueError(f"{source_label}: missing Kalshi api_key_id / KALSHI_API_KEY_ID.")

        resolved_demo = env_demo if demo is None else demo
        return cls(
            api_key_id=str(api_key_id),
            private_key_pem=_resolve_private_key_pem(
                private_key_pem=private_key_pem,
                private_key_path=private_key_path,
                source_label=source_label,
            ),
            demo=bool(resolved_demo),
            private_key_path=str(private_key_path) if private_key_path else None,
        )

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
        demo = demo_val in {"1", "true", "yes", "y"}
        if not api_key_id:
            raise ValueError("Missing required env var: KALSHI_API_KEY_ID")
        if not private_key_pem and not private_key_path:
            raise ValueError(
                "Set KALSHI_PRIVATE_KEY_PEM (inline PEM) or KALSHI_PRIVATE_KEY_PATH (file path)."
            )
        config = cls.from_mapping(
            {
                "api_key_id": api_key_id,
                "private_key_pem": private_key_pem,
                "private_key_path": private_key_path,
            },
            demo=demo,
            source_label="Environment",
        )
        if config is None:
            raise ValueError(
                "Environment: missing Kalshi auth. Set KALSHI_API_KEY_ID and either KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH."
            )
        return config


def _resolve_private_key_pem(
    *,
    private_key_pem: Any,
    private_key_path: Any,
    source_label: str,
) -> str:
    pem_text = str(private_key_pem).strip() if private_key_pem is not None else ""
    path_text = str(private_key_path).strip() if private_key_path is not None else ""

    if pem_text:
        if "BEGIN " not in pem_text and ("\n" not in pem_text and "\r" not in pem_text):
            candidate_path = Path(pem_text).expanduser()
            if candidate_path.exists() or candidate_path.suffix.lower() in {".pem", ".key"}:
                raise ValueError(
                    f"{source_label}: private_key_pem looks like a file path ({candidate_path}). "
                    "Use private_key_path for file-based keys."
                )
        return pem_text.replace("\\n", "\n")

    if path_text:
        key_path = Path(path_text).expanduser()
        if not key_path.exists():
            raise ValueError(f"{source_label}: private_key_path does not exist: {key_path}")
        try:
            return key_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"{source_label}: could not read private_key_path {key_path}: {exc}") from exc

    raise ValueError(
        f"{source_label}: missing private key material. Provide private_key_pem or private_key_path."
    )


def _load_private_key(pem: str):
    if serialization is None:
        raise ImportError(
            "The 'cryptography' package is required for Kalshi auth.\n"
            "Install it: pip install cryptography"
        )
    try:
        return serialization.load_pem_private_key(pem.encode(), password=None)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Unable to load Kalshi private key. Ensure private_key_pem contains the full PEM text "
            "including BEGIN/END lines, or use private_key_path for a PEM file path."
        ) from exc


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

    private_key = _load_private_key(config.resolved_private_key_pem())
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

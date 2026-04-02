"""Tests for Kalshi RSA-PSS auth header generation."""
from __future__ import annotations

import base64
import time
from unittest.mock import patch

import pytest

from trading_platform.kalshi.auth import KalshiConfig, build_auth_headers


def _generate_test_pem() -> str:
    """Generate a throwaway RSA-2048 key for use in tests."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture(scope="module")
def test_pem() -> str:
    pytest.importorskip("cryptography")
    return _generate_test_pem()


def test_kalshi_config_demo_urls(test_pem):
    config = KalshiConfig(api_key_id="test-key", private_key_pem=test_pem, demo=True)
    assert "demo" in config.base_url
    assert "demo" in config.ws_base


def test_kalshi_config_live_urls(test_pem):
    config = KalshiConfig(api_key_id="test-key", private_key_pem=test_pem, demo=False)
    assert "demo" not in config.base_url
    assert "elections.kalshi.com" in config.base_url


def test_kalshi_config_from_env_missing_key(monkeypatch):
    monkeypatch.delenv("KALSHI_API_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PEM", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(ValueError, match="KALSHI_API_KEY_ID"):
        KalshiConfig.from_env()


def test_kalshi_config_from_env_missing_key_material(monkeypatch):
    monkeypatch.setenv("KALSHI_API_KEY_ID", "some-uuid")
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PEM", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(ValueError, match="KALSHI_PRIVATE_KEY"):
        KalshiConfig.from_env()


def test_kalshi_config_from_mapping_accepts_raw_pem(test_pem):
    config = KalshiConfig.from_mapping(
        {
            "api_key_id": "my-key-id",
            "private_key_pem": test_pem.replace("\n", "\\n"),
        },
        demo=False,
        source_label="test mapping",
    )
    assert config is not None
    assert config.api_key_id == "my-key-id"
    assert "BEGIN RSA PRIVATE KEY" in config.private_key_pem
    assert config.demo is False


def test_kalshi_config_from_mapping_accepts_private_key_path(tmp_path, test_pem):
    key_path = tmp_path / "kalshi.pem"
    key_path.write_text(test_pem, encoding="utf-8")

    config = KalshiConfig.from_mapping(
        {
            "api_key_id": "my-key-id",
            "private_key_path": str(key_path),
        },
        source_label="test mapping",
    )
    assert config is not None
    assert config.private_key_path == str(key_path)
    assert config.private_key_pem == test_pem


def test_build_auth_headers_malformed_pem_fails_clearly():
    config = KalshiConfig(api_key_id="my-key-id", private_key_pem="not-a-valid-pem", demo=False)
    with pytest.raises(ValueError, match="Unable to load Kalshi private key"):
        build_auth_headers(config, "GET", "/markets")


def test_kalshi_config_rejects_file_path_in_private_key_pem(tmp_path):
    key_path = tmp_path / "kalshi.pem"
    key_path.write_text("dummy", encoding="utf-8")

    with pytest.raises(ValueError, match="private_key_pem looks like a file path"):
        KalshiConfig.from_mapping(
            {
                "api_key_id": "my-key-id",
                "private_key_pem": str(key_path),
            },
            source_label="test mapping",
        )


def test_kalshi_config_from_env_rejects_file_path_in_private_key_pem(tmp_path, monkeypatch):
    key_path = tmp_path / "kalshi.pem"
    key_path.write_text("dummy", encoding="utf-8")
    monkeypatch.setenv("KALSHI_API_KEY_ID", "env-key-id")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PEM", str(key_path))
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)

    with pytest.raises(ValueError, match="private_key_pem looks like a file path"):
        KalshiConfig.from_env()


def test_build_auth_headers_structure(test_pem):
    config = KalshiConfig(api_key_id="my-key-id", private_key_pem=test_pem, demo=True)
    headers = build_auth_headers(config, "GET", "/trade-api/v2/markets?status=open")

    assert headers["KALSHI-ACCESS-KEY"] == "my-key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"].isdigit()
    sig = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
    assert len(sig) == 256  # 2048-bit RSA signature = 256 bytes


def test_build_auth_headers_strips_query_from_path(test_pem):
    """The path signed must not include query parameters."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    config = KalshiConfig(api_key_id="k", private_key_pem=test_pem, demo=True)
    private_key = serialization.load_pem_private_key(test_pem.encode(), password=None)
    public_key = private_key.public_key()

    with patch("trading_platform.kalshi.auth.time") as mock_time:
        mock_time.time.return_value = 1700000000.0
        h = build_auth_headers(config, "GET", "/markets?foo=bar&baz=qux")

    ts = h["KALSHI-ACCESS-TIMESTAMP"]
    sig = base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"])
    expected_message = f"{ts}GET/markets".encode()

    # Must not raise — validates the signature was produced against the stripped path
    public_key.verify(
        sig,
        expected_message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )


def test_build_auth_headers_timestamp_recency(test_pem):
    config = KalshiConfig(api_key_id="k", private_key_pem=test_pem, demo=True)
    before = int(time.time() * 1000)
    headers = build_auth_headers(config, "POST", "/portfolio/orders")
    after = int(time.time() * 1000)

    ts = int(headers["KALSHI-ACCESS-TIMESTAMP"])
    assert before <= ts <= after

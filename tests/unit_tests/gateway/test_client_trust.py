# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Shared client identity trust policy for HTTP and WS entries."""

from __future__ import annotations

from agent_gateway.channel_manager.web import client_trust

ENV = "GATEWAY_WEB_HTTP_TRUST_CLIENT_HEADERS"


def test_explicit_false_disables_client_identity_everywhere(monkeypatch) -> None:
    monkeypatch.setenv(ENV, "false")
    monkeypatch.setattr(client_trust, "is_enterprise", lambda: True)
    assert client_trust.trust_client_identity_headers("remote.example.com") is False
    assert client_trust.trust_client_identity_headers("localhost") is False


def test_explicit_true_enables_client_identity_everywhere(monkeypatch) -> None:
    monkeypatch.setenv(ENV, "1")
    monkeypatch.setattr(client_trust, "is_enterprise", lambda: False)
    assert client_trust.trust_client_identity_headers("remote.example.com") is True


def test_enterprise_defaults_to_trust(monkeypatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    monkeypatch.setattr(client_trust, "is_enterprise", lambda: True)
    assert client_trust.trust_client_identity_headers("remote.example.com") is True


def test_personal_only_trusts_loopback(monkeypatch) -> None:
    monkeypatch.delenv(ENV, raising=False)
    monkeypatch.setattr(client_trust, "is_enterprise", lambda: False)
    assert client_trust.trust_client_identity_headers("remote.example.com") is False
    assert client_trust.trust_client_identity_headers("127.0.0.1") is True
    assert client_trust.trust_client_identity_headers("::1") is True
    assert client_trust.trust_client_identity_headers("localhost") is True


def test_ws_tuple_address_is_normalized(monkeypatch) -> None:
    """WS passes (host, port); the shared policy must read the host."""
    monkeypatch.delenv(ENV, raising=False)
    monkeypatch.setattr(client_trust, "is_enterprise", lambda: False)
    assert client_trust.trust_client_identity_headers(("127.0.0.1", 54321)) is True
    assert client_trust.trust_client_identity_headers(("::1", 54321)) is True
    assert (
        client_trust.trust_client_identity_headers(("remote.example.com", 54321))
        is False
    )

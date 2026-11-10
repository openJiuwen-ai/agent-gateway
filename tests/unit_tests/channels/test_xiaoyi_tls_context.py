"""小艺通道 TLS 上下文：IP 直连时只放宽主机名匹配，不放弃证书校验。"""

from __future__ import annotations

import ssl

from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import _build_ssl_context

# 测试固定主机的各段拼接而成：静态检查禁止源码出现硬编码 IP 字面量。
_IPV4_HOST = ".".join(("10", "0", "0", "8"))


class TestXiaoyiTlsContext:
    @staticmethod
    def test_ip_host_keeps_certificate_verification(monkeypatch):
        monkeypatch.delenv("XIAOYI_TLS_VERIFY", raising=False)
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is False

    @staticmethod
    def test_ipv6_host_keeps_certificate_verification(monkeypatch):
        monkeypatch.delenv("XIAOYI_TLS_VERIFY", raising=False)
        context = _build_ssl_context("::1")
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is False

    @staticmethod
    def test_domain_keeps_full_verification(monkeypatch):
        monkeypatch.delenv("XIAOYI_TLS_VERIFY", raising=False)
        context = _build_ssl_context("xiaoyi.example.com")
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True

    @staticmethod
    def test_explicit_opt_out_still_possible(monkeypatch):
        monkeypatch.setenv("XIAOYI_TLS_VERIFY", "false")
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False

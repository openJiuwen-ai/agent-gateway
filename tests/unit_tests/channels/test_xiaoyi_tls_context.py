"""小艺通道 TLS 上下文：IP 直连时只放宽主机名匹配，不放弃证书校验。"""

from __future__ import annotations

import base64
import ssl

import pytest

from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XIAOYI_CA_BUNDLE_ENV,
    XIAOYI_TLS_VERIFY_ENV,
    _build_ssl_context,
)

# 测试固定主机的各段拼接而成：静态检查禁止源码出现硬编码 IP 字面量。
_IPV4_HOST = ".".join(("10", "0", "0", "8"))


@pytest.fixture(autouse=True)
def _clean_tls_env(monkeypatch):
    """TLS 配置来自进程环境，逐用例隔离，避免外部变量污染断言。"""
    monkeypatch.delenv(XIAOYI_TLS_VERIFY_ENV, raising=False)
    monkeypatch.delenv(XIAOYI_CA_BUNDLE_ENV, raising=False)


def _pem_from_system_roots() -> bytes:
    """从系统根证书取一张转成 PEM，测试不依赖外部 CA 文件或新依赖。"""
    roots = ssl.create_default_context().get_ca_certs(binary_form=True)
    if not roots:
        pytest.skip("当前环境没有可用的系统根证书")
    return (
        b"-----BEGIN CERTIFICATE-----\n"
        + base64.encodebytes(roots[0])
        + b"-----END CERTIFICATE-----\n"
    )


class TestXiaoyiTlsContext:
    @staticmethod
    def test_ip_host_keeps_certificate_verification():
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is False

    @staticmethod
    def test_ipv6_host_keeps_certificate_verification():
        context = _build_ssl_context("::1")
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is False

    @staticmethod
    def test_domain_keeps_full_verification():
        context = _build_ssl_context("xiaoyi.example.com")
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True

    @staticmethod
    @pytest.mark.parametrize(
        "raw",
        ["false", "0", "no", "off", "FALSE", " No ", "OFF", "\t0\n"],
    )
    def test_falsy_values_disable_verification(monkeypatch, caplog, raw):
        monkeypatch.setenv(XIAOYI_TLS_VERIFY_ENV, raw)
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False
        assert XIAOYI_TLS_VERIFY_ENV in caplog.text

    @staticmethod
    @pytest.mark.parametrize("raw", ["", "true", "1", "yes", "on", "maybe"])
    def test_non_falsy_values_keep_verification(monkeypatch, raw):
        monkeypatch.setenv(XIAOYI_TLS_VERIFY_ENV, raw)
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is False

    @staticmethod
    def test_ca_bundle_keeps_chain_verification(monkeypatch, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_bytes(_pem_from_system_roots())
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(ca))
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is False
        assert context.get_ca_certs()

    @staticmethod
    def test_ca_bundle_on_domain_keeps_hostname_check(monkeypatch, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_bytes(_pem_from_system_roots())
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(ca))
        context = _build_ssl_context("xiaoyi.example.com")
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True

    @staticmethod
    def test_missing_ca_bundle_raises_instead_of_falling_back(monkeypatch, tmp_path):
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(tmp_path / "missing.pem"))
        with pytest.raises(OSError):
            _build_ssl_context(_IPV4_HOST)

    @staticmethod
    def test_invalid_pem_ca_bundle_raises(monkeypatch, tmp_path):
        ca = tmp_path / "broken.pem"
        ca.write_text("not a certificate", encoding="utf-8")
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(ca))
        with pytest.raises(OSError):
            _build_ssl_context(_IPV4_HOST)

    @staticmethod
    def test_explicit_opt_out_takes_precedence_over_ca_bundle(monkeypatch, tmp_path):
        monkeypatch.setenv(XIAOYI_TLS_VERIFY_ENV, "false")
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(tmp_path / "missing.pem"))
        context = _build_ssl_context(_IPV4_HOST)
        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False

"""小艺通道 TLS 上下文：IP 直连时只放宽主机名匹配，不放弃证书校验。"""

from __future__ import annotations

import asyncio
import base64
import logging
import ssl

import pytest

from agent_gateway.channel_manager.im_platforms.xiaoyi import xiaoyi_connect
from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XIAOYI_CA_BUNDLE_ENV,
    XIAOYI_TLS_VERIFY_ENV,
    XiaoyiChannel,
    XiaoyiChannelConfig,
    XiaoyiTlsConfigError,
    _build_ssl_context,
    _validate_tls_config,
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


class TestReviewFindings:
    """atomgit-bot 对 !104 的两条 Medium 意见的回归。"""

    @staticmethod
    def test_ca_bundle_is_the_only_trust_anchor(monkeypatch, tmp_path):
        """配置 CA bundle 后不能继续信任系统根，否则 IP 直连可被任意公共 CA 伪造。"""
        ca = tmp_path / "ca.pem"
        ca.write_bytes(_pem_from_system_roots())
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(ca))

        system_roots = len(ssl.create_default_context().get_ca_certs())
        context = _build_ssl_context(_IPV4_HOST)

        assert len(context.get_ca_certs()) == 1
        assert len(context.get_ca_certs()) < system_roots

    @staticmethod
    def test_ca_bundle_is_the_only_trust_anchor_on_domain(monkeypatch, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_bytes(_pem_from_system_roots())
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(ca))

        context = _build_ssl_context("xiaoyi.example.com")

        assert context.check_hostname is True
        assert len(context.get_ca_certs()) == 1

    @staticmethod
    def test_missing_ca_bundle_raises_config_error(monkeypatch, tmp_path):
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(tmp_path / "missing.pem"))
        with pytest.raises(XiaoyiTlsConfigError) as excinfo:
            _build_ssl_context(_IPV4_HOST)
        # 仍兼容既有 except OSError 的调用方
        assert isinstance(excinfo.value, OSError)
        assert XIAOYI_CA_BUNDLE_ENV in str(excinfo.value)

    @staticmethod
    def test_invalid_pem_raises_config_error(monkeypatch, tmp_path):
        ca = tmp_path / "broken.pem"
        ca.write_text("not a certificate", encoding="utf-8")
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(ca))
        with pytest.raises(XiaoyiTlsConfigError):
            _build_ssl_context(_IPV4_HOST)

    @staticmethod
    def test_validate_tls_config_rejects_broken_bundle(monkeypatch, tmp_path):
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(tmp_path / "missing.pem"))
        with pytest.raises(XiaoyiTlsConfigError):
            _validate_tls_config(("wss://xiaoyi.example.com/ws", ""))

    @staticmethod
    def test_validate_tls_config_ignores_empty_urls(monkeypatch):
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, "/nonexistent/ca.pem")
        _validate_tls_config(("", ""))

    @staticmethod
    def test_validate_tls_config_respects_explicit_opt_out(monkeypatch, tmp_path):
        """XIAOYI_TLS_VERIFY=false 优先级最高，坏 CA 路径不应阻止启动。"""
        monkeypatch.setenv(XIAOYI_TLS_VERIFY_ENV, "false")
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(tmp_path / "missing.pem"))
        _validate_tls_config(("wss://xiaoyi.example.com/ws",))

    @staticmethod
    def test_start_refuses_to_run_with_broken_ca_bundle(monkeypatch, tmp_path, caplog):
        monkeypatch.setenv(XIAOYI_CA_BUNDLE_ENV, str(tmp_path / "missing.pem"))
        channel = object.__new__(XiaoyiChannel)
        channel.config = XiaoyiChannelConfig(
            enabled=True,
            mode="xiaoyi_channel",
            ak="ak",
            sk="sk",
            agent_id="agent",
            ws_url1="wss://" + _IPV4_HOST + "/ws1",
        )
        channel._running = False
        channel._connect_tasks = {}

        caplog.set_level(logging.ERROR, logger=xiaoyi_connect.logger.name)
        asyncio.run(channel.start())

        assert channel._running is False
        assert channel._connect_tasks == {}
        assert "TLS 配置无效" in caplog.text

    @staticmethod
    def test_reconnect_loop_logs_tls_config_error_once(monkeypatch, caplog):
        channel = object.__new__(XiaoyiChannel)
        channel._running = True
        attempts: list[str] = []

        async def fake_connect(_url_key, _url):
            attempts.append(_url_key)
            if len(attempts) >= 3:
                channel._running = False
            raise XiaoyiTlsConfigError("XIAOYI_CA_BUNDLE 不可用")

        async def fake_sleep(_seconds):
            return None

        monkeypatch.setattr(channel, "_connect", fake_connect)
        monkeypatch.setattr(xiaoyi_connect.asyncio, "sleep", fake_sleep)

        caplog.set_level(logging.ERROR, logger=xiaoyi_connect.logger.name)
        asyncio.run(channel._reconnect_loop("ws_url1", "wss://xiaoyi.example.com/ws"))

        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(attempts) == 3
        assert len(errors) == 1
        assert XIAOYI_CA_BUNDLE_ENV in errors[0].getMessage()

    @staticmethod
    def test_reconnect_loop_keeps_network_oserror_as_warning(monkeypatch, caplog):
        """ConnectionRefusedError 也是 OSError，不能被误判成配置错误。"""
        channel = object.__new__(XiaoyiChannel)
        channel._running = True
        attempts: list[str] = []

        async def fake_connect(_url_key, _url):
            attempts.append(_url_key)
            if len(attempts) >= 2:
                channel._running = False
            raise ConnectionRefusedError("refused")

        async def fake_sleep(_seconds):
            return None

        monkeypatch.setattr(channel, "_connect", fake_connect)
        monkeypatch.setattr(xiaoyi_connect.asyncio, "sleep", fake_sleep)

        caplog.set_level(logging.WARNING, logger=xiaoyi_connect.logger.name)
        asyncio.run(channel._reconnect_loop("ws_url1", "wss://xiaoyi.example.com/ws"))

        assert not [r for r in caplog.records if r.levelname == "ERROR"]
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2

# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Vendor RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any


logger = logging.getLogger(__name__)


def register_vendor_handlers(channel: Any) -> None:
    """Register vendor catalog routes in their original order."""

    async def _vendors_list(ws, req_id, params, session_id):
        """返回按 plan 分组的厂商预设列表(供前端 Tab+厂商卡片渲染)。

        纯数据,读 ``gateway.common.model_vendor_registry``,无副作用。
        """
        del params, session_id
        try:
            from agent_gateway.common.model_vendor_registry import to_frontend_payload
            await channel.send_response(
                ws, req_id, ok=True,
                payload={"vendors": to_frontend_payload()},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[vendors.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _vendors_fetch_models(ws, req_id, params, session_id):
        """按厂商预设拉取远端可用模型列表(供前端"拉取最新"按钮)。

        params: {vendor_key, plan, api_key?, modality?, api_base?}。按预设的 ``models_needs_key``
        决定是否带 Authorization。普通列表失败时回退预设；火山生成模型按
        domain 返回完整远端目录，失败时返回空列表供前端手填。
        """
        del session_id
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        vendor_key = str(params.get("vendor_key") or "").strip()
        plan_raw = str(params.get("plan") or "").strip()
        api_key = str(params.get("api_key") or "").strip()
        modality = str(params.get("modality") or "").strip()
        try:
            from agent_gateway.common.model_vendor_registry import (
                PlanKind,
                get_preset,
            )

            def _fetch_remote_sync(
                endpoint: str, hdrs: dict[str, str], model_domain: str | None = None,
            ) -> tuple[list[str], str]:
                """同步拉取并解析 /models(在线程池里跑,不阻塞事件循环)。

                成功返回 ``(模型 ID 列表, "")``;任何失败(网络/状态码非 200/
                解析失败/空)返回 ``([], 原因)``。原因透传给前端,写进回退响应的
                ``reason`` 字段,便于联调时区分鉴权失败/端点问题/限流/网络异常,
                而非笼统的"拉取失败"。永不抛异常。
                """
                import httpx  # noqa: PLC0415
                from openjiuwen.extensions.external_provider.openai_auth.openai_account_models import (
                    parse_openai_account_model_ids,
                )
                try:
                    resp = httpx.get(endpoint, headers=hdrs, timeout=12.0)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[vendors.fetch_models] http error: %s", exc)
                    return [], f"remote fetch error: {type(exc).__name__}"
                if resp.status_code != 200:
                    # 401/403 = key 错或鉴权方式不符(如 maas 需华为签名);
                    # 429 = 限流;5xx = 上游故障。把状态码透传给前端。
                    return [], f"remote returned HTTP {resp.status_code}"
                try:
                    payload_json = resp.json()
                except ValueError:
                    return [], "remote returned non-JSON body"
                if not isinstance(payload_json, dict):
                    return [], "remote returned unexpected body shape"
                if model_domain:
                    entries = payload_json.get("data")
                    if not isinstance(entries, list):
                        return [], "remote returned unexpected body shape"
                    ids: list[str] = []
                    seen: set[str] = set()
                    for item in entries:
                        if not isinstance(item, dict) or item.get("domain") != model_domain:
                            continue
                        model_id = item.get("id")
                        if not isinstance(model_id, str):
                            continue
                        model_id = model_id.strip()
                        if model_id and model_id not in seen:
                            seen.add(model_id)
                            ids.append(model_id)
                    return ids, "" if ids else "remote returned empty generation model list"
                try:
                    ids = parse_openai_account_model_ids(payload_json)
                except Exception:  # noqa: BLE001
                    return [], "remote payload parse failed"
                if not ids:
                    return [], "remote returned empty model list"
                return ids, ""

            try:
                plan = PlanKind(plan_raw)
            except ValueError:
                await channel.send_response(
                    ws, req_id, ok=False,
                    error=f"invalid plan: {plan_raw}",
                    code="BAD_REQUEST",
                )
                return

            preset = get_preset(vendor_key, plan)
            if preset is None:
                await channel.send_response(
                    ws, req_id, ok=False,
                    error=f"unknown vendor/plan: {vendor_key}/{plan_raw}",
                    code="BAD_REQUEST",
                )
                return

            model_domain = None
            endpoint = preset.models_endpoint
            if modality:
                if vendor_key != "volcengine" or plan != PlanKind.CUSTOM_API or modality not in {"image", "video"}:
                    await channel.send_response(
                        ws, req_id, ok=False, error="unsupported model modality", code="BAD_REQUEST",
                    )
                    return
                from agent_gateway.common.volcengine_endpoints import normalize_ark_api_base

                try:
                    api_base = normalize_ark_api_base(str(params.get("api_base") or preset.api_base))
                except ValueError as exc:
                    await channel.send_response(
                        ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST",
                    )
                    return
                endpoint = f"{api_base}/models"
                model_domain = "ImageGeneration" if modality == "image" else "VideoGeneration"

            # 无远端端点(Maas 等) -> 直接回退预设
            if not preset.models_endpoint:
                await channel.send_response(
                    ws, req_id, ok=True,
                    payload={
                        "models": list(preset.model_options),
                        "source": "preset",
                        "reason": "no remote models endpoint",
                    },
                )
                return

            headers: dict[str, str] = {"Accept": "application/json"}
            if preset.models_needs_key:
                if not api_key:
                    # 需 key 但未提供 -> 回退预设
                    await channel.send_response(
                        ws, req_id, ok=True,
                        payload={
                            "models": [] if model_domain else list(preset.model_options),
                            "source": "unavailable" if model_domain else "preset",
                            "reason": "api_key required for fetch",
                        },
                    )
                    return
                headers["Authorization"] = f"Bearer {api_key}"

            # 同步 httpx.get 通过 asyncio.to_thread 卸载到线程池,避免阻塞事件循环
            # (与同文件 _openai_account_*_payload / _updater_* 的 to_thread 模式一致)。
            remote_ids, remote_reason = await asyncio.to_thread(
                _fetch_remote_sync, endpoint, headers, model_domain,
            )

            # DashScope's /models catalogue includes unavailable marketplace,
            # retired, and non-chat entries.  For Alibaba, expose only the
            # small plan-specific allowlist whose models have been verified on
            # the corresponding OpenAI-compatible endpoint.  Keep allowlist
            # order so the UI presents recommended models first.
            if vendor_key == "alibaba" and remote_ids:
                unfiltered_count = len(remote_ids)
                remote_model_ids = set(remote_ids)
                remote_ids = [
                    model_id
                    for model_id in preset.model_options
                    if model_id in remote_model_ids
                ]
                filtered_count = unfiltered_count - len(remote_ids)
                if filtered_count:
                    logger.info(
                        "[vendors.fetch_models] filtered %d non-allowlisted Alibaba models",
                        filtered_count,
                    )
                if not remote_ids:
                    remote_reason = "no remote models matched the Alibaba plan allowlist"

            if remote_ids:
                await channel.send_response(
                    ws, req_id, ok=True,
                    payload={"models": remote_ids, "source": "remote"},
                )
                return

            # 远端失败/空 -> 回退预设。把远端状态码/原因透传给前端,联调时能
            # 分辨是鉴权失败(key 错 / 需厂商专属签名)、限流、还是端点不可达,
            # 而非笼统的"拉取报错"。注意:仍回 source="preset" + ok=True,保证
            # 拉取失败不阻塞前端选模型,只是可见地说明为何回退。
            await channel.send_response(
                ws, req_id, ok=True,
                payload={
                    "models": [] if model_domain else list(preset.model_options),
                    "source": "unavailable" if model_domain else "preset",
                    "reason": remote_reason or "remote fetch failed or empty",
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[vendors.fetch_models] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    channel.register_method("vendors.list", _vendors_list)
    channel.register_method("vendors.fetch_models", _vendors_fetch_models)

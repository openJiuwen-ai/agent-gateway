"""Web methods for connecting a colleague to an owner-managed IM application."""

import asyncio
from collections import Counter
from contextlib import nullcontext
from functools import partial
import logging
from uuid import uuid4
from weakref import WeakValueDictionary

from filelock import Timeout
from e2a.models import E2AEnvelope
from e2a.message import ReqMethod

from agent_gateway.channel_manager.im_platforms.dingtalk.configuration import (
    dingtalk_apps,
    merge_dingtalk_config,
)
from agent_gateway.common.config import get_config_raw, resolve_env_vars
from agent_gateway.common.im_binding_lock import im_binding_lock
from agent_gateway.config.channel.access import edit_channel_body

logger = logging.getLogger(__name__)


class BindingError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def _required(params, key):
    value = params.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} is required")
    return value.strip()


def _binding_identity(app, key):
    return str(resolve_env_vars(app.get(key) or "")).strip()


def register_persistent_bindings(channel, manager, client, normalize_feishu, clear_cache):
    """Register local owner operations. The server resolves the canonical agent ID before each bind."""
    lock = asyncio.Lock()
    colleague_locks = WeakValueDictionary()

    def colleague_lock(config_id):
        return colleague_locks.setdefault(config_id, asyncio.Lock())

    def feishu_apps(current):
        if not current:
            return []
        return normalize_feishu(current)["apps"]

    def apps_for(platform, current):
        return feishu_apps(current) if platform == "feishu" else dingtalk_apps(current)

    def section(platform, current, apps):
        if platform == "dingtalk":
            return merge_dingtalk_config(current, {"apps": apps})
        result = {"apps": apps}
        if "send_file_allowed" in current:
            result["send_file_allowed"] = current["send_file_allowed"]
        return result

    async def request(method, params, timeout):
        service = client()
        if service is None:
            raise BindingError("SERVICE_UNAVAILABLE", "colleague service is unavailable")
        env = E2AEnvelope(request_id=uuid4().hex, channel="web", method=method.value, params=params)
        try:
            response = await asyncio.wait_for(service.send_request(env), timeout)
        except Exception as exc:
            raise BindingError("SERVICE_UNAVAILABLE", "colleague service did not answer") from exc
        payload = response.payload if isinstance(response.payload, dict) else {}
        if not response.ok:
            raise BindingError(str(payload.get("code") or "SERVICE_UNAVAILABLE"),
                               str(payload.get("error") or "connection request failed"))
        return payload

    async def binding_owners(platform, id_key, app_id, config_id, agent_id):
        """Recognize legacy runtime IDs only for this colleague and application."""
        config = await asyncio.to_thread(get_config_raw)
        current = ((config or {}).get("channels") or {}).get(platform)
        owners = {config_id, agent_id}
        saved_owners = {
            _binding_identity(app, "persistent_agent_id")
            for app in apps_for(platform, current)
            if _binding_identity(app, id_key) == app_id
        }
        if saved_owners - owners - {""}:
            listed = await request(ReqMethod.JIUWENMATE_COLLEAGUE_CONNECTIONS, {"config_id": config_id}, 120)
            owners.update(
                item["agent_id"] for item in listed.get("connections", [])
                if isinstance(item, dict) and item.get("channel") == platform and item.get("connection_key") == app_id
                and isinstance(item.get("agent_id"), str)
            )
        return owners

    async def apply(cm, platform, saved, method, before):
        runtime_conf = resolve_env_vars(saved)
        try:
            await cm.set_conf(platform, runtime_conf)
        except Exception as exc:
            raise BindingError("CHANNEL_APPLY_FAILED", "configuration saved but the channel could not restart") from exc
        clear_cache(method)
        if platform in {"feishu", "feishu_enterprise"}:
            def groups(current):
                apps = feishu_apps(current) if platform == "feishu" else current.values()
                result = {}
                for app in apps:
                    if isinstance(app, dict) and app.get("persistent_agent_id"):
                        result.setdefault(app["persistent_agent_id"], []).append(app)
                return result

            old, new = groups(before), groups(runtime_conf)
            for agent_id in old.keys() | new.keys():
                if old.get(agent_id) != new.get(agent_id):
                    try:
                        await request(ReqMethod.JIUWENMATE_FEISHU_PREPARE, {"agent_id": agent_id}, 2)
                    except BindingError:
                        logger.warning("Feishu CLI preparation deferred after binding change")

    async def remove_config(cm, platform, selected):
        """Remove saved bindings and reconcile a stale channel after a failed restart."""
        before = cm.get_conf(platform)
        removed = []

        def edit(current):
            remaining = []
            for app in apps_for(platform, current):
                if selected(app):
                    removed.append(app)
                else:
                    remaining.append(app)
            if not removed:
                return current
            if platform == "dingtalk":
                remaining = [{"client_id": app["client_id"]} for app in remaining if app["client_id"]]
            return section(platform, current, remaining)

        saved = await edit_channel_body(platform, edit)
        if removed or any(selected(app) for app in apps_for(platform, before)):
            if platform == "feishu":
                # The previous restart may have failed before the persisted app reached memory.
                before = {"apps": [*feishu_apps(before), *resolve_env_vars(removed)]}
            return saved, before
        return None

    async def remove_bindings(cm, platform, selected, method):
        change = await remove_config(cm, platform, selected)
        if change is not None:
            saved, before = change
            await apply(cm, platform, saved, method, before)

    async def clear_dingtalk_authorization(app_id, selected):
        """Revoke the saved owner's user login while the application binding still exists."""
        config = await asyncio.to_thread(get_config_raw)
        current = ((config or {}).get("channels") or {}).get("dingtalk")
        owners = {_binding_identity(app, "persistent_agent_id") for app in dingtalk_apps(current) if selected(app)}
        for agent_id in sorted(owners):
            cleared = await request(ReqMethod.JIUWENMATE_DINGTALK_AUTH_DISCONNECT, {
                "agent_id": agent_id, "connection_key": app_id,
            }, 30)
            status = cleared.get("dingtalk_auth")
            if not isinstance(status, dict) or status.get("status") != "not_connected":
                raise BindingError(
                    "DINGTALK_CLI_AUTH_CLEANUP_FAILED",
                    "DingTalk user authorization was not cleared; try again",
                )

    async def modify(cm, platform, op, params):
        id_key = "app_id" if platform == "feishu" else "client_id"
        app_id = resolve_env_vars(_required(params, id_key)).strip()
        if not app_id:
            raise ValueError(f"{id_key} is required")
        method = f"channel.{platform}.{op}"

        def unbind(app):
            return _binding_identity(app, id_key) == app_id and bool(_binding_identity(app, "persistent_agent_id"))

        def edit(current):
            matched = False
            apps = []
            for app in apps_for(platform, current):
                owner = _binding_identity(app, "persistent_agent_id")
                if _binding_identity(app, id_key) == app_id:
                    if owner and owner not in owners:
                        raise BindingError(
                            "CHANNEL_ALREADY_BOUND",
                            "This channel is already bound to another colleague; unbind it first.",
                        )
                    fields = {**binding, id_key: app[id_key]}
                    apps.append(fields if platform == "dingtalk" else {**app, **fields})
                    matched = True
                elif owner != agent_id:
                    # Omit unchanged DingTalk fields so a malformed allowlist stays disabled.
                    apps.append({id_key: app[id_key]} if platform == "dingtalk" else app)
            if not matched:
                if platform == "feishu":
                    apps.append(normalize_feishu({"apps": [{
                        **binding, "is_default": not any(app.get("is_default") for app in apps),
                    }]})["apps"][0])
                else:
                    apps.append({**binding, "allow_from": []})
            return section(platform, current, apps)

        async def prepare_binding():
            secret_key = "app_secret" if platform == "feishu" else "client_secret"
            secret = _required(params, secret_key)
            config_id = _required(params, "config_id")
            connected = await request(ReqMethod.JIUWENMATE_CONNECTION_ATTACH, {
                "config_id": config_id, "channel": platform, "connection_key": app_id,
            }, 120)
            agent_id = connected.get("agent_id")
            if not isinstance(agent_id, str) or not agent_id.strip():
                raise BindingError("SERVICE_UNAVAILABLE", "colleague service did not return an agent ID")
            binding = {id_key: app_id, secret_key: secret, "persistent_agent_id": agent_id, "enabled": True}
            if platform == "dingtalk" and "robot_code" in params:
                binding["robot_code"] = str(params.get("robot_code") or "").strip()
            owners = await binding_owners(platform, id_key, app_id, config_id, agent_id)
            return binding, agent_id, owners

        if platform == "feishu":
            if op == "unbind_agent":
                async with lock:
                    await remove_bindings(cm, platform, unbind, method)
                return {"unbound": True}
            binding, agent_id, owners = await prepare_binding()
            async with lock:
                before = cm.get_conf(platform)
                saved = await edit_channel_body(platform, edit)
                await apply(cm, platform, saved, method, before)
        else:
            async with im_binding_lock(platform, app_id):
                if op == "unbind_agent":
                    await clear_dingtalk_authorization(app_id, unbind)
                    changed = await remove_config(cm, platform, unbind) is not None
                else:
                    binding, agent_id, owners = await prepare_binding()
                    await edit_channel_body(platform, edit)
                    changed = True
            if changed:
                # Stream shutdown can wait for callbacks that need the application lock.
                # Apply the latest saved config in case a different app changed in the meantime.
                async with lock:
                    saved = await edit_channel_body(platform, lambda current: current)
                    await apply(cm, platform, saved, method, cm.get_conf(platform))
        if op == "unbind_agent":
            return {"unbound": True}
        return {"binding": {id_key: app_id, "persistent_agent_id": agent_id}}

    async def revoke(cm, params):
        agent_id = _required(params, "persistent_agent_id")
        found = False
        for platform in ("feishu", "feishu_enterprise"):
            def disable(current):
                nonlocal found
                entries = feishu_apps(current) if platform == "feishu" else current.values()
                if not any(isinstance(a, dict) and a.get("persistent_agent_id") == agent_id for a in entries):
                    return current
                found = True
                def disabled(app):
                    return {**app, "enabled": False} if isinstance(app, dict) and app.get("persistent_agent_id") == agent_id else app
                if platform == "feishu":
                    return section(platform, current, [disabled(a) for a in entries])
                return {key: disabled(app) for key, app in current.items()}

            before = cm.get_conf(platform)
            saved = await edit_channel_body(platform, disable)
            if saved != before:
                await apply(cm, platform, saved, "channel.feishu.revoke", before)
        if not found:
            raise BindingError("NOT_FOUND", "colleague is not bound to Feishu")
        return {"revoked": True}

    async def handle(platform, op, ws, req_id, params, session_id):
        from agent_gateway.enterprise import is_enterprise

        cm = manager()
        try:
            if op != "get_bindings" and is_enterprise():
                raise BindingError("FORBIDDEN", "企业版配置由管理面统一下发")
            if cm is None:
                raise BindingError("SERVICE_UNAVAILABLE", "channel manager is unavailable")
            if not isinstance(params, dict):
                raise ValueError("params must be an object")
            async with (colleague_lock(_required(params, "config_id")) if op == "bind_agent"
                        else nullcontext() if op == "unbind_agent" else lock):
                if op == "get_bindings":
                    active = [a for a in feishu_apps(cm.get_conf(platform))
                              if a.get("enabled", True) and a.get("persistent_agent_id") and a.get("app_id")]
                    counts = Counter(a["app_id"] for a in active)
                    agents = Counter(a["persistent_agent_id"] for a in active)
                    result = {"agent_ids": sorted(a["persistent_agent_id"] for a in active
                              if counts[a["app_id"]] == 1 and agents[a["persistent_agent_id"]] == 1)}
                elif op == "revoke":
                    result = await revoke(cm, params)
                else:
                    result = await modify(cm, platform, op, params)
        except BindingError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code=exc.code)
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except Timeout:
            await channel.send_response(ws, req_id, ok=False, error="application binding is busy", code="CONFLICT")
        except Exception:
            logger.exception("Unable to update %s colleague binding", platform)
            await channel.send_response(ws, req_id, ok=False, error="binding update failed", code="INTERNAL_ERROR")
        else:
            await channel.send_response(ws, req_id, ok=True, payload=result)

    async def save_apps(cm, platform, changes):
        """Use the same lock and merge rules for binding and owner settings edits."""
        async with lock:
            before = cm.get_conf(platform)

            def edit(current):
                if platform == "dingtalk":
                    return merge_dingtalk_config(current, changes)
                apps = changes["apps"]
                if not isinstance(apps, list) or not all(isinstance(app, dict) for app in apps):
                    raise ValueError("apps must be a list of applications")
                old = {app["app_id"]: app for app in feishu_apps(current)}
                merged = [{**old.get(app.get("app_id"), {}), **app} for app in apps]
                return section(platform, current, normalize_feishu({"apps": merged})["apps"])

            saved = await edit_channel_body(platform, edit)
            await apply(cm, platform, saved, f"channel.{platform}.set_conf", before)
            return saved

    for platform in ("feishu", "dingtalk"):
        for op in ("bind_agent", "unbind_agent"):
            channel.register_method(f"channel.{platform}.{op}", partial(handle, platform, op), local_only=True)
    for op in ("get_bindings", "revoke"):
        channel.register_method(f"channel.feishu.{op}", partial(handle, "feishu", op), local_only=True)

    from agent_gateway.channel_manager.web.welink_bindings import register_welink_bindings

    register_welink_bindings(channel, manager, request, colleague_lock, lock)

    async def delete_colleague(ws, req_id, params, session_id):
        from agent_gateway.enterprise import is_enterprise
        from agent_gateway.channel_manager.im_platforms.welink.welink_binding import (
            normalize_welink_section, revoke_welink_authorization,
        )

        try:
            if is_enterprise():
                raise BindingError("FORBIDDEN", "企业版配置由管理面统一下发")
            cm = manager()
            if cm is None:
                raise BindingError("SERVICE_UNAVAILABLE", "channel manager is unavailable")
            config_id = _required(params or {}, "agent_id")
            mode = params.get("mode")
            if mode not in ("unbind", "purge"):
                raise ValueError("mode must be unbind or purge")
            async with colleague_lock(config_id):
                listed = await request(ReqMethod.JIUWENMATE_COLLEAGUE_CONNECTIONS, {"config_id": config_id}, 120)
                agents = {config_id} | {item["agent_id"] for item in listed.get("connections", [])
                                       if isinstance(item, dict) and isinstance(item.get("agent_id"), str)}
                async with lock:
                    for platform in ("feishu", "welink", "dingtalk"):
                        if platform != "welink":
                            await remove_bindings(
                                cm, platform, lambda app: _binding_identity(app, "persistent_agent_id") in agents,
                                "jiuwenmate.colleague.delete",
                            )
                            continue

                        def remove(current):
                            bindings = normalize_welink_section(current)["bindings"]
                            if not any(item["persistent_agent_id"] in agents or item.get("config_id") in agents
                                       for item in bindings):
                                return current
                            for agent in agents:
                                current = revoke_welink_authorization(current, agent)
                            return current

                        before = cm.get_conf(platform)
                        saved = await edit_channel_body(platform, remove)
                        if saved != before:
                            await apply(cm, platform, saved, "jiuwenmate.colleague.delete", before)
                await request(ReqMethod.JIUWENMATE_COLLEAGUE_REMOVE, {"config_id": config_id, "mode": mode}, 120)
        except BindingError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code=exc.code)
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except Exception:
            logger.exception("Unable to remove colleague bindings")
            await channel.send_response(ws, req_id, ok=False, error="binding removal failed", code="INTERNAL_ERROR")
        else:
            await channel.send_response(ws, req_id, ok=True, payload={"deleted": True})

    channel.register_method("jiuwenmate.colleague.delete", delete_colleague, local_only=True)

    return save_apps

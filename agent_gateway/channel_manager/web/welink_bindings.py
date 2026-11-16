"""Owner-local Web operations for authorizing one reader and its assistant bindings."""
import asyncio
import logging

from agent_gateway.enterprise import is_enterprise
from agent_gateway.channel_manager.web.persistent_bindings import BindingError

logger = logging.getLogger(__name__)


def register_welink_bindings(channel, manager, request, colleague_lock, welink_authorization_lock):

    async def _connection_request(method, params):
        try:
            return await request(method, params, 120), None
        except BindingError as exc:
            return None, (exc.code, str(exc))

    def owner_handler(handler):
        async def owner(ws, req_id, params, session_id):
            if is_enterprise():
                await channel.send_response(ws, req_id, ok=False, error="企业版配置由管理面统一下发", code="FORBIDDEN")
                return
            await handler(ws, req_id, params, session_id)
        return owner

    async def _channel_welink_get_bindings(ws, req_id, params, session_id):
        try:
            from agent_gateway.channel_manager.im_platforms.welink.welink_binding import (
                load_welink_section, public_welink_authorization,
            )
            cm = manager()
            conf = cm.get_conf("welink") if cm is not None else load_welink_section()
            selected = str((params or {}).get("config_id") or (params or {}).get("persistent_agent_id") or "") or None
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=public_welink_authorization(conf, selected),
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.welink.get_bindings] 异常, req_id=%s: %s", req_id, e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_welink_login_start(ws, req_id, params, session_id):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        account = str(params.get("account") or "").strip()
        agent_id = str(params.get("config_id") or params.get("persistent_agent_id") or "").strip()
        if not agent_id:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="persistent_agent_id is required",
                code="BAD_REQUEST",
            )
            return
        from agent_gateway.channel_manager.im_platforms.welink.welink_login import LOGIN_MANAGER
        from agent_gateway.channel_manager.im_platforms.welink.facts import credential_dir_for
        import uuid

        cli_path = str(params.get("cli_path") or "welink-cli").strip() or "welink-cli"
        env_name = str(params.get("env") or "pro").strip() or "pro"
        # A new QR must not run inside the directory that already holds the reader
        # session. Logout and an abandoned scan were clearing that session while
        # the saved account still looked bound.
        staged = credential_dir_for(agent_id).parent / f"pending-{uuid.uuid4().hex}"
        started = await asyncio.to_thread(
            LOGIN_MANAGER.start,
            agent_id=agent_id,
            account=account,
            credential_dir=staged,
            cli_path=cli_path,
            env_name=env_name,
        )
        await channel.send_response(ws, req_id, ok=True, payload=started.snapshot())

    async def _channel_welink_login_status(ws, req_id, params, session_id):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        session_key = str(params.get("session_id") or "").strip()
        from agent_gateway.channel_manager.im_platforms.welink.welink_login import LOGIN_MANAGER

        login_session = LOGIN_MANAGER.get(session_key)
        if login_session is None:
            await channel.send_response(ws, req_id, ok=False, error="login session not found", code="NOT_FOUND")
            return
        payload = await asyncio.to_thread(LOGIN_MANAGER.refresh, login_session, "welink-cli", "pro")
        if payload.get("status") == "success" and not login_session.saved:
            try:
                payload = await _persist_welink_login_step(login_session, "cli")
            except Exception as e:  # noqa: BLE001
                logger.exception("[channel.welink.login_status] 保存绑定失败, req_id=%s: %s", req_id, e)
                login_session.status = "failed"
                login_session.error = str(e)
                payload = login_session.snapshot()
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _save_welink_connection(config_id, account, patch, *, credential_source=None):
        """Commit either authorization step to the account's own agent, in order with deletion."""
        from e2a.message import ReqMethod
        from agent_gateway.channel_manager.im_platforms.welink.welink_binding import save_welink_authorization

        from agent_gateway.channel_manager.im_platforms.welink.facts import credential_dir_for, welink_connection_key

        from agent_gateway.channel_manager.im_platforms.welink.welink_binding import (
            apply_welink_authorization, load_welink_section, normalize_welink_section,
        )

        async with colleague_lock(config_id):
            listed, failure = await _connection_request(
                ReqMethod.JIUWENMATE_COLLEAGUE_CONNECTIONS, {"config_id": config_id},
            )
            if failure is not None:
                raise BindingError(*failure)
            legacy_connections = [item for item in listed.get("connections", []) if item.get("channel") == "welink"]
            async with welink_authorization_lock:
                # Validate before creating a connection, moving credentials or applying config.
                current = load_welink_section()
                existing = next((item for item in normalize_welink_section(current)["bindings"]
                                 if item.get("config_id") == config_id or item["persistent_agent_id"] == config_id
                                 or any(connection.get("agent_id") == item["persistent_agent_id"]
                                        for connection in legacy_connections)), None)
                validation_agent = existing["persistent_agent_id"] if existing else config_id
                apply_welink_authorization(current, {**patch, "account": account,
                                                     "persistent_agent_id": validation_agent})
            connection_key = next((item["connection_key"] for item in legacy_connections
                                   if existing and item.get("agent_id") == existing["persistent_agent_id"]),
                                  welink_connection_key(account, config_id))
            connected, failure = await _connection_request(
                ReqMethod.JIUWENMATE_CONNECTION_ATTACH,
                {"config_id": config_id, "channel": "welink", "connection_key": connection_key},
            )
            if failure is not None:
                raise BindingError(*failure)
            async with welink_authorization_lock:
                agent_id = connected["agent_id"]
                # attach_connection returns the office colleague. Keep the dedicated
                # WeLink agent when that row already exists, so inbound still matches.
                if agent_id == config_id:
                    dedicated = [item["agent_id"] for item in legacy_connections
                                 if item.get("agent_id") and item["agent_id"] != config_id]
                    if len(dedicated) == 1:
                        agent_id = dedicated[0]
                stored_patch = {**patch, "account": account, "persistent_agent_id": agent_id, "config_id": config_id}
                stored_patch.setdefault("credential_dir", str((existing or {}).get("credential_dir") or credential_dir_for(agent_id)))
                apply_welink_authorization(load_welink_section(), stored_patch)
                if credential_source is not None:
                    import shutil

                    destination = credential_dir_for("shared-reader")
                    if credential_source.exists() or not destination.exists():
                        shutil.copytree(credential_source, destination, dirs_exist_ok=True)
                        shutil.rmtree(credential_source)
                    stored_patch["reader_credential_dir"] = str(destination)
                view, stored = save_welink_authorization(stored_patch)
                cm = manager()
                if cm is not None:
                    await cm.set_conf("welink", stored)
                return view

    async def _persist_welink_login_step(login_session, kind: str) -> dict:
        from agent_gateway.channel_manager.im_platforms.welink.welink_binding import reveal_new_assistant_credential

        reveal: dict[str, str] = {}
        credential_source = None
        if kind == "assistant":
            credential_id, secret = login_session.credentials()
            if not credential_id or not secret:
                login_session.status = "failed"
                login_session.error = "扫码成功，但没有拿到助手凭证"
                return login_session.snapshot()
            account = login_session.account
            reveal = reveal_new_assistant_credential(credential_id, secret)
            patch = {"assistant_credential_id": credential_id, "assistant_secret": secret}
        else:
            account = login_session.resolved_account or login_session.account
            credential_source = login_session.credential_dir
            patch = {
                "cli_path": login_session.cli_path or "welink-cli",
                "env": login_session.env_name or "pro",
                "poll_interval_seconds": 4,
                "cli_authorized": True,
            }
        view = await _save_welink_connection(
            login_session.agent_id, account, patch, credential_source=credential_source,
        )
        login_session.saved = True
        snapshot = login_session.snapshot()
        snapshot.update(view)
        snapshot.update(reveal)
        return snapshot

    async def _channel_welink_assistant_login_start(ws, req_id, params, session_id):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        account = str(params.get("account") or "").strip()
        agent_id = str(params.get("config_id") or params.get("persistent_agent_id") or "").strip()
        if not account or not agent_id:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="account and persistent_agent_id are required",
                code="BAD_REQUEST",
            )
            return
        from agent_gateway.channel_manager.im_platforms.welink.welink_assistant_login import (
            ASSISTANT_LOGIN_MANAGER,
        )

        started = await asyncio.to_thread(ASSISTANT_LOGIN_MANAGER.start, agent_id=agent_id, account=account)
        await channel.send_response(ws, req_id, ok=True, payload=started.snapshot())

    async def _channel_welink_assistant_login_status(ws, req_id, params, session_id):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        session_key = str(params.get("session_id") or "").strip()
        from agent_gateway.channel_manager.im_platforms.welink.welink_assistant_login import (
            ASSISTANT_LOGIN_MANAGER,
        )

        login_session = ASSISTANT_LOGIN_MANAGER.get(session_key)
        if login_session is None:
            await channel.send_response(ws, req_id, ok=False, error="login session not found", code="NOT_FOUND")
            return
        payload = await asyncio.to_thread(ASSISTANT_LOGIN_MANAGER.refresh, login_session)
        if payload.get("status") == "success" and not login_session.saved:
            try:
                payload = await _persist_welink_login_step(login_session, "assistant")
            except Exception as e:  # noqa: BLE001
                logger.exception("[channel.welink.assistant_login_status] 保存助手凭证失败, req_id=%s: %s", req_id, e)
                login_session.status = "failed"
                login_session.error = str(e)
                payload = login_session.snapshot()
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _channel_welink_assistant_credential_save(ws, req_id, params, session_id):
        """Store an existing assistant AK/SK for this colleague. The secret is not returned."""
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        from agent_gateway.channel_manager.im_platforms.welink.welink_binding import (
            manual_assistant_patch,
        )

        try:
            patch = manual_assistant_patch(
                agent_id=str(params.get("config_id") or params.get("persistent_agent_id") or ""),
                account=str(params.get("account") or ""),
                credential_id=str(params.get("assistant_credential_id") or ""),
                secret=str(params.get("assistant_secret") or ""),
            )
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
            return
        try:
            view = await _save_welink_connection(patch["persistent_agent_id"], patch["account"], patch)
        except Exception as exc:  # noqa: BLE001

            logger.warning("[channel.welink.assistant_credential_save] 保存失败: %s", type(exc).__name__)
            code = exc.code if isinstance(exc, BindingError) else "BAD_REQUEST" if isinstance(exc, ValueError) else "INTERNAL_ERROR"
            error = str(exc) if isinstance(exc, ValueError) else "保存助手凭证失败"
            await channel.send_response(ws, req_id, ok=False, error=error, code=code)
            return
        payload = {"status": "success", "kind": "assistant"}
        payload.update(view)
        payload.pop("assistant_secret", None)
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _channel_welink_revoke(ws, req_id, params, session_id):
        """Remove one colleague's WeLink assistant while retaining the shared reader login."""
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        selected_id = str(params.get("config_id") or params.get("persistent_agent_id") or "").strip()
        if not selected_id:
            await channel.send_response(ws, req_id, ok=False, error="config_id is required", code="BAD_REQUEST")
            return
        cm = manager()
        if cm is None:
            await channel.send_response(
                ws, req_id, ok=False, error="channel manager not available", code="SERVICE_UNAVAILABLE",
            )
            return
        try:
            async with colleague_lock(selected_id), welink_authorization_lock:
                from agent_gateway.common.config import update_config
                from agent_gateway.channel_manager.im_platforms.welink.welink_binding import (
                    normalize_welink_section, revoke_welink_authorization,
                )
                current = normalize_welink_section(cm.get_conf("welink"))
                binding = next((item for item in current["bindings"]
                                if item["persistent_agent_id"] == selected_id or item.get("config_id") == selected_id), None)
                if binding is None:
                    await channel.send_response(
                        ws, req_id, ok=False, error="colleague is not bound to WeLink", code="NOT_FOUND",
                    )
                    return
                runtime_agent_id = binding["persistent_agent_id"]

                def remove_binding(data):
                    channels = data.setdefault("channels", {})
                    channels["welink"] = revoke_welink_authorization(channels.get("welink"), runtime_agent_id)
                    return data

                stored = update_config(remove_binding)
                await cm.set_conf("welink", dict(stored["channels"]["welink"]))
                await channel.send_response(ws, req_id, ok=True, payload={"revoked": True})
        except Exception as exc:  # noqa: BLE001
            logger.exception("[channel.welink.revoke] 取消授权失败, req_id=%s: %s", req_id, exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    channel.register_method("channel.welink.get_bindings", owner_handler(_channel_welink_get_bindings), local_only=True)
    channel.register_method("channel.welink.login_start", owner_handler(_channel_welink_login_start), local_only=True)
    channel.register_method("channel.welink.login_status", owner_handler(_channel_welink_login_status), local_only=True)
    channel.register_method("channel.welink.assistant_login_start", owner_handler(_channel_welink_assistant_login_start), local_only=True)
    channel.register_method("channel.welink.assistant_login_status", owner_handler(_channel_welink_assistant_login_status), local_only=True)
    channel.register_method("channel.welink.assistant_credential_save", owner_handler(_channel_welink_assistant_credential_save), local_only=True)
    channel.register_method("channel.welink.revoke", owner_handler(_channel_welink_revoke), local_only=True)

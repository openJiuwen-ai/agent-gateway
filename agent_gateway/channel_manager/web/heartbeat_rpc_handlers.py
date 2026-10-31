# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Heartbeat RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

from typing import Any

from agent_gateway.heartbeat import HeartbeatServiceUnavailableError


def register_heartbeat_handlers(
    channel: Any, heartbeat_controller: Any, _resolve: Any
) -> None:
    """Register heartbeat RPC handlers in their original route order."""

    def _get_heartbeat():
        return _resolve(heartbeat_controller)

    async def _heartbeat_unavailable(ws, req_id, error="heartbeat not available"):
        await channel.send_response(
            ws, req_id, ok=False, error=str(error), code="SERVICE_UNAVAILABLE"
        )

    async def _hb_job_list(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        try:
            result = await hc.list_jobs(
                params if isinstance(params, dict) else {},
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload={"jobs": result.get("jobs", [])})

    async def _hb_job_meta(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        try:
            result = await hc.get_meta(user_id=str(user_id or ""))
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload=result)

    async def _hb_job_get(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            job = await hc.get_job(
                job_id,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        if job is None:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        await channel.send_response(ws, req_id, ok=True, payload={"job": job})

    async def _hb_job_create(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        # 普通 Web RPC 只能绑定当前 session；跨 session 迁移需独立管理权限入口。
        params = {**params, "channel_id": "web", "session_id": session_id, "source": "web_rpc"}
        try:
            job = await hc.create_job(params, user_id=str(user_id or ""))
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        except Exception as e:  # noqa: BLE001
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")
            return
        await channel.send_response(ws, req_id, ok=True, payload={"job": job})

    async def _hb_job_update(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        patch = params.get("patch")
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        if not isinstance(patch, dict):
            await channel.send_response(ws, req_id, ok=False, error="patch must be object", code="BAD_REQUEST")
            return
        try:
            job = await hc.update_job(
                job_id,
                patch,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload={"job": job})

    async def _hb_job_delete(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            result = await hc.delete_job(
                job_id,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except RuntimeError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="CONFLICT")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload=result)

    async def _hb_job_toggle(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        enabled = params.get("enabled")
        if not isinstance(enabled, bool):
            await channel.send_response(ws, req_id, ok=False, error="enabled must be boolean", code="BAD_REQUEST")
            return
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        try:
            job = await hc.toggle_job(
                job_id,
                enabled,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload={"job": job})

    async def _hb_job_preview(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        count = 5
        raw_count = params.get("count")
        if isinstance(raw_count, int) and raw_count > 0:
            count = raw_count
        try:
            result = await hc.preview_job(
                job_id,
                count=count,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload=result)

    async def _hb_job_run_now(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        reschedule = params.get("reschedule", False)
        if not isinstance(reschedule, bool):
            await channel.send_response(ws, req_id, ok=False, error="reschedule must be boolean", code="BAD_REQUEST")
            return
        try:
            result = await hc.run_now(
                job_id,
                reschedule=reschedule,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload=result)

    async def _hb_job_cancel(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        job_id = str(params.get("id") or "").strip()
        if not job_id:
            await channel.send_response(ws, req_id, ok=False, error="id is required", code="BAD_REQUEST")
            return
        pause_schedule = params.get("pause_schedule", False)
        if not isinstance(pause_schedule, bool):
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="pause_schedule must be boolean",
                code="BAD_REQUEST",
            )
            return
        try:
            result = await hc.cancel_run(
                job_id,
                pause_schedule=pause_schedule,
                access_session_id=session_id,
                user_id=str(user_id or ""),
            )
        except PermissionError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="FORBIDDEN")
            return
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
            return
        except HeartbeatServiceUnavailableError as e:
            await _heartbeat_unavailable(ws, req_id, e)
            return
        await channel.send_response(ws, req_id, ok=True, payload=result)

    channel.register_method("heartbeat.job.list", _hb_job_list)
    channel.register_method("heartbeat.job.meta", _hb_job_meta)
    channel.register_method("heartbeat.job.get", _hb_job_get)
    channel.register_method("heartbeat.job.create", _hb_job_create)
    channel.register_method("heartbeat.job.update", _hb_job_update)
    channel.register_method("heartbeat.job.delete", _hb_job_delete)
    channel.register_method("heartbeat.job.toggle", _hb_job_toggle)
    channel.register_method("heartbeat.job.preview", _hb_job_preview)
    channel.register_method("heartbeat.job.run_now", _hb_job_run_now)
    channel.register_method("heartbeat.job.cancel", _hb_job_cancel)

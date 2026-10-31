# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


def register_tui_heartbeat_handlers(
    channel: Any,
    path: str,
    heartbeat_controller_ref: Any,
) -> None:
    """Register TUI heartbeat.job.* handlers."""
    from agent_gateway.heartbeat import HeartbeatServiceUnavailableError

    def _get_heartbeat():
        """Resolve heartbeat_controller from ref dict or direct instance."""
        if isinstance(heartbeat_controller_ref, dict):
            return heartbeat_controller_ref.get("value")
        return heartbeat_controller_ref

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
            await channel.send_response(ws, req_id, ok=True, payload={"jobs": result.get("jobs", [])})
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _hb_job_meta(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        try:
            result = await hc.get_meta(user_id=str(user_id or ""))
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
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
            if job is None:
                await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
                return
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.get] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _hb_job_create(ws, req_id, params, session_id, user_id=None):
        hc = _get_heartbeat()
        if hc is None:
            await _heartbeat_unavailable(ws, req_id)
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        # TUI 自动继承当前 channel_id=tui + session_id;source=tui_rpc。
        create_params = {
            **params,
            "channel_id": "tui",
            "session_id": session_id,
            "source": "tui_rpc",
        }
        try:
            job = await hc.create_job(
                create_params,
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.create] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

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
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.update] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

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
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload=result)
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except RuntimeError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="CONFLICT")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.delete] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _hb_job_toggle(ws, req_id, params, session_id, user_id=None):
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
        enabled = params.get("enabled")
        if not isinstance(enabled, bool):
            await channel.send_response(ws, req_id, ok=False, error="enabled must be boolean", code="BAD_REQUEST")
            return
        try:
            job = await hc.toggle_job(
                job_id,
                enabled,
                access_session_id=session_id,
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload={"job": job})
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.toggle] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

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
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload=result)
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.preview] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

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
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload=result)
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.run_now] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

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
                user_id=str(user_id or getattr(ws, "_gateway_user_id", None) or ""),
            )
            await channel.send_response(ws, req_id, ok=True, payload=result)
        except KeyError:
            await channel.send_response(ws, req_id, ok=False, error="job not found", code="NOT_FOUND")
        except PermissionError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="FORBIDDEN")
        except HeartbeatServiceUnavailableError as exc:
            await _heartbeat_unavailable(ws, req_id, exc)
        except Exception as exc:
            logger.warning("[heartbeat.job.cancel] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    channel.register_local_handler(path, "heartbeat.job.list", _hb_job_list)
    channel.register_local_handler(path, "heartbeat.job.meta", _hb_job_meta)
    channel.register_local_handler(path, "heartbeat.job.get", _hb_job_get)
    channel.register_local_handler(path, "heartbeat.job.create", _hb_job_create)
    channel.register_local_handler(path, "heartbeat.job.update", _hb_job_update)
    channel.register_local_handler(path, "heartbeat.job.delete", _hb_job_delete)
    channel.register_local_handler(path, "heartbeat.job.toggle", _hb_job_toggle)
    channel.register_local_handler(path, "heartbeat.job.preview", _hb_job_preview)
    channel.register_local_handler(path, "heartbeat.job.run_now", _hb_job_run_now)
    channel.register_local_handler(path, "heartbeat.job.cancel", _hb_job_cancel)

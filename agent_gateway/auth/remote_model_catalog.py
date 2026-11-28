"""Worker-side remote adapter for the public login-model catalog."""
from __future__ import annotations

import asyncio

from agent_gateway.auth.public_model_catalog import (
    CatalogError,
    CatalogResult,
    LoginModelSnapshot,
    validate_auth_session,
    validate_catalog_response,
    validate_login_model_snapshot,
)
from agent_gateway.processes.control_service import (
    ControlServiceBusy,
    ControlServiceClient,
    ControlServiceError,
    ControlServiceOperationError,
    ControlProtocolError,
)


_REQUEST_TIMEOUT_S = 16.0
_SNAPSHOT_REQUEST_TIMEOUT_S = 9.0


class RemoteLoginModelCatalog:
    """Fetch catalog snapshots through one explicit Control RPC."""

    def __init__(self, client: ControlServiceClient) -> None:
        self._client = client

    async def list_models(self, auth_session: str | None) -> CatalogResult:
        normalized_session = validate_auth_session(auth_session)
        try:
            response = await self._client.call(
                "catalog.list_login_models",
                {"auth_session": normalized_session},
                timeout=_REQUEST_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            raise
        except ControlServiceBusy:
            raise CatalogError("busy") from None
        except ControlServiceOperationError as error:
            reason = (
                "not_enabled"
                if error.error_code == "OPERATION_NOT_ENABLED"
                else "unavailable"
            )
            raise CatalogError(reason) from None
        except ControlProtocolError:
            raise CatalogError("protocol") from None
        except (TimeoutError, ControlServiceError):
            raise CatalogError("unavailable") from None
        except Exception:
            raise CatalogError("unavailable") from None

        result = validate_catalog_response(response)
        if result.status == "too_large":
            raise CatalogError("too_large") from None
        if result.status == "unavailable":
            raise CatalogError("unavailable") from None
        return result


class RemoteLoginModelSnapshot:
    """Fetch complete model snapshots through one explicit Control RPC."""

    def __init__(self, client: ControlServiceClient) -> None:
        self._client = client

    async def get_snapshot(self, auth_session: str | None) -> LoginModelSnapshot:
        normalized_session = validate_auth_session(auth_session)
        try:
            response = await self._client.call(
                "catalog.get_login_model_snapshot",
                {"auth_session": normalized_session},
                timeout=_SNAPSHOT_REQUEST_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            raise
        except ControlServiceBusy:
            raise CatalogError("busy") from None
        except ControlServiceOperationError as error:
            reason = (
                "not_enabled"
                if error.error_code == "OPERATION_NOT_ENABLED"
                else "unavailable"
            )
            raise CatalogError(reason) from None
        except ControlProtocolError:
            raise CatalogError("protocol") from None
        except (TimeoutError, ControlServiceError):
            raise CatalogError("unavailable") from None
        except Exception:
            raise CatalogError("unavailable") from None

        snapshot = validate_login_model_snapshot(response)
        if snapshot.status == "too_large":
            raise CatalogError("too_large") from None
        if snapshot.status == "unavailable":
            raise CatalogError("unavailable") from None
        return snapshot

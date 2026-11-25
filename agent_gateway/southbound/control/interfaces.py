# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Separate control capabilities; no dependency on data-plane interfaces."""

from __future__ import annotations

from typing import Protocol, TypeVar

from agent_gateway.southbound.contracts import ConnectionTarget

CreateRequest = TypeVar("CreateRequest", contravariant=True)
Resource = TypeVar("Resource", covariant=True)
RouteResultT = TypeVar("RouteResultT", covariant=True)
QueryResult = TypeVar("QueryResult", covariant=True)


class StaticControl(Protocol):
    def get_target(self) -> ConnectionTarget:
        """Read fixed configuration; never create or delete an agent."""
        ...


class ManagedControl(Protocol[CreateRequest, Resource, QueryResult]):
    """YuanRong/ADX adapters retain their existing typed resource contracts.

    CreateRequest carries the platform-specific namespace, physical name,
    workspace, runtime spec, environment and mounts. Resource can be the
    existing SandboxInfo; QueryResult can retain the platform's query shape.
    The manager, not the adapter, owns concurrency and lifecycle decisions.
    Platform errors, including uncertain create and generation mismatch,
    must retain their meaning; do not translate them into an empty result.
    """

    async def create(self, request: CreateRequest) -> Resource: ...

    async def query(self, instance_id: str) -> QueryResult: ...

    async def delete(self, instance_id: str) -> None:
        """Delete the exact retired physical instance, never a logical key.

        The manager fences generations before calling this operation; neither
        platform supports an atomic generation precondition on DELETE.
        """
        ...


class DelegatedControl(Protocol[RouteResultT]):
    """Matches RuntimeSessionRouteClient; reuse its RouteResult unmodified.

    Runtime owns the binding and Pod lifecycle. Route errors propagate with
    their original retry metadata; this interface does not retry or rebind
    implicitly. No dependency on the optional Runtime extension is required.
    """

    async def route(
        self, *, session_id: str, group_id: str, bot_id: str,
        request_id: str, user_id: str | None = None,
    ) -> RouteResultT: ...

    async def touch(self, *, session_id: str, request_id: str) -> bool: ...

    async def rebind(
        self, *, from_session_id: str, to_session_id: str | None, request_id: str,
    ) -> str: ...

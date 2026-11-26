from __future__ import annotations


class NullSessionLoginBindings:
    """No-op MessageHandler login binding for tests outside the auth contract."""

    async def remember(self, session_id: str, credential_ref: str, model_name: str) -> None:
        return None

    async def lookup(self, session_id: str):
        return None

    async def model_auth_for_binding(self, binding):
        return None

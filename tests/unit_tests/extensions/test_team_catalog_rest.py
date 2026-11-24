"""Preserve catalog versus business-session team list semantics."""
import pytest
from e2a.models import E2AEnvelope
from agent_gateway.extensions.runtime_management_extension.pod_rest_map import assemble_rest_request, declared_http_routes


@pytest.mark.parametrize("sid", [None, "shared_user_userz", "webhttp_test"])
def test_catalog_has_no_business_session(sid):
    envelope = E2AEnvelope(method="team.list", request_id="catalog", session_id=sid,
                           user_id="userz", params={"user_id": "userz"})
    result = assemble_rest_request(envelope, base_url="http://pod.invalid:8766")
    assert result.verb == "GET"
    assert result.url.endswith("/api/v1/teams")
    assert "session_id" not in (result.query or {})
    assert result.query["user_id"] == "userz"
    assert ("team.list", "GET", "/teams") in declared_http_routes()


@pytest.mark.parametrize("top", [True, False])
def test_business_session_keeps_session_path(top):
    envelope = E2AEnvelope(method="team.list", request_id="business",
                           session_id="real-session" if top else None,
                           params={} if top else {"session_id": "real-session"})
    result = assemble_rest_request(envelope, base_url="http://pod.invalid:8766")
    assert result.url.endswith("/api/v1/sessions/real-session/teams")

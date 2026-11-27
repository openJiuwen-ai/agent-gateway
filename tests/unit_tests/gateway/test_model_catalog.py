from types import SimpleNamespace

from agent_gateway.auth import model_catalog


def test_get_models_refreshes_when_cache_is_empty(monkeypatch):
    models = [model_catalog.LoginModel(model_name="login-model", display_name="Login")]

    monkeypatch.setattr(model_catalog, "_memo", None)
    monkeypatch.setattr(model_catalog, "_last_discovery_failure_at", 0.0)
    monkeypatch.setattr(model_catalog, "_read_cache", lambda: {})
    monkeypatch.setattr(model_catalog, "get_remote_config", lambda allow_refresh: SimpleNamespace(is_effective=True))
    monkeypatch.setattr(model_catalog, "refresh", lambda session_id, *, service=None: models)

    assert model_catalog.get_models("session", service=None) == models

import json

import pytest

from agent_gateway.runtime.cron import file_migration


def test_migrates_both_cron_files_once(tmp_path, monkeypatch):
    old = tmp_path / "agent" / "cron_jobs.json"
    new = tmp_path / "gateway" / "cron" / "cron_jobs.json"
    old.parent.mkdir()
    old.write_text('{"jobs":[{"id":"existing","last_session_id":"web_old"}]}')
    old.with_name("cron_lifecycle_owners.json").write_text('{"owners":["owner"]}')
    monkeypatch.setattr(file_migration, "get_legacy_cron_jobs_path", lambda: old)

    file_migration.migrate_legacy_cron_files(new)
    assert json.loads(new.read_bytes())["jobs"] == [
        {"id": "existing", "last_session_id": "web_old"},
    ]
    assert json.loads(new.with_name("cron_lifecycle_owners.json").read_bytes()) == {
        "owners": ["owner"],
    }

    old.write_text('{"jobs":[]}')
    file_migration.migrate_legacy_cron_files(new)
    assert json.loads(new.read_bytes())["jobs"][0]["id"] == "existing"


def test_invalid_legacy_data_does_not_bypass_startup(tmp_path, monkeypatch):
    old = tmp_path / "legacy.json"
    new = tmp_path / "gateway" / "cron_jobs.json"
    old.write_text('{"jobs":null}')
    monkeypatch.setattr(file_migration, "get_legacy_cron_jobs_path", lambda: old)

    with pytest.raises(ValueError, match="Invalid legacy cron data"):
        file_migration.migrate_legacy_cron_files(new)

    assert not new.exists()

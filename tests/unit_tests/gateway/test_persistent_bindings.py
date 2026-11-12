"""Contracts that must work in the independent gateway process."""
import hashlib

import pytest
from e2a.message import ReqMethod


def test_bound_dingtalk_apps_keep_separate_robot_and_employee():
    from agent_gateway.channel_manager.im_platforms.dingtalk.configuration import dingtalk_channel_settings

    apps = [{"client_id": app, "client_secret": "secret", "persistent_agent_id": mate}
            for app, mate in [("one", "employee1"), ("two", "employee2")]]
    result = dingtalk_channel_settings({"apps": apps}, api_base="https://api", oapi_base="https://oapi")
    assert [(r["client_id"], r["agent_id"]) for r in result] == [("one", "employee1"), ("two", "employee2")]


@pytest.mark.parametrize("method", ["jiuwenmate.feishu.inbound", "jiuwenmate.dingtalk.inbound",
                                    "jiuwenmate.delivery.ack", "jiuwenmate.thinking_notice.ack",
                                    "jiuwenmate.group.response"])
def test_gateway_wire_method_decodes(method):
    import agent_gateway  # noqa: F401 -- registers supported transport methods
    assert ReqMethod(method).value == method


@pytest.mark.parametrize("partitioned", [True, False])
def test_only_matching_original_session_file_can_be_delivered(tmp_path, monkeypatch, partitioned):
    from agent_gateway.channel_manager import persistent_output

    monkeypatch.setattr(persistent_output, "get_user_workspace_dir", lambda: tmp_path)
    monkeypatch.setattr(persistent_output, "get_domain_data_dir", lambda: tmp_path / "domain")
    key = "local/mate_1/sessions/session_1/output/report.txt"
    root = tmp_path / "domain" if partitioned else tmp_path
    dest = root / "jiuwenmate" / "workspaces" / key
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"ok")
    digest = "sha256:" + hashlib.sha256(b"ok").hexdigest()
    assert persistent_output.resolve_output_file(key, digest, 2) == dest
    assert persistent_output.resolve_output_file(key, digest, 3) is None
    dest.write_bytes(b"no")
    assert persistent_output.resolve_output_file(key, digest, 2) is None
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "report.txt").write_bytes(b"ok")
    dest.unlink()
    dest.parent.rmdir()
    dest.parent.symlink_to(outside, target_is_directory=True)
    assert persistent_output.resolve_output_file(key, digest, 2) is None
    assert persistent_output.resolve_output_file(key.replace("session_1", ".."), digest, 2) is None

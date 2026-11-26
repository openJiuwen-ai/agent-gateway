import subprocess
import sys

from agent_gateway.channel_manager.web import dialog_capture


def test_run_dialog_capture_forwards_subprocess_options(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(dialog_capture.subprocess, "run", fake_run)

    dialog_capture.run_dialog_capture(["zenity"], timeout=12.5)

    assert calls == [
        (
            ["zenity"],
            {
                "check": False,
                "capture_output": True,
                "text": True,
                "encoding": "utf-8",
                "errors": "replace",
                "timeout": 12.5,
                "env": None,
            },
        )
    ]


def test_run_dialog_capture_decodes_utf8_path_and_replaces_invalid_bytes():
    data = "目录/文件.txt".encode("utf-8") + b"\xff"
    script = f"import sys; sys.stdout.buffer.write({data!r})"

    completed = dialog_capture.run_dialog_capture([sys.executable, "-c", script])

    assert completed.returncode == 0
    assert completed.stdout == "目录/文件.txt\ufffd"


def test_run_dialog_capture_forwards_env(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(kwargs)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(dialog_capture.subprocess, "run", fake_run)
    env = {"PYTHONIOENCODING": "utf-8"}

    dialog_capture.run_dialog_capture(["python", "-c", "pass"], env=env)

    assert calls[0]["env"] is env
    assert calls[0]["timeout"] == 600.0


def test_picker_tkinter_wrappers_use_shared_capture(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(kwargs)
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(dialog_capture.subprocess, "run", fake_run)

    from agent_gateway.channel_manager.web import directory_picker, file_picker

    assert directory_picker.run_dialog_capture is dialog_capture.run_dialog_capture
    assert file_picker.run_dialog_capture is dialog_capture.run_dialog_capture
    assert directory_picker._try_tkinter_subprocess(".") is None
    assert file_picker._try_tkinter_subprocess_files(initial_dir=".", allow_multiple=False) is None

    assert len(calls) == 2
    for kwargs in calls:
        assert kwargs["encoding"] == "utf-8"
        assert kwargs["errors"] == "replace"
        assert kwargs["timeout"] == 600.0
        assert kwargs["env"]["PYTHONIOENCODING"] == "utf-8"

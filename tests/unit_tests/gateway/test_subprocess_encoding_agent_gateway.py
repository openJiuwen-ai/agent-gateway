# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Guard: text-mode ``subprocess`` calls in ``agent_gateway`` pin a codec.

Without an explicit ``encoding``, ``text=True`` decodes the child with
``io.text_encoding(None)``: the locale codec, or ``utf-8`` once Python runs in
UTF-8 mode (``uv run`` sets ``PYTHONUTF8=1``). A localized console tool keeps
emitting ANSI/OEM bytes, the ``UnicodeDecodeError`` fires inside subprocess'
reader thread, and ``stdout`` comes back ``None`` -- the silent failure behind
``GatewayLock`` reporting a live holder as dead. An ``encoding`` alone still
raises on mismatched bytes, hence the tolerant ``errors`` assertion too.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SAFE_ERROR_HANDLERS = frozenset({"replace", "ignore", "backslashreplace"})
TEXT_SUBPROCESS_CALLS = frozenset({"run", "Popen", "check_output", "call", "check_call"})


def _keyword(call: ast.Call, name: str) -> ast.AST | None:
    return next((k.value for k in call.keywords if k.arg == name), None)


@pytest.mark.unit
def test_text_mode_subprocess_calls_pin_encoding_and_errors() -> None:
    offenders: list[str] = []
    for path in sorted((ROOT / "agent_gateway").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = path.relative_to(ROOT).as_posix()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or getattr(node.func, "attr", None) not in TEXT_SUBPROCESS_CALLS:
                continue
            text_kw = _keyword(node, "text") or _keyword(node, "universal_newlines")
            if not (isinstance(text_kw, ast.Constant) and text_kw.value is True):
                continue
            if _keyword(node, "encoding") is None:
                offenders.append(f"{rel}:{node.lineno} (missing encoding=)")
            elif getattr(_keyword(node, "errors"), "value", None) not in SAFE_ERROR_HANDLERS:
                offenders.append(f"{rel}:{node.lineno} (tolerant errors= missing)")

    assert not offenders, (
        "subprocess text-mode calls must pin 'encoding=' plus a tolerant 'errors=' "
        "(one of " + ", ".join(sorted(SAFE_ERROR_HANDLERS)) + "): " + ", ".join(offenders)
    )

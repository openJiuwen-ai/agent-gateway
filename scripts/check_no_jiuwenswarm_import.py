#!/usr/bin/env python3
"""Fail when the agent-gateway package imports the jiuwenswarm package.

默认只扫描随包发布的 ``agent_gateway/``；测试与脚本需显式 ``--include-tests``。
支持静态 import 与常量字符串形式的动态 import，命中即退出码 1。
基线模式下只允许存量减少：基线外新增即失败，基线内已修复仅提示。
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

FORBIDDEN_PREFIX = "jiuwenswarm"
DYNAMIC_IMPORT_CALLS = {"import_module", "__import__"}
SKIP_PARTS = {"__pycache__", ".venv", "build", "dist"}


def is_forbidden(module: str) -> bool:
    return module == FORBIDDEN_PREFIX or module.startswith(FORBIDDEN_PREFIX + ".")


def iter_sources(root: Path, include_tests: bool) -> list[Path]:
    bases = [root / "agent_gateway"]
    if include_tests:
        bases.append(root / "tests")
    files: list[Path] = []
    for base in bases:
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            files.append(path)
    return sorted(files)


def scan_source(text: str) -> list[tuple[int, str]]:
    """返回 (行号, 模块名) 列表；语法错误按 0 行上报。"""
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        return [(getattr(exc, "lineno", 0) or 0, "<syntax-error>")]

    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if is_forbidden(alias.name):
                    hits.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module and is_forbidden(module):
                hits.append((node.lineno, module))
        elif isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else (func.id if isinstance(func, ast.Name) else "")
            )
            if name not in DYNAMIC_IMPORT_CALLS or not node.args:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str) \
                    and is_forbidden(arg.value):
                hits.append((node.lineno, arg.value))
    return hits


def collect(root: Path, include_tests: bool) -> list[dict]:
    rows: list[dict] = []
    for path in iter_sources(root, include_tests):
        rel = path.relative_to(root).as_posix()
        for line, module in scan_source(path.read_text(encoding="utf-8")):
            rows.append({"file": rel, "line": line, "module": module})
    return rows


def entry_key(row: dict) -> str:
    return f"{row['file']}|{row['module']}"


def load_baseline(path: Path) -> dict[str, dict]:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {entry_key(row): row for row in data.get("entries", [])}


def write_baseline(path: Path, rows: list[dict]) -> None:
    unique = {entry_key(row): row for row in rows}
    payload = {
        "generated_by": "scripts/check_no_jiuwenswarm_import.py",
        "entries": [
            {"file": unique[key]["file"], "module": unique[key]["module"]}
            for key in sorted(unique)
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="agent-gateway 禁止 import jiuwenswarm（静态依赖门禁）"
    )
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1],
                        help="agent-gateway 仓库根（默认脚本上一级）")
    parser.add_argument("--include-tests", action="store_true",
                        help="同时扫描 tests/（默认只扫 agent_gateway/）")
    parser.add_argument("--baseline", type=Path,
                        help="基线文件：仅允许基线内存量，出现新增即失败")
    parser.add_argument("--update-baseline", type=Path,
                        help="把当前命中写入基线文件后退出 0")
    parser.add_argument("--json", action="store_true", help="JSON 输出")
    args = parser.parse_args()

    root: Path = args.root.resolve()
    if not (root / "agent_gateway").exists():
        print(f"[ERROR] 未找到 {root / 'agent_gateway'}", file=sys.stderr)
        return 2

    rows = collect(root, args.include_tests)

    if args.update_baseline:
        write_baseline(args.update_baseline, rows)
        print(f"[baseline] {len(rows)} 处命中写入 {args.update_baseline}")
        return 0

    baseline = load_baseline(args.baseline) if args.baseline else {}
    current = {entry_key(row): row for row in rows}
    added = [row for key, row in current.items() if key not in baseline]
    fixed = sorted(key for key in baseline if key not in current)

    if args.json:
        print(json.dumps({
            "root": str(root),
            "include_tests": args.include_tests,
            "total": len(rows),
            "added": added,
            "fixed": fixed,
            "entries": rows,
        }, ensure_ascii=False, indent=2))
    else:
        scope = "agent_gateway/ + tests/" if args.include_tests else "agent_gateway/"
        print(f"扫描范围: {scope}")
        print(f"命中 {len(rows)} 处" + (f"，其中基线外新增 {len(added)} 处" if args.baseline else ""))
        for row in added:
            print(f"  {row['file']}:{row['line']}  {row['module']}")
        if fixed:
            print(f"基线内已修复 {len(fixed)} 处（可更新基线）：")
            for key in fixed:
                print(f"  {key}")

    return 1 if added else 0


if __name__ == "__main__":
    sys.exit(main())

# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ST 流水线辅助脚本：把 shell 里冗长或易碎的步骤收敛为带明确输入输出的子命令。

由 run_st_jiuwen.sh 调用，只依赖标准库，不需要安装本包：

  cases [--root DIR]                          输出 jiuwen-test 用例路径（空格分隔）
  config [--env PATH] [--yaml PATH]           启用 swarmflow 并追加第二模型
  wait-ports PORT... [--pid N] [--timeout S]  等待端口全部可连
  probe [--env PATH]                          探测 ${API_BASE}/models 可达性
  result --rc-a N --rc-b N --link URL --out P 汇总两阶段结果写 st_result.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# jiuwen-test 固定用例清单：显式路径，避免通配符被上游目录调整带偏。
CASES = (
    "testcases/02_集群模式对话和团队协作/chat_send/test_action_003_plain_single_round_code.py",
    "testcases/02_集群模式对话和团队协作/swarmflow/swarmflow_resume_workflows/"
    "test_chain_chain_008_pause_resume_stop_run.py",
    "testcases/09_定时任务与心跳/cron_job_update/test_chain_001_create_update_get_delete_notfound.py",
    "testcases/09_定时任务与心跳/cron_job_toggle/test_chain_003_preview_full_chain.py",
    "testcases/09_定时任务与心跳/cron_job_run_now/test_chain_001_run_get_sessions_loop.py",
)

# 第二模型条目：占位符由 jiuwenswarm 启动时从 .env 展开。
MODEL2_BLOCK = (
    "    - model_client_config:\n"
    "        api_base: ${MODEL2_API_BASE}\n"
    "        api_key: ${MODEL2_API_KEY}\n"
    "        model_name: ${MODEL2_MODEL_NAME}\n"
    "        client_provider: OpenAI\n"
    "        endpoint_profile: openai_compatible\n"
    "      is_default: false\n"
    "      alias: ${MODEL2_MODEL_NAME}\n"
)

DEFAULT_ENV = Path("~/.jiuwenswarm/config/.env").expanduser()

# 只匹配键行，不碰注释里的同名字样。
_SWARMFLOW_LINE = re.compile(r"^(\s*)enable_swarmflow:\s*false\s*$", re.MULTILINE)


def cmd_cases(args: argparse.Namespace) -> int:
    root = Path(args.root or ".")
    missing = [case for case in CASES if not (root / case).is_file()]
    if missing:
        print("missing case: " + ", ".join(missing), file=sys.stderr)
        return 1
    print(" ".join(CASES))
    return 0


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


_PARSE_ERROR = object()


def _load_yaml(text: str):
    """有 PyYAML 时返回解析结果；无 PyYAML 返回 None，解析失败返回哨兵。"""
    try:
        import yaml
    except ImportError:
        return None
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return _PARSE_ERROR


def _defaults(data) -> list:
    models = data.get("models") if isinstance(data, dict) else None
    defaults = models.get("defaults") if isinstance(models, dict) else None
    return defaults if isinstance(defaults, list) else []


def _check_config(before, after, inserted: bool) -> str:
    """用结构校验兜住文本编辑，返回空串表示通过。"""
    if not isinstance(after, dict):
        return "config.yaml 写入后无法解析"
    if inserted and len(_defaults(after)) != len(_defaults(before)) + 1:
        return "第二模型条目未写入 models.defaults"
    team = ((after.get("modes") or {}).get("team") or {}).get("jiuwen_team") or {}
    if "enable_swarmflow" in team and team["enable_swarmflow"] is not True:
        return "enable_swarmflow 未生效"
    return ""


def cmd_config(args: argparse.Namespace) -> int:
    env_path = Path(args.env).expanduser()
    yaml_path = (
        Path(args.yaml).expanduser() if args.yaml else env_path.parent / "config.yaml"
    )
    if not yaml_path.is_file():
        print(f"missing config: {yaml_path}", file=sys.stderr)
        return 1
    text = yaml_path.read_text(encoding="utf-8")
    before = _load_yaml(text)
    # 只替换键行，避免子串匹配误伤注释里的同名字样。
    text = _SWARMFLOW_LINE.sub(r"\1enable_swarmflow: true", text, count=1)
    inserted = "${MODEL2_MODEL_NAME}" not in text
    if inserted:
        lines = text.splitlines(keepends=True)
        # 与旧 awk 一致：models.defaults 之后、下一个顶层键之前追加条目。
        in_models = False
        in_defaults = False
        insert_at = len(lines)
        for index, line in enumerate(lines):
            if not in_models and line.startswith("models:"):
                in_models = True
                continue
            if in_models and not in_defaults:
                if line.rstrip() == "  defaults:":
                    in_defaults = True
                continue
            if in_defaults and line.strip() and not line.startswith("    "):
                insert_at = index
                break
        lines.insert(insert_at, MODEL2_BLOCK)
        text = "".join(lines)
    after = _load_yaml(text)
    if after is _PARSE_ERROR:
        print(f"config.yaml 写入后无法解析: {yaml_path}", file=sys.stderr)
        return 1
    if isinstance(before, dict) and isinstance(after, dict):
        error = _check_config(before, after, inserted)
        if error:
            print(f"{error}: {yaml_path}", file=sys.stderr)
            return 1
    yaml_path.write_text(text, encoding="utf-8")
    print(f"config updated: {yaml_path}")
    return 0


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _connect(port: int, host: str = "127.0.0.1", timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def cmd_wait_ports(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout
    pending = list(args.ports)
    while pending and time.monotonic() < deadline:
        if args.pid and not _alive(args.pid):
            print("start exited", file=sys.stderr)
            return 2
        pending = [port for port in pending if not _connect(port)]
        if pending:
            time.sleep(0.5)
    if pending:
        print(f"port {pending[0]} not ready", file=sys.stderr)
        return 1
    return 0


def cmd_probe(args: argparse.Namespace) -> int:
    env_path = Path(args.env).expanduser()
    values = parse_env(env_path) if env_path.is_file() else {}
    base = args.base or values.get("API_BASE", "")
    key = args.key or values.get("API_KEY", "")
    if not base:
        print("API_BASE is empty", file=sys.stderr)
        return 1
    request = urllib.request.Request(
        base.rstrip("/") + "/models",
        headers={"Authorization": f"Bearer {key}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            code = response.status
    except urllib.error.HTTPError as exc:
        code = exc.code
    except OSError as exc:
        print(f"model HTTP 000 ({exc})")
        return 1
    print(f"model HTTP {code}")
    return 0


def cmd_result(args: argparse.Namespace) -> int:
    status = "PASS" if args.rc_a == 0 and args.rc_b == 0 else "FAILED"
    # 仅 A 阶段失败时把链接指向它的报告，避免评论里链到全绿的 B 阶段报告。
    link = args.link
    if args.rc_a != 0 and args.rc_b == 0 and args.link_a:
        link = args.link_a
    payload = json.dumps({"status": status, "link": link}) + "\n"
    Path(args.out).write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="agent-gateway ST 流水线辅助")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cases = subparsers.add_parser("cases", help="输出 jiuwen-test 固定用例清单")
    cases.add_argument("--root", default="", help="用例所在目录，默认当前目录")
    cases.set_defaults(func=cmd_cases)

    config = subparsers.add_parser("config", help="按 .env 改写 config.yaml")
    config.add_argument("--env", default=str(DEFAULT_ENV))
    config.add_argument("--yaml", default="")
    config.set_defaults(func=cmd_config)

    wait_ports = subparsers.add_parser("wait-ports", help="等待端口全部可连")
    wait_ports.add_argument("ports", type=int, nargs="+")
    wait_ports.add_argument("--pid", type=int, default=0, help="服务进程，退出即判失败")
    wait_ports.add_argument("--timeout", type=float, default=120.0)
    wait_ports.set_defaults(func=cmd_wait_ports)

    probe = subparsers.add_parser("probe", help="探测模型端点")
    probe.add_argument("--env", default=str(DEFAULT_ENV))
    probe.add_argument("--base", default="")
    probe.add_argument("--key", default="")
    probe.add_argument("--timeout", type=float, default=15.0)
    probe.set_defaults(func=cmd_probe)

    result = subparsers.add_parser("result", help="汇总两阶段结果")
    result.add_argument("--rc-a", type=int, required=True)
    result.add_argument("--rc-b", type=int, required=True)
    result.add_argument("--link", required=True)
    result.add_argument("--link-a", default="", help="A 阶段失败时改用该报告链接")
    result.add_argument("--out", required=True)
    result.set_defaults(func=cmd_result)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""In-process A2A-style collaboration for Designer specialist roles.

OpenJiuwen's Gateway ``A2AChannel`` is inbound HTTP (external client → chat
agent). Outbound A2A is not wired. Designer nodes need peer talk without a
chat session or ``A2A_SERVER_ENABLED``.

This module keeps the A2A message shape (``message_id`` / ``context_id`` /
``task_id`` / ``parts``) and runs specialist personas over
``complete_designer_text``. The executor calls it before a wave that contains
two or more collaboratable roles (character + scene). Handlers then read the
aligned cards instead of each inventing the world from Brief alone.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_BRIEF,
    NODE_ROLE_CHARACTER_DESIGN,
    NODE_ROLE_SCENE,
    NODE_ROLE_STORYBOARD,
    DesignerExecutionGraph,
    DesignerExecutionRun,
    node_config,
    node_role,
)
from jiuwenswarm.common.utils import get_agent_workspace_dir

logger = logging.getLogger(__name__)

COLLAB_ROLES: frozenset[str] = frozenset(
    {
        NODE_ROLE_CHARACTER_DESIGN,
        NODE_ROLE_SCENE,
    }
)

_SPECIALISTS: dict[str, tuple[str, str]] = {
    NODE_ROLE_CHARACTER_DESIGN: (
        "角色设计师",
        "你是影视角色设计师。只写外形、服装、材质、体态和辨识特征。"
        "不要写分镜表，不要安排镜头。用中文短 Markdown。",
    ),
    NODE_ROLE_SCENE: (
        "场景美术指导",
        "你是场景美术指导。只写空间、天气、光线、招牌、地面和道具。"
        "不要出现人物，不要写分镜表。用中文短 Markdown。",
    ),
    NODE_ROLE_STORYBOARD: (
        "分镜导演",
        "你是分镜导演。保证人物连续、场景地理连续，镜头服务 5 秒短片。",
    ),
}


@dataclass
class DesignerA2APart:
    text: str


@dataclass
class DesignerA2AMessage:
    """Subset of A2A SendMessage used inside Designer."""

    message_id: str
    context_id: str
    task_id: str
    sender: str
    recipient: str
    parts: list[DesignerA2APart] = field(default_factory=list)

    def text(self) -> str:
        return "\n".join(part.text for part in self.parts if part.text).strip()


class DesignerA2ABus:
    """In-process mailbox. One bus per run (``context_id`` = ``run_id``)."""

    def __init__(self, context_id: str) -> None:
        self.context_id = context_id
        self.messages: list[DesignerA2AMessage] = []

    def record(self, message: DesignerA2AMessage) -> DesignerA2AMessage:
        self.messages.append(message)
        return message

    def send(
        self,
        *,
        sender: str,
        recipient: str,
        text: str,
        task_id: str,
    ) -> DesignerA2AMessage:
        return self.record(
            DesignerA2AMessage(
                message_id=f"a2a_{uuid4().hex[:12]}",
                context_id=self.context_id,
                task_id=task_id,
                sender=sender,
                recipient=recipient,
                parts=[DesignerA2APart(text=text)],
            )
        )

    def transcript_markdown(self) -> str:
        lines = ["# Designer A2A 协作记录", ""]
        for item in self.messages:
            lines.append(f"## {item.sender} → {item.recipient}")
            lines.append("")
            lines.append(item.text() or "_(empty)_")
            lines.append("")
        return "\n".join(lines).strip() + "\n"


def collaboration_card_path(run_id: str, role: str) -> Path:
    return get_agent_workspace_dir() / f"designer_a2a_{run_id}_{role}.md"


def collaboration_transcript_path(run_id: str) -> Path:
    return get_agent_workspace_dir() / f"designer_a2a_{run_id}_transcript.md"


def collaboration_card(run_id: str, role: str) -> str:
    path = collaboration_card_path(run_id, role)
    if not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return ""


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path.resolve()


async def _complete(prompt: str, *, max_tokens: int = 800) -> str:
    from jiuwenswarm.server.runtime.designer.handlers.common import complete_designer_text

    try:
        text = await complete_designer_text(prompt, max_tokens=max_tokens)
    except Exception as exc:  # noqa: BLE001
        logger.debug("designer A2A complete failed: %s", exc)
        return ""
    return str(text or "").strip()


async def ask_specialist(role: str, task: str, *, max_tokens: int = 800) -> str:
    name, system = _SPECIALISTS.get(role, (role, "你是 Designer 专家。"))
    return await _complete(
        f"{system}\n\n你的身份：{name}（role={role}）。\n\n{task}\n",
        max_tokens=max_tokens,
    )


def _brief_text(graph: DesignerExecutionGraph, run: DesignerExecutionRun) -> str:
    from jiuwenswarm.server.runtime.designer.handlers.common import path_from_uri

    states = run.get("node_states") or {}
    for node in graph.get("nodes") or []:
        if node_role(node) != NODE_ROLE_BRIEF:
            continue
        ref = (states.get(node["id"]) or {}).get("output_ref") or {}
        path = path_from_uri(str(ref.get("uri") or ""))
        if path is not None and path.is_file():
            try:
                text = path.read_text(encoding="utf-8").strip()
            except (OSError, UnicodeDecodeError):
                text = ""
            if text:
                return text
        prompt = str((node.get("config") or {}).get("prompt") or "").strip()
        if prompt:
            return prompt
    return str(graph.get("description") or "").strip()


def _collab_roles_for_wave(
    graph: DesignerExecutionGraph,
    ready_ids: Iterable[str],
) -> list[str]:
    ready = set(ready_ids)
    roles: list[str] = []
    for node in graph.get("nodes") or []:
        if node["id"] not in ready:
            continue
        role = node_role(node)
        if role not in COLLAB_ROLES:
            continue
        if node_config(node).get("collaborate") is False:
            continue
        if role not in roles:
            roles.append(role)
    return roles


async def align_specialists(
    brief: str,
    roles: list[str],
    *,
    run_id: str,
) -> dict[str, str]:
    """Draft in parallel, then one A2A cross-talk round per pair."""
    bus = DesignerA2ABus(context_id=run_id)
    task_id = f"align_{run_id}"
    brief = brief.strip() or "未提供 Brief"
    drafts: dict[str, str] = {}

    async def _draft(role: str) -> tuple[str, str]:
        text = await ask_specialist(
            role,
            "根据下面的 Brief 写一份可执行设定卡，10～20 行。\n\nBrief：\n" + brief,
        )
        bus.send(sender=role, recipient="director", text=text or f"（{role} 草稿为空）", task_id=task_id)
        return role, text

    for role, text in await asyncio.gather(*(_draft(role) for role in roles)):
        drafts[role] = text

    for sender, recipient in combinations(roles, 2):
        for src, dst in ((sender, recipient), (recipient, sender)):
            peer = drafts.get(src) or ""
            mine = drafts.get(dst) or ""
            reply = await ask_specialist(
                dst,
                "同事用 A2A 发来设定，指出和你冲突的地方，列出你必须坚持的 3～6 条约束。"
                "不要重写整份 Brief。\n\n"
                f"同事（{src}）设定：\n{peer}\n\n你的设定：\n{mine}\n",
                max_tokens=600,
            )
            bus.send(sender=src, recipient=dst, text=peer, task_id=task_id)
            bus.send(sender=dst, recipient=src, text=reply or "无冲突", task_id=task_id)
            if reply:
                drafts[dst] = (
                    f"{mine}\n\n## 与 {src} 对齐后必须遵守\n\n{reply}".strip()
                    if mine
                    else reply
                )

    for role, text in drafts.items():
        if text:
            _write_text(collaboration_card_path(run_id, role), text)
    _write_text(collaboration_transcript_path(run_id), bus.transcript_markdown())
    return {role: text for role, text in drafts.items() if text}


async def collaborate_ready_wave(
    graph: DesignerExecutionGraph,
    run: DesignerExecutionRun,
    ready_ids: list[str],
) -> dict[str, str]:
    """Align specialist peers before a parallel wave generates media."""
    roles = _collab_roles_for_wave(graph, ready_ids)
    if len(roles) < 2:
        return {}
    run_id = str(run.get("run_id") or "")
    if not run_id:
        return {}
    existing = {role: collaboration_card(run_id, role) for role in roles}
    if all(existing.values()):
        return existing
    brief = _brief_text(graph, run)
    try:
        return await align_specialists(brief, roles, run_id=run_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("designer A2A collaboration skipped: %s", exc)
        return {}


async def review_storyboard_with_peers(table: str, *, run_id: str) -> str:
    """Let character / scene agents comment on a drafted storyboard table."""
    if not table.strip():
        return table
    character = collaboration_card(run_id, NODE_ROLE_CHARACTER_DESIGN)
    scene = collaboration_card(run_id, NODE_ROLE_SCENE)
    if not character and not scene:
        return table
    notes: list[str] = []
    if character:
        reply = await ask_specialist(
            NODE_ROLE_CHARACTER_DESIGN,
            "审这份分镜表的「人物变化」列。指出换装、换人、外形不一致。"
            "没有问题就回「OK」。\n\n"
            f"你的角色卡：\n{character}\n\n分镜表：\n{table}\n",
            max_tokens=400,
        )
        if reply and reply.strip().upper() != "OK":
            notes.append("角色设计师：\n" + reply)
    if scene:
        reply = await ask_specialist(
            NODE_ROLE_SCENE,
            "审这份分镜表的「场景变化」列。指出换地点、换天气、光线体系崩了。"
            "没有问题就回「OK」。\n\n"
            f"你的场景卡：\n{scene}\n\n分镜表：\n{table}\n",
            max_tokens=400,
        )
        if reply and reply.strip().upper() != "OK":
            notes.append("场景美术：\n" + reply)
    if not notes:
        return table
    revised = await ask_specialist(
        NODE_ROLE_STORYBOARD,
        "按同事 A2A 意见改分镜表。必须保留表头："
        "镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 | 注释。\n"
        "只输出 Markdown 表，不要解释。\n\n"
        f"原表：\n{table}\n\n意见：\n" + "\n\n".join(notes),
        max_tokens=1600,
    )
    return revised or table

# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Text intermediate handlers: brief, storyboard (includes camera script)."""

from __future__ import annotations

import re
from typing import TypedDict

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_BRIEF,
    NODE_ROLE_CHARACTER_DESIGN,
    NODE_ROLE_SCENE,
    NODE_TYPE_TABLE,
    NODE_TYPE_TEXT,
    DesignerGraphNode,
    node_config,
)
from jiuwenswarm.server.runtime.designer.handlers.common import (
    file_output_ref,
    graph_prompt,
    role_output_image_path,
    role_output_text,
    write_workspace_text,
)
from jiuwenswarm.server.runtime.designer.a2a_collab import (
    collaboration_card,
    review_storyboard_with_peers,
)
from jiuwenswarm.server.runtime.designer.subagent import complete_designer_node_text
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext, NodeResult

_BRIEF_INSTRUCTION = """把下面的创作需求整理成一份可执行的短片 Brief。
用中文 Markdown，包含：一句话 logline、视觉风格、主要角色/主体、场景、时长约束（5秒）、不要出现的内容。
只输出 Markdown，不要解释。

需求：
"""

_STORYBOARD_COLUMNS = "镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 | 注释"

_STORYBOARD_INSTRUCTION = """根据 Brief 写一份 5 秒短片的分镜表。这是摄影脚本表格，不是图画。
用中文 Markdown，必须包含下面这个二级标题和一张表：

## 分镜表

用 Markdown 表格，列必须是：
镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 | 注释

约束：
- 全片合计约 5 秒，2～4 个镜头
- 时间轴写起止秒，例如 0.0-2.0s
- 镜头视角写景别+机位，例如 全景/略俯、中景/平视
- 运镜写推/拉/摇/移/固定及速度
- 人物变化必须与角色图/角色设定对齐：同一主体、同一外形服装材质，只写该镜里的动作、朝向、进出画
- 场景变化必须与场景图/场景设定对齐：同一地点和天气光线体系，只写该镜里环境、道具、背景如何变
- 注释写这一镜关键帧的画面描述，作为后续生图 prompt：主体、构图、光线、动作瞬间、环境细节。写成可直接生图的中文，不要只重复其它列
- 不要另造角色，不要换场景世界观

不要输出分镜图画，不要解释。

Brief：
"""

_MAX_STORYBOARD_SHOTS = 6
_TABLE_SEP_CELL = re.compile(r"^:?-{3,}:?$")


class StoryboardShot(TypedDict):
    shot_no: str
    timeline: str
    camera: str
    move: str
    character_action: str
    scene_change: str
    comment: str


_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "shot_no": ("镜号",),
    "timeline": ("时间轴",),
    "camera": ("镜头视角", "景别"),
    "move": ("运镜",),
    "character_action": ("人物变化",),
    "scene_change": ("场景变化",),
    "comment": ("注释", "备注", "画面描述", "提示词"),
}
_POSITIONAL_FIELDS = (
    "shot_no",
    "timeline",
    "camera",
    "move",
    "character_action",
    "scene_change",
    "comment",
)


def _split_markdown_row(line: str) -> list[str]:
    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [cell.strip() for cell in text.split("|")]


def _empty_shot() -> StoryboardShot:
    return {
        "shot_no": "",
        "timeline": "",
        "camera": "",
        "move": "",
        "character_action": "",
        "scene_change": "",
        "comment": "",
    }


def _header_field_map(cells: list[str]) -> dict[int, str] | None:
    mapping: dict[int, str] = {}
    for index, cell in enumerate(cells):
        name = cell.strip()
        if not name:
            continue
        for field, aliases in _FIELD_ALIASES.items():
            if any(alias == name or alias in name for alias in aliases):
                mapping[index] = field
                break
    if "shot_no" in mapping.values() or "timeline" in mapping.values():
        return mapping
    return None


def _shot_from_cells(
    cells: list[str],
    field_map: dict[int, str] | None,
    fallback_no: int,
) -> StoryboardShot | None:
    shot = _empty_shot()
    if field_map:
        for index, field in field_map.items():
            if index < len(cells):
                shot[field] = cells[index]
    else:
        for index, field in enumerate(_POSITIONAL_FIELDS):
            if index < len(cells):
                shot[field] = cells[index]
    if not shot["shot_no"]:
        shot["shot_no"] = str(fallback_no)
    if not re.match(r"^\d+", shot["shot_no"]) and len(cells) < 4:
        return None
    return shot


def parse_storyboard_shots(text: str) -> list[StoryboardShot]:
    """Read shot rows from the storyboard markdown table."""
    shots: list[StoryboardShot] = []
    header_seen = False
    field_map: dict[int, str] | None = None
    for line in (text or "").splitlines():
        if "|" not in line:
            continue
        cells = _split_markdown_row(line)
        if not cells or not any(cells):
            continue
        if all(_TABLE_SEP_CELL.match(cell) for cell in cells if cell):
            continue
        joined = "".join(cells)
        if not header_seen and ("镜号" in joined or "时间轴" in joined):
            header_seen = True
            field_map = _header_field_map(cells)
            continue
        if not header_seen:
            continue
        shot = _shot_from_cells(cells, field_map, len(shots) + 1)
        if shot is None:
            continue
        shots.append(shot)
        if len(shots) >= _MAX_STORYBOARD_SHOTS:
            break
    return shots


def storyboard_shots_or_default(text: str, prompt: str = "") -> list[StoryboardShot]:
    shots = parse_storyboard_shots(text)
    if shots:
        return shots
    return parse_storyboard_shots(fallback_storyboard(prompt))


def shot_generate_prompt(shot: StoryboardShot) -> str:
    """Turn one storyboard row into the keyframe/clip generate prompt."""
    comment = str(shot.get("comment") or "").strip()
    if comment:
        return comment
    parts: list[str] = []
    timeline = str(shot.get("timeline") or "").strip()
    if timeline:
        parts.append(f"时间轴 {timeline}")
    for label, key in (
        ("镜头视角", "camera"),
        ("运镜", "move"),
        ("人物变化", "character_action"),
        ("场景变化", "scene_change"),
    ):
        value = str(shot.get(key) or "").strip()
        if value:
            parts.append(f"{label} {value}")
    return "；".join(parts)


def fallback_brief(prompt: str) -> str:
    return (
        "# Brief\n\n"
        f"- Logline：{prompt}\n"
        "- 时长：5 秒\n"
        "- 分辨率：480P（开发阶段）\n"
        "- 视觉：按用户描述执行，避免无关元素\n"
    )


def fallback_storyboard(prompt: str) -> str:
    return (
        "# 分镜表\n\n"
        "## 分镜表\n\n"
        f"| {_STORYBOARD_COLUMNS} |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
        f"| 1 | 0.0-2.0s | 全景/略俯 | 从积水倒影缓摇至主体 | 主体尚未入画或仅见倒影 | 建立场景：{prompt[:80]} | 雨夜霓虹巷的积水倒影，全景略俯，尚未见到主体 |\n"
        "| 2 | 2.0-5.0s | 中景/平视 | 跟移后停 | 主体进入并完成一个明确动作 | 霓虹与积水倒影随镜头碎开 | 中景平视，主体入画完成一个明确动作，霓虹与积水倒影碎开 |\n"
    )


class BriefNodeHandler:
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        source = graph_prompt(ctx.graph, node)
        try:
            text = await complete_designer_node_text(
                _BRIEF_INSTRUCTION + source,
                delegate=str(node_config(node).get("delegate") or ""),
            )
        except Exception:
            text = ""
        if not text:
            text = fallback_brief(source)
        path = write_workspace_text(f"designer_brief_{ctx.run_id}_{ctx.node_id}", text)
        return NodeResult(
            output_ref=file_output_ref(path, kind=NODE_TYPE_TEXT, mime_type="text/markdown"),
            message="brief written",
        )


def _storyboard_alignment_context(ctx: NodeExecutionContext) -> str:
    parts: list[str] = []
    character_notes = (
        collaboration_card(ctx.run_id, NODE_ROLE_CHARACTER_DESIGN)
        or role_output_text(ctx, NODE_ROLE_CHARACTER_DESIGN)
    )
    scene_notes = (
        collaboration_card(ctx.run_id, NODE_ROLE_SCENE)
        or role_output_text(ctx, NODE_ROLE_SCENE)
    )
    if character_notes:
        parts.append("角色图/角色设定（人物变化必须与此对齐）：\n" + character_notes)
    elif role_output_image_path(ctx, NODE_ROLE_CHARACTER_DESIGN) is not None:
        parts.append("已有角色图。人物变化必须与该角色的外貌、服装和材质对齐，不要另造角色。")
    if scene_notes:
        parts.append("场景图/场景设定（场景变化必须与此对齐）：\n" + scene_notes)
    elif role_output_image_path(ctx, NODE_ROLE_SCENE) is not None:
        parts.append("已有场景图。场景变化必须与该环境的空间、天气和光线对齐，不要换地点。")
    return "\n\n".join(parts)


class StoryboardNodeHandler:
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        source = role_output_text(ctx, NODE_ROLE_BRIEF) or graph_prompt(ctx.graph, node)
        alignment = _storyboard_alignment_context(ctx)
        prompt = _STORYBOARD_INSTRUCTION + source
        if alignment:
            prompt = f"{prompt}\n\n{alignment}\n"
        try:
            text = await complete_designer_node_text(
                prompt,
                delegate=str(node_config(node).get("delegate") or ""),
                max_tokens=1600,
            )
        except Exception:
            text = ""
        if not text:
            text = fallback_storyboard(source)
        try:
            text = await review_storyboard_with_peers(text, run_id=ctx.run_id)
        except Exception:
            pass
        path = write_workspace_text(f"designer_storyboard_{ctx.run_id}_{ctx.node_id}", text)
        return NodeResult(
            output_ref=file_output_ref(path, kind=NODE_TYPE_TABLE, mime_type="text/markdown"),
            message="storyboard table written",
        )

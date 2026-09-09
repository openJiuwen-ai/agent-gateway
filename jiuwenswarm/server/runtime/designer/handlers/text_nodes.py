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

_BRIEF_INSTRUCTION = """Turn the request below into an executable short-film Brief.
Write English Markdown with: one-line logline, visual style, main character/subject, setting, 5-second duration, and what to avoid.
Output Markdown only, no explanation.

Request:
"""

_STORYBOARD_COLUMNS = "Shot | Timeline | Camera | Move | Character action | Scene change | Comment"

_STORYBOARD_INSTRUCTION = """Write a 5-second storyboard from the Brief. This is a camera script table, not a drawing.
Use English Markdown. Include this heading and one table:

## Storyboard

Use a Markdown table whose columns MUST be:
Shot | Timeline | Camera | Move | Character action | Scene change | Comment

Rules:
- Whole film about 5 seconds, 2-4 shots
- Timeline as start-end seconds, e.g. 0.0-2.0s
- Camera is shot size + angle, e.g. wide/slight high, medium/eye-level
- Move is push/pull/pan/dolly/static and speed
- Character action must match the character sheet: same subject, look, costume, materials; only write motion, facing, and enter/exit for this shot
- Scene change must match the scene sheet: same place, weather, lighting; only write how environment, props, and background change in this shot
- Comment is the keyframe prompt for this shot: subject, composition, light, action instant, environment. Write English that can go straight to image generation. Do not only repeat other columns
- Do not invent a new character or a new world

Do not output storyboard drawings. Do not explain.

Brief:
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
    "shot_no": ("Shot", "镜号"),
    "timeline": ("Timeline", "时间轴"),
    "camera": ("Camera", "镜头视角", "景别"),
    "move": ("Move", "运镜"),
    "character_action": ("Character action", "Character", "人物变化"),
    "scene_change": ("Scene change", "Scene", "场景变化"),
    "comment": ("Comment", "Notes", "注释", "备注", "画面描述", "提示词"),
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
        header_hit = any(
            marker.casefold() in joined.casefold()
            for marker in ("Shot", "Timeline", "镜号", "时间轴")
        )
        if not header_seen and header_hit:
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
        parts.append(f"Timeline {timeline}")
    for label, key in (
        ("Camera", "camera"),
        ("Camera move", "move"),
        ("Character action", "character_action"),
        ("Scene change", "scene_change"),
    ):
        value = str(shot.get(key) or "").strip()
        if value:
            parts.append(f"{label} {value}")
    return "; ".join(parts)


def fallback_brief(prompt: str) -> str:
    return (
        "# Brief\n\n"
        f"- Logline: {prompt}\n"
        "- Duration: 5 seconds\n"
        "- Resolution: 480P (dev)\n"
        "- Visual: follow the user description, avoid unrelated elements\n"
    )


def fallback_storyboard(prompt: str) -> str:
    return (
        "# Storyboard\n\n"
        "## Storyboard\n\n"
        f"| {_STORYBOARD_COLUMNS} |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
        f"| 1 | 0.0-2.0s | wide / slight high | slow pan from puddle reflection to subject | subject not yet in frame, or only the reflection | establish the scene: {prompt[:80]} | rain-night neon alley in a puddle reflection, wide slight-high, subject not yet seen |\n"
        "| 2 | 2.0-5.0s | medium / eye-level | follow then hold | subject enters and completes one clear action | neon and puddle shatter with the camera | medium eye-level, subject enters and completes one action, neon and puddle shatter |\n"
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
        parts.append("Character sheet / notes (character action must match):\n" + character_notes)
    elif role_output_image_path(ctx, NODE_ROLE_CHARACTER_DESIGN) is not None:
        parts.append("A character sheet exists. Character action must match that look, costume, and materials. Do not invent a new character.")
    if scene_notes:
        parts.append("Scene sheet / notes (scene change must match):\n" + scene_notes)
    elif role_output_image_path(ctx, NODE_ROLE_SCENE) is not None:
        parts.append("A scene sheet exists. Scene change must match that space, weather, and lighting. Do not change location.")
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

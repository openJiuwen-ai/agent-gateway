# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Clip node handler: generate one video per storyboard shot."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_BRIEF,
    NODE_ROLE_FRAME,
    NODE_ROLE_STORYBOARD,
    NODE_TYPE_VIDEO,
    AssetRef,
    DesignerExecutionGraph,
    DesignerGraphNode,
    node_role,
    node_shot_index,
)
from jiuwenswarm.server.runtime.designer.handlers.common import (
    graph_prompt,
    node_generate_prompt,
    node_output_image_paths,
    role_output_image_path,
    role_output_text,
)
from jiuwenswarm.server.runtime.designer.handlers.text_nodes import (
    StoryboardShot,
    parse_storyboard_shots,
)
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext, NodeResult

logger = logging.getLogger(__name__)

_TIMELINE_NUM = re.compile(r"\d+(?:\.\d+)?")


def parse_shot_duration_seconds(timeline: str, default: int = 5) -> int:
    nums = [float(item) for item in _TIMELINE_NUM.findall(timeline or "")]
    if len(nums) >= 2 and nums[1] > nums[0]:
        span = int(round(nums[1] - nums[0]))
        return max(2, min(10, span if span > 0 else default))
    return max(2, min(10, int(default)))


def collect_clip_first_frame(
    ctx: NodeExecutionContext | None,
    shot_index: int = 1,
) -> Path | None:
    """Use the keyframe node that matches this shot."""
    if ctx is None:
        return None
    frames = [
        node
        for node in (ctx.graph.get("nodes") or [])
        if node_role(node) == NODE_ROLE_FRAME
    ]
    matched = next(
        (node for node in frames if node_shot_index(node) == shot_index),
        frames[0] if len(frames) == 1 else None,
    )
    if matched is not None:
        paths = node_output_image_paths(ctx, str(matched.get("id") or ""))
        if paths:
            if node_shot_index(matched) == shot_index or len(paths) == 1:
                return paths[0]
            index = max(1, int(shot_index)) - 1
            if index < len(paths):
                return paths[index]
            return None
    return role_output_image_path(ctx, NODE_ROLE_STORYBOARD)


def collect_clip_reference_images(
    ctx: NodeExecutionContext | None,
    shot_index: int = 1,
) -> list[Path]:
    """Per-shot I2V only needs this shot's keyframe."""
    frame = collect_clip_first_frame(ctx, shot_index)
    return [frame] if frame is not None else []


def _shot_for_node(
    graph: DesignerExecutionGraph,
    node: DesignerGraphNode,
    ctx: NodeExecutionContext | None,
) -> tuple[int, StoryboardShot | None]:
    index = node_shot_index(node)
    text = role_output_text(ctx, NODE_ROLE_STORYBOARD) if ctx is not None else ""
    shots = parse_storyboard_shots(text)
    if shots and 1 <= index <= len(shots):
        shot = dict(shots[index - 1])
        override = node_generate_prompt(node)
        if override:
            shot["comment"] = override
        return index, shot
    return index, None


def _format_shot_block(shot: StoryboardShot, shot_index: int) -> str:
    lines = [
        f"第{shot_index}镜（镜号 {shot.get('shot_no') or shot_index}）",
        f"- 时间轴：{shot.get('timeline') or ''}",
        f"- 镜头视角：{shot.get('camera') or ''}",
        f"- 运镜：{shot.get('move') or ''}",
        f"- 人物变化：{shot.get('character_action') or ''}",
        f"- 场景变化：{shot.get('scene_change') or ''}",
    ]
    comment = str(shot.get("comment") or "").strip()
    if comment:
        lines.append(f"- 画面描述：{comment}")
    return "\n".join(lines)


def _clip_prompt_lead(shot_index: int, duration: int) -> str:
    return (
        f"制作第{shot_index}镜视频，时长约 {duration} 秒。"
        "首帧图像是本镜关键帧。只拍这一镜，不要切到其他镜头。"
        "从该构图起幅，保持角色外貌、服装、场景和构图与首帧一致，"
        "再按本镜的镜头视角、运镜、人物变化和场景变化运动镜头。"
        "不要加字幕。\n\n"
    )


def build_clip_prompt(
    graph: DesignerExecutionGraph,
    node: DesignerGraphNode,
    ctx: NodeExecutionContext | None = None,
) -> str:
    """Brief + this shot's storyboard row. Other shots are submitted by sibling clip nodes."""
    shot_index, shot = _shot_for_node(graph, node, ctx)
    duration = parse_shot_duration_seconds((shot or {}).get("timeline") or "", default=5)
    parts: list[str] = [_clip_prompt_lead(shot_index, duration)]
    if ctx is not None:
        brief = role_output_text(ctx, NODE_ROLE_BRIEF)
        if brief:
            parts.append(brief)
    if shot is not None:
        parts.append(_format_shot_block(shot, shot_index))
    else:
        storyboard = role_output_text(ctx, NODE_ROLE_STORYBOARD) if ctx is not None else ""
        parts.append(storyboard or graph_prompt(graph, node))
    return "\n\n".join(part.strip() for part in parts if part.strip())[:6000]


async def generate_clip_video(
    prompt: str,
    save_dir: str | None = None,
    first_frame: str | None = None,
    reference_images: list[str] | None = None,
    duration: int = 5,
) -> dict[str, Any]:
    """Call the shared video-generation stack. Tests monkeypatch this function."""
    from jiuwenswarm.agents.harness.common.tools.multimodal_config import (
        apply_video_gen_model_config_from_yaml,
    )
    from jiuwenswarm.agents.harness.common.tools.video_tools import (
        _invoke_model_video_generation,
    )
    from jiuwenswarm.common.config import get_config
    from jiuwenswarm.common.utils import get_env_file
    from jiuwenswarm.dotenv_early import load_dotenv_runtime

    try:
        load_dotenv_runtime(dotenv_path=get_env_file(), override=True)
    except Exception:
        logger.debug("Failed to reload video_gen env before generation", exc_info=True)

    try:
        apply_video_gen_model_config_from_yaml(get_config())
    except Exception:
        logger.debug("Failed to apply video_gen model config from yaml", exc_info=True)

    result = await _invoke_model_video_generation(
        prompt,
        first_frame=first_frame,
        reference_images=reference_images,
        duration=max(2, min(10, int(duration or 5))),
    )
    if "error" in result:
        raise RuntimeError(str(result["error"]))

    video_path = str(result.get("video_path") or "").strip()
    if not video_path:
        raise RuntimeError("video generation returned no video_path")

    if save_dir:
        dest_dir = Path(save_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / Path(video_path).name
        Path(video_path).replace(dest)
        result = {**result, "video_path": str(dest.resolve())}

    return result


class ClipNodeHandler:
    """Submit one I2V job per storyboard shot."""

    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        shot_index = node_shot_index(node)
        has_frame_node = any(
            node_role(item) == NODE_ROLE_FRAME for item in (ctx.graph.get("nodes") or [])
        )
        first_frame = collect_clip_first_frame(ctx, shot_index)
        if has_frame_node and first_frame is None:
            raise RuntimeError(
                f"第{shot_index}镜没有对应关键帧。请先重跑该镜的「关键帧」节点生成图片。"
            )
        _, shot = _shot_for_node(ctx.graph, node, ctx)
        duration = parse_shot_duration_seconds((shot or {}).get("timeline") or "", default=5)
        prompt = build_clip_prompt(ctx.graph, node, ctx)
        result = await generate_clip_video(
            prompt,
            first_frame=str(first_frame) if first_frame is not None else None,
            reference_images=None,
            duration=duration,
        )
        path = Path(str(result["video_path"]))
        output_ref: AssetRef = {
            "kind": NODE_TYPE_VIDEO,
            "uri": path.resolve().as_uri(),
            "mime_type": "video/mp4",
            "label": path.name,
        }
        return NodeResult(output_ref=output_ref, message=f"clip {shot_index} generated")

# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Image intermediate handlers: character sheet, scene, and keyframe."""

from __future__ import annotations

from pathlib import Path
from shutil import copy2

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_BRIEF,
    NODE_ROLE_CHARACTER_DESIGN,
    NODE_ROLE_SCENE,
    NODE_ROLE_STORYBOARD,
    NODE_TYPE_IMAGE,
    NODE_TYPE_TEXT,
    AssetRef,
    DesignerGraphNode,
    node_shot_index,
)
from jiuwenswarm.server.runtime.designer.handlers import common as handler_io
from jiuwenswarm.server.runtime.designer.handlers.common import (
    file_output_ref,
    graph_prompt,
    role_output_image_path,
    role_output_text,
    write_workspace_text,
)
from jiuwenswarm.server.runtime.designer.a2a_collab import collaboration_card
from jiuwenswarm.server.runtime.designer.handlers.text_nodes import (
    StoryboardShot,
    storyboard_shots_or_default,
)
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext, NodeResult


def _character_prompt(source: str) -> str:
    return (
        "角色设定图，单一主体，全身或半身，干净背景，电影灯光，赛博朋克或按描述。"
        "不要字幕、不要分镜格子。\n"
        f"{source}"
    )


def _scene_prompt(source: str) -> str:
    return (
        "电影场景建立镜头，只有环境没有人物。"
        "交代空间、天气、光线、招牌和地面，适合后续把角色放进去。"
        "不要人物、不要字幕、不要分镜格子。\n"
        f"{source}"
    )


def _strip_markdown_tables(text: str) -> str:
    """Keep prose from Brief; drop markdown tables so the image model does not paint them."""
    lines: list[str] = []
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped.count("|") >= 2:
            continue
        if stripped.startswith("|") or stripped.endswith("|"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _shot_frame_prompt(
    shot: StoryboardShot,
    brief: str,
    *,
    has_character: bool,
    has_scene: bool,
) -> str:
    timeline = f"（{shot['timeline']}）" if shot["timeline"] else ""
    comment = str(shot.get("comment") or "").strip()
    lead = (
        f"电影关键帧，单幅写实静帧，对应分镜第 {shot['shot_no']} 镜{timeline}。"
        "构图清楚，只画这一镜的瞬间，不要多格拼图。"
    )
    if comment:
        lead += f"按下面这段画面描述生成关键帧：{comment}。"
    lead += (
        "分镜内容："
        f"镜号 {shot['shot_no']}；"
        f"时间轴 {shot['timeline'] or '未写'}；"
        f"镜头视角 {shot['camera'] or '未写'}；"
        f"运镜 {shot['move'] or '未写'}；"
        f"人物变化 {shot['character_action'] or '未写'}；"
        f"场景变化 {shot['scene_change'] or '未写'}。"
    )
    if has_character and has_scene:
        lead += (
            "这是图生图：第一张参考图是角色设定，第二张是场景。"
            "把角色放入该场景，保持角色外貌、服装、材质，以及场景的空间、光线和天气。"
        )
    elif has_character:
        lead += "角色外貌、服装和材质必须与角色设定参考图一致，不要另造一套造型。"
    elif has_scene:
        lead += "场景空间、光线和天气必须与场景参考图一致。"
    lead += (
        "画面里只能是电影场景本身。"
        "不要字幕、不要分镜格子、不要表格、不要Excel、不要单元格、不要竖线表头。"
        "不要把「镜号」「时间轴」「镜头视角」「运镜」「人物变化」「场景变化」「注释」这些词画进画面。"
    )
    visual = _strip_markdown_tables(brief)
    if visual:
        return f"{lead}\n整体视觉风格参考：\n{visual}"
    return lead


def fallback_character_sheet(source: str) -> str:
    return (
        "# 角色设定\n\n"
        f"{source.strip()}\n\n"
        "- 外形：按 Brief 中的主体描述\n"
        "- 服装/材质：与雨夜霓虹或用户指定风格一致\n"
        "- 未配置图片生成时，先用这份设定稿作为中间产物\n"
    )


def fallback_scene_notes(source: str) -> str:
    return (
        "# 场景\n\n"
        f"{source.strip()}\n\n"
        "- 只画环境，不画人物\n"
        "- 未配置图片生成时，先用这份场景说明\n"
    )


def fallback_keyframe_script(source: str, shot_index: int = 1) -> str:
    shots = storyboard_shots_or_default(source)
    index = max(1, int(shot_index or 1))
    shot = shots[index - 1] if index <= len(shots) else None
    lines = [
        "# 关键帧\n",
        f"- 这是第 {index} 镜的关键帧节点，只对应这一镜\n",
    ]
    if shot:
        lines.append(
            f"- 第 {shot['shot_no']} 镜 {shot['timeline']}: {shot['camera']} / {shot['move']}\n"
            f"- 人物：{shot['character_action']}\n"
            f"- 场景：{shot['scene_change']}\n"
        )
        comment = str(shot.get("comment") or "").strip()
        if comment:
            lines.append(f"- 画面描述：{comment}\n")
    lines.append("- 未配置图片生成时，先用这份关键帧说明\n")
    return "".join(lines)


def _publish_shot_image(src: Path, *, stem: str) -> Path:
    dest = handler_io.get_agent_workspace_dir() / f"{stem}{src.suffix or '.png'}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.resolve() != src.resolve():
        copy2(src, dest)
    return dest.resolve()


async def _image_or_notes(
    *,
    prompt: str,
    notes: str,
    stem: str,
    kind_if_text: str,
    reference_images: list[str] | None = None,
) -> NodeResult:
    generated = await handler_io.generate_designer_image(
        prompt,
        reference_images=reference_images,
    )
    if generated and generated.get("image_path"):
        path = Path(generated["image_path"])
        return NodeResult(
            output_ref=file_output_ref(path, kind=NODE_TYPE_IMAGE, mime_type="image/png"),
            message="image generated",
        )
    path = write_workspace_text(stem, notes)
    error = str((generated or {}).get("error") or "").strip()
    message = (
        f"image_gen failed: {error}; wrote notes"
        if error
        else "image_gen unavailable, wrote notes"
    )
    return NodeResult(
        output_ref=file_output_ref(path, kind=kind_if_text, mime_type="text/markdown"),
        message=message,
    )


def _aligned_source(ctx: NodeExecutionContext, role: str, node: DesignerGraphNode) -> str:
    return (
        collaboration_card(ctx.run_id, role)
        or role_output_text(ctx, NODE_ROLE_BRIEF)
        or graph_prompt(ctx.graph, node)
    )


def _with_card_ref(result: NodeResult, ctx: NodeExecutionContext, role: str) -> NodeResult:
    card = collaboration_card(ctx.run_id, role)
    if not card:
        return result
    path = write_workspace_text(f"designer_a2a_{ctx.run_id}_{role}", card)
    card_ref = file_output_ref(path, kind=NODE_TYPE_TEXT, mime_type="text/markdown")
    refs = [ref for ref in (result.output_refs or []) if ref]
    primary = result.output_ref
    if primary is not None and primary not in refs:
        refs.insert(0, primary)
    if card_ref not in refs:
        refs.append(card_ref)
    return NodeResult(
        output_ref=primary,
        output_refs=refs or [card_ref],
        message=result.message,
    )


class CharacterDesignNodeHandler:
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        source = _aligned_source(ctx, NODE_ROLE_CHARACTER_DESIGN, node)
        result = await _image_or_notes(
            prompt=_character_prompt(source),
            notes=fallback_character_sheet(source),
            stem=f"designer_character_{ctx.run_id}_{ctx.node_id}",
            kind_if_text=NODE_TYPE_TEXT,
        )
        return _with_card_ref(result, ctx, NODE_ROLE_CHARACTER_DESIGN)


class SceneNodeHandler:
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        source = _aligned_source(ctx, NODE_ROLE_SCENE, node)
        result = await _image_or_notes(
            prompt=_scene_prompt(source),
            notes=fallback_scene_notes(source),
            stem=f"designer_scene_{ctx.run_id}_{ctx.node_id}",
            kind_if_text=NODE_TYPE_TEXT,
        )
        return _with_card_ref(result, ctx, NODE_ROLE_SCENE)


class FrameNodeHandler:
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        shot_index = node_shot_index(node)
        storyboard = role_output_text(ctx, NODE_ROLE_STORYBOARD)
        brief = role_output_text(ctx, NODE_ROLE_BRIEF)
        character = role_output_image_path(ctx, NODE_ROLE_CHARACTER_DESIGN)
        scene = role_output_image_path(ctx, NODE_ROLE_SCENE)
        visual = brief or graph_prompt(ctx.graph, node)
        if character is None or scene is None:
            missing = []
            if character is None:
                missing.append("角色图")
            if scene is None:
                missing.append("场景图")
            raise RuntimeError(
                "关键帧必须把"
                + "、".join(missing)
                + "和这一镜的分镜内容一起发给生图模型。请先完成「角色图」和「场景图」节点。"
            )
        refs = [str(character), str(scene)]
        shots = storyboard_shots_or_default(storyboard, visual)
        if shot_index > len(shots):
            raise RuntimeError(
                f"第{shot_index}镜在分镜表中不存在（当前共 {len(shots)} 镜）。"
            )
        shot = dict(shots[shot_index - 1])
        override = handler_io.node_generate_prompt(node)
        if override:
            shot["comment"] = override
        generated = await handler_io.generate_designer_image(
            _shot_frame_prompt(
                shot,
                visual,
                has_character=True,
                has_scene=True,
            ),
            reference_images=refs,
        )
        if generated and generated.get("image_path"):
            path = _publish_shot_image(
                Path(generated["image_path"]),
                stem=f"designer_frame_{ctx.run_id}_{ctx.node_id}_shot{shot_index}",
            )
            ref = file_output_ref(path, kind=NODE_TYPE_IMAGE, mime_type="image/png")
            return NodeResult(
                output_ref=ref,
                output_refs=[ref],
                message=f"keyframe {shot_index} generated",
            )
        last_error = str((generated or {}).get("error") or "").strip()
        notes = fallback_keyframe_script(storyboard or visual, shot_index)
        path = write_workspace_text(f"designer_frame_{ctx.run_id}_{ctx.node_id}", notes)
        ref = file_output_ref(path, kind=NODE_TYPE_TEXT, mime_type="text/markdown")
        message = (
            f"image_gen failed: {last_error}; wrote notes"
            if last_error
            else "image_gen unavailable, wrote notes"
        )
        return NodeResult(
            output_ref=ref,
            output_refs=[ref],
            message=message,
        )

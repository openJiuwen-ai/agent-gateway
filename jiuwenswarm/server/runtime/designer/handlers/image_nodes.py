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
        "Character design sheet, single subject, full or three-quarter body, clean background, "
        "cinematic lighting. Follow the brief. No subtitles, no storyboard grid.\n"
        f"{source}"
    )


def _scene_prompt(source: str) -> str:
    return (
        "Cinematic establishing shot of the environment only, no people. "
        "Show space, weather, lighting, signage, and ground so a character can be placed later. "
        "No people, no subtitles, no storyboard grid.\n"
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
    timeline = f" ({shot['timeline']})" if shot["timeline"] else ""
    comment = str(shot.get("comment") or "").strip()
    lead = (
        f"Cinematic keyframe, one photoreal still for shot {shot['shot_no']}{timeline}. "
        "Clear composition, this instant only, no comic grid."
    )
    if comment:
        lead += f" Generate the keyframe from this shot description: {comment}."
    lead += (
        " Shot notes: "
        f"shot {shot['shot_no']}; "
        f"timeline {shot['timeline'] or 'unspecified'}; "
        f"camera {shot['camera'] or 'unspecified'}; "
        f"camera move {shot['move'] or 'unspecified'}; "
        f"character action {shot['character_action'] or 'unspecified'}; "
        f"scene change {shot['scene_change'] or 'unspecified'}."
    )
    if has_character and has_scene:
        lead += (
            " This is image-to-image. The first reference is the character sheet; "
            "the second is the scene. Place that character in that scene and keep "
            "identity, costume, materials, location, lighting, and weather."
        )
    elif has_character:
        lead += " Character look, costume, and materials must match the character reference. Do not invent a new design."
    elif has_scene:
        lead += " Location, lighting, and weather must match the scene reference."
    lead += (
        " The image must be the cinematic scene itself. "
        "No subtitles, no storyboard grid, no table, no spreadsheet, no cell borders. "
        "Do not paint words like Shot, Timeline, Camera, Move, Character action, Scene change, or Comment."
    )
    visual = _strip_markdown_tables(brief)
    if visual:
        return f"{lead}\nOverall visual style:\n{visual}"
    return lead


def fallback_character_sheet(source: str) -> str:
    return (
        "# Character\n\n"
        f"{source.strip()}\n\n"
        "- Look: follow the subject in the Brief\n"
        "- Costume / materials: match the specified style\n"
        "- Image generation is unavailable; this sheet is the intermediate artifact\n"
    )


def fallback_scene_notes(source: str) -> str:
    return (
        "# Scene\n\n"
        f"{source.strip()}\n\n"
        "- Environment only, no people\n"
        "- Image generation is unavailable; these notes are the intermediate artifact\n"
    )


def fallback_keyframe_script(source: str, shot_index: int = 1) -> str:
    shots = storyboard_shots_or_default(source)
    index = max(1, int(shot_index or 1))
    shot = shots[index - 1] if index <= len(shots) else None
    lines = [
        "# Keyframe\n",
        f"- This is the keyframe node for shot {index} only\n",
    ]
    if shot:
        lines.append(
            f"- Shot {shot['shot_no']} {shot['timeline']}: {shot['camera']} / {shot['move']}\n"
            f"- Character: {shot['character_action']}\n"
            f"- Scene: {shot['scene_change']}\n"
        )
        comment = str(shot.get("comment") or "").strip()
        if comment:
            lines.append(f"- Shot description: {comment}\n")
    lines.append("- Image generation is unavailable; these notes are the intermediate artifact\n")
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
                missing.append("Character")
            if scene is None:
                missing.append("Scene")
            raise RuntimeError(
                "Keyframe generation must send "
                + " and ".join(missing)
                + " with this shot. Finish the Character and Scene nodes first."
            )
        refs = [str(character), str(scene)]
        shots = storyboard_shots_or_default(storyboard, visual)
        if shot_index > len(shots):
            raise RuntimeError(
                f"Shot {shot_index} is not in the storyboard ({len(shots)} shots)."
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

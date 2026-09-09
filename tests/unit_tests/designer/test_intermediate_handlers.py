# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

from pathlib import Path

import pytest

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_BRIEF,
    NODE_ROLE_CHARACTER_DESIGN,
    NODE_ROLE_FRAME,
    NODE_ROLE_SCENE,
    NODE_ROLE_STORYBOARD,
    NODE_TYPE_IMAGE,
    NODE_TYPE_TABLE,
    NODE_TYPE_TEXT,
    SCHEMA_VERSION,
    normalize_execution_graph,
)
from jiuwenswarm.server.runtime.designer.handlers.image_nodes import (
    CharacterDesignNodeHandler,
    FrameNodeHandler,
    _shot_frame_prompt,
    _strip_markdown_tables,
)
from jiuwenswarm.server.runtime.designer.handlers.text_nodes import (
    BriefNodeHandler,
    StoryboardNodeHandler,
    parse_storyboard_shots,
    shot_generate_prompt,
    storyboard_shots_or_default,
)
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext


def _graph():
    return normalize_execution_graph(
        {
            "schema_version": SCHEMA_VERSION,
            "graph_id": "graph_mid01",
            "project_id": "proj_mid01",
            "title": "雨夜",
            "description": "雨夜赛博朋克街景",
            "source": "manual",
            "nodes": [
                {
                    "id": "n_brief",
                    "type": NODE_TYPE_TEXT,
                    "label": "brief",
                    "config": {"role": NODE_ROLE_BRIEF, "prompt": "雨夜赛博朋克街景"},
                },
                {
                    "id": "n_storyboard",
                    "type": NODE_TYPE_TABLE,
                    "label": "storyboard",
                    "config": {"role": NODE_ROLE_STORYBOARD},
                },
                {
                    "id": "n_character",
                    "type": NODE_TYPE_IMAGE,
                    "label": "character",
                    "config": {"role": NODE_ROLE_CHARACTER_DESIGN},
                },
                {
                    "id": "n_frame",
                    "type": NODE_TYPE_IMAGE,
                    "label": "frame",
                    "config": {"role": NODE_ROLE_FRAME},
                },
            ],
            "edges": [],
        }
    )


@pytest.fixture()
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.get_agent_workspace_dir",
        lambda: tmp_path,
    )
    return tmp_path


def _png(workspace: Path, name: str) -> Path:
    path = workspace / name
    path.write_bytes(b"png")
    return path


def _frame_run_with_refs(
    workspace: Path,
    *,
    extra_states: dict | None = None,
) -> dict:
    from jiuwenswarm.server.runtime.designer.handlers.common import file_output_ref

    character = _png(workspace, "character.png")
    scene = _png(workspace, "scene.png")
    states = {
        "n_character": {
            "status": "completed",
            "output_ref": file_output_ref(
                character, kind=NODE_TYPE_IMAGE, mime_type="image/png"
            ),
        },
        "n_scene": {
            "status": "completed",
            "output_ref": file_output_ref(
                scene, kind=NODE_TYPE_IMAGE, mime_type="image/png"
            ),
        },
    }
    if extra_states:
        states.update(extra_states)
    return {
        "character": character,
        "scene": scene,
        "run": {"node_states": states},
    }


@pytest.mark.asyncio
async def test_brief_and_storyboard_write_markdown(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_text(prompt: str, max_tokens: int = 1200) -> str:
        if "Storyboard" in prompt or "分镜" in prompt or "运镜" in prompt:
            return "## Storyboard\n| Shot | Timeline |\n| 1 | 0.0-2.0s |\n"
        return "# Brief\n雨夜短片"

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.complete_designer_text",
        fake_text,
    )

    async def no_image(
        prompt: str,
        size: str = "512x512",
        reference_image: str | None = None,
        reference_images: list[str] | None = None,
    ):
        return None

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        no_image,
    )
    graph = _graph()
    ctx = NodeExecutionContext(graph=graph, run_id="run_mid01", node_id="n_brief")
    brief = await BriefNodeHandler().execute(graph["nodes"][0], ctx)
    assert brief.output_ref is not None
    assert brief.output_ref["uri"].endswith(".md")
    brief_path = workspace / Path(brief.output_ref["label"])
    assert "雨夜" in brief_path.read_text(encoding="utf-8")

    run = {
        "node_states": {
            "n_brief": {"status": "completed", "output_ref": brief.output_ref},
        }
    }
    story = await StoryboardNodeHandler().execute(
        graph["nodes"][1],
        NodeExecutionContext(graph=graph, run_id="run_mid01", node_id="n_storyboard", run=run),
    )
    assert story.output_ref is not None
    text = (workspace / Path(story.output_ref["label"])).read_text(encoding="utf-8")
    assert "Storyboard" in text
    assert "Timeline" in text


@pytest.mark.asyncio
async def test_storyboard_writes_table_and_does_not_generate_image(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_text(prompt: str, max_tokens: int = 1200) -> str:
        assert "Timeline" in prompt or "Camera" in prompt
        assert "Comment" in prompt
        return (
            "## 分镜表\n"
            "| 镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 |\n"
            "| 1 | 0.0-2.0s | 全景/略俯 | 缓摇 | 未入画 | 雨夜巷 |\n"
        )

    async def fake_image(*_args, **_kwargs):
        raise AssertionError("storyboard must not call image generation")

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.complete_designer_text",
        fake_text,
    )
    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        fake_image,
    )
    graph = _graph()
    result = await StoryboardNodeHandler().execute(
        graph["nodes"][1],
        NodeExecutionContext(graph=graph, run_id="run_mid01", node_id="n_storyboard", run={}),
    )
    assert result.output_ref is not None
    assert result.output_ref["kind"] == NODE_TYPE_TABLE
    assert result.output_ref["uri"].endswith(".md")
    text = (workspace / Path(result.output_ref["label"])).read_text(encoding="utf-8")
    assert "时间轴" in text
    assert "运镜" in text


@pytest.mark.asyncio
async def test_storyboard_prompt_aligns_with_character_and_scene(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from jiuwenswarm.common.schema.designer_graph import NODE_ROLE_SCENE
    from jiuwenswarm.server.runtime.designer.handlers.common import file_output_ref

    character_notes = workspace / "character.md"
    scene_notes = workspace / "scene.md"
    character_notes.write_text("炭黑机器人，左肩钴蓝核", encoding="utf-8")
    scene_notes.write_text("雨夜巷，积水倒影", encoding="utf-8")
    seen: dict[str, str] = {}

    async def fake_text(prompt: str, max_tokens: int = 1200) -> str:
        seen["prompt"] = prompt
        return (
            "## 分镜表\n"
            "| 镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 |\n"
            "| 1 | 0.0-2.0s | 全景/略俯 | 缓摇 | 机器人入画 | 雨夜巷 |\n"
        )

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.complete_designer_text",
        fake_text,
    )
    graph = _graph()
    graph["nodes"].append(
        {
            "id": "n_scene",
            "type": NODE_TYPE_IMAGE,
            "label": "scene",
            "config": {"role": NODE_ROLE_SCENE},
        }
    )
    await StoryboardNodeHandler().execute(
        graph["nodes"][1],
        NodeExecutionContext(
            graph=graph,
            run_id="run_mid01",
            node_id="n_storyboard",
            run={
                "node_states": {
                    "n_character": {
                        "status": "completed",
                        "output_ref": file_output_ref(
                            character_notes, kind=NODE_TYPE_TEXT, mime_type="text/markdown"
                        ),
                    },
                    "n_scene": {
                        "status": "completed",
                        "output_ref": file_output_ref(
                            scene_notes, kind=NODE_TYPE_TEXT, mime_type="text/markdown"
                        ),
                    },
                }
            },
        ),
    )
    prompt = seen["prompt"]
    assert "炭黑机器人" in prompt
    assert "雨夜巷" in prompt
    assert "character action must match" in prompt
    assert "scene change must match" in prompt
    assert "keyframe prompt" in prompt


@pytest.mark.asyncio
async def test_character_falls_back_to_notes_when_image_missing(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_image(
        prompt: str,
        size: str = "512x512",
        reference_image: str | None = None,
        reference_images: list[str] | None = None,
    ):
        return None

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        no_image,
    )
    graph = _graph()
    result = await CharacterDesignNodeHandler().execute(
        graph["nodes"][2],
        NodeExecutionContext(graph=graph, run_id="run_mid01", node_id="n_character"),
    )
    assert result.output_ref is not None
    assert result.output_ref["uri"].endswith(".md")
    assert "Character" in (workspace / Path(result.output_ref["label"])).read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_frame_uses_image_when_available(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image = workspace / "keyframe.png"
    image.write_bytes(b"png")

    async def fake_image(
        prompt: str,
        size: str = "512x512",
        reference_image: str | None = None,
        reference_images: list[str] | None = None,
    ):
        assert "关键帧" in prompt or "短片" in prompt or "雨夜" in prompt
        return {"image_path": str(image)}

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        fake_image,
    )
    graph = _graph()
    graph["nodes"].append(
        {
            "id": "n_scene",
            "type": NODE_TYPE_IMAGE,
            "label": "scene",
            "config": {"role": NODE_ROLE_SCENE},
        }
    )
    refs = _frame_run_with_refs(workspace)
    result = await FrameNodeHandler().execute(
        graph["nodes"][3],
        NodeExecutionContext(
            graph=graph,
            run_id="run_mid01",
            node_id="n_frame",
            run=refs["run"],
        ),
    )
    assert result.output_ref is not None
    assert result.output_ref["kind"] == NODE_TYPE_IMAGE
    assert result.output_ref["label"].endswith(".png")
    assert len(result.output_refs or []) == 1


@pytest.mark.asyncio
async def test_frame_passes_character_and_scene_as_img2img_references(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from jiuwenswarm.common.schema.designer_graph import NODE_ROLE_SCENE
    from jiuwenswarm.server.runtime.designer.handlers.common import file_output_ref

    character = workspace / "character.png"
    scene = workspace / "scene.png"
    character.write_bytes(b"png-character")
    scene.write_bytes(b"png-scene")
    keyframe = workspace / "keyframe.png"
    keyframe.write_bytes(b"png-frame")
    seen: dict[str, object] = {}

    async def fake_image(
        prompt: str,
        size: str = "512x512",
        reference_image: str | None = None,
        reference_images: list[str] | None = None,
    ):
        seen["reference_images"] = reference_images
        assert "image-to-image" in prompt
        return {"image_path": str(keyframe)}

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        fake_image,
    )
    graph = _graph()
    graph["nodes"].append(
        {
            "id": "n_scene",
            "type": NODE_TYPE_IMAGE,
            "label": "scene",
            "config": {"role": NODE_ROLE_SCENE},
        }
    )
    ctx = NodeExecutionContext(
        graph=graph,
        run_id="run_mid01",
        node_id="n_frame",
        run={
            "node_states": {
                "n_character": {
                    "status": "completed",
                    "output_ref": file_output_ref(
                        character, kind=NODE_TYPE_IMAGE, mime_type="image/png"
                    ),
                },
                "n_scene": {
                    "status": "completed",
                    "output_ref": file_output_ref(
                        scene, kind=NODE_TYPE_IMAGE, mime_type="image/png"
                    ),
                },
            }
        },
    )
    result = await FrameNodeHandler().execute(graph["nodes"][3], ctx)
    assert result.output_ref is not None
    assert seen["reference_images"] == [str(character.resolve()), str(scene.resolve())]


@pytest.mark.asyncio
async def test_frame_uses_node_generate_prompt_as_shot_comment(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, str] = {}

    async def fake_image(
        prompt: str,
        size: str = "512x512",
        reference_image: str | None = None,
        reference_images: list[str] | None = None,
    ):
        seen["prompt"] = prompt
        path = workspace / "keyframe.png"
        path.write_bytes(b"png")
        return {"image_path": str(path)}

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        fake_image,
    )
    graph = _graph()
    graph["nodes"].append(
        {
            "id": "n_scene",
            "type": NODE_TYPE_IMAGE,
            "label": "scene",
            "config": {"role": NODE_ROLE_SCENE},
        }
    )
    graph["nodes"][3]["config"]["generate"] = {
        "prompt": "中景平视，白领推开地铁门",
        "prompt_origin": "user",
    }
    story = workspace / "storyboard.md"
    story.write_text(
        "## 分镜表\n"
        "| 镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 | 注释 |\n"
        "| 1 | 0.0-5.0s | 中景 | 固定 | 入画 | 站台 | 分镜表里的旧注释 |\n",
        encoding="utf-8",
    )
    from jiuwenswarm.server.runtime.designer.handlers.common import file_output_ref

    refs = _frame_run_with_refs(
        workspace,
        extra_states={
            "n_storyboard": {
                "status": "completed",
                "output_ref": file_output_ref(
                    story, kind=NODE_TYPE_TABLE, mime_type="text/markdown"
                ),
            },
        },
    )
    await FrameNodeHandler().execute(
        graph["nodes"][3],
        NodeExecutionContext(
            graph=graph,
            run_id="run_mid01",
            node_id="n_frame",
            run=refs["run"],
        ),
    )
    assert "Generate the keyframe from this shot description: 中景平视，白领推开地铁门" in seen["prompt"]
    assert "分镜表里的旧注释" not in seen["prompt"]


def test_shot_frame_prompt_uses_shot_fields_not_markdown_table() -> None:
    brief = (
        "# Brief\n\n雨夜霓虹巷。\n\n"
        "| 镜号 | 时间轴 | 镜头视角 |\n"
        "| --- | --- | --- |\n"
        "| 1 | 0.0-2.0s | 全景 |\n"
    )
    prompt = _shot_frame_prompt(
        {
            "shot_no": "1",
            "timeline": "0.0-2.0s",
            "camera": "全景/略俯",
            "move": "缓摇",
            "character_action": "未入画",
            "scene_change": "雨夜巷",
        },
        brief,
        has_character=False,
        has_scene=False,
    )
    assert "Shot notes" in prompt
    assert "character action 未入画" in prompt
    assert "scene change 雨夜巷" in prompt
    assert "| 镜号 |" not in prompt
    assert "Do not paint words" in prompt
    assert _strip_markdown_tables(brief) == "# Brief\n\n雨夜霓虹巷。"


def test_parse_storyboard_shots_reads_table_rows() -> None:
    text = (
        "## 分镜表\n\n"
        "| 镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 |\n"
        "| --- | --- | --- | --- | --- | --- |\n"
        "| 1 | 0.0-2.0s | 全景/略俯 | 缓摇 | 未入画 | 雨夜巷 |\n"
        "| 2 | 2.0-5.0s | 中景/平视 | 跟移 | 主体入画 | 霓虹闪 |\n"
    )
    shots = parse_storyboard_shots(text)
    assert [shot["shot_no"] for shot in shots] == ["1", "2"]
    assert shots[0]["timeline"] == "0.0-2.0s"
    assert shots[1]["character_action"] == "主体入画"
    assert shots[0]["comment"] == ""
    assert shots[1]["comment"] == ""


def test_storyboard_shots_or_default_falls_back() -> None:
    shots = storyboard_shots_or_default("没有表格", "雨夜")
    assert len(shots) == 2
    assert shots[0]["comment"]
    assert shots[1]["comment"]


def test_parse_storyboard_shots_reads_comment_column() -> None:
    text = (
        "## 分镜表\n\n"
        "| 镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 | 注释 |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
        "| 1 | 0.0-2.0s | 全景/略俯 | 缓摇 | 未入画 | 雨夜巷 | 积水倒影里的霓虹巷，全景略俯，主体尚未入画 |\n"
        "| 2 | 2.0-5.0s | 中景/平视 | 跟移 | 主体入画 | 霓虹闪 | 中景平视，白领从地铁门走出 |\n"
    )
    shots = parse_storyboard_shots(text)
    assert shots[0]["comment"] == "积水倒影里的霓虹巷，全景略俯，主体尚未入画"
    assert shots[1]["comment"] == "中景平视，白领从地铁门走出"


def test_shot_frame_prompt_uses_comment_as_generation_prompt() -> None:
    prompt = _shot_frame_prompt(
        {
            "shot_no": "2",
            "timeline": "2.0-5.0s",
            "camera": "中景/平视",
            "move": "跟移",
            "character_action": "主体入画",
            "scene_change": "霓虹闪",
            "comment": "中景平视，白领推开地铁门，霓虹映在积水上",
        },
        "雨夜",
        has_character=True,
        has_scene=True,
    )
    assert "Generate the keyframe from this shot description: 中景平视，白领推开地铁门，霓虹映在积水上" in prompt
    assert "character action 主体入画" in prompt


def test_shot_generate_prompt_prefers_comment_then_row_fields() -> None:
    with_comment = {
        "shot_no": "1",
        "timeline": "0.0-2.0s",
        "camera": "全景/略俯",
        "move": "缓摇",
        "character_action": "未入画",
        "scene_change": "雨夜巷",
        "comment": "积水倒影里的霓虹巷",
    }
    without_comment = {**with_comment, "comment": ""}
    assert shot_generate_prompt(with_comment) == "积水倒影里的霓虹巷"
    assert "Camera 全景/略俯" in shot_generate_prompt(without_comment)
    assert "Character action 未入画" in shot_generate_prompt(without_comment)


@pytest.mark.asyncio
async def test_frame_generates_one_image_per_storyboard_shot(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    story = workspace / "storyboard.md"
    story.write_text(
        "## 分镜表\n\n"
        "| 镜号 | 时间轴 | 镜头视角 | 运镜 | 人物变化 | 场景变化 |\n"
        "| --- | --- | --- | --- | --- | --- |\n"
        "| 1 | 0.0-1.5s | 全景/略俯 | 缓摇 | 未入画 | 雨夜巷 |\n"
        "| 2 | 1.5-3.5s | 中景/平视 | 跟移 | 主体入画 | 霓虹闪 |\n"
        "| 3 | 3.5-5.0s | 近景/平视 | 固定 | 转身 | 积水碎开 |\n",
        encoding="utf-8",
    )
    prompts: list[str] = []
    seen_refs: list[list[str] | None] = []

    async def fake_image(
        prompt: str,
        size: str = "512x512",
        reference_image: str | None = None,
        reference_images: list[str] | None = None,
    ):
        prompts.append(prompt)
        seen_refs.append(reference_images)
        path = workspace / f"keyframe_{len(prompts)}.png"
        path.write_bytes(b"png")
        return {"image_path": str(path)}

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.handlers.common.generate_designer_image",
        fake_image,
    )
    graph = _graph()
    graph["nodes"].append(
        {
            "id": "n_scene",
            "type": NODE_TYPE_IMAGE,
            "label": "scene",
            "config": {"role": NODE_ROLE_SCENE},
        }
    )
    from jiuwenswarm.server.runtime.designer.handlers.common import file_output_ref

    refs = _frame_run_with_refs(
        workspace,
        extra_states={
            "n_storyboard": {
                "status": "completed",
                "output_ref": file_output_ref(
                    story, kind=NODE_TYPE_TABLE, mime_type="text/markdown"
                ),
            }
        },
    )
    ctx = NodeExecutionContext(
        graph=graph,
        run_id="run_mid01",
        node_id="n_frame",
        run=refs["run"],
    )
    result = await FrameNodeHandler().execute(graph["nodes"][3], ctx)
    assert len(prompts) == 1
    assert "shot 1" in prompts[0]
    assert "Shot notes" in prompts[0]
    assert "character action 未入画" in prompts[0]
    expected_refs = [str(refs["character"].resolve()), str(refs["scene"].resolve())]
    assert seen_refs[0] == expected_refs
    assert "| 镜号 |" not in prompts[0]
    assert prompts[0].count("|") == 0
    assert result.output_refs is not None
    assert len(result.output_refs) == 1
    assert result.output_refs[0]["label"] == "designer_frame_run_mid01_n_frame_shot1.png"

    graph["nodes"][3]["config"]["shot_index"] = 2
    second = await FrameNodeHandler().execute(graph["nodes"][3], ctx)
    assert len(prompts) == 2
    assert "shot 2" in prompts[1]
    assert "character action 主体入画" in prompts[1]
    assert seen_refs[1] == expected_refs
    assert second.output_refs is not None
    assert second.output_refs[0]["label"] == "designer_frame_run_mid01_n_frame_shot2.png"


@pytest.mark.asyncio
async def test_frame_requires_character_and_scene_images(workspace: Path) -> None:
    graph = _graph()
    with pytest.raises(RuntimeError, match="Character and Scene"):
        await FrameNodeHandler().execute(
            graph["nodes"][3],
            NodeExecutionContext(graph=graph, run_id="run_mid01", node_id="n_frame"),
        )

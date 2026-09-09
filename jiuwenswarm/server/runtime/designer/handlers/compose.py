# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compose node handler: concatenate per-shot clip videos into one film."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_CLIP,
    NODE_TYPE_VIDEO,
    AssetRef,
    DesignerGraphNode,
    node_role,
    node_shot_index,
)
from jiuwenswarm.server.runtime.designer.handlers import common as handler_io
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext, NodeResult

logger = logging.getLogger(__name__)

_VIDEO_SUFFIXES = {".mp4", ".webm", ".mov", ".m4v"}


def _find_ffmpeg() -> str:
    found = shutil.which("ffmpeg")
    if found:
        return found
    try:
        import imageio_ffmpeg

        exe = str(imageio_ffmpeg.get_ffmpeg_exe() or "").strip()
        if exe:
            return exe
    except Exception:
        logger.debug("imageio_ffmpeg is not available", exc_info=True)
    raise RuntimeError("未找到 ffmpeg，无法合并视频片段。请安装 ffmpeg 或 imageio-ffmpeg。")


def _concat_file_line(path: Path) -> str:
    posix = path.resolve().as_posix().replace("'", r"'\''")
    return f"file '{posix}'"


def concatenate_clip_videos(paths: list[Path], dest: Path) -> Path:
    """Join shot clips in order. Tests monkeypatch this function."""
    if not paths:
        raise RuntimeError("没有可合并的视频片段")
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if len(paths) == 1:
        shutil.copyfile(paths[0], dest)
        return dest.resolve()

    ffmpeg = _find_ffmpeg()
    handle, list_name = tempfile.mkstemp(prefix="designer_concat_", suffix=".txt")
    os.close(handle)
    list_path = Path(list_name)
    try:
        list_path.write_text(
            "\n".join(_concat_file_line(path) for path in paths) + "\n",
            encoding="utf-8",
        )
        copy_cmd = [
            ffmpeg,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            "-c",
            "copy",
            str(dest),
        ]
        copied = subprocess.run(copy_cmd, capture_output=True, text=True, check=False)
        if copied.returncode == 0 and dest.is_file() and dest.stat().st_size > 0:
            return dest.resolve()
        logger.warning("ffmpeg concat copy failed: %s", (copied.stderr or copied.stdout or "")[:800])
        encode_cmd = [
            ffmpeg,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-movflags",
            "+faststart",
            str(dest),
        ]
        encoded = subprocess.run(encode_cmd, capture_output=True, text=True, check=False)
        if encoded.returncode != 0 or not dest.is_file() or dest.stat().st_size <= 0:
            detail = (encoded.stderr or encoded.stdout or copied.stderr or "").strip()[:800]
            raise RuntimeError(f"合并视频片段失败：{detail or 'ffmpeg concat error'}")
        return dest.resolve()
    finally:
        list_path.unlink(missing_ok=True)


def collect_clip_video_paths(ctx: NodeExecutionContext) -> list[Path]:
    states = (ctx.run or {}).get("node_states") or {}
    clips = sorted(
        [node for node in (ctx.graph.get("nodes") or []) if node_role(node) == NODE_ROLE_CLIP],
        key=node_shot_index,
    )
    paths: list[Path] = []
    missing: list[str] = []
    for node in clips:
        node_id = str(node.get("id") or "")
        ref = (states.get(node_id) or {}).get("output_ref") or {}
        path = handler_io.path_from_uri(str(ref.get("uri") or ""))
        if (
            path is None
            or not path.is_file()
            or path.suffix.lower() not in _VIDEO_SUFFIXES
        ):
            missing.append(node_id or f"shot {node_shot_index(node)}")
            continue
        paths.append(path.resolve())
    if missing:
        raise RuntimeError(
            "以下视频片段尚未生成，无法成片：" + "、".join(missing)
        )
    return paths


class ComposeNodeHandler:
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        paths = collect_clip_video_paths(ctx)
        dest = handler_io.get_agent_workspace_dir() / f"designer_compose_{ctx.run_id}.mp4"
        merged = concatenate_clip_videos(paths, dest)
        output_ref: AssetRef = {
            "kind": NODE_TYPE_VIDEO,
            "uri": merged.resolve().as_uri(),
            "mime_type": "video/mp4",
            "label": merged.name,
        }
        return NodeResult(output_ref=output_ref, message="composed video")

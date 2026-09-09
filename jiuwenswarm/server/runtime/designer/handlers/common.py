# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared helpers for Designer node handlers."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import unquote, urlparse

from jiuwenswarm.common.schema.designer_graph import (
    NODE_ROLE_BRIEF,
    AssetRef,
    DesignerExecutionGraph,
    DesignerGraphNode,
    node_role,
)
from jiuwenswarm.common.utils import get_agent_workspace_dir
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext

logger = logging.getLogger(__name__)


def graph_prompt(graph: DesignerExecutionGraph, node: DesignerGraphNode | None = None) -> str:
    if node is not None:
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        node_prompt = str(config.get("prompt") or "").strip()
        if node_prompt:
            return node_prompt
        generate = config.get("generate")
        if isinstance(generate, dict):
            generate_prompt = str(generate.get("prompt") or "").strip()
            if generate_prompt:
                return generate_prompt
        if node_role(node) == NODE_ROLE_BRIEF:
            pass
    for candidate in graph.get("nodes") or []:
        if node_role(candidate) != NODE_ROLE_BRIEF:
            continue
        brief_config = candidate.get("config") if isinstance(candidate.get("config"), dict) else {}
        brief_prompt = str(brief_config.get("prompt") or "").strip()
        if brief_prompt:
            return brief_prompt
    description = str(graph.get("description") or "").strip()
    if description:
        return description
    title = str(graph.get("title") or "").strip()
    return title or "短视频"


def node_generate_prompt(node: DesignerGraphNode | None) -> str:
    if node is None:
        return ""
    config = node.get("config") if isinstance(node.get("config"), dict) else {}
    generate = config.get("generate") if isinstance(config, dict) else None
    if isinstance(generate, dict):
        text = str(generate.get("prompt") or "").strip()
        if text:
            return text
    return str((config or {}).get("prompt") or "").strip()


def path_from_uri(uri: str) -> Path | None:
    value = (uri or "").strip()
    if not value or value.startswith("designer://"):
        return None
    if value.startswith("file:"):
        parsed = urlparse(value)
        path = unquote(parsed.path)
        if len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        return Path(path)
    candidate = Path(value)
    return candidate if candidate.exists() else None


def write_workspace_text(stem: str, content: str) -> Path:
    directory = get_agent_workspace_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}.md"
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path.resolve()


def file_output_ref(path: Path, *, kind: str, mime_type: str) -> AssetRef:
    resolved = path.resolve()
    return {
        "kind": kind,
        "uri": resolved.as_uri(),
        "mime_type": mime_type,
        "label": resolved.name,
    }


_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".jfif"}


def node_output_refs(ctx: NodeExecutionContext, node_id: str) -> list[dict]:
    if ctx.run is None or not node_id:
        return []
    state = (ctx.run.get("node_states") or {}).get(node_id) or {}
    raw_refs = state.get("output_refs")
    collected: list[dict] = []
    if isinstance(raw_refs, list):
        collected = [
            item for item in raw_refs if isinstance(item, dict) and str(item.get("uri") or "").strip()
        ]
    if not collected:
        single = state.get("output_ref") or {}
        if isinstance(single, dict) and str(single.get("uri") or "").strip():
            collected = [single]
    return collected


def node_output_image_paths(ctx: NodeExecutionContext, node_id: str) -> list[Path]:
    paths: list[Path] = []
    seen: set[str] = set()
    for ref in node_output_refs(ctx, node_id):
        path = path_from_uri(str(ref.get("uri") or ""))
        if path is None or not path.is_file():
            continue
        kind = str(ref.get("kind") or "").lower()
        mime = str(ref.get("mime_type") or "").lower()
        if kind != "image" and not mime.startswith("image/") and path.suffix.lower() not in _IMAGE_SUFFIXES:
            continue
        resolved = path.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        paths.append(resolved)
    return paths


def role_output_refs(ctx: NodeExecutionContext, role: str) -> list[dict]:
    if ctx.run is None:
        return []
    for node in ctx.graph.get("nodes") or []:
        if node_role(node) != role:
            continue
        collected = node_output_refs(ctx, str(node.get("id") or ""))
        if collected:
            return collected
    return []


def role_output_image_paths(ctx: NodeExecutionContext, role: str) -> list[Path]:
    paths: list[Path] = []
    seen: set[str] = set()
    for ref in role_output_refs(ctx, role):
        path = path_from_uri(str(ref.get("uri") or ""))
        if path is None or not path.is_file():
            continue
        kind = str(ref.get("kind") or "").lower()
        mime = str(ref.get("mime_type") or "").lower()
        if kind != "image" and not mime.startswith("image/") and path.suffix.lower() not in _IMAGE_SUFFIXES:
            continue
        resolved = path.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        paths.append(resolved)
    return paths


def role_output_image_path(ctx: NodeExecutionContext, role: str) -> Path | None:
    paths = role_output_image_paths(ctx, role)
    return paths[0] if paths else None


def role_output_text(ctx: NodeExecutionContext, role: str) -> str:
    if ctx.run is None:
        return ""
    states = ctx.run.get("node_states") or {}
    for node in ctx.graph.get("nodes") or []:
        if node_role(node) != role:
            continue
        ref = (states.get(node["id"]) or {}).get("output_ref") or {}
        path = path_from_uri(str(ref.get("uri") or ""))
        if path is None or not path.is_file():
            continue
        text_suffixes = {".md", ".txt", ".markdown", ".csv"}
        candidates = [path] if path.suffix.lower() in text_suffixes else []
        sidecar = path.with_suffix(".md")
        if sidecar not in candidates:
            candidates.append(sidecar)
        for candidate in candidates:
            if not candidate.is_file():
                continue
            try:
                return candidate.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
    return ""


async def complete_designer_text(prompt: str, *, max_tokens: int = 1200) -> str:
    """Call the default chat model. Tests monkeypatch this function."""
    from jiuwenswarm.common.config import get_config, get_default_models
    from openjiuwen.core.foundation.llm import Model, ModelClientConfig

    entries = get_default_models(get_config())
    entry = next((item for item in entries if item.get("is_default") is True), None)
    if entry is None and entries:
        entry = entries[0]
    client = (entry or {}).get("model_client_config") if isinstance(entry, dict) else {}
    if not isinstance(client, dict):
        return ""
    api_key = str(client.get("api_key") or "").strip()
    api_base = str(client.get("api_base") or "").strip()
    model_name = str(client.get("model_name") or "").strip()
    provider = str(client.get("client_provider") or "").strip()
    if not api_key or not model_name:
        return ""
    kwargs: dict[str, object] = {
        "api_key": api_key,
        "api_base": api_base,
        "client_provider": provider,
    }
    profile = str(client.get("endpoint_profile") or "").strip()
    if profile:
        kwargs["endpoint_profile"] = profile
    model = Model(model_client_config=ModelClientConfig(**kwargs))
    response = await model.invoke(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
        max_tokens=max_tokens,
        model=model_name,
    )
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("text"):
                parts.append(str(block["text"]))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts).strip()
    return str(content).strip()


def _image_gen_switch_enabled() -> bool:
    raw = (os.environ.get("IMAGE_GEN_ENABLED") or "").strip().lower()
    if not raw:
        return True
    return raw in {"true", "1", "yes", "on", "enabled"}


async def generate_designer_image(
    prompt: str,
    size: str = "1328x1328",
    reference_image: str | None = None,
    reference_images: list[str] | None = None,
) -> dict[str, str] | None:
    """Call image_gen when configured. Tests monkeypatch this function."""
    from jiuwenswarm.agents.harness.common.tools.image_tools import _invoke_model_image_generation
    from jiuwenswarm.agents.harness.common.tools.multimodal_config import (
        apply_image_gen_model_config_from_yaml,
    )
    from jiuwenswarm.common.config import get_config
    from jiuwenswarm.common.utils import get_env_file
    from jiuwenswarm.dotenv_early import load_dotenv_runtime

    try:
        load_dotenv_runtime(dotenv_path=get_env_file(), override=True)
    except Exception:
        logger.debug("Failed to reload image_gen env before generation", exc_info=True)

    if not _image_gen_switch_enabled():
        logger.info("Designer image generation skipped: IMAGE_GEN_ENABLED is off")
        return None
    try:
        apply_image_gen_model_config_from_yaml(get_config())
    except Exception:
        logger.debug("Failed to apply image_gen model config from yaml", exc_info=True)
    refs = [str(item).strip() for item in (reference_images or []) if str(item).strip()]
    single = (reference_image or "").strip()
    if single and single not in refs:
        refs.insert(0, single)
    if refs:
        logger.info(
            "Designer image generation using reference_images=%s",
            ",".join(Path(item).name for item in refs),
        )
    result = await _invoke_model_image_generation(prompt, size=size, reference_images=refs or None)
    if "error" in result:
        logger.info("Designer image generation unavailable: %s", result["error"])
        return {"error": str(result["error"])}
    image_path = str(result.get("image_path") or "").strip()
    if not image_path:
        return None
    return {"image_path": image_path}

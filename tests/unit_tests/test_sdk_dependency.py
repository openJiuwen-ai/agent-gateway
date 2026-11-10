# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Keep published SDK requirements and the uv resolution on one official pin."""

from pathlib import Path
import re
import tomllib


ROOT = Path(__file__).resolve().parents[2]
SDK_REPOSITORY = "https://gitcode.com/openJiuwen/agent-core.git"


def _sdk_revision() -> str:
    with (ROOT / "pyproject.toml").open("rb") as stream:
        project = tomllib.load(stream).get("project", {})
    requirements = []
    for requirement in project.get("dependencies", []):
        if requirement.split("@", 1)[0].strip() == "openjiuwen":
            requirements.append(requirement)
    assert len(requirements) == 1
    prefix = f"openjiuwen @ git+{SDK_REPOSITORY}@"
    assert requirements[0].startswith(prefix)
    revision = requirements[0].removeprefix(prefix)
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    return revision


def test_sdk_lock_matches_published_requirement() -> None:
    revision = _sdk_revision()
    with (ROOT / "uv.lock").open("rb") as stream:
        packages = tomllib.load(stream).get("package", [])
    sdk_packages = [package for package in packages if package.get("name") == "openjiuwen"]
    assert len(sdk_packages) == 1
    assert sdk_packages[0].get("source", {}).get("git") == (
        f"{SDK_REPOSITORY}?rev={revision}#{revision}"
    )


def test_root_lock_metadata_matches_published_requirement() -> None:
    revision = _sdk_revision()
    with (ROOT / "uv.lock").open("rb") as stream:
        packages = tomllib.load(stream).get("package", [])
    root_packages = [package for package in packages if package.get("name") == "agent-gateway"]
    assert len(root_packages) == 1
    requirements = root_packages[0].get("metadata", {}).get("requires-dist", [])
    sdk_requirements = [item for item in requirements if item.get("name") == "openjiuwen"]
    assert sdk_requirements == [{"name": "openjiuwen", "git": f"{SDK_REPOSITORY}?rev={revision}"}]

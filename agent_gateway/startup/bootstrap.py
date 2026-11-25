"""Startup stages invoked by the Gateway process entrypoint."""

from collections.abc import Callable

StageMarker = Callable[[str], None]


def initialize_early_environment(mark: StageMarker) -> None:
    """Load the selected environment before runtime imports."""
    from agent_gateway.dotenv_early import parse_dotenv_early

    parse_dotenv_early("agent-gateway")
    mark("dotenv_parsed")


def configure_process_logging(mark: StageMarker) -> None:
    """Configure third-party logging before Gateway logging."""
    import logging
    from agent_gateway.common.openjiuwen_logging import (
        configure_openjiuwen_core_logging,
        configure_openjiuwen_runtime_logging,
    )

    configure_openjiuwen_runtime_logging()
    configure_openjiuwen_core_logging(suppress_level=logging.CRITICAL)
    mark("third_party_logging_pinned")

    from agent_gateway.common.logging_setup import setup_gateway_logger

    setup_gateway_logger()
    mark("logging_ready")


def prepare_group_workspace(mark: StageMarker) -> None:
    """Prepare workspace resources unless the parent already prepared them."""
    import os
    from agent_gateway.common.paths import (
        cleanup_stale_openjiuwen_descs,
        prepare_runtime_workspace,
    )

    cleanup_stale_openjiuwen_descs()
    if os.environ.get("JIUWENSWARM_RUNTIME_WORKSPACE_READY") != "1":
        prepare_runtime_workspace(cleanup_stale_descs=False)
    mark("runtime_workspace_ready")


def apply_group_environment(mark: StageMarker) -> None:
    """Apply persisted environment and compatible startup migrations."""
    from agent_gateway.common.paths import apply_free_search_runtime_defaults, get_env_file
    from agent_gateway.dotenv_early import load_dotenv_runtime

    env_file = get_env_file()
    load_dotenv_runtime(dotenv_path=env_file, override=True)
    apply_free_search_runtime_defaults()
    mark("runtime_environment_applied")

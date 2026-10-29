"""Compatibility alias for transport-neutral DingTalk cron bindings."""

import sys

from agent_gateway.runtime.cron import dingtalk_routing as _implementation

sys.modules[__name__] = _implementation

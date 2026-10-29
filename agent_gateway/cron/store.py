"""Compatibility alias for cron persistence now owned by Agent Runtime."""

import sys

from agent_gateway.runtime.cron import store as _implementation

sys.modules[__name__] = _implementation

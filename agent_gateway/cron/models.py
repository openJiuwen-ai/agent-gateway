"""Compatibility alias for cron models now owned by Agent Runtime."""

import sys

from agent_gateway.runtime.cron import models as _implementation

sys.modules[__name__] = _implementation

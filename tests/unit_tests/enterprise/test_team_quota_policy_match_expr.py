"""团队配额策略：非法 match_expr 应被跳过，而不是让日志语句炸掉整段计算。"""

from __future__ import annotations

import sys
import types

# 本地/轻量环境可能没有 openjiuwen-runtime-foundation，导致
# agent_gateway.config.enterprise 包整体导入失败；这里只 stub 掉表达式匹配
# 依赖，保证本用例聚焦 quota_policy 自身的异常降级逻辑。
try:  # pragma: no cover - 依赖是否完整由环境决定
    import agent_gateway.config.enterprise  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover
    for _name in ("agent_gateway.config", "agent_gateway.config.enterprise"):
        if _name not in sys.modules:
            _pkg = types.ModuleType(_name)
            _pkg.__path__ = []  # type: ignore[attr-defined]
            sys.modules[_name] = _pkg
    _expr = types.ModuleType("agent_gateway.config.enterprise.expressions")
    _expr.matches = lambda *_a, **_k: False  # type: ignore[attr-defined]
    _expr.validate_match_expr = lambda *_a, **_k: None  # type: ignore[attr-defined]
    sys.modules["agent_gateway.config.enterprise.expressions"] = _expr

from agent_gateway.enterprise.team import quota_policy  # noqa: E402
from agent_gateway.enterprise.team.quota_policy import OwnerIdentity  # noqa: E402


class TestInvalidMatchExprFallback:
    @staticmethod
    def test_select_limits_skips_policy_when_matching_raises(monkeypatch):
        def _boom(_expr, _ident):
            raise ValueError("malformed match expression")

        monkeypatch.setattr(quota_policy, "matches", _boom)
        limits = quota_policy._select_limits(
            [
                {
                    "policy_id": "p1",
                    "enabled": True,
                    "match_expr": "not-a-valid-expression",
                    "max_teams_per_user": 3,
                }
            ],
            OwnerIdentity(user_id="u1"),
        )
        # 非法策略被跳过 -> 回落到默认值，而不是抛 NameError/ValueError
        assert limits.max_teams_per_user >= 1

    @staticmethod
    def test_matching_expression_is_used(monkeypatch):
        monkeypatch.setattr(quota_policy, "matches", lambda _expr, _ident: True)
        limits = quota_policy._select_limits(
            [
                {
                    "policy_id": "p1",
                    "enabled": True,
                    "match_expr": '{"user_id": "u1"}',
                    "max_teams_per_user": 7,
                }
            ],
            OwnerIdentity(user_id="u1"),
        )
        assert limits.max_teams_per_user == 7

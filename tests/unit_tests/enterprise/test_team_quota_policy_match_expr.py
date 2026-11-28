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


class TestUserPolicyZFallback:
    """A user-scoped policy that carries Z must still enforce the org cap.

    When no org-scoped policy exists, ``_limits_for_rows`` falls back to the Z
    of the policy that matched the user, so a single user/workspace policy that
    sets both X and Z still enforces the organization team cap. This covers the
    issue-110 scenario where X=3 and Z=3 are configured on one user policy.
    """

    @staticmethod
    def test_select_limits_surfaces_user_policy_z(monkeypatch):
        monkeypatch.setattr(quota_policy, "matches", lambda _expr, _ident: True)
        limits = quota_policy._select_limits(
            [
                {
                    "policy_id": "user",
                    "enabled": True,
                    "match_expr": "user_id == 'u1'",
                    "max_teams_per_user": 3,
                    "max_teammates_per_team": 5,
                    "max_teams_per_org": 3,
                }
            ],
            OwnerIdentity(user_id="u1", group_id="g1", bot_id="b1"),
        )
        assert limits.max_teams_per_user == 3
        assert limits.max_teams_per_org == 3

    @staticmethod
    def test_select_limits_skips_bot_scoped_z(monkeypatch):
        """An agent-scoped (bot_id) policy must not widen its Z into an org cap."""
        monkeypatch.setattr(quota_policy, "matches", lambda _expr, _ident: True)
        limits = quota_policy._select_limits(
            [
                {
                    "policy_id": "agent",
                    "enabled": True,
                    "match_expr": "bot_id == 'b1'",
                    "max_teams_per_user": 3,
                    "max_teammates_per_team": 5,
                    "max_teams_per_org": 3,
                }
            ],
            OwnerIdentity(user_id="u1", group_id="g1", bot_id="b1"),
        )
        assert limits.max_teams_per_user == 3
        assert limits.max_teams_per_org == 0

    @staticmethod
    def test_limits_for_rows_falls_back_to_user_policy_z(monkeypatch):
        monkeypatch.setattr(quota_policy, "matches", lambda _expr, _ident: True)

        class _Store(quota_policy.TeamQuotaPolicyStore):
            pass

        store = _Store()
        rows = [
            {
                "policy_id": "user",
                "enabled": True,
                "match_expr": "user_id == 'u1'",
                "priority": 10,
                "max_teams_per_user": 3,
                "max_teammates_per_team": 5,
                "max_teams_per_org": 3,
                "updated_at": "",
            }
        ]
        limits = store._limits_for_rows(
            rows, OwnerIdentity(user_id="u1", group_id="g1", bot_id="b1")
        )
        assert limits.max_teams_per_user == 3
        assert limits.max_teams_per_org == 3

    @staticmethod
    def test_explicit_org_zero_is_honored_not_tightened(monkeypatch):
        """An org-scoped policy that explicitly sets Z=0 stays unlimited.

        The fallback must not tighten an explicit "no org cap" (Z=0) to the
        user-policy Z. Only a missing org-scoped policy triggers the fallback.
        """
        monkeypatch.setattr(quota_policy, "matches", lambda _expr, _ident: True)

        class _Store(quota_policy.TeamQuotaPolicyStore):
            pass

        store = _Store()
        rows = [
            {
                "policy_id": "org",
                "enabled": True,
                "match_expr": "group_id == 'g1'",
                "priority": 5,
                "max_teams_per_user": 5,
                "max_teammates_per_team": 5,
                "max_teams_per_org": 0,
                "updated_at": "",
            },
            {
                "policy_id": "user",
                "enabled": True,
                "match_expr": "user_id == 'u1'",
                "priority": 10,
                "max_teams_per_user": 3,
                "max_teammates_per_team": 5,
                "max_teams_per_org": 3,
                "updated_at": "",
            },
        ]
        limits = store._limits_for_rows(
            rows, OwnerIdentity(user_id="u1", group_id="g1", bot_id="b1")
        )
        assert limits.max_teams_per_user == 3
        assert limits.max_teams_per_org == 0

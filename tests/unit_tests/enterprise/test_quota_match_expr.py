# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared normalization for quota match expressions read from persistence."""

from __future__ import annotations

import pytest

from agent_gateway.common.match_expr import coerce_match_expr
from agent_gateway.enterprise.config.manager_config.core.quota import team_quota
from agent_gateway.enterprise.team import quota_policy


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, []),
        ("", []),
        ("   ", []),
        ("null", None),
        ('{"user_id": "u1"}', {"user_id": "u1"}),
        (
            '["user_id == \'u1\'", "group_id == \'g1\'"]',
            ["user_id == 'u1'", "group_id == 'g1'"],
        ),
        ('"user_id == \'u1\'"', "user_id == 'u1'"),
        ("42", 42),
        ("not-json", "not-json"),
        ("  not-json  ", "not-json"),
        (42, 42),
        (True, "True"),
    ],
)
def test_coerce_match_expr_normalizes_persisted_values(raw, expected):
    assert coerce_match_expr(raw) == expected


def test_coerce_match_expr_preserves_list_and_dict_values():
    expression = ["user_id == 'u1'"]
    expression_object = {"user_id": "u1"}

    assert coerce_match_expr(expression) is expression
    assert coerce_match_expr(expression_object) is expression_object


@pytest.mark.parametrize("module", [quota_policy, team_quota])
def test_quota_modules_share_persisted_json_handling(module):
    assert module._is_unconditional("[]") is True
    assert module._is_unconditional('["user_id == \'u1\'"]') is False
    assert (
        module._expr_blob('["user_id == \'u1\'", "group_id == \'g1\'"]')
        == "user_id == 'u1'\ngroup_id == 'g1'"
    )

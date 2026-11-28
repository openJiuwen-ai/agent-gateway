# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""sort_by_order 内存排序兜底的排序键类型安全回归。"""

from agent_gateway.storage.backends.db.reader import sort_by_order


def _ids(rows: list[dict]) -> list:
    return [row["id"] for row in rows]


def test_null_mixed_with_string_does_not_crash():
    """NULL 与字符串混排列（可空时间字段常见形态）不应抛 TypeError。"""
    rows = [
        {"id": "a", "deadline": "2026-01-15"},
        {"id": "b", "deadline": None},
        {"id": "c", "deadline": "2025-12-01"},
        {"id": "d", "deadline": "2026-03-08"},
    ]
    out = sort_by_order(rows, "deadline DESC")
    # None 恒排最前（DESC 时最先），其余按字典序倒序
    assert _ids(out) == ["b", "d", "a", "c"]
    assert _ids(sort_by_order(rows, "deadline")) == ["c", "a", "d", "b"]


def test_numeric_string_mixed_with_text_does_not_crash():
    """可转 int 的值与普通字符串同列时不应抛 TypeError。"""
    rows = [{"id": 1, "k": "50"}, {"id": 2, "k": "abc"}, {"id": 3, "k": "7"}]
    assert _ids(sort_by_order(rows, "k")) == [3, 1, 2]


def test_numeric_column_with_null_ordered_numerically():
    """数值列 + NULL：数值按数值序，NULL 恒排最前。"""
    rows = [{"id": 1, "v": 3}, {"id": 2, "v": None}, {"id": 3, "v": 1}]
    assert _ids(sort_by_order(rows, "v DESC")) == [2, 1, 3]
    assert _ids(sort_by_order(rows, "v")) == [3, 1, 2]


def test_desc_and_minus_prefix_syntax():
    rows = [{"id": 1, "v": 1}, {"id": 2, "v": 3}, {"id": 3, "v": 2}]
    assert _ids(sort_by_order(rows, "v DESC")) == [2, 3, 1]
    assert _ids(sort_by_order(rows, "-v")) == [2, 3, 1]
    assert _ids(sort_by_order(rows, "v")) == [1, 3, 2]


def test_empty_order_by_returns_rows_unchanged():
    rows = [{"id": 1}, {"id": 2}]
    assert sort_by_order(rows, "") is rows
    assert sort_by_order(rows, "   ") is rows
    assert sort_by_order(rows, "-") is rows

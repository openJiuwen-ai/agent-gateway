# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""sort_by_order 内存排序兜底的排序键类型安全回归。

语义与 SQL ORDER BY（sqlite）对齐：NULL 视为最小（ASC 最前 / DESC 最后）。
"""

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
    # NULL 视为最小：ASC 最前 / DESC 最后（与 sqlite ORDER BY 原生语义一致）
    assert _ids(out) == ["d", "a", "c", "b"]
    assert _ids(sort_by_order(rows, "deadline")) == ["b", "c", "a", "d"]


def test_numeric_string_mixed_with_text_does_not_crash():
    """可转 int 的值与普通字符串同列时不应抛 TypeError。"""
    rows = [{"id": 1, "k": "50"}, {"id": 2, "k": "abc"}, {"id": 3, "k": "7"}]
    assert _ids(sort_by_order(rows, "k")) == [3, 1, 2]


def test_numeric_column_with_null_ordered_numerically():
    """数值列 + NULL：NULL 视为最小（与 develop 及 sqlite 原生行为一致），数值按数值序。"""
    rows = [{"id": 1, "v": 3}, {"id": 2, "v": None}, {"id": 3, "v": 1}]
    assert _ids(sort_by_order(rows, "v DESC")) == [1, 3, 2]
    assert _ids(sort_by_order(rows, "v")) == [2, 3, 1]


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


def test_float_and_decimal_ordered_without_truncation():
    """float / Decimal 不被 int() 截断失序（检视意见：int("50") 语义修正）。"""
    rows = [{"id": 1, "v": 1.2}, {"id": 2, "v": 1.8}, {"id": 3, "v": 1.1}]
    assert _ids(sort_by_order(rows, "v")) == [3, 1, 2]
    assert _ids(sort_by_order(rows, "v DESC")) == [2, 1, 3]
    rows2 = [{"id": 1, "v": "1.9"}, {"id": 2, "v": "1"}]
    assert _ids(sort_by_order(rows2, "v")) == [2, 1]


def test_infinity_does_not_raise():
    """float("inf") / Decimal("Infinity") 不再抛 OverflowError。"""
    rows = [{"id": 1, "v": 1}, {"id": 2, "v": float("inf")}]
    assert _ids(sort_by_order(rows, "v")) == [1, 2]
    assert _ids(sort_by_order(rows, "v DESC")) == [2, 1]


def test_aware_datetimes_ordered_chronologically():
    """混时区 datetime 保留原生比较（按时刻排序），不降级为字典序。"""
    from datetime import datetime, timedelta, timezone
    dt_beijing = datetime(2026, 1, 15, 10, 0, tzinfo=timezone(timedelta(hours=8)))
    dt_utc = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)
    rows = [{"id": 1, "ts": dt_beijing}, {"id": 2, "ts": dt_utc}]
    # 北京 10:00 = 02:00 UTC，早于 UTC 10:00 → 时刻序 [1, 2]
    assert _ids(sort_by_order(rows, "ts")) == [1, 2]
    assert _ids(sort_by_order(rows, "ts DESC")) == [2, 1]

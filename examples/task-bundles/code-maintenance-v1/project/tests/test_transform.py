from __future__ import annotations

from project.src.transform import transform_rows


def test_transform_rows_returns_rows() -> None:
    rows = [{"Order ID": "1001", "Amount USD": "12.50"}]
    assert transform_rows(rows) == rows

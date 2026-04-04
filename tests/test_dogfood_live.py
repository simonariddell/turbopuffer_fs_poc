from __future__ import annotations

import os

import pytest

from turbopuffer_fs.dogfood import run_dogfood


pytestmark = pytest.mark.skipif(
    os.environ.get("TURBOPUFFER_FS_LIVE") != "1"
    or not os.environ.get("TURBOPUFFER_API_KEY")
    or not os.environ.get("TURBOPUFFER_REGION"),
    reason="live dogfood tests require TURBOPUFFER_FS_LIVE=1 plus turbopuffer credentials",
)


def test_live_dogfood_short_seeded_run() -> None:
    summary = run_dogfood(
        steps=12,
        seed=7,
        mount_prefix="dogfoodlive",
        keep_on_fail=False,
        keep_always=False,
    )
    assert summary["steps"] == 12
    assert summary["counts"]

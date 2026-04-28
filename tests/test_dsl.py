"""DSL evaluator unit tests — isolated from cmd_plot, exercise pa.dsl directly."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pa.dsl import (
    And, BinOp, Const, Contains, Count, CountDistinct, Eq, FromSource, In,
    IsNotNull, Median, Not, Or, Ratio, Sum, Var,
)


def ms(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def make_rows():
    """Three PRs across two months, mixed states/reviewers."""
    return [
        {"state": "MERGED",   "created_date": ms("2026-01-05"),
         "closed_date": ms("2026-01-10"), "repo_id": 1, "reviewers": '["alice"]'},
        {"state": "DECLINED", "created_date": ms("2026-01-08"),
         "closed_date": ms("2026-01-12"), "repo_id": 1, "reviewers": '["bob"]'},
        {"state": "MERGED",   "created_date": ms("2026-02-15"),
         "closed_date": ms("2026-02-20"), "repo_id": 2, "reviewers": '["alice","bob"]'},
    ]


# ── filters ───────────────────────────────────────────────────────────────────

def test_eq_with_var():
    rows = make_rows()
    expr = Count(where=Eq("state", Var("state")))
    assert expr.eval(rows, "month", {"state": "MERGED"}) == {"2026-01": 1, "2026-02": 1}
    assert expr.eval(rows, "month", {"state": "DECLINED"}) == {"2026-01": 1}


def test_in_filter():
    rows = make_rows()
    expr = Count(where=In("state", ["MERGED", "DECLINED"]))
    assert expr.eval(rows, "month", {}) == {"2026-01": 2, "2026-02": 1}


def test_and_filter():
    rows = make_rows()
    expr = Count(where=And((Eq("state", "MERGED"), Eq("repo_id", 1))))
    assert expr.eval(rows, "month", {}) == {"2026-01": 1}


def test_or_filter():
    rows = make_rows()
    expr = Count(where=Or((Eq("repo_id", 1), Eq("repo_id", 99))))
    assert expr.eval(rows, "month", {}) == {"2026-01": 2}


def test_not_filter():
    rows = make_rows()
    expr = Count(where=Not(Eq("state", "MERGED")))
    assert expr.eval(rows, "month", {}) == {"2026-01": 1}


def test_contains_value_in_field():
    """Contains: $slug in reviewers (membership in a JSON-array field)."""
    rows = make_rows()
    expr = Count(where=Contains("alice", "reviewers"))
    assert expr.eval(rows, "month", {}) == {"2026-01": 1, "2026-02": 1}
    expr_var = Count(where=Contains(Var("slug"), "reviewers"))
    assert expr_var.eval(rows, "month", {"slug": "bob"}) == {"2026-01": 1, "2026-02": 1}


def test_contains_with_set_field():
    """Contains works against a per-row set (e.g. commenters)."""
    rows = [
        {"closed_date": ms("2026-01-10"), "commenters": {"alice", "bob"}},
        {"closed_date": ms("2026-01-12"), "commenters": {"carol"}},
    ]
    expr = Count(where=Contains("alice", "commenters"))
    assert expr.eval(rows, "month", {}) == {"2026-01": 1}


def test_is_not_null():
    rows = [
        {"state": "MERGED", "closed_date": ms("2026-01-10"), "x": 5},
        {"state": "MERGED", "closed_date": ms("2026-01-12"), "x": None},
    ]
    assert Count(where=IsNotNull("x")).eval(rows, "month", {}) == {"2026-01": 1}


# ── aggregators ───────────────────────────────────────────────────────────────

def test_count_with_no_filter():
    rows = make_rows()
    assert Count().eval(rows, "month", {}) == {"2026-01": 2, "2026-02": 1}


def test_count_distinct():
    rows = make_rows() + [{"state": "MERGED", "closed_date": ms("2026-01-15"),
                            "repo_id": 1, "reviewers": '[]'}]
    # Jan: repos {1} (3 rows but all repo_id=1); Feb: repos {2}
    assert CountDistinct("repo_id").eval(rows, "month", {}) == {"2026-01": 1, "2026-02": 1}


def test_sum_with_field():
    rows = [
        {"closed_date": ms("2026-01-10"), "n": 3},
        {"closed_date": ms("2026-01-15"), "n": 5},
        {"closed_date": ms("2026-02-01"), "n": 2},
    ]
    assert Sum("n").eval(rows, "month", {}) == {"2026-01": 8.0, "2026-02": 2.0}


def test_sum_with_callable_field():
    rows = [
        {"closed_date": ms("2026-01-10"), "created_date": ms("2026-01-08")},
        {"closed_date": ms("2026-01-15"), "created_date": ms("2026-01-12")},
    ]
    cycle = lambda r: (r["closed_date"] - r["created_date"]) / 86400_000  # days
    assert Sum(cycle).eval(rows, "month", {}) == {"2026-01": 5.0}  # 2 + 3


def test_median():
    rows = [
        {"closed_date": ms("2026-01-10"), "h": 1.0},
        {"closed_date": ms("2026-01-12"), "h": 2.0},
        {"closed_date": ms("2026-01-15"), "h": 3.0},
        {"closed_date": ms("2026-02-01"), "h": 7.0},
    ]
    assert Median("h").eval(rows, "month", {}) == {"2026-01": 2.0, "2026-02": 7.0}


def test_bucket_field_override():
    """Aggregate by created_date instead of default closed_date."""
    rows = [
        {"created_date": ms("2026-01-05"), "closed_date": ms("2026-02-20"), "state": "M"},
    ]
    by_closed  = Count().eval(rows, "month", {})
    by_created = Count(bucket_field="created_date").eval(rows, "month", {})
    assert by_closed  == {"2026-02": 1}
    assert by_created == {"2026-01": 1}


# ── binary ops ────────────────────────────────────────────────────────────────

def test_ratio_basic():
    rows = make_rows()
    # acceptance_rate = MERGED / (MERGED+DECLINED) * 100
    expr = Ratio(
        Count(where=Eq("state", "MERGED")),
        Count(where=In("state", ["MERGED", "DECLINED"])),
    )
    out = expr.eval(rows, "month", {})
    assert out["2026-01"] == pytest.approx(50.0)   # 1/2
    assert out["2026-02"] == pytest.approx(100.0)  # 1/1


def test_const_broadcast():
    rows = make_rows()
    # 2 * count(merged) = doubles
    expr = BinOp("*", Const(2), Count(where=Eq("state", "MERGED")))
    assert expr.eval(rows, "month", {}) == {"2026-01": 2, "2026-02": 2}


def test_addition_treats_missing_as_zero():
    rows = [
        {"closed_date": ms("2026-01-10"), "state": "MERGED"},
        {"closed_date": ms("2026-02-10"), "state": "DECLINED"},
    ]
    expr = BinOp("+",
                 Count(where=Eq("state", "MERGED")),
                 Count(where=Eq("state", "DECLINED")))
    # Jan: 1+0=1; Feb: 0+1=1
    assert expr.eval(rows, "month", {}) == {"2026-01": 1, "2026-02": 1}


def test_division_yields_zero_when_numerator_empty():
    """Denominator drives: bucket present in denom but missing in num → 0.0.
    This matches rate semantics ('0 of N items match' = 0%, not undefined)."""
    rows = [{"closed_date": ms("2026-01-10"), "state": "DECLINED"}]
    expr = BinOp("/", Count(where=Eq("state", "MERGED")), Count())
    assert expr.eval(rows, "month", {}) == {"2026-01": 0.0}


def test_division_drops_zero_denominator():
    """When denominator is itself 0 in a bucket, the bucket is dropped (skipped)."""
    rows = [{"closed_date": ms("2026-01-10"), "state": "MERGED"}]
    # numerator: state=DECLINED → 0;  denominator: state=DECLINED → 0
    expr = BinOp("/", Count(where=Eq("state", "DECLINED")),
                      Count(where=Eq("state", "DECLINED")))
    assert expr.eval(rows, "month", {}) == {}


def test_const_only():
    assert Const(42.0).eval([], "month", {}) == {"__const__": 42.0}
    assert BinOp("+", Const(1), Const(2)).eval([], "month", {}) == {"__const__": 3.0}


def test_from_source_replaces_rows():
    """FromSource calls a source(vars) function and pipes its rows to inner."""
    fake_rows = [
        {"closed_date": ms("2026-01-10"), "verdict": "yes"},
        {"closed_date": ms("2026-01-12"), "verdict": "no"},
        {"closed_date": ms("2026-02-01"), "verdict": "yes"},
    ]
    expr = FromSource(
        source=lambda vars: fake_rows,
        inner=Count(where=Eq("verdict", "yes")),
    )
    # Outer rows are ignored — FromSource swaps in fake_rows
    assert expr.eval([{"closed_date": ms("2030-01-01")}], "month", {}) \
        == {"2026-01": 1, "2026-02": 1}


def test_from_source_passes_vars_through():
    """Inner expression sees the same vars (incl. resolution of Var())."""
    rows = [{"closed_date": ms("2026-01-10"), "verdict": "yes"}]
    captured: dict = {}
    def src(v):
        captured.update(v)
        return rows
    expr = FromSource(src, Count(where=Eq("verdict", Var("target"))))
    out = expr.eval([], "month", {"target": "yes", "extra": "X"})
    assert out == {"2026-01": 1}
    assert captured == {"target": "yes", "extra": "X"}


def test_weighted_sum_pattern():
    """merge_acceptance_rate = (YES + 0.5*PARTIAL) / (YES+PARTIAL+NO) * 100."""
    rows = [
        {"closed_date": ms("2026-01-10"), "verdict": "YES"},
        {"closed_date": ms("2026-01-12"), "verdict": "PARTIAL"},
        {"closed_date": ms("2026-01-15"), "verdict": "NO"},
    ]
    yes_part = BinOp("+",
                     Count(where=Eq("verdict", "YES")),
                     BinOp("*", Const(0.5), Count(where=Eq("verdict", "PARTIAL"))))
    total    = Count(where=In("verdict", ["YES", "PARTIAL", "NO"]))
    expr     = Ratio(yes_part, total)
    out      = expr.eval(rows, "month", {})
    # (1 + 0.5)/3 * 100 = 50.0
    assert out["2026-01"] == pytest.approx(50.0)

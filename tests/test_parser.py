"""Parser tests — direct grammar checks + roundtrip against format_expr."""
from __future__ import annotations

import pytest

from pa.dsl import (
    And, BinOp, Const, Contains, Count, CountDistinct, Eq, FromSource, In,
    IsNotNull, Median, Or, Ratio, Sum, Var, format_expr,
)
from pa.metrics import METRICS
from pa.parser import parse_expr


# ── tokenizer + grammar smoke tests ───────────────────────────────────────────

def test_count_no_filter():
    e = parse_expr("count()")
    assert e == Count()


def test_count_with_eq_var():
    e = parse_expr("count(state=$state)")
    assert e == Count(where=Eq("state", Var("state")))


def test_count_with_string_literal():
    e = parse_expr("count(state='MERGED')")
    assert e == Count(where=Eq("state", "MERGED"))


def test_count_with_in():
    e = parse_expr("count(state in ['MERGED', 'DECLINED'])")
    assert e == Count(where=In("state", ["MERGED", "DECLINED"]))


def test_count_with_and_or():
    e = parse_expr("count((state=$state and ($slug in reviewers or $other in commenters)))")
    assert e == Count(where=And((
        Eq("state", Var("state")),
        Or((Contains(Var("slug"), "reviewers"),
            Contains(Var("other"), "commenters"))),
    )))


def test_contains_value_in_field():
    e = parse_expr("count('alice' in reviewers)")
    assert e == Count(where=Contains("alice", "reviewers"))


def test_contains_with_var():
    e = parse_expr("count($slug in commenters)")
    assert e == Count(where=Contains(Var("slug"), "commenters"))


def test_count_with_bucket_override():
    e = parse_expr("count(state='MERGED', @created_date)")
    assert e == Count(where=Eq("state", "MERGED"), bucket_field="created_date")


def test_count_only_bucket():
    e = parse_expr("count(@created_date)")
    assert e == Count(bucket_field="created_date")


def test_count_distinct():
    e = parse_expr("count_distinct(repo_id, state='MERGED', @created_date)")
    assert e == CountDistinct("repo_id", where=Eq("state", "MERGED"),
                              bucket_field="created_date")


def test_is_not_null():
    e = parse_expr("count(file_path is not null)")
    assert e == Count(where=IsNotNull("file_path"))


def test_ratio_sugar():
    e = parse_expr("ratio(count(state='MERGED'), count())")
    # Ratio = BinOp("*", BinOp("/", num, den), Const(100))
    assert isinstance(e, BinOp) and e.op == "*"
    assert isinstance(e.left, BinOp) and e.left.op == "/"
    assert e.right == Const(100.0)


def test_arithmetic_precedence():
    e = parse_expr("count() + 0.5 * count(state=$state)")
    # + binds looser than *
    assert isinstance(e, BinOp) and e.op == "+"
    assert isinstance(e.right, BinOp) and e.right.op == "*"


def test_source_wrapper():
    e = parse_expr("@analysis(count(verdict='yes'))")
    assert isinstance(e, FromSource)
    assert isinstance(e.inner, Count)


def test_unknown_source_errors():
    with pytest.raises(SyntaxError, match="unknown source"):
        parse_expr("@bogus(count())")


def test_unknown_function_errors():
    with pytest.raises(SyntaxError, match="unknown function"):
        parse_expr("zorch(state=$state)")


def test_unbalanced_parens_errors():
    with pytest.raises(SyntaxError):
        parse_expr("count(state=$state")


# ── roundtrip: every registered metric expr should parse back to itself ──────

@pytest.mark.parametrize("name", sorted(n for n in METRICS if METRICS[n].expr is not None))
def test_roundtrip_each_metric(name):
    """parse(format_expr(metric.expr)) should yield the same AST.

    Lambdas (Sum/Median field) won't roundtrip — those metrics are skipped
    in the equality check by re-formatting and comparing strings instead.
    """
    expr = METRICS[name].expr
    rendered = format_expr(expr)

    # Strip any leading whitespace/newlines from multi-line render so the
    # parser sees the expression as one unit.
    flat = " ".join(line.strip() for line in rendered.splitlines() if line.strip())

    try:
        parsed = parse_expr(flat)
    except SyntaxError as e:
        pytest.fail(f"failed to parse:\n{flat}\n→ {e}")

    # For lambdas in field positions we can't compare AST directly — fall back
    # to comparing the formatted strings.
    re_rendered = format_expr(parsed)
    flat_re = " ".join(line.strip() for line in re_rendered.splitlines() if line.strip())
    assert flat_re == flat, (
        f"roundtrip mismatch for {name!r}:\n"
        f"  original:   {flat}\n"
        f"  re-render:  {flat_re}"
    )

"""
Metric DSL — declarative aggregations over PR-derived rows.

Goal: replace per-metric custom Python in metrics.py with composable
expressions. Today this covers @pr source only (Phase 1). Other sources
(@comments, @analysis, @merge) are added in Phase 2.

Core types
----------
Filter      — predicate on a row: (row, vars) -> bool
Aggregator  — count/sum/median/count_distinct over rows → {bucket: value}
Expr        — Aggregator | BinOp | Const, evaluates to {bucket: value}

Variables
---------
Var("state"), Var("author"), Var("reviewer_slug"), ...
Resolved against the `vars` dict passed to evaluate().

Buckets
-------
Each Aggregator buckets rows by `bucket_field` (default "closed_date"),
applying the period grouping (week/month). Override per metric via
`bucket_field=` kwarg.
"""
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from typing import Any, Callable

from pa.buckets import bucket_key

# ── Variables and field references ────────────────────────────────────────────

@dataclass
class Var:
    """A CLI-supplied variable: $state, $author, $reviewer_slug, $judge_model."""
    name: str


def _resolve(value: Any, vars: dict) -> Any:
    """Unwrap Var(name) → vars[name]; pass other values through."""
    return vars.get(value.name) if isinstance(value, Var) else value


# ── Filters: row × vars → bool ────────────────────────────────────────────────

class Filter:
    def __call__(self, row: dict, vars: dict) -> bool:
        raise NotImplementedError


@dataclass
class Eq(Filter):
    field: str
    value: Any  # literal or Var

    def __call__(self, row, vars):
        return row.get(self.field) == _resolve(self.value, vars)


@dataclass
class In(Filter):
    field: str
    values: list[Any]

    def __call__(self, row, vars):
        resolved = {_resolve(v, vars) for v in self.values}
        return row.get(self.field) in resolved


@dataclass
class IsNotNull(Filter):
    field: str

    def __call__(self, row, vars):
        return row.get(self.field) is not None


@dataclass
class Contains(Filter):
    """`value in row.field` — membership test against a per-row collection.
    `field` is a row attribute holding a list/set/JSON array (e.g. reviewers,
    commenters). When a string, parsed as JSON for backward compatibility
    with legacy rows that store reviewers as a JSON-encoded string.
    """
    value: Any  # literal or Var
    field: str

    def __call__(self, row, vars):
        target = _resolve(self.value, vars)
        if target is None:
            return False
        coll = row.get(self.field)
        if isinstance(coll, str):
            try:
                coll = json.loads(coll or "[]")
            except (json.JSONDecodeError, ValueError):
                coll = []
        return target in (coll or [])


@dataclass
class And(Filter):
    parts: tuple[Filter, ...]

    def __call__(self, row, vars):
        return all(p(row, vars) for p in self.parts)


@dataclass
class Or(Filter):
    parts: tuple[Filter, ...]

    def __call__(self, row, vars):
        return any(p(row, vars) for p in self.parts)


@dataclass
class Not(Filter):
    inner: Filter

    def __call__(self, row, vars):
        return not self.inner(row, vars)


# ── Expressions: rows × period × vars → {bucket: value} ───────────────────────

class Expr:
    def eval(self, rows: list[dict], period: str, vars: dict) -> dict[str, float]:
        """Single-series evaluation — returns {bucket: value}."""
        raise NotImplementedError

    def eval_series(self, rows: list[dict], period: str, vars: dict
                    ) -> list[tuple[str, dict[str, float]]]:
        """Multi-series evaluation — returns [(label, {bucket: value}), ...].
        Default: wrap single-series eval in one entry with empty label.
        Group/Split override this to produce multiple entries.
        """
        return [("", self.eval(rows, period, vars))]


@dataclass
class Const(Expr):
    value: float

    def eval(self, rows, period, vars):
        # Constant has no buckets; binary ops broadcast it across the other side.
        return {"__const__": self.value}


# ── Aggregators ───────────────────────────────────────────────────────────────

@dataclass
class Count(Expr):
    """count(*) per bucket, with optional row filter."""
    where: Filter | None = None
    bucket_field: str = "closed_date"

    def eval(self, rows, period, vars):
        out: dict[str, int] = {}
        for r in rows:
            ts = r.get(self.bucket_field)
            if not ts:
                continue
            if self.where and not self.where(r, vars):
                continue
            bk = bucket_key(ts, period)
            out[bk] = out.get(bk, 0) + 1
        return {k: float(v) for k, v in out.items()}


@dataclass
class CountDistinct(Expr):
    """count(distinct field) per bucket, with optional row filter."""
    field: str
    where: Filter | None = None
    bucket_field: str = "closed_date"

    def eval(self, rows, period, vars):
        sets: dict[str, set] = {}
        for r in rows:
            ts = r.get(self.bucket_field)
            if not ts:
                continue
            if self.where and not self.where(r, vars):
                continue
            sets.setdefault(bucket_key(ts, period), set()).add(r.get(self.field))
        return {k: float(len(v)) for k, v in sets.items()}


@dataclass
class Sum(Expr):
    """sum(field) per bucket, with optional row filter. field can be a callable
    (row -> number) for derived values like (closed_date - created_date)/3600000."""
    field: str | Callable
    where: Filter | None = None
    bucket_field: str = "closed_date"

    def eval(self, rows, period, vars):
        out: dict[str, float] = {}
        for r in rows:
            ts = r.get(self.bucket_field)
            if not ts:
                continue
            if self.where and not self.where(r, vars):
                continue
            v = self.field(r) if callable(self.field) else r.get(self.field)
            if v is None:
                continue
            bk = bucket_key(ts, period)
            out[bk] = out.get(bk, 0) + float(v)
        return out


@dataclass
class Median(Expr):
    """median(field) per bucket, with optional row filter. field can be a callable."""
    field: str | Callable
    where: Filter | None = None
    bucket_field: str = "closed_date"

    def eval(self, rows, period, vars):
        per_bucket: dict[str, list[float]] = {}
        for r in rows:
            ts = r.get(self.bucket_field)
            if not ts:
                continue
            if self.where and not self.where(r, vars):
                continue
            v = self.field(r) if callable(self.field) else r.get(self.field)
            if v is None:
                continue
            per_bucket.setdefault(bucket_key(ts, period), []).append(float(v))
        return {k: statistics.median(vs) for k, vs in per_bucket.items()}


# ── Binary operations: combine two expressions per bucket ────────────────────

@dataclass
class BinOp(Expr):
    """Element-wise binary op over buckets. Const broadcasts; missing buckets
    on either side: + and − treat missing as 0; * and / require both sides."""
    op: str  # "+", "-", "*", "/"
    left: Expr
    right: Expr

    def _combine_buckets(self, L: dict, R: dict) -> dict:
        l_const = "__const__" in L
        r_const = "__const__" in R
        if l_const and r_const:
            return {"__const__": _apply_op(self.op, L["__const__"], R["__const__"])}

        out: dict[str, float] = {}
        if l_const:
            cv = L["__const__"]
            for bk, v in R.items():
                out[bk] = _apply_op(self.op, cv, v)
            return out
        if r_const:
            cv = R["__const__"]
            for bk, v in L.items():
                out[bk] = _apply_op(self.op, v, cv)
            return out

        # Bucket key choice:
        #   + / - : union (missing side counts as 0 — additive identity)
        #   *     : intersection (multiplicative identity is 1, but we don't fabricate it)
        #   /     : right (denominator) drives — missing numerator = 0
        if self.op in ("+", "-"):
            keys = set(L) | set(R)
        elif self.op == "/":
            keys = set(R)
        else:
            keys = set(L) & set(R)
        for bk in keys:
            lv = L.get(bk, 0.0)
            rv = R.get(bk, 0.0)
            try:
                out[bk] = _apply_op(self.op, lv, rv)
            except ZeroDivisionError:
                continue  # skip undefined buckets in division
        return out

    def eval(self, rows, period, vars):
        return self._combine_buckets(
            self.left.eval(rows, period, vars),
            self.right.eval(rows, period, vars),
        )

    def eval_series(self, rows, period, vars):
        """Combine multi-series sides label-by-label.

        Cross-source ratio like `ratio(@merge(group(p, ...)), @comments(group(p, ...)))`
        produces a multi-series numerator and denominator with matching project
        labels — pair them up. If one side is single-series with empty label
        ("") it broadcasts across all labels of the other side.
        """
        L = self.left.eval_series(rows, period, vars)
        R = self.right.eval_series(rows, period, vars)

        single_L = len(L) == 1 and L[0][0] == ""
        single_R = len(R) == 1 and R[0][0] == ""
        if single_L and single_R:
            return [("", self._combine_buckets(L[0][1], R[0][1]))]

        l_map = dict(L)
        r_map = dict(R)
        labels = sorted(set(l_map) | set(r_map))
        out: list[tuple[str, dict]] = []
        for label in labels:
            if label == "" and (single_L or single_R):
                continue  # placeholder side, handled via broadcast below
            l_buckets = l_map.get(label) if not single_L else l_map.get("")
            r_buckets = r_map.get(label) if not single_R else r_map.get("")
            if l_buckets is None or r_buckets is None:
                continue
            out.append((label, self._combine_buckets(l_buckets, r_buckets)))
        return out


def _apply_op(op: str, a: float, b: float) -> float:
    if op == "+": return a + b
    if op == "-": return a - b
    if op == "*": return a * b
    if op == "/": return a / b
    raise ValueError(f"unknown op: {op}")


# ── Row-level expressions (callable: row -> number | None) ────────────────────
# Used as the `field` of Sum/Median when it's an arithmetic combination of
# row fields. They are callable so existing call sites that do `field(r)`
# continue to work, but they're also dataclasses so the parser/printer
# can introspect them for roundtripping.

class RowExpr:
    def __call__(self, row: dict):
        raise NotImplementedError


@dataclass
class RowField(RowExpr):
    name: str
    def __call__(self, row):
        return row.get(self.name)


@dataclass
class RowConst(RowExpr):
    value: Any
    def __call__(self, row):
        return self.value


@dataclass
class RowBinOp(RowExpr):
    op: str
    left: RowExpr
    right: RowExpr
    def __call__(self, row):
        l = self.left(row)
        r = self.right(row)
        if l is None or r is None:
            return None
        try:
            return _apply_op(self.op, l, r)
        except ZeroDivisionError:
            return None


def _fmt_row_expr(r) -> str:
    if isinstance(r, RowField):
        return r.name
    if isinstance(r, RowConst):
        return repr(r.value)
    if isinstance(r, RowBinOp):
        return f"({_fmt_row_expr(r.left)} {r.op} {_fmt_row_expr(r.right)})"
    return repr(r)


# ── DateRange: filter input rows to a date window before evaluating inner ───

@dataclass
class DateRange(Expr):
    """Restrict the data window to `created_date` ∈ [since, until].

    Three things happen:
      1. Incoming `rows` are filtered.
      2. `vars["_pr_rows"]` is filtered (so a downstream `@pr` source sees
         the trimmed window even though it fetches via vars, not the row arg).
      3. `_since_ts`/`_until_ts` are injected/overwritten so source fetchers
         (`@comments`/`@analysis`/`@merge`) issue the correct SQL filter.
    """
    since: str | None  # YYYY-MM-DD
    until: str | None
    inner: Expr

    def _ctx(self, vars):
        from pa.utils import date_to_ms
        s = date_to_ms(self.since) if self.since else None
        u = date_to_ms(self.until, end_of_day=True) if self.until else None
        new_vars = dict(vars)
        if s is not None: new_vars["_since_ts"] = s
        if u is not None: new_vars["_until_ts"] = u
        if "_pr_rows" in vars:
            new_vars["_pr_rows"] = self._filter(vars["_pr_rows"], s, u)
        return s, u, new_vars

    def _filter(self, rows, s, u):
        if s is None and u is None:
            return rows
        return [
            r for r in rows
            if (s is None or (r.get("created_date") or 0) >= s)
            and (u is None or (r.get("created_date") or 0) <= u)
        ]

    def eval(self, rows, period, vars):
        s, u, new_vars = self._ctx(vars)
        return self.inner.eval(self._filter(rows, s, u), period, new_vars)

    def eval_series(self, rows, period, vars):
        s, u, new_vars = self._ctx(vars)
        return self.inner.eval_series(self._filter(rows, s, u), period, new_vars)


# ── Period: override the time-bucket granularity passed downstream ───────────

@dataclass
class Period(Expr):
    """Set the bucket granularity (week/month) for the inner expression.
    Outermost in auto_wrap so --period is visible in --explain."""
    period: str  # "week" | "month"
    inner: Expr

    def eval(self, rows, period, vars):
        return self.inner.eval(rows, self.period, vars)

    def eval_series(self, rows, period, vars):
        return self.inner.eval_series(rows, self.period, vars)


# ── Multi-series wrappers: Group, Split ──────────────────────────────────────

@dataclass
class Group(Expr):
    """Partition rows by row[field], evaluate inner per partition, emit
    one series per group value. Composes with Split (Group(Split(inner)))."""
    field: str
    inner: Expr

    def eval(self, rows, period, vars):
        raise NotImplementedError("Group is multi-series; use eval_series()")

    def eval_series(self, rows, period, vars):
        partitions: dict[str, list] = {}
        for r in rows:
            partitions.setdefault(r.get(self.field, ""), []).append(r)
        out: list[tuple[str, dict]] = []
        for g in sorted(partitions):
            for sub_label, buckets in self.inner.eval_series(partitions[g], period, vars):
                combined = f"{g} / {sub_label}" if sub_label else g
                out.append((combined, buckets))
        return out


@dataclass
class Mean(Expr):
    """Partition rows by row[field], evaluate inner per partition, then
    return the arithmetic mean of inner's bucket values across partitions
    (single series). The dual of Group: Group emits N series, Mean emits 1."""
    field: str
    inner: Expr

    def eval(self, rows, period, vars):
        partitions: dict[str, list] = {}
        for r in rows:
            partitions.setdefault(r.get(self.field, ""), []).append(r)
        per_bucket: dict[str, list[float]] = {}
        for prows in partitions.values():
            buckets = self.inner.eval(prows, period, vars)
            for bk, v in buckets.items():
                per_bucket.setdefault(bk, []).append(v)
        return {bk: sum(vs) / len(vs) for bk, vs in per_bucket.items() if vs}

    def eval_series(self, rows, period, vars):
        return [("", self.eval(rows, period, vars))]


@dataclass
class Split(Expr):
    """Repo-level cohort split. Two cohorts emitted:
       + : repos with ≥1 row matching the predicate (in --state if given)
       − : repos with strictly 0 such rows
    All rows of a repo go into the cohort the repo belongs to.
    `kind` is informational; the matching is delegated to a Filter `pred`.
    """
    kind: str           # "reviewer" | "commenter" — used only for label
    slug: Any           # Var or literal — used only for label
    pred: Filter        # row → bool, classifies a row as "+ candidate"
    inner: Expr

    def eval(self, rows, period, vars):
        raise NotImplementedError("Split is multi-series; use eval_series()")

    def eval_series(self, rows, period, vars):
        slug_value = _resolve(self.slug, vars)
        state_filter = vars.get("state")
        plus_repos: set = set()
        for r in rows:
            if state_filter and r.get("state") != state_filter:
                continue
            if self.pred(r, vars):
                plus_repos.add(r.get("repo_id"))
        plus_rows  = [r for r in rows if r.get("repo_id") in plus_repos]
        minus_rows = [r for r in rows if r.get("repo_id") not in plus_repos]
        out: list[tuple[str, dict]] = []
        for tag, sub in [(f"+ {slug_value}", plus_rows),
                         (f"- {slug_value}", minus_rows)]:
            for sub_label, buckets in self.inner.eval_series(sub, period, vars):
                out.append((f"{tag} / {sub_label}" if sub_label else tag, buckets))
        return out


# ── Sources: produce rows from somewhere other than the default series rows ──

@dataclass
class FromSource(Expr):
    """Switch the row-set under this expression by calling a source function.

    A source is a callable `source(vars) -> list[dict]` that fetches and
    normalizes rows (typically by joining a non-PR table with pull_requests
    so each row has bucket-able fields like closed_date).

    `vars` carries CLI args (state, author, judge_model, …) plus context
    keys injected by cmd_plot:  _conn, _since_ts, _until_ts, _repo_ids.
    """
    source: Callable        # (vars) -> list[dict]
    inner: Expr

    def eval(self, rows, period, vars):
        return self.inner.eval(self.source(vars), period, vars)

    def eval_series(self, rows, period, vars):
        return self.inner.eval_series(self.source(vars), period, vars)


# ── Sugar constructors (readable Python definitions) ──────────────────────────

def Ratio(num: Expr, den: Expr, scale: float = 100.0) -> Expr:
    """num / den * scale — common pattern for rates expressed in %."""
    return BinOp("*", BinOp("/", num, den), Const(scale))


def _has_source(e) -> bool:
    """True if any FromSource appears in the expression tree."""
    if isinstance(e, FromSource):
        return True
    if isinstance(e, BinOp):
        return _has_source(e.left) or _has_source(e.right)
    if isinstance(e, (Group, Split, Mean, Period, DateRange)):
        return _has_source(e.inner)
    return False


def find_outer_ratio(e):
    """Walk wrappers (Period, DateRange, FromSource, Group, Split) to find
    a Ratio at the data-level. Returns (num_expr, den_expr) or None.
    Stops at Mean (group-collapsing) — for averaged ratios the per-bucket
    numerator/denominator pair is no longer meaningful."""
    cur = e
    while True:
        r = _is_ratio(cur)
        if r is not None:
            return r  # (num, den)
        if isinstance(cur, Mean):
            return None
        if isinstance(cur, (Period, DateRange, FromSource, Group, Split)):
            cur = cur.inner
            continue
        return None


def replace_ratio(e, replacement):
    """Return a copy of `e` with its outermost Ratio replaced by `replacement`.
    Preserves all wrappers (Period/Range/Group/Split/@source). Returns `e`
    unchanged if no Ratio is reachable. Does not descend into Mean — the
    sister of `find_outer_ratio`."""
    from dataclasses import replace as _dc_replace
    if _is_ratio(e) is not None:
        return replacement
    if isinstance(e, (Period, DateRange, FromSource, Group, Split)):
        return _dc_replace(e, inner=replace_ratio(e.inner, replacement))
    return e


def has_mean(e) -> bool:
    """True if the expression collapses groups via Mean — used by the renderer
    to draw such series with a bold dashed style ("baseline overlay")."""
    if isinstance(e, Mean):
        return True
    if isinstance(e, BinOp):
        return has_mean(e.left) or has_mean(e.right)
    if isinstance(e, (Group, Split, FromSource, Period, DateRange)):
        return has_mean(e.inner)
    return False


def substitute_vars(node, values: dict) -> object:
    """Walk the AST and replace `Var(name)` with `values[name]` where present.
    Used by --new-dsl to bake CLI-provided slugs/state into the emitted DSL
    so the resulting command is self-contained.
    """
    def walk(n):
        if isinstance(n, Var):
            return values.get(n.name, n) if n.name in values else n
        if hasattr(n, "__dataclass_fields__"):
            from dataclasses import fields, replace
            new_kwargs = {}
            for f in fields(n):
                v = getattr(n, f.name)
                if isinstance(v, tuple):
                    v = tuple(walk(x) for x in v)
                elif isinstance(v, list):
                    v = [walk(x) for x in v]
                else:
                    v = walk(v)
                new_kwargs[f.name] = v
            return replace(n, **new_kwargs)
        return n
    return walk(node)


def _apply_split_group(inner: Expr, split: str | None, group_by: str | None,
                       skip_split: bool, default_per_repo: bool) -> Expr:
    """Apply Split / Group around `inner` per CLI flags."""
    if split and not skip_split:
        if split.startswith("reviewer:"):
            inner = Split("reviewer", Var("reviewer_slug"),
                          Contains(Var("reviewer_slug"), "reviewers"), inner)
        elif split.startswith("commenter:"):
            inner = Split("commenter", Var("commenter_slug"),
                          Contains(Var("commenter_slug"), "commenters"), inner)
        # "total[:label]" — explicit single combined series, no wrap

    if group_by == "project":
        inner = Group("project_key", inner)
    elif default_per_repo and not split and not skip_split:
        # No CLI grouping at all → default to per-repo series.
        inner = Group("repo_label", inner)
    return inner


def auto_wrap(expr: Expr, *, split: str | None = None, group_by: str | None = None,
              period: str | None = None, since: str | None = None,
              until: str | None = None, skip_split: bool = False) -> Expr:
    """Wrap a metric expression with Period/Range/@source/Group/Split based
    on CLI flags. The result is a fully self-describing DSL form for `--explain`.

    Order, outermost first:
      Period → DateRange → @source → Group → Split → inner

    For source-bearing metrics (already containing `@comments`/`@analysis`/
    `@merge`), Group/Split are inserted *inside* the existing FromSource so
    the source's pre-fetched rows are partitioned (rather than the empty
    outer rows). Default per-repo grouping is suppressed for source metrics
    since their rows live in different tables and a "repo_label" is not
    naturally available without extra joins — they emit a single series
    unless --group-by is explicitly provided.
    """
    if isinstance(expr, FromSource):
        # Insert Split/Group between the source and its inner aggregator.
        new_inner = _apply_split_group(
            expr.inner, split, group_by, skip_split, default_per_repo=False,
        )
        expr = FromSource(expr.source, new_inner)
    else:
        expr = _apply_split_group(
            expr, split, group_by, skip_split, default_per_repo=True,
        )
        # Surface @pr explicitly so --explain shows row provenance.
        from pa.sources import pr_source
        expr = FromSource(pr_source, expr)

    if since or until:
        expr = DateRange(since, until, expr)
    if period:
        expr = Period(period, expr)
    return expr


# ── Pretty-printer (for --explain) ────────────────────────────────────────────

_LINE_BUDGET = 90  # try to fit on one line under this many chars


def _fmt_val(v) -> str:
    if isinstance(v, Var):
        return f"${v.name}"
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    return repr(v)


def _fmt_filter(f) -> str:
    if isinstance(f, Eq):
        return f"{f.field}={_fmt_val(f.value)}"
    if isinstance(f, In):
        return f"{f.field} in [{', '.join(_fmt_val(v) for v in f.values)}]"
    if isinstance(f, IsNotNull):
        return f"{f.field} is not null"
    if isinstance(f, Contains):
        return f"{_fmt_val(f.value)} in {f.field}"
    if isinstance(f, And):
        return "(" + " and ".join(_fmt_filter(p) for p in f.parts) + ")"
    if isinstance(f, Or):
        return "(" + " or ".join(_fmt_filter(p) for p in f.parts) + ")"
    if isinstance(f, Not):
        return f"not {_fmt_filter(f.inner)}"
    return repr(f)


def _is_ratio(e) -> tuple | None:
    """Detect Ratio sugar: BinOp('*', BinOp('/', num, den), Const(100)).
    Strict on scale=100 to keep `ratio()` parser sugar unambiguous.
    Returns (num, den) or None."""
    if (isinstance(e, BinOp) and e.op == "*"
            and isinstance(e.left, BinOp) and e.left.op == "/"
            and isinstance(e.right, Const) and e.right.value == 100.0):
        return e.left.left, e.left.right
    return None


def _fmt_inline(e) -> str:
    """Single-line rendering."""
    if isinstance(e, Const):
        v = e.value
        return f"{int(v)}" if v == int(v) else f"{v}"

    ratio = _is_ratio(e)
    if ratio is not None:
        num, den = ratio
        return f"ratio({_fmt_inline(num)}, {_fmt_inline(den)})"

    if isinstance(e, BinOp):
        return f"({_fmt_inline(e.left)} {e.op} {_fmt_inline(e.right)})"

    if isinstance(e, FromSource):
        src = getattr(e.source, "__name__", repr(e.source))
        # Trim "_source" suffix for readability: analysis_source → @analysis
        if src.endswith("_source"):
            src = src[:-len("_source")]
        return f"@{src}({_fmt_inline(e.inner)})"

    if isinstance(e, Group):
        return f"group({e.field}, {_fmt_inline(e.inner)})"

    if isinstance(e, Mean):
        return f"mean({e.field}, {_fmt_inline(e.inner)})"

    if isinstance(e, Split):
        return f"split({e.kind}:{_fmt_val(e.slug)}, {_fmt_inline(e.inner)})"

    if isinstance(e, Period):
        return f"period({e.period}, {_fmt_inline(e.inner)})"

    if isinstance(e, DateRange):
        bits = []
        if e.since: bits.append(f"since={e.since}")
        if e.until: bits.append(f"until={e.until}")
        return f"range({', '.join(bits)}, {_fmt_inline(e.inner)})"

    if isinstance(e, (Count, CountDistinct, Sum, Median)):
        name = type(e).__name__.lower()
        parts = []
        if isinstance(e, (CountDistinct, Sum, Median)):
            f = e.field
            if isinstance(f, RowExpr):
                parts.append(_fmt_row_expr(f))
            elif callable(f):
                parts.append(getattr(f, "__name__", repr(f)))
            else:
                parts.append(str(f))
        if e.where is not None:
            parts.append(_fmt_filter(e.where))  # filter as positional, no where= prefix
        bf = getattr(e, "bucket_field", "closed_date")
        if bf != "closed_date":
            parts.append(f"@{bf}")  # @-prefix for the bucket field, distinguishes it
        return f"{name}({', '.join(parts)})"

    return repr(e)


def format_expr(e, indent: int = 0) -> str:
    """Render an expression tree. Tries one line first; multi-line if too wide."""
    pad = "  " * indent
    inline = _fmt_inline(e)
    if len(inline) + len(pad) <= _LINE_BUDGET and "\n" not in inline:
        return f"{pad}{inline}"

    # Multi-line — prefer ratio sugar
    ratio = _is_ratio(e)
    if ratio is not None:
        num, den = ratio
        return (f"{pad}ratio(\n"
                f"{format_expr(num, indent + 1)},\n"
                f"{format_expr(den, indent + 1)},\n"
                f"{pad})")

    if isinstance(e, BinOp):
        return (f"{pad}(\n"
                f"{format_expr(e.left, indent + 1)}\n"
                f"{pad}  {e.op}\n"
                f"{format_expr(e.right, indent + 1)}\n"
                f"{pad})")

    if isinstance(e, FromSource):
        src = getattr(e.source, "__name__", repr(e.source))
        if src.endswith("_source"):
            src = src[:-len("_source")]
        return (f"{pad}@{src}(\n"
                f"{format_expr(e.inner, indent + 1)}\n"
                f"{pad})")

    if isinstance(e, Group):
        return (f"{pad}group({e.field},\n"
                f"{format_expr(e.inner, indent + 1)},\n"
                f"{pad})")

    if isinstance(e, Mean):
        return (f"{pad}mean({e.field},\n"
                f"{format_expr(e.inner, indent + 1)},\n"
                f"{pad})")

    if isinstance(e, Split):
        return (f"{pad}split({e.kind}:{_fmt_val(e.slug)},\n"
                f"{format_expr(e.inner, indent + 1)},\n"
                f"{pad})")

    if isinstance(e, Period):
        return (f"{pad}period({e.period},\n"
                f"{format_expr(e.inner, indent + 1)},\n"
                f"{pad})")

    if isinstance(e, DateRange):
        bits = []
        if e.since: bits.append(f"since={e.since}")
        if e.until: bits.append(f"until={e.until}")
        return (f"{pad}range({', '.join(bits)},\n"
                f"{format_expr(e.inner, indent + 1)},\n"
                f"{pad})")

    return f"{pad}{inline}"

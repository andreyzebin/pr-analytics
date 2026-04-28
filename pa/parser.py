"""
Tiny recursive-descent parser for the metric DSL.

Grammar (mirrors what `pa.dsl.format_expr` produces):

  expr        := term (('+' | '-') term)*
  term        := factor (('*' | '/') factor)*
  factor      := NUMBER
               | '(' expr ')'
               | '@' IDENT '(' expr ')'                 # source: @analysis(...)
               | IDENT '(' arglist? ')'                 # call: count(…), ratio(…)
  arglist     := arg (',' arg)*
  arg         := '@' IDENT                              # @bucket_field override
               | filter
               | expr

  filter      := disjunction
  disjunction := conjunction ('or' conjunction)*
  conjunction := negation ('and' negation)*
  negation    := 'not' negation | comparison
  comparison  := '(' filter ')'
               | value 'in' IDENT                       # Contains: value ∈ row.field
               | IDENT '=' value                        # Eq
               | IDENT 'in' '[' value (',' value)* ']'  # In: row.field ∈ literal_list
               | IDENT 'is' 'not'? 'null'

  value       := STRING | NUMBER | '$' IDENT | 'null' | 'true' | 'false'

Function signatures are positional and known to the parser:
  count(filter?, @bucket?)
  count_distinct(field, filter?, @bucket?)
  sum(field_expr, filter?, @bucket?)
  median(field_expr, filter?, @bucket?)
  ratio(num, den)                                       # sugar for /…*100

Source aliases: @pr (no-op for now), @comments, @analysis, @merge.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pa.dsl import (
    And, BinOp, Const, Contains, Count, CountDistinct, DateRange, Eq, Expr,
    Filter, FromSource, Group, In, IsNotNull, Median, Not, Or, Period, Ratio,
    RowBinOp, RowConst, RowExpr, RowField, Split, Sum, Var,
)
from pa.sources import analysis_source, comments_source, merge_source, pr_source


SOURCE_FNS = {
    "pr": pr_source,
    "comments": comments_source,
    "analysis": analysis_source,
    "merge": merge_source,
}

# ── Tokenizer ─────────────────────────────────────────────────────────────────

@dataclass
class Tok:
    kind: str   # IDENT, NUMBER, STRING, VAR, OP, KW, EOF
    value: Any
    pos: int


_KEYWORDS = {"in", "is", "not", "null", "and", "or", "true", "false"}
_TOKEN_RE = re.compile(r"""
    \s+                                  |  # whitespace
    (?P<NUMBER>\d+\.\d+|\d+)             |  # number
    '(?P<STRSQ>[^']*)'                   |  # 'string'
    "(?P<STRDQ>[^"]*)"                   |  # "string"
    \$(?P<VAR>[A-Za-z_][A-Za-z0-9_]*)    |  # $variable
    (?P<IDENT>[A-Za-z_][A-Za-z0-9_]*)    |  # identifier or keyword
    (?P<OP>!=|==|=|\+|-|\*|/|\(|\)|\[|\]|,|@|:)  # operators / punctuation
""", re.VERBOSE)


def tokenize(s: str) -> list[Tok]:
    out: list[Tok] = []
    i = 0
    while i < len(s):
        m = _TOKEN_RE.match(s, i)
        if m is None:
            raise SyntaxError(f"unexpected char at {i}: {s[i:i+20]!r}")
        if m.group("NUMBER"):
            v = m.group("NUMBER")
            out.append(Tok("NUMBER", float(v) if "." in v else int(v), i))
        elif m.group("STRSQ") is not None:
            out.append(Tok("STRING", m.group("STRSQ"), i))
        elif m.group("STRDQ") is not None:
            out.append(Tok("STRING", m.group("STRDQ"), i))
        elif m.group("VAR"):
            out.append(Tok("VAR", m.group("VAR"), i))
        elif m.group("IDENT"):
            ident = m.group("IDENT")
            kind = "KW" if ident in _KEYWORDS else "IDENT"
            out.append(Tok(kind, ident, i))
        elif m.group("OP"):
            out.append(Tok("OP", m.group("OP"), i))
        i = m.end()
    out.append(Tok("EOF", None, len(s)))
    return out


# ── Parser ────────────────────────────────────────────────────────────────────

class Parser:
    def __init__(self, tokens: list[Tok], src: str):
        self.toks = tokens
        self.src = src
        self.i = 0

    def peek(self, ahead: int = 0) -> Tok:
        return self.toks[self.i + ahead]

    def eat(self) -> Tok:
        t = self.toks[self.i]
        self.i += 1
        return t

    def accept(self, kind: str, value: Any = None) -> Tok | None:
        t = self.peek()
        if t.kind == kind and (value is None or t.value == value):
            return self.eat()
        return None

    def expect(self, kind: str, value: Any = None) -> Tok:
        t = self.accept(kind, value)
        if t is None:
            cur = self.peek()
            v_repr = repr(value) if value is not None else ""
            raise SyntaxError(
                f"expected {kind} {v_repr} at pos {cur.pos}, "
                f"got {cur.kind} {cur.value!r}"
            )
        return t

    # ── expressions ──────────────────────────────────────────────────────

    def expr(self) -> Expr:
        node = self.term()
        while True:
            op = self.accept("OP", "+") or self.accept("OP", "-")
            if not op:
                break
            node = BinOp(op.value, node, self.term())
        return node

    def term(self) -> Expr:
        node = self.factor()
        while True:
            op = self.accept("OP", "*") or self.accept("OP", "/")
            if not op:
                break
            node = BinOp(op.value, node, self.factor())
        return node

    def factor(self) -> Expr:
        t = self.peek()
        if t.kind == "NUMBER":
            self.eat()
            return Const(float(t.value))
        if t.kind == "OP" and t.value == "(":
            self.eat()
            inner = self.expr()
            self.expect("OP", ")")
            return inner
        if t.kind == "OP" and t.value == "@":
            self.eat()
            name = self.expect("IDENT").value
            self.expect("OP", "(")
            inner = self.expr()
            self.expect("OP", ")")
            if name not in SOURCE_FNS:
                raise SyntaxError(f"unknown source @{name} at pos {t.pos}; "
                                  f"available: {sorted(SOURCE_FNS)}")
            return FromSource(SOURCE_FNS[name], inner)
        if t.kind == "IDENT":
            return self.call()
        raise SyntaxError(f"unexpected token at pos {t.pos}: {t.kind} {t.value!r}")

    # ── calls: count/count_distinct/sum/median/ratio ──────────────────────

    def call(self) -> Expr:
        name = self.expect("IDENT").value
        self.expect("OP", "(")
        if name == "count":
            return self._call_count()
        if name == "count_distinct" or name == "countdistinct":
            return self._call_count_distinct()
        if name == "sum":
            return self._call_field_aggr(Sum)
        if name == "median":
            return self._call_field_aggr(Median)
        if name == "ratio":
            return self._call_ratio()
        if name == "period":
            return self._call_period()
        if name == "group":
            return self._call_group()
        if name == "split":
            return self._call_split()
        if name == "range":
            return self._call_range()
        raise SyntaxError(f"unknown function {name!r}")

    def _call_period(self) -> Expr:
        # period(week|month, expr)
        period = self.expect("IDENT").value
        if period not in ("week", "month"):
            raise SyntaxError(f"period must be 'week' or 'month', got {period!r}")
        self.expect("OP", ",")
        inner = self.expr()
        self.accept("OP", ",")  # trailing comma
        self._eat_close()
        return Period(period, inner)

    def _call_group(self) -> Expr:
        # group(field, expr)
        field = self.expect("IDENT").value
        self.expect("OP", ",")
        inner = self.expr()
        self.accept("OP", ",")
        self._eat_close()
        return Group(field, inner)

    def _call_split(self) -> Expr:
        # split(kind:value, predicate, expr)  or  split(kind:value, expr)
        # When predicate is omitted we synthesize Contains(value, kind+'s')
        # which mirrors auto_wrap's Split construction.
        kind = self.expect("IDENT").value
        self.expect("OP", ":")
        slug = self._value()
        self.expect("OP", ",")
        # Lookahead: was a predicate filter passed? (rare in user input)
        # In practice only the auto_wrap form is round-tripped, so synthesize
        # the standard predicate by `kind`.
        inner = self.expr()
        self.accept("OP", ",")
        self._eat_close()
        if kind == "reviewer":
            pred = Contains(slug, "reviewers")
        elif kind == "commenter":
            pred = Contains(slug, "commenters")
        else:
            raise SyntaxError(f"split kind must be 'reviewer' or 'commenter', got {kind!r}")
        return Split(kind, slug, pred, inner)

    def _call_range(self) -> Expr:
        # range(since=DATE, until=DATE, expr)  — both date args optional
        since = until = None
        while True:
            t = self.peek()
            if t.kind == "IDENT" and t.value in ("since", "until"):
                key = self.eat().value
                self.expect("OP", "=")
                # date: parse a STRING or a sequence "YYYY-MM-DD" of NUMBER-OP-NUMBER-OP-NUMBER
                val = self._date_value()
                if key == "since":
                    since = val
                else:
                    until = val
                self.expect("OP", ",")
                continue
            break
        inner = self.expr()
        self.accept("OP", ",")
        self._eat_close()
        return DateRange(since, until, inner)

    def _date_value(self) -> str:
        """Parse a YYYY-MM-DD date — either as a string literal or as bare digits."""
        t = self.peek()
        if t.kind == "STRING":
            return self.eat().value
        # bare YYYY-MM-DD: NUMBER '-' NUMBER '-' NUMBER
        y = self.expect("NUMBER").value
        self.expect("OP", "-")
        m = self.expect("NUMBER").value
        self.expect("OP", "-")
        d = self.expect("NUMBER").value
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"

    def _eat_close(self) -> None:
        self.expect("OP", ")")

    def _try_bucket_override(self) -> str | None:
        """`@field` arg → bucket_field override."""
        if (self.peek().kind == "OP" and self.peek().value == "@"
                and self.peek(1).kind == "IDENT"):
            self.eat()  # @
            return self.expect("IDENT").value
        return None

    def _call_count(self) -> Expr:
        # count() | count(filter) | count(@bucket) | count(filter, @bucket)
        if self.accept("OP", ")"):
            return Count()
        bucket = self._try_bucket_override()
        if bucket is not None:
            self._eat_close()
            return Count(bucket_field=bucket)
        flt = self.filter()
        bucket = None
        if self.accept("OP", ","):
            bucket = self._try_bucket_override()
            if bucket is None:
                raise SyntaxError("expected @bucket after second comma in count()")
        self._eat_close()
        return Count(where=flt, bucket_field=bucket or "closed_date")

    def _call_count_distinct(self) -> Expr:
        # count_distinct(field, filter?, @bucket?)
        field = self.expect("IDENT").value
        flt: Filter | None = None
        bucket = "closed_date"
        while self.accept("OP", ","):
            b = self._try_bucket_override()
            if b is not None:
                bucket = b
                break
            flt = self.filter()
        self._eat_close()
        return CountDistinct(field, where=flt, bucket_field=bucket)

    def _call_field_aggr(self, cls):
        # sum(field_expr, filter?, @bucket?)
        # field_expr is a full row-level expression: bare field, parens, arithmetic.
        # If it's a single bare IDENT, return as str for legacy compatibility;
        # otherwise return a RowExpr tree.
        field = self._row_expr()
        if isinstance(field, RowField):
            field = field.name  # bare ident → str
        flt: Filter | None = None
        bucket = "closed_date"
        while self.accept("OP", ","):
            b = self._try_bucket_override()
            if b is not None:
                bucket = b
                break
            flt = self.filter()
        self._eat_close()
        return cls(field, where=flt, bucket_field=bucket)

    def _row_expr(self) -> RowExpr:
        node = self._row_term()
        while True:
            op = self.accept("OP", "+") or self.accept("OP", "-")
            if not op:
                break
            node = RowBinOp(op.value, node, self._row_term())
        return node

    def _row_term(self) -> RowExpr:
        node = self._row_factor()
        while True:
            op = self.accept("OP", "*") or self.accept("OP", "/")
            if not op:
                break
            node = RowBinOp(op.value, node, self._row_factor())
        return node

    def _row_factor(self) -> RowExpr:
        t = self.peek()
        if t.kind == "OP" and t.value == "(":
            self.eat()
            inner = self._row_expr()
            self.expect("OP", ")")
            return inner
        if t.kind == "NUMBER":
            self.eat()
            return RowConst(t.value)
        if t.kind == "IDENT":
            self.eat()
            return RowField(t.value)
        raise SyntaxError(f"unexpected token in row expression at pos {t.pos}: {t.kind}")

    def _call_ratio(self) -> Expr:
        num = self.expr()
        self.expect("OP", ",")
        den = self.expr()
        # Allow trailing comma
        self.accept("OP", ",")
        self._eat_close()
        return Ratio(num, den)

    # ── filters ──────────────────────────────────────────────────────────

    def filter(self) -> Filter:
        return self._disjunction()

    def _disjunction(self) -> Filter:
        node = self._conjunction()
        if self.accept("KW", "or"):
            parts = [node, self._conjunction()]
            while self.accept("KW", "or"):
                parts.append(self._conjunction())
            return Or(tuple(parts))
        return node

    def _conjunction(self) -> Filter:
        node = self._negation()
        if self.accept("KW", "and"):
            parts = [node, self._negation()]
            while self.accept("KW", "and"):
                parts.append(self._negation())
            return And(tuple(parts))
        return node

    def _negation(self) -> Filter:
        if self.accept("KW", "not"):
            return Not(self._negation())
        return self._comparison()

    def _comparison(self) -> Filter:
        if self.accept("OP", "("):
            inner = self.filter()
            self.expect("OP", ")")
            return inner

        # Lookahead: 'value in IDENT' is Contains (value-first form).
        # Recognized when LHS is a literal/var/null/bool, not a bare IDENT.
        t = self.peek()
        if t.kind in ("STRING", "NUMBER", "VAR") or (
                t.kind == "KW" and t.value in ("null", "true", "false")):
            val = self._value()
            self.expect("KW", "in")
            field = self.expect("IDENT").value
            return Contains(val, field)

        ident = self.expect("IDENT").value
        # 'is' [ 'not' ] 'null'
        if self.accept("KW", "is"):
            negate = bool(self.accept("KW", "not"))
            self.expect("KW", "null")
            return IsNotNull(ident) if negate else Not(IsNotNull(ident))
        # 'in' '[' value (',' value)* ']'
        if self.accept("KW", "in"):
            self.expect("OP", "[")
            vals = [self._value()]
            while self.accept("OP", ","):
                vals.append(self._value())
            self.expect("OP", "]")
            return In(ident, vals)
        # ident '=' value
        self.expect("OP", "=")
        val = self._value()
        return Eq(ident, val)

    def _value(self):
        t = self.peek()
        if t.kind == "STRING":
            return self.eat().value
        if t.kind == "NUMBER":
            return self.eat().value
        if t.kind == "VAR":
            return Var(self.eat().value)
        if t.kind == "KW" and t.value == "null":
            self.eat()
            return None
        if t.kind == "KW" and t.value in ("true", "false"):
            return self.eat().value == "true"
        raise SyntaxError(f"expected value at pos {t.pos}, got {t.kind} {t.value!r}")


def parse_expr(s: str) -> Expr:
    """Parse a DSL string into an AST (raises SyntaxError on bad input)."""
    p = Parser(tokenize(s), s)
    out = p.expr()
    p.expect("EOF")
    return out

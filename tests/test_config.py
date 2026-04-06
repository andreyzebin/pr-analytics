"""Tests for config loading and resolution."""
import os
import textwrap
from pathlib import Path

import pytest

import pr_analytics as pa


# ── _deep_merge ───────────────────────────────────────────────────────────────

def test_deep_merge_flat():
    assert pa._deep_merge({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_deep_merge_override():
    assert pa._deep_merge({"a": 1}, {"a": 2}) == {"a": 2}


def test_deep_merge_nested():
    base = {"bitbucket": {"url": "https://base.example.com", "token": "old"}}
    override = {"bitbucket": {"token": "new"}}
    result = pa._deep_merge(base, override)
    assert result == {"bitbucket": {"url": "https://base.example.com", "token": "new"}}


def test_deep_merge_does_not_mutate():
    base = {"a": {"x": 1}}
    override = {"a": {"y": 2}}
    pa._deep_merge(base, override)
    assert base == {"a": {"x": 1}}


# ── _expand_env ───────────────────────────────────────────────────────────────

def test_expand_env_known_var(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "secret123")
    result = pa._expand_env({"token": "${MY_TOKEN}"})
    assert result == {"token": "secret123"}


def test_expand_env_unknown_var(monkeypatch):
    monkeypatch.delenv("NONEXISTENT_VAR_XYZ", raising=False)
    result = pa._expand_env("${NONEXISTENT_VAR_XYZ}")
    assert result == ""


def test_expand_env_nested(monkeypatch):
    monkeypatch.setenv("CA_PATH", "/etc/ca.pem")
    cfg = {"bitbucket": {"ca_bundle": "${CA_PATH}"}}
    result = pa._expand_env(cfg)
    assert result["bitbucket"]["ca_bundle"] == "/etc/ca.pem"


def test_expand_env_non_string_passthrough():
    assert pa._expand_env(42) == 42
    assert pa._expand_env(True) is True
    assert pa._expand_env(None) is None


# ── load_config ───────────────────────────────────────────────────────────────

def test_load_config_reads_base(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(textwrap.dedent("""\
        bitbucket:
          url: "https://bb.example.com"
        cache:
          concurrency: 6
    """))
    cfg = pa.load_config()
    assert cfg["bitbucket"]["url"] == "https://bb.example.com"
    assert cfg["cache"]["concurrency"] == 6


def test_load_config_local_overrides(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(textwrap.dedent("""\
        bitbucket:
          url: "https://bb.example.com"
          token: "base-token"
        cache:
          concurrency: 4
    """))
    (tmp_path / "config.local.yaml").write_text(textwrap.dedent("""\
        bitbucket:
          token: "local-token"
        cache:
          concurrency: 8
    """))
    cfg = pa.load_config()
    assert cfg["bitbucket"]["url"] == "https://bb.example.com"   # from base
    assert cfg["bitbucket"]["token"] == "local-token"             # overridden
    assert cfg["cache"]["concurrency"] == 8


def test_load_config_empty_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = pa.load_config()
    assert cfg == {}


# ── resolve_* ─────────────────────────────────────────────────────────────────

def test_resolve_token_env_priority(monkeypatch):
    monkeypatch.setenv("BB_TOKEN", "env-token")
    cfg = {"bitbucket": {"token": "config-token"}}
    assert pa.resolve_token("cli-token", cfg) == "env-token"


def test_resolve_token_cli_over_config(monkeypatch):
    monkeypatch.delenv("BB_TOKEN", raising=False)
    monkeypatch.delenv("BITBUCKET_SERVER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BITBUCKET_SERVER__BEARER_TOKEN", raising=False)
    cfg = {"bitbucket": {"token": "config-token"}}
    assert pa.resolve_token("cli-token", cfg) == "cli-token"


def test_resolve_token_from_config(monkeypatch):
    monkeypatch.delenv("BB_TOKEN", raising=False)
    monkeypatch.delenv("BITBUCKET_SERVER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BITBUCKET_SERVER__BEARER_TOKEN", raising=False)
    cfg = {"bitbucket": {"token": "config-token"}}
    assert pa.resolve_token(None, cfg) == "config-token"


def test_resolve_token_empty_string_treated_as_none(monkeypatch):
    monkeypatch.setenv("BB_TOKEN", "")
    monkeypatch.delenv("BITBUCKET_SERVER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BITBUCKET_SERVER__BEARER_TOKEN", raising=False)
    cfg = {"bitbucket": {"token": "config-token"}}
    assert pa.resolve_token(None, cfg) == "config-token"


def test_resolve_db_default(monkeypatch):
    monkeypatch.delenv("BB_DB", raising=False)
    assert pa.resolve_db(None, {}) == pa.DEFAULT_DB


def test_resolve_db_from_config(monkeypatch):
    monkeypatch.delenv("BB_DB", raising=False)
    cfg = {"cache": {"db": "/tmp/custom.db"}}
    assert pa.resolve_db(None, cfg) == "/tmp/custom.db"


def test_resolve_ca_bundle_env(monkeypatch):
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/etc/ssl/ca.pem")
    assert pa.resolve_ca_bundle({}) == "/etc/ssl/ca.pem"


def test_resolve_ca_bundle_from_config(monkeypatch):
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    cfg = {"bitbucket": {"ca_bundle": "/opt/certs/ca.pem"}}
    assert pa.resolve_ca_bundle(cfg) == "/opt/certs/ca.pem"


def test_resolve_ca_bundle_none_when_missing(monkeypatch):
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    assert pa.resolve_ca_bundle({}) is None

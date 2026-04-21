from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional

OUTPUT_DIR = "output"
DEFAULT_DB = f"{OUTPUT_DIR}/bitbucket_cache.db"
CONFIG_YAML = "config.yaml"
CONFIG_LOCAL_YAML = "config.local.yaml"
DEFAULT_CONCURRENCY = 4
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
DEFAULT_JUDGE_MODEL = "claude-opus-4-6"


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _expand_env(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    if isinstance(obj, str):
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
    return obj


def load_config() -> dict:
    import yaml

    cfg: dict = {}
    base = Path(CONFIG_YAML)
    if base.exists():
        with open(base) as f:
            cfg = yaml.safe_load(f) or {}

    local = Path(CONFIG_LOCAL_YAML)
    if local.exists():
        with open(local) as f:
            cfg = _deep_merge(cfg, yaml.safe_load(f) or {})

    return _expand_env(cfg)


def _bb(cfg: dict) -> dict:
    return cfg.get("bitbucket", {})


def _cache_cfg(cfg: dict) -> dict:
    return cfg.get("cache", {})


def resolve_token(args_token: Optional[str], cfg: dict) -> Optional[str]:
    return (
        os.environ.get("BB_TOKEN")
        or os.environ.get("BITBUCKET_SERVER_BEARER_TOKEN")
        or os.environ.get("BITBUCKET_SERVER__BEARER_TOKEN")
        or args_token
        or _bb(cfg).get("token")
    ) or None


def resolve_url(args_url: Optional[str], cfg: dict) -> Optional[str]:
    return (
        os.environ.get("BB_URL")
        or args_url
        or _bb(cfg).get("url")
    ) or None


def resolve_db(args_db: Optional[str], cfg: dict) -> str:
    return (
        os.environ.get("BB_DB")
        or args_db
        or _cache_cfg(cfg).get("db")
        or DEFAULT_DB
    )


def resolve_ca_bundle(cfg: dict) -> Optional[str]:
    return (
        os.environ.get("REQUESTS_CA_BUNDLE")
        or _bb(cfg).get("ca_bundle")
    ) or None


def resolve_client_cert(cfg: dict) -> Optional[str]:
    return (
        os.environ.get("BITBUCKET_SERVER_CLIENT_CERT")
        or os.environ.get("BITBUCKET_SERVER__CLIENT_CERT")
        or _bb(cfg).get("client_cert")
    ) or None


def _judge_cfg(cfg: dict) -> dict:
    return cfg.get("judge", {})


def resolve_judge_model(args_model: Optional[str], cfg: dict) -> str:
    return (
        args_model
        or _judge_cfg(cfg).get("model")
        or DEFAULT_JUDGE_MODEL
    )


def resolve_judge_api_key(cfg: dict) -> Optional[str]:
    return (
        os.environ.get("ANTHROPIC_API_KEY")
        or _judge_cfg(cfg).get("api_key")
    ) or None


def resolve_judge_base_url(cfg: dict) -> Optional[str]:
    return _judge_cfg(cfg).get("base_url") or None


def resolve_judge_tool_choice(cfg: dict) -> Optional[str]:
    return _judge_cfg(cfg).get("tool_choice") or None

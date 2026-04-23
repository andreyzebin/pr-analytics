"""
LLM judge client for analyze-feedback and other LLM-based commands.

Supports:
  - Anthropic Claude (default, via ANTHROPIC_API_KEY)
  - Any OpenAI-compatible endpoint (set base_url in config)
  - tool_choice: "auto" — uses function calling for structured JSON output
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

log = logging.getLogger(__name__)

# Suppress noisy HTTP request logs from openai/httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)


@dataclass
class JudgeVerdict:
    verdict: str     # "yes" | "no" | "unclear"
    confidence: str  # "high" | "medium" | "low"
    reasoning: str
    tokens_used: int = 0  # total tokens consumed by this call


# ── Tool schemas for function calling ─────────────────────────────────────

_VERDICT_TOOL = {
    "type": "function",
    "function": {
        "name": "submit_verdict",
        "description": "Submit the judge verdict for a code review comment",
        "parameters": {
            "type": "object",
            "properties": {
                "verdict": {"type": "string", "enum": ["yes", "no", "unclear"]},
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                "reasoning": {"type": "string", "description": "One-sentence explanation"},
            },
            "required": ["verdict", "confidence", "reasoning"],
        },
    },
}

_GENERIC_JSON_TOOL = {
    "type": "function",
    "function": {
        "name": "submit_result",
        "description": "Submit the structured analysis result",
        "parameters": {
            "type": "object",
            "properties": {},  # Accept any JSON
            "additionalProperties": True,
        },
    },
}


class LLMJudge:
    def __init__(self, model: str, api_key: str, base_url: str | None = None,
                 tool_choice: str | None = None,
                 extra_body: dict | None = None,
                 max_tokens: int | None = None,
                 no_temperature: bool = False):
        self._model = model
        self._api_key = api_key
        self._base_url = base_url    # None → Anthropic; str → OpenAI-compatible
        self._tool_choice = tool_choice  # "auto" → use function calling
        self._extra_body = extra_body  # e.g. {"chat_template_kwargs": {"enable_thinking": False}}
        self._cfg_max_tokens = max_tokens
        self._no_temperature = no_temperature

    def judge(self, prompt: str) -> JudgeVerdict:
        verdict, _ = self.judge_raw(prompt)
        return verdict

    def judge_raw(self, prompt: str) -> tuple[JudgeVerdict, str]:
        """Same as judge(), but also returns the raw LLM response text
        (or JSON-dumped tool-call args) for debugging/verbose output.
        On parse error, the raw text is attached to the exception as .raw."""
        if self._tool_choice == "auto" and self._base_url:
            data, tokens = self._call_with_tool(prompt, _VERDICT_TOOL)
            verdict = self._normalize_verdict(data)
            verdict.tokens_used = tokens
            return verdict, json.dumps(data, ensure_ascii=False)
        raw, tokens = self._call(prompt)
        try:
            verdict = self._parse(raw)
        except Exception as exc:
            exc.raw = raw  # type: ignore[attr-defined]
            exc.tokens_used = tokens  # type: ignore[attr-defined]
            raise
        verdict.tokens_used = tokens
        return verdict, raw

    def call_json(self, prompt: str) -> tuple[dict, int]:
        """Generic call: returns (parsed_dict, tokens_used)."""
        if self._tool_choice == "auto" and self._base_url:
            return self._call_with_tool(prompt, _GENERIC_JSON_TOOL)
        raw, tokens = self._call(prompt)
        return self._parse_json(raw), tokens

    # ── Plain text call ───────────────────────────────────────────────────

    def _call(self, prompt: str) -> tuple[str, int]:
        """Returns (response_text, total_tokens_used)."""
        model_lower = self._model.lower()
        # Heuristic: known reasoning models that can't take temperature and
        # need headroom for think tokens. Conservative — only clear cases.
        # For Qwen3 with thinking enabled set judge.max_tokens + no_temperature
        # in config, or disable thinking via judge.extra_body.chat_template_kwargs.
        is_reasoner = "reasoner" in model_lower or "-r1" in model_lower

        max_tokens = self._cfg_max_tokens or (4096 if is_reasoner else 1024)
        skip_temp = self._no_temperature or is_reasoner

        if self._base_url:
            from openai import OpenAI
            client = OpenAI(api_key=self._api_key, base_url=self._base_url)
            kwargs: dict = dict(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
            )
            if not skip_temp:
                kwargs["temperature"] = 0
            if self._extra_body:
                kwargs["extra_body"] = self._extra_body
            resp = client.chat.completions.create(**kwargs)
            tokens = resp.usage.total_tokens if resp.usage else 0
            return resp.choices[0].message.content or "", tokens
        else:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)
            msg = client.messages.create(
                model=self._model,
                max_tokens=max_tokens,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )
            tokens = (msg.usage.input_tokens + msg.usage.output_tokens) if msg.usage else 0
            return msg.content[0].text, tokens

    # ── Function calling ──────────────────────────────────────────────────

    def _call_with_tool(self, prompt: str, tool: dict) -> tuple[dict, int]:
        """Call LLM with function calling, return (parsed_args_dict, tokens)."""
        from openai import OpenAI
        client = OpenAI(api_key=self._api_key, base_url=self._base_url)
        model_lower = self._model.lower()
        is_reasoner = "reasoner" in model_lower or "-r1" in model_lower
        max_tokens = self._cfg_max_tokens or (4096 if is_reasoner else 1024)
        skip_temp = self._no_temperature or is_reasoner
        kwargs: dict = dict(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            tools=[tool],
            tool_choice="auto",
            max_tokens=max_tokens,
        )
        if not skip_temp:
            kwargs["temperature"] = 0
        if self._extra_body:
            kwargs["extra_body"] = self._extra_body
        resp = client.chat.completions.create(**kwargs)
        tokens = resp.usage.total_tokens if resp.usage else 0
        msg = resp.choices[0].message

        # Model used a tool call → parse arguments
        if msg.tool_calls:
            raw_args = msg.tool_calls[0].function.arguments
            return json.loads(raw_args), tokens

        # Fallback: model responded with plain text (some models ignore tools)
        content = msg.content or ""
        return self._parse_json(content), tokens

    # ── Parsing ───────────────────────────────────────────────────────────

    @staticmethod
    def _parse_json(raw: str) -> dict:
        """Parse JSON from LLM response, stripping markdown fences and
        Qwen/DeepSeek-style <think>...</think> reasoning blocks."""
        text = raw

        # Strip <think>...</think> blocks (Qwen3, DeepSeek-R1 in thinking mode).
        # Handles both closed and unclosed (truncated) think blocks.
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
        # If a <think> was opened but not closed (response truncated), drop everything up to it.
        if "<think>" in text:
            text = text.split("<think>", 1)[0]

        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:])
            if text.endswith("```"):
                text = text[:-3]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                return json.loads(m.group())
            raise ValueError(f"Cannot parse judge response: {raw[:300]}")

    @staticmethod
    def _normalize_verdict(data: dict) -> JudgeVerdict:
        verdict = str(data.get("verdict", "unclear")).lower()
        if verdict not in ("yes", "no", "unclear"):
            verdict = "unclear"
        confidence = str(data.get("confidence", "low")).lower()
        if confidence not in ("high", "medium", "low"):
            confidence = "low"
        reasoning = str(data.get("reasoning", ""))
        return JudgeVerdict(verdict=verdict, confidence=confidence, reasoning=reasoning)

    @staticmethod
    def _parse(raw: str) -> JudgeVerdict:
        data = LLMJudge._parse_json(raw)
        return LLMJudge._normalize_verdict(data)


def build_judge(model: str, api_key: str, base_url: str | None, cfg: dict) -> "LLMJudge":
    """Build an LLMJudge honoring all judge.* config options."""
    from pa.config import (
        resolve_judge_tool_choice,
        resolve_judge_extra_body,
        resolve_judge_max_tokens,
        resolve_judge_no_temperature,
    )
    return LLMJudge(
        model=model,
        api_key=api_key,
        base_url=base_url,
        tool_choice=resolve_judge_tool_choice(cfg),
        extra_body=resolve_judge_extra_body(cfg),
        max_tokens=resolve_judge_max_tokens(cfg),
        no_temperature=resolve_judge_no_temperature(cfg),
    )

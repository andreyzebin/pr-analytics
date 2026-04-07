"""
LLM judge client for analyze-feedback command.

Supports:
  - Anthropic Claude (default, via ANTHROPIC_API_KEY)
  - Any OpenAI-compatible endpoint (set base_url in config)
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class JudgeVerdict:
    verdict: str     # "yes" | "no" | "unclear"
    confidence: str  # "high" | "medium" | "low"
    reasoning: str
    tokens_used: int = 0  # total tokens consumed by this call


class LLMJudge:
    def __init__(self, model: str, api_key: str, base_url: str | None = None):
        self._model = model
        self._api_key = api_key
        self._base_url = base_url  # None → Anthropic; str → OpenAI-compatible

    def judge(self, prompt: str) -> JudgeVerdict:
        raw, tokens = self._call(prompt)
        verdict = self._parse(raw)
        verdict.tokens_used = tokens
        return verdict

    def _call(self, prompt: str) -> tuple[str, int]:
        """Returns (response_text, total_tokens_used)."""
        if self._base_url:
            from openai import OpenAI
            client = OpenAI(api_key=self._api_key, base_url=self._base_url)
            is_reasoner = "reasoner" in self._model or "-r1" in self._model.lower()
            kwargs: dict = dict(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1024,
            )
            if not is_reasoner:
                kwargs["temperature"] = 0
            resp = client.chat.completions.create(**kwargs)
            tokens = resp.usage.total_tokens if resp.usage else 0
            return resp.choices[0].message.content or "", tokens
        else:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)
            msg = client.messages.create(
                model=self._model,
                max_tokens=1024,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )
            tokens = (msg.usage.input_tokens + msg.usage.output_tokens) if msg.usage else 0
            return msg.content[0].text, tokens

    @staticmethod
    def _parse(raw: str) -> JudgeVerdict:
        text = raw.strip()
        # Strip markdown code blocks if present
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:])
            if text.endswith("```"):
                text = text[:-3]
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                data = json.loads(m.group())
            else:
                raise ValueError(f"Cannot parse judge response: {raw[:300]}")

        verdict = str(data.get("verdict", "unclear")).lower()
        if verdict not in ("yes", "no", "unclear"):
            verdict = "unclear"
        confidence = str(data.get("confidence", "low")).lower()
        if confidence not in ("high", "medium", "low"):
            confidence = "low"
        reasoning = str(data.get("reasoning", ""))
        return JudgeVerdict(verdict=verdict, confidence=confidence, reasoning=reasoning)

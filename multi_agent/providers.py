"""Pluggable "agent brain" behind the activities.

The demo runs fully offline by default with a deterministic MockProvider, so the
whole multi-agent pipeline is hermetic and needs no API key. Set
AGENT_PROVIDER=anthropic (and ANTHROPIC_API_KEY) to route the generative steps to
a real Claude model instead -- the orchestration code does not change.

Only ACTIVITIES import this module. Workflow code never calls a provider directly
(that would be non-deterministic I/O inside the workflow sandbox).
"""

import os

from .shared import CONFIDENCE_BAR, Critique


class AgentProvider:
    name = "base"

    async def complete(self, system: str, prompt: str) -> str:  # pragma: no cover
        raise NotImplementedError

    async def critique(self, angle: str, finding: str) -> Critique:  # pragma: no cover
        """Score a finding 0.0-1.0 and say what would improve it."""
        raise NotImplementedError


class MockProvider(AgentProvider):
    """Deterministic, offline stand-in for an LLM.

    Output length grows when reviewer feedback is present, which lets the
    self-critique loop demonstrate a real (deterministic) refinement step.
    """

    name = "mock"

    async def complete(self, system: str, prompt: str) -> str:
        base = (
            "Based on established engineering practice, the essentials are clear "
            "ownership, measurable reliability, and small incremental delivery."
        )
        # Use only the first line (the question), not any appended feedback.
        question = prompt.strip().splitlines()[0]
        focus = question.split(":", 1)[-1].strip().rstrip("?")[:110]
        out = f"{base} On {focus}, prioritize those three."
        if "feedback" in prompt.lower():
            out += (
                " Addressing the reviewer's note, this revision names concrete "
                "trade-offs and failure modes and recommends a sensible default "
                "with rationale, so the guidance is actionable rather than generic."
            )
        return out

    async def critique(self, angle: str, finding: str) -> Critique:
        """Deterministic stand-in for an LLM judge: score on length.

        Crude on purpose. A first-pass mock finding lands below the bar and a
        revised one lands above it, so the reflection loop is exercised
        identically on every run -- which is what makes the tests hermetic.
        """
        score = round(min(len(finding) / 300.0, 1.0), 3)
        if score >= CONFIDENCE_BAR:
            return Critique(score=score, feedback="Solid; meets the bar.")
        return Critique(
            score=score,
            feedback="Too generic -- add concrete trade-offs, failure modes, and a default.",
        )


class AnthropicProvider(AgentProvider):
    """Optional real-LLM path. Requires `pip install anthropic` and
    ANTHROPIC_API_KEY. Model id is configurable via AGENT_MODEL."""

    name = "anthropic"

    def __init__(self) -> None:
        import anthropic  # imported lazily so the demo runs without the dep

        # Async client: activities already run on the worker's event loop, so
        # there is no reason to burn a thread on a network-bound call.
        self._client = anthropic.AsyncAnthropic()  # reads ANTHROPIC_API_KEY
        # Configurable: set AGENT_MODEL to override.
        self._model = os.environ.get("AGENT_MODEL", "claude-opus-4-8")

    async def complete(self, system: str, prompt: str) -> str:
        msg = await self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        # content is a list of blocks; only text blocks carry prose.
        return "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")

    async def critique(self, angle: str, finding: str) -> Critique:
        """Real LLM-as-judge.

        The response is constrained to a JSON schema (structured outputs), so
        the score is guaranteed parseable rather than scraped out of prose --
        the difference between a judge you can build control flow on and one
        that occasionally returns "I'd rate this an 8/10!".
        """
        import json

        msg = await self._client.messages.create(
            model=self._model,
            max_tokens=512,
            system=(
                "You are a demanding reviewer. Score the finding on whether it is "
                "specific and actionable for an engineer: 1.0 is concrete with "
                "real trade-offs and failure modes, 0.0 is generic filler. Give "
                "one sentence of feedback naming the single biggest improvement."
            ),
            messages=[
                {
                    "role": "user",
                    "content": f"Topic angle: {angle}\n\nFinding:\n{finding}",
                }
            ],
            output_config={
                "format": {
                    "type": "json_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "score": {
                                "type": "number",
                                "description": "Quality from 0.0 (generic) to 1.0 (concrete).",
                            },
                            "feedback": {
                                "type": "string",
                                "description": "One sentence naming the biggest improvement.",
                            },
                        },
                        "required": ["score", "feedback"],
                        "additionalProperties": False,
                    },
                }
            },
        )
        text = next(b.text for b in msg.content if getattr(b, "type", "") == "text")
        data = json.loads(text)
        # Clamp: the schema can't express a numeric range, so enforce it here.
        score = round(max(0.0, min(float(data["score"]), 1.0)), 3)
        return Critique(score=score, feedback=data["feedback"])


_PROVIDER = None


def get_provider() -> AgentProvider:
    """Process-wide singleton chosen by the AGENT_PROVIDER env var (default: mock)."""
    global _PROVIDER
    if _PROVIDER is None:
        name = os.environ.get("AGENT_PROVIDER", "mock").lower()
        _PROVIDER = AnthropicProvider() if name == "anthropic" else MockProvider()
    return _PROVIDER

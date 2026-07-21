"""Pluggable "agent brain" behind the activities.

The demo runs fully offline by default with a deterministic MockProvider, so the
whole multi-agent pipeline is hermetic and needs no API key. Set
AGENT_PROVIDER=anthropic (and ANTHROPIC_API_KEY) to route the generative steps to
a real Claude model instead -- the orchestration code does not change.

Only ACTIVITIES import this module. Workflow code never calls a provider directly
(that would be non-deterministic I/O inside the workflow sandbox).
"""

import os


class AgentProvider:
    name = "base"

    async def complete(self, system: str, prompt: str) -> str:  # pragma: no cover
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


class AnthropicProvider(AgentProvider):
    """Optional real-LLM path. Requires `pip install anthropic` and
    ANTHROPIC_API_KEY. Model id is configurable via AGENT_MODEL."""

    name = "anthropic"

    def __init__(self) -> None:
        import anthropic  # imported lazily so the demo runs without the dep

        self._client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
        # Configurable: set AGENT_MODEL to your current model id.
        self._model = os.environ.get("AGENT_MODEL", "claude-sonnet-4-5")

    async def complete(self, system: str, prompt: str) -> str:
        import asyncio

        def _call() -> str:
            msg = self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            return "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")

        # Run the blocking SDK call off the event loop.
        return await asyncio.to_thread(_call)


_PROVIDER = None


def get_provider() -> AgentProvider:
    """Process-wide singleton chosen by the AGENT_PROVIDER env var (default: mock)."""
    global _PROVIDER
    if _PROVIDER is None:
        name = os.environ.get("AGENT_PROVIDER", "mock").lower()
        _PROVIDER = AnthropicProvider() if name == "anthropic" else MockProvider()
    return _PROVIDER

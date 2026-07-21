"""Activities = the side-effecting work the agents actually do.

Everything non-deterministic (LLM/tool/network calls, wall-clock work) lives
here, never in workflow code. Temporal retries activities independently and
records their results in the workflow history, which is what makes the whole
pipeline durable and replay-safe.
"""

import asyncio
from typing import List

from temporalio import activity

from .providers import get_provider
from .shared import (
    AgentResult,
    Critique,
    ResearchInput,
    ResearchRequest,
    ReviewInput,
    ReviewNotes,
    Subtask,
    SynthInput,
    SynthResult,
)

# Facets a topic can be decomposed into. In production this list would come from
# an LLM planning call returning structured output.
ANGLES = [
    "background and core definitions",
    "current state of the art",
    "key risks and failure modes",
    "operational best practices",
    "concrete recommendations",
    "cost and scaling considerations",
]

_CONFIDENCE_BAR = 0.70  # sections below this get one feedback-guided rewrite


@activity.defn
async def plan_subtasks(req: ResearchRequest) -> List[Subtask]:
    """Planner agent: decompose the topic into independent subtasks."""
    n = max(1, min(req.max_subtasks, len(ANGLES)))
    subs = [
        Subtask(
            id=i + 1,
            angle=ANGLES[i],
            question=(
                f"What should an engineer know about the {ANGLES[i]} "
                f"of: {req.topic}?"
            ),
        )
        for i in range(n)
    ]
    activity.logger.info("Planned %d subtasks for topic=%r", len(subs), req.topic)
    return subs


@activity.defn
async def research_subtask(inp: ResearchInput) -> AgentResult:
    """Research agent: produce a finding for one subtask.

    Emits heartbeats so a long-running or stuck task is observable and
    cancellable -- the Temporal way to keep long activities healthy.
    """
    st = inp.subtask
    for step in range(3):
        activity.heartbeat(f"{st.angle}: step {step + 1}/3 (attempt {inp.attempt})")
        await asyncio.sleep(0.02)

    system = "You are a meticulous research agent. Answer in 3-4 sentences."
    prompt = st.question
    if inp.feedback:
        prompt += f"\n\nReviewer feedback to address: {inp.feedback}"

    finding = await get_provider().complete(system, prompt)
    slug = st.angle.replace(" ", "-")
    return AgentResult(
        subtask_id=st.id,
        angle=st.angle,
        finding=finding,
        confidence=0.0,  # filled in by critique
        attempts=inp.attempt,
        sources=[f"source://{slug}/{inp.attempt}/a", f"source://{slug}/{inp.attempt}/b"],
    )


@activity.defn
async def critique_finding(result: AgentResult) -> Critique:
    """Critic agent (LLM-as-judge stand-in): score a finding and give feedback."""
    score = round(min(len(result.finding) / 300.0, 1.0), 3)
    if score >= _CONFIDENCE_BAR:
        return Critique(score=score, feedback="Solid; meets the bar.")
    return Critique(
        score=score,
        feedback="Too generic -- add concrete trade-offs, failure modes, and a default.",
    )


@activity.defn
async def review_report(inp: ReviewInput) -> ReviewNotes:
    """Orchestrator-level critic: assess the assembled report."""
    results = inp.results
    avg = round(sum(r.confidence for r in results) / max(len(results), 1), 3)
    weak = [r.subtask_id for r in results if r.confidence < _CONFIDENCE_BAR]
    overall = f"Reviewed {len(results)} sections; average confidence {avg}. " + (
        "All sections meet the bar." if not weak else f"Still-weak sections: {weak}."
    )
    return ReviewNotes(overall=overall, weak_subtask_ids=weak, avg_confidence=avg)


@activity.defn
async def synthesize_report(inp: SynthInput) -> SynthResult:
    """Synthesis agent: compose the final executive summary."""
    provider = get_provider()
    body = "\n".join(f"- {r.angle}: {r.finding}" for r in inp.results)
    system = "You are a synthesis agent. Produce a tight executive summary."
    prompt = (
        f"Topic: {inp.topic}\n"
        f"Reviewer notes: {inp.review.overall}\n"
        f"Findings:\n{body}\n\nWrite a 4-6 sentence executive summary."
    )
    summary = await provider.complete(system, prompt)
    return SynthResult(summary=summary, provider=provider.name)

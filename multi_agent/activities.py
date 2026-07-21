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
    CONFIDENCE_BAR,
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


async def _heartbeat_until_cancelled(interval: float, detail: str) -> None:
    """Heartbeat on a fixed cadence until cancelled.

    The model call below is a single long await. Without this, the activity
    would go silent for its whole duration and Temporal would (correctly) treat
    it as dead once heartbeat_timeout elapsed -- so a slow-but-healthy LLM call
    looked exactly like a hung worker.
    """
    while True:
        await asyncio.sleep(interval)
        activity.heartbeat(detail)


@activity.defn
async def research_subtask(inp: ResearchInput) -> AgentResult:
    """Research agent: produce a finding for one subtask.

    Emits heartbeats throughout -- including while waiting on the model -- so a
    long-running or stuck task stays observable and cancellable. That is the
    Temporal way to keep long activities healthy.
    """
    st = inp.subtask
    for step in range(3):
        activity.heartbeat(f"{st.angle}: step {step + 1}/3 (attempt {inp.attempt})")
        await asyncio.sleep(0.02)

    # Be explicit about form as well as length: current models calibrate output
    # length to how complex they judge the task to be, so a soft "3-4 sentences"
    # loses to a meaty topic and comes back as multi-paragraph markdown.
    system = (
        "You are a meticulous research agent. Reply with 3-4 sentences of plain "
        "prose and nothing else. Do not use markdown, headings, bullet points, "
        "bold, or a preamble such as 'Here is'. Start directly with the content."
    )
    prompt = st.question
    if inp.feedback:
        prompt += f"\n\nReviewer feedback to address: {inp.feedback}"

    beat = asyncio.create_task(
        _heartbeat_until_cancelled(2.0, f"{st.angle}: awaiting model (attempt {inp.attempt})")
    )
    try:
        finding = await get_provider().complete(system, prompt)
    finally:
        beat.cancel()
    slug = st.angle.replace(" ", "-")
    return AgentResult(
        subtask_id=st.id,
        angle=st.angle,
        finding=finding,
        confidence=0.0,  # filled in by critique
        attempts=inp.attempt,
        trace_ids=[f"agent://{slug}/attempt-{inp.attempt}"],
    )


@activity.defn
async def critique_finding(result: AgentResult) -> Critique:
    """Critic agent: score a finding and say what would make it better.

    Delegated to the provider so the judge is real when a real model is wired
    up (LLM-as-judge with a constrained JSON schema) and deterministic when it
    is not -- which is what keeps the tests hermetic.
    """
    return await get_provider().critique(result.angle, result.finding)


@activity.defn
async def review_report(inp: ReviewInput) -> ReviewNotes:
    """Orchestrator-level critic: assess the assembled report."""
    results = inp.results
    avg = round(sum(r.confidence for r in results) / max(len(results), 1), 3)
    weak = [r.subtask_id for r in results if r.confidence < CONFIDENCE_BAR]
    overall = f"Reviewed {len(results)} sections; average confidence {avg}. " + (
        "All sections meet the bar." if not weak else f"Still-weak sections: {weak}."
    )
    return ReviewNotes(overall=overall, weak_subtask_ids=weak, avg_confidence=avg)


@activity.defn
async def synthesize_report(inp: SynthInput) -> SynthResult:
    """Synthesis agent: compose the final executive summary."""
    provider = get_provider()
    body = "\n".join(f"- {r.angle}: {r.finding}" for r in inp.results)
    system = (
        "You are a synthesis agent. Reply with a single paragraph of 4-6 "
        "sentences of plain prose and nothing else. Do not use markdown, "
        "headings, bullet points, or a title. Start directly with the summary. "
        # The reviewer notes are context for *you*, not material for the reader:
        # without this the summary ends up reporting its own confidence scores.
        "Write only about the topic itself. Never mention the review, the "
        "confidence scores, the number of sections, or the process that "
        "produced these findings."
    )
    prompt = (
        f"Topic: {inp.topic}\n"
        f"Reviewer notes: {inp.review.overall}\n"
        f"Findings:\n{body}\n\nWrite the executive summary."
    )
    summary = await provider.complete(system, prompt)
    return SynthResult(summary=summary, provider=provider.name)

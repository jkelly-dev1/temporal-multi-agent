"""Shared, deterministic data types passed between the orchestrator, the agents,
and the activities.

These are plain stdlib dataclasses so Temporal's default data converter can
serialize them across the workflow/activity boundary. Keep this module free of
side effects and non-deterministic imports -- it is imported directly into the
workflow sandbox.
"""

from dataclasses import dataclass, field
from typing import List

# Findings scoring at or above this are accepted; below it the agent gets one
# feedback-guided rewrite. Defined here because the workflow (which decides
# whether to refine), the activities, and the providers (which score) must all
# agree -- it was previously duplicated in all three.
CONFIDENCE_BAR = 0.70


@dataclass
class ResearchRequest:
    """The top-level task handed to the orchestrator (the 'main agent')."""

    topic: str
    max_subtasks: int = 4
    require_approval: bool = False  # if True, wait for a human 'approve_plan' signal


@dataclass
class Subtask:
    """One facet of the topic, delegated to a specialized research agent."""

    id: int
    angle: str
    question: str


@dataclass
class ResearchInput:
    """Input to the research activity; carries reviewer feedback on retries."""

    subtask: Subtask
    attempt: int = 1
    feedback: str = ""


@dataclass
class Critique:
    """Output of the self-critique activity (an LLM-as-judge stand-in)."""

    score: float
    feedback: str


@dataclass
class AgentResult:
    """A finished section produced by one specialized agent."""

    subtask_id: int
    angle: str
    finding: str
    confidence: float
    attempts: int
    # Synthetic per-attempt identifiers, used to show that each agent's work is
    # individually addressable in the workflow history. These are NOT citations
    # and carry no provenance -- a real pipeline would attach retrieved
    # documents here, which is why the shape exists at all.
    trace_ids: List[str] = field(default_factory=list)


@dataclass
class ReviewInput:
    topic: str
    results: List[AgentResult]


@dataclass
class ReviewNotes:
    """The orchestrator-level critic's assessment of the whole report."""

    overall: str
    weak_subtask_ids: List[int] = field(default_factory=list)
    avg_confidence: float = 0.0


@dataclass
class SynthInput:
    topic: str
    results: List[AgentResult]
    review: ReviewNotes


@dataclass
class SynthResult:
    summary: str
    provider: str


@dataclass
class FinalReport:
    topic: str
    summary: str
    sections: List[AgentResult]
    review: ReviewNotes
    provider: str

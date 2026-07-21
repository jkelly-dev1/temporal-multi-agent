"""The durable orchestration layer.

Workflow code must be deterministic and replay-safe: no direct I/O, clocks, or
randomness. All real work is delegated to activities; concurrency uses
asyncio, and durable coordination uses Temporal signals/queries/child workflows.

Topology
--------
    OrchestratorWorkflow  (the "main agent")
      1. plan_subtasks                      -> task planning
      2. [optional] await approve_plan      -> human-in-the-loop signal
      3. fan out N ResearchAgentWorkflow    -> concurrent specialized agents
      4. review_report                      -> orchestrator-level critic
      5. synthesize_report                  -> final answer

    ResearchAgentWorkflow (one specialized agent, isolated per subtask)
      research_subtask -> critique_finding -> (refine once if weak)  [reflection]
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from . import activities
    from .shared import (
        AgentResult,
        Critique,
        FinalReport,
        ResearchInput,
        ResearchRequest,
        ReviewInput,
        ReviewNotes,
        Subtask,
        SynthInput,
        SynthResult,
    )

# One retry policy reused across activities: exponential backoff, capped attempts.
DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=5,
)

_CONFIDENCE_BAR = 0.70


@workflow.defn
class ResearchAgentWorkflow:
    """A specialized agent: research a subtask, self-critique, and refine once.

    Runs as a child workflow so each agent has its own isolated, durable history
    and can fail/retry without affecting its siblings.
    """

    MAX_ATTEMPTS = 2

    @workflow.run
    async def run(self, subtask: Subtask) -> AgentResult:
        attempt, feedback = 1, ""
        result: AgentResult
        while True:
            result = await workflow.execute_activity(
                activities.research_subtask,
                ResearchInput(subtask=subtask, attempt=attempt, feedback=feedback),
                start_to_close_timeout=timedelta(seconds=30),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=DEFAULT_RETRY,
            )
            critique: Critique = await workflow.execute_activity(
                activities.critique_finding,
                result,
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=DEFAULT_RETRY,
            )
            result.confidence = critique.score
            result.attempts = attempt
            if critique.score >= _CONFIDENCE_BAR or attempt >= self.MAX_ATTEMPTS:
                return result
            attempt += 1
            feedback = critique.feedback


@workflow.defn
class OrchestratorWorkflow:
    """The main agent: plans, coordinates the specialized agents, and synthesizes."""

    def __init__(self) -> None:
        self._stage = "starting"
        self._plan: list = []
        self._approved = False

    # --- human-in-the-loop + observability ---------------------------------
    @workflow.signal
    def approve_plan(self) -> None:
        self._approved = True

    @workflow.query
    def get_stage(self) -> str:
        return self._stage

    @workflow.query
    def get_plan(self) -> list:
        return [s.angle for s in self._plan]

    # --- the orchestration --------------------------------------------------
    @workflow.run
    async def run(self, req: ResearchRequest) -> FinalReport:
        self._stage = "planning"
        self._plan = await workflow.execute_activity(
            activities.plan_subtasks,
            req,
            start_to_close_timeout=timedelta(seconds=20),
            retry_policy=DEFAULT_RETRY,
        )

        if req.require_approval:
            self._stage = "awaiting-approval"
            try:
                # Durable wait: survives worker restarts; the workflow simply
                # sleeps in history until the signal (or the timeout) arrives.
                await workflow.wait_condition(
                    lambda: self._approved, timeout=timedelta(minutes=5)
                )
            except asyncio.TimeoutError:
                workflow.logger.info("Approval timed out; proceeding (auto-approve).")

        self._stage = "researching"
        # Fan out: one child workflow per subtask, all running concurrently.
        agents = [
            workflow.execute_child_workflow(
                ResearchAgentWorkflow.run,
                s,
                id=f"{workflow.info().workflow_id}-agent-{s.id}",
                retry_policy=DEFAULT_RETRY,
            )
            for s in self._plan
        ]
        results: list = list(await asyncio.gather(*agents))
        results.sort(key=lambda r: r.subtask_id)

        self._stage = "reviewing"
        review: ReviewNotes = await workflow.execute_activity(
            activities.review_report,
            ReviewInput(topic=req.topic, results=results),
            start_to_close_timeout=timedelta(seconds=20),
            retry_policy=DEFAULT_RETRY,
        )

        self._stage = "synthesizing"
        synth: SynthResult = await workflow.execute_activity(
            activities.synthesize_report,
            SynthInput(topic=req.topic, results=results, review=review),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=DEFAULT_RETRY,
        )

        self._stage = "done"
        return FinalReport(
            topic=req.topic,
            summary=synth.summary,
            sections=results,
            review=review,
            provider=synth.provider,
        )

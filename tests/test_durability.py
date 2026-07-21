"""Tests for the properties the README actually claims: durable execution,
replay safety, and automatic retry.

The happy-path tests in test_end_to_end.py show the pipeline produces a report.
These show it survives things going wrong:

  1. test_survives_worker_crash        -- kill the worker mid-run, a new worker
                                          picks the workflow up and finishes it
  2. test_workflow_history_replays     -- recorded history replays against the
                                          current workflow code (determinism)
  3. test_activity_retries_on_failure  -- a flaky activity is retried by the
                                          configured RetryPolicy until it passes

All run against a real in-process Temporal dev server; no API key, no Docker.
"""

import asyncio
import uuid

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from multi_agent import TASK_QUEUE
from multi_agent.shared import AgentResult, ResearchInput, ResearchRequest
from multi_agent.worker import ACTIVITIES, WORKFLOWS
from multi_agent.workflows import OrchestratorWorkflow


async def _await_stage(handle, stage: str, timeout: float = 20.0) -> None:
    """Poll the workflow's `get_stage` query until it reports `stage`.

    Queries are how you observe a running workflow without disturbing it, so
    this doubles as a check that the query handlers work on a live execution.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await handle.query(OrchestratorWorkflow.get_stage) == stage:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"workflow never reached stage {stage!r} "
        f"(last seen: {await handle.query(OrchestratorWorkflow.get_stage)!r})"
    )


def _worker(env, activities=None) -> Worker:
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=activities or ACTIVITIES,
    )


# ---------------------------------------------------------------------------
# 1. Durable execution: the workflow outlives the process running it.
# ---------------------------------------------------------------------------
async def test_survives_worker_crash():
    """Start the orchestration, tear the worker down while the workflow is
    still in flight, then bring up a *different* worker and let it finish.

    This is the core durable-execution claim: workflow state lives in the
    Temporal service as an event history, not in the worker's memory. The
    replacement worker reconstructs the in-progress orchestration by replaying
    that history and carries on from where the dead one stopped.
    """
    async with await WorkflowEnvironment.start_local() as env:
        wf_id = f"crash-{uuid.uuid4().hex[:8]}"

        # --- worker #1: starts the run, then dies ---------------------------
        async with _worker(env):
            handle = await env.client.start_workflow(
                OrchestratorWorkflow.run,
                ResearchRequest("durable execution", max_subtasks=3, require_approval=True),
                id=wf_id,
                task_queue=TASK_QUEUE,
            )
            # Let it get past planning and park on the human-approval signal,
            # so there is real accumulated state to lose.
            await _await_stage(handle, "awaiting-approval")
            plan_before = await handle.query(OrchestratorWorkflow.get_plan)
            assert len(plan_before) == 3
        # <- worker #1 is now gone. Nothing is polling the task queue.
        # Note: the workflow is NOT queryable right now. Queries are answered by
        # a worker replaying the history, so with no worker there is nobody to
        # answer -- the *state* is durable, the ability to read it is not.
        # Signals, by contrast, are accepted by the service and buffered.
        await handle.signal(OrchestratorWorkflow.approve_plan)

        # --- worker #2: a brand-new worker resumes and completes the run -----
        async with _worker(env):
            # Answering this query at all means worker #2 rebuilt the in-flight
            # orchestration from history: it never saw the planning step run.
            assert await handle.query(OrchestratorWorkflow.get_plan) == plan_before
            report = await handle.result()

    assert len(report.sections) == 3
    assert report.summary.strip()
    # The plan survived the restart unchanged -- no work was redone or lost.
    assert [s.angle for s in report.sections] == plan_before


# ---------------------------------------------------------------------------
# 2. Replay safety: today's code can still replay yesterday's history.
# ---------------------------------------------------------------------------
async def test_workflow_history_replays():
    """Run an orchestration, then replay its recorded history against the
    current workflow definitions.

    Temporal rebuilds workflow state by re-executing workflow code against a
    stored event history. If that code makes a non-deterministic decision --
    calls a clock, iterates a set, reorders activities -- the replay diverges
    from history and raises. This test is the standard guard against shipping a
    change that would strand every workflow already in flight.
    """
    async with await WorkflowEnvironment.start_local() as env:
        async with _worker(env):
            handle = await env.client.start_workflow(
                OrchestratorWorkflow.run,
                ResearchRequest("replay safety", max_subtasks=3),
                id=f"replay-{uuid.uuid4().hex[:8]}",
                task_queue=TASK_QUEUE,
            )
            await handle.result()
            history = await handle.fetch_history()

        # Raises if the current code diverges from what history recorded.
        await Replayer(workflows=WORKFLOWS).replay_workflow(history)


# ---------------------------------------------------------------------------
# 3. Retries: the RetryPolicy on the activities is load-bearing, not decoration.
# ---------------------------------------------------------------------------
_attempts: dict = {}
FAILURES_BEFORE_SUCCESS = 2


@activity.defn(name="research_subtask")
async def flaky_research(inp: ResearchInput) -> AgentResult:
    """Stand-in for the real research activity that fails its first two
    attempts per subtask, then succeeds.

    Registered under the *same activity name*, so the workflow code is unchanged
    and unaware -- exactly how a real transient failure (a 503 from a model API,
    a dropped connection) would present itself.
    """
    n = _attempts.get(inp.subtask.id, 0) + 1
    _attempts[inp.subtask.id] = n
    if n <= FAILURES_BEFORE_SUCCESS:
        raise RuntimeError(f"simulated transient failure {n} for subtask {inp.subtask.id}")
    return AgentResult(
        subtask_id=inp.subtask.id,
        angle=inp.subtask.angle,
        finding="Recovered finding produced after transient failures. " * 6,
        confidence=0.0,
        attempts=inp.attempt,
        trace_ids=["agent://retry/attempt-1"],
    )


async def test_activity_retries_on_failure():
    """The pipeline still produces a complete report when an activity keeps
    failing, and the failures are actually retried rather than swallowed."""
    _attempts.clear()
    patched = [a for a in ACTIVITIES if a.__name__ != "research_subtask"] + [flaky_research]

    async with await WorkflowEnvironment.start_local() as env:
        async with _worker(env, activities=patched):
            handle = await env.client.start_workflow(
                OrchestratorWorkflow.run,
                ResearchRequest("retry behaviour", max_subtasks=2),
                id=f"retry-{uuid.uuid4().hex[:8]}",
                task_queue=TASK_QUEUE,
            )
            report = await handle.result()

    # Every subtask failed twice and then succeeded -- the workflow itself never
    # saw an error, because Temporal retried the activity underneath it.
    assert len(report.sections) == 2
    assert report.summary.strip()
    for subtask_id, count in _attempts.items():
        assert count > FAILURES_BEFORE_SUCCESS, (
            f"subtask {subtask_id} was not retried past the injected failures"
        )

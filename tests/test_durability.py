"""Tests for the properties the README actually claims: durable execution,
replay safety, and automatic retry.

The happy-path tests in test_end_to_end.py show the pipeline produces a report.
These show it survives things going wrong:

  1. test_survives_worker_crash         the worker is shut down mid-run, and a
                                        new worker picks the workflow up and
                                        finishes it
  2. test_workflow_history_replays      recorded history replays against the
                                        current workflow code (determinism)
  3. test_activity_retries_on_failure   a flaky activity is retried by the
                                        configured RetryPolicy until it passes
  4. test_every_activity_call_binds...  that RetryPolicy is on every call
  5. test_the_research_activity_heart.. the activity stays observable while
                                        it is blocked on the model

The first three run against a real in-process Temporal dev server; no API key,
no Docker. The last two need neither a server nor a workflow: they exercise the
activity and the workflow module directly, which is the only way to reach the
two properties at all.
"""

import ast
import asyncio
import inspect
import uuid

from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from multi_agent import TASK_QUEUE, activities, workflows
from multi_agent.activities import research_subtask
from multi_agent.providers import MockProvider
from multi_agent.shared import AgentResult, ResearchInput, ResearchRequest, Subtask
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
        # See tests/test_end_to_end.py::_FAIL_FAST. A workflow-code exception
        # is a workflow TASK failure, which Temporal retries forever, so an
        # unconverted one hangs this suite instead of failing it.
        workflow_failure_exception_types=[Exception],
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
        # <- worker #1 is now gone. Nothing is polling the task queue. Note: the
        # workflow is NOT queryable right now. Queries are answered by a worker
        # replaying the history, so with no worker there is nobody to answer;
        # the *state* is durable, the ability to read it is not. Signals, by
        # contrast, are accepted by the service and buffered.
        await handle.signal(OrchestratorWorkflow.approve_plan)

        # --- worker #2: a brand-new worker resumes and completes the run -----
        async with _worker(env):
            # Answering this query at all means worker #2 rebuilt the in-flight
            # orchestration from history: it never saw the planning step run.
            assert await handle.query(OrchestratorWorkflow.get_plan) == plan_before
            report = await handle.result()

    assert len(report.sections) == 3
    assert report.summary.strip()
    # The plan survived the restart unchanged; no work was redone or lost.
    assert [s.angle for s in report.sections] == plan_before


# ---------------------------------------------------------------------------
# 2. Replay safety: today's code can still replay yesterday's history.
# ---------------------------------------------------------------------------
async def test_workflow_history_replays():
    """Run an orchestration, then replay its recorded history against the
    current workflow definitions.

    Temporal rebuilds workflow state by re-executing workflow code against a
    stored event history. If that code makes a non-deterministic decision,
    calls a clock, iterates a set, reorders activities, the replay diverges
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
# 3. Retries: the RetryPolicy on the activities is required, not decoration.
# ---------------------------------------------------------------------------
_attempts: dict = {}
# The highest value Temporal's own attempt counter reached for each subtask.
# This is the reading that separates "the activity was retried" from "the agent
# workflow was restarted around it": a retry increments it, a child-workflow
# restart resets it to 1. _attempts alone cannot tell the two apart, because
# both produce three invocations.
_activity_attempts: dict = {}
FAILURES_BEFORE_SUCCESS = 2


@activity.defn(name="research_subtask")
async def flaky_research(inp: ResearchInput) -> AgentResult:
    """Stand-in for the real research activity that fails its first two
    attempts per subtask, then succeeds.

    Registered under the *same activity name*, so the workflow code is unchanged
    and unaware. Exactly how a real transient failure (a 503 from a model API, a
    dropped connection) would present itself.
    """
    n = _attempts.get(inp.subtask.id, 0) + 1
    _attempts[inp.subtask.id] = n
    _activity_attempts[inp.subtask.id] = max(
        _activity_attempts.get(inp.subtask.id, 0), activity.info().attempt
    )
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
    failing, and the failures are actually RETRIED IN PLACE rather than
    swallowed or papered over by restarting the agent around them.

    The distinction is the whole claim. An invocation count cannot make it:
    three invocations happen whether the activity is retried in place or the
    child ResearchAgentWorkflow is restarted around a failure that was never
    retried, and in the second case the workflow does see the error. One
    DEFAULT_RETRY object serves both the activity and the child workflow, so
    setting maximum_attempts=1 on the activity alone still produces three
    invocations per agent.

    The assertion is therefore on Temporal's own attempt counter, which a
    restart resets and a retry does not.
    """
    _attempts.clear()
    _activity_attempts.clear()
    patched = [a for a in ACTIVITIES if a.__name__ != "research_subtask"] + [flaky_research]

    async with await WorkflowEnvironment.start_local() as env:
        async with _worker(env, activities=patched):
            handle = await env.client.start_workflow(
                OrchestratorWorkflow.run,
                ResearchRequest("retry behavior", max_subtasks=2),
                id=f"retry-{uuid.uuid4().hex[:8]}",
                task_queue=TASK_QUEUE,
            )
            report = await handle.result()

    # Every subtask failed twice and then succeeded: the workflow itself never
    # saw an error, because Temporal retried the activity underneath it.
    assert len(report.sections) == 2
    assert report.summary.strip()
    assert _attempts, (
        "the flaky activity never ran, so nothing about retries was observed; "
        "the activity substitution did not take"
    )
    expected = FAILURES_BEFORE_SUCCESS + 1
    for subtask_id, count in _attempts.items():
        assert count == expected, (
            f"subtask {subtask_id} ran {count} times, not {expected}: the "
            "injected failures were not carried by exactly one retry sequence"
        )
    # The discriminating assertion. The counts above are identical whether the
    # activity was retried or the whole agent workflow was restarted around it.
    # Temporal's attempt number is not: a child restart begins a fresh activity
    # at attempt 1, a retry reaches attempt 3.
    assert set(_activity_attempts) == set(_attempts), (
        "an invocation went unrecorded by the attempt counter"
    )
    for subtask_id, attempt in _activity_attempts.items():
        assert attempt == expected, (
            f"subtask {subtask_id} only ever reached activity attempt "
            f"{attempt}, not {expected}: the activity was NOT retried in "
            "place. The agent workflow was restarted around the failure, so "
            "the workflow did see the error"
        )


# ---------------------------------------------------------------------------
# 4. The shared retry policy is on every call, not just the ones under test.
# ---------------------------------------------------------------------------
def test_every_activity_call_binds_the_shared_retry_policy():
    """The README says `DEFAULT_RETRY` is on EVERY activity, and no behavioral
    test can see that it is.

    Deleting `retry_policy=DEFAULT_RETRY` from the research activity leaves
    test_activity_retries_on_failure green, because Temporal's DEFAULT policy
    for an activity with no explicit one retries indefinitely with the same
    one-second initial interval and the same 2.0 coefficient. The observable
    behavior is identical; the README row is not. So this is asserted
    structurally, against the source, which is where the claim lives.

    It is a claim about ALL of them, so the check has to see all of them: if
    the walk finds no calls at all it fails rather than passing on an empty
    set, which would report "every call is fine" about a file it never read.

    The name each call passes is RESOLVED rather than spelled out. Asserting
    the literal string "DEFAULT_RETRY" would decide by a word, and a word can
    be rebound to anything; the claim is that ONE module-level `RetryPolicy`
    object covers every call, whatever it happens to be called.
    """
    tree = ast.parse(inspect.getsource(workflows))
    calls = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr in ("execute_activity", "execute_child_workflow")
    ]
    assert len(calls) >= 5, (
        f"found only {len(calls)} activity/child-workflow calls in "
        "multi_agent.workflows; the source walk examined nothing useful, so "
        "this check cannot report on the retry policy either way"
    )
    bound = set()
    for call in calls:
        target = ast.unparse(call.args[0]) if call.args else "<unknown>"
        policy = {k.arg: k.value for k in call.keywords}.get("retry_policy")
        assert policy is not None, (
            f"{call.func.attr}({target}) has no retry_policy, so it falls back "
            "to Temporal's default, and the README's 'on every activity' is "
            "false"
        )
        assert isinstance(policy, ast.Name), (
            f"{call.func.attr}({target}) builds retry_policy="
            f"{ast.unparse(policy)} inline instead of sharing the module-level "
            "one, so the two can drift apart without anything noticing"
        )
        bound.add(policy.id)
    assert len(bound) == 1, (
        f"the calls are split across {sorted(bound)}; the claim is that ONE "
        "shared policy covers all of them"
    )
    (name,) = bound
    shared = getattr(workflows, name, None)
    assert isinstance(shared, RetryPolicy), (
        f"{name} resolves to {shared!r} in multi_agent.workflows, not to a "
        "RetryPolicy, so the calls share a name rather than a retry policy"
    )


# ---------------------------------------------------------------------------
# 5. Heartbeats: the activity stays observable while it waits on the model.
# ---------------------------------------------------------------------------
async def test_the_research_activity_heartbeats_through_its_steps_and_its_wait(
    monkeypatch,
):
    """The README lists "Activity heartbeats / timeouts" as demonstrated, and
    this is the test that demonstrates it.

    There are two call sites and they need different conditions to fire:

      the per-step beat  runs before the model call and always fires
      the periodic beat  fires only DURING the model call, and the offline
                         mock returns instantly, so at the shipped 2.0s cadence
                         a run produces exactly zero of them

    The provider is therefore replaced with a slow one, the cadence is
    shortened, and both kinds are asserted separately. `activities.get_provider` is the
    binding patched, not `providers.get_provider`: activities.py did
    `from .providers import get_provider` and holds its own reference, so
    patching the source module would change nothing this code calls.
    """
    beats: list = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: beats.append(details)

    class _SlowProvider(MockProvider):
        """A model call that takes long enough for the cadence to matter."""

        async def complete(self, system: str, prompt: str) -> str:
            await asyncio.sleep(0.3)
            return await super().complete(system, prompt)

    monkeypatch.setattr(activities, "get_provider", _SlowProvider)
    monkeypatch.setattr(activities, "HEARTBEAT_INTERVAL_SECONDS", 0.02)

    st = Subtask(
        id=1,
        angle="key risks and failure modes",
        question="What should an engineer know about the key risks and failure "
                "modes of: activity heartbeats?",
    )
    result = await env.run(research_subtask, ResearchInput(subtask=st))
    assert result.finding.strip(), "the activity did not actually complete"

    details = [d for (d,) in beats]
    step_beats = [d for d in details if "step" in d]
    wait_beats = [d for d in details if "awaiting model" in d]
    assert len(step_beats) == 3, (
        f"expected one beat per research step, got {len(step_beats)}: "
        f"{details}"
    )
    assert len(wait_beats) >= 2, (
        f"the activity emitted {len(wait_beats)} beats while blocked on the "
        "model, so a slow-but-healthy model call is indistinguishable from a "
        f"hung worker and heartbeat_timeout would kill it: {details}"
    )

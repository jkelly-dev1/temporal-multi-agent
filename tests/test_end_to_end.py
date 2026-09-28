"""Tests that run the whole multi-agent pipeline on a local Temporal test
server the SDK starts: no server of your own, no API key, fully deterministic.

    pytest -q
"""

import asyncio
import os
import pathlib
import re
import uuid

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from multi_agent import TASK_QUEUE
from multi_agent.shared import FinalReport, ResearchRequest
from multi_agent.worker import ACTIVITIES, WORKFLOWS
from multi_agent.workflows import OrchestratorWorkflow

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# Both documented ways in: Option A in the README is run_demo.py, Option B is
# `python -m multi_agent.starter`. They each carry their own default topic, and
# a check that reads one of them says nothing about the other.
_ENTRY_POINTS = ("run_demo.py", "multi_agent/starter.py")

# Anchored on the assignment, not on a bare `or "..."`, which cannot see a
# default that sits on a continuation line. The optional paren keeps it
# working if either default is wrapped in parentheses.
_DEFAULT_TOPIC = re.compile(r'sys\.argv\[1:\]\)\.strip\(\)\s*or\s*\(?\s*"([^"]+)"')


# A bug in workflow code raises inside a workflow task, and Temporal retries a
# failed workflow task forever, so that a deployed fix can rescue workflows
# already in flight. In a test suite that retry loop shows up as a hang:
# stubbing `research_subtask` to return the wrong type makes every pipeline
# test sit there until the runner's wall clock kills it, with no assertion and
# no red line. So the test workers convert any such exception into a workflow
# failure and the test fails in seconds. Production keeps the default, which
# is the behavior wanted in a deployment.
_FAIL_FAST = [Exception]


async def _run(env, req: ResearchRequest, *, approve: bool = False) -> FinalReport:
    async with Worker(
        env.client, task_queue=TASK_QUEUE, workflows=WORKFLOWS,
        activities=ACTIVITIES, workflow_failure_exception_types=_FAIL_FAST
    ):
        handle = await env.client.start_workflow(
            OrchestratorWorkflow.run,
            req,
            id=f"test-{uuid.uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
        )
        if approve:
            await handle.signal(OrchestratorWorkflow.approve_plan)
        return await handle.result()


@pytest.mark.asyncio
async def test_pipeline_completes_and_fans_out():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        report = await _run(env, ResearchRequest("multi-agent orchestration", max_subtasks=4))

    assert isinstance(report, FinalReport)
    assert len(report.sections) == 4  # planner -> 4 concurrent agents
    assert report.summary.strip()
    assert report.provider == "mock"
    # every agent produced a scored finding with a recorded trace id
    for s in report.sections:
        assert 0.0 <= s.confidence <= 1.0
        assert s.finding.strip()
        assert len(s.trace_ids) == 1


@pytest.mark.asyncio
async def test_self_critique_refines_weak_sections():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        report = await _run(env, ResearchRequest("reliability", max_subtasks=3))

    # The first-pass mock finding is short and scores below the 0.70 bar, so each
    # agent should refine once and end at 2 attempts with higher confidence.
    assert all(s.attempts == 2 for s in report.sections)
    assert all(s.confidence >= 0.70 for s in report.sections)
    assert report.review.weak_subtask_ids == []
    assert report.review.avg_confidence >= 0.70


@pytest.mark.asyncio
async def test_human_in_the_loop_signal_gates_execution():
    async with await WorkflowEnvironment.start_local() as env:
        report = await _run(
            env,
            ResearchRequest("agent platform design", max_subtasks=2, require_approval=True),
            approve=True,
        )
    assert len(report.sections) == 2
    assert report.summary.strip()


@pytest.mark.asyncio
async def test_the_approval_gate_actually_blocks_before_the_signal():
    """The gate must block, which a report coming back cannot show.

    test_human_in_the_loop_signal_gates_execution sends the signal and asserts
    a report comes back. A handler that discarded the signal would pass that
    test too, because the workflow auto-approves after its timeout and would
    simply take minutes instead of seconds.

    This test asserts the two things that distinguish a gate from a delay:
    it is parked at `awaiting-approval` and has started no agent before the
    signal, and it proceeds after it.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=WORKFLOWS, activities=ACTIVITIES,
                          workflow_failure_exception_types=_FAIL_FAST):
            handle = await env.client.start_workflow(
                OrchestratorWorkflow.run,
                ResearchRequest("multi-agent orchestration", max_subtasks=2,
                                require_approval=True),
                id=f"test-gate-{uuid.uuid4().hex[:8]}",
                task_queue=TASK_QUEUE,
            )
            # Park at the gate. Query rather than sleep: the stage is the
            # workflow's own answer about where it is.
            for _ in range(100):
                if await handle.query(OrchestratorWorkflow.get_stage) == "awaiting-approval":
                    break
                await asyncio.sleep(0.05)
            stage = await handle.query(OrchestratorWorkflow.get_stage)
            assert stage == "awaiting-approval", (
                f"expected to be parked at the gate, stage is {stage!r}")

            plan = await handle.query(OrchestratorWorkflow.get_plan)
            assert len(plan) == 2, "the plan must exist before approval is asked for"

            # It must still be parked a moment later, with nothing running.
            await asyncio.sleep(0.2)
            assert await handle.query(OrchestratorWorkflow.get_stage) == "awaiting-approval", (
                "the workflow advanced past the gate without a signal")

            # "No agent started" is read off the history, not the stage label:
            # a workflow can report awaiting-approval while children run.
            history = await handle.fetch_history()
            children = [e for e in history.events if e.event_type
                        == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED]
            research = [e for e in history.events if e.event_type
                        == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
                        and e.activity_task_scheduled_event_attributes
                        .activity_type.name == "research_subtask"]
            assert children == [], f"{len(children)} agent(s) started before approval"
            # research_subtask is scheduled by the CHILD workflows, so with no
            # child started this sees only research the orchestrator scheduled
            # itself; the children check above is the one that covers agents.
            assert research == [], "research ran before approval"

            await handle.signal(OrchestratorWorkflow.approve_plan)
            report = await handle.result()

    assert len(report.sections) == 2
    assert report.summary.strip()
    # It proceeded because of the signal, not because the wait expired.
    # Without this the gate can discard the human's decision entirely and
    # still produce a normal report once the timeout fires, which is a
    # delay, not a gate, and reads identically in the output.
    assert report.approval == "approved", (
        f"the run proceeded via {report.approval!r}, not via the signal")


@pytest.mark.asyncio
async def test_the_agents_actually_run_concurrently():
    """The fan-out is concurrent, measured as overlap.

    A sequential `for` loop in place of the `asyncio.gather` fan-out produces
    the same number of sections, so a count cannot tell the two apart.
    Concurrency is a claim about overlap, so it is measured as overlap: the
    agents' activity spans must intersect.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        report = await _run(env, ResearchRequest("multi-agent orchestration",
                                                 max_subtasks=4))

    spans = [(s.started_at, s.finished_at) for s in report.sections
             if s.started_at is not None and s.finished_at is not None]
    assert len(spans) == 4, (
        "sections do not carry start/finish times, so overlap cannot be "
        "measured and the concurrency claim cannot be checked")

    peak = 0
    for start, _ in spans:
        peak = max(peak, sum(1 for a, b in spans if a <= start < b))
    assert peak > 1, (
        f"no two agent spans overlap (peak concurrency {peak}); the fan-out "
        "ran sequentially")


def test_the_documented_default_topic_demonstrates_self_critique():
    """Both documented one-command demos must demonstrate the feature.

    The mock judge scores a finding by length and the mock's finding grows
    with the topic, so a longer default topic scores above the bar and the
    reflection loop never runs.

    The judge is called here, not re-implemented, so a change to its formula
    is a change to what this test measures. The question is built by the real
    planner for the same reason: a hand-written question would be a second
    copy of something the application already decides. Both entry points are
    read, because each carries its own default.
    """
    from multi_agent.activities import plan_subtasks
    from multi_agent.providers import MockProvider
    from multi_agent.shared import CONFIDENCE_BAR

    for entry in _ENTRY_POINTS:
        src = (_REPO_ROOT / entry).read_text()
        match = _DEFAULT_TOPIC.search(src)
        assert match, f"could not find {entry}'s default topic"
        topic = match.group(1)

        # run_demo.py and starter.py both request max_subtasks=4.
        subtasks = asyncio.run(
            plan_subtasks(ResearchRequest(topic=topic, max_subtasks=4))
        )
        assert subtasks, f"{entry}: the planner produced no subtasks"
        for st in subtasks:
            finding = asyncio.run(MockProvider().complete("sys", st.question))
            critique = asyncio.run(MockProvider().critique(st.angle, finding))
            assert critique.score < CONFIDENCE_BAR, (
                f"{entry}: the default topic {topic!r} scores "
                f"{critique.score} on {st.angle!r} against a bar of "
                f"{CONFIDENCE_BAR}, so running it with no argument shows no "
                "self-critique"
            )


def test_the_readme_test_count_is_the_one_the_suite_collects():
    """A hand-typed count in a README drifts the moment a test is added.

    The count is not recomputed here, since that would be a second copy of
    pytest's collection. A collect-only child run is asked, and its answer is
    compared to the figure the README prints. `--collect-only` runs no test, so there is no
    recursion.
    """
    import subprocess
    import sys

    readme = (_REPO_ROOT / "README.md").read_text()
    stated = re.search(r"pytest -q\s+#\s*(\d+) tests", readme)
    assert stated, (
        "README.md no longer prints a test count in the form this check "
        "reads, so the figure is unchecked rather than agreed with")

    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "--collect-only"],
        cwd=_REPO_ROOT, capture_output=True, text=True, timeout=300)
    collected = re.search(r"(\d+) tests? collected", run.stdout)
    assert collected, (
        f"the collect-only child printed no count, so nothing was compared. "
        f"exit={run.returncode}\n{run.stdout[-500:]}")

    assert int(collected.group(1)) == int(stated.group(1)), (
        f"README.md says {stated.group(1)} tests; pytest collects "
        f"{collected.group(1)}")


def test_the_suite_itself_cannot_reach_a_real_model(request):
    """The floor tests/conftest.py puts under the whole suite.

    `get_provider()` selects the real Anthropic path when AGENT_PROVIDER and a
    credential are both present, and the README's "use a real LLM" recipe
    tells the reader to export exactly that pair. A check on
    `report.provider == "mock"` after a run would be a report on money already
    spent, not a guard.

    This asserts the guard itself instead of its consequence, so deleting the
    autouse fixture fails here regardless of what the developer's shell holds.
    """
    from multi_agent.providers import get_provider

    # The environment checks below can pass without the fixture when the
    # developer's own shell exports AGENT_PROVIDER=mock and no key, so the
    # fixture's presence is asserted directly as well.
    assert "force_the_offline_mock" in request.fixturenames, (
        "the autouse fixture that pins the offline provider is not active")
    assert os.environ.get("AGENT_PROVIDER") == "mock", (
        "the suite is not pinned to the offline provider; a stray "
        "AGENT_PROVIDER can reach a real model from inside these tests")
    assert not [v for v in os.environ if v.startswith("ANTHROPIC_")], (
        "an ANTHROPIC_* variable is visible to the suite")
    assert get_provider().name == "mock", (
        "get_provider() did not return the mock under the pinned environment")


class TestProviderSelection:
    """The provider seam: a name and its credential select the real path.

    The SDK defers auth to request time, so a real provider built without a
    key would surface later as five retried activity failures and a dead
    workflow. A stray AGENT_PROVIDER in a shell must not turn `pytest` into
    live billed calls either.
    """

    @staticmethod
    def _pick(env):
        from multi_agent import providers
        saved = {k: os.environ.get(k) for k in ("AGENT_PROVIDER", "ANTHROPIC_API_KEY")}
        try:
            for k in saved:
                os.environ.pop(k, None)
            os.environ.update(env)
            providers._PROVIDER = None
            return providers.get_provider().__class__.__name__
        finally:
            for k, v in saved.items():
                os.environ.pop(k, None)
                if v is not None:
                    os.environ[k] = v
            providers._PROVIDER = None

    def test_no_configuration_uses_the_mock(self):
        assert self._pick({}) == "MockProvider"

    def test_the_name_without_its_credential_falls_back_to_the_mock(self):
        assert self._pick({"AGENT_PROVIDER": "anthropic"}) == "MockProvider"

    def test_a_credential_without_the_name_uses_the_mock(self):
        assert self._pick({"ANTHROPIC_API_KEY": "sk-x"}) == "MockProvider"

    def test_both_together_select_the_real_provider(self):
        """The only case here that needs the optional dependency.

        AnthropicProvider.__init__ imports `anthropic` lazily, and
        requirements.txt leaves that package commented out so the demo runs
        offline. So this one case skips where it is absent, which is CI, and is
        the point of installing only what requirements.txt lists.

        The four negative cases in this class do not skip, and must not: they
        are the ones that keep a stray AGENT_PROVIDER from turning a test run
        into live billed calls, and they never construct the real provider.
        """
        pytest.importorskip(
            "anthropic",
            reason="the optional real-model dependency is not installed; the "
                   "fallback cases still run")
        assert self._pick({"AGENT_PROVIDER": "anthropic",
                           "ANTHROPIC_API_KEY": "sk-x"}) == "AnthropicProvider"

    def test_an_unknown_provider_name_falls_back_rather_than_reaching_out(self):
        assert self._pick({"AGENT_PROVIDER": "openai",
                           "ANTHROPIC_API_KEY": "sk-x"}) == "MockProvider"


def test_a_topic_that_says_feedback_is_not_read_as_a_revision():
    """The mock revises only when the activity appended reviewer feedback. A
    topic that merely contains the word would otherwise score as a revised
    answer on its first pass, and the reflection loop would never run."""
    from multi_agent.providers import FEEDBACK_MARKER, MockProvider

    question = "What should an engineer know about feedback loops in control?"
    first = asyncio.run(MockProvider().complete("sys", question))
    revised = asyncio.run(MockProvider().complete(
        "sys", f"{question}\n\n{FEEDBACK_MARKER} name the trade-offs"))
    assert "Addressing the reviewer's note" not in first
    assert "Addressing the reviewer's note" in revised

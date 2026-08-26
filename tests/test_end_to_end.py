"""End-to-end tests that run the whole multi-agent pipeline in an in-process
Temporal test environment: no external server, no API key, fully deterministic.

    pytest -q
"""

import asyncio
import os
import pathlib
import uuid

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from multi_agent import TASK_QUEUE
from multi_agent.shared import FinalReport, ResearchRequest
from multi_agent.worker import ACTIVITIES, WORKFLOWS
from multi_agent.workflows import OrchestratorWorkflow


async def _run(env, req: ResearchRequest, *, approve: bool = False) -> FinalReport:
    async with Worker(
        env.client, task_queue=TASK_QUEUE, workflows=WORKFLOWS, activities=ACTIVITIES
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
    """The gate must block, and the old test could not tell.

    test_human_in_the_loop_signal_gates_execution sends the signal and asserts
    a report comes back. Replacing the whole `approve_plan` handler body with
    `pass`, discarding the human's decision entirely, left every test in this
    file passing, because the workflow auto-approves after its timeout and
    simply took ten minutes instead of six seconds.

    So this asserts the two things that distinguish a gate from a delay: it is
    parked at `awaiting-approval` and has started no agent BEFORE the signal,
    and it proceeds AFTER it.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=WORKFLOWS, activities=ACTIVITIES):
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

            await handle.signal(OrchestratorWorkflow.approve_plan)
            report = await handle.result()

    assert len(report.sections) == 2
    assert report.summary.strip()
    # And it proceeded because of the signal, not because the wait expired.
    # Without this the gate can discard the human's decision entirely and
    # still produce a normal report once the timeout fires, which is a
    # delay, not a gate, and reads identically in the output.
    assert report.approval == "approved", (
        f"the run proceeded via {report.approval!r}, not via the signal")


@pytest.mark.asyncio
async def test_the_agents_actually_run_concurrently():
    """"Fans out CONCURRENTLY" was described and never asserted.

    Replacing the `asyncio.gather` fan-out with a sequential `for` loop left
    every test green, because they all assert the COUNT of sections and a
    sequential loop produces the same count. Concurrency is a claim about
    OVERLAP, so it is measured as overlap: the agents' activity spans must
    intersect.
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
    """The one-command demo must demonstrate the feature it is for.

    The mock judge scores a finding by length and the mock's finding grows
    with the topic, so a longer default topic scores ABOVE the bar and the
    reflection loop never runs. The previous default was 55 characters and
    scored 0.727 against a bar of 0.70: `python run_demo.py` with no
    argument, the README's headline command, exercised none of the
    self-critique the repository is built around, and nothing said so.
    """
    import re
    from multi_agent.providers import MockProvider
    from multi_agent.shared import CONFIDENCE_BAR

    src = (pathlib.Path(__file__).resolve().parents[1] / "run_demo.py").read_text()
    match = re.search(r'or "([^"]+)"', src)
    assert match, "could not find run_demo.py's default topic"
    topic = match.group(1)

    finding = asyncio.run(MockProvider().complete("sys", f"Research: {topic}"))
    score = min(len(finding) / 300, 1.0)
    assert score < CONFIDENCE_BAR, (
        f"the default topic {topic!r} scores {score:.3f} against a bar of "
        f"{CONFIDENCE_BAR}, so `python run_demo.py` shows no self-critique")


class TestProviderSelection:
    """The seam, which no test touched.

    `AGENT_PROVIDER=anthropic` with no key constructed AnthropicProvider
    anyway; the SDK defers auth to request time, so the missing key surfaced
    later as five retried activity failures and a dead workflow. It also meant
    a stray AGENT_PROVIDER in a shell turned `pytest` into live billed calls.
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
        requirements.txt leaves that package commented out on purpose so the
        demo runs offline. So this one case skips where it is absent, which is
        CI, and is the point of installing only what requirements.txt lists.

        The four negative cases above do NOT skip, and must not: they are the
        ones that keep a stray AGENT_PROVIDER from turning a test run into
        live billed calls, and they never construct the real provider.
        """
        pytest.importorskip(
            "anthropic",
            reason="the optional real-model dependency is not installed; the "
                   "fallback cases above still run")
        assert self._pick({"AGENT_PROVIDER": "anthropic",
                           "ANTHROPIC_API_KEY": "sk-x"}) == "AnthropicProvider"

    def test_an_unknown_provider_name_falls_back_rather_than_reaching_out(self):
        assert self._pick({"AGENT_PROVIDER": "openai",
                           "ANTHROPIC_API_KEY": "sk-x"}) == "MockProvider"

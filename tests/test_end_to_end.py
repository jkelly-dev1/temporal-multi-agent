"""End-to-end tests that run the whole multi-agent pipeline in an in-process
Temporal test environment -- no external server, no API key, fully deterministic.

    pytest -q
"""

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
    # every agent produced a scored finding with recorded sources
    for s in report.sections:
        assert 0.0 <= s.confidence <= 1.0
        assert s.finding.strip()
        assert len(s.sources) == 2


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

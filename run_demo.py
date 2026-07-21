"""One-command demo: spins up an in-process Temporal dev server, runs a worker,
starts an orchestration, approves its plan via a signal, and prints the report.

No external Temporal CLI, Docker, or API key required:

    python run_demo.py "optional research topic"
"""

import asyncio
import sys
import time
import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from multi_agent import TASK_QUEUE
from multi_agent.report import print_report
from multi_agent.shared import ResearchRequest
from multi_agent.worker import ACTIVITIES, WORKFLOWS
from multi_agent.workflows import OrchestratorWorkflow


_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


async def _spinner(handle, stop: asyncio.Event) -> None:
    """Progress indicator that reports what the workflow is actually doing.

    The stage text comes from the workflow's `get_stage` query, so this is not
    a decorative spinner -- it is live observability of a running execution,
    polled once a second while the frames animate at 10fps.

    Writes to stderr and no-ops when stderr is not a TTY, so piping or
    redirecting the demo's output stays clean.
    """
    if not sys.stderr.isatty():
        return
    started = time.monotonic()
    stage, next_poll, i = "starting", 0.0, 0
    try:
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_poll:
                try:
                    stage = await handle.query(OrchestratorWorkflow.get_stage)
                except Exception:  # completed, or no worker to answer -- keep last
                    pass
                next_poll = now + 1.0
            sys.stderr.write(
                f"\r{_FRAMES[i % len(_FRAMES)]} {stage}… ({now - started:.0f}s)\033[K"
            )
            sys.stderr.flush()
            i += 1
            await asyncio.sleep(0.1)
    finally:
        sys.stderr.write("\r\033[K")  # erase the line on the way out
        sys.stderr.flush()


async def main() -> None:
    topic = " ".join(sys.argv[1:]).strip() or (
        "designing a reliable multi-agent orchestration platform"
    )
    # start_local() downloads and runs a real Temporal dev server in-process.
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=WORKFLOWS,
            activities=ACTIVITIES,
        ):
            handle = await env.client.start_workflow(
                OrchestratorWorkflow.run,
                ResearchRequest(topic=topic, max_subtasks=4, require_approval=True),
                id=f"demo-{uuid.uuid4().hex[:8]}",
                task_queue=TASK_QUEUE,
            )
            await asyncio.sleep(0.5)
            print("stage:", await handle.query(OrchestratorWorkflow.get_stage))
            print("plan :", await handle.query(OrchestratorWorkflow.get_plan))
            print("approving plan (human-in-the-loop signal)...")
            await handle.signal(OrchestratorWorkflow.approve_plan)

            # The agents fan out here; with a real model this is the slow part,
            # so show what the orchestrator is doing while we wait.
            stop = asyncio.Event()
            spin = asyncio.create_task(_spinner(handle, stop))
            try:
                report = await handle.result()
            finally:
                stop.set()
                await spin

            print_report(report)


if __name__ == "__main__":
    asyncio.run(main())

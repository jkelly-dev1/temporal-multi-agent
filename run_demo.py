"""One-command demo: spins up an in-process Temporal dev server, runs a worker,
starts an orchestration, approves its plan via a signal, and prints the report.

No external Temporal CLI, Docker, or API key required:

    python run_demo.py "optional research topic"
"""

import asyncio
import sys
import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from multi_agent import TASK_QUEUE
from multi_agent.report import print_report
from multi_agent.shared import ResearchRequest
from multi_agent.worker import ACTIVITIES, WORKFLOWS
from multi_agent.workflows import OrchestratorWorkflow


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

            report = await handle.result()
            print_report(report)


if __name__ == "__main__":
    asyncio.run(main())

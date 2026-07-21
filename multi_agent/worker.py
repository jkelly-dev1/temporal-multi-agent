"""Worker process: hosts the workflows and activities and polls a task queue.

Run this against a real Temporal server (e.g. `temporal server start-dev`):

    python -m multi_agent.worker
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from . import TASK_QUEUE, activities
from .workflows import OrchestratorWorkflow, ResearchAgentWorkflow

ACTIVITIES = [
    activities.plan_subtasks,
    activities.research_subtask,
    activities.critique_finding,
    activities.review_report,
    activities.synthesize_report,
]
WORKFLOWS = [OrchestratorWorkflow, ResearchAgentWorkflow]


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=ACTIVITIES,
    )
    print(f"Worker polling task queue {TASK_QUEUE!r}. Web UI: http://localhost:8233")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

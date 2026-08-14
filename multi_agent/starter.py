"""Client that starts an orchestration, observes it via queries, approves the
plan via a signal, and prints the final report.

Requires a running worker (`python -m multi_agent.worker`) and Temporal server.

    python -m multi_agent.starter "your research topic here"
"""

import asyncio
import sys
import uuid

from temporalio.client import Client

from . import TASK_QUEUE
from .report import print_report
from .shared import ResearchRequest
from .workflows import OrchestratorWorkflow


async def main() -> None:
    topic = " ".join(sys.argv[1:]).strip() or (
        "designing a reliable multi-agent orchestration platform"
    )
    client = await Client.connect("localhost:7233")
    wf_id = f"research-{uuid.uuid4().hex[:8]}"

    handle = await client.start_workflow(
        OrchestratorWorkflow.run,
        ResearchRequest(topic=topic, max_subtasks=4, require_approval=True),
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    print(f"Started {wf_id}  ->  http://localhost:8233/namespaces/default/workflows/{wf_id}")

    # Observe live via queries, then release the human-in-the-loop gate.
    await asyncio.sleep(1)
    print("stage:", await handle.query(OrchestratorWorkflow.get_stage))
    print("plan :", await handle.query(OrchestratorWorkflow.get_plan))
    print("approving plan...")
    await handle.signal(OrchestratorWorkflow.approve_plan)

    report = await handle.result()
    print_report(report)


if __name__ == "__main__":
    asyncio.run(main())

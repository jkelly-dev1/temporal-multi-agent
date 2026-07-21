# Temporal Multi-Agent Orchestration Demo

[![tests](https://github.com/jkelly-dev1/temporal-multi-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/jkelly-dev1/temporal-multi-agent/actions/workflows/ci.yml)

A small but production-shaped **multi-agent platform** built on
[Temporal](https://temporal.io): a **main orchestrator agent** plans a task,
coordinates a fleet of **specialized agents** that run concurrently, each agent
**self-critiques and refines** its own work, and the orchestrator **reviews and
synthesizes** the result, all as **durable, replay-safe** workflows.

It runs **fully offline** with a deterministic mock "brain," so `python run_demo.py`
works with no API key, no Docker, and no external Temporal server. Point it at a
real Claude model by setting two env vars (see below); the orchestration code
doesn't change.

## Why Temporal

The hard part of a multi-agent platform isn't the prompts; it's the *systems*:
coordinating long-running, failure-prone work across many concurrent agents and
recovering cleanly when something dies mid-flight. Temporal gives us **durable
execution**: workflow state is reconstructed by replaying an event history, so a
crashed worker resumes exactly where it left off. That turns "orchestrate agents"
into a tractable, testable engineering problem.

## Architecture

```
                         ┌────────────────────────────┐
   ResearchRequest ─────▶│    OrchestratorWorkflow     │   (the "main agent")
                         │                              │
                         │  1. plan_subtasks  ──────────┼──▶ Planner  (activity)
                         │  2. await approve_plan  ◀────┼──── human signal (HITL)
                         │  3. fan-out (asyncio.gather) │
                         │        │   │   │   │          │
                         │        ▼   ▼   ▼   ▼          │
                         │   ResearchAgentWorkflow ×N   │   (specialized agents,
                         │     research→critique→refine │    one child wf each)
                         │  4. review_report  ──────────┼──▶ Critic   (activity)
                         │  5. synthesize_report ───────┼──▶ Synthesizer (activity)
                         └──────────────┬───────────────┘
                                        ▼
                                   FinalReport
```

- **Workflows** (`workflows.py`) hold the deterministic coordination logic. No
  I/O, clocks, or randomness; that's what makes them replayable.
- **Activities** (`activities.py`) do all the side-effecting work (the LLM/tool
  calls). Temporal retries them independently with backoff and records results
  in history.
- **Child workflows** isolate each specialized agent: its own durable history,
  its own retries, failing without taking down its siblings.

## Temporal concepts demonstrated

| Concept | Where |
|---|---|
| Durable execution / replay-safe orchestration | `OrchestratorWorkflow` |
| Task planning & decomposition | `plan_subtasks` activity |
| Fan-out / concurrent agents | `asyncio.gather` over child workflows |
| Agent isolation | `ResearchAgentWorkflow` child workflows |
| Reflection loop (self-critique → refine) | `ResearchAgentWorkflow.run` |
| Retries with exponential backoff | `DEFAULT_RETRY` on every activity |
| Activity heartbeats / timeouts | `research_subtask` |
| Human-in-the-loop | `approve_plan` **signal** + `wait_condition` |
| Live observability | `get_stage` / `get_plan` **queries** |
| Deterministic, hermetic tests | `tests/` via the in-process test server |

## How it maps to an agent-platform role

- *"core infrastructure for multi-agent orchestration, task planning,
  coordination, execution, and recovery"* → the orchestrator + child agents +
  retry/heartbeat/reflection.
- *"platform primitives … that enable internal teams to build and deploy new
  agents"* → adding an agent = write one `@workflow.defn` child + its activities;
  the orchestrator composes them.
- *"reliable backend services for … state management, scheduling, and
  observability"* → durable state, signals/queries, task-queue scheduling.
- *"long-running workflows across multiple concurrent agents"* → child-workflow
  fan-out with independent durable histories.

## Run it

Install (Python 3.10+):

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

**Option A: one command, zero setup** (in-process dev server, no Docker/CLI):

```bash
python run_demo.py "designing a fleet-telemetry ingestion pipeline"
```

**Option B: real server + Web UI** (great for showing observability):

```bash
temporal server start-dev          # UI at http://localhost:8233
python -m multi_agent.worker       # terminal 2
python -m multi_agent.starter "your topic here"   # terminal 3
```

**Tests** (hermetic, no API key or Docker needed):

```bash
pytest -q          # 6 tests, ~25s -- runs a real in-process Temporal dev server
```

## What the tests prove

Durability claims are cheap to write and easy to get wrong, so each one here is
backed by a test that fails if the property breaks.

| Claim | Test | How it's shown |
|---|---|---|
| The pipeline plans, fans out, and synthesizes | `test_pipeline_completes_and_fans_out` | 4 subtasks → 4 concurrent child workflows → one report |
| Agents self-critique and refine | `test_self_critique_refines_weak_sections` | weak first drafts end at 2 attempts, above the 0.70 bar |
| Human-in-the-loop gates execution | `test_human_in_the_loop_signal_gates_execution` | the run blocks until an `approve_plan` signal arrives |
| **Work survives losing the worker** | `test_survives_worker_crash` | worker is killed mid-orchestration; a *new* worker rebuilds the in-flight run from history and finishes it |
| **Workflow code is replay-safe** | `test_workflow_history_replays` | recorded history is replayed against current code via Temporal's `Replayer`; non-determinism raises |
| **Retries are load-bearing** | `test_activity_retries_on_failure` | an activity fails its first two attempts per subtask; the `RetryPolicy` carries it through and the workflow never sees an error |

Two details worth noting, because they're the parts that are easy to get wrong:

- After the worker dies the workflow is **not queryable**; queries are answered
  by a worker replaying history, so with no worker there is nobody to answer.
  Signals are still accepted, because the service buffers them. Durable state and
  live observability are different guarantees.
- The retry test was checked against a mutation: setting `maximum_attempts=1`
  makes it fail. A retry test that passes with retries disabled proves nothing.

## Use a real LLM (optional)

```bash
pip install anthropic
export AGENT_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-...
export AGENT_MODEL=claude-opus-4-8    # optional; this is the default
python run_demo.py "your topic"
```

Only the `providers.py` "brain" changes; the orchestration, retries, reflection,
and durability are identical.

**The critic swaps too.** `AgentProvider.critique()` is the LLM-as-judge seam:

- **Real path:** the judge is a model call constrained by a JSON schema
  (structured outputs), so the score comes back as a `number` rather than being
  scraped out of prose. That is the difference between a judge you can branch
  on and one that occasionally answers "I'd rate this an 8/10!".
- **Mock path:** a deterministic length heuristic. Crude on purpose: a
  first-pass finding lands below the bar and a revised one lands above it, so
  the reflection loop runs identically on every test run and the suite stays
  hermetic.

Both score against the single `CONFIDENCE_BAR` in `shared.py`, which the
workflow also reads when deciding whether to refine.

[**SAMPLE_RUN.md**](SAMPLE_RUN.md) is a verbatim capture of a real run, so the
self-critique behaviour can be inspected without an API key. In that run the
judge sent three of four sections back for a rewrite; the rewritten ones come
back with concrete thresholds and trade-offs where the untouched one stays at
"use idempotent writes and a dead-letter queue".

## Layout

```
multi_agent/
  shared.py       dataclasses passed across the wf/activity boundary
  providers.py    MockProvider (default, offline) + optional AnthropicProvider
  activities.py   planner / researcher / critic / synthesizer (the real work)
  workflows.py    OrchestratorWorkflow + ResearchAgentWorkflow (durable coordination)
  worker.py       hosts workflows+activities against a Temporal server
  starter.py      client: start, query, signal-approve, print
  report.py       pretty-printer
run_demo.py       self-contained in-process runner
tests/            end-to-end tests on the in-process test server
```

## License

MIT. See [LICENSE](LICENSE).

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
                      +--------------------------------+
ResearchRequest ----->|      OrchestratorWorkflow      |   (the "main agent")
                      |                                |
                      |  1. plan_subtasks  ------------+--> Planner  (activity)
                      |  2. await approve_plan  <------+--- human signal (HITL)
                      |  3. fan-out (asyncio.gather)   |
                      |        |   |   |   |           |
                      |        v   v   v   v           |
                      |   ResearchAgentWorkflow xN     |   (specialized agents,
                      | research -> critique -> refine |    one child wf each)
                      |  4. review_report  ------------+--> Critic   (activity)
                      |  5. synthesize_report ---------+--> Synthesizer (activity)
                      +--------------+-----------------+
                                     v
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
| Reflection loop (self-critique -> refine) | `ResearchAgentWorkflow.run` |
| Retries with exponential backoff | `DEFAULT_RETRY` on every activity |
| Activity heartbeats / timeouts | `research_subtask` |
| Human-in-the-loop | `approve_plan` **signal** + `wait_condition` |
| Live observability | `get_stage` / `get_plan` **queries** |
| Deterministic, hermetic tests | `tests/` via the in-process test server |

## How it maps to an agent-platform role

- *"core infrastructure for multi-agent orchestration, task planning,
  coordination, execution, and recovery"* -> the orchestrator + child agents +
  retry/heartbeat/reflection.
- *"platform primitives ... that enable internal teams to build and deploy new
  agents"* -> adding an agent = write one `@workflow.defn` child + its activities;
  the orchestrator composes them.
- *"reliable backend services for ... state management, scheduling, and
  observability"* -> durable state, signals/queries, task-queue scheduling.
- *"long-running workflows across multiple concurrent agents"* -> child-workflow
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

## Claims backed by tests

Durability claims are cheap to write and easy to get wrong, so each one here is
backed by a test that fails if the property breaks.

| Claim | Test | How it's shown |
|---|---|---|
| The pipeline plans, fans out, and synthesizes | `tests/test_end_to_end.py::test_pipeline_completes_and_fans_out` | 4 subtasks -> 4 concurrent child workflows -> one report |
| Agents self-critique and refine | `tests/test_end_to_end.py::test_self_critique_refines_weak_sections` | weak first drafts end at 2 attempts, above the 0.70 bar |
| Human-in-the-loop gates execution | `tests/test_end_to_end.py::test_human_in_the_loop_signal_gates_execution` | the run blocks until an `approve_plan` signal arrives |
| **Work survives losing the worker** | `tests/test_durability.py::test_survives_worker_crash` | worker is killed mid-orchestration; a *new* worker rebuilds the in-flight run from history and finishes it |
| **Workflow code is replay-safe** | `tests/test_durability.py::test_workflow_history_replays` | recorded history is replayed against current code via Temporal's `Replayer`; non-determinism raises |
| **Retries are load-bearing** | `tests/test_durability.py::test_activity_retries_on_failure` (mutation-checked) | an activity fails its first two attempts per subtask; the `RetryPolicy` carries it through and the workflow never sees an error |

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
self-critique behavior can be inspected without an API key. In that run the
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

## Sibling projects

One of several small projects on the theme of AI systems you can trust and
prove, all following the same discipline of claims mapped to tests, mutation
checks on the tests that matter, and behavior verified before publishing:

- [prompt-injection-benchmark](https://github.com/jkelly-dev1/prompt-injection-benchmark) measures
  defenses against a synthetic attack corpus rather than asserting them.
- [ai-data-boundary-proxy](https://github.com/jkelly-dev1/ai-data-boundary-proxy) enforces PII
  egress policy and measures what a right-to-erasure operation misses.
- [llm-eval-gate](https://github.com/jkelly-dev1/llm-eval-gate) measures the
  judges rather than trusting them.
- [federated-retrieval-router](https://github.com/jkelly-dev1/federated-retrieval-router)
  fans one query out across four stores concurrently and reports the cost of
  being right beside the correctness, because a routing score without its
  backends-per-query number is not a result.
- [hardened-mcp-server](https://github.com/jkelly-dev1/hardened-mcp-server)
  pins MCP tool definitions and measures how long a rug pull goes unnoticed,
  which turns out to be exactly the cache lifetime the server itself asked
  for. Its clock is injected for the same reason this repo's is.
- [vlm-extraction-integrity](https://github.com/jkelly-dev1/vlm-extraction-integrity)
  audits its own instrument the way this repo audits its clock: the page five
  paid runs had been measured on drew two fields on one baseline, so 13 of
  what were published as validation catches were the harness's own misreads.
- [airgapped-ai-bundle](https://github.com/jkelly-dev1/airgapped-ai-bundle)
  is this repo's staleness question with the network removed. Simulating an
  enclave over two years, 62% of the days on which everything was healthy
  produce a status artifact BYTE-IDENTICAL to one from a day something was
  months out of date -- six of eight components cannot report their own input
  age, and two of the silent ones are the revocation list and the CVE feed.
- [least-privilege-agent](https://github.com/jkelly-dev1/least-privilege-agent)
- [citation-abstention-rag](https://github.com/jkelly-dev1/citation-abstention-rag)
- [agentic-review-gate](https://github.com/jkelly-dev1/agentic-review-gate)
- [typed-agent-service](https://github.com/jkelly-dev1/typed-agent-service)
- [llm-observability-stack](https://github.com/jkelly-dev1/llm-observability-stack)
- [ai-compliance-checker](https://github.com/jkelly-dev1/ai-compliance-checker)
- [agent-sandbox-escape](https://github.com/jkelly-dev1/agent-sandbox-escape)
- [parser-eval](https://github.com/jkelly-dev1/parser-eval)

## License

MIT. See [LICENSE](LICENSE).

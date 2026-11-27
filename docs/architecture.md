# Implemented architecture

Universal Swarms is a Python application with a CLI, FastAPI session service, FastMCP tool service, a shared scheduler and PostgreSQL storage. Profiles under `config/` select peers, tools and workflow rules. The architecture below describes the code in `src/universal_swarms/`.

## Components

```mermaid
flowchart LR
  Client[CLI or test client] --> API[FastAPI sessions and SSE]
  Client --> CLI[CLI run, resume, inspect, ledger]
  API --> Engine[Engine scheduler]
  CLI --> Engine
  Engine --> Model[OpenAI-compatible model endpoint]
  Engine -->|persisted call identity| MCP[FastMCP tool server]
  MCP --> ToolEngine[Engine.execute_tool]
  Engine --> Swarm[(Swarm PostgreSQL)]
  ToolEngine --> Swarm
  ToolEngine --> Fixtures[Fixture tools]
  Fixtures --> State[(Fixture PostgreSQL)]
```

| Component | Implementation | Responsibility |
| --- | --- | --- |
| CLI | `__init__.py` | Loads environment, migrates, runs or resumes a profile, and inspects or exports a run. |
| HTTP service | `api.py` | Admits sessions, returns projections and streams committed events. |
| Scheduler | `runtime.py` | Leases and claims work, constructs context, invokes the model and MCP, validates results and closes activations. |
| Tool service | `mcp_server.py` | Resolves a persisted call for an in-flight activation and invokes the shared tool executor. |
| Store | `store.py`, `schema.sql` | PostgreSQL state, admission, event sequencing and state projection. |
| Model adapter | `llm.py` | Chat-completions requests, response parsing, argument parsing and provider diagnostics. |
| Profiles and fixtures | `config.py`, `fixtures.py`, `config/*.yaml` | Tool definitions, profile validation and scenario-specific fixture operations. |

## Request and activation sequence

```mermaid
sequenceDiagram
  participant C as Client
  participant A as FastAPI
  participant E as Engine
  participant D as Swarm database
  participant L as Model
  participant M as MCP
  C->>A: POST /sessions
  A->>D: Admit run, seed peers and assignment
  C->>A: POST /sessions/{id}/run/stream
  A->>E: Run scheduler
  E->>D: Acquire lease and claim envelope
  E->>L: Context and allowed tools
  L-->>E: Decision
  E->>D: Persist decision
  E->>M: Execute persisted call identity
  M->>D: Verify lease, call and receipt
  M->>D: Commit tool result and effects
  M-->>E: Tool receipt
  E->>D: Close activation and route output
  A-->>C: Ordered ledger events and terminal state
```

The scheduler can make several model calls per activation. A terminal tool stages a routing intent for closure. The API event stream reads the same ledger that supports inspection. MCP validates the run lease, fence, in-flight message, persisted decision and call position before executing a call.

## Data model

```mermaid
erDiagram
  SWARM_RUNS ||--o{ SWARM_ROSTER : contains
  SWARM_RUNS ||--o{ SWARM_MAILBOX : contains
  SWARM_RUNS ||--o{ SWARM_ARTIFACTS : contains
  SWARM_RUNS ||--o{ SWARM_LEDGER : records
  SWARM_ROSTER ||--o{ SWARM_MAILBOX : sends_or_receives
  SWARM_ROSTER ||--o{ SWARM_ARTIFACTS : produces
  SWARM_LEDGER ||--o{ SWARM_RESPONSE_MARKERS : supports
```

- `swarm_runs` stores admission identity, goal, resolved profile, status, deadline, lease, fence and final answer.
- `swarm_roster` stores peer identity, template, prompt revision, creator, grants, lifetime counters and retirement reason.
- `swarm_mailbox` stores envelopes, correlation and reply references, delivery state, activation, fence and pinned snapshot.
- `swarm_artifacts` stores immutable key/version content and producer.
- `swarm_ledger` stores ordered events and operation receipts. A run-scoped operation key is unique.
- `swarm_response_markers` records learned model response markers linked to ledger evidence.

The schema uses run-scoped peer and message foreign keys. The database migration is `universal-swarms migrate`. The fixture database is configured separately using `STATE_DATABASE_URL`.

## State and recovery

```mermaid
stateDiagram-v2
  [*] --> queued
  queued --> in_flight: claim and pin
  in_flight --> in_flight: lease takeover and resume
  in_flight --> acknowledged: close activation
  queued --> cancelled: recipient retires
  in_flight --> cancelled: run ends or peer retires
```

A run lease has an owner, expiry and monotonically increasing fence. Guarded mutations check ownership and lease against database time. The scheduler renews the lease during external calls. After takeover, it resumes an in-flight envelope with its pinned snapshot and reconstructs decisions and tool receipts from the ledger. A tool operation is identified by activation, decision sequence and call position. An existing receipt is reused only when its fingerprint matches.

A committed final response creates a `final_answer` artifact and ends the run. A deadline, exhausted activation or call budget, or lack of runnable work can end it as blocked. Termination retires remaining peers and cancels outstanding work.

## Interfaces and configuration

The API supports `POST /sessions`, `GET /sessions/{run_id}` and `POST /sessions/{run_id}/run/stream`. The stream can replay events using `Last-Event-ID`. The CLI exposes `migrate`, `migrate-fixtures`, `serve`, `run`, `resume`, `inspect` and `ledger`.

The profile loader validates configuration and records the resolved profile in the run. Credentials are supplied through the environment. See [README.md](../README.md) for startup and [test.md](test.md) for executable scenarios.

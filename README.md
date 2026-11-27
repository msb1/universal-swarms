# Universal Swarms

Universal Swarms is a Python runtime for coordinating multiple AI agents on a shared goal. A YAML profile defines the initial agents, their instructions and tools, the workflow, and the conditions for completing a run. Agents exchange assignments and handoffs through mailboxes, create versioned artifacts, and can review each other's work. A scheduler processes their messages and records decisions, tool results, and state changes in a durable ledger.

The runtime uses a unified TTL-bound peer model for both temporary and evolving agents. Every agent is a peer with a role, prompt, tool grants, and an optional time-to-live measured in completed message activations. A short-lived specialist can be recruited for a specific task and retire automatically after its allotted turns. A peer with no turn limit can remain active for the run and, when authorized, inspect and revise (i.e., mutate) another peer's prompt or tool grants. Each activation pins the recipient's current revision, so a change takes effect on subsequent work while an activation already in progress retains its original context.

PostgreSQL stores the roster, mailboxes, artifacts, and execution ledger, supporting inspection and recovery of interrupted runs. A FastMCP server executes agent tools, a FastAPI service exposes sessions and event streams, and the CLI supports running profiles, resuming runs, and inspecting their history. Configured limits, tool grants, routing checks, and evidence requirements bound the work agents can perform.

## Setup and operation

Install the project with `uv sync --locked`. The CLI loads settings from the repository's `.env`. The model endpoint and credentials use `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `MAIN_MODEL`. `SWARM_DATABASE_URL` selects the swarm database, and `STATE_DATABASE_URL` selects the existing fixture database. `SWARM_MCP_URL` and `SWARM_MCP_PORT` configure the tool server. `PORT` and `AGENT_URL` configure the agent service.

Start the tool server with `uv run --locked universal-swarms-mcp`. Initialize the swarm schema with `uv run --locked universal-swarms migrate`, then start the agent service with `uv run --locked universal-swarms serve`. The FastAPI service binds to loopback and exposes its API reference at `/docs`.

To run a profile through the CLI, use `uv run --locked universal-swarms run --config config/log-analysis.yaml`. Supply `--key` to retry an identical admission or `resume RUN_ID` to resume an interrupted active run. Use a separate migration owner for shared deployments and give the runtime role SELECT and INSERT on `swarm_ledger` and the privileges required by the mutable tables.

## Test scenarios

The test scenarios exercise the runtime from peer lifecycle operations through complete workflows. A fixture workflow runs agents against prepared data and checks their tool results and final output against known values. Review and roster mutation test how agents use evidence to assess work, recruit specialists, and update authorized peers' instructions or tool grants during a run.

### Phase 1 — Peer runtime

- **Delegation and lifetime** tests fan-out to temporary specialists, correlated replies, and retirement after a configured number of activations. It also checks retirement routing and cleanup of pending messages.
- **Roster changes and recovery** tests peer recruitment, prompt mutation, handoffs, and recovery after interrupted work. Scripted SQL and compliance reviews exercise the same scheduler and tool entry points.

### Phase 2 — Fixture workflows

- **Log analysis** examines access logs to identify the IP address responsible for the most HTTP 500 errors. The workflow handles malformed records and validates the resulting IP and count.
- **Revenue analysis** calculates Chicago's first-quarter revenue from customer and order fixtures. It normalizes customer keys and city names, then validates the total.
- **Employee audit** identifies corrupted employee records, calculates the New York median salary, and generates a city-level report. Each phase requires recorded evidence before the workflow advances.

### Phase 3 — Review and roster mutation

- **Flawed SQL generator** starts with a writer that submits a failing query against imperfect data. A critic updates the writer's instructions and capabilities so it can inspect the schema and submit a corrected query.
- **Compliance policy drift** sends a marketing draft to a critic that discovers missing policy disclosures. The critic recruits a two-activation legal specialist, then reviews the returned artifact before acceptance.
- **Sycophantic critic** tests an initially approved cache design under simulated load. The failure prompts the architect to revise the critic's instructions and validate an improved design.

The [test guide](docs/test.md) includes prerequisites, commands, and the ledger assertions for each case.

## Documentation

Documentation is available in the [design](docs/design.md), [architecture](docs/architecture.md), [concepts](docs/concept.md), and [test guide](docs/test.md). The [digest](digest.md) records project progress and verification.

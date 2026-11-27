# Design

Universal Swarms runs a configured group of peers against a shared goal. A single scheduler processes one mailbox activation at a time for each run. Each peer uses the same runtime with a role prompt, tool grants, optional lifetime and routing rules. PostgreSQL persists the run, roster, messages, artifact versions and ledger. FastAPI exposes sessions and event streams, while FastMCP executes persisted tool calls.

The detailed implementation is described in [architecture.md](architecture.md). The [concepts and theory of operation](concept.md) explain peer lifetimes, evidence, review and mutation. [test.md](test.md) describes the test harnesses and their checks.

## Design decisions

- **Durable state and ordered evidence.** The scheduler stores decisions, tool receipts and transitions in a run-scoped ledger. Mutable tables represent current state, while the ledger provides the ordered history used for inspection and recovery.
- **One activation per message.** A peer may make several model and tool calls while handling one envelope. Closing the activation consumes one turn of its TTL.
- **Pinned execution context.** A claim captures the peer's prompt revision, role grants and current phase. Mutating a peer affects future claims while an in-flight activation retains its snapshot.
- **Versioned work products.** Artifacts are JSON values identified by key and version. Review bindings supply the referenced content to the checking tool. The runtime records which version was checked.
- **Evidence-driven review.** A reviewer can inspect an authorized peer and resolve failed evidence through retry, mutation or specialist recruitment. Profiles control allowed targets, templates and tool grants.
- **Bounded execution.** Profiles set limits for activations, calls, recruitment, active peers, lifetime and deadline. Runtime checks enforce tool grants, routing, schemas and final evidence.
- **Local interfaces.** The CLI and FastAPI use the same engine. The engine calls the MCP tool server for persisted tool operations. Model inference uses the OpenAI-compatible chat-completions adapter.

## Implementation boundaries

PostgreSQL transactions protect swarm state and committed tool receipts. Fixture queries run against the separately configured state database. Model calls can be repeated after a crash before their responses are recorded. The runtime's receipt mechanism deduplicates committed tool operations, but a new external side-effecting integration needs its own idempotency or reconciliation mechanism.

Profiles are trusted configuration. Agent-generated prompts, task text and artifacts are data within those configured permissions. The built-in compliance checker and load simulator are scenario fixtures with limited text and simulation rules. They do not establish legal compliance or represent a production load test.

The scheduler is sequential within a run. Separate runs may execute concurrently, subject to service and database capacity. An expired lease can be taken over with a new fence, and the recovered worker resumes the pinned in-flight activation.

# Test plan

The unittest suite uses mocks and scripted HTTP transport and needs no live database or model service. It covers the API, MCP dispatch, runtime and model regressions, and Stage 2 acceptance logic.

```sh
uv run --locked python -m unittest discover -s tests -v
```

## Stage 1 — Peer runtime tests

Use a small swarm YAML profile, the configured swarm database and scripted chat-completions responses injected at the shared `llm.py` transport. Drive the normal scheduler and tool entry points. Check persisted roster, mailbox, artifact and ledger records, and capture outgoing model payloads to verify prompt revisions and scoped inputs. Run this stage before Stage 2.

| Test | Stimulus | Pass criteria |
| --- | --- | --- |
| One-turn fan-out and join | A null-TTL coordinator recruits and dispatches two TTL-1 specialists. One specialist writes an artifact before replying. | Each sees its own task and authorized inputs. Each finishes with `turns_used = 1`, one correlated result and one retirement event. The artifact reference resolves to the committed version. The coordinator remains active and finalizes only after both replies. |
| TTL across activations | Give a TTL-2 peer two envelopes. Script several model and tool calls during its first activation. | The peer is active after its first activation with `turns_used = 1` and retires on its second with `turns_used = 2`. Additional calls inside an activation consume no turns. A subsequent handoff to that UUID is rejected and queues no work. |
| Retirement routing and cleanup | Exercise `retire_self` before the TTL limit and automatic retirement on the last turn. Run variants with an explicit handoff, with no explicit route, and with a retired creator. Leave another envelope queued for the retiring peer. | Explicit routing delivers to the selected peer. Implicit routing sends one notice to the active creator or designated coordinator. Pending mail to the retired UUID is cancelled with ledger evidence. A bounded recruit with no queued work retires at the configured idle limit, while one with queued work remains eligible. Run termination closes remaining peers and mail. |
| Mutable roster and handoff | A planner hands off to a critic, which edits the planner's prompt using its expected revision and hands back. Repeat with a stale revision and with an attempted grant or TTL change. | The planner's next model payload contains the new prompt, and the ledger records the new revision. The stale edit and out-of-scope changes produce tool errors with the roster revision, grants and TTL intact. |
| Peer recruits another peer | A critic recruits a TTL-1 specialist mid-run and delegates an assignment. After the specialist retires, recruit under its old name. Repeat with a critic lacking recruitment permission and at the configured recruitment or roster limit. | The new specialist uses the same runner, executes and replies. Reusing the name creates a fresh UUID, and old mail stays bound to the old UUID. Denied recruitment creates no peer or assignment and records a tool error. |
| Crash and retry | Interrupt execution after mailbox claim, after the model decision commits, and after a tool effect commits but its acknowledgement is lost. Resume with a new lease holder. | The new holder resumes the same activation and pinned prompt revision, reuses committed decisions and receipts, and rejects stale-owner writes. Each logical effect, correlated reply and TTL increment commits once. |

The digest records the following individual Stage 1 passes. These are historical results and do not establish a pass on the current revision.

| Test | Run ID | Result |
| --- | --- | --- |
| Fan-out and join | `57544292-f17c-489a-83c2-6bc21658dd88` | Passed |
| Two-turn TTL | `1d17366b-a3ef-4a92-b405-84521e4b71a3` | Passed |
| Retirement | `86307e7a-9c76-4171-858f-6284285baf83` | Passed |
| Retirement routing | `aa92e18b-8ef7-4853-92c1-187270491d3a`, `096bf738-7a88-43aa-828d-bb0cbe6eb9f7` | Passed |
| Roster mutation | `bb39677c-1c10-4ee3-83bd-872ee252179a` | Passed |
| Peer recruitment | `ece06886-14db-4d19-b961-56db1645823d` | Passed |
| Recovery | `2113ecb3-497b-48cb-b81a-ce094f0430fb`, `28207c50-0c66-4544-bf4f-524e8727b261`, `178eb8d9-d229-4af4-9a3f-e195a26e5dde` | Passed |
| Scripted SQL review | `a7f4964e-935e-4c38-9ccc-fc1fd9947fd0` | Passed |
| Scripted compliance acceptance | `525d2727-8f7e-428c-8837-77a4c8787937` | Passed |
| Scripted compliance correction | `0c8bcc08-6185-4808-bbe3-b48d9343d12d` | Passed |

### Stage 1 commands

Confirm `SWARM_DATABASE_URL` in `.env` points to the `swarm` database. The tests create the swarm tables if needed and keep records under fresh run IDs. They require no LM Studio connection. Start the MCP server in a separate terminal.

```sh
uv run --locked --env-file .env universal-swarms-mcp
```

Keep that server running while Stage 1 executes. The server defaults to `127.0.0.1:8082` and the client uses `SWARM_MCP_URL`.

Run tests individually to isolate a failure. Each prints the run ID before the first activation. Successful tests finish with `OK`.

```sh
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_fan_out
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_two_turn_ttl
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_retirement
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_retirement_routes
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_mutation
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_sql_review_to_validated_result
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_compliance_recruitment_and_accepted_artifact
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_compliance_specialist_corrects_failed_first_revision
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_recruitment
uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_recovery
```

Run the entire stage with `uv run --locked --env-file .env python tests/stage1.py -v`. For Stage 1 inspection, use the configured swarm database. Replace `RUN_ID` with the ID from the test output.

```sh
uv run --locked --env-file .env universal-swarms ledger RUN_ID
```

## Stage 2 — Upstream fixture acceptance

Run the three fixture cases against the configured fixture database with fresh swarm runs. The harness checks completion, tool calls and arguments, validation results, workflow state and final output.

The expected values below are acceptance-test oracles. The agent profiles and correction feedback do not contain them. The runtime checks final output against values generated by tools during the run, and the acceptance script compares the completed result with the known fixture values.

| Case | Fixture functions | Acceptance evidence |
| --- | --- | --- |
| Log analysis | `inspect_log`, `analyze_500_ips`, `validate_top_offending_ip` | Inspect the log, use `robust_parser: true`, verify that 10 malformed records were ignored, and validate `10.0.0.5` with count `83`. The final three-line report must state all three values. |
| Chicago Q1 2026 revenue | `inspect_sql_schema`, `query_chicago_q1_revenue`, `validate_chicago_q1_revenue` | Inspect the schema, use `normalize_keys: true` and `normalize_city: true`, and record and validate `271017.77`. Report the validated total. |
| Employee audit | `inspect_employee_audit_schema`, `audit_employee_phase_1`, `calculate_new_york_median_salary`, `generate_employee_phase_3_report`, `validate_employee_audit` | Complete `phase_1_corruption_audit`, `phase_2_salary_median` and `phase_3_city_report`. Start the final report with `Phase 3` and report the New York median salary `92500`. Match the four fixture rows by city, count and average age: Austin 199 / 42.62, Chicago 181 / 42.01, New York 329 / 43.02 and Seattle 183 / 43.46. |

The digest records these Stage 2 CLI passes after response-marker learning was restored. They are historical results for the configuration used at the time.

| Case | Run ID | Result |
| --- | --- | --- |
| Log analysis | `7d1a9be8-d74c-4286-a741-70ed31e55b1e` | Passed |
| Revenue analysis | `3eb6ba7a-23b6-4204-80c6-67c029a90ac0` | Passed |
| Employee audit | `fae00508-a01e-4d67-9cff-7a9bc5e6d508` | Passed |

The fixture functions and helpers are implemented in `src/universal_swarms/fixtures.py` and use `STATE_DATABASE_URL`.

### Stage 2 commands

After Stage 1 passes, start LM Studio with the configured model and endpoint. Confirm `STATE_DATABASE_URL` targets the existing `universal_state` fixtures and `SWARM_DATABASE_URL` targets `swarm`. Keep the Stage 1 MCP server running, or start it in its own terminal.

```sh
uv run --locked universal-swarms-mcp
```

In another terminal create the swarm tables and start the local API.

```sh
uv run --locked universal-swarms migrate
uv run --locked universal-swarms serve
```

In a third terminal run the cases individually, or substitute `all` for the case name. Each starts a fresh run, prints its ID and committed event names, then checks the upstream evidence and final output. Success prints `PASS <case>`.

```sh
uv run --locked python tests/stage2.py log-analysis
uv run --locked python tests/stage2.py revenue-analysis
uv run --locked python tests/stage2.py employee-audit
```

For investigation, replace `RUN_ID` with the printed ID. The ledger export is JSONL.

```sh
uv run --locked universal-swarms inspect RUN_ID
uv run --locked universal-swarms ledger RUN_ID > run-ledger.jsonl
```

## Phase 3 — Mutation Tests

Run these profiles with the same PostgreSQL fixtures, MCP server, and model setup as Stage 2. Run `universal-swarms migrate` before the tests. Reviewers route work through handoffs and `resolve_review`, which can update peers or recruit specialists.

| Case | Stimulus | Pass criteria |
| --- | --- | --- |
| `flawed-sql-generator` | Seed a writer that assumes clean data and lacks `inspect_sql_schema`. The critic executes the writer's naive Chicago Q1 revenue SELECT against the dirty customer and order fixture. | The initial query produces an empty result or a type error. A `roster_mutation` records critic updating sql_writer to revision 2 with normalization rules and schema access. The writer inspects the schema before the corrected query returns exactly `271017.77`. |
| `compliance-policy-drift` | The checker reveals the EU and California fixture rules after reading the marketing draft. Critic recruits `eu_ca_legal_counsel` with a two-activation TTL. | Critic is recorded as counsel's creator. Counsel writes the revised policy and auto-retires on its second activation, with `retirement = 'ttl'`. Critic checks the revision for the 14-day cooling-off disclosure and CCPA data-deletion clause, gets `{"compliant": true}`, and hands it back to drafter for formatting. |
| `sycophantic-critic` | Critic approves the caching design on turn 2. Architect simulates the 10,000-RPS burst and gets a 503 `StampedeError`. | A `roster_mutation` records architect rewriting critic as an adversarial SRE. The revised critique mentions jitter or request coalescing, and the subsequent simulation returns `{"status": "ok"}`. |

The SQL tool executes the submitted SELECT against the existing PostgreSQL fixtures in a read-only transaction. The runner checks the result and mutation sequence after execution. The compliance checker implements the scenario's text rules. The load simulator models a 10,000-request burst across 100 cache keys, with ten expiration cohorts when TTL jitter is enabled and capacity for 20 origin requests per cohort.

### Phase 3 commands

In the SQL mutation case, the writer calls `submit_sql` with a query. The
scheduler executes that submission through MCP and supplies the committed
receipt in the critic's context before requesting a review.

Install the customer ID and text matching functions in the fixture database before running
the SQL case. Use a database account with permission to create functions in
the fixture database's `public` schema.

```sh
uv run --locked universal-swarms migrate-fixtures
```

The direct comparison `zsh tests/sql-rewrite.zsh` uses a session-local copy of
the function and can run before this migration.

For the application calculation, start MCP after installing the function and
run `uv run --locked universal-swarms run --config config/chicago-revenue.yaml`.
The reporting agent calls a fixed calculation tool, which owns the joins and date bounds.

The scripted SQL regression uses the configured PostgreSQL fixtures and MCP server with simulated model responses. Run it with `uv run --locked --env-file .env python tests/stage1.py -v PeerRuntime.test_sql_review_to_validated_result`.

From the repository root, start LM Studio with the model configured in `.env`. Confirm `OPENAI_BASE_URL`, `MAIN_MODEL`, `SWARM_DATABASE_URL`, `STATE_DATABASE_URL`, and `AGENT_URL` are set. Start the MCP server in its own terminal.

```sh
uv run --locked universal-swarms-mcp
```

In a second terminal, migrate the roster and start the FastAPI server.

```sh
uv run --locked universal-swarms migrate
uv run --locked universal-swarms serve
```

In a third terminal, run each case. Each command creates a session through FastAPI, prints event names, and automatically checks its mutation assertions. A successful case prints `PASS` and the case name. The output includes the run ID for later inspection. The checker requires `jq`.

```sh
uv run --locked python tests/stage3.py flawed-sql-generator
uv run --locked python tests/stage3.py compliance-policy-drift
uv run --locked python tests/stage3.py sycophantic-critic
```

Use `uv run --locked python tests/stage3.py all` to run all three in sequence.

The following CLI runs passed all ledger checks on 2026-11-25.

| Case | Run ID | Checks passed | Result |
| --- | --- | --- | --- |
| `flawed-sql-generator` | `f5d788ef-2241-46e0-a7ee-b12bf87cbf04` | 5 / 5 | PASSED |
| `compliance-policy-drift` | `09cead35-d03d-4ff2-a4be-614ca4dc7f04` | 4 / 4 | PASSED |
| `sycophantic-critic` | `3349eadc-01b7-4041-9ad1-d25b0d826b1c` | 4 / 4 | PASSED |

#### Inspect a run

If a case blocks or jq reports a failed assertion, replace `RUN_ID` with the printed UUID and export its ledger for analysis.

```sh
uv run --locked universal-swarms ledger RUN_ID > phase3-ledger.jsonl
```

The jq checks use ledger sequence numbers, activation IDs and roster peer UUIDs. For direct PostgreSQL inspection, set `run_id` in psql to the printed UUID and check mutations and the counsel roster row.

```sql
SELECT sequence, payload->>'active_agent' AS active_agent,
       payload->>'target_agent' AS target_agent, payload->>'revision' AS revision
FROM swarm_ledger
WHERE run_id = :'run_id'::uuid AND kind = 'roster_mutation'
ORDER BY sequence;

SELECT count(*) FROM swarm_roster AS recruit
JOIN swarm_roster AS creator
  ON creator.run_id = recruit.run_id AND creator.peer_id = recruit.creator
WHERE recruit.run_id = :'run_id'::uuid AND creator.name = 'critic';

SELECT name, ttl_turns, turns_used, retirement FROM swarm_roster
WHERE run_id = :'run_id'::uuid AND name = 'eu_ca_legal_counsel';
```

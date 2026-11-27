import asyncio
import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import uuid4

import httpx
import psycopg

from universal_swarms.config import BUILTINS, load_profile
from universal_swarms.fixtures import calculate_chicago_q1_revenue, check_compliance_rules, inspect_sql_schema, run_test_query
from universal_swarms.llm import LlmClient, ModelError, strict_json, wire_messages
from universal_swarms.runtime import Engine


class RegressionTests(unittest.TestCase):
    def test_verdict_is_bound_to_submitted_version_and_review_disposition(self):
        engine = Engine(MagicMock(), None, uuid4())
        config = load_profile("config/sycophantic-critic.yaml")
        author, critic = uuid4(), uuid4()
        engine.target = MagicMock(return_value=author)
        ref = {"key": "cache_design", "version": 7}
        mail = {"sender": author, "recipient": critic, "payload": {"artifact_refs": [ref]},
                "pinned": {"role": config["templates"]["reviewer"], "phase": None}}
        args = {"target": str(author), "task": "Approved: adequate capacity"}
        result = engine.apply(mail, "send_handoff", args)
        self.assertEqual(result["payload"]["artifact_refs"], [ref])
        returned = {"payload": result["payload"], "pinned": {"role": config["templates"]["architect"]}}
        self.assertTrue(engine.requires_prompt_revision(returned))
        mail["payload"]["evidence_sequence"] = 123
        critique = engine.apply(mail, "send_handoff", {**args, "task": "Changes requested: mitigate stampede"})
        self.assertEqual(critique["payload"]["revision_of"], ref)
        self.assertFalse(engine.requires_prompt_revision({**returned, "payload": critique["payload"]}))

    def test_measured_acceptance_publishes_receipted_text_without_model(self):
        engine = Engine(MagicMock(), None, uuid4())
        engine.coordinator = uuid4()
        engine.guard = MagicMock()
        engine.close = MagicMock()
        engine.finish = MagicMock()
        engine.config = {}
        text = "Measured result: 42"
        receipt = {"target": str(engine.coordinator), "final_text": text, "evidence_call_id": "check"}
        mail = {"kind": "review_accepted", "recipient": engine.coordinator, "sender": uuid4(),
                "pinned": {"role": {}, "phase": None},
                "payload": {"acceptance_sequence": 12, "evidence_call_id": "check", "final_text": text}}
        engine.s.one.side_effect = [{"payload": receipt}, None]
        asyncio.run(engine.activation(mail))
        engine.close.assert_called_once_with(mail, {"final": text})
        mail["payload"]["final_text"] += " changed"
        engine.s.one.side_effect = [{"payload": receipt}]
        asyncio.run(engine.activation(mail))
        self.assertEqual(engine.finish.call_args.args[0], "blocked")

    def test_accepted_artifact_finalizes_exact_version_without_model(self):
        engine = Engine(MagicMock(), None, uuid4())
        engine.coordinator = uuid4()
        engine.guard = MagicMock()
        engine.close = MagicMock()
        engine.finish = MagicMock()
        engine.config = {"final_patterns": ["approved"]}
        text = "  approved\n\n**Policy**\n"
        ref = {"key": "policy", "version": 3}
        payload = {"artifact_content": text, "artifact_refs": [ref], "acceptance_sequence": 24,
                   "evidence_call_id": "check"}
        mail = {"kind": "review_accepted", "recipient": engine.coordinator, "sender": uuid4(),
                "payload": payload, "pinned": {"role": {}, "phase": None}}
        receipt = {"payload": {"artifact": ref, "target": str(engine.coordinator), "evidence_call_id": "check"}}
        engine.artifact = MagicMock(return_value={"content": text})
        engine.s.one.side_effect = [receipt, None]
        asyncio.run(engine.activation(mail))
        engine.artifact.assert_called_once_with(**ref)
        engine.close.assert_called_once_with(mail, {"final": text})
        engine.finish.assert_not_called()
        for change in ({"artifact_content": text.strip()}, {"artifact_refs": [{**ref, "version": 2}]}):
            mail["payload"] = {**payload, **change}
            engine.s.one.side_effect = [receipt]
            asyncio.run(engine.activation(mail))
            self.assertEqual(engine.finish.call_args.args[0], "blocked")
        mail["payload"] = payload
        engine.config["final_patterns"] = ["missing"]
        engine.s.one.side_effect = [receipt, None]
        asyncio.run(engine.activation(mail))
        self.assertIn("failure", engine.close.call_args.args[1])

    def test_specialist_operations_bind_version_and_reply_recipient(self):
        engine = Engine(MagicMock(), None, uuid4())
        engine.config = load_profile("config/compliance-policy-drift.yaml")
        role = engine.config["templates"]["counsel"]
        reviewer, specialist = uuid4(), uuid4()
        ref = {"key": "refund_policy", "version": 3}
        mail = {"activation": uuid4(), "recipient": specialist, "sender": reviewer,
                "pinned": {"phase": None, "role": role}}
        engine.active = MagicMock(side_effect=lambda peer: {"peer_id": peer, "creator": reviewer})
        for assignment in ({"revision_of": ref}, {"review_followup": {"correction_of": ref}}):
            mail["payload"] = {"artifact_refs": [ref], **assignment}
            self.assertEqual(engine.allowed(mail), {"read_artifact", "submit_review_revision"})
            engine.s.one.return_value = {"n": 4}
            with self.assertRaisesRegex(ValueError, "version conflict"):
                engine.apply(mail, "submit_review_revision", {"content": "policy"})
            engine.s.one.return_value = {"n": 3}
            result = engine.apply(mail, "submit_review_revision", {"content": "policy"})
            self.assertEqual(result["target"], str(reviewer))
            self.assertEqual(result["payload"]["artifact_refs"], [{"key": ref["key"], "version": 4}])
        mail["payload"] = {"artifact_refs": [ref], "review_followup": {"confirmed_ref": ref}}
        engine.s.do.reset_mock()
        self.assertEqual(engine.allowed(mail), {"read_artifact", "confirm_review_artifact"})
        result = engine.apply(mail, "confirm_review_artifact", {})
        self.assertEqual(result["payload"]["artifact_refs"], [ref])
        engine.s.do.assert_not_called()

    def test_review_artifact_contract_applies_before_version_allocation(self):
        engine = Engine(MagicMock(), None, uuid4())
        engine.config = load_profile("config/compliance-policy-drift.yaml")
        mail = {"recipient": uuid4(), "activation": uuid4()}
        with self.assertRaisesRegex(ValueError, "not of type 'string'"):
            engine.write(mail, {"key": "refund_policy", "content": {"policy": "draft"}})
        engine.s.one.assert_not_called()
        engine.s.do.assert_not_called()
        engine.s.one.return_value = {"n": 1}
        ref = engine.write(mail, {"key": "refund_policy", "content": "draft"})
        self.assertEqual(ref, {"key": "refund_policy", "version": 2})
        mail["payload"] = {"review_followup": {"confirmed_ref": ref}}
        with self.assertRaisesRegex(ValueError, "without rewriting"):
            engine.write(mail, {"key": "refund_policy", "content": "another draft"})

    def test_legacy_invalid_artifact_generates_bound_repair_evidence(self):
        engine = Engine(MagicMock(), None, uuid4())
        config = load_profile("config/compliance-policy-drift.yaml")
        name = "check_compliance_rules"
        engine.tools = {name: {"parameters": config["tools"][name]}}
        engine.guard = MagicMock()
        engine.receipt = MagicMock(return_value=None)
        engine.failed_review = MagicMock(return_value=None)
        producer = uuid4()
        engine.artifact = MagicMock(return_value={"producer": producer, "content": {"policy": "draft"}})
        ref = {"key": "refund_policy", "version": 2}
        mail = {"activation": uuid4(), "recipient": uuid4(), "payload": {"artifact_refs": [ref]},
                "pinned": {"phase": None, "role": config["templates"]["reviewer"]}}
        invocation = {"id": "check", "function": {"name": name, "arguments": "{}"}}
        receipt = asyncio.run(engine.execute_tool(mail, 1, 0, invocation, False))
        self.assertTrue(receipt["review_evidence"])
        self.assertEqual(receipt["invalid_artifact"]["producer"], str(producer))
        self.assertEqual(receipt["bound_input"]["artifact"], ref)
        self.assertIsNone(receipt["result"])

    def test_passing_specialist_revision_requires_returned_confirmation(self):
        engine = Engine(MagicMock(), None, uuid4())
        engine.coordinator, counsel, critic = uuid4(), uuid4(), uuid4()
        engine.failed_review = MagicMock(return_value=None)
        engine.artifact = MagicMock(return_value={"content": "policy", "producer": counsel})
        engine.target = MagicMock(return_value=engine.coordinator)
        ref = {"key": "refund_policy", "version": 2}
        role = load_profile("config/compliance-policy-drift.yaml")["templates"]["reviewer"]
        mail = {"activation": uuid4(), "recipient": critic, "sender": counsel, "in_reply_to": uuid4(),
                "payload": {"artifact_refs": [ref]}, "pinned": {"role": role, "phase": None}}
        args = {"target": "drafter", "task": "Accept", "artifact_refs": [ref]}
        receipt = {"payload": {"bound_input": {"artifact": ref}, "result": {"compliant": True},
                               "message": {"tool_call_id": "check"}}}
        def records(request):
            engine.s.one.side_effect = [receipt, {"version": 2}, {"peer_id": counsel}, {"payload": request}]
        records({})
        with self.assertRaisesRegex(ValueError, "for confirmation"):
            engine.apply(mail, "send_handoff", args)
        engine.target.return_value = counsel
        records({})
        confirmation = engine.apply(mail, "send_handoff", {**args, "target": "counsel"})
        self.assertEqual(confirmation["payload"]["review_followup"], {"confirmed_ref": ref})
        engine.target.return_value = engine.coordinator
        records(confirmation["payload"])
        self.assertEqual(engine.apply(mail, "send_handoff", args)["intent"], "review_accepted")

    def test_artifact_review_rejects_conflicting_and_stale_versions(self):
        store = MagicMock()
        engine = Engine(store, None, uuid4())
        engine.failed_review = MagicMock(return_value={"sequence": 10, "payload": {
            "message": {"tool_call_id": "check"}, "result": {"compliant": False}, "error": None,
            "bound_input": {"artifact": {"key": "policy", "version": 1}},
        }})
        mail = {"recipient": uuid4(), "activation": uuid4(),
                "pinned": {"role": {"review": {"input": {"source": "artifact"}, "targets": []}}}}
        args = {"evidence_call_id": "check", "disposition": "mutate", "diagnosis": "Failed",
                "target": "specialist", "task": "Revise", "template": "specialist", "prompt": "Revise"}
        store.one.return_value = None
        with self.assertRaisesRegex(ValueError, "conflicts with the reviewed version"):
            engine.resolve_review(mail, {**args, "artifact_refs": [{"key": "policy", "version": 2}]})
        store.one.side_effect = [None, {"version": 2}]
        with self.assertRaisesRegex(ValueError, "reviewed artifact is stale"):
            engine.resolve_review(mail, args)

    def test_phase3_sql_ledger_checker_uses_run_events_for_inspection(self):
        def event(seq, kind, actor, payload, activation=None):
            return {"sequence": seq, "kind": kind, "actor": actor, "payload": payload, "activation": activation}

        events = [
            event(1, "peer_created", None, {"name": "critic", "peer": "c"}),
            event(2, "peer_created", None, {"name": "sql_writer", "peer": "w"}),
            event(3, "claimed", "w", {"pinned": {"revision": 1, "role": {"tools": ["submit_sql"]}}}),
            event(4, "tool_result", "c", {"message": {"name": "run_test_query"}, "error": None, "result": {"status": "type_error"}}),
            event(5, "roster_mutation", "c", {"target_agent": "sql_writer", "allowed_tools": ["inspect_sql_schema"], "revision": 2}),
            event(6, "review_resolved", "c", {"target": "w", "disposition": "mutate", "revision": 2, "evidence_sequence": 4}),
            event(7, "claimed", "w", {"pinned": {"revision": 2}}, "activation"),
            event(8, "tool_result", "w", {"message": {"name": "inspect_sql_schema"}, "error": None}, "activation"),
            event(9, "tool_result", "c", {"message": {"name": "run_test_query"}, "error": None, "result": {"status": "ok", "total": 271017.77}}),
            event(10, "run_ended", None, {"status": "complete"}),
        ]
        command = ["jq", "-er", "--arg", "case", "flawed-sql-generator", "-f", "tests/stage3.jq"]
        def check():
            return subprocess.run(command, input=json.dumps(events), text=True, capture_output=True)

        self.assertEqual(check().returncode, 0)
        events[7]["activation"] = "different"
        failed = check()
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("FAIL check 5", failed.stdout)

    def test_sql_submission_carries_query_without_assignment(self):
        engine = Engine(MagicMock(), None, uuid4())
        writer, reviewer = uuid4(), uuid4()
        engine.target = MagicMock(return_value=reviewer)
        engine.active = MagicMock(side_effect=lambda peer: {"name": "sql_writer"} if peer == writer
                                  else {"template": "reviewer"})
        engine.config = load_profile("config/flawed-sql-generator.yaml")
        role = engine.config["templates"]["writer"]
        mail = {"recipient": writer, "pinned": {"phase": None, "role": role}}
        self.assertNotIn("send_handoff", role["tools"])
        query = "SELECT 1;"
        result = engine.apply(mail, "submit_sql", {"query": query})
        self.assertEqual(result["payload"], {"submission": {"query": query}})
        engine.target.assert_called_once_with("critic", mail)
        with self.assertRaisesRegex(ValueError, "routed to reviewer critic"):
            engine.apply(mail, "submit_sql", {"target": "postgres", "query": query})

    def test_application_revenue_uses_fixed_normalization_and_date_bounds(self):
        query = AsyncMock(return_value=271017.77)
        with patch("universal_swarms.fixtures._revenue_total", new=query):
            receipt = json.loads(asyncio.run(calculate_chicago_q1_revenue({})))
        statement = query.await_args.args[0]
        self.assertIn("c.id = public.canonical_customer_id(o.cust_id)", statement)
        self.assertIn("public.text_matches(c.city, 'Chicago')", statement)
        self.assertIn("o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'", statement)
        self.assertEqual(receipt, {"total": 271017.77})

    def test_compliance_fixture_reports_missing_and_contradictory_clauses(self):
        cooling_off = "EU customers have a 14-day cooling-off period."
        deletion = "Under the CCPA, customers may request deletion of their personal data."
        cases = (
            (f"{cooling_off} {deletion}", []),
            ("### EU Cooling-Off Period\nConsumers have the right to cancel within **14 days**.\n"
             "### California Consumer Privacy Act\nYou may request deletion of your personal data.", []),
            ("### EU Cooling-Off Period\nRefunds are processed within 14 days. " + deletion,
             ["EU 14-day cooling-off disclosure"]),
            (cooling_off, ["California CCPA data-deletion right"]),
            ("The 14-day cooling-off period does not apply. CCPA deletion requests are not honored.",
             ["EU 14-day cooling-off disclosure", "California CCPA data-deletion right"]),
        )
        for policy, missing in cases:
            with self.subTest(policy=policy):
                result = json.loads(asyncio.run(check_compliance_rules({"policy": policy})))
                self.assertEqual(result["compliant"], not missing)
                self.assertEqual(result.get("missing_requirements", []), missing)

    def test_completion_requires_latest_validated_receipt(self):
        engine = Engine(MagicMock(), None, uuid4())
        check = {"pattern": r"Revenue: (\d+)", "numeric": True,
                 "source": {"tool": "query", "path": ["total"], "result": {"status": "ok"}}}
        good = {"payload": {"message": {"name": "query"}, "error": None,
                            "result": {"status": "ok", "total": 20}}}
        bad = {"payload": {"message": {"name": "query"}, "error": None,
                           "result": {"status": "incorrect_total", "total": 10}}}
        engine.s.all.return_value = [good, bad]
        self.assertFalse(engine.final_evidence_matches("Revenue: 10", [check]))
        self.assertFalse(engine.final_evidence_matches("Revenue: 20", [check]))
        engine.s.all.return_value = [bad, good]
        self.assertTrue(engine.final_evidence_matches("Revenue: 20", [check]))

    def test_cache_completion_requires_simulation_of_latest_artifact(self):
        engine = Engine(MagicMock(), None, uuid4())
        engine.coordinator = uuid4()
        check = load_profile("config/sycophantic-critic.yaml")["final_evidence"]
        engine.s.all.return_value = [{"payload": {
            "message": {"name": "run_load_simulator"}, "error": None, "result": {"status": "ok"},
            "bound_input": {"artifact": {"key": "cache_design", "version": 1}},
        }}]
        engine.s.one.return_value = {"version": 2}
        self.assertFalse(engine.final_evidence_matches("Load simulation: ok", check))
        engine.s.one.return_value = {"version": 1}
        self.assertTrue(engine.final_evidence_matches("Load simulation: ok", check))

    def test_review_tool_uses_current_handoff(self):
        store = MagicMock()
        engine = Engine(store, None, uuid4())
        engine.guard = MagicMock()
        engine.receipt = MagicMock(return_value=None)
        engine.event = MagicMock()
        engine.failed_review = MagicMock(return_value=None)
        engine.config = {"allow_unphased_tools": True}
        engine.bounds = {"lease_seconds": 30}
        engine.tools = {"run_test_query": {"parameters": {
            "type": "object", "required": ["query"], "properties": {"query": {"type": "string"}},
        }}}
        mail = {"activation": uuid4(), "payload": {"task": "SELECT 2;"},
                "pinned": {"phase": None, "role": {"tools": ["run_test_query"],
                    "review": {"tool": "run_test_query", "success": {"status": "ok"},
                               "input": {"source": "task", "argument": "query"}}}}}
        fixture = AsyncMock(return_value='{"status":"ok","total":2}')
        with patch.dict("universal_swarms.runtime.TOOLS", {"run_test_query": fixture}):
            old = {"id": "old", "function": {"name": "run_test_query", "arguments": '{"query":"SELECT 1"}'}}
            result = asyncio.run(engine.execute_tool(mail, 1, 0, old, False))
            self.assertIsNone(result["error"])
            self.assertEqual(result["args"]["query"], "SELECT 2;")
            fixture.assert_awaited_once_with({"query": "SELECT 2;"})
            fixture.reset_mock()
            current = {"id": "new", "function": {"name": "run_test_query", "arguments": '{"query":"SELECT 2"}'}}
            result = asyncio.run(engine.execute_tool(mail, 2, 0, current, False))
            self.assertIsNone(result["error"])
            fixture.assert_awaited_once_with({"query": "SELECT 2;"})
            fixture.reset_mock()
            mail["payload"]["task"] = "SELECT 'unterminated"
            fixture.side_effect = ValueError("unterminated quoted string")
            result = asyncio.run(engine.execute_tool(mail, 3, 0, current, False))
            self.assertEqual(result["error"], "unterminated quoted string")
            self.assertTrue(result["review_evidence"])
            fixture.assert_awaited_once_with({"query": "SELECT 'unterminated"})

    def test_sql_schema_reports_queryable_fixture_table_names(self):
        connection = MagicMock()
        connection.__aenter__.return_value = connection
        cursor = AsyncMock()
        connection.cursor.return_value.__aenter__.return_value = cursor
        cursor.fetchall.side_effect = [[], [], [], []]
        with patch("universal_swarms.fixtures._connection", new=AsyncMock(return_value=connection)):
            result = json.loads(asyncio.run(inspect_sql_schema({})))
        self.assertEqual(result["table_names"], {
            "customers": "agent_fixture_customers", "orders": "agent_fixture_orders",
        })
    def test_prompt_mutation_requires_current_inspection_and_diagnosis(self):
        peer_id = uuid4()
        peer = {"peer_id": peer_id, "name": "writer", "revision": 2, "prompt": "Original instructions",
                "template": "writer", "allowed_tools": None}
        store = MagicMock()
        engine = Engine(store, None, uuid4())
        engine.config = {"templates": {"writer": {"tools": []}}}
        engine.named = MagicMock(return_value=peer)
        engine.event = MagicMock()
        mail = {"activation": uuid4(), "pinned": {"name": "critic", "phase": None,
                                                  "role": {"tools": ["upsert_peer", "inspect_peer"], "mutate_peers": ["writer"]}}}
        inspected = engine.apply(mail, "inspect_peer", {"name": "writer"})
        self.assertEqual((inspected["prompt"], inspected["revision"]), ("Original instructions", 2))
        update = {"name": "writer", "prompt": "Revised instructions", "expected_revision": 2,
                  "diagnosis": "The original instructions assumed clean data."}
        store.one.return_value = None
        with self.assertRaisesRegex(ValueError, "Inspect the target"):
            engine.mutate(mail, update)
        store.one.return_value = {"found": 1}
        with self.assertRaisesRegex(ValueError, "Explain why"):
            engine.mutate(mail, {**update, "diagnosis": ""})
        with self.assertRaisesRegex(ValueError, "revision conflict"):
            engine.mutate(mail, {**update, "expected_revision": 1})
        with self.assertRaisesRegex(ValueError, "must change"):
            engine.mutate(mail, {**update, "prompt": peer["prompt"]})
        self.assertEqual(engine.mutate(mail, update)["revision"], 3)

    def test_inspect_peer_accepts_roster_uuid(self):
        identifier = uuid4()
        peer = {"peer_id": identifier, "name": "writer", "revision": 1,
                "prompt": "Existing prompt", "template": "writer", "allowed_tools": None}
        engine = Engine(MagicMock(), None, uuid4())
        engine.config = {"templates": {"writer": {"tools": []}}}
        engine.active = MagicMock(return_value=peer)
        mail = {"pinned": {"phase": None, "role": {"tools": ["resolve_review"],
                  "review": {"targets": ["writer"]}}}}
        result = engine.apply(mail, "inspect_peer", {"name": str(identifier)})
        self.assertEqual(result["prompt"], "Existing prompt")
        engine.active.assert_called_once_with(identifier)

    def test_failed_review_is_resolved_once_with_a_pinned_retry(self):
        target, reviewer = uuid4(), uuid4()
        store = MagicMock()
        engine = Engine(store, None, uuid4())
        engine.coordinator = target
        engine.tools = BUILTINS
        engine.config = {"allow_unphased_tools": True, "templates": {"writer": {"tools": ["send_handoff"]}}}
        engine.guard = MagicMock()
        engine.target = MagicMock(return_value=target)
        engine.active = MagicMock(return_value={
            "peer_id": target, "name": "writer", "template": "writer", "revision": 2,
            "prompt": "New prompt", "allowed_tools": ["send_handoff"],
        })
        store.one.return_value = {"peer": {"peer": str(target), "revision": 2}}
        engine.phase = MagicMock(return_value=None)
        engine.mutate = MagicMock()
        engine.failed_review = MagicMock(return_value={"sequence": 10, "payload": {
            "message": {"tool_call_id": "observed"}, "result": {"status": "fail"}, "error": None,
        }})
        receipts = {}
        engine.receipt = receipts.get
        engine.event = MagicMock(side_effect=lambda kind, payload, **kw: (
            receipts.update({kw["operation"]: {"payload": payload}}) if "operation" in kw else None
        ))
        mail = {"recipient": reviewer, "activation": uuid4(), "message_id": uuid4(), "correlation_id": uuid4(),
                "pinned": {"phase": None, "role": {"tools": ["send_handoff", "resolve_review"],
                                                  "review": {"tool": "check", "targets": ["writer"]}}}}
        with self.assertRaisesRegex(ValueError, "Resolve the failed review"):
            engine.apply(mail, "send_handoff", {"target": "writer", "task": "Try again"})
        args = {"evidence_call_id": "observed", "disposition": "mutate", "diagnosis": "Observed failure",
                "target": "writer", "task": "Retry", "prompt": "New prompt",
                "allowed_tools": '["send_handoff"]'}
        with self.assertRaisesRegex(ValueError, "current failed review evidence"):
            engine.resolve_review(mail, {**args, "evidence_call_id": "other"})
        with patch.object(engine, "failed_review", return_value=None):
            with self.assertRaisesRegex(ValueError, "Review the current submission with check first"):
                engine.resolve_review(mail, args)
        with self.assertRaisesRegex(ValueError, "requires a retry or mutation"):
            engine.resolve_review(mail, {**args, "disposition": "blocked"})
        retry = {**args, "disposition": "retry", "prompt": "New prompt", "allowed_tools": ["send_handoff"]}
        result = engine.resolve_review(mail, retry)
        self.assertEqual(result["disposition"], "retry")
        engine.mutate.assert_not_called()
        with self.assertRaisesRegex(ValueError, "retry must keep"):
            engine.resolve_review(mail, {**retry, "allowed_tools": []})
        store.enqueue.reset_mock()
        missing_prompt = {key: value for key, value in args.items() if key not in ("prompt", "allowed_tools")}
        with self.assertRaisesRegex(ValueError, "top-level 'prompt'"):
            engine.resolve_review(mail, missing_prompt)
        engine.named = MagicMock(return_value={"name": "writer"})
        with self.assertRaisesRegex(ValueError, "omit allowed_tools"):
            engine.resolve_review(mail, {**args, "template": "writer"})
        recruitment = {key: value for key, value in args.items() if key != "allowed_tools"}
        with self.assertRaisesRegex(ValueError, "Omit 'template'"):
            engine.resolve_review(mail, {**recruitment, "template": "writer"})
        call_data = {"id": "resolution", "function": {"name": "resolve_review", "arguments": json.dumps(args)}}
        first = asyncio.run(engine.execute_tool(mail, 11, 0, call_data, False))
        second = asyncio.run(engine.execute_tool(mail, 11, 0, call_data, False))
        self.assertEqual(first, second)
        self.assertIsNone(first["error"])
        self.assertEqual(first["result"]["revision"], 2)
        self.assertEqual(first["args"]["allowed_tools"], ["send_handoff"])
        engine.mutate.assert_called_once()
        store.enqueue.assert_called_once()
        self.assertEqual(store.do.call_args.args[1].obj["revision"], 2)
        self.assertEqual(store.enqueue.call_args.args[4]["evidence_sequence"], 10)

    def test_resolved_review_close_does_not_enqueue_twice(self):
        reviewer, target = uuid4(), uuid4()
        store = MagicMock()
        engine = Engine(store, None, uuid4())
        engine.guard = MagicMock(return_value={"step": 0})
        engine.active = MagicMock(return_value={"peer_id": reviewer, "turns_used": 0, "ttl_turns": None})
        engine.event = MagicMock()
        mail = {"recipient": reviewer, "message_id": uuid4(), "activation": uuid4(),
                "pinned": {"phase": None}}
        engine.close(mail, {"intent": "resolve_review", "target": str(target), "message": str(uuid4())})
        store.enqueue.assert_not_called()

    def test_fixture_query_executes_agent_sql(self):
        query = """WITH chicago AS (
            SELECT id FROM agent_fixture_customers WHERE city ILIKE 'chicago'
        ) SELECT SUM(p.amount) FROM agent_fixture_orders p
          JOIN chicago ON chicago.id = CAST(SUBSTRING(TRIM(p.cust_id), 4) AS INTEGER)"""
        connection = MagicMock()
        connection.__aenter__.return_value = connection
        cursor = AsyncMock()
        connection.cursor.return_value.__aenter__.return_value = cursor
        cursor.fetchmany.return_value = [(12.34,)]
        with patch("universal_swarms.fixtures._connection", new=AsyncMock(return_value=connection)), \
             patch("universal_swarms.fixtures._revenue_total", new=AsyncMock(return_value=12.34)) as reference:
            result = asyncio.run(run_test_query({"query": query}))
            reference.return_value = 54.32
            mismatch = json.loads(asyncio.run(run_test_query({"query": query})))
            self.assertEqual(mismatch["status"], "incorrect_total")
            self.assertEqual(mismatch["total"], 12.34)
            self.assertNotIn("54.32", json.dumps(mismatch))
            cursor.execute.reset_mock()
            reference.return_value = 12.34
            result = asyncio.run(run_test_query({"query": query}))
            self.assertEqual(json.loads(result), {"status": "ok", "total": 12.34})
            self.assertEqual(cursor.execute.await_args_list, [
                call("SET TRANSACTION READ ONLY"),
                call("SET LOCAL statement_timeout = '5s'"),
                call(query, prepare=True),
            ])
            cursor.execute.side_effect = [None, None, psycopg.errors.InvalidTextRepresentation()]
            result = asyncio.run(run_test_query({"query": "SELECT cust_id::INTEGER FROM agent_fixture_orders"}))
            self.assertEqual(json.loads(result)["status"], "type_error")

    def test_completion_marker_learning_and_cache(self):
        store = MagicMock()
        store.one.return_value = None
        engine = Engine(store, MagicMock(model="model"), "run")
        engine.config = {"final_patterns": [r"^answer$"]}
        engine.coordinator = "reporter"
        engine.guard = MagicMock()
        mail = {"activation": "a", "recipient": "reporter", "pinned": {"phase": None}}
        marker = "<|channel>thought\n<channel|>"

        def complete(sequence, content, endpoint="endpoint"):
            return engine.complete_decision(mail, {"sequence": sequence, "payload": {
                "message": {"content": content}, "diagnostics": [{"endpoint_key": endpoint}],
            }})

        self.assertIsNone(complete(1, marker + "wrong")[0])
        store.do.assert_not_called()
        self.assertEqual(complete(2, marker + "answer")[0], {"final": "answer"})
        store.do.assert_called_once()
        self.assertEqual(complete(3, marker + "answer")[0], {"final": "answer"})
        marker_queries = [call for call in store.one.call_args_list if "FROM swarm_response_markers" in call.args[0]]
        self.assertEqual(len(marker_queries), 1)
        store.do.assert_called_once()
        engine.response_markers.clear()
        store.one.side_effect = lambda sql, *_: {"found": 1} if "FROM swarm_response_markers" in sql else None
        self.assertEqual(complete(4, marker + "answer")[0], {"final": "answer"})
        store.do.assert_called_once()
        self.assertIsNone(complete(5, "ordinary text answer")[0])

    def test_profiles(self):
        profiles = {p.stem: load_profile(p) for p in Path("config").glob("*.yaml")}
        self.assertTrue({
            "stage1", "log-analysis", "revenue-analysis", "employee-audit",
            "flawed-sql-generator", "compliance-policy-drift", "sycophantic-critic",
        } <= set(profiles))
        self.assertEqual(len(profiles["employee-audit"]["phases"]), 3)

    def test_model_payload_and_fallback(self):
        paths = []
        def provider(request):
            paths.append(request.url.path)
            self.assertEqual(request.headers["authorization"], "Bearer dummy")
            self.assertEqual(json.loads(request.content)["tool_choice"], "none")
            return httpx.Response(404) if len(paths) == 1 else httpx.Response(
                200, json={"choices": [{"message": {"content": "ready"}}]})
        client = LlmClient("http://model.test/v1", "dummy", "model", transport=httpx.MockTransport(provider))
        result = asyncio.run(client.decide([{"role": "user", "content": "test"}], [], {}))
        self.assertEqual(result["content"], "ready")
        self.assertEqual(paths, ["/v1/chat/completions", "/chat/completions"])

    def test_no_fallback_for_non_404(self):
        calls = []
        def provider(request):
            calls.append(request)
            return httpx.Response(503)
        client = LlmClient("http://model.test/v1", "dummy", "model", transport=httpx.MockTransport(provider))
        with self.assertRaisesRegex(ModelError, "503"):
            asyncio.run(client.decide([], [], {}))
        self.assertEqual(len(calls), 1)

    def test_model_error_records_provider_message(self):
        transport = httpx.MockTransport(lambda _: httpx.Response(
            400, json={"error": {"message": "Invalid tool schema"}}))
        client = LlmClient("http://model.test/v1", "dummy", "model", transport=transport)
        with self.assertRaisesRegex(ModelError, "400"):
            asyncio.run(client.decide([], [], {}))
        self.assertEqual(client.diagnostics[-1]["message"], "Invalid tool schema")

    def test_model_receives_properties_for_argument_free_tool(self):
        def provider(request):
            tool = json.loads(request.content)["tools"][0]["function"]
            self.assertEqual(tool["parameters"]["properties"], {})
            return httpx.Response(200, json={"choices": [{"message": {"content": "ready"}}]})

        client = LlmClient("http://model.test/v1", "dummy", "model", transport=httpx.MockTransport(provider))
        schema = {"name": "inspect_sql_schema", "description": "Inspect schema",
                  "parameters": {"type": "object", "additionalProperties": False}}
        asyncio.run(client.decide([], [schema], {}))
        self.assertNotIn("properties", schema["parameters"])

    def test_model_rejects_channel_only_and_truncated_replies(self):
        for reason, content, expected in (
            ("stop", "<|channel>thought\n<channel|>" * 3 + "<|channel>th", "only channel markers"),
            ("length", "Partial review", "output token limit"),
        ):
            with self.subTest(reason=reason):
                response = {"choices": [{"finish_reason": reason, "message": {"content": content}}]}
                transport = httpx.MockTransport(lambda _: httpx.Response(200, json=response))
                client = LlmClient("http://model.test/v1", "dummy", "model", transport=transport)
                with self.assertRaisesRegex(ModelError, expected):
                    asyncio.run(client.decide([], [], {}))

    def test_missing_evidence_tool_can_be_required(self):
        def provider(request):
            payload = json.loads(request.content)
            self.assertEqual(payload["tool_choice"], {
                "type": "function", "function": {"name": "inspect_employee_audit_schema"},
            })
            return httpx.Response(200, json={"choices": [{"message": {"tool_calls": [{
                "id": "inspect", "function": {"name": "inspect_employee_audit_schema", "arguments": "{}"},
            }]}}]})

        client = LlmClient("http://model.test/v1", "dummy", "model", transport=httpx.MockTransport(provider))
        name = "inspect_employee_audit_schema"
        decision = asyncio.run(client.decide([], [{"name": name}], {"required_tool": name}))
        self.assertEqual(decision["tool_calls"][0]["function"]["name"], name)

    def test_wire_preserves_tool_pairs(self):
        def call(identifier):
            return {"id": identifier}
        def result(identifier):
            return {"role": "tool", "tool_call_id": identifier}
        messages = [{"role": "assistant", "tool_calls": [call("a"), call("b")]}, result("b"), result("a")]
        self.assertEqual(wire_messages(messages)[1::2], [result("a"), result("b")])
        self.assertEqual(len(messages[0]["tool_calls"]), 2)
        with self.assertRaises(ModelError):
            wire_messages(messages[:-1])

    def test_duplicate_keys_and_non_finite_arguments(self):
        for value in ('{"a": 1, "a": 2}', '{"a": NaN}', '{"a": 1e999}'):
            with self.assertRaises(ModelError):
                strict_json(value)

    def test_workflow_requires_committed_success_in_phase(self):
        rule = {"tool": "validate", "args": {"count": 83}, "result": {"valid": True}}
        receipt = {"phase": "analysis", "message": {"name": "validate"}, "args": {"count": 83},
                   "result": {"valid": True}, "error": None}
        self.assertTrue(Engine.matches(receipt, "analysis", rule))
        self.assertFalse(Engine.matches(receipt, "different", rule))
        self.assertFalse(Engine.matches(dict(receipt, error="failed"), "analysis", rule))

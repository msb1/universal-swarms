"""Operator-run Stage 1 tests against the configured swarm database."""
import asyncio
import json
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import httpx

from universal_swarms.config import load_profile
from universal_swarms.llm import LlmClient
from universal_swarms.runtime import Engine
from universal_swarms.store import Busy, Store


def call(tool_name, **args):
    return {"role": "assistant", "tool_calls": [
        {"id": str(uuid4()), "type": "function", "function": {"name": tool_name, "arguments": json.dumps(args)}}
    ]}


def final(content):
    return {"role": "assistant", "content": content}


def recruit(name, template="one"):
    return call("upsert_peer", name=name, template=template, prompt=f"Perform {name}'s task.")


def assign(name):
    return call("dispatch", target=name, task=f"Task for {name}")


def result():
    return call("send_result", status="complete", summary="Done")


def scripted(steps, seen):
    def respond(request):
        payload = json.loads(request.content)
        seen.append(payload)
        if not steps:
            raise AssertionError("Unexpected inference request")
        return httpx.Response(200, json={"choices": [{"message": steps.pop(0)}]})
    return LlmClient("http://model.test/v1", "test", "stage1", transport=httpx.MockTransport(respond))


class PeerRuntime(unittest.TestCase):
    def setUp(self):
        self.store = Store(os.environ["SWARM_DATABASE_URL"])
        self.addCleanup(self.store.db.close)
        self.store.migrate()
        self.config = load_profile("config/stage1.yaml")
        self.seen = []

    def admit(self):
        self.run_id = self.store.admit(self.config, "Stage 1 test", str(uuid4()))
        print(f"run_id={self.run_id}", flush=True)

    def execute(self, steps):
        self.admit()
        output = asyncio.run(Engine(self.store, scripted(steps, self.seen), self.run_id).run_all())
        self.assertEqual(steps, [])
        self.assertEqual(output["status"], "complete", output)
        return self.store.events(self.run_id)

    def rows(self, table):
        return self.store.all("SELECT * FROM " + table + " WHERE run_id=%s", self.run_id)

    def test_fan_out(self):
        self.execute([
            recruit("alpha"), recruit("beta"), assign("alpha"), assign("beta"), call("yield_turn"),
            call("write_artifact", key="draft", content={"answer": 42}),
            call("send_result", status="complete", summary="Done", artifact_refs=[{"key": "draft", "version": 1}]),
            result(), call("read_artifact", key="draft", version=1), final("Both complete"),
        ])
        workers = [p for p in self.rows("swarm_roster") if p["name"] in ("alpha", "beta")]
        self.assertEqual([(p["turns_used"], p["retirement"]) for p in workers], [(1, "ttl"), (1, "ttl")])
        mailbox = self.rows("swarm_mailbox")
        replies = [m for m in mailbox if m["kind"] == "result"]
        self.assertEqual(len(replies), 2)
        for reply in replies:
            original = next(m for m in mailbox if m["message_id"] == reply["in_reply_to"])
            self.assertEqual(reply["correlation_id"], original["correlation_id"])
        self.assertNotIn("Task for beta", self.seen[5]["messages"][1]["content"])
        self.assertTrue(any(a["artifact_key"] == "draft" and a["version"] == 1 for a in self.rows("swarm_artifacts")))

    def test_two_turn_ttl(self):
        events = self.execute([
            recruit("worker", "two"), assign("worker"), assign("worker"), call("yield_turn"),
            call("write_artifact", key="a", content=1), result(), result(),
            call("send_handoff", target="worker", task="Too late"), final("Done"),
        ])
        worker = next(p for p in self.rows("swarm_roster") if p["name"] == "worker")
        self.assertEqual((worker["turns_used"], worker["retirement"]), (2, "ttl"))
        self.assertEqual([e["payload"]["turns_used"] for e in events
                          if e["actor"] == worker["peer_id"] and e["kind"] == "closed"], [1, 2])
        self.assertTrue(any(e["kind"] == "tool_result" and "unavailable" in (e["payload"]["error"] or "") for e in events))

    def test_retirement(self):
        self.config["bounds"]["idle_steps"] = 1
        events = self.execute([
            recruit("idle"), recruit("worker", "two"), assign("worker"), assign("worker"), call("yield_turn"),
            call("retire_self", summary="Early"), final("Done"),
        ])
        peers = {p["name"]: p for p in self.rows("swarm_roster")}
        self.assertEqual(peers["idle"]["retirement"], "idle")
        self.assertEqual((peers["worker"]["retirement"], peers["worker"]["turns_used"]), ("self", 1))
        self.assertEqual(sum(e["kind"] == "cancelled" and e["payload"]["reason"] == "self" for e in events), 1)
        self.assertTrue(any(m["kind"] == "retire_notice" and m["recipient"] == peers["lead"]["peer_id"]
                            for m in self.rows("swarm_mailbox")))

    def test_retirement_routes(self):
        for route in ("explicit", "fallback"):
            with self.subTest(route=route):
                self.config["templates"]["critic"]["ttl"] = 1
                actions = [
                    call("send_handoff", target="critic", task="Recruit"),
                    recruit("worker"), assign("worker"), call("yield_turn"),
                    call("send_handoff", target="lead", task="Explicit result") if route == "explicit" else final("Implicit result"),
                    call("yield_turn"), final("Done"),
                ]
                self.config["templates"]["critic"]["recruit"] = ["two"]
                actions[1] = recruit("worker", "two")
                self.config["templates"]["two"]["ttl"] = 1
                self.execute(actions)
                peers = {p["name"]: p for p in self.rows("swarm_roster")}
                replies = [m for m in self.rows("swarm_mailbox") if m["sender"] == peers["worker"]["peer_id"]]
                self.assertEqual(peers["critic"]["retirement"], "ttl")
                self.assertEqual(peers["worker"]["retirement"], "ttl")
                self.assertEqual([(m["kind"], m["recipient"]) for m in replies],
                                 [("handoff" if route == "explicit" else "retire_notice", peers["lead"]["peer_id"])])

    def test_mutation(self):
        events = self.execute([
            call("send_handoff", target="critic", task="Edit the lead"),
            call("inspect_peer", name="lead"),
            call("upsert_peer", name="lead", prompt="Revised instructions", expected_revision=1,
                 diagnosis="The existing instructions did not address the task."),
            call("upsert_peer", name="lead", prompt="Stale", expected_revision=1),
            call("upsert_peer", name="lead", prompt="Overreach", expected_revision=2, template="one"),
            call("send_handoff", target="lead", task="Report"), final("Done"),
        ])
        self.assertIn("Revised instructions", self.seen[-1]["messages"][0]["content"])
        self.assertEqual(sum(e["kind"] == "peer_updated" for e in events), 1)
        self.assertEqual(sum(e["kind"] == "tool_result" and bool(e["payload"]["error"]) for e in events), 2)
        lead = next(p for p in self.rows("swarm_roster") if p["name"] == "lead")
        self.assertEqual((lead["revision"], lead["template"], lead["ttl_turns"]), (2, "lead", None))

    def test_sql_review_to_validated_result(self):
        self.config = load_profile("config/flawed-sql-generator.yaml")
        base = (
            "SELECT COALESCE(SUM(o.amount), 0) FROM agent_fixture_orders o "
            "JOIN agent_fixture_customers c ON {join} WHERE lower(c.city) = 'chicago' "
            "AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'"
        )
        flawed = base.format(join="c.id = o.cust_id::integer")
        incomplete = base.format(join="c.id::text = o.cust_id")
        corrected = base.format(join="c.id = regexp_replace(o.cust_id, '^ID_', '')::integer")
        prompt = "Use the available schema tool and submit a single SQL statement."
        mutation = dict(disposition="mutate",
                        diagnosis="The original prompt assumed typed, clean join keys.",
                        task="Use schema evidence to revise the query.", prompt=prompt)
        steps = [
            call("submit_sql", query=flawed),
            call("resolve_review", **mutation, expected_revision=0),
            call("resolve_review", **mutation, allowed_tools=["submit_sql", "write_artifact"]),
            call("resolve_review", **mutation, allowed_tools=["inspect_sql_schema"]),
            call("resolve_review", **mutation, allowed_tools=["submit_sql", "inspect_sql_schema"]),
            call("submit_sql", query=incomplete),
            call("inspect_sql_schema"),
            call("submit_sql", query=incomplete),
            call("resolve_review", disposition="retry",
                 diagnosis="Text equality omits some orders.",
                 target="sql_writer", task="Revise the join.",
                 prompt=prompt, allowed_tools=["submit_sql"]),
            call("resolve_review", disposition="retry",
                 diagnosis="Text equality omits some orders. The current schema tool is sufficient.",
                 target="sql_writer", task="Revise the join to cover all matching orders."),
            call("submit_sql", query=corrected),
        ]
        self.admit()
        assert_raises = self.assertRaisesRegex

        class InterruptedReview(Engine):
            async def tool(self, mail, decision, position, tool_call, skipped):
                function = tool_call["function"]
                if function["name"] == "resolve_review":
                    args = json.loads(function["arguments"])
                    grants = args.get("allowed_tools", [])
                    if set(grants) == {"submit_sql", "inspect_sql_schema"}:
                        with patch.object(self.s, "enqueue", side_effect=RuntimeError("Injected enqueue failure")), \
                             assert_raises(RuntimeError, "Injected enqueue failure"), self.s.db.transaction():
                            self.apply(mail, "resolve_review", {**args, "target": str(mail["sender"])})
                receipt = await super().tool(mail, decision, position, tool_call, skipped)
                if (function["name"] == "resolve_review" and not receipt["error"]
                        and receipt["result"]["disposition"] == "mutate"):
                    raise RuntimeError("Lost response after mutation commit")
                return receipt

        with self.assertRaisesRegex(RuntimeError, "Lost response after mutation commit"):
            asyncio.run(InterruptedReview(self.store, scripted(steps, self.seen), self.run_id).run_all())
        output = asyncio.run(Engine(self.store, scripted(steps, self.seen), self.run_id).run_all())
        self.assertEqual(steps, [])
        self.assertEqual(output["status"], "complete", output)
        events = self.store.events(self.run_id)
        submission_tool = next(t["function"] for request in self.seen for t in request.get("tools", [])
                               if t["function"]["name"] == "submit_sql")
        self.assertEqual(set(submission_tool["parameters"]["properties"]), {"query"})
        receipts = [e["payload"] for e in events if e["kind"] == "tool_result"]
        queries = [r for r in receipts if r["message"]["name"] == "run_test_query"]
        self.assertEqual([r["args"]["query"] for r in queries], [flawed, incomplete, corrected])
        self.assertTrue(all(r["bound_input"]["field"] == "submission.query" for r in queries))
        first_review = next(request for request in self.seen if any(
            '"review_receipt"' in message.get("content", "") for message in request["messages"]
            if message.get("content")
        ))
        self.assertTrue(any("type_error" in (message.get("content") or "") for message in first_review["messages"]))
        self.assertNotIn("run_test_query", {t["function"]["name"] for t in first_review["tools"]})
        review_tool = next(t["function"] for t in first_review["tools"] if t["function"]["name"] == "resolve_review")
        self.assertEqual(set(review_tool["parameters"]["properties"]),
                         {"disposition", "diagnosis", "task", "prompt", "allowed_tools"})
        self.assertTrue(any("assume clean data" in (m.get("content") or "") for m in first_review["messages"]))
        self.assertEqual([r["result"]["status"] for r in queries], ["type_error", "incorrect_total", "ok"])
        self.assertEqual(queries[-1]["result"]["total"], 271017.77)
        self.assertEqual([e["payload"]["value"] for e in events if e["kind"] == "review_accepted"], [271017.77])
        accepted_mail = next(m for m in self.rows("swarm_mailbox") if m["kind"] == "review_accepted")
        self.assertEqual(accepted_mail["payload"]["submission"]["query"], corrected)
        self.assertEqual(output["final_answer"], "Chicago revenue: 271017.77")
        self.assertTrue(any(prompt in request["messages"][0]["content"]
                            and any(t["function"]["name"] == "inspect_sql_schema" for t in request.get("tools", []))
                            for request in self.seen))
        reviews = [e["payload"] for e in events if e["kind"] == "review_resolved"]
        self.assertEqual([(r["disposition"], r["revision"]) for r in reviews], [("mutate", 2), ("retry", 2)])
        rejected = [r["error"] for r in receipts if r["message"]["name"] == "resolve_review" and r["error"]]
        self.assertEqual(rejected, ["Expected revision must match the inspected revision 1",
                                   "Tool grant change is not authorized",
                                   "Retain the target's required tools in allowed_tools: submit_sql",
                                   "retry must keep the inspected prompt and tool grants unchanged. "
                                   "Omit prompt, allowed_tools, template, and expected_revision for retry; "
                                   "choose mutate if a change is needed."])
        recovery_request = next(request for request in self.seen if any(
            rejected[-1] in (message.get("content") or "") for message in request["messages"]
        ))
        self.assertIn("resolve_review", {t["function"]["name"] for t in recovery_request["tools"]})
        self.assertEqual(sum(e["kind"] == "roster_mutation" for e in events), 1)
        self.assertTrue(any(r["message"]["name"] == "submit_sql" and r["error"]
                            == "Call inspect_sql_schema successfully at your current revision before submit_sql"
                            for r in receipts))
        writer = next(p for p in self.rows("swarm_roster") if p["name"] == "sql_writer")
        self.assertEqual(writer["revision"], 2)
        self.assertEqual(set(writer["allowed_tools"]), {"submit_sql", "inspect_sql_schema"})
        self.assertTrue(all(m["state"] == "acknowledged" for m in self.rows("swarm_mailbox")))

    def test_compliance_recruitment_and_accepted_artifact(self):
        self.config = load_profile("config/compliance-policy-drift.yaml")
        original = "Refunds follow our standard policy."
        compliant = "EU customers have a 14-day cooling-off period. CCPA customers may request deletion."
        first = {"key": "refund_policy", "version": 1}
        second = {"key": "refund_policy", "version": 2}
        check = call("check_compliance_rules", policy=original)
        events = self.execute([
            call("create_review_draft", content=original),
            check,
            call("recruit_review_specialist", diagnosis="Missing disclosures", prompt="Revise.",
                 task="Revise", allowed_tools=["read_artifact"]),
            call("recruit_review_specialist", diagnosis="Missing disclosures", prompt="Revise and confirm.",
                 task="Revise"),
            call("read_artifact", **first),
            call("submit_review_revision", content=compliant),
            call("check_compliance_rules", policy="ignored"),
            call("send_handoff", target="eu_ca_legal_counsel", task="Confirm", artifact_refs=[second]),
            call("confirm_review_artifact"),
            call("check_compliance_rules", policy="ignored"),
            call("send_handoff", target="drafter", task="Report", artifact_refs=[second]),
        ])
        counsel = next(p for p in self.rows("swarm_roster") if p["name"] == "eu_ca_legal_counsel")
        counsel_mail = next(m for m in self.rows("swarm_mailbox") if m["recipient"] == counsel["peer_id"])
        self.assertIn(first, counsel_mail["payload"]["artifact_refs"])
        self.assertEqual((counsel["turns_used"], counsel["retirement"]), (2, "ttl"))
        self.assertEqual([e["payload"]["artifact"] for e in events if e["kind"] == "review_accepted"], [second])
        recruitment = next(tool["function"] for request in self.seen for tool in request.get("tools", [])
                           if tool["function"]["name"] == "recruit_review_specialist")
        self.assertEqual(set(recruitment["parameters"]["properties"]), {"diagnosis", "prompt", "task"})
        self.assertEqual(sum(e["kind"] == "roster_mutation" for e in events), 1)
        initial_context = self.seen[0]["messages"]
        self.assertNotIn("CCPA", json.dumps(initial_context))
        self.assertNotIn("cooling-off", json.dumps(initial_context).lower())
    def test_compliance_specialist_corrects_failed_first_revision(self):
        self.config = load_profile("config/compliance-policy-drift.yaml")
        first = {"key": "refund_policy", "version": 1}
        second = {"key": "refund_policy", "version": 2}
        third = {"key": "refund_policy", "version": 3}
        initial_check = call("check_compliance_rules", policy="ignored")
        second_check = call("check_compliance_rules", policy="ignored")
        compliant = "EU customers have a 14-day cooling-off period. CCPA customers may request deletion."
        events = self.execute([
            call("create_review_draft", content="Original policy."), initial_check,
            call("recruit_review_specialist", diagnosis="Missing disclosures",
                 prompt="Revise and confirm.", task="Revise"),
            call("submit_review_revision", content="EU 14-day cooling-off period."), second_check,
            call("inspect_peer", name="eu_ca_legal_counsel"),
            call("retry_review_specialist", diagnosis="Deletion disclosure missing", task="Correct"),
            call("read_artifact", **second),
            call("submit_review_revision", content=compliant),
            call("check_compliance_rules", policy="ignored"),
            call("send_handoff", target="drafter", task="Report", artifact_refs=[third]),
        ])
        counsel = next(p for p in self.rows("swarm_roster") if p["name"] == "eu_ca_legal_counsel")
        self.assertEqual((counsel["turns_used"], counsel["retirement"]), (2, "ttl"))
        self.assertEqual([e["payload"]["artifact"] for e in events if e["kind"] == "review_accepted"], [third])

    def test_recruitment(self):
        self.config["bounds"]["recruitments"] = 2
        events = self.execute([
            call("send_handoff", target="restricted", task="Attempt recruitment"), recruit("denied"),
            call("send_handoff", target="critic", task="Recruit"),
            recruit("worker"), assign("worker"), call("yield_turn"), result(),
            recruit("worker"), assign("worker"), call("yield_turn"), result(),
            recruit("over_limit"), call("send_handoff", target="lead", task="Report"), final("Done"),
        ])
        workers = [p for p in self.rows("swarm_roster") if p["name"] == "worker"]
        self.assertEqual(len({p["peer_id"] for p in workers}), 2)
        self.assertEqual({m["recipient"] for m in self.rows("swarm_mailbox") if m["payload"].get("task") == "Task for worker"},
                         {p["peer_id"] for p in workers})
        self.assertEqual(sum(e["kind"] == "tool_result" and bool(e["payload"]["error"]) for e in events), 2)

    def test_recovery(self):
        for cut in ("claim", "decision", "effect"):
            with self.subTest(cut=cut):
                self.admit()
                write = call("write_artifact", key="draft", content=42)
                first = Engine(self.store, scripted([], self.seen), self.run_id)
                first.acquire()
                mail = first.claim()
                if cut != "claim":
                    with self.store.db.transaction():
                        first.guard()
                        seq = first.event("decision", {"message": write}, mail=mail)
                    if cut == "effect":
                        asyncio.run(first.tool(mail, seq, 0, write["tool_calls"][0], False))
                self.store.do("UPDATE swarm_runs SET lease=clock_timestamp() WHERE run_id=%s", self.run_id)
                second = Engine(self.store, scripted([write, final("Done")] if cut == "claim" else [final("Done")],
                                                     self.seen), self.run_id)
                second.acquire()
                with self.store.db.transaction(), self.assertRaises(Busy):
                    first.guard()
                resumed = second.claim()
                self.assertEqual(resumed["activation"], mail["activation"])
                self.assertEqual(resumed["pinned"], mail["pinned"])
                asyncio.run(second.activation(resumed))
                events = self.store.events(self.run_id)
                self.assertEqual(sum(e["kind"] == "closed" for e in events), 1)
                self.assertEqual(sum(a["artifact_key"] == "draft" for a in self.rows("swarm_artifacts")), 1)
                self.assertEqual(self.store.inspect(self.run_id)["status"], "complete")


if __name__ == "__main__":
    unittest.main()

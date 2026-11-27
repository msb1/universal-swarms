"""Run the same mailbox activation loop for every peer."""
import asyncio
import json
import os
import re
from decimal import Decimal, InvalidOperation
from uuid import UUID, uuid4

import psycopg
from jsonschema import ValidationError, validate

from .config import BUILTINS
from .fixtures import TOOLS
from .llm import ModelError, channel_markers_only, strict_json
from .store import Busy, dump, fingerprint, js


class Engine:
    def __init__(self, store, llm, run):
        self.s, self.llm, self.run = store, llm, run
        self.owner = uuid4()
        self.response_markers = set()

    def acquire(self):
        with self.s.db.transaction():
            row = self.s.lock(self.run)
            if row["status"] != "active":
                return False
            if row["held"]:
                raise Busy("Run already has an active scheduler")
            self.config = row["config"]
            self.bounds = self.config["bounds"]
            self.fence = row["fence"] + 1
            self.coordinator = row["coordinator"]
            self.tools = BUILTINS | {
                name: {"name": name, "description": name.replace("_", " "), "parameters": schema}
                for name, schema in self.config["tools"].items()
            }
            self.renew()
            self.event("lease_acquired", {"fence": self.fence})
        return True

    def renew(self):
        self.s.do("""UPDATE swarm_runs SET owner=%s,fence=%s,lease=clock_timestamp()+%s*interval '1 second'
                     WHERE run_id=%s""", self.owner, self.fence, self.bounds["lease_seconds"], self.run)

    def guard(self):
        row = self.s.lock(self.run)
        if row["owner"] != self.owner or row["fence"] != self.fence or not row["held"] or row["status"] != "active":
            raise Busy("Run lease was lost")
        if row["expired"]:
            raise TimeoutError("deadline")
        self.renew()
        return row

    async def external(self, call):
        task = asyncio.ensure_future(call)
        try:
            while not task.done():
                await asyncio.wait({task}, timeout=self.bounds["lease_seconds"] / 3)
                with self.s.db.transaction():
                    self.guard()
            return task.result()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    def event(self, kind, payload, *, mail=None, operation=None):
        return self.s.event(self.run, kind, payload, activation=mail["activation"] if mail else None,
                            actor=mail["recipient"] if mail else None, operation=operation)

    def active(self, target):
        return self.s.one("SELECT * FROM swarm_roster WHERE run_id=%s AND peer_id=%s AND retirement IS NULL",
                          self.run, target) if target else None

    def named(self, name):
        return self.s.one("SELECT * FROM swarm_roster WHERE run_id=%s AND name=%s AND retirement IS NULL",
                          self.run, name)

    def submission_reviewers(self, mail):
        sender = self.active(mail["recipient"])
        return [name for name, template in self.config["initial"].items()
                if sender["name"] in self.config["templates"][template].get("review", {}).get("targets", [])
                and self.config["templates"][template]["review"].get("input", {}).get("source") == "submission"]

    def claim(self):
        with self.s.db.transaction():
            row = self.guard()
            if row["step"] >= self.bounds["activations"]:
                self.finish("blocked", "activation limit")
                return None
            self.sweep(row["step"])
            mail = self.s.one("""SELECT m.* FROM swarm_mailbox m JOIN swarm_roster p
                                 ON (p.run_id,p.peer_id)=(m.run_id,m.recipient)
                                 WHERE m.run_id=%s AND m.state IN ('in_flight','queued') AND p.retirement IS NULL
                                 ORDER BY (m.state='in_flight') DESC,m.queue_sequence LIMIT 1""", self.run)
            if not mail:
                self.finish("blocked", "no runnable work")
                return None
            pinned = mail["pinned"]
            if not pinned:
                pinned = self.snapshot(self.active(mail["recipient"]))
            activation = mail["activation"] or uuid4()
            mail.update(activation=activation, pinned=pinned)
            self.s.do("""UPDATE swarm_mailbox SET state='in_flight',activation=%s,pinned=%s,attempt=attempt+1,fence=%s
                         WHERE run_id=%s AND message_id=%s""", activation, js(pinned), self.fence, self.run, mail["message_id"])
            self.event("resumed" if mail["attempt"] else "claimed",
                       {"message": mail["message_id"], "pinned": pinned, "fence": self.fence}, mail=mail)
            return mail

    def snapshot(self, peer):
        role = self.config["templates"][peer["template"]]
        if peer["allowed_tools"] is not None:
            role = {**role, "tools": peer["allowed_tools"]}
        return {"name": peer["name"], "revision": peer["revision"], "prompt": peer["prompt"],
                "role": role, "phase": self.phase()}

    def sweep(self, step):
        idle = self.bounds["idle_steps"]
        if not idle:
            return
        peers = self.s.all("""SELECT p.peer_id FROM swarm_roster p WHERE run_id=%s AND retirement IS NULL
                           AND ttl_turns IS NOT NULL AND %s-last_active_step >= %s AND NOT EXISTS
                           (SELECT 1 FROM swarm_mailbox m WHERE m.run_id=p.run_id AND m.recipient=p.peer_id
                            AND m.state IN ('queued','in_flight'))""", self.run, step, idle)
        for peer in peers:
            self.retire(peer["peer_id"], "idle")

    def retire(self, peer, reason, mail=None, notify=True):
        self.s.do("UPDATE swarm_roster SET retirement=%s WHERE run_id=%s AND peer_id=%s", reason, self.run, peer)
        self.event("peer_retired", {"peer": peer, "reason": reason}, mail=mail)
        pending = self.s.all("""UPDATE swarm_mailbox SET state='cancelled'
                              WHERE run_id=%s AND recipient=%s AND state IN ('queued','in_flight')
                              RETURNING message_id,sender,correlation_id""", self.run, peer)
        for item in pending:
            self.event("cancelled", {"message": item["message_id"], "reason": reason}, mail=mail)
            if notify and self.active(item["sender"]):
                self.s.enqueue(self.run, peer, item["sender"], "delivery_failure",
                               {"reason": reason, "recipient": str(peer), "message_id": str(item["message_id"])},
                               item["correlation_id"], item["message_id"])

    def finish(self, status, reason=None, answer=None):
        for peer in self.s.all("SELECT peer_id FROM swarm_roster WHERE run_id=%s AND retirement IS NULL", self.run):
            self.retire(peer["peer_id"], "run_ended", notify=False)
        self.s.do("UPDATE swarm_runs SET status=%s,reason=%s,final_answer=%s,lease=NULL WHERE run_id=%s",
                  status, reason, answer, self.run)
        self.event("run_ended", {"status": status, "reason": reason, "answer": answer})

    def phase(self):
        completed = self.s.one("SELECT count(*) AS n FROM swarm_ledger WHERE run_id=%s AND kind='phase_complete'", self.run)["n"]
        return next(iter(self.config.get("phases", [])[completed:]), None)

    def evidence(self, phase):
        if not phase:
            return False
        return self.missing_evidence(phase) is None

    def missing_evidence(self, phase):
        receipts = self.s.all("SELECT payload FROM swarm_ledger WHERE run_id=%s AND kind='tool_result' ORDER BY sequence", self.run)
        remaining = iter(receipts)
        for rule in phase["evidence"]:
            for item in remaining:
                payload = item["payload"]
                if self.matches(payload, phase["id"], rule) and all(
                    key in payload["args"]
                    and (value := self.evidence_value(reference, receipts[:receipts.index(item)])) is not None
                    and payload["args"][key] == value
                    for key, reference in rule.get("args_from", {}).items()
                ):
                    break
            else:
                return rule["tool"]
        return None

    @staticmethod
    def evidence_value(reference, receipts):
        for item in reversed(receipts):
            payload = item["payload"]
            if payload["message"]["name"] == reference["tool"]:
                if payload["error"]:
                    if reference.get("result"):
                        return None
                    continue
                value = payload["result"]
                if any(not isinstance(value, dict) or value.get(key) != expected
                       for key, expected in reference.get("result", {}).items()):
                    return None
                if reference.get("artifact_version"):
                    return payload.get("bound_input", {}).get("artifact")
                try:
                    for key in reference.get("path", []):
                        value = value[key]
                    return value
                except (KeyError, IndexError, TypeError):
                    return None
        return None

    def final_evidence_matches(self, content, checks=None):
        if checks is None:
            checks = self.config.get("final_evidence", [])
        if not checks:
            return True
        receipts = self.s.all("SELECT payload FROM swarm_ledger WHERE run_id=%s AND kind='tool_result' ORDER BY sequence", self.run)
        for check in checks:
            value = self.evidence_value(check["source"], receipts)
            if value is None:
                return False
            if key := check.get("latest_artifact"):
                latest = self.s.one("""SELECT version FROM swarm_artifacts
                                       WHERE run_id=%s AND artifact_key=%s AND producer=%s
                                       ORDER BY version DESC LIMIT 1""", self.run, key, self.coordinator)
                validated = self.evidence_value({**check["source"], "artifact_version": True}, receipts)
                if not latest or validated != {"key": key, "version": latest["version"]}:
                    return False
            if check.get("table_rows"):
                pattern = r"(?m)^\|\s*([^|]+?)\s*\|\s*(\d+)\s*\|\s*(\d+(?:\.\d+)?)\s*\|\s*$"
                expected = re.findall(pattern, value) if isinstance(value, str) else []
                actual = re.findall(pattern, content)
                if not expected or sorted(expected) != sorted(actual):
                    return False
                continue
            match = re.search(check["pattern"], content)
            if not match:
                return False
            actual = match.group(1).replace(",", "")
            if check.get("numeric"):
                try:
                    if Decimal(actual) != Decimal(str(value)):
                        return False
                except InvalidOperation:
                    return False
            elif actual != str(value):
                return False
        return True

    @staticmethod
    def matches(item, phase, rule):
        if item["phase"] != phase or item["message"]["name"] != rule["tool"] or item["error"]:
            return False
        return (all(item["args"].get(key) == value for key, value in rule.get("args", {}).items())
                and all(isinstance(item["result"], dict) and item["result"].get(key) == value
                        for key, value in rule.get("result", {}).items())
                and all(value in item["message"]["content"] for value in rule.get("contains", [])))

    def requires_prompt_revision(self, mail):
        return (mail.get("payload", {}).get("verdict") == "Approved"
                and mail["pinned"]["role"].get("review", {}).get("on_rejected_approval") == "revise_prompt")

    def specialist_action(self, mail):
        grants = mail["pinned"].get("role", {}).get("tools", [])
        payload = mail.get("payload", {})
        followup = payload.get("review_followup", {})
        if "confirm_review_artifact" in grants and followup.get("confirmed_ref"):
            return "confirm_review_artifact"
        if "submit_review_revision" in grants and (payload.get("revision_of") or followup.get("correction_of")):
            return "submit_review_revision"

    def allowed(self, mail):
        pinned = mail["pinned"]
        phase = pinned["phase"]
        role = pinned["role"]
        if action := self.specialist_action(mail):
            return set(role["tools"]) & {"read_artifact", action}
        if role.get("review", {}).get("specialist"):
            if self.failed_review(mail):
                sender = self.active(mail["sender"])
                if sender and sender["creator"] == mail["recipient"]:
                    return {"retry_review_specialist", "inspect_peer"}
                return {"recruit_review_specialist"}
            return set(role["tools"]) - {"resolve_review"}
        if role.get("review_draft") and mail.get("kind") == "assignment" and mail["sender"] is None:
            return {"create_review_draft"}
        if role.get("review_draft") and mail.get("kind") == "review_accepted":
            return {"read_artifact"}
        if not phase:
            return {name for name in pinned["role"].get("tools", [])
                    if name not in ("submit_review_revision", "confirm_review_artifact")
                    and (name in BUILTINS or self.config.get("allow_unphased_tools"))}
        return {name for name in pinned["role"].get("tools", [])
                if name in BUILTINS or (pinned["name"] == phase["peer"] and name in {r["tool"] for r in phase["evidence"]})}

    def target(self, name, mail):
        try:
            identifier = UUID(name)
        except ValueError:
            peer = self.named(name)
        else:
            peer = self.active(identifier)
        if not peer:
            raise ValueError(f"Target peer '{name}' is unavailable or retired; choose an active peer")
        sender = self.active(mail["recipient"])
        if mail["pinned"]["role"].get("parent_only") and peer["peer_id"] != sender["creator"]:
            raise ValueError("Sender may route only to its creator")
        return peer["peer_id"]

    def artifact(self, key, version):
        return self.s.one("SELECT producer,content FROM swarm_artifacts WHERE run_id=%s AND artifact_key=%s AND version=%s",
                          self.run, key, version)

    def artifact_contracts(self):
        for template in self.config["templates"].values():
            review = template.get("review", {})
            binding = review.get("input", {})
            if binding.get("source") == "artifact":
                yield binding["key"], self.config["tools"][review["tool"]]["properties"][binding["argument"]]

    def write(self, mail, args):
        key = args["key"]
        confirmed = mail.get("payload", {}).get("review_followup", {}).get("confirmed_ref", {})
        if confirmed.get("key") == key:
            raise ValueError("The checked artifact must be confirmed without rewriting it")
        for artifact_key, schema in self.artifact_contracts():
            if artifact_key == key:
                try:
                    validate(args["content"], schema)
                except ValidationError as issue:
                    raise ValueError(f"Invalid content for artifact '{key}': {issue.message}") from issue
        version = self.s.one("SELECT coalesce(max(version),0) AS n FROM swarm_artifacts WHERE run_id=%s AND artifact_key=%s",
                             self.run, key)["n"]
        if args.get("expected_version", version) != version:
            raise ValueError(f"Artifact '{key}' version conflict; current version is {version}")
        self.s.do("""INSERT INTO swarm_artifacts (run_id,artifact_key,version,producer,activation,content)
                     VALUES (%s,%s,%s,%s,%s,%s)""",
                  self.run, key, version + 1, mail["recipient"], mail["activation"], js(args["content"]))
        self.event("artifact_written", {"key": key, "version": version + 1, "content": args["content"]}, mail=mail)
        return {"key": key, "version": version + 1}

    def mutate(self, mail, args):
        name, prompt = args["name"], args["prompt"]
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", name) or not prompt.strip():
            raise ValueError("Invalid peer name or prompt")
        peer = self.named(name)
        if peer:
            if "template" in args or args.get("expected_revision") != peer["revision"]:
                raise ValueError(f"Peer '{name}' revision conflict or attempted template change; current revision is {peer['revision']}")
            inspection = self.s.one("""SELECT 1 FROM swarm_ledger WHERE run_id=%s AND activation=%s
                                      AND kind='tool_result' AND payload->'message'->>'name'='inspect_peer'
                                      AND payload->>'error' IS NULL
                                      AND payload->'result'->>'peer'=%s
                                      AND (payload->'result'->>'revision')::integer=%s LIMIT 1""",
                                    self.run, mail["activation"], str(peer["peer_id"]), peer["revision"])
            if not inspection:
                raise ValueError("Inspect the target's current prompt and revision before updating it")
            if not args.get("diagnosis", "").strip():
                raise ValueError("Explain why the current prompt or capabilities need changing")
            role = mail["pinned"]["role"]
            targets = set(role.get("mutate_peers", [])) | set(role.get("review", {}).get("targets", []))
            if name not in targets:
                raise ValueError("Prompt mutation target is not authorized")
            revision = peer["revision"] + 1
            grants = args.get("allowed_tools", peer["allowed_tools"])
            if "allowed_tools" in args:
                authorized = mail["pinned"]["role"].get("mutate_tools", {}).get(name)
                baseline = self.config["templates"][peer["template"]].get("tools", [])
                if authorized is None or set(grants) - (set(baseline) | set(authorized)):
                    raise ValueError("Tool grant change is not authorized")
            baseline = self.config["templates"][peer["template"]].get("tools", [])
            current = baseline if peer["allowed_tools"] is None else peer["allowed_tools"]
            proposed = baseline if grants is None else grants
            required = set(self.config["templates"][peer["template"]].get("required_tools", []))
            if missing := required - set(proposed):
                raise ValueError("Retain the target's required tools in allowed_tools: " + ", ".join(sorted(missing)))
            if prompt == peer["prompt"] and set(current) == set(proposed):
                raise ValueError("Mutation must change the prompt or tool grants. Use retry for unchanged peers.")
            self.s.do("UPDATE swarm_roster SET prompt=%s,revision=%s,allowed_tools=%s WHERE run_id=%s AND peer_id=%s",
                      prompt, revision, grants, self.run, peer["peer_id"])
            self.event("peer_updated", {"peer": peer["peer_id"], "revision": revision, "prompt": prompt}, mail=mail)
            self.event("roster_mutation", {
                "active_agent": mail["pinned"]["name"], "target_agent": name, "revision": revision,
                "system_prompt": prompt, "allowed_tools": grants,
            }, mail=mail)
            return {"peer": str(peer["peer_id"]), "revision": revision}
        template = args.get("template")
        if {"expected_revision", "allowed_tools"} & args.keys() or template not in mail["pinned"]["role"].get("recruit", []):
            raise ValueError("Recruitment template is not granted")
        counts = self.s.one("""SELECT count(*) FILTER (WHERE retirement IS NULL) AS active,
                               count(*) FILTER (WHERE creator IS NOT NULL) AS recruited FROM swarm_roster WHERE run_id=%s""", self.run)
        if counts["active"] >= self.bounds["active_peers"] or counts["recruited"] >= self.bounds["recruitments"]:
            raise ValueError("Recruitment limit")
        step = self.s.lock(self.run)["step"]
        peer = self.s.peer(self.run, name, template, prompt, self.config["templates"][template].get("ttl"),
                           step, mail["recipient"])
        self.event("roster_mutation", {
            "active_agent": mail["pinned"]["name"], "target_agent": name, "revision": 1,
            "system_prompt": prompt, "template": template,
        }, mail=mail)
        return {"peer": str(peer), "revision": 1}

    def apply(self, mail, name, args):
        phase = mail["pinned"]["phase"]
        role = mail["pinned"].get("role", {})
        if mail.get("kind") == "review_accepted" and (name in (
            "submit_sql", "send_handoff", "dispatch", "retire_self", "yield_turn", "resolve_review", "write_artifact"
        ) or (name == "send_result" and mail["recipient"] == self.coordinator)):
            raise ValueError("This review is accepted. Report the accepted result as your final response; do not submit it again.")
        for prerequisite in role.get("tool_prerequisites", {}).get(name, []):
            if prerequisite not in role.get("tools", []):
                continue
            receipt = self.s.one("""SELECT 1 FROM swarm_ledger result
                                    JOIN swarm_ledger claim ON claim.run_id=result.run_id
                                        AND claim.activation=result.activation AND claim.kind='claimed'
                                    WHERE result.run_id=%s AND result.actor=%s AND result.kind='tool_result'
                                      AND result.payload->'message'->>'name'=%s
                                      AND result.payload->>'error' IS NULL
                                      AND (claim.payload->'pinned'->>'revision')::integer=%s
                                    LIMIT 1""",
                                 self.run, mail["recipient"], prerequisite, mail["pinned"]["revision"])
            if not receipt:
                raise ValueError(f"Call {prerequisite} successfully at your current revision before {name}")
        if phase and phase.get("summary_evidence") and name in ("send_handoff", "submit_sql", "send_result", "retire_self", "yield_turn"):
            raise ValueError("Complete this phase by returning the required evidence-checked summary as a final response")
        if name in ("submit_review_revision", "confirm_review_artifact"):
            if name != self.specialist_action(mail):
                raise ValueError("Review assignment does not authorize this operation")
            target = self.target(str(mail["sender"]), mail)
            payload = mail["payload"]
            followup = payload.get("review_followup", {})
            ref = payload.get("revision_of") or followup.get("correction_of") or followup["confirmed_ref"]
            if ref not in payload["artifact_refs"]:
                raise ValueError("Review assignment is missing its bound artifact")
            if name == "submit_review_revision":
                ref = self.write(mail, {"key": ref["key"], "expected_version": ref["version"],
                                        "content": args["content"]})
            return {"intent": "send_handoff", "target": str(target), "payload": {"artifact_refs": [ref]}}
        if name == "create_review_draft":
            draft = role["review_draft"]
            target = self.target(draft["reviewer"], mail)
            ref = self.write(mail, {"key": draft["key"], "content": args["content"], "expected_version": 0})
            return {"intent": "send_handoff", "target": str(target), "payload": {"artifact_refs": [ref]}}
        if name in ("recruit_review_specialist", "retry_review_specialist"):
            if name == "recruit_review_specialist":
                args = {**args, **role["review"]["specialist"], "disposition": "mutate"}
            else:
                args = {**args, "target": str(mail["sender"]), "disposition": "retry"}
            return self.resolve_review(mail, args, bind_evidence=True)
        for ref in args.get("artifact_refs", []):
            artifact = self.artifact(ref["key"], ref["version"])
            if not artifact or (artifact["producer"] != mail["recipient"] and ref not in mail["payload"].get("artifact_refs", [])):
                raise ValueError("Artifact reference is unavailable to this peer")
        if name == "resolve_review":
            return self.resolve_review(mail, args)
        if name == "inspect_peer":
            if not ({"upsert_peer", "resolve_review"} & set(mail["pinned"]["role"].get("tools", []))):
                raise ValueError("Peer inspection is not authorized")
            try:
                identifier = UUID(args["name"])
            except ValueError:
                peer = self.named(args["name"])
            else:
                peer = self.active(identifier)
            if not peer:
                raise ValueError("Peer is unavailable")
            actor_role = mail["pinned"]["role"]
            targets = set(actor_role.get("mutate_peers", [])) | set(actor_role.get("review", {}).get("targets", []))
            if peer["name"] not in targets and peer.get("creator") != mail["recipient"]:
                raise ValueError("Peer inspection is not authorized")
            role = self.config["templates"][peer["template"]]
            result = {"peer": str(peer["peer_id"]), "name": peer["name"], "revision": peer["revision"],
                      "prompt": peer["prompt"], "allowed_tools": peer["allowed_tools"]
                      if peer["allowed_tools"] is not None else role.get("tools", [])}
            grants = mail["pinned"]["role"].get("mutate_tools", {}).get(peer["name"])
            if grants is not None:
                result["permitted_tool_grants"] = sorted(set(role.get("tools", [])) | set(grants))
            if role.get("required_tools"):
                result["required_tools"] = role["required_tools"]
            return result
        if name in ("upsert_peer", "dispatch", "send_handoff", "submit_sql", "send_result", "retire_self", "yield_turn") and self.failed_review(mail):
            raise ValueError("Resolve the failed review with resolve_review")
        if name == "upsert_peer":
            return self.mutate(mail, args)
        if name == "write_artifact":
            return self.write(mail, args)
        if name == "read_artifact":
            artifact = self.artifact(args["key"], args["version"])
            if not artifact or (artifact["producer"] != mail["recipient"] and args not in mail["payload"].get("artifact_refs", [])):
                raise ValueError("Artifact is unavailable to this peer")
            return artifact["content"]
        if name in ("dispatch", "send_handoff", "submit_sql"):
            if name == "submit_sql":
                reviewers = self.submission_reviewers(mail)
                if len(reviewers) == 1:
                    if args.get("target") and args["target"] != reviewers[0]:
                        raise ValueError(f"SQL submissions are routed to reviewer {reviewers[0]}")
                    target = self.target(reviewers[0], mail)
                else:
                    if not args.get("target"):
                        raise ValueError("Choose a target reviewer for this SQL submission")
                    if reviewers and args["target"] not in reviewers:
                        raise ValueError("Choose a configured reviewer for this SQL submission")
                    target = self.target(args["target"], mail)
                peer = self.active(target)
                review = self.config["templates"][peer["template"]].get("review", {})
                if review.get("input", {}).get("source") != "submission":
                    raise ValueError("Target does not accept SQL submissions")
                return {"intent": name, "target": str(target),
                        "payload": {"submission": {"query": args["query"]}}}
            target = self.target(args["target"], mail)
            if name == "send_handoff" and role.get("artifact_verdict"):
                key = role["artifact_verdict"]
                refs = [r for r in mail["payload"].get("artifact_refs", []) if r["key"] == key]
                if target != mail["sender"] or len(refs) != 1:
                    raise ValueError("Return the verdict to the submitting peer for its submitted version")
                payload = {"task": args["task"], "artifact_refs": refs, "verdict": args["task"].split(": ", 1)[0]}
                if "evidence_sequence" in mail["payload"]:
                    payload["revision_of"] = refs[0]
                return {"intent": name, "target": str(target), "payload": payload}
            if name == "dispatch":
                identifier = self.s.enqueue(self.run, mail["recipient"], target, "assignment",
                                            {key: value for key, value in args.items() if key != "target"},
                                            reply=mail["message_id"])
                return {"dispatched": str(identifier)}
            review = role.get("review", {})
            followup = mail.get("payload", {}).get("review_followup", {})
            if confirmed := followup.get("confirmed_ref"):
                if confirmed not in args.get("artifact_refs", []):
                    raise ValueError("Return the exact artifact reference supplied for confirmation")
            if correction := followup.get("correction_of") or mail.get("payload", {}).get("revision_of"):
                if not any(ref["key"] == correction["key"] and ref["version"] > correction["version"]
                           for ref in args.get("artifact_refs", [])):
                    raise ValueError("Return the revised artifact version for review")
            confirm = review.get("require_specialist_confirmation") and mail["sender"] != self.coordinator
            if review.get("accept_artifact") and (target == self.coordinator or confirm):
                receipt = self.s.one("""SELECT payload FROM swarm_ledger WHERE run_id=%s AND activation=%s
                                      AND kind='tool_result' AND payload->'message'->>'name'=%s
                                      ORDER BY sequence DESC LIMIT 1""", self.run, mail["activation"], review["tool"])
                evidence = receipt["payload"] if receipt else {}
                ref = evidence.get("bound_input", {}).get("artifact")
                if (evidence.get("error") or not ref or ref not in args.get("artifact_refs", [])
                        or any(evidence.get("result", {}).get(k) != v for k, v in review["success"].items())):
                    raise ValueError("Hand off the exact artifact version that passed the current review")
                latest = self.s.one("SELECT max(version) AS version FROM swarm_artifacts WHERE run_id=%s AND artifact_key=%s",
                                    self.run, ref["key"])
                if latest["version"] != ref["version"]:
                    raise ValueError("Review the latest artifact version before accepting it")
                artifact = self.artifact(ref["key"], ref["version"])
                if confirm:
                    specialist = self.s.one(
                        """SELECT peer_id FROM swarm_roster WHERE run_id=%s AND creator=%s
                           AND peer_id=%s AND template=%s""",
                        self.run, mail["recipient"], mail["sender"], review["specialist"]["template"])
                    if not specialist or artifact["producer"] != mail["sender"]:
                        raise ValueError("Specialist review is required before acceptance")
                    request = self.s.one(
                        """SELECT payload FROM swarm_mailbox WHERE run_id=%s AND message_id=%s
                           AND sender=%s AND recipient=%s""",
                        self.run, mail["in_reply_to"], mail["recipient"], mail["sender"])
                    confirmation = request["payload"].get("review_followup") if request else None
                    if not confirmation:
                        if target != mail["sender"]:
                            raise ValueError("Return the checked artifact to the specialist for confirmation")
                        return {"intent": name, "target": str(target), "payload": {
                            "task": args["task"], "artifact_refs": [ref],
                            "review_followup": {"confirmed_ref": ref}}}
                    if (confirmation.get("confirmed_ref", ref) != ref or target != self.coordinator
                            or ("correction_of" in confirmation and
                                ref["version"] <= confirmation["correction_of"]["version"])):
                        raise ValueError("Deliver the checked specialist confirmation to the coordinator")
                acceptance = self.event("review_accepted", {"target": str(target), "artifact": ref,
                    "result": evidence["result"], "evidence_call_id": evidence["message"]["tool_call_id"]}, mail=mail)
                return {"intent": "review_accepted", "target": str(target), "payload": {
                    "task": args["task"], "artifact_refs": [ref], "artifact_content": artifact["content"],
                    "acceptance_sequence": acceptance, "evidence_call_id": evidence["message"]["tool_call_id"]}}
            return {"intent": name, "target": str(target), "payload": {key: value for key, value in args.items() if key != "target"}}
        if name == "send_result":
            peer = self.active(mail["recipient"])
            for target in (mail["sender"], peer["creator"], self.coordinator):
                if target and target != mail["recipient"] and self.active(target):
                    target = self.target(str(target), mail)
                    return {"intent": name, "target": str(target), "payload": args}
            raise ValueError("No active reply recipient")
        return {"intent": name, **args}

    def failed_review(self, mail):
        review = mail["pinned"].get("role", {}).get("review")
        if not review:
            return None
        return self.s.one("""SELECT sequence, payload FROM swarm_ledger
                             WHERE run_id=%s AND activation=%s AND kind='tool_result'
                             AND payload->'message'->>'name'=%s
                             AND payload->>'review_evidence' IS DISTINCT FROM 'false'
                             AND (payload->>'error' IS NOT NULL
                                  OR NOT coalesce((payload->'result') @> %s, false))
                             AND NOT EXISTS (SELECT 1 FROM swarm_ledger
                                             WHERE run_id=%s AND activation=%s AND kind='review_resolved')
                             ORDER BY sequence DESC LIMIT 1""",
                          self.run, mail["activation"], review["tool"], js(review["success"]),
                          self.run, mail["activation"])

    def resolve_review(self, mail, args, *, bind_evidence=False):
        evidence = self.failed_review(mail)
        if not evidence:
            review = mail["pinned"].get("role", {}).get("review")
            if review:
                raise ValueError(f"No failed evidence in this activation. Review the current submission with {review['tool']} first.")
            raise ValueError("No failed review is configured for this peer")
        typed_submission = mail["pinned"]["role"].get("review", {}).get("input", {}).get("source") == "submission"
        evidence_id = evidence["payload"]["message"]["tool_call_id"]
        if args.get("evidence_call_id", evidence_id if typed_submission or bind_evidence or mail.get("payload", {}).get("verdict") else None) != evidence_id:
            raise ValueError("Reference the current failed review evidence")
        disposition = args["disposition"]
        if disposition not in ("retry", "mutate"):
            raise ValueError("A failed review requires a retry or mutation")
        diagnosis = args["diagnosis"].strip()
        if not diagnosis:
            raise ValueError("A diagnosis is required")
        role = mail["pinned"]["role"]
        target_name = args.get("target")
        if role["review"].get("recruit_only") and disposition == "mutate" and "template" not in args:
            raise ValueError("This review permits recruitment, not mutation of an existing peer")
        if "template" in args and "allowed_tools" in args:
            raise ValueError("Recruitment inherits template tools; omit allowed_tools")
        reviewed_peer = None
        reviewed = self.s.one("""SELECT payload->'result' AS peer FROM swarm_ledger
                                WHERE run_id=%s AND activation=%s AND kind='tool_result'
                                AND payload->'message'->>'name'='inspect_peer'
                                AND payload->>'error' IS NULL ORDER BY sequence DESC LIMIT 1""",
                             self.run, mail["activation"])
        if not reviewed and "template" not in args:
            raise ValueError("Inspect the failing peer's current prompt before resolving the review")
        if reviewed and "template" not in args:
            reviewed_peer = self.active(UUID(reviewed["peer"]["peer"]))
            if not reviewed_peer or reviewed_peer["revision"] != reviewed["peer"]["revision"]:
                raise ValueError("The inspected peer changed; inspect its current prompt again")
            if (reviewed_peer["name"] not in role["review"].get("targets", [])
                    and reviewed_peer.get("creator") != mail["recipient"]):
                raise ValueError("Inspect an authorized peer involved in the review")
        payload = {"diagnosis": diagnosis, "evidence_sequence": evidence["sequence"],
                   "evidence": evidence["payload"]["result"], "error": evidence["payload"]["error"]}
        refs = list(args.get("artifact_refs", []))
        if role["review"].get("input", {}).get("source") == "artifact":
            reviewed_artifact = evidence["payload"].get("bound_input", {}).get("artifact")
            if not reviewed_artifact:
                raise ValueError("The failed review has no bound artifact")
            if any(ref["key"] == reviewed_artifact["key"] and ref != reviewed_artifact for ref in refs):
                raise ValueError("Artifact reference conflicts with the reviewed version")
            latest = self.s.one("SELECT max(version) AS version FROM swarm_artifacts WHERE run_id=%s AND artifact_key=%s",
                                self.run, reviewed_artifact["key"])
            if not latest or latest["version"] != reviewed_artifact["version"]:
                raise ValueError("The reviewed artifact is stale; review the latest version")
            if reviewed_artifact not in refs:
                refs.append(reviewed_artifact)
        if not target_name or not args.get("task", "").strip():
            raise ValueError("A retry target and task are required")
        change = {key: args[key] for key in ("prompt", "expected_revision", "template", "allowed_tools") if key in args}
        if disposition == "mutate" and not change:
            raise ValueError("For mutate, add the top-level 'prompt' field containing the complete replacement system prompt, or 'allowed_tools' to change grants. The 'task' field is only the handoff assignment.")
        if disposition == "retry" and "template" in change:
            raise ValueError("Recruitment requires mutate, not retry")
        if disposition == "retry" and change:
            current_tools = reviewed_peer["allowed_tools"]
            if current_tools is None:
                current_tools = self.config["templates"][reviewed_peer["template"]].get("tools", [])
            same_prompt = "prompt" not in change or change["prompt"] == reviewed_peer["prompt"]
            same_tools = "allowed_tools" not in change or set(change["allowed_tools"]) == set(current_tools)
            if not same_prompt or not same_tools or "template" in change:
                raise ValueError(
                    "retry must keep the inspected prompt and tool grants unchanged. "
                    "Omit prompt, allowed_tools, template, and expected_revision for retry; "
                    "choose mutate if a change is needed."
                )
            if "expected_revision" in change and change["expected_revision"] != reviewed_peer["revision"]:
                raise ValueError("The inspected peer changed; inspect its current prompt again")
            change = {}
        if "template" in change:
            if self.named(target_name):
                raise ValueError("The target already exists. Omit 'template' when updating an existing peer and provide 'prompt' or 'allowed_tools' at the top level.")
            outcome = self.mutate(mail, {"name": target_name, "prompt": "", **change})
            target = UUID(outcome["peer"])
        else:
            target = self.target(target_name, mail)
            peer = self.active(target)
            if role["review"].get("recruit_only") and disposition == "retry" and peer.get("creator") != mail["recipient"]:
                raise ValueError("This review permits retry only for a recruited specialist")
            if peer["name"] not in role["review"].get("targets", []) and peer.get("creator") != mail["recipient"]:
                raise ValueError("Review target is not authorized")
            if change:
                if not {"prompt", "allowed_tools"} & change.keys():
                    raise ValueError("For mutate, provide the top-level 'prompt' field with the complete replacement prompt, or 'allowed_tools' to change grants.")
                if target != reviewed_peer["peer_id"]:
                    raise ValueError("Inspect the target peer before proposing a mutation")
                if "expected_revision" in change and change["expected_revision"] != reviewed_peer["revision"]:
                    raise ValueError(f"Expected revision must match the inspected revision {reviewed_peer['revision']}")
                change["expected_revision"] = reviewed_peer["revision"]
                self.mutate(mail, {"name": peer["name"], "prompt": peer["prompt"], "diagnosis": diagnosis, **change})
        if role["review"].get("require_specialist_confirmation"):
            if disposition == "retry":
                payload["review_followup"] = {"correction_of": reviewed_artifact}
            else:
                payload["revision_of"] = reviewed_artifact
        payload.update(task=args["task"], artifact_refs=refs)
        target_peer = self.active(target)
        record = {"disposition": disposition, "diagnosis": diagnosis, "evidence_sequence": evidence["sequence"],
                  "target": str(target), "revision": target_peer["revision"],
                  "reviewed_peer": str(reviewed_peer["peer_id"]) if reviewed_peer else None,
                  "reviewed_revision": reviewed_peer["revision"] if reviewed_peer else None}
        self.event("review_resolved", record, mail=mail)
        message_id = self.s.enqueue(self.run, mail["recipient"], target, "handoff", payload,
                                    mail["correlation_id"], mail["message_id"])
        if disposition == "mutate":
            self.s.do("UPDATE swarm_mailbox SET pinned=%s WHERE run_id=%s AND message_id=%s",
                      js(self.snapshot(target_peer)), self.run, message_id)
        return {"intent": "resolve_review", "message": str(message_id), **record}

    def receipt(self, operation):
        return self.s.one("SELECT sequence, payload FROM swarm_ledger WHERE run_id=%s AND operation=%s", self.run, operation)

    async def tool(self, mail, decision, position, call, skipped):
        from fastmcp import Client

        async def invoke():
            async with Client(os.getenv("SWARM_MCP_URL", "http://127.0.0.1:8082/mcp")) as client:
                name = call["function"]["name"]
                arguments = {
                    "run_id": str(self.run), "owner": str(self.owner), "fence": self.fence,
                    "message_id": str(mail["message_id"]), "decision": decision,
                    "position": position, "skipped": skipped,
                }
                if name not in self.tools:
                    arguments["tool_name"] = name
                    name = "execute_agent_tool"
                response = await client.call_tool(name, arguments)
                return response.data

        return await self.external(invoke())

    async def execute_tool(self, mail, decision, position, call, skipped):
        operation = f"{mail['activation']}:{decision}:{position}"
        function = call["function"]
        name = function["name"]
        digest = fingerprint(function)
        previous = self.receipt(operation)
        if previous:
            if previous["payload"]["fingerprint"] != digest:
                raise ValueError("Tool operation fingerprint conflict")
            return previous["payload"]
        args, result, error = {}, None, None
        fixture_attempted = False
        bound_input = None
        invalid_artifact = None
        try:
            if skipped:
                raise ValueError("Skipped after terminal tool")
            if name not in self.allowed(mail):
                available = ", ".join(sorted(self.allowed(mail)))
                raise ValueError(f"Tool '{name}' is not granted in this activation. Available tools: {available}")
            if name not in ("resolve_review", "inspect_peer", "recruit_review_specialist", "retry_review_specialist") and self.failed_review(mail):
                raise ValueError("Inspect the failing peer using inspect_peer, then call resolve_review with the failed evidence_call_id")
            args = strict_json(function["arguments"])
            review = mail["pinned"]["role"].get("review")
            if (name == "resolve_review" and review
                    and ((review.get("input", {}).get("source") == "submission" and "submission" in mail.get("payload", {}))
                         or (review.get("evaluate_verdict") and mail["payload"].get("verdict")))):
                args.setdefault("target", str(mail["sender"]))
            if name == "send_handoff" and mail["pinned"]["role"].get("artifact_verdict"):
                verdict = args.pop("verdict", None)
                if verdict not in ("Approved", "Changes requested"):
                    raise ValueError("Specify an explicit review verdict")
                args["target"] = str(mail["sender"])
                args["task"] = f"{verdict}: {args.get('task', '')}"
            if name == "resolve_review" and self.requires_prompt_revision(mail):
                args["disposition"] = "mutate"
                if not args.get("prompt") or "allowed_tools" in args:
                    raise ValueError("Revise the inspected reviewer's prompt, preserving its tool grants")
            for key, schema in self.tools[name]["parameters"].get("properties", {}).items():
                if schema.get("type") == "array" and isinstance(args.get(key), str):
                    try:
                        decoded = json.loads(args[key])
                    except ValueError:
                        continue
                    if isinstance(decoded, list):
                        args[key] = decoded
            review = mail["pinned"]["role"].get("review")
            if review and name == review["tool"] and (binding := review.get("input")):
                source = binding["source"]
                if source == "submission":
                    submitted = mail["payload"].get("submission", {}).get("query")
                    if not isinstance(submitted, str):
                        raise ValueError("Review requires a typed SQL submission")
                    bound_input = {"message": str(mail["message_id"]), "field": "submission.query"}
                elif source == "artifact":
                    key = binding["key"]
                    refs = [ref for ref in mail["payload"].get("artifact_refs", []) if ref["key"] == key]
                    ref = max(refs, key=lambda item: item["version"], default=None)
                    if ref:
                        artifact = self.artifact(key, ref["version"])
                    else:
                        artifact = self.s.one(
                            """SELECT content, version, producer FROM swarm_artifacts
                               WHERE run_id=%s AND artifact_key=%s AND producer=%s
                               ORDER BY version DESC LIMIT 1""", self.run, key, mail["recipient"],
                        )
                        if artifact:
                            ref = {"key": key, "version": artifact["version"]}
                    if not artifact:
                        raise ValueError(f"Review requires a submitted {key} artifact")
                    submitted = artifact["content"]
                    bound_input = {"artifact": ref}
                else:
                    submitted = mail["payload"].get(source)
                    if not isinstance(submitted, str):
                        raise ValueError(f"Review requires a text {source} in the current handoff")
                    bound_input = {"message": str(mail.get("message_id")), "field": source}
                argument = binding["argument"]
                bound_input["requested"] = args.get(argument)
                args[argument] = submitted
            if bound_input and "artifact" in bound_input:
                try:
                    validate(submitted, self.tools[name]["parameters"]["properties"][argument])
                except ValidationError as issue:
                    invalid_artifact = {"artifact": ref, "producer": str(artifact["producer"]),
                                        "expected_schema": self.tools[name]["parameters"]["properties"][argument],
                                        "actual_type": type(submitted).__name__, "reason": issue.message}
                    raise ValueError(f"Invalid stored artifact {ref}: {issue.message}") from issue
            validate(args, self.tools[name]["parameters"])
            if name in TOOLS:
                fixture_attempted = True
                raw = await self.external(TOOLS[name](args))
                try:
                    result = json.loads(raw)
                except ValueError:
                    result = raw
                if raw.startswith(("ERROR:", "AssertionError:")):
                    error = raw
        except (ValueError, ValidationError, psycopg.Error) as issue:
            if isinstance(issue, Busy):
                raise
            error = ("Fixture query failed" if isinstance(issue, psycopg.Error) else
                     f"Invalid arguments at {'.'.join(map(str, issue.absolute_path)) or 'root'}: {issue.message}"
                     if isinstance(issue, ValidationError) else str(issue))
        with self.s.db.transaction():
            self.guard()
            if previous := self.receipt(operation):
                if previous["payload"]["fingerprint"] != digest:
                    raise ValueError("Tool operation fingerprint conflict")
                return previous["payload"]
            if not error and name in BUILTINS:
                try:
                    with self.s.db.transaction():
                        result = self.apply(mail, name, args)
                except ValueError as issue:
                    error = str(issue)
            message = {"role": "tool", "name": name, "tool_call_id": call["id"],
                       "content": dump({"error": error}) if error and result is None else raw if name in TOOLS else dump(result)}
            review = mail["pinned"].get("role", {}).get("review")
            is_review_tool = review and name == review["tool"]
            if is_review_tool and (fixture_attempted or invalid_artifact):
                evidence = {"evidence_call_id": call["id"], "result": result, "error": error}
                if error or not isinstance(result, dict) or any(
                    key not in result or result[key] != value for key, value in review["success"].items()
                ):
                    evidence["next_action"] = ("Use the review action exposed for this failed check."
                                               if review.get("specialist") else "Inspect the failing peer with inspect_peer, then call resolve_review with this evidence_call_id.")
                message["content"] = dump(evidence)
            phase = mail["pinned"]["phase"]
            item = {"fingerprint": digest, "message": message, "args": args, "result": result, "error": error,
                    "phase": phase["id"] if phase else None}
            if is_review_tool:
                item["review_evidence"] = fixture_attempted or invalid_artifact is not None
                if invalid_artifact:
                    item["invalid_artifact"] = invalid_artifact
                if bound_input is not None:
                    item["bound_input"] = bound_input
            self.event("tool_result", item, mail=mail, operation=operation)
        return item

    def context(self, mail):
        pinned = mail["pinned"]
        phase = pinned["phase"]
        peers = self.s.all("SELECT peer_id,name,template,revision FROM swarm_roster WHERE run_id=%s AND retirement IS NULL ORDER BY name", self.run)
        join = []
        if mail["recipient"] == self.coordinator:
            join = self.s.all("""SELECT m.message_id,m.correlation_id,m.payload,
                              EXISTS(SELECT 1 FROM swarm_mailbox r WHERE r.run_id=m.run_id
                              AND r.in_reply_to=m.message_id AND r.kind IN ('result','retire_notice','delivery_failure')) AS replied
                              FROM swarm_mailbox m WHERE m.run_id=%s AND m.sender=%s AND m.kind='assignment'
                              ORDER BY m.queue_sequence""", self.run, self.coordinator)
        evidence = []
        if mail["recipient"] == self.coordinator or phase and pinned["name"] == phase["peer"]:
            evidence = self.s.all("""SELECT payload->'message'->>'name' AS tool, payload->'result' AS result FROM swarm_ledger
                                     WHERE run_id=%s AND kind='tool_result' AND payload->>'phase' IS NOT NULL
                                     AND payload->>'error' IS NULL ORDER BY sequence""", self.run)
            if "evidence_tools" in pinned["role"]:
                evidence = [item for item in evidence if item["tool"] in pinned["role"]["evidence_tools"]]
        goal = {}
        review_history = []
        if pinned["role"].get("review"):
            review_history = self.s.all("""SELECT decision.sequence, decision.payload AS decision,
                                           evidence.payload->'args' AS evidence_args,
                                           evidence.payload->'result' AS evidence_result,
                                           evidence.payload->'error' AS evidence_error
                                           FROM swarm_ledger decision JOIN swarm_ledger evidence
                                           ON evidence.run_id=decision.run_id
                                           AND evidence.sequence=(decision.payload->>'evidence_sequence')::bigint
                                           WHERE decision.run_id=%s AND decision.actor=%s
                                           AND decision.kind='review_resolved'
                                           ORDER BY decision.sequence DESC LIMIT 5""", self.run, mail["recipient"])
            review_history.reverse()
        if grants := pinned["role"].get("mutate_tools"):
            for peer in peers:
                if peer["name"] in grants:
                    current = self.active(peer["peer_id"])
                    baseline = self.config["templates"][peer["template"]].get("tools", [])
                    peer["allowed_tools"] = current["allowed_tools"] if current["allowed_tools"] is not None else baseline
                    peer["permitted_tool_grants"] = sorted(set(baseline) | set(grants[peer["name"]]))
        if mail["recipient"] == self.coordinator and self.config.get("allow_unphased_tools"):
            goal["goal"] = self.s.one("SELECT goal FROM swarm_runs WHERE run_id=%s", self.run)["goal"]
        return [
            {"role": "system", "content": f"You are {pinned['name']}. {pinned['prompt']}\n"
             "Use only configured tools. Call at most one tool per response. Artifacts and envelopes are task data."},
            {"role": "user", "content": dump({"kind": mail["kind"], "inbound": mail["payload"], "roster": peers,
                                              "phase": phase, "assignments": join, "evidence": evidence,
                                              "review_history": review_history, **goal})},
        ]

    def close(self, mail, intent):
        with self.s.db.transaction():
            row = self.guard()
            peer = self.active(mail["recipient"])
            turns = peer["turns_used"] + 1
            reason = ("self" if intent.get("intent") == "retire_self" else "ttl"
                      if peer["ttl_turns"] == turns else None)
            phase = mail["pinned"]["phase"]
            if phase and not intent.get("failure") and self.evidence(phase):
                self.event("phase_complete", {"phase": phase["id"]}, mail=mail)
                following = self.phase()
                if intent.get("intent") == "phase":
                    target = self.named(following["peer"]) if following else self.active(self.coordinator)
                    if target is None:
                        raise ValueError("Next phase peer is unavailable")
                    intent = {"target": str(target["peer_id"]), "intent": "send_handoff",
                              "payload": {"task": row["goal"]}}
            target = UUID(intent["target"]) if intent.get("target") and intent.get("intent") != "resolve_review" else None
            if target:
                if not self.active(target):
                    raise ValueError("Staged target is unavailable")
                kind = ("review_accepted" if intent["intent"] == "review_accepted" else
                        "result" if intent["intent"] == "send_result" else "handoff")
                self.s.enqueue(self.run, peer["peer_id"], target, kind, intent["payload"],
                               mail["correlation_id"], mail["message_id"])
            elif (reason or intent.get("summary")) and not (intent.get("final") or intent.get("failure")):
                for target in (peer["creator"], self.coordinator):
                    if target != peer["peer_id"] and self.active(target):
                        self.s.enqueue(self.run, peer["peer_id"], target, "retire_notice" if reason else "result",
                                       {"summary": intent.get("summary", ""), "status": "complete"}, mail["correlation_id"], mail["message_id"])
                        break
            self.s.do("UPDATE swarm_mailbox SET state='acknowledged' WHERE run_id=%s AND message_id=%s", self.run, mail["message_id"])
            self.s.do("UPDATE swarm_runs SET step=step+1 WHERE run_id=%s", self.run)
            self.s.do("UPDATE swarm_roster SET turns_used=%s,last_active_step=%s WHERE run_id=%s AND peer_id=%s",
                      turns, row["step"] + 1, self.run, peer["peer_id"])
            if reason:
                self.retire(peer["peer_id"], reason, mail)
            self.event("closed", {"message": mail["message_id"], "turns_used": turns, "intent": intent},
                       mail=mail, operation=f"{mail['activation']}:close")
            if intent.get("final"):
                artifact = self.write(mail, {"key": "final_answer", "content": intent["final"]})
                self.event("final_artifact", artifact, mail=mail)
                self.finish("complete", answer=intent["final"])

    def final_intent(self, mail, content):
        if mail["pinned"].get("role", {}).get("review_draft") and mail.get("kind") != "review_accepted":
            return None
        if self.failed_review(mail):
            return None
        if mail.get("kind") == "review_accepted":
            accepted = self.s.one("""SELECT payload FROM swarm_ledger WHERE run_id=%s AND sequence=%s
                                     AND kind='review_accepted' AND actor=%s""",
                                  self.run, mail["payload"]["acceptance_sequence"], mail["sender"])
            if (not accepted or str(accepted["payload"]["target"]) != str(mail["recipient"])
                    or accepted["payload"]["evidence_call_id"] != mail["payload"]["evidence_call_id"]):
                return None
            if "final_text" in mail["payload"] and content != accepted["payload"].get("final_text"):
                return None
            if "artifact_content" in mail["payload"]:
                if (mail["payload"].get("artifact_refs") != [accepted["payload"].get("artifact")]
                        or content != mail["payload"]["artifact_content"]):
                    return None
        phase = mail["pinned"]["phase"]
        if phase:
            if not self.evidence(phase):
                return None
            if phase.get("summary_patterns") and any(not re.search(pattern, content) for pattern in phase["summary_patterns"]):
                return None
            if not self.final_evidence_matches(content, phase.get("summary_evidence", [])):
                return None
            if phase != self.config["phases"][-1] or mail["recipient"] != self.coordinator:
                return {"intent": "phase"}
        if mail["recipient"] != self.coordinator:
            if mail["pinned"].get("role", {}).get("require_handoff"):
                return None
            return {"summary": content} if not self.phase() else None
        pending = self.s.one("""SELECT 1 FROM swarm_mailbox m WHERE m.run_id=%s AND m.sender=%s AND m.kind='assignment'
                                AND NOT EXISTS (SELECT 1 FROM swarm_mailbox r WHERE r.run_id=m.run_id
                                AND r.in_reply_to=m.message_id AND r.kind IN ('result','retire_notice','delivery_failure')) LIMIT 1""",
                             self.run, self.coordinator)
        if pending or any(not re.search(pattern, content) for pattern in self.config.get("final_patterns", [])) or not self.final_evidence_matches(content):
            return None
        return {"final": content}

    def completion_feedback(self, mail, content):
        if action := self.specialist_action(mail):
            return f"Complete this review assignment with {action}."
        if mail["pinned"].get("role", {}).get("review_draft") and mail.get("kind") != "review_accepted":
            return "Submit the draft for review using create_review_draft."
        if mail.get("kind") == "review_accepted" and "artifact_content" in mail["payload"]:
            return "Return the accepted artifact text exactly; changes require a new review."
        if self.failed_review(mail):
            return "Resolve the failed review with resolve_review and cite its evidence_call_id."
        if mail["pinned"].get("role", {}).get("require_handoff"):
            return "Use send_handoff to deliver your review to the peer."
        phase = mail["pinned"]["phase"]
        missing = self.missing_evidence(phase) if phase else None
        if missing:
            return f"Completion rejected: phase '{phase['id']}' is missing required successful evidence from '{missing}', or its arguments do not match earlier results. Review the phase requirements and tool results."
        if phase and any(not re.search(pattern, content) for pattern in phase.get("summary_patterns", [])):
            return "Completion rejected: the phase summary does not match the required format. " + phase.get("summary_format_description", "Follow the phase summary instructions.")
        if phase:
            failures = [check.get("label", check["source"]["tool"]) for check in phase.get("summary_evidence", [])
                        if not self.final_evidence_matches(content, [check])]
            if failures:
                return "Completion rejected: these phase summary fields do not match the recorded tool evidence: " + ", ".join(failures) + ". Review the corresponding tool results."
        pending = self.s.one("""SELECT 1 FROM swarm_mailbox m WHERE m.run_id=%s AND m.sender=%s AND m.kind='assignment'
                                AND NOT EXISTS (SELECT 1 FROM swarm_mailbox r WHERE r.run_id=m.run_id
                                AND r.in_reply_to=m.message_id AND r.kind IN ('result','retire_notice','delivery_failure')) LIMIT 1""",
                             self.run, self.coordinator)
        if pending:
            return "Completion rejected: an assignment is still awaiting a reply. Wait for its result before completing."
        if any(not re.search(pattern, content) for pattern in self.config.get("final_patterns", [])):
            return "Completion rejected: the final response does not match the required format. " + self.config.get(
                "final_format_description", "Follow the output format in your role instructions.")
        failures = [check.get("label", check["source"]["tool"]) for check in self.config.get("final_evidence", [])
                    if not self.final_evidence_matches(content, [check])]
        if failures:
            return self.config.get("final_evidence_feedback") or (
                "Completion rejected: these fields do not match the recorded tool evidence: "
                + ", ".join(failures) + ". Review the corresponding tool results."
            )
        return "Completion rejected: complete the current workflow phase before returning a final answer."

    def complete_decision(self, mail, decision):
        payload = decision["payload"]
        content = payload["message"]["content"]
        marker_only = channel_markers_only(content)
        intent = None if marker_only else self.final_intent(mail, content)
        if intent:
            return intent, None

        operation = f"{mail['activation']}:{decision['sequence']}:feedback"
        diagnostics = payload.get("diagnostics", [])
        endpoint = diagnostics[-1].get("endpoint_key") if diagnostics else None
        match = re.match(r"\A<\|channel>[a-z_]{1,32}\r?\n<channel\|>", content) if endpoint else None
        marker = match.group() if match else None
        normalized = content[len(marker):] if marker else None
        corrected = self.final_intent(mail, normalized) if normalized and normalized.strip() else None
        with self.s.db.transaction():
            self.guard()
            known = False
            if corrected:
                model = self.config.get("llm", {}).get("model") or self.llm.model
                key = (model, endpoint, marker)
                known = key in self.response_markers or self.s.one(
                    "SELECT 1 FROM swarm_response_markers WHERE model=%s AND endpoint_key=%s AND marker=%s", *key
                ) is not None
            if not known:
                receipt = self.receipt(operation)
                if receipt:
                    feedback = receipt["payload"]["message"]
                    rejection = receipt["sequence"]
                else:
                    feedback = {"role": "user", "content": (
                        "Return a tool call or a substantive reply. The previous response contained only channel markers."
                        if marker_only else self.completion_feedback(mail, content)
                    )}
                    rejection = self.event("completion_rejected", {"message": feedback}, mail=mail, operation=operation)
            if corrected:
                if not known:
                    self.s.do("""INSERT INTO swarm_response_markers
                                 (model, endpoint_key, marker, learned_run, rejection_sequence)
                                 VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                              *key, self.run, rejection)
                normalization = operation + ":normalized"
                if not self.receipt(normalization):
                    self.event("completion_normalized", {"decision": decision["sequence"], "marker": marker,
                               "model": model, "endpoint_key": endpoint, "content": normalized},
                               mail=mail, operation=normalization)
        if corrected:
            self.response_markers.add(key)
            return corrected, None
        return None, feedback

    async def activation(self, mail):
        if mail.get("kind") == "review_accepted" and ("artifact_content" in mail["payload"] or "final_text" in mail["payload"]):
            with self.s.db.transaction():
                self.guard()
                refs = mail["payload"].get("artifact_refs", [])
                artifact = self.artifact(**refs[0]) if "artifact_content" in mail["payload"] and len(refs) == 1 else None
                content = mail["payload"].get("final_text") if "final_text" in mail["payload"] else (artifact["content"] if artifact else None)
                intent = self.final_intent(mail, content) if isinstance(content, str) else None
                if intent and "final" in intent:
                    self.close(mail, intent)
                else:
                    reason = "Accepted result failed final verification"
                    self.close(mail, {"failure": reason})
                    self.finish("blocked", reason)
            return
        history = self.s.all("SELECT * FROM swarm_ledger WHERE run_id=%s AND activation=%s ORDER BY sequence",
                             self.run, mail["activation"])
        decisions = [item for item in history if item["kind"] == "decision"]
        attempts = sum(item["kind"] == "model_attempt" for item in history)
        messages = next((item["payload"]["messages"] for item in history if item["kind"] == "context"), None)
        if messages is None:
            with self.s.db.transaction():
                self.guard()
                messages = self.context(mail)
                self.event("context", {"messages": messages}, mail=mail)
        calls_used = 0
        index = 0
        recover_evidence = False
        phase = mail["pinned"]["phase"]
        review = mail["pinned"]["role"].get("review", {})
        typed_review = review.get("input", {}).get("source") == "submission" and "submission" in mail["payload"]
        verdict_review = review.get("evaluate_verdict") and mail["payload"].get("verdict") and "revision_of" not in mail["payload"]
        if typed_review or verdict_review:
            evaluation = next((item for item in history if item["kind"] == "review_evaluation"), None)
            if evaluation is None:
                call = {"id": str(mail["activation"]),
                        "function": {"name": review["tool"], "arguments": "{}"}}
                inspect = {"id": f"{mail['activation']}:peer",
                           "function": {"name": "inspect_peer", "arguments": dump({"name": str(mail["sender"])})}}
                with self.s.db.transaction():
                    self.guard()
                    sequence = self.event("review_evaluation", {"message": {"tool_calls": [call, inspect]}}, mail=mail)
            else:
                sequence = evaluation["sequence"]
                call = evaluation["payload"]["message"]["tool_calls"][0]
                inspect = next(iter(evaluation["payload"]["message"]["tool_calls"][1:]), None)
            receipt = await self.tool(mail, sequence, 0, call, False)
            result = receipt["result"]
            failed = receipt["error"] or not isinstance(result, dict) or any(
                result.get(key) != value for key, value in review["success"].items()
            )
            if not failed and (handoff := review.get("success_handoff")):
                value = result.get(handoff["result_field"])
                if value is None:
                    raise ValueError("Successful review is missing the configured handoff result")
                target = mail["recipient"] if verdict_review else mail["sender"]
                subject = ({"artifact_refs": [receipt["bound_input"]["artifact"]]} if verdict_review
                           else {"submission": mail["payload"]["submission"]})
                final_text = handoff["task_prefix"] + str(value)
                with self.s.db.transaction():
                    self.guard()
                    acceptance = self.event("review_accepted", {
                        "evidence_call_id": call["id"], "target": target, "result_field": handoff["result_field"],
                        "value": value, "result": result, "final_text": final_text, **subject,
                    }, mail=mail)
                    self.close(mail, {"intent": "review_accepted", "target": str(target), "payload": {
                        "task": final_text, "final_text": final_text, "acceptance_sequence": acceptance,
                        "evidence_call_id": call["id"], "result": result, **subject}})
                return
            inspection = await self.tool(mail, sequence, 1, inspect, False) if failed and inspect else None
            messages.append({"role": "user", "content": dump({"review_receipt": {
                "evidence_call_id": call["id"], "args": receipt["args"],
                "result": receipt["result"], "error": receipt["error"],
            }, "reviewed_peer": inspection["result"] if inspection else None})})
            calls_used += 1 + bool(inspection)
        while index < len(decisions) or attempts < self.bounds["model_calls"]:
            if index == len(decisions):
                allowed = self.allowed(mail)
                if typed_review or verdict_review:
                    allowed = allowed - {review["tool"]}
                if verdict_review:
                    allowed &= {"resolve_review"}
                required = self.missing_evidence(phase) if recover_evidence and phase else None
                llm_config = self.config.get("llm", {})
                if required in allowed:
                    allowed = {required}
                    llm_config = {**llm_config, "required_tool": required}
                with self.s.db.transaction():
                    self.guard()
                    self.event("model_attempt", {"attempt": attempts + 1}, mail=mail)
                attempts += 1
                try:
                    tools = [self.tools[name] for name in sorted(allowed)]
                    for operation in allowed & {"submit_review_revision", "create_review_draft"}:
                        key = (mail["pinned"]["role"]["review_draft"]["key"] if operation == "create_review_draft"
                               else (mail["payload"].get("revision_of") or mail["payload"]["review_followup"]["correction_of"])["key"])
                        schemas = [schema for artifact_key, schema in self.artifact_contracts() if artifact_key == key]
                        if not schemas:
                            continue
                        tool = self.tools[operation]
                        tools[sorted(allowed).index(tool["name"])] = {
                            **tool, "description": "Save the complete revised artifact and return it to the reviewer.",
                            "parameters": {**tool["parameters"], "properties": {"content": schemas[0] if len(schemas) == 1 else {"allOf": schemas}}}}
                    binding = review.get("input", {})
                    if binding.get("source") == "artifact" and review["tool"] in allowed:
                        tool = self.tools[review["tool"]]
                        parameters = tool["parameters"]
                        argument = binding["argument"]
                        tools[sorted(allowed).index(review["tool"])] = {
                            **tool, "description": "Check the exact submitted artifact version.",
                            "parameters": {**parameters,
                                           "required": [k for k in parameters.get("required", []) if k != argument],
                                           "properties": {k: v for k, v in parameters["properties"].items()
                                                          if k != argument}},
                        }
                    if "submit_sql" in allowed and len(self.submission_reviewers(mail)) == 1:
                        tool = self.tools["submit_sql"]
                        parameters = tool["parameters"]
                        tools[sorted(allowed).index("submit_sql")] = {
                            **tool, "description": "Submit the SQL query to the configured reviewer.",
                            "parameters": {**parameters, "properties": {"query": parameters["properties"]["query"]}},
                        }
                    elif "submit_sql" in allowed:
                        tool = self.tools["submit_sql"]
                        tools[sorted(allowed).index("submit_sql")] = {
                            **tool, "parameters": {**tool["parameters"], "required": ["query", "target"]},
                        }
                    if (typed_review or verdict_review) and "resolve_review" in allowed:
                        fields = {"disposition", "diagnosis", "task", "prompt", "allowed_tools"}
                        tool = self.tools["resolve_review"]
                        parameters = tool["parameters"]
                        tools[sorted(allowed).index("resolve_review")] = {
                            **tool,
                            "description": (
                                "Resolve the failed check for the inspected peer. Use retry if its current "
                                "instructions and tools suffice. Use mutate with a replacement prompt or updated "
                                "allowed_tools if they need to change. Distinguish observed evidence from hypotheses; "
                                "do not present unverified causes as facts. Explain your diagnosis and assign the next task."
                            ),
                            "parameters": {**parameters,
                                           "required": [key for key in parameters["required"] if key in fields],
                                           "properties": {key: value for key, value in parameters["properties"].items()
                                                          if key in fields}},
                        }
                    if "send_handoff" in allowed and mail["pinned"]["role"].get("artifact_verdict"):
                        tool = self.tools["send_handoff"]
                        tools[sorted(allowed).index("send_handoff")] = {
                            **tool, "description": "Return your verdict and critique to the artifact author.",
                            "parameters": {"type": "object", "additionalProperties": False,
                                           "required": ["verdict", "task"], "properties": {
                                               "verdict": {"enum": ["Approved", "Changes requested"]},
                                               "task": {"type": "string", "minLength": 1}}}}
                    if "resolve_review" in allowed and self.requires_prompt_revision(mail):
                        tool = self.tools["resolve_review"]
                        fields = ["diagnosis", "prompt", "task"]
                        tools[sorted(allowed).index("resolve_review")] = {
                            **tool, "description": "The measured result contradicts the approval. Diagnose the review "
                            "failure, write the reviewer's replacement system prompt, and assign a new critique.",
                            "parameters": {**tool["parameters"], "required": fields, "properties": {
                                k: tool["parameters"]["properties"][k] for k in fields}}}
                    available = ", ".join(tool["name"] for tool in tools) or "none"
                    instructions = {
                        "role": "system",
                        "content": (
                            f"Available function names for this response: {available}. "
                            "Call only a listed function name. Values in function arguments are not function names."
                            + (" The review was accepted. Complete the task by reporting the accepted result "
                               "in the requested final format; do not submit another review."
                               if mail["kind"] == "review_accepted" else "")
                            + (" Return the accepted artifact text exactly, preserving its existing formatting."
                               if mail["kind"] == "review_accepted" and "artifact_content" in mail["payload"] else "")
                        ),
                    }
                    message = await self.external(self.llm.decide([*messages, instructions], tools, llm_config))
                except ModelError as issue:
                    with self.s.db.transaction():
                        self.guard()
                        self.event("model_error", {"error": str(issue), "diagnostics": self.llm.diagnostics}, mail=mail)
                    continue
                with self.s.db.transaction():
                    self.guard()
                    payload = {"message": message, "diagnostics": self.llm.diagnostics}
                    seq = self.event("decision", payload, mail=mail)
                    decisions.append({"sequence": seq, "payload": payload})
            decision = decisions[index]
            index += 1
            message = decision["payload"]["message"]
            messages.append(message)
            intent = None
            for position, call in enumerate(message.get("tool_calls", [])):
                calls_used += 1
                item = await self.tool(mail, decision["sequence"], position, call,
                                       bool(intent) or calls_used > self.bounds["tool_calls"])
                if not item["error"] and call["function"]["name"] in (
                    "send_handoff", "submit_sql", "send_result", "retire_self", "yield_turn", "resolve_review",
                    "submit_review_revision", "confirm_review_artifact",
                    "create_review_draft", "recruit_review_specialist", "retry_review_specialist",
                ):
                    intent = item["result"]
                if (review.get("require_specialist_confirmation") and call["function"]["name"] == review["tool"]
                        and item.get("review_evidence") and not self.active(mail["sender"])
                        and (item["error"] or not isinstance(item["result"], dict)
                             or any(item["result"].get(k) != v for k, v in review["success"].items()))):
                    intent = {"failure": "Final specialist artifact failed review after specialist retirement"}
                messages.append(item["message"])
            if not message.get("tool_calls"):
                intent, feedback = self.complete_decision(mail, decision)
                if not intent:
                    messages.append(feedback)
                    recover_evidence = True
            if intent:
                with self.s.db.transaction():
                    self.close(mail, intent)
                    if intent.get("failure"):
                        self.finish("blocked", intent["failure"])
                return
            if calls_used >= self.bounds["tool_calls"]:
                break
        with self.s.db.transaction():
            self.guard()
            self.close(mail, {"failure": "activation model or tool limit"})
            self.finish("blocked", "activation model or tool limit")

    async def run_all(self):
        if self.acquire():
            try:
                while (mail := self.claim()) is not None:
                    await self.activation(mail)
                    if self.s.one("SELECT status FROM swarm_runs WHERE run_id=%s", self.run)["status"] != "active":
                        break
            except TimeoutError:
                with self.s.db.transaction():
                    row = self.s.lock(self.run)
                    if row["owner"] != self.owner or row["fence"] != self.fence:
                        raise Busy("Run lease was lost")
                    self.finish("blocked", "deadline")
            finally:
                self.s.do("UPDATE swarm_runs SET lease=NULL WHERE run_id=%s AND owner=%s AND fence=%s",
                          self.run, self.owner, self.fence)
        return self.s.inspect(self.run)

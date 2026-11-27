"""PostgreSQL transaction boundaries and ordered swarm records."""
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class Busy(ValueError):
    pass


class RunNotFound(ValueError):
    pass


class AdmissionConflict(ValueError):
    pass


def dump(value):
    return json.dumps(value, default=str, ensure_ascii=False, sort_keys=True, allow_nan=False)


def js(value):
    return Jsonb(value, dumps=dump)


def fingerprint(value):
    return hashlib.sha256(dump(value).encode()).hexdigest()


class Store:
    def __init__(self, dsn):
        self.db = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.db.close()

    def migrate(self):
        self.db.execute(Path(__file__).with_name("schema.sql").read_text())

    def one(self, sql, *args):
        return self.db.execute(sql, args).fetchone()

    def all(self, sql, *args):
        return self.db.execute(sql, args).fetchall()

    def do(self, sql, *args):
        return self.db.execute(sql, args)

    def event(self, run, kind, payload, *, activation=None, actor=None, operation=None):
        seq = self.one("UPDATE swarm_runs SET sequence=sequence+1 WHERE run_id=%s RETURNING sequence", run)["sequence"]
        self.do("INSERT INTO swarm_ledger (run_id,sequence,kind,payload,activation,actor,operation) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                run, seq, kind, js(payload), activation, actor, operation)
        return seq

    def peer(self, run, name, template, prompt, ttl, step=0, creator=None):
        peer = uuid4()
        self.do("""INSERT INTO swarm_roster (run_id,peer_id,name,template,prompt,ttl_turns,last_active_step,creator)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", run, peer, name, template, prompt, ttl, step, creator)
        self.event(run, "peer_created", {"peer": peer, "name": name, "template": template, "prompt": prompt, "ttl": ttl},
                   actor=creator)
        return peer

    def enqueue(self, run, sender, target, kind, payload, correlation=None, reply=None):
        identifier = uuid4()
        correlation = correlation or identifier
        seq = self.event(run, "enqueued", {"message": identifier, "target": target, "kind": kind,
                         "correlation_id": correlation, "in_reply_to": reply, "payload": payload}, actor=sender)
        self.do("""INSERT INTO swarm_mailbox
                   (run_id,message_id,sender,recipient,kind,correlation_id,in_reply_to,payload,queue_sequence)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                run, identifier, sender, target, kind, correlation, reply, js(payload), seq)
        return identifier

    def admit(self, config, goal, key, scope="local"):
        digest = fingerprint([config, goal])
        run = uuid4()
        with self.db.transaction():
            inserted = self.one("""INSERT INTO swarm_runs (run_id,scope,admission_key,fingerprint,goal,config,deadline)
                       VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp()+%s*interval '1 second')
                       ON CONFLICT (scope,admission_key) DO NOTHING RETURNING run_id""",
                       run, scope, key, digest, goal, js(config), config["bounds"]["deadline_seconds"])
            if not inserted:
                row = self.one("SELECT run_id,fingerprint FROM swarm_runs WHERE scope=%s AND admission_key=%s", scope, key)
                if row["fingerprint"] != digest:
                    raise AdmissionConflict("Admission key already used for a different request")
                return row["run_id"]
            self.event(run, "admitted", {"goal": goal, "config": config})
            for name, template in config["initial"].items():
                role = config["templates"][template]
                peer = self.peer(run, name, template, role["prompt"], role.get("ttl"))
                if name == config["coordinator"]:
                    coordinator = peer
            self.do("UPDATE swarm_runs SET coordinator=%s WHERE run_id=%s", coordinator, run)
            first = config.get("phases", [])
            recipient = coordinator if not first else self.one(
                "SELECT peer_id FROM swarm_roster WHERE run_id=%s AND name=%s", run, first[0]["peer"],
            )["peer_id"]
            self.enqueue(run, None, recipient, "assignment", {"task": goal})
        return run

    def lock(self, run):
        row = self.one("""SELECT *, lease > clock_timestamp() AS held, deadline <= clock_timestamp() AS expired
                          FROM swarm_runs WHERE run_id=%s FOR UPDATE""", run)
        if row is None:
            raise RunNotFound("Unknown run")
        return row

    def events(self, run, after=0):
        return self.all("SELECT * FROM swarm_ledger WHERE run_id=%s AND sequence>%s ORDER BY sequence", run, after)

    def inspect(self, run):
        with self.db.transaction():
            self.do("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = self.one("SELECT status,reason,final_answer FROM swarm_runs WHERE run_id=%s", run)
            if row is None:
                raise RunNotFound("Unknown run")
            events = self.events(run)
            row.update(run_id=run, run_status=row["status"], memory=[
                event["payload"]["message"] for event in events if event["kind"] in ("decision", "tool_result")
            ], normalizations=[event["payload"] for event in events if event["kind"] == "completion_normalized"],
                workflow={"completed_phases": [
                event["payload"]["phase"] for event in events if event["kind"] == "phase_complete"
            ]})
            return row

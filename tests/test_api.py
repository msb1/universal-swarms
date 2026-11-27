import json
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient

from universal_swarms.api import create_app
from universal_swarms.store import AdmissionConflict, RunNotFound


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.run = uuid4()
        self.state = {
            "run_id": self.run, "status": "active", "run_status": "active",
            "reason": None, "final_answer": None, "memory": [], "workflow": {"completed_phases": []},
        }
        self.app = create_app("test-dsn", lambda: None, Path("config/log-analysis.yaml"), model_name="test")
        store = self.enterContext(patch("universal_swarms.api.Store"))
        self.store = store.return_value.__enter__.return_value
        self.store.admit.return_value = self.run
        self.store.inspect.return_value = self.state
        self.client = self.enterContext(TestClient(self.app))

    def test_admission_and_state(self):
        response = self.client.post("/sessions", json={}, headers={"Idempotency-Key": "same-key"})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["session"]["id"], str(self.run))
        self.assertEqual(self.store.admit.call_args.args[2], "same-key")
        self.assertEqual(self.client.get(f"/sessions/{self.run}").json()["status"], "active")
        self.store.admit.side_effect = AdmissionConflict("Key already used")
        self.assertEqual(self.client.post("/sessions", json={}).status_code, 409)

    def test_validation_and_missing_resources(self):
        self.assertEqual(self.client.get("/sessions/invalid").status_code, 422)
        self.assertEqual(self.client.post("/sessions", json={"config_ref": "../other.yaml"}).status_code, 422)
        self.assertEqual(self.client.post("/sessions", json={"user_prompt": " "}).status_code, 422)
        self.assertEqual(self.client.post("/sessions", json={"config_ref": "missing.yaml"}).status_code, 404)
        self.store.inspect.side_effect = RunNotFound("Unknown run")
        self.assertEqual(self.client.get(f"/sessions/{self.run}").status_code, 404)
        self.assertEqual(self.client.post(f"/sessions/{self.run}/run/stream").status_code, 404)

    def test_stream_replays_committed_events_and_terminal_state(self):
        self.store.events.side_effect = lambda _, after: (
            [{"sequence": 4, "kind": "run_ended", "payload": {}}] if after < 4 else []
        )

        async def run_all():
            self.state.update(status="complete", run_status="complete", final_answer="Done")
            return self.state

        with patch("universal_swarms.api.Engine") as engine:
            engine.return_value.run_all.side_effect = run_all
            response = self.client.post(f"/sessions/{self.run}/run/stream", headers={"Last-Event-ID": "3"})
            engine.assert_called_once()
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/event-stream", response.headers["Content-Type"])
        self.assertIn("id: 4\nevent: run_ended", response.text)
        final = response.text.split("event: state\ndata: ")[1].strip()
        self.assertEqual(json.loads(final)["status"], "complete")
        self.assertEqual(self.store.events.call_args_list[0].args, (self.run, 3))

    def test_database_error_does_not_expose_connection_details(self):
        self.store.inspect.side_effect = psycopg.OperationalError("sensitive DSN")
        response = self.client.get(f"/sessions/{self.run}")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "Swarm database unavailable"})

    def test_openapi_declares_stream_media_type(self):
        operation = self.client.get("/openapi.json").json()["paths"]["/sessions/{run_id}/run/stream"]["post"]
        self.assertIn("text/event-stream", operation["responses"]["200"]["content"])

import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
import asyncio

from stage2 import acceptance
from universal_swarms.config import load_profile
from universal_swarms.runtime import Engine


class LogAnalysisAcceptanceTests(unittest.TestCase):
    def test_employee_acceptance_reads_normalized_audit_summary(self):
        results = [
            ("inspect_employee_audit_schema", {}, "schema"),
            ("audit_employee_phase_1", {"parser": "robust"}, json.dumps({
                "corrupted_age_ids": [1], "corrupted_salary_ids": [2], "corrupted_row_ids": [1, 2]})),
            ("calculate_new_york_median_salary", {}, "median"),
            ("generate_employee_phase_3_report", {}, "report"),
            ("validate_employee_audit", {}, '{"valid": true}'),
        ]
        memory = []
        for name, args, content in results:
            memory.extend([
                {"role": "assistant", "tool_calls": [{"id": name, "function": {"name": name, "arguments": json.dumps(args)}}]},
                {"role": "tool", "tool_call_id": name, "content": content},
            ])
        summary = "Corrupted age IDs: 1\nCorrupted salary IDs: 1\nCorrupted rows: 2"
        memory.append({"role": "assistant", "content": "<|channel>thought\n<channel|>" + summary})
        state = {"status": "complete", "run_status": "complete", "reason": None, "memory": memory,
                 "normalizations": [{"decision": 1, "content": summary}],
                 "workflow": {"completed_phases": ["phase_1_corruption_audit", "phase_2_salary_median", "phase_3_city_report"]},
                 "final_answer": "Phase 3\n| Austin | 199 | 42.62 |\n| Chicago | 181 | 42.01 |\n"
                                 "| New York | 329 | 43.02 |\n| Seattle | 183 | 43.46 |\nNew York median salary: 92500"}
        acceptance("employee-audit", state)
        state["normalizations"][0]["content"] = summary.replace("rows: 2", "rows: 3")
        with self.assertRaisesRegex(AssertionError, "Missing validated corruption audit summary"):
            acceptance("employee-audit", state)

    def state(self, malformed, answer):
        messages = []
        for name, args, output in [
            ("inspect_log", {"start_line": 1, "line_count": 1}, "log"),
            ("analyze_500_ips", {"robust_parser": True},
             json.dumps({"top": [["10.0.0.5", 83]], "malformed_records_ignored": malformed})),
            ("validate_top_offending_ip", {"ip": "10.0.0.5", "count": 83}, '{"valid": true}'),
        ]:
            messages.extend([
                {"role": "assistant", "tool_calls": [{"id": name, "function": {
                    "name": name, "arguments": json.dumps(args)}}]},
                {"role": "tool", "tool_call_id": name, "content": output},
            ])
        return {"status": "complete", "run_status": "complete", "reason": None,
                "final_answer": answer, "memory": messages}

    def test_report_matches_analysis(self):
        answer = "HTTP 500 IP: 10.0.0.5\nHTTP 500 count: 83\nMalformed records ignored: 10"
        acceptance("log-analysis", self.state(10, answer))
        with self.assertRaises(AssertionError):
            acceptance("log-analysis", self.state(10, answer.replace("ignored: 10", "ignored: 1")))
        with self.assertRaises(AssertionError):
            acceptance("log-analysis", self.state(1, answer))
        with self.assertRaises(AssertionError):
            acceptance("log-analysis", self.state(10, answer + "\nOne malformed record was excluded."))

    def test_report_contract_rejects_truncated_count(self):
        profile = load_profile("config/log-analysis.yaml")
        engine = Engine(Mock(), None, "run")
        engine.config = profile
        engine.s.all.return_value = [{"payload": {"message": {"name": "analyze_500_ips"},
            "result": {"top": [["192.0.2.1", 37]], "malformed_records_ignored": 4}, "error": None}}]
        correct = "HTTP 500 IP: 192.0.2.1\nHTTP 500 count: 37\nMalformed records ignored: 4"
        self.assertTrue(engine.final_evidence_matches(correct))
        self.assertFalse(engine.final_evidence_matches(correct.replace("count: 37", "count: 3")))
        self.assertNotIn("37", profile["final_feedback"])

    def test_profiles_do_not_include_fixture_answers(self):
        for name in ("log-analysis", "revenue-analysis", "employee-audit"):
            profile = json.dumps(load_profile(f"config/{name}.yaml"))
            for answer in ("10.0.0.5", "271017.77", "92500", "42.62", "43.46"):
                self.assertNotIn(answer, profile)

    def test_final_checks_follow_recorded_revenue_and_employee_data(self):
        engine = Engine(Mock(), None, "run")
        engine.config = load_profile("config/revenue-analysis.yaml")
        engine.s.all.return_value = [{"payload": {"message": {"name": "query_chicago_q1_revenue"},
            "result": {"total": 123.45}, "error": None}}]
        self.assertTrue(engine.final_evidence_matches("Chicago revenue: $123.45"))
        self.assertFalse(engine.final_evidence_matches("Chicago revenue: $271,017.77"))

        engine.config = load_profile("config/employee-audit.yaml")
        engine.s.all.return_value = [
            {"payload": {"message": {"name": "calculate_new_york_median_salary"},
                         "result": {"median_salary": 40000}, "error": None}},
            {"payload": {"message": {"name": "generate_employee_phase_3_report"},
                         "result": "| Austin | 2 | 35.00 |", "error": None}},
        ]
        answer = "Phase 3\n| Austin | 2 | 35.00 |\nNew York median salary: 40000"
        self.assertTrue(engine.final_evidence_matches(answer))
        self.assertFalse(engine.final_evidence_matches(answer.replace("40000", "92500")))
        self.assertFalse(engine.final_evidence_matches(answer.replace("35.00", "42.62")))

    def test_employee_validator_calculates_fixture_median(self):
        from universal_swarms.fixtures import validate_employee_audit
        rows = [(1, "A", "30", "40000", "New York"), (2, "B", "40", "60000", "new york")]
        with patch("universal_swarms.fixtures._employee_rows", new_callable=AsyncMock, return_value=rows):
            args = {"phase_1_logged": True, "normalize_city": True, "drop_invalid_age": True, "median_salary": 50000}
            self.assertTrue(json.loads(asyncio.run(validate_employee_audit(args)))["valid"])
            args["median_salary"] = 92500
            self.assertFalse(json.loads(asyncio.run(validate_employee_audit(args)))["valid"])

    def test_audit_counts_and_intermediate_summary_gate(self):
        from universal_swarms.fixtures import audit_employee_phase_1
        rows = [(1, "A", "NULL", "40000", "New York"),
                (2, "B", "30", "UNKNOWN", "Chicago"),
                (3, "C", "40;bad", "$10", "Austin")]
        with patch("universal_swarms.fixtures._employee_rows", new_callable=AsyncMock, return_value=rows):
            result = json.loads(asyncio.run(audit_employee_phase_1({"parser": "robust"})))
        self.assertEqual((result["corrupted_age_count"], result["corrupted_salary_count"], result["corrupted_row_count"]), (2, 2, 3))
        engine = Engine(Mock(), None, "run")
        engine.config = load_profile("config/employee-audit.yaml")
        phase = engine.config["phases"][0]
        engine.coordinator = "reporter"
        engine.s.all.return_value = [
            {"payload": {"phase": phase["id"], "message": {"name": "inspect_employee_audit_schema", "content": ""}, "args": {}, "result": {}, "error": None}},
            {"payload": {"phase": phase["id"], "message": {"name": "audit_employee_phase_1", "content": ""}, "args": {"parser": "robust"}, "result": result, "error": None}},
        ]
        mail = {"recipient": "auditor", "pinned": {"phase": phase}}
        correct = "Corrupted age IDs: 2\nCorrupted salary IDs: 2\nCorrupted rows: 3"
        wrong = correct.replace("age IDs: 2", "age IDs: 5")
        self.assertIsNone(engine.final_intent(mail, wrong))
        self.assertIn("corrupted age IDs", engine.completion_feedback(mail, wrong))
        self.assertEqual(engine.final_intent(mail, correct), {"intent": "phase"})
        self.assertIsNone(engine.final_intent(mail, correct + "\nOther claim: 12"))
        with self.assertRaisesRegex(ValueError, "evidence-checked summary"):
            engine.apply(mail, "send_handoff", {"target": "reporter", "task": "done"})

    def test_validation_uses_prior_calculated_result(self):
        engine = Engine(Mock(), None, "run")
        phase = load_profile("config/log-analysis.yaml")["phases"][0]
        def receipt(name, args, result):
            return {"payload": {"phase": phase["id"], "message": {"name": name, "content": ""},
                                "args": args, "result": result, "error": None}}
        engine.s.all.return_value = [
            receipt("inspect_log", {}, "sample"),
            receipt("analyze_500_ips", {"robust_parser": True}, {"top": [["192.0.2.1", 37]]}),
            receipt("validate_top_offending_ip", {"ip": "192.0.2.1", "count": 37}, {"valid": True}),
        ]
        self.assertTrue(engine.evidence(phase))
        engine.s.all.return_value[-1]["payload"]["args"]["count"] = 83
        self.assertFalse(engine.evidence(phase))

    def test_completion_feedback_identifies_format_and_evidence_errors(self):
        engine = Engine(Mock(), None, "run")
        engine.config = load_profile("config/log-analysis.yaml")
        engine.coordinator = "reporter"
        engine.s.one.return_value = None
        engine.s.all.return_value = [{"payload": {"message": {"name": "analyze_500_ips"},
            "result": {"top": [["192.0.2.1", 37]], "malformed_records_ignored": 4}, "error": None}}]
        mail = {"pinned": {"phase": None}}
        correct = "HTTP 500 IP: 192.0.2.1\nHTTP 500 count: 37\nMalformed records ignored: 4"
        feedback = engine.completion_feedback(mail, "<|channel|>" + correct)
        self.assertIn("format", feedback)
        self.assertIn("markers", feedback)
        feedback = engine.completion_feedback(mail, correct.replace("count: 37", "count: 3"))
        self.assertIn("HTTP 500 count", feedback)
        self.assertNotIn("37", feedback)

    def test_completion_feedback_identifies_missing_phase_evidence(self):
        engine = Engine(Mock(), None, "run")
        engine.config = load_profile("config/log-analysis.yaml")
        engine.s.all.return_value = []
        feedback = engine.completion_feedback({"pinned": {"phase": engine.config["phases"][0]}}, "done")
        self.assertIn("identify_500_ips", feedback)
        self.assertIn("missing", feedback)

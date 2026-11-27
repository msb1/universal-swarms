"""Operator-run upstream fixture acceptance through the swarm HTTP API."""
import argparse
import json
import os
import re

import httpx
from dotenv import load_dotenv

CASES = {
    "log-analysis": ("inspect_log", "analyze_500_ips", "validate_top_offending_ip"),
    "revenue-analysis": ("inspect_sql_schema", "query_chicago_q1_revenue", "validate_chicago_q1_revenue"),
    "employee-audit": ("inspect_employee_audit_schema", "audit_employee_phase_1", "calculate_new_york_median_salary",
                       "generate_employee_phase_3_report", "validate_employee_audit"),
}


def acceptance(case, state):
    assert state["status"] == state["run_status"] == "complete", state["reason"]
    answer = state["final_answer"]
    calls = {name: [] for name in CASES[case]}
    outcomes = {}
    for message in state["memory"]:
        for call in message.get("tool_calls", []):
            function = call["function"]
            if function["name"] in calls:
                try:
                    arguments = json.loads(function["arguments"])
                except ValueError:
                    continue
                calls[function["name"]].append((call["id"], arguments))
        if message["role"] == "tool":
            outcomes[message["tool_call_id"]] = message["content"]

    def require(name, arguments=None, contains=()):
        assert any(isinstance(args, dict) and all(args.get(k) == v for k, v in (arguments or {}).items())
                   and identifier in outcomes and all(value in outcomes[identifier] for value in contains)
                   for identifier, args in calls[name]), f"Missing required evidence for {name}"

    if case == "log-analysis":
        require("inspect_log")
        require("analyze_500_ips", {"robust_parser": True}, ("10.0.0.5", "83", "malformed_records_ignored"))
        require("validate_top_offending_ip", {"ip": "10.0.0.5", "count": 83}, ('"valid": true',))
        results = [json.loads(outcomes[identifier]) for identifier, args in calls["analyze_500_ips"]
                   if args.get("robust_parser") is True and identifier in outcomes]
        assert any(result.get("malformed_records_ignored") == 10 for result in results)
        assert re.fullmatch(
            r"HTTP 500 IP: *10\.0\.0\.5\s*\nHTTP 500 count: *83\s*\nMalformed records ignored: *10\s*",
            answer, re.IGNORECASE,
        ), "Final report must match the validated IP, count and malformed-record count"
    elif case == "revenue-analysis":
        require("inspect_sql_schema")
        require("query_chicago_q1_revenue", {"normalize_keys": True, "normalize_city": True}, ("271017.77",))
        require("validate_chicago_q1_revenue", {"total": 271017.77}, ('"valid": true',))
        assert re.search(r"271,?017\.77", answer)
    else:
        for name in CASES[case]:
            require(name)
        require("validate_employee_audit", contains=('"valid": true',))
        audit_id = next(identifier for identifier, args in calls["audit_employee_phase_1"]
                        if args.get("parser") == "robust" and identifier in outcomes)
        audit = json.loads(outcomes[audit_id])
        summary = (f"Corrupted age IDs: {len(audit['corrupted_age_ids'])}\n"
                   f"Corrupted salary IDs: {len(audit['corrupted_salary_ids'])}\n"
                   f"Corrupted rows: {len(audit['corrupted_row_ids'])}")
        raw_summaries = [message.get("content") or "" for message in state["memory"] if message.get("role") == "assistant"]
        normalized_summaries = [item["content"] for item in state.get("normalizations", [])]
        assert any(content.strip() == summary for content in raw_summaries + normalized_summaries), \
            "Missing validated corruption audit summary"
        assert state["workflow"]["completed_phases"] == [
            "phase_1_corruption_audit", "phase_2_salary_median", "phase_3_city_report"]
        assert answer.startswith("Phase 3\n") and re.search(r"New York median salary:[ \t]*92500(?:\.0+)?(?:\s|$)", answer)
        rows = [re.sub(r"[ \t\r]", "", line) for line in answer.splitlines()]
        city_rows = [row for row in rows if re.match(r"^\|(Austin|Chicago|NewYork|Seattle)\|", row)]
        assert sorted(city_rows) == sorted(["|Austin|199|42.62|", "|Chicago|181|42.01|",
                                           "|NewYork|329|43.02|", "|Seattle|183|43.46|"])


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=[*CASES, "all"])
    args = parser.parse_args()
    with httpx.Client(base_url=os.environ["AGENT_URL"], timeout=None) as client:
        for case in CASES if args.case == "all" else [args.case]:
            response = client.post("/sessions", json={"config_ref": f"{case}.yaml"})
            response.raise_for_status()
            session = response.json()["session"]
            assert session["status"] == "active" and session["config_ref"] == f"{case}.yaml"
            url = f"/sessions/{session['id']}"
            print(f"{case} run_id={session['id']}", flush=True)
            with client.stream("POST", f"{url}/run/stream", json={}) as stream:
                stream.raise_for_status()
                for line in stream.iter_lines():
                    if line.startswith("event: "):
                        print(line, flush=True)
            response = client.get(url)
            response.raise_for_status()
            acceptance(case, response.json())
            print(f"PASS {case}", flush=True)


if __name__ == "__main__":
    main()

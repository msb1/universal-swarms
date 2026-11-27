"""Run mutation acceptance cases through the swarm HTTP API."""
import argparse
import json
import os
import subprocess
from pathlib import Path

import httpx
from dotenv import load_dotenv

CASES = ("flawed-sql-generator", "compliance-policy-drift", "sycophantic-critic")


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
            run_id = session["id"]
            print(f"{case} run_id={run_id}", flush=True)
            events = []
            event_type = None
            with client.stream("POST", f"/sessions/{run_id}/run/stream", json={}) as stream:
                stream.raise_for_status()
                for line in stream.iter_lines():
                    if line.startswith("event: "):
                        event_type = line[7:]
                        print(line, flush=True)
                    elif line.startswith("data: "):
                        event = json.loads(line[6:])
                        if event_type == "error":
                            raise RuntimeError(f"Scheduler failed for {run_id}: {line[6:]}")
                        if event_type != "state":
                            events.append(event)
                        else:
                            state = event
                            if state["status"] != "complete":
                                print(f"Run {run_id} ended as {state['status']}: {state['reason']}", flush=True)
            result = subprocess.run(
                ["jq", "-er", "--arg", "case", case, "-f", str(Path(__file__).with_name("stage3.jq"))],
                input=json.dumps(events), text=True,
            )
            if result.returncode:
                parser.exit(1, f"FAIL {case} run_id={run_id}: ledger assertions failed\n")


if __name__ == "__main__":
    main()

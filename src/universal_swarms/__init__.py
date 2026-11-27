"""Python CLI for the TTL-bound swarm."""
import argparse
import asyncio
import os
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from dotenv import load_dotenv

from .config import load_profile
from .fixtures import customer_id_function_ddl
from .llm import LlmClient
from .runtime import Engine
from .store import Store, dump


def model():
    return LlmClient(os.environ["OPENAI_BASE_URL"], os.environ["OPENAI_API_KEY"], os.environ["MAIN_MODEL"])


def main():
    load_dotenv(Path.cwd() / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate")
    commands.add_parser("migrate-fixtures")
    commands.add_parser("serve")
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--config")
    run_parser.add_argument("--goal")
    run_parser.add_argument("--key")
    for name in ("resume", "inspect", "ledger"):
        commands.add_parser(name).add_argument("run_id", type=UUID)
    args = parser.parse_args()
    try:
        if args.command == "migrate-fixtures":
            with psycopg.connect(os.environ["STATE_DATABASE_URL"], connect_timeout=5) as conn:
                conn.execute(customer_id_function_ddl("public"))
            print("Fixture customer ID and text matching functions ready")
            return
        dsn = os.environ["SWARM_DATABASE_URL"]
        default = Path(os.getenv("STATE_DRIVEN_CONFIG", "config/stage1.yaml"))
        if args.command == "serve":
            from .api import serve
            serve(dsn, model, default, int(os.getenv("PORT", "8081")))
            return
        with Store(dsn) as store:
            match args.command:
                case "migrate":
                    store.migrate()
                    print("Swarm schema ready")
                case "inspect":
                    print(dump(store.inspect(args.run_id)))
                case "ledger":
                    for row in store.events(args.run_id):
                        print(dump(row))
                case "run" | "resume":
                    if args.command == "run":
                        config = load_profile(args.config or default)
                        config.setdefault("llm", {})["model"] = os.environ["MAIN_MODEL"]
                        goal = args.goal or config.get("goal")
                        if not goal:
                            raise ValueError("Specify --goal or define goal in the profile")
                        identifier = store.admit(config, goal, args.key or str(uuid4()))
                    else:
                        identifier = args.run_id
                    result = asyncio.run(Engine(store, model(), identifier).run_all())
                    print(dump(result))
                    return 0 if result["status"] == "complete" else 1
    except KeyError as error:
        parser.exit(2, f"Missing required setting or field {error}\n")
    except (ValueError, OSError) as error:
        parser.exit(2, f"{error}\n")
    except psycopg.Error as error:
        parser.exit(2, f"PostgreSQL operation failed ({type(error).__name__})\n")

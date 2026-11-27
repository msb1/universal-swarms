"""Local MCP service for agent tool execution."""
import os
from uuid import UUID

from dotenv import load_dotenv
from fastmcp import FastMCP

from .config import BUILTINS
from .fixtures import TOOLS
from .runtime import Engine
from .store import Busy, Store

mcp = FastMCP("Universal Swarms Tools")


@mcp.tool
async def execute_agent_tool(
    tool_name: str,
    run_id: UUID, owner: UUID, fence: int, message_id: UUID,
    decision: int, position: int, skipped: bool = False,
) -> dict:
    """Execute the persisted tool call for a leased swarm activation."""
    with Store(os.environ["SWARM_DATABASE_URL"]) as store:
        engine = Engine(store, None, run_id)
        engine.owner, engine.fence = owner, fence
        with store.db.transaction():
            row = store.lock(run_id)
            engine.config = row["config"]
            engine.bounds = engine.config["bounds"]
            engine.coordinator = row["coordinator"]
            engine.tools = BUILTINS | {
                name: {"name": name, "description": name.replace("_", " "), "parameters": schema}
                for name, schema in engine.config["tools"].items()
            }
            engine.guard()
            mail = store.one(
                "SELECT * FROM swarm_mailbox WHERE run_id=%s AND message_id=%s AND state='in_flight' AND fence=%s",
                run_id, message_id, fence,
            )
            if not mail:
                raise Busy("Activation is unavailable")
            record = store.one(
                "SELECT payload FROM swarm_ledger WHERE run_id=%s AND sequence=%s AND activation=%s "
                "AND kind IN ('decision','review_evaluation')",
                run_id, decision, mail["activation"],
            )
            if not record:
                raise ValueError("Decision is unavailable")
            calls = record["payload"]["message"].get("tool_calls", [])
            if position < 0 or position >= len(calls):
                raise ValueError("Tool call is unavailable")
            call = calls[position]
            if call["function"]["name"] != tool_name:
                raise ValueError("Tool does not match persisted decision")
        return await engine.execute_tool(mail, decision, position, call, skipped)


def register_tool(name: str) -> None:
    async def execute(
        run_id: UUID, owner: UUID, fence: int, message_id: UUID,
        decision: int, position: int, skipped: bool = False,
    ) -> dict:
        return await execute_agent_tool(name, run_id, owner, fence, message_id, decision, position, skipped)

    mcp.tool(name=name, description=f"Execute the persisted {name} tool call.")(execute)


for tool_name in BUILTINS.keys() | TOOLS.keys():
    register_tool(tool_name)


def main():
    load_dotenv()
    mcp.run(transport="http", host="127.0.0.1", port=int(os.getenv("SWARM_MCP_PORT", "8082")), path="/mcp")


if __name__ == "__main__":
    main()

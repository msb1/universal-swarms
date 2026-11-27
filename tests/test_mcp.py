import asyncio
import unittest
from contextlib import nullcontext
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastmcp import Client

from universal_swarms.mcp_server import mcp


class McpTests(unittest.TestCase):
    def test_persisted_call_runs_through_mcp(self):
        run, owner, message, activation = (uuid4() for _ in range(4))
        call = {"id": "call-1", "function": {"name": "yield_turn", "arguments": "{}"}}
        row = {"config": {"bounds": {"lease_seconds": 30}, "tools": {}}, "coordinator": owner}
        mail = {"activation": activation}
        expected = {"message": {"role": "tool", "content": "{}"}, "error": None}

        async def exercise():
            with patch("universal_swarms.mcp_server.Store") as factory, \
                 patch("universal_swarms.mcp_server.Engine.guard"), \
                 patch("universal_swarms.mcp_server.Engine.execute_tool", new_callable=AsyncMock) as execute:
                store = factory.return_value.__enter__.return_value
                store.db.transaction.return_value = nullcontext()
                store.lock.return_value = row
                store.one.side_effect = [mail, {"payload": {"message": {"tool_calls": [call]}}}]
                execute.return_value = expected
                with patch.dict("os.environ", {"SWARM_DATABASE_URL": "test"}):
                    async with Client(mcp) as client:
                        result = await client.call_tool("yield_turn", {
                            "run_id": str(run), "owner": str(owner), "fence": 1,
                            "message_id": str(message), "decision": 2, "position": 0,
                        })
                self.assertEqual(result.data, expected)
                execute.assert_awaited_once_with(mail, 2, 0, call, False)

        asyncio.run(exercise())

"""Shared OpenAI-compatible chat-completions adapter."""
import hashlib
import json
import math
import re
import time

import httpx


class ModelError(ValueError):
    pass


def channel_markers_only(content):
    return "<|" in content and not re.sub(
        r"<\|channel>[a-z_]*|<channel\|>|<\|(?:channel)?$|<channel\|?$",
        "", content.strip(),
    ).strip(" \t\r\n:")


def strict_json(value):
    def pairs(items):
        result = dict(items)
        if len(items) != len(result):
            raise ModelError("Duplicate JSON key")
        return result

    def invalid(_):
        raise ModelError("Non-finite JSON number")

    def finite(text):
        number = float(text)
        if not math.isfinite(number):
            invalid(text)
        return number

    return json.loads(value, object_pairs_hook=pairs, parse_constant=invalid, parse_float=finite)


def wire_messages(messages):
    wire = []
    index = 0
    while index < len(messages):
        message = messages[index]
        calls = message.get("tool_calls", [])
        if calls:
            results = messages[index + 1:index + 1 + len(calls)]
            by_id = {item.get("tool_call_id"): item for item in results if item["role"] == "tool"}
            if len(by_id) != len(calls) or set(by_id) != {call["id"] for call in calls}:
                raise ModelError("Incomplete tool history")
            for position, call in enumerate(calls):
                assistant = dict(message, tool_calls=[call])
                if position:
                    assistant.pop("content", None)
                wire.extend((assistant, by_id[call["id"]]))
            index += len(calls)
        elif message["role"] == "tool":
            raise ModelError("Orphan tool result")
        else:
            wire.append(message)
        index += 1
    return wire


class LlmClient:
    def __init__(self, base, key, model, *, transport=None):
        base = base.rstrip("/")
        suffix = "/chat/completions"
        self.urls = [base if base.endswith(suffix) else base + suffix]
        if base.endswith("/v1"):
            self.urls.append(base[:-3] + suffix)
        self.headers = {"Authorization": f"Bearer {key}"}
        self.model = model
        self.transport = transport
        self.diagnostics = []

    async def decide(self, messages, tools, config):
        self.diagnostics = []
        single = config.get("single_call", True)
        maximum = config.get("max_output_tokens", 1500)
        payload = {
            "model": config.get("model", self.model), "messages": wire_messages(messages) if single else messages,
            "temperature": config.get("temperature", 0.1), "max_tokens": maximum,
            "tool_choice": "auto" if tools else "none",
        }
        if tools:
            payload["tools"] = [{
                "type": "function",
                "function": {**tool, "parameters": {"type": "object", "properties": {}, **tool.get("parameters", {})}},
            } for tool in tools]
            if required := config.get("required_tool"):
                payload["tool_choice"] = {"type": "function", "function": {"name": required}}
            if single:
                payload["parallel_tool_calls"] = False
        size = len(json.dumps(payload, ensure_ascii=False).encode())
        if size + maximum > config.get("context_budget", 65536):
            raise ModelError("Request exceeds conservative context budget")
        async with httpx.AsyncClient(headers=self.headers, timeout=config.get("timeout", 180),
                                     transport=self.transport) as client:
            for url in self.urls:
                start = time.monotonic()
                try:
                    response = await client.post(url, json=payload)
                except httpx.RequestError as error:
                    self.diagnostics.append({"category": type(error).__name__, "seconds": time.monotonic() - start})
                    raise ModelError("Model transport failed") from None
                self.diagnostics.append({"status": response.status_code, "seconds": time.monotonic() - start})
                if response.status_code != 404 or url == self.urls[-1]:
                    break
        if not response.is_success:
            try:
                detail = response.json().get("error", {})
                message = detail.get("message") if isinstance(detail, dict) else detail
                if isinstance(message, str) and message.strip():
                    self.diagnostics[-1]["message"] = message.strip()[:2000]
            except (ValueError, AttributeError):
                pass
            raise ModelError(f"Model returned HTTP {response.status_code}")
        self.diagnostics[-1]["endpoint_key"] = hashlib.sha256(url.encode()).hexdigest()
        try:
            choice = response.json()["choices"][0]
            if choice.get("finish_reason") == "length":
                raise ModelError("Model response reached the output token limit")
            message = choice["message"]
            calls = message.get("tool_calls") or []
            if calls:
                ids = [call["id"] for call in calls]
                if any(not isinstance(item, str) or not item for item in ids) or len(set(ids)) != len(ids):
                    raise ValueError
                for call in calls:
                    if not isinstance(call["function"]["name"], str) or not isinstance(call["function"]["arguments"], str):
                        raise ValueError
                return {"role": "assistant", "content": message.get("content"), "tool_calls": calls}
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                if channel_markers_only(content):
                    raise ModelError("Model returned only channel markers")
                return {"role": "assistant", "content": content}
        except ModelError:
            raise
        except (AttributeError, KeyError, IndexError, TypeError, ValueError):
            pass
        raise ModelError("Invalid model response")

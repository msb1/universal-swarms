"""Swarm profiles and tool contracts."""
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


def tool(name, required, optional=None):
    properties = {**required, **(optional or {})}
    return {"name": name, "description": name.replace("_", " "), "parameters": {
        "type": "object", "required": list(required), "additionalProperties": False,
        "properties": {key: {"type": value} if isinstance(value, str) else value for key, value in properties.items()},
    }}


REFS = {"type": "array", "items": {"type": "object", "required": ["key", "version"],
        "additionalProperties": False, "properties": {"key": {"type": "string"}, "version": {"type": "integer", "minimum": 1}}}}
BUILTINS = {item["name"]: item for item in (
    tool("inspect_peer", {"name": "string"}),
    tool("upsert_peer", {"name": "string", "prompt": "string"}, {
        "template": "string", "expected_revision": "integer",
        "diagnosis": {"type": "string", "minLength": 1},
        "allowed_tools": {"type": "array", "uniqueItems": True, "items": {"type": "string"}},
    }),
    tool("resolve_review", {
        "disposition": {"enum": ["retry", "mutate"]},
        "diagnosis": {"type": "string", "minLength": 1},
        "target": "string", "task": "string",
    }, {
        "evidence_call_id": "string",
        "artifact_refs": REFS, "prompt": "string", "expected_revision": "integer",
        "template": "string",
        "allowed_tools": {"type": "array", "uniqueItems": True, "items": {"type": "string"}},
    }),
    tool("dispatch", {"target": "string", "task": "string"}, {"artifact_refs": REFS}),
    tool("send_handoff", {"target": "string", "task": "string"}, {"artifact_refs": REFS}),
    tool("submit_sql", {"query": {"type": "string", "minLength": 1}}, {"target": "string"}),
    tool("send_result", {"status": {"enum": ["complete", "blocked", "needs_specialist"]}, "summary": "string"},
         {"artifact_refs": REFS, "open_questions": {"type": "array", "items": {"type": "string"}}}),
    tool("retire_self", {"summary": "string"}),
    tool("yield_turn", {}),
    tool("submit_review_revision", {"content": {}}),
    tool("confirm_review_artifact", {}),
    tool("create_review_draft", {"content": {}}),
    tool("recruit_review_specialist", {"diagnosis": "string", "prompt": "string", "task": "string"}),
    tool("retry_review_specialist", {"diagnosis": "string", "task": "string"}),
    tool("write_artifact", {"key": "string", "content": {}}, {"expected_version": "integer"}),
    tool("read_artifact", {"key": "string", "version": "integer"}),
)}
BUILTINS["resolve_review"]["description"] = (
    "Resolve a failed review and explain your diagnosis. Cite evidence_call_id unless the scheduler "
    "already supplied a review receipt for a typed submission. "
    "Detect and diagnose the failure. When a peer's instructions or capabilities need to change, "
    "choose mutate with target, task and proposed prompt as top-level fields so that peer performs the work. "
    "Choose retry when existing instructions and capabilities are sufficient. "
    "Before deciding, inspect the existing peer with inspect_peer and consider its prompt. "
    "The runtime uses the inspected revision when updating an existing peer. "
    "Use a granted template and prompt when recruiting."
)
BUILTINS["submit_sql"]["description"] = "Submit the SQL statement itself for review in query."
BUILTINS["resolve_review"]["parameters"]["properties"].update({
    "task": {"type": "string", "minLength": 1, "description": "Assignment sent to the target after the review."},
    "prompt": {"type": "string", "minLength": 1, "description": "Full replacement system prompt for a mutation. This is separate from task."},
    "template": {"type": "string", "description": "Only for recruiting a new peer. Omit for an existing peer."},
    "allowed_tools": {"type": "array", "uniqueItems": True, "items": {"type": "string"},
                      "description": (
                          "REQUIRED when granting new capabilities: the complete list of tools the target "
                          "peer should have after mutation, including tools it should retain. "
                          "Omitting this field preserves its current tool grants. "
                          "Use only tools listed in the peer's permitted_tool_grants."
                      )},
})
BUILTINS["upsert_peer"]["description"] = (
    "Recruit a peer using a granted template, or update an existing peer after calling inspect_peer "
    "to review its current prompt. For an update, supply expected_revision and a diagnosis explaining "
    "why the current prompt or capabilities failed."
)
BUILTINS["inspect_peer"]["description"] = (
    "Inspect an active peer by name or UUID to review its current prompt, revision and tool grants "
    "before deciding how to address a failure."
)
BOUNDS = {"activations": 40, "model_calls": 12, "tool_calls": 16, "recruitments": 8,
          "active_peers": 8, "deadline_seconds": 900, "lease_seconds": 30, "idle_steps": 0}


def load_profile(path):
    config = yaml.safe_load(Path(path).read_text())
    if not isinstance(config, dict) or not {"coordinator", "initial", "templates"} <= config.keys():
        raise ValueError("Profile requires coordinator, initial and templates")
    config["bounds"] = {**BOUNDS, **config.get("bounds", {})}
    for key, value in config["bounds"].items():
        if key not in BOUNDS or type(value) is not int or value < (0 if key == "idle_steps" else 1):
            raise ValueError(f"Invalid bound: {key}")
    templates = config["templates"]
    for template in templates.values():
        if {"upsert_peer", "resolve_review"} & set(template.get("tools", [])):
            template["tools"] = list(dict.fromkeys([*template["tools"], "inspect_peer"]))
    initial = config["initial"]
    if config["coordinator"] not in initial or not initial or len(initial) > config["bounds"]["active_peers"]:
        raise ValueError("Invalid initial roster or coordinator")
    tools = config.setdefault("tools", {})
    from .fixtures import TOOLS
    if set(tools) - TOOLS.keys():
        raise ValueError("Unknown fixture tool")
    for name, schema in tools.items():
        if schema.get("type") != "object":
            raise ValueError(f"Expected object schema for {name}")
        Draft202012Validator.check_schema(schema)
    for name, template in templates.items():
        ttl = template.get("ttl")
        if ttl is not None and (type(ttl) is not int or ttl < 1):
            raise ValueError(f"Invalid TTL for {name}")
        if not isinstance(template.get("prompt"), str) or not template["prompt"].strip():
            raise ValueError(f"Missing prompt for {name}")
        if set(template.get("tools", [])) - (BUILTINS.keys() | tools.keys()):
            raise ValueError(f"Unknown grant for {name}")
        if set(template.get("required_tools", [])) - set(template.get("tools", [])):
            raise ValueError(f"Required tools must be granted to {name}")
        for action, prerequisites in template.get("tool_prerequisites", {}).items():
            if action not in template.get("tools", []) or not isinstance(prerequisites, list) or not prerequisites:
                raise ValueError(f"Invalid tool prerequisites for {name}")
            if len(prerequisites) != len(set(prerequisites)) or set(prerequisites) - tools.keys():
                raise ValueError(f"Unknown prerequisite tool for {name}")
        if set(template.get("recruit", [])) - templates.keys():
            raise ValueError(f"Unknown recruitment template for {name}")
        if set(template.get("mutate_peers", [])) - initial.keys():
            raise ValueError(f"Invalid prompt mutation target for {name}")
        for target, grants in template.get("mutate_tools", {}).items():
            if target not in initial or set(grants) - tools.keys():
                raise ValueError(f"Unknown mutation target or grant for {name}")
        if review := template.get("review"):
            grants = set(template.get("tools", []))
            if (review["tool"] not in tools or {review["tool"], "resolve_review"} - grants
                    or not review["success"] or set(review.get("targets", [])) - initial.keys()):
                raise ValueError(f"Invalid review configuration for {name}")
            binding = review.get("input")
            if binding and (binding.get("argument") not in tools[review["tool"]].get("properties", {})
                            or binding.get("source") not in ("task", "artifact", "submission")
                            or (binding["source"] == "artifact" and not binding.get("key"))):
                raise ValueError(f"Invalid review input binding for {name}")
            if review.get("require_specialist_confirmation") and not (
                binding and binding["source"] == "artifact" and review.get("accept_artifact")
                and review.get("specialist", {}).get("template") in template.get("recruit", [])
            ):
                raise ValueError(f"Specialist confirmation requires artifact acceptance and recruitment for {name}")
            if review.get("evaluate_verdict"):
                draft = template.get("review_draft", {})
                reviewer = templates.get(initial.get(draft.get("reviewer")), {})
                if (not binding or binding["source"] != "artifact" or draft.get("key") != binding["key"]
                        or reviewer.get("artifact_verdict") != binding["key"]
                        or not {"create_review_draft", "submit_review_revision"} <= grants):
                    raise ValueError(f"Invalid artifact verdict workflow for {name}")
            if "on_rejected_approval" in review and (
                not review.get("evaluate_verdict") or review["on_rejected_approval"] != "revise_prompt"
            ):
                raise ValueError(f"Invalid rejected approval disposition for {name}")
            if success_handoff := review.get("success_handoff"):
                if (binding is None or not (binding.get("source") == "submission" or
                        (binding.get("source") == "artifact" and review.get("evaluate_verdict")))
                        or not isinstance(success_handoff.get("result_field"), str)
                        or not isinstance(success_handoff.get("task_prefix"), str)):
                    raise ValueError(f"Invalid review success handoff for {name}")
    if set(initial.values()) - templates.keys() or templates[initial[config["coordinator"]]].get("ttl") is not None:
        raise ValueError("Initial templates must exist and coordinator TTL must be null")
    ids = set()
    for phase in config.get("phases", []):
        if phase["id"] in ids or phase["peer"] not in initial or not phase.get("evidence"):
            raise ValueError("Invalid workflow phase")
        ids.add(phase["id"])
        if any(rule["tool"] not in tools or rule["tool"] not in templates[initial[phase["peer"]]].get("tools", [])
               for rule in phase["evidence"]):
            raise ValueError("Phase evidence requires a registered tool granted to the phase peer")
    return config

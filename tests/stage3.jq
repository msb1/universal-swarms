def require($condition; $message):
  .checks += [{passed: $condition, failure: $message}];

def events($kind):
  .events[] | select(.kind == $kind);

def calls($actor; $name):
  events("tool_result") | select(.actor == $actor and .payload.message.name == $name and .payload.error == null);

def peer($name):
  first(events("peer_created") | select(.payload.name == $name)) // null;

def mutation($actor; $source; $target):
  first(events("roster_mutation")
        | select(.actor == $actor and .payload.active_agent == $source and .payload.target_agent == $target));

def handoff($actor; $target):
  calls($actor; "send_handoff") | select(.payload.result.target == $target);

def resolved_mutation($actor; $target):
  events("review_resolved") | select(.actor == $actor and .payload.target == $target
                                      and .payload.disposition == "mutate");

{events: ., checks: []}
| . as $ledger
| require(any(events("run_ended"); .payload.status == "complete"); "Run did not complete")
| peer("critic").payload.peer as $critic
| if $case == "flawed-sql-generator" then
    peer("sql_writer").payload.peer as $writer
    | [events("tool_result") | select(.actor == $critic and .payload.message.name == "run_test_query"
                                     and .payload.review_evidence != false)] as $queries
    | [events("roster_mutation") | select(.actor == $critic and .payload.target_agent == "sql_writer"
         and ((.payload.allowed_tools // []) | index("inspect_sql_schema") != null))][0] as $edit
    | require(
        any(events("claimed"); .actor == $writer and .payload.pinned.revision == 1
          and (.payload.pinned.role.tools | index("inspect_sql_schema") == null));
        "Writer started with schema access or without revision 1")
    | require($edit != null;
        "Critic did not commit a writer mutation that grants schema access")
    | require(
        any($queries[]; .sequence < $edit.sequence
          and (.payload.result.status != "ok"));
        "Missing pre-mutation execution of the naive SQL")
    | require(
        any(resolved_mutation($critic; $writer); . as $review | .payload.revision == $edit.payload.revision
          and any($queries[]; .sequence == $review.payload.evidence_sequence and .sequence < $edit.sequence))
        and any(calls($writer; "inspect_sql_schema"); . as $inspection
          | .sequence > $edit.sequence and .sequence < $queries[-1].sequence
          and any($ledger | events("claimed"); .activation == $inspection.activation and .payload.pinned.revision == $edit.payload.revision))
        and ($queries[-1].payload.result | .status == "ok" and .total == 271017.77);
        "Writer did not inspect the schema before the corrected SQL returned 271017.77")
  elif $case == "compliance-policy-drift" then
    [peer("eu_ca_legal_counsel")][0] as $recruit
    | $recruit.payload.peer as $counsel
    | [calls($critic; "check_compliance_rules")] as $checks
    | [events("peer_retired") | select(.payload.peer == $counsel and .payload.reason == "ttl")][0] as $retirement
    | require($recruit.actor == $critic and $recruit.payload.ttl == 2
        and any(resolved_mutation($critic; $counsel); .payload.evidence_sequence == $checks[0].sequence)
        and $checks[0].sequence < $recruit.sequence and $checks[0].payload.result.compliant == false;
        "Missing failed compliance check followed by critic recruitment of TTL-2 counsel")
    | require(
        $retirement != null
        and ([events("closed") | select(.actor == $counsel) | .payload.turns_used] == [1, 2])
        and any(events("closed"); .actor == $counsel and .activation == $retirement.activation and .payload.turns_used == 2);
        "Counsel did not auto-retire on the second activation")
    | require($checks[-1].sequence > $retirement.sequence and $checks[-1].payload.result == {"compliant": true}
        and any(events("artifact_written"); .actor == $counsel and .payload.key == "refund_policy"
          and .sequence < $retirement.sequence and .payload.content == $checks[-1].payload.args.policy)
        and any(handoff($critic; peer("drafter").payload.peer); .sequence > $checks[-1].sequence
          and any(.payload.args.artifact_refs[]; .key == "refund_policy" and .version > 1));
        "Missing final compliance pass on counsel's artifact and handoff to drafter")
  elif $case == "sycophantic-critic" then
    peer("architect").payload.peer as $architect
    | [events("claimed")] as $turns
    | [calls($architect; "run_load_simulator")] as $simulations
    | [mutation($architect; "architect"; "critic")][0] as $edit
    | require($turns[1].actor == $critic
        and any(handoff($critic; $architect); .activation == $turns[1].activation
          and .sequence < $simulations[0].sequence and (.payload.args.task | test("approv|looks good"; "i")))
        and ($simulations[0].payload.result | .status == 503 and .error == "StampedeError" and .rps == 10000);
        "Missing turn-2 approval followed by 503 StampedeError")
    | require($edit != null and $edit.sequence > $simulations[0].sequence
        and $edit.payload.revision == 2
        and any(resolved_mutation($architect; $critic); .payload.evidence_sequence == $simulations[0].sequence)
        ;
        "Architect did not rewrite critic after the failure")
    | require(any(handoff($critic; $architect); .sequence > $edit.sequence and .sequence < $simulations[-1].sequence
        and .payload.args.task != "")
        and $simulations[-1].payload.result.status == "ok"
        and (([events("artifact_written") | select(.actor == $architect and .payload.key == "cache_design")][-1]) as $design
          | $design != null and $design.sequence < $simulations[-1].sequence
          and $design.payload.version == $simulations[-1].payload.bound_input.artifact.version
          and $design.payload.content == $simulations[-1].payload.args.design
          and $design.payload.content.request_coalescing == true and $design.payload.content.jittered_ttls == true);
        "Missing revised critique followed by successful load simulation")
  else
    error("Unknown Phase 3 case")
  end
| .checks as $checks
| ($checks | to_entries[] | if .value.passed then "PASS check \(.key + 1)"
              else "FAIL check \(.key + 1): \(.value.failure)" end),
  (if all($checks[]; .passed) then "PASS \($case)" else "" | halt_error(1) end)

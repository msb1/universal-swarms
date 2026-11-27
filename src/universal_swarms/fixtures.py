"""Read-only fixture tools adapted from state-driven-agent."""
from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable

import psycopg
from psycopg import sql

IP = re.compile(r"^(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\s+.*?\s(?P<status>\d{3})$")
Tool = Callable[[dict[str, Any]], Awaitable[str]]
CUSTOMER_ID_CONTRACT = (
    "Customer IDs are integers. An order customer ID is text and may encode the same identifier "
    "as plain decimal digits or as ID_ followed by decimal digits, with possible leading zeros. "
    "For example, '2', '002', and 'ID_002' all refer to customer 2. "
    "Match by numeric identity. Ignore missing or invalid order identifiers without raising an error."
)
CUSTOMER_ID_FUNCTION = (
    "canonical_customer_id(text) returns the integer customer ID or NULL for a missing, invalid, "
    "or out-of-range order key. Use it on the order key when joining to customers.id."
)


def customer_id_function_ddl(schema: str):
    return sql.SQL(Path(__file__).with_name("customer_id.sql").read_text()).format(
        sql.Identifier(schema, "canonical_customer_id"),
        sql.Identifier(schema, "text_matches"),
    )


async def _connection() -> psycopg.AsyncConnection[Any]:
    return await psycopg.AsyncConnection.connect(
        os.environ["STATE_DATABASE_URL"], options="-c default_transaction_read_only=on",
        connect_timeout=5,
    )


async def _log_lines() -> list[str]:
    async with await _connection() as conn, conn.cursor() as cursor:
        await cursor.execute("SELECT line FROM agent_fixture_access_logs ORDER BY line_number")
        return [row[0] for row in await cursor.fetchall()]


async def inspect_log(args: dict[str, Any]) -> str:
    start, count = int(args["start_line"]), int(args["line_count"])
    lines = await _log_lines()
    return "\n".join(f"{index}: {line}" for index, line in enumerate(lines[start - 1:start - 1 + count], start))


async def analyze_500_ips(args: dict[str, Any]) -> str:
    robust = bool(args["robust_parser"])
    counts: Counter[str] = Counter()
    malformed = 0
    try:
        for line_number, line in enumerate(await _log_lines(), 1):
            if robust:
                match = IP.match(line)
                if not match:
                    malformed += 1
                    continue
                if match.group("status") == "500":
                    counts[match.group("ip")] += 1
            else:
                # Deliberately fragile baseline: malformed data raises instead of being hidden.
                fields = line.split(" ")
                if fields[7] == "500":
                    counts[fields[0]] += 1
    except (IndexError, ValueError) as error:
        return f"ERROR: naive parser failed at line {line_number}: {type(error).__name__}: {error}. Try robust_parser=true."
    return json.dumps({"top": counts.most_common(5), "malformed_records_ignored": malformed})


async def validate_top_offending_ip(args: dict[str, Any]) -> str:
    expected = await _expected_log_result()
    valid = args["ip"] == expected[0] and int(args["count"]) == expected[1]
    return json.dumps({"valid": valid, "message": "Validated." if valid else "Incorrect; inspect parsing and retry."})


async def _expected_log_result() -> tuple[str, int]:
    counts: Counter[str] = Counter()
    for line in await _log_lines():
        match = IP.match(line)
        if match and match.group("status") == "500":
            counts[match.group("ip")] += 1
    return counts.most_common(1)[0]


async def inspect_sql_schema(_: dict[str, Any]) -> str:
    async with await _connection() as conn, conn.cursor() as cursor:
        schemas: dict[str, list[tuple[Any, ...]]] = {}
        for table in ("agent_fixture_customers", "agent_fixture_orders"):
            await cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table,))
            schemas[table.removeprefix("agent_fixture_")] = await cursor.fetchall()
        await cursor.execute("SELECT id, name, city FROM agent_fixture_customers WHERE lower(city) = 'chicago' LIMIT 6")
        examples = await cursor.fetchall()
        await cursor.execute("SELECT order_id, cust_id, amount, ordered_at FROM agent_fixture_orders WHERE cust_id LIKE 'ID_%%' LIMIT 6")
        bad_keys = await cursor.fetchall()
    return json.dumps({
        "schema": schemas,
        "table_names": {"customers": "agent_fixture_customers", "orders": "agent_fixture_orders"},
        "customer_id_contract": CUSTOMER_ID_CONTRACT,
        "customer_id_function": "public." + CUSTOMER_ID_FUNCTION,
        "text_match_function": "public.text_matches(text, text) compares full text values case-insensitively.",
        "chicago_examples": examples, "string_key_examples": bad_keys,
    }, default=str)


async def query_chicago_q1_revenue(args: dict[str, Any]) -> str:
    keys, city = bool(args["normalize_keys"]), bool(args["normalize_city"])
    customer_key = "CAST(REPLACE(o.cust_id, 'ID_', '') AS INTEGER)" if keys else "o.cust_id::INTEGER"
    city_condition = "lower(c.city) = 'chicago'" if city else "c.city = 'Chicago'"
    total = await _revenue_total(_chicago_query(customer_key, city_condition))
    return json.dumps({"total": float(total), "normalize_keys": keys, "normalize_city": city})


def _chicago_query(customer_key: str, city_condition: str) -> str:
    return (f"SELECT ROUND(COALESCE(SUM(o.amount), 0), 2) FROM agent_fixture_orders o "
            f"JOIN agent_fixture_customers c ON c.id = {customer_key} WHERE {city_condition} "
            "AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'")


async def _normalized_chicago_q1_total():
    return await _revenue_total(_chicago_query(
        "public.canonical_customer_id(o.cust_id)", "public.text_matches(c.city, 'Chicago')",
    ))


async def calculate_chicago_q1_revenue(_: dict[str, Any]) -> str:
    total = await _normalized_chicago_q1_total()
    return json.dumps({"total": float(total)})


async def _revenue_total(sql: str):
    async with await _connection() as conn, conn.cursor() as cursor:
        await cursor.execute(sql)
        return (await cursor.fetchone())[0]


async def run_test_query(args: dict[str, Any]) -> str:
    query = args["query"]
    if not re.match(r"\s*(?:SELECT|WITH)\b", query, re.IGNORECASE):
        raise ValueError("Submit one SELECT statement")
    try:
        async with await _connection() as conn, conn.cursor() as cursor:
            await cursor.execute("SET TRANSACTION READ ONLY")
            await cursor.execute("SET LOCAL statement_timeout = '5s'")
            await cursor.execute(query, prepare=True)
            rows = await cursor.fetchmany(2)
    except (psycopg.errors.InvalidTextRepresentation, psycopg.errors.DatatypeMismatch,
            psycopg.errors.UndefinedFunction) as error:
        return json.dumps({"status": "type_error", "sqlstate": error.sqlstate,
                           "message": error.diag.message_primary})
    except (psycopg.ProgrammingError, psycopg.errors.ReadOnlySqlTransaction,
            psycopg.errors.QueryCanceled) as error:
        raise ValueError(error.diag.message_primary or str(error)) from error
    if not rows or rows == [(None,)]:
        return json.dumps({"status": "empty", "total": 0})
    if (len(rows) != 1 or len(rows[0]) != 1
            or type(rows[0][0]) not in (int, float, Decimal)
            or not math.isfinite(rows[0][0])):
        raise ValueError("Return one numeric revenue total")
    total = Decimal(str(rows[0][0]))
    reference = await _normalized_chicago_q1_total()
    if total != Decimal(str(reference)):
        return json.dumps({"status": "incorrect_total", "total": float(total),
                           "message": "The result differs from the fixture reference. Check join coverage and filters."})
    return json.dumps({"status": "ok", "total": float(total)})


def _policy_clauses(policy: str):
    heading = ""
    for line in policy.splitlines():
        text = line.strip().replace("**", "").lower()
        if not text:
            continue
        if text.startswith("#") or (line.strip().startswith("**") and line.strip().endswith("**")):
            heading = text
        for clause in re.split(r"[.!?;]+", text):
            if clause.strip():
                yield heading, clause


async def check_compliance_rules(args: dict[str, Any]) -> str:
    duration = r"\b(?:14|fourteen)[ -]days?\b"
    cooling = r"\bcooling[ -]off\b"
    cancellation = r"\b(?:cancel|withdraw)\b"
    eu = r"\b(?:eu|european union)\b"
    privacy = r"\b(?:ccpa|california consumer privacy act)\b"
    deletion = r"\bdelet(?:e|ion)\b"
    cooling_found = deletion_found = cooling_denied = deletion_denied = False
    for heading, clause in _policy_clauses(args["policy"]):
        context = f"{heading} {clause}"
        if re.search(eu, context):
            # A refund processing deadline is distinct from a cancellation window.
            window = re.search(rf"{cancellation}[^,;]*?\bwithin\s+{duration}", clause)
            cooling_found |= bool(window or (re.search(cooling, clause) and re.search(duration, clause)))
        if re.search(cooling, clause) or (re.search(eu, context) and re.search(cancellation, clause)):
            cooling_denied |= bool(re.search(
                rf"\b(?:no|without)\s+(?:(?:a|an|the)\s+)?(?:{duration}\s+)?{cooling}|"
                rf"{cooling}.*?\b(?:does not apply|unavailable|waived)\b|"
                rf"\b(?:cannot|may not|no right to)\s+{cancellation}", clause))
        if re.search(privacy, context) and re.search(deletion, clause):
            deletion_found |= bool(re.search(r"\b(?:right|request|may|can|will)\b", clause))
            deletion_denied |= bool(re.search(
                r"\b(?:cannot|no right|do not|not allowed)\b.*?delet|delet.*?\b(?:denied|not honored)\b", clause))
    missing = []
    if not cooling_found or cooling_denied:
        missing.append("EU 14-day cooling-off disclosure")
    if not deletion_found or deletion_denied:
        missing.append("California CCPA data-deletion right")
    if not missing:
        return json.dumps({"compliant": True})
    return json.dumps({
        "compliant": False, "missing_requirements": missing,
        "constraint": "Jurisdiction is EU + California; requires 14-day cooling-off disclosures and "
                      "CCPA data-deletion clauses, which the current drafter lacks legal expertise to write.",
    })


async def run_load_simulator(args: dict[str, Any]) -> str:
    requests_per_second, cache_keys = 10000, 100
    design = args.get("design", args)
    expiration_cohorts = 10 if design["jittered_ttls"] else 1
    origin_requests = cache_keys if design["request_coalescing"] else requests_per_second
    if origin_requests // expiration_cohorts <= 20:
        return json.dumps({"status": "ok"})
    return json.dumps({"status": 503, "error": "StampedeError", "rps": requests_per_second})


async def validate_chicago_q1_revenue(args: dict[str, Any]) -> str:
    expected = json.loads(await query_chicago_q1_revenue({"normalize_keys": True, "normalize_city": True}))["total"]
    valid = round(float(args["total"]), 2) == expected
    return json.dumps({"valid": valid, "expected_hint": "Normalize city case and ID_ foreign keys." if not valid else "Validated."})


async def _employee_rows() -> list[tuple[int, str, str, str, str]]:
    async with await _connection() as conn, conn.cursor() as cursor:
        await cursor.execute("SELECT emp_id, name, age, salary, city FROM agent_fixture_employees ORDER BY emp_id")
        return await cursor.fetchall()


def _employee_failure(row: int, field: str, value: str) -> str:
    """A deliberately verbose, deterministic failure for compaction testing."""
    window = "\n".join(
        f"  {number:>4} | parsed_{field}[{number}] = {field}_parser(record[{number!r}])"
        for number in range(max(1, row - 8), row + 8)
    )
    return (
        f"ERROR: Phase 1 parser crashed at employee {row}; {field}={value!r}.\n"
        "Traceback (most recent call last):\n"
        "  File \"/simulated/audit.py\", line 84, in parse_employee_batch\n"
        f"    normalized_{field} = {field}_parser(raw_{field})\n"
        f"  File \"/simulated/audit.py\", line 39, in {field}_parser\n"
        f"    raise ValueError('unsupported {field} representation')\n"
        f"ValueError: unsupported {field} representation {value!r}\n"
        "Parser window (not a successful audit):\n"
        f"{window}\n"
        "Revise only the parser assumption exposed by this failure, then retry incrementally."
    )


def _employee_metric_failure(message: str) -> str:
    """Emit assertion detail representative of a failed metric test run."""
    attempts = "\n".join(
        f"  assertion-frame {index:02d}: candidate=[raw salary values]; city predicate and invalid-value filter under test"
        for index in range(1, 33)
    )
    return f"{message}\nMetric test runner context:\n{attempts}\nRevise the stated normalization rule and retry the metric."


async def inspect_employee_audit_schema(_: dict[str, Any]) -> str:
    rows = await _employee_rows()
    return json.dumps({
        "schema": {"table": "employees", "columns": ["emp_id INTEGER", "name TEXT", "age TEXT", "salary TEXT", "city TEXT"]},
        "record_count": len(rows),
        "examples": [rows[index] for index in (8, 9, 14, 21, 27)],
        "rules": "age is invalid when NULL or semicolon-delimited; salary is invalid when currency-formatted or UNKNOWN; city matching is case-insensitive.",
    })


async def audit_employee_phase_1(args: dict[str, Any]) -> str:
    parser = args["parser"]
    failures = {
        "naive": (10, "salary", "$80,000"),
        "currency_cleaned": (15, "age", "NULL"),
        "null_cleaned": (22, "age", "41; extra_field_corrupted"),
    }
    if parser in failures:
        return _employee_failure(*failures[parser])
    if parser != "robust":
        return "ERROR: parser must be naive, currency_cleaned, null_cleaned, or robust."
    rows = await _employee_rows()
    bad_age = [ident for ident, _, age, _, _ in rows if age == "NULL" or ";" in age]
    bad_salary = [ident for ident, _, _, salary, _ in rows if salary == "UNKNOWN" or salary.startswith("$")]
    bad_rows = sorted(set(bad_age) | set(bad_salary))
    return json.dumps({"phase": "Phase 1", "corrupted_age_ids": bad_age, "corrupted_salary_ids": bad_salary,
                       "corrupted_row_ids": bad_rows, "corrupted_age_count": len(bad_age),
                       "corrupted_salary_count": len(bad_salary), "corrupted_row_count": len(bad_rows), "status": "logged"})


async def calculate_new_york_median_salary(args: dict[str, Any]) -> str:
    city_normalized, invalid_excluded = bool(args["normalize_city"]), bool(args["exclude_invalid_salary"])
    if not invalid_excluded:
        return _employee_metric_failure("AssertionError: salary array contains UNKNOWN; filter invalid salary fields before median().")
    if not city_normalized:
        return _employee_metric_failure("AssertionError: Match city case-insensitively before calculating the median.")
    salaries = [float(salary) for _, _, _, salary, city in await _employee_rows()
                if city.lower() == "new york" and salary != "UNKNOWN" and not salary.startswith("$")]
    salaries.sort()
    middle = len(salaries) // 2
    median = (salaries[middle - 1] + salaries[middle]) / 2 if len(salaries) % 2 == 0 else salaries[middle]
    return json.dumps({"phase": "Phase 2", "median_salary": median, "valid_new_york_employees": len(salaries), "status": "calculated"})


async def generate_employee_phase_3_report(args: dict[str, Any]) -> str:
    city_normalized, invalid_age_dropped = bool(args["normalize_city"]), bool(args["drop_invalid_age"])
    if not city_normalized:
        return "ERROR: Phase 3 would split New York and new york. Set normalize_city=true."
    if not invalid_age_dropped:
        return "ERROR: Phase 3 cannot average age while NULL and semicolon-corrupted ages remain. Set drop_invalid_age=true."
    groups: dict[str, list[int]] = {}
    for _, _, age, _, city in await _employee_rows():
        if age == "NULL" or ";" in age:
            continue
        groups.setdefault(city.title(), []).append(int(age))
    lines = ["Phase 3 — clean city audit", "| City | Valid headcount | Average age |", "| --- | ---: | ---: |"]
    for city in sorted(groups):
        ages = groups[city]
        lines.append(f"| {city} | {len(ages)} | {sum(ages) / len(ages):.2f} |")
    # This is intentionally large enough that the *next* validation action
    # crosses the 2,000-token threshold. The final model response is instructed
    # to present the compact table above, not this diagnostic trace.
    trace = "\n".join(
        f"normalization-check {index:02d}: city=lowercase, age=NULL-or-semicolon excluded, salary=UNKNOWN-or-currency excluded, aggregate=verified"
        for index in range(1, 49)
    )
    return "\n".join(lines) + "\n\nPhase 3 normalization trace (diagnostic evidence):\n" + trace


async def validate_employee_audit(args: dict[str, Any]) -> str:
    calculated = json.loads(await calculate_new_york_median_salary({
        "normalize_city": True, "exclude_invalid_salary": True
    }))["median_salary"]
    valid = (bool(args["phase_1_logged"]) and bool(args["normalize_city"]) and bool(args["drop_invalid_age"])
             and round(float(args["median_salary"]), 2) == round(calculated, 2))
    return json.dumps({"valid": valid,
                       "hint": "Check phase 1, normalization, invalid ages, and the calculated median." if not valid else "Validated all three phases."})


TOOLS: dict[str, Tool] = {
    "run_test_query": run_test_query, "check_compliance_rules": check_compliance_rules,
    "calculate_chicago_q1_revenue": calculate_chicago_q1_revenue,
    "run_load_simulator": run_load_simulator,
    "inspect_log": inspect_log, "analyze_500_ips": analyze_500_ips, "validate_top_offending_ip": validate_top_offending_ip,
    "inspect_sql_schema": inspect_sql_schema, "query_chicago_q1_revenue": query_chicago_q1_revenue, "validate_chicago_q1_revenue": validate_chicago_q1_revenue,
    "inspect_employee_audit_schema": inspect_employee_audit_schema, "audit_employee_phase_1": audit_employee_phase_1,
    "calculate_new_york_median_salary": calculate_new_york_median_salary,
    "generate_employee_phase_3_report": generate_employee_phase_3_report, "validate_employee_audit": validate_employee_audit,
}


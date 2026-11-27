#!/bin/zsh
set -eu
cd "${0:A:h}/.."

uv run --locked python - <<'PY'
import os
import re
from decimal import Decimal

import httpx
import psycopg
from dotenv import load_dotenv
from universal_swarms.fixtures import CUSTOMER_ID_CONTRACT, CUSTOMER_ID_FUNCTION, customer_id_function_ddl

load_dotenv(".env")
base_url = os.environ["OPENAI_BASE_URL"].rstrip("/")
endpoint = base_url if base_url.endswith("/chat/completions") else f"{base_url}/chat/completions"
model = os.environ["MAIN_MODEL"]

fixture = """
CREATE TEMP TABLE customers (id integer PRIMARY KEY, city text NOT NULL);
CREATE TEMP TABLE orders (
    order_id integer PRIMARY KEY, cust_id text, amount numeric(12,2) NOT NULL, ordered_at date NOT NULL
);
INSERT INTO customers VALUES (2, 'Chicago'), (7, 'chicago'), (9, 'CHICAGO'), (11, 'Boston');
INSERT INTO orders VALUES
    (1, '2', 100.00, '2026-01-01'),
    (2, 'ID_002', 40.00, '2026-01-12'),
    (3, 'ID_007', 25.00, '2026-02-15'),
    (4, '9', 15.00, '2026-03-31'),
    (5, '2', 500.00, '2026-04-01'),
    (6, 'ID_011', 700.00, '2026-02-01'),
    (7, '2', 900.00, '2025-12-31');
"""
extra_rows = """
INSERT INTO customers VALUES (12, 'Chicago Heights');
INSERT INTO orders VALUES
    (8, 'UNKNOWN', 300.00, '2026-02-02'),
    (9, NULL, 400.00, '2026-02-02'),
    (10, 'ID_009', 8.75, '2026-02-02'),
    (11, 'ID_012', 1050.00, '2026-02-02');
"""
cases = [
    ("basic aggregate",
     "Return the total of all orders placed in Q1 2026, including January 1 and March 31. No customer lookup is needed.",
     "880.00"),
    ("date boundary",
     """Fix this query, which includes extra revenue for Q1 2026. Preserve the rest of its behavior.
SELECT SUM(amount) AS total_revenue
FROM orders WHERE ordered_at BETWEEN '2026-01-01' AND '2026-04-01';""",
     "880.00"),
    ("cast error",
     """This query fails with PostgreSQL error 22P02 because ID_002 cannot be cast directly to integer.
Rewrite it to count all Q1 2026 orders belonging to customer 2, including prefixed identifiers.
SELECT SUM(o.amount) AS total_revenue
FROM orders o JOIN customers c ON c.id = o.cust_id::integer
WHERE c.id = 2 AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01';""",
     "140.00"),
    ("executing query undercounts",
     """This query runs but undercounts. Count all Q1 2026 orders belonging to Chicago customers.
Some order identifiers have prefixes or leading zeros. Match city names case-insensitively.
SELECT SUM(o.amount) AS total_revenue
FROM orders o JOIN customers c ON c.id::text = o.cust_id
WHERE c.city = 'Chicago' AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01';""",
     "180.00"),
    ("combined diagnosis",
     """Rewrite this query to compute total revenue for Chicago customers in Q1 2026.
It fails with 22P02: invalid input syntax for type integer: "ID_002".
Use the schema and sample rows to determine all necessary corrections.
SELECT SUM(o.amount) AS total_revenue
FROM orders o JOIN customers c ON c.id = o.cust_id::integer
WHERE c.city = 'Chicago' AND o.ordered_at BETWEEN '2026-01-01' AND '2026-04-01';""",
     "180.00"),
    ("generalization",
     """Write a query for total Chicago revenue in Q1 2026. Match the city case-insensitively.
Order customer IDs can be plain integers represented as text or prefixed identifiers like ID_002.
Exclude rows with missing or invalid identifiers without failing the query.""",
     "180.00"),
]
instructions = (
    "Write PostgreSQL SQL for the fixture below. Return exactly one executable SELECT statement, "
    "optionally starting with WITH, as plain SQL. It must calculate one row with one numeric "
    "column named total_revenue from the fixture data.\n\n"
)


def evaluate(connection, sql, expected):
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '5s'")
        cursor.execute(sql, prepare=True)
        columns = [column.name for column in cursor.description]
        rows = cursor.fetchmany(2)
    if columns != ["total_revenue"] or len(rows) != 1 or type(rows[0][0]) not in (int, float, Decimal):
        return f"FAIL result format: columns={columns!r}, rows={rows!r}"
    actual = Decimal(str(rows[0][0]))
    return None if actual == Decimal(expected) else f"FAIL correctness: got {actual}, expected {expected}"


def extract_sql(content):
    blocks = re.findall(r"^```(?:sql|postgresql)?[ \t]*\r?\n(.*?)^```[ \t]*\r?$",
                        content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
    if len(blocks) > 1:
        raise ValueError("multiple SQL blocks; expected one unambiguous answer")
    sql = (blocks[0] if blocks else content).strip()
    if not re.match(r"(?:SELECT|WITH)\b", sql, re.I):
        raise ValueError("expected one SQL SELECT, either plain or in a SQL block")
    return sql


def run_case(client, prompt, expected, generalize, data_contract, use_function):
    response = client.post(endpoint, json={
        "model": model, "temperature": 0, "max_tokens": 2048,
        "messages": [{"role": "user", "content": instructions + fixture + "\n" + data_contract + prompt}],
    })
    response.raise_for_status()
    choice = response.json()["choices"][0]
    content = (choice["message"].get("content") or "").strip()
    print(content or "(empty response)", flush=True)
    if choice.get("finish_reason") == "length":
        return "FAIL format: model response reached the output token limit"
    try:
        sql = extract_sql(content)
    except ValueError as error:
        return f"FAIL format: {error}"
    with psycopg.connect(os.environ["STATE_DATABASE_URL"], autocommit=True, connect_timeout=5) as connection:
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ WRITE")
            cursor.execute(fixture)
            if use_function:
                cursor.execute(customer_id_function_ddl("pg_temp"))
        failure = evaluate(connection, sql, expected)
        if failure or not generalize:
            return failure
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ WRITE")
            cursor.execute(extra_rows)
        return evaluate(connection, sql, "188.75")


scores = {}
print(f"Model: {model}", flush=True)
with httpx.Client(headers={"Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY', '')}"}, timeout=180) as client:
    for mode, contract, use_function in (
        ("Baseline prompts", "", False),
        ("Explicit identifier contract", CUSTOMER_ID_CONTRACT + "\n\n", False),
        ("Normalization function", CUSTOMER_ID_CONTRACT + "\n"
         "Available PostgreSQL function: pg_temp." + CUSTOMER_ID_FUNCTION + "\n"
         "Use it when the task requires matching order and customer IDs. "
         "Q1 includes January 1 through March 31, 2026. Exclude April 1.\n\n", True),
    ):
        scores[mode] = 0
        print(f"\n{mode}", flush=True)
        for number, (name, prompt, expected) in enumerate(cases, 1):
            print(f"\n{number}. {name}", flush=True)
            try:
                case_contract = contract if number >= 3 else ""
                if use_function and number >= 4:
                    case_contract += (
                        "Use pg_temp.text_matches(city, 'Chicago') for full-name matching. "
                        "It includes Chicago, chicago, and CHICAGO and excludes other city names.\n\n"
                    )
                failure = run_case(client, prompt, expected, number == len(cases),
                                   case_contract, use_function and number >= 3)
            except httpx.HTTPError as error:
                failure = f"FAIL model request: {error}"
            except psycopg.Error as error:
                failure = f"FAIL database execution: {error}"
            except (ValueError, KeyError, IndexError, TypeError) as error:
                failure = f"FAIL response format: {error}"
            print(failure or "PASS", flush=True)
            scores[mode] += failure is None
for mode, passed in scores.items():
    print(f"{mode}: {passed}/{len(cases)} passed")
raise SystemExit(any(passed != len(cases) for passed in scores.values()))
PY

"""Query bridge flows from Dune API per chain pair."""

import csv
import itertools
import json
import os
import sys
import time

import requests

DUNE_API_BASE = "https://api.dune.com/api"
CONSIDERED_CHAINS = [
    "ethereum", "arbitrum", "optimism", "base", "polygon", "hyperliquid",
    # "bnb", "avalanche_c",

    # "gnosis", "zksync", "linea",
    # "scroll", "mantle", "blast", "fantom", "nova",
    # "zora", "celo",
]

LIMIT = 10000

def _build_query(src: str, dst: str) -> str:
    return (
        "SELECT * FROM bridges_evms.flows "
        f"WHERE deposit_chain = '{src}' "
        f"AND withdrawal_chain = '{dst}' "
        f"LIMIT {LIMIT}"
    )


def get_api_key() -> str:
    key = os.environ.get("DUNE_API_KEY")
    if not key:
        print("Error: Set DUNE_API_KEY environment variable")
        sys.exit(1)
    return key


def execute_query(api_key: str, sql: str) -> dict:
    """Execute SQL query via Dune API and wait for results."""
    headers = {"X-Dune-API-Key": api_key, "Content-Type": "application/json"}

    resp = requests.post(
        f"{DUNE_API_BASE}/v1/sql/execute",
        headers=headers,
        json={"sql": sql, "performance": "medium"},
    )
    resp.raise_for_status()
    data = resp.json()
    execution_id = data["execution_id"]
    print(f"  Execution started: {execution_id}")

    while True:
        resp = requests.get(
            f"{DUNE_API_BASE}/v1/execution/{execution_id}/results",
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state", data.get("execution", {}).get("state"))

        if state == "QUERY_STATE_COMPLETED":
            rows = data["result"]["rows"]
            print(f"  Completed. Rows: {len(rows)}")
            return data
        elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
            print(f"  Query failed: {state}")
            return {"result": {"rows": []}}
        else:
            print(f"  State: {state}, waiting...")
            time.sleep(2)


def parse_rows(data: dict) -> list:
    rows = data["result"]["rows"]
    fields = [
        "deposit_chain", "deposit_chain_id", "withdrawal_chain", "withdrawal_chain_id",
        "bridge_name", "bridge_version",
        "deposit_block_date", "deposit_block_time", "deposit_block_number",
        "withdrawal_block_date", "withdrawal_block_time", "withdrawal_block_number",
        "deposit_amount_raw", "deposit_amount", "withdrawal_amount_raw", "withdrawal_amount",
        "amount_usd", "sender", "recipient",
        "deposit_token_standard", "withdrawal_token_standard",
        "deposit_token_address", "withdrawal_token_address",
        "deposit_tx_from", "deposit_tx_hash", "withdrawal_tx_hash",
        "bridge_transfer_id", "duplicate_index",
    ]
    return [{f: row.get(f) for f in fields} for row in rows]


def save_csv(rows: list, path: str):
    if not rows:
        print(f"  No rows, skipping {path}")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows)} rows to {path}")


def main():
    api_key = get_api_key()

    # Generate all ordered pairs (A->B and B->A)
    pairs = list(itertools.permutations(CONSIDERED_CHAINS, 2))
    total = len(pairs)
    print(f"Running {total} queries for {len(CONSIDERED_CHAINS)} chains\n")

    flows_dir = "flows"
    os.makedirs(flows_dir, exist_ok=True)

    total_rows = 0
    skipped = 0
    for i, (src, dst) in enumerate(pairs, 1):
        csv_path = os.path.join(flows_dir, f"bridge_flows_{src}_{dst}.csv")
        if os.path.exists(csv_path):
            print(f"[{i}/{total}] {src} -> {dst} — already exists, skipping")
            skipped += 1
            continue

        print(f"[{i}/{total}] {src} -> {dst}")
        sql = _build_query(src, dst)
        data = execute_query(api_key, sql)
        rows = parse_rows(data)
        total_rows += len(rows)

        save_csv(rows, csv_path)
        print()

    print(f"Done. {total_rows} new rows, {skipped} pairs skipped (already existed).")


if __name__ == "__main__":
    main()

"""Post-Bridge Activity Analyzer.

Reads flows/bridge_flows_{src}_{dst}.csv files for all chain pairs from
CONSIDERED_CHAINS. Finds withdrawal txs where missing, finds next 3 outgoing
txs per recipient on the destination chain. Outputs per-pair CSV files in
analysis/ directory. Parallelizes work across destination chains.
"""

import argparse
import csv
import glob
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional, Tuple

import requests

from query_bridge_flows import CONSIDERED_CHAINS

# ---------------------------------------------------------------------------
# Chain → RPC mapping
# ---------------------------------------------------------------------------

ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY", "")

ALCHEMY_CHAINS = {
    "ethereum": "eth-mainnet",
    "polygon": "polygon-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
    "base": "base-mainnet",
    "zksync": "zksync-mainnet",
    "linea": "linea-mainnet",
    "hyperliquid": "hyperliquid-mainnet",
}

PUBLIC_RPCS = {
    "bnb": "https://bsc-dataseed.binance.org",
    "avalanche_c": "https://api.avax.network/ext/bc/C/rpc",
    "hyperliquid": "https://rpc.hyperliquid.xyz/evm",
    "scroll": "https://rpc.scroll.io",
    "mantle": "https://rpc.mantle.xyz",
    "zora": "https://rpc.zora.energy",
    "blast": "https://rpc.blast.io",
    "nova": "https://nova.arbitrum.io/rpc",
    "gnosis": "https://rpc.gnosischain.com",
}

EXPLORER_APIS = {
    "bnb": "https://api.bscscan.com/api",
    "avalanche_c": "https://api.snowtrace.io/api",
    "scroll": "https://api.scrollscan.com/api",
    "mantle": "https://api.mantlescan.xyz/api",
    "blast": "https://api.blastscan.io/api",
    "nova": "https://api-nova.arbiscan.io/api",
    "gnosis": "https://api.gnosisscan.io/api",
}

FLOWS_DIR = "flows"
ANALYSIS_DIR = "analysis"
CACHE_FILE = "analysis_cache.json"
RPC_TIMEOUT = 20
WITHDRAWAL_SEARCH_MIN = 30 * 60
WITHDRAWAL_SEARCH_MAX = 4 * 60 * 60

# Per-chain rate limiters (so parallel chains don't block each other)
_chain_locks = {}
_chain_last_call = {}

OUTPUT_FIELDS = [
    "source_chain", "destination_chain", "original_amount", "original_amount_usd",
    "withdrawal_tx_hash", "withdrawal_block_number", "recipient",
    "next_tx_hash_1", "next_block_1", "next_to_1", "next_value_1",
    "next_tx_hash_2", "next_block_2", "next_to_2", "next_value_2",
    "next_tx_hash_3", "next_block_3", "next_to_3", "next_value_3",
]

# ---------------------------------------------------------------------------
# Per-chain rate limiting
# ---------------------------------------------------------------------------

def _get_chain_lock(chain: str) -> threading.Lock:
    if chain not in _chain_locks:
        _chain_locks[chain] = threading.Lock()
        _chain_last_call[chain] = 0.0
    return _chain_locks[chain]


def _rate_limit_chain(chain: str, delay: float = 0.2):
    lock = _get_chain_lock(chain)
    with lock:
        elapsed = time.time() - _chain_last_call.get(chain, 0)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        _chain_last_call[chain] = time.time()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_rpc_url(chain: str) -> Optional[str]:
    if chain in ALCHEMY_CHAINS:
        if not ALCHEMY_API_KEY:
            return None
        return f"https://{ALCHEMY_CHAINS[chain]}.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    if chain in PUBLIC_RPCS:
        return PUBLIC_RPCS[chain]
    return None


def is_alchemy_chain(chain: str) -> bool:
    return chain in ALCHEMY_CHAINS


def rpc_call(chain: str, url: str, method: str, params: Optional[list] = None) -> dict:
    """JSON-RPC call with per-chain rate limiting and retry."""
    _rate_limit_chain(chain)
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    for attempt in range(2):
        try:
            resp = requests.post(url, json=payload, timeout=RPC_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(f"RPC error: {data['error']}")
            return data.get("result")
        except requests.exceptions.Timeout:
            if attempt == 0:
                continue
            raise
        except requests.exceptions.ConnectionError:
            if attempt == 0:
                time.sleep(1)
                continue
            raise


def hex_to_int(h: str) -> int:
    if not h:
        return 0
    return int(h, 16)


def int_to_hex(n: int) -> str:
    return hex(n)

# ---------------------------------------------------------------------------
# Block timestamp estimation
# ---------------------------------------------------------------------------

CHAIN_BLOCK_INFO = {
    "ethereum":    {"block_time": 12.0,  "anchor": (17_000_000, 1681338455)},
    "polygon":     {"block_time": 2.0,   "anchor": (42_000_000, 1681338000)},
    "arbitrum":    {"block_time": 0.25,  "anchor": (80_000_000, 1681338000)},
    "optimism":    {"block_time": 2.0,   "anchor": (90_000_000, 1681338000)},
    "base":        {"block_time": 2.0,   "anchor": (1_000_000,  1691070000)},
    "zksync":      {"block_time": 1.0,   "anchor": (10_000_000, 1681338000)},
    "linea":       {"block_time": 3.0,   "anchor": (1_000_000,  1694000000)},
    "bnb":         {"block_time": 3.0,   "anchor": (27_000_000, 1681338000)},
    "avalanche_c": {"block_time": 2.0,   "anchor": (28_000_000, 1681338000)},
    "scroll":      {"block_time": 3.0,   "anchor": (1_000_000,  1697000000)},
    "mantle":      {"block_time": 2.0,   "anchor": (1_000_000,  1694000000)},
    "zora":        {"block_time": 2.0,   "anchor": (1_000_000,  1694000000)},
    "blast":       {"block_time": 2.0,   "anchor": (1_000_000,  1709000000)},
    "nova":        {"block_time": 0.25,  "anchor": (10_000_000, 1681338000)},
    "gnosis":      {"block_time": 5.0,   "anchor": (27_000_000, 1681338000)},
    "hyperliquid": {"block_time": 2.0,   "anchor": (1_000_000,  1709000000)},
}

_latest_block_cache = {}
_latest_block_lock = threading.Lock()
LATEST_BLOCK_TTL = 300


def estimate_block_from_timestamp(chain: str, target_ts: int) -> int:
    info = CHAIN_BLOCK_INFO.get(chain)
    if not info:
        return 0
    anchor_block, anchor_ts = info["anchor"]
    diff_blocks = int((target_ts - anchor_ts) / info["block_time"])
    return max(0, anchor_block + diff_blocks)


def get_block_timestamp(chain: str, rpc_url: str, block_num: int) -> int:
    result = rpc_call(chain, rpc_url, "eth_getBlockByNumber", [int_to_hex(block_num), False])
    if not result:
        return 0
    return hex_to_int(result["timestamp"])


def get_latest_block(chain: str, rpc_url: str) -> int:
    with _latest_block_lock:
        if chain in _latest_block_cache:
            cached_block, cached_time = _latest_block_cache[chain]
            if time.time() - cached_time < LATEST_BLOCK_TTL:
                return cached_block
    result = rpc_call(chain, rpc_url, "eth_blockNumber")
    block = hex_to_int(result)
    with _latest_block_lock:
        _latest_block_cache[chain] = (block, time.time())
    return block


def find_block_by_timestamp(chain: str, rpc_url: str, target_ts: int, latest_block: int) -> int:
    estimate = estimate_block_from_timestamp(chain, target_ts)
    if estimate > 0:
        lo = max(0, estimate - 5000)
        hi = min(latest_block, estimate + 5000)
    else:
        lo, hi = 0, latest_block
    best = lo
    for _ in range(15):
        if lo > hi:
            break
        mid = (lo + hi) // 2
        ts = get_block_timestamp(chain, rpc_url, mid)
        if ts == 0:
            lo = mid + 1
            continue
        if ts <= target_ts:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best

# ---------------------------------------------------------------------------
# Step 2: Find withdrawal tx
# ---------------------------------------------------------------------------

def parse_deposit_time(deposit_block_time: str) -> int:
    dt_str = deposit_block_time.strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f %Z",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(dt_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    dt_str = dt_str.replace(" UTC", "").strip()
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def find_withdrawal_tx_alchemy(chain: str, rpc_url: str, recipient: str,
                               from_block: int, to_block: int) -> Tuple[Optional[str], Optional[int]]:
    params = [{
        "toAddress": recipient,
        "category": ["external", "erc20"],
        "fromBlock": int_to_hex(from_block),
        "toBlock": int_to_hex(to_block),
        "maxCount": int_to_hex(5),
        "order": "asc",
    }]
    try:
        result = rpc_call(chain, rpc_url, "alchemy_getAssetTransfers", params)
    except Exception as e:
        print(f"  Alchemy withdrawal error: {e}", flush=True)
        return None, None
    transfers = (result or {}).get("transfers", [])
    if transfers:
        tx = transfers[0]
        return tx.get("hash"), hex_to_int(tx.get("blockNum", "0x0"))
    return None, None


def explorer_get_txlist(chain: str, address: str, startblock: int, endblock: int,
                        sort: str = "asc") -> list:
    api_url = EXPLORER_APIS.get(chain)
    if not api_url:
        return []
    _rate_limit_chain(chain)
    params = {
        "module": "account", "action": "txlist", "address": address,
        "startblock": startblock, "endblock": endblock,
        "sort": sort, "page": 1, "offset": 50,
    }
    try:
        resp = requests.get(api_url, params=params, timeout=RPC_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Explorer API error ({chain}): {e}", flush=True)
        return []
    if data.get("status") != "1":
        return []
    return data.get("result", [])


def find_withdrawal_tx_explorer(chain: str, recipient: str,
                                from_block: int, to_block: int) -> Tuple[Optional[str], Optional[int]]:
    txs = explorer_get_txlist(chain, recipient, from_block, to_block, sort="asc")
    recipient_lower = recipient.lower()
    for tx in txs:
        if (tx.get("to") or "").lower() == recipient_lower:
            return tx.get("hash"), int(tx.get("blockNumber", 0))
    return None, None


def find_withdrawal_tx_rpc_fallback(chain: str, rpc_url: str, recipient: str,
                                    from_block: int, to_block: int) -> Tuple[Optional[str], Optional[int]]:
    recipient_lower = recipient.lower()
    scan_limit = min(to_block, from_block + 100)
    for block_num in range(from_block, scan_limit + 1):
        result = rpc_call(chain, rpc_url, "eth_getBlockByNumber", [int_to_hex(block_num), True])
        if not result or not result.get("transactions"):
            continue
        for tx in result["transactions"]:
            if (tx.get("to") or "").lower() == recipient_lower:
                return tx["hash"], block_num
    return None, None


def find_withdrawal_tx(chain: str, rpc_url: str, recipient: str,
                       deposit_block_time: str) -> Tuple[Optional[str], Optional[int]]:
    deposit_ts = parse_deposit_time(deposit_block_time)
    ts_min = deposit_ts + WITHDRAWAL_SEARCH_MIN
    ts_max = deposit_ts + WITHDRAWAL_SEARCH_MAX

    from_block = estimate_block_from_timestamp(chain, ts_min)
    to_block = estimate_block_from_timestamp(chain, ts_max)
    if from_block >= to_block:
        return None, None

    if is_alchemy_chain(chain):
        return find_withdrawal_tx_alchemy(chain, rpc_url, recipient, from_block, to_block)
    elif chain in EXPLORER_APIS:
        return find_withdrawal_tx_explorer(chain, recipient, from_block, to_block)
    else:
        latest = get_latest_block(chain, rpc_url)
        from_block = find_block_by_timestamp(chain, rpc_url, ts_min, latest)
        to_block = find_block_by_timestamp(chain, rpc_url, ts_max, latest)
        if from_block >= to_block:
            return None, None
        return find_withdrawal_tx_rpc_fallback(chain, rpc_url, recipient, from_block, to_block)

# ---------------------------------------------------------------------------
# Step 3: Find next 3 outgoing txs
# ---------------------------------------------------------------------------

def find_next_txs_alchemy(chain: str, rpc_url: str, recipient: str, from_block: int) -> list:
    params = [{
        "fromAddress": recipient,
        "category": ["external", "erc20"],
        "fromBlock": int_to_hex(from_block),
        "maxCount": int_to_hex(3),
        "order": "asc",
    }]
    try:
        result = rpc_call(chain, rpc_url, "alchemy_getAssetTransfers", params)
    except Exception as e:
        print(f"  Alchemy next txs error: {e}", flush=True)
        return []
    transfers = (result or {}).get("transfers", [])
    return [{
        "tx_hash": tx.get("hash", ""),
        "block_number": hex_to_int(tx.get("blockNum", "0x0")),
        "to_address": tx.get("to", ""),
        "value": tx.get("value", 0),
    } for tx in transfers[:3]]


def find_next_txs_explorer(chain: str, recipient: str, from_block: int) -> list:
    txs = explorer_get_txlist(chain, recipient, from_block, 99999999, sort="asc")
    recipient_lower = recipient.lower()
    results = []
    for tx in txs:
        if (tx.get("from") or "").lower() == recipient_lower:
            results.append({
                "tx_hash": tx.get("hash", ""),
                "block_number": int(tx.get("blockNumber", 0)),
                "to_address": tx.get("to", ""),
                "value": int(tx.get("value", "0")) / 1e18,
            })
            if len(results) >= 3:
                break
    return results


def find_next_txs_rpc_fallback(chain: str, rpc_url: str, recipient: str, from_block: int) -> list:
    recipient_lower = recipient.lower()
    results = []
    for block_num in range(from_block, from_block + 100):
        if len(results) >= 3:
            break
        result = rpc_call(chain, rpc_url, "eth_getBlockByNumber", [int_to_hex(block_num), True])
        if not result or not result.get("transactions"):
            continue
        for tx in result["transactions"]:
            if (tx.get("from") or "").lower() == recipient_lower:
                results.append({
                    "tx_hash": tx["hash"],
                    "block_number": block_num,
                    "to_address": tx.get("to", ""),
                    "value": hex_to_int(tx.get("value", "0x0")) / 1e18,
                })
                if len(results) >= 3:
                    break
    return results


def find_next_txs(chain: str, rpc_url: str, recipient: str, withdrawal_block: int) -> list:
    from_block = withdrawal_block + 1
    if is_alchemy_chain(chain):
        return find_next_txs_alchemy(chain, rpc_url, recipient, from_block)
    elif chain in EXPLORER_APIS:
        return find_next_txs_explorer(chain, recipient, from_block)
    else:
        return find_next_txs_rpc_fallback(chain, rpc_url, recipient, from_block)

# ---------------------------------------------------------------------------
# Cache (thread-safe)
# ---------------------------------------------------------------------------

_cache = {}
_cache_lock = threading.Lock()


def load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            _cache = json.load(f)
    else:
        _cache = {}
    print(f"Cache entries: {len(_cache)}")


def save_cache():
    with _cache_lock:
        with open(CACHE_FILE, "w") as f:
            json.dump(_cache, f, indent=2, default=str)


def cache_get(key: str) -> Optional[dict]:
    with _cache_lock:
        return _cache.get(key)


def cache_set(key: str, value: dict):
    with _cache_lock:
        _cache[key] = value


def cache_key(chain: str, recipient: str, block) -> str:
    return f"{chain}:{recipient}:{block}"

# ---------------------------------------------------------------------------
# Row processing
# ---------------------------------------------------------------------------

def process_row(row: dict) -> Optional[dict]:
    dest_chain = (row.get("withdrawal_chain") or "").strip()
    recipient = (row.get("recipient") or "").strip()
    if not recipient:
        return None

    try:
        amount_usd = float(row.get("amount_usd") or 0)
    except (ValueError, TypeError):
        amount_usd = 0
    if amount_usd < 1.0:
        return None

    rpc_url = get_rpc_url(dest_chain)
    if not rpc_url:
        return None

    withdrawal_tx = (row.get("withdrawal_tx_hash") or "").strip()
    withdrawal_block = (row.get("withdrawal_block_number") or "").strip()

    ckey = cache_key(dest_chain, recipient, withdrawal_block or row.get("deposit_block_number", ""))
    cached = cache_get(ckey)
    if cached:
        return cached

    if not withdrawal_tx:
        deposit_time = row.get("deposit_block_time", "")
        if not deposit_time:
            return None
        try:
            wtx, wblock = find_withdrawal_tx(dest_chain, rpc_url, recipient, deposit_time)
        except Exception as e:
            print(f"  Error finding withdrawal tx: {e}", flush=True)
            wtx, wblock = None, None

        if wtx:
            withdrawal_tx = wtx
            withdrawal_block = str(wblock)
        else:
            result = {
                "source_chain": row.get("deposit_chain", ""),
                "destination_chain": dest_chain,
                "original_amount": row.get("deposit_amount", ""),
                "original_amount_usd": row.get("amount_usd", ""),
                "withdrawal_tx_hash": "not_found",
                "withdrawal_block_number": "",
                "recipient": recipient,
            }
            for i in range(1, 4):
                result[f"next_tx_hash_{i}"] = ""
                result[f"next_block_{i}"] = ""
                result[f"next_to_{i}"] = ""
                result[f"next_value_{i}"] = ""
            cache_set(ckey, result)
            return result

    wblock_int = int(withdrawal_block) if withdrawal_block else 0
    if wblock_int == 0:
        next_txs = []
    else:
        try:
            next_txs = find_next_txs(dest_chain, rpc_url, recipient, wblock_int)
        except Exception as e:
            print(f"  Error finding next txs: {e}", flush=True)
            next_txs = []

    result = {
        "source_chain": row.get("deposit_chain", ""),
        "destination_chain": dest_chain,
        "original_amount": row.get("deposit_amount", ""),
        "original_amount_usd": row.get("amount_usd", ""),
        "withdrawal_tx_hash": withdrawal_tx,
        "withdrawal_block_number": withdrawal_block,
        "recipient": recipient,
    }
    for i in range(3):
        idx = i + 1
        if i < len(next_txs):
            tx = next_txs[i]
            result[f"next_tx_hash_{idx}"] = tx.get("tx_hash", "")
            result[f"next_block_{idx}"] = tx.get("block_number", "")
            result[f"next_to_{idx}"] = tx.get("to_address", "")
            result[f"next_value_{idx}"] = tx.get("value", "")
        else:
            result[f"next_tx_hash_{idx}"] = ""
            result[f"next_block_{idx}"] = ""
            result[f"next_to_{idx}"] = ""
            result[f"next_value_{idx}"] = ""

    ckey_final = cache_key(dest_chain, recipient, withdrawal_block)
    cache_set(ckey_final, result)
    if ckey != ckey_final:
        cache_set(ckey, result)
    return result

# ---------------------------------------------------------------------------
# Per-pair processing
# ---------------------------------------------------------------------------

def process_pair(src: str, dst: str, limit: int = 0) -> str:
    """Process a single src->dst pair. Returns status string."""
    input_path = os.path.join(FLOWS_DIR, f"bridge_flows_{src}_{dst}.csv")
    output_path = os.path.join(ANALYSIS_DIR, f"analysis_{src}_{dst}.csv")

    if not os.path.exists(input_path):
        return f"{src}->{dst}: no input CSV"

    with open(input_path) as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return f"{src}->{dst}: empty CSV"

    if limit > 0:
        rows = rows[:limit]

    # Load existing results and build a set of already-analyzed keys
    existing_results = []
    analyzed_keys = set()
    if os.path.exists(output_path):
        with open(output_path) as f:
            existing_results = list(csv.DictReader(f))
        for r in existing_results:
            recip = (r.get("recipient") or "").lower()
            wblock = (r.get("withdrawal_block_number") or "").strip()
            analyzed_keys.add((recip, wblock))

    # Filter to only rows not yet analyzed
    pending_rows = []
    for row in rows:
        recip = (row.get("recipient") or "").strip().lower()
        wblock = (row.get("withdrawal_block_number") or "").strip()
        # Also check the cache for this row
        ckey = cache_key(dst, recip, wblock or (row.get("deposit_block_number") or ""))
        if (recip, wblock) in analyzed_keys or cache_get(ckey) is not None:
            continue
        pending_rows.append(row)

    if not pending_rows:
        return f"{src}->{dst}: all {len(rows)} rows already analyzed, skipping"

    results = list(existing_results)
    found = 0
    not_found = 0
    cached = 0
    errors = 0
    start = time.time()

    print(f"  {src}->{dst}: {len(pending_rows)} new rows to analyze ({len(existing_results)} existing)", flush=True)

    for i, row in enumerate(pending_rows):
        try:
            result = process_row(row)
            if result:
                results.append(result)
                wtx = result.get("withdrawal_tx_hash", "")
                if wtx == "not_found":
                    not_found += 1
                elif wtx:
                    found += 1
        except Exception as e:
            errors += 1

        if (i + 1) % 100 == 0:
            elapsed = time.time() - start
            print(f"  {src}->{dst}: [{i+1}/{len(pending_rows)}] ok={found} miss={not_found} err={errors} ({elapsed:.0f}s)", flush=True)
            save_cache()

    # Write output (existing + new)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(results)

    new_count = len(results) - len(existing_results)
    elapsed = time.time() - start
    return f"{src}->{dst}: {new_count} new rows ({found} found, {not_found} miss, {errors} err) in {elapsed:.1f}s -> {output_path} [total: {len(results)}]"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Post-bridge activity analyzer")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max rows to analyze per pair (0 = all)")
    args = parser.parse_args()

    if args.limit:
        print(f"Limiting to {args.limit} rows per pair")

    if not ALCHEMY_API_KEY:
        print("Warning: ALCHEMY_API_KEY not set. Alchemy chains will be skipped.")

    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    # Initialize per-chain rate limiters
    all_chains = set(ALCHEMY_CHAINS.keys()) | set(PUBLIC_RPCS.keys())
    for chain in all_chains:
        _get_chain_lock(chain)

    load_cache()

    # Build all pairs from CONSIDERED_CHAINS
    pairs = []
    for src in CONSIDERED_CHAINS:
        for dst in CONSIDERED_CHAINS:
            if src != dst:
                pairs.append((src, dst))

    # Group by destination chain for parallel execution
    by_dst = {}
    for src, dst in pairs:
        by_dst.setdefault(dst, []).append((src, dst))

    total = len(pairs)
    print(f"Processing {total} pairs across {len(by_dst)} destination chains")
    print(f"Chains: {', '.join(CONSIDERED_CHAINS)}\n")

    completed = 0
    start_time = time.time()
    row_limit = args.limit

    def process_dst_group(dst, dst_pairs):
        results = []
        for src, dst in dst_pairs:
            status = process_pair(src, dst, limit=row_limit)
            results.append(status)
        return results

    max_workers = min(len(by_dst), 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for dst, dst_pairs in by_dst.items():
            future = executor.submit(process_dst_group, dst, dst_pairs)
            futures[future] = dst

        for future in as_completed(futures):
            dst = futures[future]
            try:
                statuses = future.result()
                for s in statuses:
                    completed += 1
                    print(f"[{completed}/{total}] {s}", flush=True)
            except Exception as e:
                print(f"[ERROR] dst={dst}: {e}", flush=True)

            save_cache()

    save_cache()
    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f}s. Results in {ANALYSIS_DIR}/")

    # Classify any new addresses
    classify_new_addresses()


# ---------------------------------------------------------------------------
# Address type classification (EOA vs contract via eth_getCode)
# ---------------------------------------------------------------------------

ADDRESS_TYPE_CACHE_FILE = "address_type_cache.json"


def _load_address_type_cache() -> dict:
    if os.path.exists(ADDRESS_TYPE_CACHE_FILE):
        with open(ADDRESS_TYPE_CACHE_FILE) as f:
            return json.load(f)
    return {}


def _save_address_type_cache(cache: dict):
    with open(ADDRESS_TYPE_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def _eth_get_code(rpc_url: str, address: str) -> str:
    """Returns 'eoa' or 'contract'."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_getCode",
        "params": [address, "latest"],
    }
    resp = requests.post(rpc_url, json=payload, timeout=RPC_TIMEOUT)
    resp.raise_for_status()
    code = resp.json().get("result", "0x")
    return "eoa" if code in ("0x", "0x0", "") else "contract"


def classify_new_addresses():
    """Scan analysis CSVs for unclassified addresses and classify them."""
    at_cache = _load_address_type_cache()
    cached_lower = {k.lower() for k in at_cache}

    # Collect addresses by destination chain
    by_chain: dict[str, set] = {}
    for path in glob.glob(os.path.join(ANALYSIS_DIR, "analysis_*.csv")):
        with open(path) as f:
            for row in csv.DictReader(f):
                chain = (row.get("destination_chain") or "").strip().lower()
                if not chain:
                    continue
                for field in ("next_to_1", "next_to_2", "next_to_3"):
                    addr = (row.get(field) or "").strip().lower()
                    if addr and addr != "0x" and len(addr) == 42 and addr not in cached_lower:
                        by_chain.setdefault(chain, set()).add(addr)

    total = sum(len(v) for v in by_chain.values())
    if total == 0:
        print("\nAll addresses already classified.")
        return

    print(f"\nClassifying {total} new addresses across {len(by_chain)} chains...")

    new_count = 0
    for chain, addrs in sorted(by_chain.items()):
        rpc_url = get_rpc_url(chain)
        if not rpc_url:
            print(f"  {chain}: no RPC, skipping {len(addrs)} addresses")
            continue

        errors = 0
        for i, addr in enumerate(addrs):
            try:
                addr_type = _eth_get_code(rpc_url, addr)
                at_cache[addr] = addr_type
                new_count += 1
            except Exception:
                errors += 1

            if (i + 1) % 200 == 0:
                print(f"  {chain}: {i+1}/{len(addrs)} ({errors} errors)")
                _save_address_type_cache(at_cache)

            # Rate limit for public RPCs
            if chain in PUBLIC_RPCS:
                time.sleep(0.05)

        _save_address_type_cache(at_cache)
        print(f"  {chain}: {len(addrs) - errors}/{len(addrs)} classified")

    print(f"Address classification done: {new_count} new "
          f"({sum(1 for v in at_cache.values() if v == 'eoa')} EOA, "
          f"{sum(1 for v in at_cache.values() if v == 'contract')} contract)")


if __name__ == "__main__":
    main()

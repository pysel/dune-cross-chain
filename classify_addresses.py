"""Classify unclassified addresses from analysis results via eth_getCode.

Temporary script — reads all analysis CSVs, finds addresses not yet in
address_type_cache.json, classifies them as EOA or contract, and saves back.
"""

import csv
import glob
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

CACHE_FILE = "address_type_cache.json"
ANALYSIS_DIR = "analysis"

ALCHEMY_API_KEY = os.environ.get("ALCHEMY_API_KEY", "")

# We need RPCs for destination chains — addresses live on the dest chain.
# But we don't know which chain an address belongs to from the analysis CSV alone.
# However, we DO know the dest chain from the filename (analysis_{src}_{dst}.csv)
# and from the destination_chain field in each row.
# Strategy: group addresses by dest chain, query the right RPC for each.

ALCHEMY_CHAINS = {
    "ethereum": "eth-mainnet",
    "polygon": "polygon-mainnet",
    "arbitrum": "arb-mainnet",
    "optimism": "opt-mainnet",
    "base": "base-mainnet",
    "hyperliquid": "hyperliquid-mainnet",
}

PUBLIC_RPCS = {
    "ethereum": "https://eth.llamarpc.com",
    "polygon": "https://polygon-rpc.com",
    "arbitrum": "https://arb1.arbitrum.io/rpc",
    "optimism": "https://mainnet.optimism.io",
    "base": "https://mainnet.base.org",
    "hyperliquid": "https://rpc.hyperliquid.xyz/evm",
    # "bnb": "https://bsc-dataseed.binance.org",
    # "avalanche_c": "https://api.avax.network/ext/bc/C/rpc",
}

RPC_TIMEOUT = 10


def get_rpc_url(chain: str):
    # Prefer Alchemy if key available, fall back to public RPCs
    if chain in ALCHEMY_CHAINS and ALCHEMY_API_KEY:
        return f"https://{ALCHEMY_CHAINS[chain]}.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    if chain in PUBLIC_RPCS:
        return PUBLIC_RPCS[chain]
    return None


def eth_get_code(rpc_url: str, address: str) -> str:
    """Returns 'eoa' or 'contract'."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_getCode",
        "params": [address, "latest"],
    }
    resp = requests.post(rpc_url, json=payload, timeout=RPC_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    code = data.get("result", "0x")
    return "eoa" if code in ("0x", "0x0", "") else "contract"


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def collect_addresses_by_chain():
    """Returns {chain: set(addresses)} from analysis CSVs."""
    by_chain = {}
    for path in glob.glob(os.path.join(ANALYSIS_DIR, "analysis_*.csv")):
        with open(path) as f:
            for row in csv.DictReader(f):
                chain = (row.get("destination_chain") or "").strip().lower()
                if not chain:
                    continue
                for field in ["next_to_1", "next_to_2", "next_to_3"]:
                    addr = (row.get(field) or "").strip().lower()
                    if addr and addr != "0x" and len(addr) == 42:
                        by_chain.setdefault(chain, set()).add(addr)
    return by_chain


def classify_chain(chain: str, addresses: list, cache: dict) -> dict:
    """Classify a batch of addresses on one chain. Returns {addr: type}."""
    rpc_url = get_rpc_url(chain)
    if not rpc_url:
        print(f"  {chain}: no RPC available, skipping {len(addresses)} addresses")
        return {}

    results = {}
    errors = 0
    for i, addr in enumerate(addresses):
        try:
            addr_type = eth_get_code(rpc_url, addr)
            results[addr] = addr_type
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  {chain}: error for {addr}: {e}")

        if (i + 1) % 200 == 0:
            print(f"  {chain}: {i+1}/{len(addresses)} classified ({errors} errors)")

        # Rate limit: ~50 req/s for Alchemy, slower for public RPCs
        if chain in PUBLIC_RPCS:
            time.sleep(0.05)

    print(f"  {chain}: done — {len(results)} classified, {errors} errors")
    return results


def main():
    cache = load_cache()
    cached_lower = {k.lower() for k in cache}

    by_chain = collect_addresses_by_chain()

    # Filter out already-cached addresses
    total_need = 0
    for chain in by_chain:
        by_chain[chain] = {a for a in by_chain[chain] if a.lower() not in cached_lower}
        total_need += len(by_chain[chain])

    print(f"Addresses to classify: {total_need} across {len(by_chain)} chains")
    for chain, addrs in sorted(by_chain.items(), key=lambda x: -len(x[1])):
        print(f"  {chain}: {len(addrs)}")

    if total_need == 0:
        print("Nothing to do.")
        return

    new_total = 0
    for chain, addrs in sorted(by_chain.items()):
        if not addrs:
            continue
        print(f"\nClassifying {len(addrs)} addresses on {chain}...")
        results = classify_chain(chain, list(addrs), cache)
        for addr, addr_type in results.items():
            cache[addr] = addr_type
        new_total += len(results)

        # Save after each chain
        save_cache(cache)
        print(f"  Cache saved ({len(cache)} total entries)")

    print(f"\nDone. Classified {new_total} new addresses.")
    print(f"Cache now has {len(cache)} entries "
          f"({sum(1 for v in cache.values() if v == 'eoa')} EOA, "
          f"{sum(1 for v in cache.values() if v == 'contract')} contract)")


if __name__ == "__main__":
    main()

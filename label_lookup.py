"""Look up unclassified contract addresses against eth-labels.com registry.

Fetches label data per chain, matches against addresses in analysis CSVs
that are not yet in KNOWN_CONTRACTS, and outputs matches + updates explorer.py.
"""

import csv
import glob
import json
import os

import requests

ANALYSIS_DIR = "analysis"
ADDRESS_TYPE_CACHE = "address_type_cache.json"
LABELS_CACHE_DIR = "label_cache"

CHAIN_IDS = {
    "ethereum": 1,
    "polygon": 137,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
    "bnb": 56,
    "avalanche_c": 43114,
    "hyperliquid": 999,
}

# Load known contracts dynamically from eth-labels cache
ALREADY_KNOWN = set()
if os.path.isdir(LABELS_CACHE_DIR):
    for fname in os.listdir(LABELS_CACHE_DIR):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(LABELS_CACHE_DIR, fname)) as f:
            entries = json.load(f)
        for entry in entries:
            addr = (entry.get("address") or "").lower()
            tag = (entry.get("nameTag") or "").strip()
            elabel = (entry.get("label") or "").strip()
            if not addr or not tag:
                continue
            if tag.startswith("Null:") or elabel in ("blocked", "burn", "genesis"):
                continue
            ALREADY_KNOWN.add(addr)


def fetch_labels(chain_id: int, chain_name: str) -> dict:
    """Fetch labels from eth-labels.com, with local caching."""
    os.makedirs(LABELS_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(LABELS_CACHE_DIR, f"labels_{chain_name}_{chain_id}.json")

    if os.path.exists(cache_path):
        print(f"  {chain_name}: loading from cache")
        with open(cache_path) as f:
            data = json.load(f)
    else:
        print(f"  {chain_name}: fetching chainId={chain_id} from eth-labels.com...")
        resp = requests.get(
            f"https://eth-labels.com/accounts?chainId={chain_id}",
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        with open(cache_path, "w") as f:
            json.dump(data, f)
        print(f"  {chain_name}: got {len(data)} entries")

    # Build lookup: address -> best nameTag
    lookup = {}
    for entry in data:
        addr = (entry.get("address") or "").lower()
        tag = (entry.get("nameTag") or "").strip()
        label = (entry.get("label") or "").strip()
        if not addr or not tag:
            continue
        # Skip generic/useless tags
        if tag.startswith("Null:") or label in ("blocked", "burn", "genesis"):
            continue
        # Keep the longest/most descriptive tag per address
        if addr not in lookup or len(tag) > len(lookup[addr]):
            lookup[addr] = tag
    return lookup


def collect_addresses_by_chain() -> dict:
    """Returns {chain: Counter(address -> count)} from analysis CSVs."""
    from collections import Counter
    by_chain: dict[str, Counter] = {}
    for path in glob.glob(os.path.join(ANALYSIS_DIR, "analysis_*.csv")):
        with open(path) as f:
            for row in csv.DictReader(f):
                chain = (row.get("destination_chain") or "").strip().lower()
                if not chain:
                    continue
                for field in ("next_to_1",):
                    addr = (row.get(field) or "").strip().lower()
                    if addr and addr != "0x" and len(addr) == 42:
                        by_chain.setdefault(chain, Counter())[addr] += 1
    return by_chain


def main():
    # Load address type cache to filter to contracts only
    with open(ADDRESS_TYPE_CACHE) as f:
        at_cache = {k.lower(): v for k, v in json.load(f).items()}

    by_chain = collect_addresses_by_chain()

    # Results to output
    all_found = {}  # {(chain, address): (nameTag, count)}
    all_not_found = {}  # {(chain, address): count}

    for chain_name, chain_id in sorted(CHAIN_IDS.items()):
        if chain_name not in by_chain:
            continue

        lookup = fetch_labels(chain_id, chain_name)
        print(f"  {chain_name}: {len(lookup)} labeled addresses in registry")

        addrs = by_chain[chain_name]
        for addr, count in addrs.most_common():
            if addr in ALREADY_KNOWN:
                continue
            if at_cache.get(addr) != "contract":
                continue

            tag = lookup.get(addr)
            if tag:
                all_found[(chain_name, addr)] = (tag, count)
            else:
                all_not_found[(chain_name, addr)] = count

    # Print found matches, grouped by chain
    print(f"\n{'='*80}")
    print(f"  MATCHES FOUND IN REGISTRY")
    print(f"{'='*80}")

    by_chain_found: dict[str, list] = {}
    for (chain, addr), (tag, count) in all_found.items():
        by_chain_found.setdefault(chain, []).append((addr, tag, count))

    for chain in sorted(by_chain_found):
        entries = sorted(by_chain_found[chain], key=lambda x: -x[2])
        print(f"\n  {chain.upper()} ({len(entries)} matches)")
        print(f"  {'Address':<44} {'Count':>5}  {'Label'}")
        print(f"  {'-'*44} {'-'*5}  {'-'*40}")
        for addr, tag, count in entries[:30]:
            print(f"  {addr}  {count:>5}  {tag}")

    # Summary
    total_found = len(all_found)
    total_not_found = len(all_not_found)
    print(f"\n{'='*80}")
    print(f"  SUMMARY: {total_found} matched, {total_not_found} not found in registry")
    print(f"{'='*80}")

    # Save results to JSON for further use
    results = {}
    for (chain, addr), (tag, count) in all_found.items():
        results[addr] = {"chain": chain, "nameTag": tag, "count": count}
    with open("label_lookup_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to label_lookup_results.json")


if __name__ == "__main__":
    main()

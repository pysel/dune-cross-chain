"""Show top unclassified contract addresses by interaction count, per chain."""

import csv
import glob
import json
import os
from collections import Counter

ANALYSIS_DIR = "analysis"
ADDRESS_TYPE_CACHE = "address_type_cache.json"
LABEL_CACHE_DIR = "label_cache"

# Load known contracts dynamically from eth-labels cache
KNOWN_CONTRACTS_LOWER = set()
if os.path.isdir(LABEL_CACHE_DIR):
    for fname in os.listdir(LABEL_CACHE_DIR):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(LABEL_CACHE_DIR, fname)) as f:
            entries = json.load(f)
        for entry in entries:
            addr = (entry.get("address") or "").lower()
            tag = (entry.get("nameTag") or "").strip()
            elabel = (entry.get("label") or "").strip()
            if not addr or not tag:
                continue
            if tag.startswith("Null:") or elabel in ("blocked", "burn", "genesis"):
                continue
            KNOWN_CONTRACTS_LOWER.add(addr)

# Also exclude manually labeled addresses from explorer.py
# (parsed at import time to avoid triggering Streamlit)
import ast as _ast
with open("explorer.py") as _f:
    _tree = _ast.parse(_f.read())
for _node in _ast.walk(_tree):
    if isinstance(_node, _ast.Assign):
        for _t in _node.targets:
            if isinstance(_t, _ast.Name) and _t.id == "MANUAL_LABELS":
                for _key in _node.value.keys:
                    if isinstance(_key, _ast.Constant):
                        KNOWN_CONTRACTS_LOWER.add(str(_key.value).lower())

# Load address type cache
with open(ADDRESS_TYPE_CACHE) as f:
    at_cache = json.load(f)
at_lower = {k.lower(): v for k, v in at_cache.items()}

# Count interactions per (chain, address), only for contracts not in known list
# chain_counts[chain] = Counter({address: count})
chain_counts: dict[str, Counter] = {}

for path in glob.glob(os.path.join(ANALYSIS_DIR, "analysis_*.csv")):
    with open(path) as f:
        for row in csv.DictReader(f):
            chain = (row.get("destination_chain") or "").strip().lower()
            if not chain:
                continue
            for field in ("next_to_1",):  # only first hop
                addr = (row.get(field) or "").strip().lower()
                if not addr or addr == "0x" or len(addr) != 42:
                    continue
                if addr in KNOWN_CONTRACTS_LOWER:
                    continue
                # Only include contracts (skip EOAs and unclassified)
                addr_type = at_lower.get(addr)
                if addr_type != "contract":
                    continue
                chain_counts.setdefault(chain, Counter())[addr] += 1

# Print results
for chain in sorted(chain_counts.keys()):
    counts = chain_counts[chain]
    top = counts.most_common(20)
    print(f"\n{'='*70}")
    print(f"  {chain.upper()} — top unclassified contracts (next_to_1)")
    print(f"{'='*70}")
    print(f"  {'Address':<44} {'Count':>6}")
    print(f"  {'-'*44} {'-'*6}")
    for addr, count in top:
        print(f"  {addr}  {count:>6}")

"""
Loop analysis: for each source→dest chain pair, what fraction of bridge
transfers have a corresponding return transfer (same sender, opposite direction)?

A "loop" means: sender X bridged A→B, and sender X also bridged B→A at any point.

Outputs: loop_analysis.json

Usage: python analyze_loops.py
"""

import csv
import json
import os
from collections import defaultdict

FLOWS_DIR = "flows"
OUTPUT_FILE = "loop_analysis.json"


def analyze():
    # Step 1: Build set of senders per directed chain pair
    # (source_chain, dest_chain) → set of sender addresses
    pair_senders = defaultdict(set)
    # Also count total txs per pair
    pair_tx_count = defaultdict(int)
    # And per-sender tx count per pair for volume breakdown
    pair_sender_txs = defaultdict(lambda: defaultdict(int))

    for fname in sorted(os.listdir(FLOWS_DIR)):
        if not fname.endswith(".csv"):
            continue
        with open(os.path.join(FLOWS_DIR, fname)) as f:
            reader = csv.DictReader(f)
            for row in reader:
                src = row.get("deposit_chain", "")
                dst = row.get("withdrawal_chain", "")
                sender = (row.get("sender") or "").lower().strip()
                if not src or not dst or not sender:
                    continue
                pair = (src, dst)
                pair_senders[pair].add(sender)
                pair_tx_count[pair] += 1
                pair_sender_txs[pair][sender] += 1

    # Step 2: For each pair A→B, find senders who also appear in B→A
    results = []
    for (src, dst), senders in sorted(pair_senders.items()):
        reverse = (dst, src)
        reverse_senders = pair_senders.get(reverse, set())

        # Loopers: senders present in both directions
        loopers = senders & reverse_senders
        total_txs = pair_tx_count[(src, dst)]
        total_senders = len(senders)

        # Count how many txs in A→B belong to loopers
        loop_txs = sum(pair_sender_txs[(src, dst)][s] for s in loopers)

        loop_tx_pct = round(loop_txs / total_txs * 100, 2) if total_txs > 0 else 0
        loop_sender_pct = round(len(loopers) / total_senders * 100, 2) if total_senders > 0 else 0

        results.append({
            "source_chain": src,
            "dest_chain": dst,
            "total_txs": total_txs,
            "total_senders": total_senders,
            "loop_senders": len(loopers),
            "loop_txs": loop_txs,
            "loop_tx_pct": loop_tx_pct,
            "loop_sender_pct": loop_sender_pct,
        })

    # Sort by volume for readability
    results.sort(key=lambda r: -r["total_txs"])

    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    # Print summary
    print(f"Wrote {OUTPUT_FILE}")
    print(f"Chain pairs analyzed: {len(results)}")
    print()
    print(f"{'Source':<15} {'Dest':<15} {'TotalTxs':>10} {'LoopTxs':>10} {'TxLoop%':>8} {'Senders':>8} {'Loopers':>8} {'SndLoop%':>9}")
    print("-" * 95)
    for r in results:
        print(
            f"{r['source_chain']:<15} {r['dest_chain']:<15} "
            f"{r['total_txs']:>10,} {r['loop_txs']:>10,} {r['loop_tx_pct']:>7.1f}% "
            f"{r['total_senders']:>8,} {r['loop_senders']:>8,} {r['loop_sender_pct']:>8.1f}%"
        )


if __name__ == "__main__":
    analyze()

"""
Cross-chain asset flow analysis.

For each source chain + source asset, compute what percentage of volume
goes to each destination asset on the destination chain.

Outputs: asset_flow_analysis.json

Usage: python analyze_asset_flows.py
"""

import csv
import json
import os
from collections import defaultdict

FLOWS_DIR = "flows"
TOKENS_DIR = "tokens"
OUTPUT_FILE = "asset_flow_analysis.json"

# Manual token mappings for addresses not in the Uniswap token lists
EXTRA_TOKENS = {
    "arbitrum": {
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT",
        "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "WBTC",
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
        "0x641441c631e2f909700d2f41fd87f0aa6a6b4edb": "USX",
        "0x0c880f6761f1af8d9aa9c466984b80dab9a8c9e8": "PENDLE",
        "0xae6aab43c4f3e0cea4ab83752c278f8debaba689": "dForce",
        "0x040d1edc9569d4bab2d15287dc5a4f10f56a56b8": "BAL",
    },
    "optimism": {
        "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": "USDT",
        "0x68f180fcce6836688e9084f035309e29bf0a2095": "WBTC",
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
        "0xbfd291da8a403daaf7e5e9dc1ec0aceacd4848b9": "USX",
        "0x01bff41798a0bcf287b996046ca68b395dbc1071": "OATH",
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee": "ETH",
    },
    "bnb": {
        "0x55d398326f99059ff775485246999027b3197955": "USDT",
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": "USDC",
        "0x2170ed0880ac9a755fd29b2688956bd959f933f8": "ETH",
        "0xe9e7cea3dedca5984780bafc599bd69add087d56": "BUSD",
        "0x8457ca5040ad67fdebbcc8edce889a335bc0fbfb": "LEVER",
        "0x4691937a7508860f876c9c0a2a617e7d9e945d4b": "WOO",
        "0xb5102cee1528ce2c760893034a4603663495fd72": "USX",
    },
    "avalanche_c": {
        "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664": "USDC.e",
        "0xc7198437980c041c805a1edcba50c1ce5db95118": "USDT.e",
        "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": "WETH.e",
        "0xabc9547b534519ff73921b1fba6e672b5f58d083": "wMEMO",
    },
    "ethereum": {
        "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee": "weETH",
        "0xeec2be5c91ae7f8a338e1e5f3b5de49d07afdc81": "yvCurve",
        "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": "wstETH",
        "0xae78736cd615f374d3085123a210448e74fc6393": "rETH",
        "0x43044f861ec040db59a7e324c40507addb673142": "BOBA",
        "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e": "crvUSD",
        "0xc581b735a1688071a1746c968e0798d642ede491": "EURO3",
        "0x968cbe62c830a0ccf4381614662398505657a2a9": "cUSDO",
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee": "ETH",
    },
    "polygon": {
        "0xcf66eb3d546f0415b368d98a95eaf56ded7aa752": "USX",
    },
    "base": {
        "0xc142171b138db17a1b7cb999c44526094a4dae05": "USX",
        "0xfde4c96cc914de7bcb2583ee1b3a9f35cf43be0c": "USDT",
        "0x4158734d47fc9692176b5085e0f52ee0da5d47f1": "BAL",
    },
    "hyperliquid": {},
}

# Map flow chain names → token file names
CHAIN_TO_TOKEN_FILE = {
    "arbitrum": "arbitrum.json",
    "avalanche_c": "avalanche.json",
    "base": "base.json",
    "blast": "blast.json",
    "bnb": "bnb.json",
    "celo": "celo.json",
    "ethereum": "mainnet.json",
    "optimism": "optimism.json",
    "polygon": "polygon.json",
    "zksync": "zksync.json",
    "zora": "zora.json",
}

# Standard chain IDs for bridgeInfo lookups
CHAIN_NAME_TO_ID = {
    "arbitrum": "42161",
    "avalanche_c": "43114",
    "base": "8453",
    "blast": "81457",
    "bnb": "56",
    "celo": "42220",
    "ethereum": "1",
    "optimism": "10",
    "polygon": "137",
    "zksync": "324",
    "zora": "7777777",
    "hyperliquid": None,
}


def build_token_lookup():
    """Build chain_name → {lowercase_address → symbol} from token files + bridgeInfo."""
    lookup = {}  # chain_name -> {address -> symbol}

    # Direct entries from each chain's token file
    for chain_name, fname in CHAIN_TO_TOKEN_FILE.items():
        path = os.path.join(TOKENS_DIR, fname)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            tokens = json.load(f)
        addr_map = {}
        for t in tokens:
            addr = t.get("address", "").lower()
            symbol = t.get("symbol", "")
            if addr and symbol:
                addr_map[addr] = symbol
        lookup[chain_name] = addr_map

    # Also use bridgeInfo from mainnet.json to fill gaps on other chains
    mainnet_path = os.path.join(TOKENS_DIR, "mainnet.json")
    if os.path.exists(mainnet_path):
        with open(mainnet_path) as f:
            mainnet_tokens = json.load(f)
        for t in mainnet_tokens:
            symbol = t.get("symbol", "")
            if not symbol:
                continue
            bridge_info = (t.get("extensions") or {}).get("bridgeInfo", {})
            for chain_id_str, info in bridge_info.items():
                bridge_addr = (info.get("tokenAddress") or "").lower()
                if not bridge_addr:
                    continue
                # Find which chain_name has this chain_id
                for cname, cid in CHAIN_NAME_TO_ID.items():
                    if cid == chain_id_str:
                        if cname not in lookup:
                            lookup[cname] = {}
                        # Don't overwrite existing entries (chain-specific file takes priority)
                        if bridge_addr not in lookup[cname]:
                            lookup[cname][bridge_addr] = symbol
                        break

    # Apply EXTRA_TOKENS (manual overrides always win)
    for chain_name, tokens in EXTRA_TOKENS.items():
        if chain_name not in lookup:
            lookup[chain_name] = {}
        for addr, symbol in tokens.items():
            lookup[chain_name][addr.lower()] = symbol

    return lookup


def resolve_symbol(chain_name, token_address, token_lookup):
    """Resolve a token address to its symbol."""
    if not token_address:
        return "NATIVE"
    addr = token_address.lower()
    chain_map = token_lookup.get(chain_name, {})
    if addr in chain_map:
        return chain_map[addr]
    # Check if it's a native/wrapped ETH pattern
    if addr == "0x0000000000000000000000000000000000000000":
        return "NATIVE"
    return addr[:10]  # shortened address as fallback


def analyze():
    token_lookup = build_token_lookup()

    # Accumulate: (source_chain, dest_chain, source_symbol, dest_symbol) → total_usd
    pair_volume = defaultdict(float)
    # Also track: (source_chain, source_symbol) → total_usd for percentage calc
    source_total = defaultdict(float)

    for fname in sorted(os.listdir(FLOWS_DIR)):
        if not fname.endswith(".csv"):
            continue
        filepath = os.path.join(FLOWS_DIR, fname)
        with open(filepath) as f:
            reader = csv.DictReader(f)
            for row in reader:
                amount_usd = row.get("amount_usd", "")
                try:
                    usd = float(amount_usd)
                except (ValueError, TypeError):
                    continue
                if usd <= 0:
                    continue

                src_chain = row.get("deposit_chain", "")
                dst_chain = row.get("withdrawal_chain", "")
                src_addr = row.get("deposit_token_address", "")
                dst_addr = row.get("withdrawal_token_address", "")

                src_symbol = resolve_symbol(src_chain, src_addr, token_lookup)
                dst_symbol = resolve_symbol(dst_chain, dst_addr, token_lookup)

                # If destination token is empty/NATIVE and it's a known
                # L1-deposit chain (e.g. hyperliquid), inherit source symbol
                if dst_symbol == "NATIVE" and not dst_addr:
                    dst_symbol = src_symbol

                key = (src_chain, dst_chain, src_symbol, dst_symbol)
                pair_volume[key] += usd
                source_total[(src_chain, src_symbol)] += usd

    # Build structured output
    # Structure: {source_chain: {source_symbol: {dest_chain: {dest_symbol: {volume, pct}}}}}
    result = {}

    for (src_chain, dst_chain, src_sym, dst_sym), vol in sorted(pair_volume.items()):
        total = source_total[(src_chain, src_sym)]
        pct = round(vol / total * 100, 2) if total > 0 else 0

        if src_chain not in result:
            result[src_chain] = {}
        if src_sym not in result[src_chain]:
            result[src_chain][src_sym] = {
                "total_volume_usd": 0,
                "destinations": {},
            }
        result[src_chain][src_sym]["total_volume_usd"] = round(total, 2)

        if dst_chain not in result[src_chain][src_sym]["destinations"]:
            result[src_chain][src_sym]["destinations"][dst_chain] = {}
        result[src_chain][src_sym]["destinations"][dst_chain][dst_sym] = {
            "volume_usd": round(vol, 2),
            "pct_of_source": pct,
        }

    # Also build a flat summary for the explorer tab
    flat_rows = []
    for (src_chain, dst_chain, src_sym, dst_sym), vol in sorted(pair_volume.items()):
        total = source_total[(src_chain, src_sym)]
        pct = round(vol / total * 100, 2) if total > 0 else 0
        flat_rows.append({
            "source_chain": src_chain,
            "dest_chain": dst_chain,
            "source_asset": src_sym,
            "dest_asset": dst_sym,
            "volume_usd": round(vol, 2),
            "pct_of_source": pct,
            "source_total_usd": round(total, 2),
        })

    output = {
        "by_chain": result,
        "flat": flat_rows,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # Print summary
    print(f"Wrote {OUTPUT_FILE}")
    print(f"Total pairs: {len(pair_volume)}")
    print()

    for src_chain in sorted(result):
        print(f"\n{'='*60}")
        print(f"  SOURCE CHAIN: {src_chain.upper()}")
        print(f"{'='*60}")
        for src_sym in sorted(result[src_chain], key=lambda s: -result[src_chain][s]["total_volume_usd"]):
            info = result[src_chain][src_sym]
            print(f"\n  {src_sym} (total: ${info['total_volume_usd']:,.0f})")
            # Flatten all dest symbols across dest chains, sort by pct
            all_dests = []
            for dc, syms in info["destinations"].items():
                for ds, data in syms.items():
                    all_dests.append((dc, ds, data["volume_usd"], data["pct_of_source"]))
            all_dests.sort(key=lambda x: -x[2])
            for dc, ds, vol, pct in all_dests[:10]:  # top 10
                label = f"{ds}" if ds == src_sym else f"{ds} (swap)"
                print(f"    → {dc}: {label} — ${vol:,.0f} ({pct}%)")


if __name__ == "__main__":
    analyze()

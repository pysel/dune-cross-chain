"""
Interactive Bridge Analytics Explorer
Run: streamlit run explorer.py
Deps: pip install streamlit duckdb plotly
labels: https://eth-labels.com/accounts?chainId=1
"""

import glob
import json
import math
import os

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config (must be first st call) ────────────────────────────────────────

st.set_page_config(
    page_title="Bridge Analytics",
    page_icon="<>",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────

# No custom CSS — use Streamlit's default light theme

# ── Constants ──────────────────────────────────────────────────────────────────

FLOWS_DIR = "flows"
CACHE_FILE = "analysis_cache.json"
ADDRESS_TYPE_CACHE = "address_type_cache.json"

CHAIN_BLOCK_TIMES = {
    "ethereum": 12.0, "polygon": 2.0, "arbitrum": 0.25, "optimism": 2.0,
    "base": 2.0, "zksync": 1.0, "linea": 3.0, "bnb": 3.0,
    "avalanche_c": 2.0, "scroll": 3.0, "mantle": 2.0, "zora": 2.0,
    "blast": 2.0, "nova": 0.25, "gnosis": 5.0, "hyperliquid": 2.0,
}

LABEL_CACHE_DIR = "label_cache"

# Manual overrides — these take priority over eth-labels.com registry.
# Use for addresses not in the registry or where the registry label is wrong.
MANUAL_LABELS = {
    "0x0000000000000000000000000000000000000000": "Burn/Null",
    # Avalanche tokens (not in registry since chainId=43114 returns 0 results)
    "0xc7198437980c041c805a1edcba50c1ce5db95118": "USDT.e Token",
    "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": "USDC Token",
    "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664": "USDC.e Token",
    "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": "WETH.e Token",
    # Avalanche protocols (no registry data)
    "0x60ae616a2155ee3d9a68541ba4544862310933d4": "Trader Joe: LBRouter",
    "0x794a61358d6845594f94dc1db02a252b5b4814ad": "Aave: Pool V3",
    "0x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31": "Metamask: Swap Router",
    "0x6b25532e1060ce10cc3b0a99e5683b91bfde6982": "Circle: Token Messenger",
    "0xef3c714c9425a8f3697a9c969dc1af30ba82e5d4": "Celer Network: cBridge",
    # Polygon (no registry data)
    "0xc590175e458b83680867afd273527ff58f74c02b": "Metamask: Swap Router",
    "0x0009876c47f6b2f0bcb41eb9729736757486c75f": "TeleSwap: Burn Router",
    # Contracts verified manually but not in registry
    "0x1195cf65f83b3a5768f3c496d3a05ad6412c64b7": "Layer3: CUBE",
    "0x3a23f943181408eac424116af7b7790c94cb97a5": "Socket: Gateway",
    "0xe4edb277e41dc89ab076a1f049f4a3efa700bce8": "Orbiter Finance: Bridge",
    "0x3b4d794a66304f130a4db8f2551b0070dfcf5ca7": "Lighter: ZkLighter",
    "0x6b4d38ed0a555d4516ae81c6c8d9b19f4365b523": "Cross-chain: FeeManager",
    "0x2df1c51e09aecf9cacb7bc98cb1742757f163df7": "Hyperliquid: Deposit Bridge 2",
    "0x89c6340b1a1f4b25d36cd8b063d49045caf3f818": "LI.FI: Permit2 Proxy 2",
    "0x4dac9d1769b9b304cb04741dcdeb2fc14abdf110": "LI.FI: Executor",
    "0x33e76c5c31cb928dc6fe6487ab3b2c0769b1a1e3": "Circle: TokenMinter",
    "0x9d39fc627a6d9d9f8c831c16995b209548cc3401": "Celer Network: cBridge",
    "0x9dda6ef3d919c9bc8885d5560999a3640431e8e6": "Metamask: Swap Router",
    "0xe45b133ddc64be80252b0e9c75a8e74ef280eed6": "Circle: TokenMinter",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH Token",
    "0x6f26bf09b1c792e3228e5467807a900a503c0281": "Across Protocol: Spoke Pool",
    "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae": "LI.FI: LiFi Diamond",
    "0x6ff5693b99212da76ad316178a184ab56d299b43": "Uniswap V4: Universal Router",
}

# Keyword rules for auto-categorizing nameTag → behavior group.
# Checked in order; first match wins.
CATEGORY_RULES = [
    # Burn/null (first — very specific)
    (["burn", "null: 0x000"], "Burn/Null"),
    # Lending (before tokens — "Aave: USDC V3" should be Lending, not Token)
    (["aave", "compound", "morpho", "lending", "notional"], "Lending"),
    # Staking
    (["staking", "lido", "staked", "pooled staking"], "Staking"),
    # MEV
    (["mev bot"], "MEV Bot"),
    # CEX
    (["coinbase", "binance", "kraken"], "CEX"),
    # Bridge aggregators
    (["li.fi", "lifi", "socket: gateway"], "Bridge Aggregator"),
    # Bridges
    (["bridge", "spoke pool", "cctp", "cbridge", "wormhole", "stargate",
      "hop protocol", "synapse", "multichain", "debridge", "orbiter",
      "delayed inbox", "scroll: l1", "zksync era: diamond",
      "optimism: gateway", "hyperliquid", "teleswap", "tokenminter",
      "token messenger"], "Bridge"),
    # DEX / Swap
    (["swap", "router", "exchange proxy", "aggregat", "settler",
      "uniswap", "sushiswap", "pancakeswap", "kyberswap", "paraswap",
      "1inch", "0x:", "cow protocol", "cow swap", "camelot", "trader joe",
      "velora", "curve", "balancer", "pendle", "lighter", "rizzolver",
      "banana gun"], "DEX/Swap"),
    # NFT
    (["opensea", "seaport", "blur", "nft", "marketplace", "wyvern",
      "quix", "layer3", "cube"], "NFT Marketplace"),
    # Token transfers (last among specific — catches remaining token contracts)
    (["usdc", "usdt", "dai token", "weth token", "wbtc token",
      "s*usdc", "s*usdt", "s*sgeth", "anyusdc", "susds token",
      "steth token"], "Token Transfer"),
]


def _categorize_label(name_tag: str) -> str:
    """Map a nameTag string to a behavior category using keyword rules."""
    tag_lower = name_tag.lower()
    for keywords, category in CATEGORY_RULES:
        for kw in keywords:
            if kw in tag_lower:
                return category
    return "Other Contract"


def load_known_contracts() -> dict:
    """Build address → (label, category) from eth-labels cache + manual overrides."""
    contracts = {}  # address -> {"label": str, "label_group": str}

    # 1. Load from eth-labels.com cache files
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
                # Keep longest tag per address
                if addr in contracts and len(tag) <= len(contracts[addr]["label"]):
                    continue
                contracts[addr] = {
                    "label": tag,
                    "label_group": _categorize_label(tag),
                }

    # 2. Apply manual overrides (always win)
    for addr, label in MANUAL_LABELS.items():
        addr = addr.lower()
        contracts[addr] = {
            "label": label,
            "label_group": _categorize_label(label),
        }

    return contracts


KNOWN_CONTRACTS = load_known_contracts()

BEHAVIOR_COLORS = {
    "EOA/Wallet": "#059669", "DEX/Swap": "#2563eb", "Bridge Aggregator": "#7c3aed",
    "Bridge": "#6d28d9", "Lending": "#0d9488", "Staking": "#0e7490",
    "CEX": "#b45309", "MEV Bot": "#be123c",
    "NFT/Token": "#ea580c", "NFT Marketplace": "#c2410c",
    "Token Transfer": "#d97706",
    "Other Contract": "#9ca3af", "No Activity": "#e5e7eb",
    "Unknown": "#dc2626", "Burn/Null": "#6b7280",
}

BEHAVIOR_ORDER = [
    "EOA/Wallet", "DEX/Swap", "Bridge Aggregator", "Bridge",
    "Lending", "Staking", "CEX", "MEV Bot",
    "NFT/Token", "NFT Marketplace", "Token Transfer",
    "Other Contract", "No Activity", "Unknown", "Burn/Null",
]

BUCKET_ORDER = ["$1-10", "$10-100", "$100-1K", "$1K-10K", "$10K-100K", "$100K+"]
TB_ORDER = ["<10s", "10-59s", "1-5 min", "5-15 min", "15-60 min", "1hr+"]

PLOTLY_LAYOUT = dict(template="plotly_white")

# Neutral bar palette for non-behavior charts
BAR_COLORS = ["#1e3a5f", "#2d5a8e", "#3b82b6", "#60a5d4", "#94c5e8", "#c7dff0"]

# ── Database setup ─────────────────────────────────────────────────────────────

def _safe_float(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return None

def _safe_int(v):
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _data_fingerprint():
    """Hash of flow/cache file mod-times so Streamlit cache invalidates on data changes."""
    import hashlib
    h = hashlib.md5()
    for pattern in [f"{FLOWS_DIR}/bridge_flows_*.csv", CACHE_FILE, ADDRESS_TYPE_CACHE]:
        for p in sorted(glob.glob(pattern)):
            h.update(f"{p}:{os.path.getmtime(p)}".encode())
    return h.hexdigest()


@st.cache_resource
def init_db(fingerprint=None):
    con = duckdb.connect(":memory:")

    con.execute(f"""
        CREATE TABLE flows AS
        SELECT * FROM read_csv_auto('{FLOWS_DIR}/bridge_flows_*.csv',
            ignore_errors=true, union_by_name=true)
    """)

    with open(CACHE_FILE) as f:
        cache = json.load(f)
    analysis_rows = []
    for key, entry in cache.items():
        analysis_rows.append({
            "cache_key": key,
            "a_source_chain": entry.get("source_chain", ""),
            "a_destination_chain": entry.get("destination_chain", ""),
            "a_amount_usd": _safe_float(entry.get("original_amount_usd")),
            "a_withdrawal_block": _safe_int(entry.get("withdrawal_block_number")),
            "a_recipient": (entry.get("recipient") or "").lower(),
            "next_to_1": (entry.get("next_to_1") or "").lower(),
            "next_block_1": _safe_int(entry.get("next_block_1")),
            "next_to_2": (entry.get("next_to_2") or "").lower(),
            "next_block_2": _safe_int(entry.get("next_block_2")),
            "next_to_3": (entry.get("next_to_3") or "").lower(),
            "next_block_3": _safe_int(entry.get("next_block_3")),
        })
    con.register("_analysis_df", pd.DataFrame(analysis_rows))
    con.execute("CREATE TABLE analysis AS SELECT * FROM _analysis_df")
    con.unregister("_analysis_df")

    addr_types = {}
    if os.path.exists(ADDRESS_TYPE_CACHE):
        with open(ADDRESS_TYPE_CACHE) as f:
            addr_types = json.load(f)
    at_rows = [{"address": k, "addr_type": v} for k, v in addr_types.items()]
    if at_rows:
        con.register("_at_df", pd.DataFrame(at_rows))
        con.execute("CREATE TABLE address_types AS SELECT * FROM _at_df")
        con.unregister("_at_df")
    else:
        con.execute("CREATE TABLE address_types (address VARCHAR, addr_type VARCHAR)")

    kc_rows = [{"address": k, "label": v["label"], "label_group": v["label_group"]}
               for k, v in KNOWN_CONTRACTS.items()]
    con.register("_kc_df", pd.DataFrame(kc_rows))
    con.execute("CREATE TABLE known_contracts AS SELECT * FROM _kc_df")
    con.unregister("_kc_df")

    bt_rows = [{"chain": k, "block_time": v} for k, v in CHAIN_BLOCK_TIMES.items()]
    con.register("_bt_df", pd.DataFrame(bt_rows))
    con.execute("CREATE TABLE block_times AS SELECT * FROM _bt_df")
    con.unregister("_bt_df")

    con.execute("""
        CREATE TABLE bridge_data AS
        SELECT
            f.deposit_chain AS source_chain,
            f.withdrawal_chain AS dest_chain,
            f.bridge_name,
            TRY_CAST(f.amount_usd AS DOUBLE) AS amount_usd,
            f.sender,
            LOWER(f.recipient) AS recipient,
            TRY_CAST(f.deposit_block_date AS DATE) AS tx_date,
            TRY_CAST(f.withdrawal_block_number AS BIGINT) AS withdrawal_block,
            a.next_to_1,
            a.next_block_1,
            a.next_to_2,
            a.next_to_3,
            CASE
                WHEN a.cache_key IS NULL THEN NULL
                WHEN a.next_to_1 IS NULL OR a.next_to_1 = '' THEN 'No Activity'
                WHEN kc.label_group IS NOT NULL THEN kc.label_group
                WHEN aty.addr_type = 'eoa' THEN 'EOA/Wallet'
                WHEN aty.addr_type = 'contract' THEN 'Other Contract'
                ELSE 'Unknown'
            END AS behavior,
            CASE
                WHEN a.next_block_1 IS NOT NULL AND a.a_withdrawal_block IS NOT NULL
                     AND a.next_block_1 > a.a_withdrawal_block AND a.a_withdrawal_block > 0
                THEN (a.next_block_1 - a.a_withdrawal_block) * COALESCE(bt.block_time, 2.0)
                ELSE NULL
            END AS time_to_action_sec,
            CASE WHEN a.cache_key IS NOT NULL THEN 1 ELSE 0 END AS has_analysis,
            CASE
                WHEN TRY_CAST(f.amount_usd AS DOUBLE) >= 100000 THEN '$100K+'
                WHEN TRY_CAST(f.amount_usd AS DOUBLE) >= 10000 THEN '$10K-100K'
                WHEN TRY_CAST(f.amount_usd AS DOUBLE) >= 1000 THEN '$1K-10K'
                WHEN TRY_CAST(f.amount_usd AS DOUBLE) >= 100 THEN '$100-1K'
                WHEN TRY_CAST(f.amount_usd AS DOUBLE) >= 10 THEN '$10-100'
                WHEN TRY_CAST(f.amount_usd AS DOUBLE) >= 1 THEN '$1-10'
                ELSE NULL
            END AS amount_bucket,
            CASE
                WHEN (a.next_block_1 IS NOT NULL AND a.a_withdrawal_block IS NOT NULL
                      AND a.next_block_1 > a.a_withdrawal_block AND a.a_withdrawal_block > 0) THEN
                    CASE
                        WHEN (a.next_block_1 - a.a_withdrawal_block) * COALESCE(bt.block_time, 2.0) >= 3600 THEN '1hr+'
                        WHEN (a.next_block_1 - a.a_withdrawal_block) * COALESCE(bt.block_time, 2.0) >= 900 THEN '15-60 min'
                        WHEN (a.next_block_1 - a.a_withdrawal_block) * COALESCE(bt.block_time, 2.0) >= 300 THEN '5-15 min'
                        WHEN (a.next_block_1 - a.a_withdrawal_block) * COALESCE(bt.block_time, 2.0) >= 60 THEN '1-5 min'
                        WHEN (a.next_block_1 - a.a_withdrawal_block) * COALESCE(bt.block_time, 2.0) >= 10 THEN '10-59s'
                        ELSE '<10s'
                    END
                ELSE NULL
            END AS time_bucket
        FROM flows f
        LEFT JOIN analysis a
            ON f.withdrawal_chain = a.a_destination_chain
            AND LOWER(f.recipient) = a.a_recipient
            AND TRY_CAST(f.withdrawal_block_number AS BIGINT) = a.a_withdrawal_block
        LEFT JOIN known_contracts kc ON a.next_to_1 = kc.address
        LEFT JOIN address_types aty ON a.next_to_1 = aty.address
        LEFT JOIN block_times bt ON f.withdrawal_chain = bt.chain
    """)

    return con


# ── Helpers ────────────────────────────────────────────────────────────────────

def q(con, sql):
    """Run SQL, return DataFrame (empty on error)."""
    try:
        return con.execute(sql).fetchdf()
    except Exception:
        return pd.DataFrame()


def safe_int_metric(val):
    """Safely convert a metric value to formatted int string."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "0"
    return f"{int(val):,}"


def safe_dollar_metric(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "$0"
    return f"${val:,.0f}"


def build_where(filters):
    clauses = []
    if filters.get("source_chains"):
        vals = ",".join(f"'{c}'" for c in filters["source_chains"])
        clauses.append(f"source_chain IN ({vals})")
    if filters.get("dest_chains"):
        vals = ",".join(f"'{c}'" for c in filters["dest_chains"])
        clauses.append(f"dest_chain IN ({vals})")
    if filters.get("bridges"):
        vals = ",".join(f"'{c}'" for c in filters["bridges"])
        clauses.append(f"bridge_name IN ({vals})")
    if filters.get("amount_min") is not None:
        clauses.append(f"amount_usd >= {filters['amount_min']}")
    if filters.get("amount_max") is not None:
        clauses.append(f"amount_usd <= {filters['amount_max']}")
    if filters.get("behaviors"):
        vals = ",".join(f"'{c}'" for c in filters["behaviors"])
        clauses.append(f"behavior IN ({vals})")
    if filters.get("time_buckets"):
        vals = ",".join(f"'{c}'" for c in filters["time_buckets"])
        clauses.append(f"time_bucket IN ({vals})")
    if filters.get("date_from"):
        clauses.append(f"(tx_date IS NULL OR tx_date >= '{filters['date_from']}')")
    if filters.get("date_to"):
        clauses.append(f"(tx_date IS NULL OR tx_date <= '{filters['date_to']}')")
    return " AND ".join(clauses) if clauses else "1=1"


def empty_state(msg="No data matches your current filters."):
    st.info(msg)


# ── App ────────────────────────────────────────────────────────────────────────

st.markdown('<h1>Bridge Analytics</h1>', unsafe_allow_html=True)
st.caption("Cross-chain bridge flows &middot; DuckDB &middot; 420K transactions")

con = init_db(fingerprint=_data_fingerprint())

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Filters")

    all_sources = q(con, "SELECT DISTINCT source_chain FROM bridge_data ORDER BY 1")
    sel_sources = st.multiselect("Source Chain",
        all_sources["source_chain"].tolist() if not all_sources.empty else [])

    if sel_sources:
        vals = ",".join(f"'{c}'" for c in sel_sources)
        dest_opts = q(con, f"SELECT DISTINCT dest_chain FROM bridge_data WHERE source_chain IN ({vals}) ORDER BY 1")
    else:
        dest_opts = q(con, "SELECT DISTINCT dest_chain FROM bridge_data ORDER BY 1")
    sel_dests = st.multiselect("Destination Chain",
        dest_opts["dest_chain"].tolist() if not dest_opts.empty else [])

    partial = build_where({"source_chains": sel_sources, "dest_chains": sel_dests})
    bridge_opts = q(con, f"SELECT DISTINCT bridge_name FROM bridge_data WHERE {partial} AND bridge_name IS NOT NULL ORDER BY 1")
    sel_bridges = st.multiselect("Bridge Protocol",
        bridge_opts["bridge_name"].tolist() if not bridge_opts.empty else [])

    st.markdown("##### Amount (USD)")
    ac1, ac2 = st.columns(2)
    amount_min = ac1.number_input("Min", value=0, min_value=0, step=100)
    amount_max = ac2.number_input("Max", value=0, min_value=0, step=100, help="0 = no limit")

    sel_time_buckets = st.multiselect("Time-to-First-Action", TB_ORDER)
    sel_behaviors = st.multiselect("Post-Bridge Behavior", BEHAVIOR_ORDER)

    st.markdown("##### Date Range")
    date_range = q(con, "SELECT MIN(tx_date) AS mn, MAX(tx_date) AS mx FROM bridge_data WHERE tx_date IS NOT NULL")
    if not date_range.empty and date_range.iloc[0]["mn"] is not None:
        d_min = date_range.iloc[0]["mn"]
        d_max = date_range.iloc[0]["mx"]
        dc1, dc2 = st.columns(2)
        date_from = dc1.date_input("From", value=d_min, min_value=d_min, max_value=d_max)
        date_to = dc2.date_input("To", value=d_max, min_value=d_min, max_value=d_max)
    else:
        date_from, date_to = None, None

filters = {
    "source_chains": sel_sources,
    "dest_chains": sel_dests,
    "bridges": sel_bridges,
    "amount_min": amount_min if amount_min > 0 else None,
    "amount_max": amount_max if amount_max > 0 else None,
    "behaviors": sel_behaviors,
    "time_buckets": sel_time_buckets,
    "date_from": str(date_from) if date_from and date_from != d_min else None,
    "date_to": str(date_to) if date_to and date_to != d_max else None,
}
where = build_where(filters)

# ── Metrics row ────────────────────────────────────────────────────────────────

metrics_df = q(con, f"""
    SELECT
        COUNT(*) AS total_txs,
        COALESCE(SUM(amount_usd), 0) AS total_volume,
        COUNT(DISTINCT sender) AS unique_senders,
        COUNT(DISTINCT recipient) AS unique_recipients,
        COALESCE(SUM(has_analysis), 0) AS with_analysis
    FROM bridge_data WHERE {where}
""")

if not metrics_df.empty:
    m = metrics_df.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Transactions", safe_int_metric(m.get("total_txs")))
    c2.metric("Volume", safe_dollar_metric(m.get("total_volume")))
    c3.metric("Senders", safe_int_metric(m.get("unique_senders")))
    c4.metric("Recipients", safe_int_metric(m.get("unique_recipients")))
    c5.metric("Analyzed", safe_int_metric(m.get("with_analysis")))
else:
    for c in st.columns(5):
        c.metric("-", "0")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Heatmap", "Protocols", "Amounts", "Behavior", "Timing", "Cross-Tabs", "Asset Flows", "Loops", "SQL",
])

# ── Tab 1: Volume Heatmap ─────────────────────────────────────────────────────
with tab1:
    heatmap_df = q(con, f"""
        SELECT source_chain, dest_chain, SUM(amount_usd) AS volume
        FROM bridge_data WHERE {where}
        GROUP BY source_chain, dest_chain
    """)
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot(index="dest_chain", columns="source_chain", values="volume").fillna(0)
        fig = go.Figure(data=go.Heatmap(
            z=np.log10(pivot.values + 1),
            x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale="YlOrRd",
            text=[[f"${v:,.0f}" for v in row] for row in pivot.values],
            texttemplate="%{text}", textfont={"size": 9},
            colorbar=dict(title="log10(USD)"),
            hovertemplate="Source: %{x}<br>Dest: %{y}<br>Volume: %{text}<extra></extra>",
        ))
        fig.update_layout(height=550, **PLOTLY_LAYOUT,
                          xaxis_title="Source Chain", yaxis_title="Destination Chain")
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

# ── Tab 2: Bridge Protocols ───────────────────────────────────────────────────
with tab2:
    bridge_df = q(con, f"""
        SELECT bridge_name, SUM(amount_usd) AS volume
        FROM bridge_data WHERE {where} AND bridge_name IS NOT NULL
        GROUP BY bridge_name ORDER BY volume DESC LIMIT 10
    """)
    if not bridge_df.empty:
        fig = go.Figure(data=[go.Pie(
            labels=bridge_df["bridge_name"].tolist(),
            values=bridge_df["volume"].tolist(),
            hole=0.45, textinfo="label+percent",
            marker=dict(colors=px.colors.qualitative.Set2),
        )])
        fig.update_layout(height=500, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

# ── Tab 3: Amount Distribution ────────────────────────────────────────────────
with tab3:
    amount_df = q(con, f"""
        SELECT amount_bucket, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND amount_bucket IS NOT NULL
        GROUP BY amount_bucket
    """)
    if not amount_df.empty:
        amount_df["sort_key"] = amount_df["amount_bucket"].map({b: i for i, b in enumerate(BUCKET_ORDER)})
        amount_df = amount_df.sort_values("sort_key")
        fig = go.Figure(go.Bar(
            x=amount_df["amount_bucket"], y=amount_df["cnt"],
            text=amount_df["cnt"], textposition="auto",
            textfont=dict(size=11),
            marker_color=BAR_COLORS[:len(amount_df)],
        ))
        fig.update_layout(height=400, **PLOTLY_LAYOUT,
                          xaxis_title="Amount Range", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

    # Drill-down
    st.markdown("### Behavior by Amount")
    amt_beh = q(con, f"""
        SELECT amount_bucket, behavior, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND amount_bucket IS NOT NULL AND behavior IS NOT NULL
        GROUP BY amount_bucket, behavior
    """)
    if not amt_beh.empty:
        amt_beh["sort_key"] = amt_beh["amount_bucket"].map({b: i for i, b in enumerate(BUCKET_ORDER)})
        amt_beh = amt_beh.sort_values("sort_key")
        fig = px.bar(amt_beh, x="amount_bucket", y="cnt", color="behavior",
                     color_discrete_map=BEHAVIOR_COLORS,
                     category_orders={"behavior": BEHAVIOR_ORDER, "amount_bucket": BUCKET_ORDER},
                     labels={"cnt": "Count", "amount_bucket": "Amount Range", "behavior": "Behavior"})
        fig.update_layout(barmode="stack", height=450, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 4: Post-Bridge Behavior ───────────────────────────────────────────────
with tab4:
    behavior_df = q(con, f"""
        SELECT dest_chain, behavior, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND behavior IS NOT NULL
        GROUP BY dest_chain, behavior
    """)
    if not behavior_df.empty:
        fig = px.bar(behavior_df, x="dest_chain", y="cnt", color="behavior",
                     color_discrete_map=BEHAVIOR_COLORS,
                     category_orders={"behavior": BEHAVIOR_ORDER},
                     labels={"cnt": "Count", "dest_chain": "Destination Chain", "behavior": "Behavior"})
        fig.update_layout(barmode="stack", height=550, **PLOTLY_LAYOUT,
                          legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5))
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

    st.markdown("### Behavior Counts")
    beh_totals = q(con, f"""
        SELECT behavior, COUNT(*) AS count,
               ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER(), 0), 1) AS pct
        FROM bridge_data WHERE {where} AND behavior IS NOT NULL
        GROUP BY behavior ORDER BY count DESC
    """)
    if not beh_totals.empty:
        st.dataframe(beh_totals, use_container_width=True, hide_index=True)

# ── Tab 5: Time-to-Action ─────────────────────────────────────────────────────
with tab5:
    time_df = q(con, f"""
        SELECT time_bucket, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND time_bucket IS NOT NULL
        GROUP BY time_bucket
    """)
    if not time_df.empty:
        time_df["sort_key"] = time_df["time_bucket"].map({b: i for i, b in enumerate(TB_ORDER)})
        time_df = time_df.sort_values("sort_key")

        col_l, col_r = st.columns(2)
        with col_l:
            fig = go.Figure(go.Bar(
                x=time_df["time_bucket"], y=time_df["cnt"],
                text=time_df["cnt"], textposition="auto",
                textfont=dict(size=11),
                marker_color=BAR_COLORS[:len(time_df)],
            ))
            fig.update_layout(height=400, **PLOTLY_LAYOUT,
                              xaxis_title="Time Bucket", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            box_df = q(con, f"""
                SELECT dest_chain, time_to_action_sec / 60.0 AS minutes
                FROM bridge_data WHERE {where} AND time_to_action_sec IS NOT NULL
                    AND time_to_action_sec < 86400
            """)
            if not box_df.empty:
                fig = px.box(box_df, x="dest_chain", y="minutes",
                             labels={"dest_chain": "Chain", "minutes": "Minutes"},
                             color_discrete_sequence=["#2d5a8e"])
                fig.update_layout(height=400, **PLOTLY_LAYOUT, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

    st.markdown("### Behavior by Time Bucket")
    time_beh = q(con, f"""
        SELECT time_bucket, behavior, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND time_bucket IS NOT NULL AND behavior IS NOT NULL
        GROUP BY time_bucket, behavior
    """)
    if not time_beh.empty:
        time_beh["sort_key"] = time_beh["time_bucket"].map({b: i for i, b in enumerate(TB_ORDER)})
        time_beh = time_beh.sort_values("sort_key")
        fig = px.bar(time_beh, x="time_bucket", y="cnt", color="behavior",
                     color_discrete_map=BEHAVIOR_COLORS,
                     category_orders={"behavior": BEHAVIOR_ORDER, "time_bucket": TB_ORDER},
                     labels={"cnt": "Count", "time_bucket": "Time Bucket", "behavior": "Behavior"})
        fig.update_layout(barmode="stack", height=450, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 6: Cross-Tabs ─────────────────────────────────────────────────────────
with tab6:
    st.markdown("### Behavior vs Amount (Normalized %)")
    cross_df = q(con, f"""
        SELECT amount_bucket, behavior, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND amount_bucket IS NOT NULL AND behavior IS NOT NULL
        GROUP BY amount_bucket, behavior
    """)
    if not cross_df.empty:
        cross_df["sort_key"] = cross_df["amount_bucket"].map({b: i for i, b in enumerate(BUCKET_ORDER)})
        cross_df = cross_df.sort_values("sort_key")
        totals = cross_df.groupby("amount_bucket")["cnt"].transform("sum")
        cross_df["pct"] = (cross_df["cnt"] / totals * 100).round(1)
        fig = px.bar(cross_df, x="amount_bucket", y="pct", color="behavior",
                     color_discrete_map=BEHAVIOR_COLORS,
                     category_orders={"behavior": BEHAVIOR_ORDER, "amount_bucket": BUCKET_ORDER},
                     labels={"pct": "Percentage (%)", "amount_bucket": "Amount Range", "behavior": "Behavior"})
        fig.update_layout(barmode="stack", height=450, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

    st.markdown("### Behavior vs Time (Normalized %)")
    cross_t = q(con, f"""
        SELECT time_bucket, behavior, COUNT(*) AS cnt
        FROM bridge_data WHERE {where} AND time_bucket IS NOT NULL AND behavior IS NOT NULL
        GROUP BY time_bucket, behavior
    """)
    if not cross_t.empty:
        cross_t["sort_key"] = cross_t["time_bucket"].map({b: i for i, b in enumerate(TB_ORDER)})
        cross_t = cross_t.sort_values("sort_key")
        totals = cross_t.groupby("time_bucket")["cnt"].transform("sum")
        cross_t["pct"] = (cross_t["cnt"] / totals * 100).round(1)
        fig = px.bar(cross_t, x="time_bucket", y="pct", color="behavior",
                     color_discrete_map=BEHAVIOR_COLORS,
                     category_orders={"behavior": BEHAVIOR_ORDER, "time_bucket": TB_ORDER},
                     labels={"pct": "Percentage (%)", "time_bucket": "Time Bucket", "behavior": "Behavior"})
        fig.update_layout(barmode="stack", height=450, **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
    else:
        empty_state()

# ── Tab 7: Asset Flows ────────────────────────────────────────────────────────
with tab7:
    ASSET_FLOW_FILE = "asset_flow_analysis.json"

    @st.cache_data
    def load_asset_flows():
        if not os.path.exists(ASSET_FLOW_FILE):
            return pd.DataFrame()
        with open(ASSET_FLOW_FILE) as f:
            data = json.load(f)
        return pd.DataFrame(data.get("flat", []))

    af_df = load_asset_flows()

    if af_df.empty:
        empty_state("No asset flow data. Run: python analyze_asset_flows.py")
    else:
        st.markdown("### Cross-Chain Asset Routing")
        st.caption(
            "For each source asset on each chain, shows what destination assets receive the volume. "
            "Same-asset transfers = bridging, different assets = cross-chain swaps."
        )

        # Transfer type filter
        transfer_type = st.radio(
            "Transfer Type", ["All", "Same Asset", "Cross-Chain Swap"],
            horizontal=True, key="af_transfer_type",
        )

        # Use sidebar filters
        af_filtered = af_df.copy()
        if sel_sources:
            af_filtered = af_filtered[af_filtered["source_chain"].isin(sel_sources)]
        if sel_dests:
            af_filtered = af_filtered[af_filtered["dest_chain"].isin(sel_dests)]
        if transfer_type == "Same Asset":
            af_filtered = af_filtered[af_filtered["source_asset"] == af_filtered["dest_asset"]]
        elif transfer_type == "Cross-Chain Swap":
            af_filtered = af_filtered[af_filtered["source_asset"] != af_filtered["dest_asset"]]

        if af_filtered.empty:
            empty_state()
        else:
            # ── Overview: top source assets by volume ──
            st.markdown("#### Top Source Assets by Volume")
            top_src = (
                af_filtered.groupby("source_asset")["volume_usd"]
                .sum()
                .sort_values(ascending=False)
                .head(15)
                .reset_index()
            )
            fig = go.Figure(go.Bar(
                x=top_src["source_asset"], y=top_src["volume_usd"],
                text=[f"${v:,.0f}" for v in top_src["volume_usd"]],
                textposition="auto", textfont=dict(size=10),
                marker_color=BAR_COLORS[2],
            ))
            fig.update_layout(height=400, **PLOTLY_LAYOUT,
                              xaxis_title="Source Asset", yaxis_title="Volume (USD)")
            st.plotly_chart(fig, use_container_width=True)

            # ── Same-asset vs cross-chain swap breakdown ──
            st.markdown("#### Same-Asset Bridge vs Cross-Chain Swap")
            af_filtered_copy = af_filtered.copy()
            af_filtered_copy["transfer_type"] = af_filtered_copy.apply(
                lambda r: "Same Asset" if r["source_asset"] == r["dest_asset"] else "Cross-Chain Swap",
                axis=1,
            )
            type_summary = (
                af_filtered_copy.groupby("transfer_type")["volume_usd"]
                .sum()
                .reset_index()
            )
            fig = go.Figure(data=[go.Pie(
                labels=type_summary["transfer_type"].tolist(),
                values=type_summary["volume_usd"].tolist(),
                hole=0.45, textinfo="label+percent",
                marker=dict(colors=["#2d5a8e", "#ea580c"]),
            )])
            fig.update_layout(height=400, **PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)

            # ── Per source-chain, per source-asset breakdown ──
            st.markdown("#### Destination Asset Breakdown per Source Asset")

            # Recalculate percentages based on filtered data
            src_totals = af_filtered.groupby(["source_chain", "source_asset"])["volume_usd"].sum().reset_index()
            src_totals.rename(columns={"volume_usd": "filtered_total_usd"}, inplace=True)
            af_detail = af_filtered.merge(src_totals, on=["source_chain", "source_asset"])
            af_detail["pct"] = (af_detail["volume_usd"] / af_detail["filtered_total_usd"] * 100).round(2)

            # Show per chain
            chains_to_show = sel_sources if sel_sources else sorted(af_filtered["source_chain"].unique())
            for chain in chains_to_show:
                chain_data = af_detail[af_detail["source_chain"] == chain]
                if chain_data.empty:
                    continue

                st.markdown(f"##### {chain.replace('_', ' ').title()}")

                # Top assets for this chain
                chain_assets = (
                    chain_data.groupby("source_asset")["volume_usd"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(8)
                    .index.tolist()
                )

                for asset in chain_assets:
                    asset_data = chain_data[chain_data["source_asset"] == asset].sort_values(
                        "volume_usd", ascending=False
                    )
                    total_vol = asset_data["filtered_total_usd"].iloc[0]

                    with st.expander(f"{asset} — ${total_vol:,.0f} total volume"):
                        # Stacked bar: dest_chain x dest_asset
                        fig = px.bar(
                            asset_data.head(20),
                            x="dest_chain", y="volume_usd", color="dest_asset",
                            text=asset_data.head(20)["pct"].apply(lambda p: f"{p:.1f}%"),
                            labels={"volume_usd": "Volume (USD)", "dest_chain": "Dest Chain",
                                    "dest_asset": "Dest Asset"},
                            color_discrete_sequence=px.colors.qualitative.Set2,
                        )
                        fig.update_layout(barmode="stack", height=350, **PLOTLY_LAYOUT)
                        st.plotly_chart(fig, use_container_width=True)

                        # Table
                        display = asset_data[["dest_chain", "dest_asset", "volume_usd", "pct"]].copy()
                        display.columns = ["Dest Chain", "Dest Asset", "Volume (USD)", "% of Source"]
                        display["Volume (USD)"] = display["Volume (USD)"].apply(lambda v: f"${v:,.0f}")
                        display["% of Source"] = display["% of Source"].apply(lambda v: f"{v:.2f}%")
                        st.dataframe(display, use_container_width=True, hide_index=True)

            # ── Sankey diagram for top flows ──
            st.markdown("#### Top Asset Flow Paths (Sankey)")
            all_assets = sorted(af_filtered["source_asset"].unique())
            sankey_asset = st.multiselect("Filter by asset", all_assets, key="sankey_asset")
            sankey_data = af_filtered[af_filtered["source_asset"].isin(sankey_asset)] if sankey_asset else af_filtered
            top_flows = sankey_data.nlargest(30, "volume_usd")
            if not top_flows.empty:
                # Build sankey: source_chain:asset → dest_chain:asset
                src_labels = (top_flows["source_chain"] + ": " + top_flows["source_asset"]).tolist()
                dst_labels = (top_flows["dest_chain"] + ": " + top_flows["dest_asset"]).tolist()
                all_labels = list(dict.fromkeys(src_labels + dst_labels))  # ordered unique
                label_idx = {l: i for i, l in enumerate(all_labels)}

                # Assign distinct colors per unique chain
                sankey_palette = [
                    "#3b82f6", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6",
                    "#ec4899", "#06b6d4", "#f97316", "#84cc16", "#6366f1",
                ]
                chain_names = list(dict.fromkeys(
                    top_flows["source_chain"].tolist() + top_flows["dest_chain"].tolist()
                ))
                chain_color = {c: sankey_palette[i % len(sankey_palette)] for i, c in enumerate(chain_names)}

                node_colors = []
                for lbl in all_labels:
                    chain = lbl.split(":")[0].strip()
                    node_colors.append(chain_color.get(chain, "#94a3b8"))

                # Link colors: translucent version of source node color
                link_colors = []
                for s in src_labels:
                    chain = s.split(":")[0].strip()
                    base = chain_color.get(chain, "#94a3b8")
                    # hex to rgba
                    r, g, b = int(base[1:3], 16), int(base[3:5], 16), int(base[5:7], 16)
                    link_colors.append(f"rgba({r},{g},{b},0.35)")

                fig = go.Figure(go.Sankey(
                    node=dict(
                        label=all_labels,
                        pad=20, thickness=24,
                        color=node_colors,
                        line=dict(width=0),
                    ),
                    link=dict(
                        source=[label_idx[s] for s in src_labels],
                        target=[label_idx[d] for d in dst_labels],
                        value=top_flows["volume_usd"].tolist(),
                        color=link_colors,
                    ),
                ))
                fig.update_layout(
                    height=650, **PLOTLY_LAYOUT,
                    font=dict(size=12, color="#e2e8f0"),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

# ── Tab 8: Loops ──────────────────────────────────────────────────────────────
with tab8:
    LOOP_FILE = "loop_analysis.json"

    @st.cache_data
    def load_loops():
        if not os.path.exists(LOOP_FILE):
            return pd.DataFrame()
        with open(LOOP_FILE) as f:
            return pd.DataFrame(json.load(f))

    loop_df = load_loops()

    if loop_df.empty:
        empty_state("No loop data. Run: python analyze_loops.py")
    else:
        st.markdown("### Round-Trip (Loop) Analysis")
        st.caption(
            "For each source→dest chain pair, shows the fraction of transactions where the same sender "
            "also bridged back in the opposite direction at any point."
        )

        # Apply sidebar filters
        lp_filtered = loop_df.copy()
        if sel_sources:
            lp_filtered = lp_filtered[lp_filtered["source_chain"].isin(sel_sources)]
        if sel_dests:
            lp_filtered = lp_filtered[lp_filtered["dest_chain"].isin(sel_dests)]

        if lp_filtered.empty:
            empty_state()
        else:
            # ── Metrics ──
            lp_c1, lp_c2, lp_c3 = st.columns(3)
            total_txs = lp_filtered["total_txs"].sum()
            total_loop_txs = lp_filtered["loop_txs"].sum()
            total_loop_senders = lp_filtered["loop_senders"].sum()
            lp_c1.metric("Total Transactions", f"{total_txs:,}")
            lp_c2.metric("Loop Transactions", f"{total_loop_txs:,}")
            lp_c3.metric("Overall Loop Rate", f"{total_loop_txs / total_txs * 100:.1f}%" if total_txs else "0%")

            # ── Heatmap: loop tx % by source→dest ──
            st.markdown("#### Loop Rate by Chain Pair (% of transactions)")
            pivot = lp_filtered.pivot(
                index="dest_chain", columns="source_chain", values="loop_tx_pct"
            ).fillna(0)
            fig = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns.tolist(), y=pivot.index.tolist(),
                colorscale="RdYlGn",
                text=[[f"{v:.1f}%" for v in row] for row in pivot.values],
                texttemplate="%{text}", textfont={"size": 11},
                colorbar=dict(title="Loop %"),
                hovertemplate="Source: %{x}<br>Dest: %{y}<br>Loop Rate: %{text}<extra></extra>",
            ))
            fig.update_layout(height=500, **PLOTLY_LAYOUT,
                              xaxis_title="Source Chain", yaxis_title="Destination Chain")
            st.plotly_chart(fig, use_container_width=True)

            # ── Bar: loop rate per source chain (averaged across destinations) ──
            st.markdown("#### Loop Rate by Source Chain")
            src_agg = lp_filtered.groupby("source_chain").apply(
                lambda g: pd.Series({
                    "loop_tx_pct": g["loop_txs"].sum() / g["total_txs"].sum() * 100 if g["total_txs"].sum() > 0 else 0,
                    "loop_sender_pct": g["loop_senders"].sum() / g["total_senders"].sum() * 100 if g["total_senders"].sum() > 0 else 0,
                })
            ).reset_index()
            src_agg = src_agg.sort_values("loop_tx_pct", ascending=False)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=src_agg["source_chain"], y=src_agg["loop_tx_pct"],
                name="% of Txs from Loopers",
                text=[f"{v:.1f}%" for v in src_agg["loop_tx_pct"]],
                textposition="auto",
                marker_color="#3b82f6",
            ))
            fig.add_trace(go.Bar(
                x=src_agg["source_chain"], y=src_agg["loop_sender_pct"],
                name="% of Senders who Loop",
                text=[f"{v:.1f}%" for v in src_agg["loop_sender_pct"]],
                textposition="auto",
                marker_color="#f59e0b",
            ))
            fig.update_layout(barmode="group", height=400, **PLOTLY_LAYOUT,
                              xaxis_title="Source Chain", yaxis_title="Percentage (%)")
            st.plotly_chart(fig, use_container_width=True)

            # ── Table ──
            st.markdown("#### Detailed Breakdown")
            display = lp_filtered[[
                "source_chain", "dest_chain", "total_txs", "loop_txs",
                "loop_tx_pct", "total_senders", "loop_senders", "loop_sender_pct",
            ]].copy()
            display.columns = [
                "Source", "Dest", "Total Txs", "Loop Txs",
                "Loop Tx %", "Total Senders", "Loop Senders", "Loop Sender %",
            ]
            display = display.sort_values("Loop Tx %", ascending=False)
            display["Total Txs"] = display["Total Txs"].apply(lambda v: f"{v:,}")
            display["Loop Txs"] = display["Loop Txs"].apply(lambda v: f"{v:,}")
            display["Loop Tx %"] = display["Loop Tx %"].apply(lambda v: f"{v:.1f}%")
            display["Total Senders"] = display["Total Senders"].apply(lambda v: f"{v:,}")
            display["Loop Senders"] = display["Loop Senders"].apply(lambda v: f"{v:,}")
            display["Loop Sender %"] = display["Loop Sender %"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(display, use_container_width=True, hide_index=True)

# ── Tab 9: SQL ─────────────────────────────────────────────────────────────────
with tab9:
    st.caption(
        "Columns: source_chain, dest_chain, bridge_name, amount_usd, sender, recipient, "
        "tx_date, behavior, time_to_action_sec, time_bucket, amount_bucket, has_analysis, "
        "next_to_1, next_to_2, next_to_3"
    )
    default_sql = f"""SELECT behavior, COUNT(*) AS cnt, ROUND(AVG(amount_usd), 2) AS avg_usd
FROM bridge_data
WHERE {where}
  AND behavior IS NOT NULL
GROUP BY behavior
ORDER BY cnt DESC"""
    user_sql = st.text_area("Query", value=default_sql, height=150)
    if st.button("Run"):
        result = q(con, user_sql)
        if not result.empty:
            st.dataframe(result, use_container_width=True, hide_index=True)
            csv_data = result.to_csv(index=False)
            st.download_button("Download CSV", csv_data, "query_result.csv", "text/csv")
        else:
            empty_state("Query returned no results.")

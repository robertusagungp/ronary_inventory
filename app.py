# ============================================
# RONARY INVENTORY SYSTEM - FINAL PRODUCTION
# Single file Streamlit app
# Auto sync Google Sheets
# ============================================

import streamlit as st
import pandas as pd
import sqlite3
import datetime as dt
import urllib.parse
import time
import os

# ============================================
# CONFIG
# ============================================

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_NAME = "Final Master Product"

AUTO_SYNC_SECONDS = 15
DB_FILE = "ronary_inventory.db"

st.set_page_config(
    page_title="Ronary Inventory System",
    layout="wide"
)

# ============================================
# DATABASE
# ============================================

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():

    conn = get_conn()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS products(
        item_sku TEXT PRIMARY KEY,
        base_sku TEXT,
        product_name TEXT,
        item_name TEXT,
        size TEXT,
        color TEXT,
        vendor TEXT,
        cost REAL,
        price REAL,
        created_at TEXT
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS stock(
        item_sku TEXT PRIMARY KEY,
        qty INTEGER,
        updated_at TEXT
    )
    """)

    conn.commit()
    conn.close()

# ============================================
# GOOGLE SHEET LOADER
# ============================================

def load_sheet():

    encoded_sheet = urllib.parse.quote(SHEET_NAME)

    url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet}"
    )

    df = pd.read_csv(url)

    df.columns = df.columns.str.strip()

    required = [
        "Product Name",
        "Item Name",
        "Size Name",
        "Warna Name",
        "Vendor Name",
        "SKU",
        "Item SKU",
        "Stock",
        "HPP",
        "Revenue"
    ]

    for col in required:
        if col not in df.columns:
            raise Exception(f"Missing column: {col}")

    df = df.rename(columns={
        "Product Name": "product_name",
        "Item Name": "item_name",
        "Size Name": "size",
        "Warna Name": "color",
        "Vendor Name": "vendor",
        "SKU": "base_sku",
        "Item SKU": "item_sku",
        "Stock": "stock",
        "HPP": "cost",
        "Revenue": "price"
    })

    df["item_sku"] = df["item_sku"].astype(str)
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    return df

# ============================================
# UPSERT LOGIC
# ============================================

def sync_db(df):

    conn = get_conn()

    inserted = 0
    updated = 0
    stock_updated = 0

    for _, r in df.iterrows():

        sku = r["item_sku"]
        new_stock = int(r["stock"])

        exists = conn.execute(
            "SELECT 1 FROM products WHERE item_sku=?",
            (sku,)
        ).fetchone()

        if exists:

            conn.execute("""
            UPDATE products SET
                base_sku=?,
                product_name=?,
                item_name=?,
                size=?,
                color=?,
                vendor=?,
                cost=?,
                price=?
            WHERE item_sku=?
            """, (
                r["base_sku"],
                r["product_name"],
                r["item_name"],
                r["size"],
                r["color"],
                r["vendor"],
                r["cost"],
                r["price"],
                sku
            ))

            updated += 1

        else:

            conn.execute("""
            INSERT INTO products VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (
                sku,
                r["base_sku"],
                r["product_name"],
                r["item_name"],
                r["size"],
                r["color"],
                r["vendor"],
                r["cost"],
                r["price"],
                dt.datetime.now().isoformat()
            ))

            inserted += 1

        stock_exists = conn.execute(
            "SELECT qty FROM stock WHERE item_sku=?",
            (sku,)
        ).fetchone()

        if stock_exists:

            old_stock = stock_exists[0]

            if old_stock != new_stock:

                conn.execute("""
                UPDATE stock
                SET qty=?, updated_at=?
                WHERE item_sku=?
                """, (
                    new_stock,
                    dt.datetime.now().isoformat(),
                    sku
                ))

                stock_updated += 1

        else:

            conn.execute("""
            INSERT INTO stock VALUES(?,?,?)
            """, (
                sku,
                new_stock,
                dt.datetime.now().isoformat()
            ))

            stock_updated += 1

    conn.commit()
    conn.close()

    return inserted, updated, stock_updated

# ============================================
# FETCH INVENTORY
# ============================================

def get_inventory():

    conn = get_conn()

    df = pd.read_sql_query("""
    SELECT
        p.base_sku,
        p.product_name,
        p.item_name,
        p.size,
        p.color,
        p.vendor,
        p.cost,
        p.price,
        s.qty,
        (p.price - p.cost) as profit
    FROM products p
    JOIN stock s
    ON p.item_sku = s.item_sku
    ORDER BY p.product_name
    """, conn)

    conn.close()

    return df

# ============================================
# AUTO SYNC CONTROLLER
# ============================================

def run_sync():

    try:

        df = load_sheet()

        ins, upd, stock_upd = sync_db(df)

        return True, ins, upd, stock_upd

    except Exception as e:

        return False, str(e), 0, 0

# ============================================
# INIT
# ============================================

init_db()

if "last_sync" not in st.session_state:
    st.session_state.last_sync = 0

if "sync_status" not in st.session_state:
    st.session_state.sync_status = "Never synced"

# ============================================
# HEADER
# ============================================

st.title("Ronary Inventory System")

col1, col2 = st.columns([1,1])

# Force Sync Button
if col1.button("Force Sync Now"):

    st.session_state.sync_status = "Syncing..."

    ok, ins, upd, stock_upd = run_sync()

    if ok:
        st.session_state.sync_status = (
            f"SYNC OK (new={ins}, updated={upd}, stock={stock_upd})"
        )
    else:
        st.session_state.sync_status = f"SYNC FAILED: {ins}"

    st.session_state.last_sync = time.time()

# Auto Sync
if time.time() - st.session_state.last_sync > AUTO_SYNC_SECONDS:

    ok, ins, upd, stock_upd = run_sync()

    if ok:
        st.session_state.sync_status = (
            f"SYNC OK (new={ins}, updated={upd}, stock={stock_upd})"
        )
    else:
        st.session_state.sync_status = f"SYNC FAILED: {ins}"

    st.session_state.last_sync = time.time()

# Status indicator
status = st.session_state.sync_status

if "FAILED" in status:
    st.error(status)
elif "Syncing" in status:
    st.warning(status)
elif "OK" in status:
    st.success(status)
else:
    st.info(status)

# ============================================
# DASHBOARD
# ============================================

df = get_inventory()

col1, col2, col3 = st.columns(3)

col1.metric("Total Units", int(df["qty"].sum()))
col2.metric("Inventory Value", int((df["qty"] * df["cost"]).sum()))
col3.metric("Profit Potential", int((df["qty"] * df["profit"]).sum()))

st.dataframe(df, use_container_width=True)

import streamlit as st
import pandas as pd
import sqlite3
import requests
import datetime as dt
import time

# ============================================
# CONFIG
# ============================================

st.set_page_config(
    page_title="Ronary Inventory System",
    layout="wide"
)

DB_FILE = "inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_NAME = "Final Master Product"

AUTO_SYNC_SECONDS = 15


# ============================================
# DATABASE
# ============================================

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def migrate_schema():

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

import urllib.parse

def load_sheet():

    encoded_sheet = urllib.parse.quote(SHEET_NAME)

    url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet}"

    df = pd.read_csv(url)

    df.columns = df.columns.str.strip()

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

    df["stock"] = df["stock"].fillna(0).astype(int)

    return df



# ============================================
# UPSERT LOGIC
# ============================================

def upsert(df):

    conn = get_conn()

    inserted = 0
    updated = 0
    stock_updated = 0

    for _, r in df.iterrows():

        sku = str(r["item_sku"]).strip()

        if not sku:
            continue

        new_stock = int(r["stock"])

        # check product exists
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
                float(r["cost"]),
                float(r["price"]),
                sku
            ))

            updated += 1

        else:

            conn.execute("""
            INSERT INTO products
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sku,
                r["base_sku"],
                r["product_name"],
                r["item_name"],
                r["size"],
                r["color"],
                r["vendor"],
                float(r["cost"]),
                float(r["price"]),
                dt.datetime.now().isoformat()
            ))

            inserted += 1

        # stock logic (FIXED)
        exists_stock = conn.execute(
            "SELECT qty FROM stock WHERE item_sku=?",
            (sku,)
        ).fetchone()

        if exists_stock:

            old_stock = exists_stock[0]

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
            INSERT INTO stock
            VALUES (?, ?, ?)
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
# INVENTORY VIEW
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
        (p.price - p.cost) AS profit
    FROM products p
    LEFT JOIN stock s ON p.item_sku = s.item_sku
    ORDER BY p.product_name
    """, conn)

    conn.close()

    df["qty"] = df["qty"].fillna(0)

    return df


# ============================================
# SYNC
# ============================================

def sync():

    try:

        df = load_sheet()

        ins, upd, stock_upd = upsert(df)

        return True, f"SYNC OK | inserted={ins} updated={upd} stock_updated={stock_upd}"

    except Exception as e:

        return False, str(e)


# ============================================
# UI
# ============================================

migrate_schema()

st.title("Ronary Inventory System")

# sync state
if "last_sync" not in st.session_state:
    st.session_state.last_sync = 0

if "sync_status" not in st.session_state:
    st.session_state.sync_status = "Not synced yet"

# auto sync
now = time.time()

if now - st.session_state.last_sync > AUTO_SYNC_SECONDS:

    ok, msg = sync()

    if ok:
        st.session_state.sync_status = msg
        st.session_state.sync_ok = True
    else:
        st.session_state.sync_status = msg
        st.session_state.sync_ok = False

    st.session_state.last_sync = now


# status indicator
if st.session_state.get("sync_ok", False):

    st.success(st.session_state.sync_status)

else:

    st.error(st.session_state.sync_status)


# force sync button
if st.button("Force Sync Now"):

    ok, msg = sync()

    if ok:
        st.success(msg)
    else:
        st.error(msg)


# inventory view
df = get_inventory()

col1, col2, col3 = st.columns(3)

col1.metric("Total Units", int(df["qty"].sum()))
col2.metric("Inventory Value", int((df["qty"] * df["cost"]).sum()))
col3.metric("Profit Potential", int((df["qty"] * df["profit"]).sum()))

st.dataframe(df, use_container_width=True)


# auto refresh
time.sleep(1)
st.rerun()

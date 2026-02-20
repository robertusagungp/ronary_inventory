import streamlit as st
import sqlite3
import pandas as pd
import requests
from io import StringIO
import datetime as dt
import os

# =====================================================
# CONFIG
# =====================================================

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"

SHEET_PRODUCTS = "Final Master Product"
SHEET_PRICE = "Master Price"

SYNC_CACHE_SECONDS = 300


# =====================================================
# DATABASE CONNECTION
# =====================================================

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)


# =====================================================
# AUTO MIGRATION
# =====================================================

def table_exists(conn, table):

    result = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()

    return result is not None


def get_columns(conn, table):

    cursor = conn.execute(f"PRAGMA table_info({table})")

    return [row[1] for row in cursor.fetchall()]


def migrate_schema():

    conn = get_conn()

    # PRODUCTS TABLE
    if not table_exists(conn, "products"):

        conn.execute("""
        CREATE TABLE products (

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

    else:

        cols = get_columns(conn, "products")

        required = [
            "item_sku",
            "base_sku",
            "product_name",
            "item_name",
            "size",
            "color",
            "vendor",
            "cost",
            "price"
        ]

        for col in required:

            if col not in cols:

                conn.execute(
                    f"ALTER TABLE products ADD COLUMN {col} TEXT"
                )

    # STOCK TABLE
    if not table_exists(conn, "stock"):

        conn.execute("""
        CREATE TABLE stock (

            item_sku TEXT PRIMARY KEY,

            qty INTEGER,

            updated_at TEXT

        )
        """)

    # MOVEMENTS TABLE
    if not table_exists(conn, "movements"):

        conn.execute("""
        CREATE TABLE movements (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            ts TEXT,

            item_sku TEXT,

            movement TEXT,

            qty INTEGER,

            reason TEXT

        )
        """)

    conn.commit()
    conn.close()


# =====================================================
# GOOGLE SHEETS
# =====================================================

def sheet_url(sheet):

    sheet = sheet.replace(" ", "%20")

    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet}"


@st.cache_data(ttl=SYNC_CACHE_SECONDS)
def load_sheet(sheet):

    url = sheet_url(sheet)

    r = requests.get(url, timeout=30)

    if r.status_code != 200:
        raise Exception(f"Cannot load {sheet}")

    df = pd.read_csv(StringIO(r.text))

    df.columns = df.columns.str.strip()

    return df


@st.cache_data(ttl=SYNC_CACHE_SECONDS)
def load_master():

    df_products = load_sheet(SHEET_PRODUCTS)

    df_price = load_sheet(SHEET_PRICE)

    df_products = df_products.rename(columns={
        "Item SKU": "item_sku",
        "SKU": "base_sku",
        "Product Name": "product_name",
        "Item Name": "item_name",
        "Size Name": "size",
        "Warna Name": "color",
        "Vendor Name": "vendor",
        "Stock": "stock"
    })

    df_price = df_price.rename(columns={
        "SKU": "base_sku",
        "HPP": "cost",
        "Revenue": "price"
    })

    df_price["cost"] = (
        df_price["cost"]
        .astype(str)
        .str.replace("Rp", "")
        .str.replace(".", "")
        .str.replace(",", "")
        .astype(float)
    )

    df_price["price"] = (
        df_price["price"]
        .astype(str)
        .str.replace("Rp", "")
        .str.replace(".", "")
        .str.replace(",", "")
        .astype(float)
    )

    df = df_products.merge(
        df_price[["base_sku", "cost", "price"]],
        on="base_sku",
        how="left"
    )

    df["stock"] = df["stock"].fillna(0)

    return df


# =====================================================
# AUTO SYNC
# =====================================================

def auto_sync():

    df = load_master()

    conn = get_conn()

    for _, row in df.iterrows():

        sku = row["item_sku"]

        exists = conn.execute(
            "SELECT item_sku FROM products WHERE item_sku=?",
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

                row["base_sku"],
                row["product_name"],
                row["item_name"],
                row["size"],
                row["color"],
                row["vendor"],
                row["cost"],
                row["price"],
                sku

            ))

        else:

            conn.execute("""
            INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (

                sku,
                row["base_sku"],
                row["product_name"],
                row["item_name"],
                row["size"],
                row["color"],
                row["vendor"],
                row["cost"],
                row["price"],
                dt.datetime.now().isoformat()

            ))

            conn.execute("""
            INSERT INTO stock VALUES (?, ?, ?)
            """, (

                sku,
                int(row["stock"]),
                dt.datetime.now().isoformat()

            ))

    conn.commit()
    conn.close()


# =====================================================
# STOCK OPS
# =====================================================

def add_stock(sku, qty):

    conn = get_conn()

    conn.execute(
        "UPDATE stock SET qty=qty+?, updated_at=? WHERE item_sku=?",
        (qty, dt.datetime.now().isoformat(), sku)
    )

    conn.execute(
        "INSERT INTO movements VALUES(NULL, ?, ?, 'IN', ?, 'MANUAL')",
        (dt.datetime.now().isoformat(), sku, qty)
    )

    conn.commit()
    conn.close()


def remove_stock(sku, qty):

    conn = get_conn()

    conn.execute(
        "UPDATE stock SET qty=qty-?, updated_at=? WHERE item_sku=?",
        (qty, dt.datetime.now().isoformat(), sku)
    )

    conn.execute(
        "INSERT INTO movements VALUES(NULL, ?, ?, 'OUT', ?, 'SOLD')",
        (dt.datetime.now().isoformat(), sku, qty)
    )

    conn.commit()
    conn.close()


# =====================================================
# LOAD DATA
# =====================================================

def get_inventory():

    conn = get_conn()

    df = pd.read_sql_query("""

    SELECT
    p.item_sku,
    p.product_name,
    p.size,
    p.color,
    p.vendor,
    p.cost,
    p.price,
    s.qty
    FROM products p
    JOIN stock s ON p.item_sku=s.item_sku
    ORDER BY p.product_name

    """, conn)

    conn.close()

    return df


# =====================================================
# STARTUP
# =====================================================

migrate_schema()

auto_sync()


# =====================================================
# UI
# =====================================================

st.set_page_config(layout="wide")

st.title("Ronary Inventory System")


menu = st.sidebar.selectbox(

    "Menu",

    [
        "Dashboard",
        "Add Stock",
        "Remove Stock",
        "Movement History"
    ]

)


if menu == "Dashboard":

    df = get_inventory()

    st.metric("Total Units", int(df["qty"].sum()))

    st.dataframe(df, use_container_width=True)


elif menu == "Add Stock":

    df = get_inventory()

    sku = st.selectbox("Item SKU", df["item_sku"])

    qty = st.number_input("Qty", min_value=1)

    if st.button("Add"):

        add_stock(sku, qty)

        st.rerun()


elif menu == "Remove Stock":

    df = get_inventory()

    sku = st.selectbox("Item SKU", df["item_sku"])

    qty = st.number_input("Qty", min_value=1)

    if st.button("Remove"):

        remove_stock(sku, qty)

        st.rerun()


elif menu == "Movement History":

    conn = get_conn()

    df = pd.read_sql_query(
        "SELECT * FROM movements ORDER BY id DESC",
        conn
    )

    conn.close()

    st.dataframe(df, use_container_width=True)

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

CACHE_TTL = 300


# =====================================================
# DATABASE
# =====================================================

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def table_exists(conn, name):

    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,)
    ).fetchone() is not None


def migrate_schema():

    conn = get_conn()

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

    if not table_exists(conn, "stock"):

        conn.execute("""
        CREATE TABLE stock (

            item_sku TEXT PRIMARY KEY,

            qty INTEGER,

            updated_at TEXT

        )
        """)

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


@st.cache_data(ttl=CACHE_TTL)
def load_sheet(sheet):

    url = sheet_url(sheet)

    r = requests.get(url)

    if r.status_code != 200:
        raise Exception(f"Failed loading sheet: {sheet}")

    df = pd.read_csv(StringIO(r.text))

    df.columns = df.columns.str.strip().str.lower()

    return df


def clean_currency(series):

    return (
        series.astype(str)
        .str.replace("rp", "", case=False)
        .str.replace(".", "")
        .str.replace(",", "")
        .str.strip()
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )


@st.cache_data(ttl=CACHE_TTL)
def load_master():

    df_products = load_sheet(SHEET_PRODUCTS)
    df_price = load_sheet(SHEET_PRICE)

    # rename products
    rename_products = {

        "item sku": "item_sku",
        "sku": "base_sku",
        "product name": "product_name",
        "item name": "item_name",
        "size name": "size",
        "warna name": "color",
        "vendor name": "vendor",
        "stock": "stock"

    }

    df_products = df_products.rename(columns=rename_products)

    if "stock" not in df_products.columns:
        df_products["stock"] = 0

    df_products["stock"] = pd.to_numeric(
        df_products["stock"],
        errors="coerce"
    ).fillna(0)

    # rename price
    rename_price = {

        "sku": "base_sku",
        "hpp": "cost",
        "revenue": "price"

    }

    df_price = df_price.rename(columns=rename_price)

    if "cost" in df_price.columns:
        df_price["cost"] = clean_currency(df_price["cost"])

    else:
        df_price["cost"] = 0

    if "price" in df_price.columns:
        df_price["price"] = clean_currency(df_price["price"])

    else:
        df_price["price"] = 0

    df = df_products.merge(
        df_price[["base_sku", "cost", "price"]],
        on="base_sku",
        how="left"
    )

    df["cost"] = df["cost"].fillna(0)
    df["price"] = df["price"].fillna(0)

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
# LOAD INVENTORY
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
    (p.price - p.cost) AS profit,
    s.qty

    FROM products p

    JOIN stock s

    ON p.item_sku=s.item_sku

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

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Units", int(df.qty.sum()))
    col2.metric("Total Value", int((df.price * df.qty).sum()))
    col3.metric("Total Profit Potential", int((df.profit * df.qty).sum()))

    st.dataframe(df, use_container_width=True)


elif menu == "Add Stock":

    df = get_inventory()

    sku = st.selectbox("Item SKU", df.item_sku)

    qty = st.number_input("Qty", min_value=1)

    if st.button("Add"):

        add_stock(sku, qty)
        st.rerun()


elif menu == "Remove Stock":

    df = get_inventory()

    sku = st.selectbox("Item SKU", df.item_sku)

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

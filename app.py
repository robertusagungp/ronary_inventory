import sqlite3
import datetime as dt
import pandas as pd
import streamlit as st
import requests
from io import StringIO

# =========================
# CONFIG
# =========================

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1Sn9oeiU_SV_rzJjY0sOMugnFKVvXm3lw_-TuhmlFRAg"

SHEET_MASTER_PRODUCT = "Final Master Product"
SHEET_MASTER_PRICE = "Master Price"


# =========================
# GOOGLE SHEETS FUNCTIONS
# =========================

def sheet_to_csv_url(sheet_name):

    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"


def load_sheet(sheet_name):

    url = sheet_to_csv_url(sheet_name)

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed loading sheet: {sheet_name}")

    return pd.read_csv(StringIO(response.text))


def load_master_products():

    df_products = load_sheet(SHEET_MASTER_PRODUCT)

    df_price = load_sheet(SHEET_MASTER_PRICE)

    df_products.columns = df_products.columns.str.lower()
    df_price.columns = df_price.columns.str.lower()

    # EXPECTED columns:
    # sku, product_name, category, color, size
    # price sheet: sku, cost, price

    df = df_products.merge(df_price, on="sku", how="left")

    df["is_active"] = 1

    return df


# =========================
# DATABASE INIT
# =========================

def get_conn():

    return sqlite3.connect(DB_FILE, check_same_thread=False)


def init_db():

    conn = get_conn()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS products (
        sku TEXT PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        color TEXT,
        size TEXT,
        cost REAL,
        price REAL,
        is_active INTEGER,
        created_at TEXT
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS stock (
        sku TEXT,
        location TEXT,
        qty INTEGER,
        low_stock_threshold INTEGER,
        updated_at TEXT,
        PRIMARY KEY (sku, location)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        sku TEXT,
        movement_type TEXT,
        location TEXT,
        qty INTEGER,
        reason TEXT
    )
    """)

    conn.commit()
    conn.close()


# =========================
# PRODUCT SYNC
# =========================

def sync_products_from_gsheet():

    df = load_master_products()

    conn = get_conn()

    inserted = 0
    updated = 0

    for _, row in df.iterrows():

        sku = str(row["sku"]).strip().upper()

        product_name = str(row.get("product_name", ""))
        category = str(row.get("category", ""))
        color = str(row.get("color", ""))
        size = str(row.get("size", ""))

        cost = float(row.get("cost", 0))
        price = float(row.get("price", 0))

        exists = conn.execute(
            "SELECT sku FROM products WHERE sku=?",
            (sku,)
        ).fetchone()

        if exists:

            conn.execute("""
            UPDATE products SET
            product_name=?,
            category=?,
            color=?,
            size=?,
            cost=?,
            price=?,
            is_active=1
            WHERE sku=?
            """, (
                product_name,
                category,
                color,
                size,
                cost,
                price,
                sku
            ))

            updated += 1

        else:

            conn.execute("""
            INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sku,
                product_name,
                category,
                color,
                size,
                cost,
                price,
                1,
                dt.datetime.now().isoformat()
            ))

            conn.execute("""
            INSERT INTO stock VALUES (?, ?, ?, ?, ?)
            """, (
                sku,
                "MAIN",
                0,
                5,
                dt.datetime.now().isoformat()
            ))

            inserted += 1

    conn.commit()
    conn.close()

    return inserted, updated


# =========================
# STOCK FUNCTIONS
# =========================

def add_stock(sku, qty):

    conn = get_conn()

    conn.execute("""
    UPDATE stock
    SET qty = qty + ?
    WHERE sku=? AND location='MAIN'
    """, (qty, sku))

    conn.execute("""
    INSERT INTO movements
    VALUES (NULL, ?, ?, 'IN', 'MAIN', ?, 'MANUAL')
    """, (
        dt.datetime.now().isoformat(),
        sku,
        qty
    ))

    conn.commit()
    conn.close()


def remove_stock(sku, qty):

    conn = get_conn()

    conn.execute("""
    UPDATE stock
    SET qty = qty - ?
    WHERE sku=? AND location='MAIN'
    """, (qty, sku))

    conn.execute("""
    INSERT INTO movements
    VALUES (NULL, ?, ?, 'OUT', 'MAIN', ?, 'SOLD')
    """, (
        dt.datetime.now().isoformat(),
        sku,
        qty
    ))

    conn.commit()
    conn.close()


# =========================
# DATA LOAD
# =========================

def get_inventory():

    conn = get_conn()

    df = pd.read_sql_query("""
    SELECT
    p.sku,
    p.product_name,
    p.category,
    p.color,
    p.size,
    p.price,
    s.qty
    FROM products p
    JOIN stock s ON p.sku = s.sku
    """, conn)

    conn.close()

    return df


def get_products():

    conn = get_conn()

    df = pd.read_sql_query(
        "SELECT sku, product_name FROM products",
        conn
    )

    conn.close()

    return df


def get_movements():

    conn = get_conn()

    df = pd.read_sql_query(
        "SELECT * FROM movements ORDER BY id DESC",
        conn
    )

    conn.close()

    return df


# =========================
# UI
# =========================

st.set_page_config(layout="wide")

init_db()

st.title("Ronary Inventory System")

menu = st.sidebar.selectbox(
    "Menu",
    [
        "Dashboard",
        "Sync Master Product",
        "Add Stock",
        "Remove Stock",
        "Movement History"
    ]
)


# DASHBOARD

if menu == "Dashboard":

    df = get_inventory()

    total = df["qty"].sum()

    st.metric("Total Stock", total)

    st.dataframe(df)


# SYNC

elif menu == "Sync Master Product":

    st.write("Sync from Google Sheets")

    if st.button("Sync Now"):

        inserted, updated = sync_products_from_gsheet()

        st.success(f"Inserted: {inserted}, Updated: {updated}")


# ADD STOCK

elif menu == "Add Stock":

    products = get_products()

    sku = st.selectbox("SKU", products["sku"])

    qty = st.number_input("Qty", min_value=1)

    if st.button("Add"):

        add_stock(sku, qty)

        st.success("Stock added")


# REMOVE STOCK

elif menu == "Remove Stock":

    products = get_products()

    sku = st.selectbox("SKU", products["sku"])

    qty = st.number_input("Qty", min_value=1)

    if st.button("Remove"):

        remove_stock(sku, qty)

        st.success("Stock removed")


# MOVEMENTS

elif menu == "Movement History":

    df = get_movements()

    st.dataframe(df)

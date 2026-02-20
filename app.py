import streamlit as st
import sqlite3
import pandas as pd
import requests
from io import StringIO
import datetime as dt


# =====================================
# CONFIG
# =====================================

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"

SHEET_PRODUCTS = "Final Master Product"
SHEET_PRICE = "Master Price"


# =====================================
# GOOGLE SHEETS LOADER
# =====================================

def sheet_to_csv_url(sheet_name):

    sheet_name_encoded = sheet_name.replace(" ", "%20")

    return (
        f"https://docs.google.com/spreadsheets/d/"
        f"{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet_name_encoded}"
    )


def load_sheet(sheet_name):

    url = sheet_to_csv_url(sheet_name)

    response = requests.get(url, timeout=30)

    if response.status_code != 200:

        raise Exception(
            f"Failed loading sheet: {sheet_name}\n"
            f"Status: {response.status_code}\n"
            f"URL: {url}"
        )

    df = pd.read_csv(StringIO(response.text))

    if df.empty:

        raise Exception(f"Sheet '{sheet_name}' is empty")

    df.columns = df.columns.str.strip().str.lower()

    return df


def load_master_products():

    df_products = load_sheet(SHEET_PRODUCTS)

    df_price = load_sheet(SHEET_PRICE)

    if "sku" not in df_products.columns:
        raise Exception("Column 'sku' not found in Final Master Product")

    if "sku" not in df_price.columns:
        raise Exception("Column 'sku' not found in Master Price")

    df = df_products.merge(df_price, on="sku", how="left")

    if "cost" not in df.columns:
        df["cost"] = 0

    if "price" not in df.columns:
        df["price"] = 0

    df["is_active"] = 1

    return df


# =====================================
# DATABASE
# =====================================

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
        sku TEXT PRIMARY KEY,
        qty INTEGER,
        updated_at TEXT
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        sku TEXT,
        movement TEXT,
        qty INTEGER,
        reason TEXT
    )
    """)

    conn.commit()
    conn.close()


# =====================================
# SYNC PRODUCTS
# =====================================

def sync_products():

    df = load_master_products()

    conn = get_conn()

    inserted = 0
    updated = 0

    for _, row in df.iterrows():

        sku = str(row["sku"]).strip().upper()

        name = str(row.get("product_name", ""))

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
                name,
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
                name,
                category,
                color,
                size,
                cost,
                price,
                1,
                dt.datetime.now().isoformat()
            ))

            conn.execute("""
            INSERT INTO stock VALUES (?, ?, ?)
            """, (
                sku,
                0,
                dt.datetime.now().isoformat()
            ))

            inserted += 1

    conn.commit()
    conn.close()

    return inserted, updated


# =====================================
# STOCK FUNCTIONS
# =====================================

def add_stock(sku, qty):

    conn = get_conn()

    conn.execute("""
    UPDATE stock SET qty = qty + ?, updated_at=?
    WHERE sku=?
    """, (
        qty,
        dt.datetime.now().isoformat(),
        sku
    ))

    conn.execute("""
    INSERT INTO movements VALUES (NULL, ?, ?, 'IN', ?, 'MANUAL')
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
    UPDATE stock SET qty = qty - ?, updated_at=?
    WHERE sku=?
    """, (
        qty,
        dt.datetime.now().isoformat(),
        sku
    ))

    conn.execute("""
    INSERT INTO movements VALUES (NULL, ?, ?, 'OUT', ?, 'SOLD')
    """, (
        dt.datetime.now().isoformat(),
        sku,
        qty
    ))

    conn.commit()
    conn.close()


# =====================================
# LOAD DATA
# =====================================

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
    ORDER BY p.product_name
    """, conn)

    conn.close()

    return df


def get_products():

    conn = get_conn()

    df = pd.read_sql_query(
        "SELECT sku FROM products ORDER BY sku",
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


# =====================================
# APP UI
# =====================================

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


# =====================================
# DASHBOARD
# =====================================

if menu == "Dashboard":

    df = get_inventory()

    total = df["qty"].sum()

    st.metric("Total Stock", total)

    st.dataframe(df, use_container_width=True)


# =====================================
# SYNC
# =====================================

elif menu == "Sync Master Product":

    st.write("Sync from Google Sheets")

    if st.button("Test Connection"):

        try:

            df = load_master_products()

            st.success("Connection OK")

            st.dataframe(df.head())

        except Exception as e:

            st.error(str(e))

    if st.button("Sync Now"):

        try:

            inserted, updated = sync_products()

            st.success(
                f"Sync complete. Inserted: {inserted}, Updated: {updated}"
            )

        except Exception as e:

            st.error(str(e))


# =====================================
# ADD STOCK
# =====================================

elif menu == "Add Stock":

    products = get_products()

    sku = st.selectbox("SKU", products["sku"])

    qty = st.number_input("Quantity", min_value=1)

    if st.button("Add"):

        add_stock(sku, qty)

        st.success("Stock added")


# =====================================
# REMOVE STOCK
# =====================================

elif menu == "Remove Stock":

    products = get_products()

    sku = st.selectbox("SKU", products["sku"])

    qty = st.number_input("Quantity", min_value=1)

    if st.button("Remove"):

        remove_stock(sku, qty)

        st.success("Stock removed")


# =====================================
# HISTORY
# =====================================

elif menu == "Movement History":

    df = get_movements()

    st.dataframe(df, use_container_width=True)

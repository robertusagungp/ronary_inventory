import streamlit as st
import sqlite3
import pandas as pd
import requests
from io import StringIO
import datetime as dt

# =====================================================
# CONFIG
# =====================================================

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"

SHEET_PRODUCTS = "Final Master Product"
SHEET_PRICE = "Master Price"

# =====================================================
# GOOGLE SHEETS LOADER
# =====================================================

def sheet_url(sheet):

    sheet = sheet.replace(" ", "%20")

    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet}"


def load_sheet(sheet):

    url = sheet_url(sheet)

    r = requests.get(url, timeout=30)

    if r.status_code != 200:

        raise Exception(f"Cannot load sheet: {sheet}")

    df = pd.read_csv(StringIO(r.text))

    df.columns = df.columns.str.strip()

    return df


# =====================================================
# MASTER LOAD
# =====================================================

def load_master():

    df_products = load_sheet(SHEET_PRODUCTS)

    df_price = load_sheet(SHEET_PRICE)

    # normalize
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

    # convert currency
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

    # join
    df = df_products.merge(
        df_price[["base_sku", "cost", "price"]],
        on="base_sku",
        how="left"
    )

    df["stock"] = df["stock"].fillna(0)

    return df


# =====================================================
# DATABASE INIT
# =====================================================

def get_conn():

    return sqlite3.connect(DB_FILE, check_same_thread=False)


def init_db():

    conn = get_conn()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS products (

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
    CREATE TABLE IF NOT EXISTS stock (

        item_sku TEXT PRIMARY KEY,

        qty INTEGER,

        updated_at TEXT

    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS movements (

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
# SYNC
# =====================================================

def sync():

    df = load_master()

    conn = get_conn()

    inserted = 0
    updated = 0

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

            updated += 1

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

            inserted += 1

    conn.commit()

    conn.close()

    return inserted, updated


# =====================================================
# STOCK OPS
# =====================================================

def add_stock(sku, qty):

    conn = get_conn()

    conn.execute("""

    UPDATE stock

    SET qty = qty + ?, updated_at=?

    WHERE item_sku=?

    """, (

        qty,

        dt.datetime.now().isoformat(),

        sku

    ))

    conn.execute("""

    INSERT INTO movements VALUES(NULL, ?, ?, 'IN', ?, 'MANUAL')

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

    SET qty = qty - ?, updated_at=?

    WHERE item_sku=?

    """, (

        qty,

        dt.datetime.now().isoformat(),

        sku

    ))

    conn.execute("""

    INSERT INTO movements VALUES(NULL, ?, ?, 'OUT', ?, 'SOLD')

    """, (

        dt.datetime.now().isoformat(),

        sku,

        qty

    ))

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

    p.item_name,

    p.size,

    p.color,

    p.vendor,

    p.cost,

    p.price,

    s.qty

    FROM products p

    JOIN stock s

    ON p.item_sku=s.item_sku

    ORDER BY p.product_name

    """, conn)

    conn.close()

    return df


def get_skus():

    conn = get_conn()

    df = pd.read_sql_query(

        "SELECT item_sku FROM products ORDER BY item_sku",

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


# =====================================================
# UI
# =====================================================

st.set_page_config(layout="wide")

init_db()

st.title("Ronary Inventory System")


menu = st.sidebar.selectbox(

    "Menu",

    [

        "Dashboard",

        "Sync Master",

        "Add Stock",

        "Remove Stock",

        "Movement History"

    ]

)


if menu == "Dashboard":

    df = get_inventory()

    st.metric("Total Units", int(df["qty"].sum()))

    st.dataframe(df, use_container_width=True)


elif menu == "Sync Master":

    if st.button("Sync Now"):

        inserted, updated = sync()

        st.success(f"Inserted {inserted}, Updated {updated}")


elif menu == "Add Stock":

    skus = get_skus()

    sku = st.selectbox("Item SKU", skus["item_sku"])

    qty = st.number_input("Qty", min_value=1)

    if st.button("Add"):

        add_stock(sku, qty)

        st.success("Added")


elif menu == "Remove Stock":

    skus = get_skus()

    sku = st.selectbox("Item SKU", skus["item_sku"])

    qty = st.number_input("Qty", min_value=1)

    if st.button("Remove"):

        remove_stock(sku, qty)

        st.success("Removed")


elif menu == "Movement History":

    df = get_movements()

    st.dataframe(df, use_container_width=True)

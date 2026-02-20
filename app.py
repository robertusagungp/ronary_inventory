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

CACHE_TTL = 300


# =====================================================
# DATABASE
# =====================================================

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def migrate_schema():

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
        raise Exception(f"Cannot load {sheet}")

    df = pd.read_csv(StringIO(r.text))

    df.columns = df.columns.str.strip().str.lower()

    return df


def find_column(df, possible_names):

    for name in possible_names:
        if name in df.columns:
            return name

    return None


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

    # auto detect columns
    col_item_sku = find_column(df_products, ["item sku"])
    col_base_sku = find_column(df_products, ["sku"])
    col_product = find_column(df_products, ["product name"])
    col_item = find_column(df_products, ["item name"])
    col_size = find_column(df_products, ["size name"])
    col_color = find_column(df_products, ["warna name"])
    col_vendor = find_column(df_products, ["vendor name"])
    col_stock = find_column(df_products, ["stock"])

    if col_item_sku is None:
        raise Exception(f"Item SKU column not found. Available: {df_products.columns}")

    df_products = df_products.rename(columns={
        col_item_sku: "item_sku",
        col_base_sku: "base_sku",
        col_product: "product_name",
        col_item: "item_name",
        col_size: "size",
        col_color: "color",
        col_vendor: "vendor"
    })

    if col_stock:
        df_products = df_products.rename(columns={col_stock: "stock"})
    else:
        df_products["stock"] = 0

    df_products["stock"] = pd.to_numeric(df_products["stock"], errors="coerce").fillna(0)

    # price
    col_price_sku = find_column(df_price, ["sku"])
    col_cost = find_column(df_price, ["hpp"])
    col_price = find_column(df_price, ["revenue"])

    df_price = df_price.rename(columns={
        col_price_sku: "base_sku",
        col_cost: "cost",
        col_price: "price"
    })

    df_price["cost"] = clean_currency(df_price["cost"])
    df_price["price"] = clean_currency(df_price["price"])

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

        sku = str(row["item_sku"]).strip()

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

                row.get("base_sku", ""),
                row.get("product_name", ""),
                row.get("item_name", ""),
                row.get("size", ""),
                row.get("color", ""),
                row.get("vendor", ""),
                row.get("cost", 0),
                row.get("price", 0),
                sku

            ))

        else:

            conn.execute("""
            INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (

                sku,
                row.get("base_sku", ""),
                row.get("product_name", ""),
                row.get("item_name", ""),
                row.get("size", ""),
                row.get("color", ""),
                row.get("vendor", ""),
                row.get("cost", 0),
                row.get("price", 0),
                dt.datetime.now().isoformat()

            ))

            conn.execute("""
            INSERT INTO stock VALUES (?, ?, ?)
            """, (

                sku,
                int(row.get("stock", 0)),
                dt.datetime.now().isoformat()

            ))

    conn.commit()
    conn.close()


# =====================================================
# INVENTORY
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

    ["Dashboard"]

)


df = get_inventory()

st.metric("Total Units", int(df.qty.sum()))
st.metric("Total Value", int((df.price * df.qty).sum()))
st.metric("Total Profit", int((df.profit * df.qty).sum()))

st.dataframe(df, use_container_width=True)

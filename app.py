import streamlit as st
import sqlite3
import pandas as pd
import requests
from io import StringIO
import datetime as dt
import re

# =====================================================
# CONFIG
# =====================================================

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_NAME = "Final Master Product"


# =====================================================
# DATABASE
# =====================================================

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

    conn.execute("""
    CREATE TABLE IF NOT EXISTS movements(
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
# GOOGLE SHEET LOADER
# =====================================================

def sheet_url():

    sheet = SHEET_NAME.replace(" ", "%20")

    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet}"


def rp_to_number(x):

    x = str(x)

    x = x.replace("Rp","")
    x = x.replace(".","")
    x = x.replace(",","")

    x = re.sub(r"[^\d\-]","",x)

    if x == "":
        return 0

    return float(x)


@st.cache_data(ttl=300)
def load_sheet():

    url = sheet_url()

    r = requests.get(url)

    if r.status_code != 200:

        raise Exception("Cannot access Google Sheet. Make sure sharing is PUBLIC.")

    df = pd.read_csv(StringIO(r.text))

    # STRIP COLUMN NAMES
    df.columns = df.columns.str.strip()

    required_cols = [
        "Item SKU",
        "SKU",
        "Product Name",
        "Item Name",
        "Size Name",
        "Warna Name",
        "Vendor Name",
        "Stock",
        "HPP",
        "Revenue"
    ]

    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        raise Exception(f"Missing columns in sheet: {missing}")

    out = pd.DataFrame()

    out["item_sku"] = df["Item SKU"].astype(str).str.strip()

    out["base_sku"] = df["SKU"].astype(str).str.strip()

    out["product_name"] = df["Product Name"]

    out["item_name"] = df["Item Name"]

    out["size"] = df["Size Name"]

    out["color"] = df["Warna Name"]

    out["vendor"] = df["Vendor Name"]

    out["stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0)

    out["cost"] = df["HPP"].apply(rp_to_number)

    out["price"] = df["Revenue"].apply(rp_to_number)

    out = out[out["item_sku"] != ""]

    return out


# =====================================================
# SYNC
# =====================================================

def sync_master():

    df = load_sheet()

    conn = get_conn()

    inserted = 0
    updated = 0

    for _, r in df.iterrows():

        exists = conn.execute(
            "SELECT 1 FROM products WHERE item_sku=?",
            (r.item_sku,)
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
            """,
            (
                r.base_sku,
                r.product_name,
                r.item_name,
                r.size,
                r.color,
                r.vendor,
                r.cost,
                r.price,
                r.item_sku
            ))

            updated+=1

        else:

            conn.execute("""
            INSERT INTO products VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                r.item_sku,
                r.base_sku,
                r.product_name,
                r.item_name,
                r.size,
                r.color,
                r.vendor,
                r.cost,
                r.price,
                dt.datetime.now().isoformat()
            ))

            conn.execute("""
            INSERT OR IGNORE INTO stock VALUES (?,?,?)
            """,
            (
                r.item_sku,
                int(r.stock),
                dt.datetime.now().isoformat()
            ))

            inserted+=1

    conn.commit()
    conn.close()

    return inserted, updated


# =====================================================
# INVENTORY OPS
# =====================================================

def get_inventory():

    conn = get_conn()

    df = pd.read_sql_query("""

        SELECT
            p.item_sku,
            p.base_sku,
            p.product_name,
            p.item_name,
            p.size,
            p.color,
            p.vendor,
            p.cost,
            p.price,
            s.qty,
            (p.price - p.cost) profit

        FROM products p
        JOIN stock s ON p.item_sku=s.item_sku

        ORDER BY product_name,color,size

    """, conn)

    conn.close()

    return df


def add_stock(item_sku, qty):

    conn = get_conn()

    conn.execute("""
        UPDATE stock
        SET qty=qty+?,
        updated_at=?
        WHERE item_sku=?
    """,
    (qty,dt.datetime.now().isoformat(),item_sku))

    conn.commit()
    conn.close()


def remove_stock(item_sku, qty):

    conn = get_conn()

    conn.execute("""
        UPDATE stock
        SET qty=qty-?,
        updated_at=?
        WHERE item_sku=?
    """,
    (qty,dt.datetime.now().isoformat(),item_sku))

    conn.commit()
    conn.close()


# =====================================================
# STARTUP
# =====================================================

migrate_schema()

try:

    ins, upd = sync_master()

    SYNC_STATUS = f"Auto Sync OK (Inserted {ins}, Updated {upd})"

except Exception as e:

    SYNC_STATUS = f"Sync Failed: {e}"


# =====================================================
# UI
# =====================================================

st.set_page_config(layout="wide")

st.title("Ronary Inventory System")

st.caption(SYNC_STATUS)

menu = st.sidebar.selectbox(
    "Menu",
    ["Dashboard","Add Stock","Remove Stock"]
)

df = get_inventory()

if menu=="Dashboard":

    c1,c2,c3=st.columns(3)

    c1.metric("Total Units", int(df.qty.sum()))

    c2.metric("Inventory Value", int((df.price*df.qty).sum()))

    c3.metric("Profit Potential", int((df.profit*df.qty).sum()))

    st.dataframe(df,use_container_width=True)


elif menu=="Add Stock":

    sku=st.selectbox("Item SKU", df.item_sku)

    qty=st.number_input("Qty",1,1000,1)

    if st.button("Add"):

        add_stock(sku,qty)

        st.rerun()


elif menu=="Remove Stock":

    sku=st.selectbox("Item SKU", df.item_sku)

    qty=st.number_input("Qty",1,1000,1)

    if st.button("Remove"):

        remove_stock(sku,qty)

        st.rerun()

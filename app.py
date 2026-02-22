import streamlit as st
import pandas as pd
import sqlite3
import datetime as dt
import urllib.parse
import requests
from io import StringIO
import re

# Optional Google Sheets write
HAS_GSHEETS_WRITE = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSHEETS_WRITE = True
except:
    HAS_GSHEETS_WRITE = False


# =====================================================
# CONFIG
# =====================================================

st.set_page_config(page_title="Ronary Inventory System", layout="wide")

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_NAME = "Final Master Product"

AUTO_PULL_SECONDS = 15
LOW_STOCK_THRESHOLD_DEFAULT = 2


# =====================================================
# HELPERS
# =====================================================

def now_iso():
    return dt.datetime.now().isoformat(timespec="seconds")


def norm(s):
    s = str(s)
    s = s.replace("\ufeff", "")
    s = s.replace("\u00a0", " ")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def rp_to_number(x):
    x = str(x)
    x = x.replace("Rp", "")
    x = x.replace(".", "")
    x = x.replace(",", "")
    x = re.sub(r"[^\d]", "", x)
    return float(x) if x else 0


def sheet_csv_url():
    encoded_sheet = urllib.parse.quote(SHEET_NAME)
    return (
        f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export"
        f"?format=csv&sheet={encoded_sheet}&_ts={int(dt.datetime.now().timestamp())}"
    )


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
# CORE INVENTORY
# =====================================================

def db_get_inventory():

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
        COALESCE(s.qty,0) qty,
        (p.price-p.cost) profit,
        COALESCE(s.updated_at,"") updated_at

    FROM products p
    LEFT JOIN stock s ON p.item_sku=s.item_sku

    ORDER BY product_name,item_name,color,size

    """,conn)

    conn.close()

    return df


def db_adjust_stock(item_sku,delta,movement,reason):

    conn=get_conn()

    cur=conn.execute(
        "SELECT qty FROM stock WHERE item_sku=?",
        (item_sku,)
    ).fetchone()

    old=cur[0] if cur else 0

    new=old+delta

    conn.execute("""
    INSERT OR REPLACE INTO stock
    VALUES(?,?,?)
    """,(item_sku,new,now_iso()))

    conn.execute("""
    INSERT INTO movements(ts,item_sku,movement,qty,reason)
    VALUES(?,?,?,?,?)
    """,(now_iso(),item_sku,movement,delta,reason))

    conn.commit()
    conn.close()

    st.session_state.local_dirty_stock=True


def db_get_movements(limit=500):

    conn=get_conn()

    df=pd.read_sql_query(
        "SELECT * FROM movements ORDER BY id DESC LIMIT ?",
        conn,
        params=(limit,)
    )

    conn.close()

    return df


# =====================================================
# GOOGLE SHEET SYNC
# =====================================================

def sheet_pull_master():

    r=requests.get(sheet_csv_url())

    df=pd.read_csv(StringIO(r.text))

    df.columns=[norm(x) for x in df.columns]

    out=pd.DataFrame()

    out["product_name"]=df["Product Name"]
    out["item_name"]=df["Item Name"]
    out["size"]=df["Size Name"]
    out["color"]=df["Warna Name"]
    out["vendor"]=df["Vendor Name"]
    out["base_sku"]=df["SKU"]
    out["item_sku"]=df["Item SKU"]
    out["stock"]=df["Stock"].fillna(0)
    out["cost"]=df["HPP"].apply(rp_to_number)
    out["price"]=df["Revenue"].apply(rp_to_number)

    return out


def db_upsert_from_master(df):

    conn=get_conn()

    for _,r in df.iterrows():

        conn.execute("""
        INSERT OR REPLACE INTO products
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """,(r.item_sku,r.base_sku,r.product_name,
             r.item_name,r.size,r.color,r.vendor,
             r.cost,r.price,now_iso()))

        conn.execute("""
        INSERT OR REPLACE INTO stock
        VALUES(?,?,?)
        """,(r.item_sku,int(r.stock),now_iso()))

    conn.commit()
    conn.close()


def gsheets_client():

    creds=Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    return gspread.authorize(creds)


def sheet_push_stock_from_db(df):

    gc=gsheets_client()

    ws=gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)

    header=ws.row_values(1)

    col_sku=header.index("Item SKU")+1
    col_stock=header.index("Stock")+1

    col_data=ws.col_values(col_sku)

    row_map={v:i+1 for i,v in enumerate(col_data)}

    cells=[]

    for _,r in df.iterrows():

        if r.item_sku in row_map:

            cells.append(
                gspread.Cell(row_map[r.item_sku],col_stock,int(r.qty))
            )

    ws.update_cells(cells)


# =====================================================
# SYNC WRAPPERS
# =====================================================

def pull():

    df=sheet_pull_master()

    db_upsert_from_master(df)


def push():

    df=db_get_inventory()

    sheet_push_stock_from_db(df)


# =====================================================
# UI START
# =====================================================

migrate_schema()

if "local_dirty_stock" not in st.session_state:
    st.session_state.local_dirty_stock=False


st.title("Ronary Inventory System")


# =====================================================
# PUSH PULL BUTTONS RESTORED
# =====================================================

c1,c2=st.columns(2)

with c1:
    if st.button("⬇️ Force PULL (Sheet → App)"):
        pull()
        st.success("PULL SUCCESS")

with c2:
    if st.button("⬆️ Force PUSH (App → Sheet)"):
        push()
        st.success("PUSH SUCCESS")


# =====================================================
# SIDEBAR
# =====================================================

menu=st.sidebar.radio("Menu",[
    "Dashboard",
    "Add Stock",
    "Remove Stock",
    "Movement History"
])

df=db_get_inventory()

low_thr=LOW_STOCK_THRESHOLD_DEFAULT


# =====================================================
# DASHBOARD
# =====================================================

if menu=="Dashboard":

    st.subheader("Dashboard")

    q=st.text_input("Search")

    vendor=st.selectbox(
        "Vendor",
        ["(All)"]+sorted(df.vendor.unique())
    )

    only_low=st.checkbox("Only low stock")

    view=df.copy()

    if q:
        view=view[view.product_name.str.contains(q,case=False)]

    if vendor!="(All)":
        view=view[view.vendor==vendor]

    if only_low:
        view=view[view.qty<=low_thr]

    # SUMMARY FOLLOW FILTER
    total_units=view.qty.sum()
    inventory_value=(view.price*view.qty).sum()
    profit=(view.profit*view.qty).sum()
    low_count=(view.qty<=low_thr).sum()

    a,b,c,d=st.columns(4)

    a.metric("Total Units",int(total_units))
    b.metric("Inventory Value",int(inventory_value))
    c.metric("Profit Potential",int(profit))
    d.metric("Low Stock",int(low_count))

    st.dataframe(view,use_container_width=True)


    # PROCUREMENT ENGINE
    st.subheader("Procurement Recommendation")

    analysis=view.copy()

    analysis["priority"]=(analysis.qty<=low_thr)*100-analysis.qty

    critical=analysis.sort_values("priority",ascending=False)

    st.dataframe(critical.head(20))


# =====================================================
# ADD STOCK
# =====================================================

elif menu=="Add Stock":

    sku=st.selectbox("SKU",df.item_sku)

    qty=st.number_input("Qty",1)

    if st.button("Add"):
        db_adjust_stock(sku,qty,"IN","RESTOCK")
        st.success("Added")


# =====================================================
# REMOVE STOCK
# =====================================================

elif menu=="Remove Stock":

    sku=st.selectbox("SKU",df.item_sku)

    qty=st.number_input("Qty",1)

    if st.button("Remove"):
        db_adjust_stock(sku,-qty,"OUT","SOLD")
        st.success("Removed")


# =====================================================
# MOVEMENTS
# =====================================================

elif menu=="Movement History":

    st.dataframe(db_get_movements())

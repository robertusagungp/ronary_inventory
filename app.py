import streamlit as st
import sqlite3
import pandas as pd
import requests
from io import StringIO
import datetime as dt
import re

# =========================
# CONFIG
# =========================
DB_FILE = "ronary_inventory.db"
GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_NAME = "Final Master Product"
CACHE_TTL = 300

# =========================
# DB
# =========================
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

# =========================
# SHEET
# =========================
def sheet_url():
    sheet = SHEET_NAME.replace(" ", "%20")
    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet}"

def norm(s: str) -> str:
    """Aggressive normalize for matching."""
    s = str(s)
    s = s.replace("\ufeff", "")        # BOM
    s = s.replace("\u00a0", " ")       # NBSP
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9]", "", s)    # keep alnum only
    return s

def rp_to_number(x):
    x = str(x)
    x = x.replace("\ufeff", "").replace("\u00a0", " ").strip()
    x = re.sub(r"(?i)rp", "", x)
    x = x.replace(".", "").replace(",", "")
    x = re.sub(r"[^0-9\-]", "", x)
    if x == "" or x == "-":
        return 0.0
    try:
        return float(x)
    except:
        return 0.0

def detect_header_row(df_raw: pd.DataFrame) -> int:
    """
    Find row index that looks like the header.
    We search for a row containing at least 2-3 signature headers.
    """
    signatures = {"productname", "itemsku", "sku", "stock", "hpp", "revenue"}
    best_idx = 0
    best_score = -1

    # search first 30 rows
    limit = min(len(df_raw), 30)
    for i in range(limit):
        row = df_raw.iloc[i].tolist()
        row_norm = {norm(x) for x in row}
        score = len(signatures.intersection(row_norm))
        if score > best_score:
            best_score = score
            best_idx = i

    # if score is too low, assume first row is header
    if best_score < 2:
        return 0
    return best_idx

@st.cache_data(ttl=CACHE_TTL)
def load_products_from_sheet():
    url = sheet_url()
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise Exception("Cannot access Google Sheet. Pastikan share: Anyone with link (Viewer).")

    # Read raw without assuming header
    df_raw = pd.read_csv(StringIO(r.text), header=None)

    header_idx = detect_header_row(df_raw)

    # Build proper df using detected header
    header = df_raw.iloc[header_idx].astype(str).tolist()
    df = df_raw.iloc[header_idx+1:].copy()
    df.columns = header
    df = df.reset_index(drop=True)

    # Clean column names (keep original + normalized map)
    df.columns = [str(c).replace("\ufeff","").replace("\u00a0"," ").strip() for c in df.columns]
    col_map = {norm(c): c for c in df.columns}

    # Expected columns in your sheet:
    # Product Name, Item Name, Size Name, Warna Name, Vendor Name, SKU, Item SKU, Stock, HPP, Revenue
    required_norm = {
        "productname": None,
        "itemname": None,
        "sizename": None,
        "warnaname": None,
        "vendorname": None,
        "sku": None,
        "itemsku": None,
        "stock": None,
        "hpp": None,
        "revenue": None,
    }

    missing = []
    for k in list(required_norm.keys()):
        if k not in col_map:
            missing.append(k)

    if missing:
        raise Exception(
            "Header tidak terbaca benar dari CSV export. "
            f"Missing(normalized)={missing}. Detected columns={list(df.columns)}. "
            f"HeaderRowIndex={header_idx}"
        )

    # Create normalized output
    out = pd.DataFrame()
    out["product_name"] = df[col_map["productname"]].astype(str).str.strip()
    out["item_name"] = df[col_map["itemname"]].astype(str).str.strip()
    out["size"] = df[col_map["sizename"]].astype(str).str.strip()
    out["color"] = df[col_map["warnaname"]].astype(str).str.strip()
    out["vendor"] = df[col_map["vendorname"]].astype(str).str.strip()

    out["base_sku"] = df[col_map["sku"]].astype(str).str.strip()
    out["item_sku"] = df[col_map["itemsku"]].astype(str).str.strip()

    out["stock"] = pd.to_numeric(df[col_map["stock"]], errors="coerce").fillna(0).astype(int)
    out["cost"] = df[col_map["hpp"]].apply(rp_to_number)
    out["price"] = df[col_map["revenue"]].apply(rp_to_number)

    out = out[out["item_sku"] != ""].copy()
    return out, {"detected_columns": list(df.columns), "header_row_index": header_idx, "preview_raw": df_raw.head(8)}

# =========================
# SYNC
# =========================
def sync_master():
    df, dbg = load_products_from_sheet()
    conn = get_conn()
    inserted = 0
    updated = 0

    for _, r in df.iterrows():
        sku = str(r["item_sku"]).strip()
        if not sku:
            continue

        exists = conn.execute("SELECT 1 FROM products WHERE item_sku=?", (sku,)).fetchone()

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
                r["base_sku"], r["product_name"], r["item_name"], r["size"], r["color"], r["vendor"],
                float(r["cost"]), float(r["price"]), sku
            ))
            updated += 1
        else:
            conn.execute("""
            INSERT INTO products(item_sku, base_sku, product_name, item_name, size, color, vendor, cost, price, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sku, r["base_sku"], r["product_name"], r["item_name"], r["size"], r["color"], r["vendor"],
                float(r["cost"]), float(r["price"]), dt.datetime.now().isoformat()
            ))
            conn.execute("""
            INSERT OR IGNORE INTO stock(item_sku, qty, updated_at)
            VALUES (?, ?, ?)
            """, (sku, int(r["stock"]), dt.datetime.now().isoformat()))
            inserted += 1

    conn.commit()
    conn.close()
    return inserted, updated, dbg

# =========================
# INVENTORY OPS
# =========================
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
        (p.price - p.cost) AS profit
    FROM products p
    JOIN stock s ON p.item_sku = s.item_sku
    ORDER BY p.product_name, p.color, p.size
    """, conn)
    conn.close()
    return df

def add_stock(item_sku, qty):
    conn = get_conn()
    conn.execute("UPDATE stock SET qty=qty+?, updated_at=? WHERE item_sku=?",
                 (qty, dt.datetime.now().isoformat(), item_sku))
    conn.execute("INSERT INTO movements(ts, item_sku, movement, qty, reason) VALUES (?, ?, 'IN', ?, 'MANUAL')",
                 (dt.datetime.now().isoformat(), item_sku, qty))
    conn.commit()
    conn.close()

def remove_stock(item_sku, qty):
    conn = get_conn()
    conn.execute("UPDATE stock SET qty=qty-?, updated_at=? WHERE item_sku=?",
                 (qty, dt.datetime.now().isoformat(), item_sku))
    conn.execute("INSERT INTO movements(ts, item_sku, movement, qty, reason) VALUES (?, ?, 'OUT', ?, 'SOLD')",
                 (dt.datetime.now().isoformat(), item_sku, qty))
    conn.commit()
    conn.close()

def get_movements():
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM movements ORDER BY id DESC", conn)
    conn.close()
    return df

# =========================
# STARTUP
# =========================
migrate_schema()

sync_ok = True
sync_status = ""
sync_debug = None

try:
    ins, upd, dbg = sync_master()
    sync_status = f"Auto Sync OK (Inserted {ins}, Updated {upd})"
    sync_debug = dbg
except Exception as e:
    sync_ok = False
    sync_status = f"Auto Sync FAILED: {e}"

# =========================
# UI
# =========================
st.set_page_config(layout="wide")
st.title("Ronary Inventory System")

if sync_ok:
    st.caption(sync_status)
else:
    st.error(sync_status)
    st.info("Aplikasi tetap jalan (tidak crash). Silakan buka debug di bawah untuk lihat header/kolom yang kebaca.")
    with st.expander("Debug (kolom & raw preview dari CSV export)"):
        if sync_debug:
            st.write("Detected columns:", sync_debug.get("detected_columns"))
            st.write("Header row index:", sync_debug.get("header_row_index"))
            st.write("Raw preview (top rows):")
            st.dataframe(sync_debug.get("preview_raw"))

menu = st.sidebar.selectbox("Menu", ["Dashboard", "Add Stock", "Remove Stock", "Movement History"])

df = get_inventory()

if menu == "Dashboard":
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Units", int(df["qty"].sum()) if not df.empty else 0)
    c2.metric("Inventory Value", int((df["price"] * df["qty"]).sum()) if not df.empty else 0)
    c3.metric("Profit Potential", int((df["profit"] * df["qty"]).sum()) if not df.empty else 0)
    st.dataframe(df, use_container_width=True)

elif menu == "Add Stock":
    if df.empty:
        st.warning("Data inventory kosong. Pastikan auto-sync berhasil.")
    else:
        sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty", min_value=1, value=1)
        if st.button("Add"):
            add_stock(sku, int(qty))
            st.rerun()

elif menu == "Remove Stock":
    if df.empty:
        st.warning("Data inventory kosong. Pastikan auto-sync berhasil.")
    else:
        sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty", min_value=1, value=1)
        if st.button("Remove"):
            remove_stock(sku, int(qty))
            st.rerun()

elif menu == "Movement History":
    st.dataframe(get_movements(), use_container_width=True)

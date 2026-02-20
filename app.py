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

AUTO_SYNC_SECONDS = 15   # ubah: 5 / 10 / 30 sesuai kebutuhan
REQUEST_TIMEOUT = 30

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
def sheet_url(cache_bust: bool = True) -> str:
    sheet = SHEET_NAME.replace(" ", "%20")
    base = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet}"
    if cache_bust:
        # cache buster so changes are fetched immediately
        base += f"&_ts={int(dt.datetime.now().timestamp())}"
    return base

def norm(s: str) -> str:
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
    signatures = {"productname", "itemsku", "sku", "stock", "hpp", "revenue"}
    best_idx = 0
    best_score = -1
    limit = min(len(df_raw), 30)
    for i in range(limit):
        row = df_raw.iloc[i].tolist()
        row_norm = {norm(x) for x in row}
        score = len(signatures.intersection(row_norm))
        if score > best_score:
            best_score = score
            best_idx = i
    if best_score < 2:
        return 0
    return best_idx

def fetch_csv_text() -> str:
    url = sheet_url(cache_bust=True)
    headers = {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise Exception("Cannot access Google Sheet. Pastikan share: Anyone with link (Viewer).")
    return r.text

def load_products_from_csv_text(csv_text: str):
    df_raw = pd.read_csv(StringIO(csv_text), header=None)
    header_idx = detect_header_row(df_raw)

    header = df_raw.iloc[header_idx].astype(str).tolist()
    df = df_raw.iloc[header_idx + 1:].copy()
    df.columns = header
    df = df.reset_index(drop=True)

    df.columns = [str(c).replace("\ufeff","").replace("\u00a0"," ").strip() for c in df.columns]
    col_map = {norm(c): c for c in df.columns}

    needed = ["productname","itemname","sizename","warnaname","vendorname","sku","itemsku","stock","hpp","revenue"]
    missing = [k for k in needed if k not in col_map]
    if missing:
        raise Exception(
            f"Header/kolom tidak terbaca benar. Missing(normalized)={missing}. "
            f"Detected={list(df.columns)} | header_row_index={header_idx}"
        )

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

    debug = {
        "detected_columns": list(df.columns),
        "header_row_index": header_idx,
        "raw_preview": df_raw.head(8)
    }
    return out, debug

# =========================
# SYNC
# =========================
def upsert_master(df: pd.DataFrame):
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
    return inserted, updated

def run_sync(force: bool = False):
    """
    Sync only if sheet content changed (hash), unless force=True.
    """
    csv_text = fetch_csv_text()
    content_hash = hash(csv_text)

    if "last_sheet_hash" not in st.session_state:
        st.session_state.last_sheet_hash = None

    changed = (st.session_state.last_sheet_hash != content_hash)

    if force or changed:
        df, dbg = load_products_from_csv_text(csv_text)
        ins, upd = upsert_master(df)
        st.session_state.last_sheet_hash = content_hash
        return True, f"SYNC OK (inserted={ins}, updated={upd})", dbg
    else:
        return True, "No changes detected (already up-to-date).", None

# =========================
# INVENTORY
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
# UI + AUTO REFRESH
# =========================
st.set_page_config(layout="wide")
st.title("Ronary Inventory System")

migrate_schema()

# Auto refresh (timer rerun)
try:
    st.autorefresh(interval=AUTO_SYNC_SECONDS * 1000, key="ronary_autorefresh")
except Exception:
    # fallback if older streamlit: do nothing (still can use Force Sync)
    pass

# Top control bar
left, mid, right = st.columns([2, 2, 2])

with left:
    st.write(f"Auto-sync interval: **{AUTO_SYNC_SECONDS}s**")

with mid:
    force = st.button("🔄 Force Sync Now")

with right:
    # status placeholder
    status_box = st.empty()

syncing_box = st.empty()

# Run sync on every rerun, but actually update DB only if content changed
syncing_box.info("Syncing...")
ok = True
msg = ""
dbg = None

try:
    ok, msg, dbg = run_sync(force=force)
except Exception as e:
    ok = False
    msg = f"SYNC FAILED: {e}"

syncing_box.empty()

if ok:
    status_box.success(msg)
else:
    status_box.error(msg)

if (not ok) or (dbg is not None and force):
    with st.expander("Debug (kolom & preview CSV export)"):
        if dbg:
            st.write("Detected columns:", dbg.get("detected_columns"))
            st.write("Header row index:", dbg.get("header_row_index"))
            st.write("Raw preview (top rows):")
            st.dataframe(dbg.get("raw_preview"))

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
        st.warning("Data inventory kosong. Pastikan sync berhasil.")
    else:
        sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty", min_value=1, value=1)
        if st.button("Add"):
            add_stock(sku, int(qty))
            st.rerun()

elif menu == "Remove Stock":
    if df.empty:
        st.warning("Data inventory kosong. Pastikan sync berhasil.")
    else:
        sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty", min_value=1, value=1)
        if st.button("Remove"):
            remove_stock(sku, int(qty))
            st.rerun()

elif menu == "Movement History":
    st.dataframe(get_movements(), use_container_width=True)

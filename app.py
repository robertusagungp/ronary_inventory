import streamlit as st
import pandas as pd
import sqlite3
import datetime as dt
import urllib.parse
import requests
from io import StringIO
import re

# Optional (for write-back to Google Sheets)
HAS_GSHEETS_WRITE = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSHEETS_WRITE = True
except Exception:
    HAS_GSHEETS_WRITE = False


# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Ronary Inventory System", layout="wide")

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_NAME = "Final Master Product"

AUTO_PULL_SECONDS = 15  # auto PULL sheet -> app
LOW_STOCK_THRESHOLD_DEFAULT = 2

# Expected columns in Google Sheet (exact)
COLS_SHEET = [
    "Product Name",
    "Item Name",
    "Size Name",
    "Warna Name",
    "Vendor Name",
    "SKU",
    "Item SKU",
    "Stock",
    "HPP",
    "Revenue",
    "Profit",
    "% Profit",
]

# =========================================================
# HELPERS
# =========================================================
def now_iso():
    return dt.datetime.now().isoformat(timespec="seconds")


def norm(s: str) -> str:
    s = str(s)
    s = s.replace("\ufeff", "")        # BOM
    s = s.replace("\u00a0", " ")       # NBSP
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def rp_to_number(x):
    x = str(x)
    x = x.replace("\ufeff", "").replace("\u00a0", " ").strip()
    x = re.sub(r"(?i)rp", "", x)
    x = x.replace(".", "")
    x = x.replace(",", "")
    x = re.sub(r"[^0-9\-]", "", x)
    if x in ("", "-"):
        return 0.0
    try:
        return float(x)
    except:
        return 0.0


def sheet_csv_url(cache_bust=True) -> str:
    # Using export CSV (works when sheet is shared "Anyone with link Viewer")
    encoded_sheet = urllib.parse.quote(SHEET_NAME)
    url = (
        f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export"
        f"?format=csv&sheet={encoded_sheet}"
    )
    if cache_bust:
        url += f"&_ts={int(dt.datetime.now().timestamp())}"
    return url


# =========================================================
# DATABASE
# =========================================================
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
        movement TEXT,          -- IN / OUT / SET
        qty INTEGER,
        reason TEXT
    )
    """)

    conn.commit()
    conn.close()


def db_upsert_from_master(df_master: pd.DataFrame):
    """
    Upsert product attributes + set stock to match master.
    This makes Sheet -> App authoritative for stock (PULL).
    """
    conn = get_conn()
    inserted_prod = 0
    updated_prod = 0
    stock_set = 0

    for _, r in df_master.iterrows():
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
                r["base_sku"], r["product_name"], r["item_name"], r["size"],
                r["color"], r["vendor"], float(r["cost"]), float(r["price"]), sku
            ))
            updated_prod += 1
        else:
            conn.execute("""
            INSERT INTO products(item_sku, base_sku, product_name, item_name, size, color, vendor, cost, price, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                sku, r["base_sku"], r["product_name"], r["item_name"], r["size"],
                r["color"], r["vendor"], float(r["cost"]), float(r["price"]), now_iso()
            ))
            inserted_prod += 1

        # Stock: SET to sheet value (authoritative on pull)
        new_qty = int(r["stock"])
        exists_stock = conn.execute("SELECT qty FROM stock WHERE item_sku=?", (sku,)).fetchone()

        if exists_stock:
            old_qty = int(exists_stock[0])
            if old_qty != new_qty:
                conn.execute("UPDATE stock SET qty=?, updated_at=? WHERE item_sku=?",
                             (new_qty, now_iso(), sku))
                stock_set += 1
        else:
            conn.execute("INSERT INTO stock(item_sku, qty, updated_at) VALUES (?,?,?)",
                         (sku, new_qty, now_iso()))
            stock_set += 1

    conn.commit()
    conn.close()
    return inserted_prod, updated_prod, stock_set


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
        COALESCE(s.qty, 0) AS qty,
        (p.price - p.cost) AS profit,
        COALESCE(s.updated_at, '') AS stock_updated_at
    FROM products p
    LEFT JOIN stock s ON p.item_sku = s.item_sku
    ORDER BY p.product_name, p.item_name, p.color, p.size
    """, conn)
    conn.close()
    return df


def db_adjust_stock(item_sku: str, delta: int, movement: str, reason: str):
    conn = get_conn()
    cur = conn.execute("SELECT qty FROM stock WHERE item_sku=?", (item_sku,)).fetchone()
    old = int(cur[0]) if cur else 0
    new = old + int(delta)

    if cur:
        conn.execute("UPDATE stock SET qty=?, updated_at=? WHERE item_sku=?",
                     (new, now_iso(), item_sku))
    else:
        conn.execute("INSERT INTO stock(item_sku, qty, updated_at) VALUES (?,?,?)",
                     (item_sku, new, now_iso()))

    conn.execute("INSERT INTO movements(ts, item_sku, movement, qty, reason) VALUES (?,?,?,?,?)",
                 (now_iso(), item_sku, movement, int(delta), reason))

    conn.commit()
    conn.close()

    # mark local dirty (for optional auto-push)
    st.session_state["local_dirty_stock"] = True


def db_get_movements(limit=500):
    conn = get_conn()
    df = pd.read_sql_query("""
    SELECT id, ts, item_sku, movement, qty, reason
    FROM movements
    ORDER BY id DESC
    LIMIT ?
    """, conn, params=(limit,))
    conn.close()
    return df


# =========================================================
# GOOGLE SHEET READ (CSV export)
# =========================================================
def sheet_pull_master():
    """
    Read master from Google Sheet (public export CSV).
    Returns normalized df with:
      item_sku, base_sku, product_name, item_name, size, color, vendor, stock, cost, price
    """
    url = sheet_csv_url(cache_bust=True)
    r = requests.get(url, timeout=30, headers={"Cache-Control": "no-cache", "Pragma": "no-cache"})
    if r.status_code != 200:
        raise Exception("Tidak bisa akses Google Sheet. Pastikan share: Anyone with link (Viewer).")

    df = pd.read_csv(StringIO(r.text))
    df.columns = [norm(c) for c in df.columns]

    # Ensure required exist (use exact names)
    required = [
        "Product Name", "Item Name", "Size Name", "Warna Name", "Vendor Name",
        "SKU", "Item SKU", "Stock", "HPP", "Revenue"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise Exception(f"Kolom sheet tidak sesuai. Missing={missing}. Detected={list(df.columns)}")

    out = pd.DataFrame()
    out["product_name"] = df["Product Name"].astype(str).str.strip()
    out["item_name"] = df["Item Name"].astype(str).str.strip()
    out["size"] = df["Size Name"].astype(str).str.strip()
    out["color"] = df["Warna Name"].astype(str).str.strip()
    out["vendor"] = df["Vendor Name"].astype(str).str.strip()
    out["base_sku"] = df["SKU"].astype(str).str.strip()
    out["item_sku"] = df["Item SKU"].astype(str).str.strip()

    out["stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0).astype(int)

    # cost/price can be numeric or Rp string
    out["cost"] = df["HPP"].apply(rp_to_number)
    out["price"] = df["Revenue"].apply(rp_to_number)

    out = out[out["item_sku"] != ""].copy()
    return out


# =========================================================
# GOOGLE SHEET WRITE BACK (requires Service Account in st.secrets)
# =========================================================
def gsheets_client():

    if not HAS_GSHEETS_WRITE:
        raise Exception("gspread tidak tersedia")

    if "gcp_service_account" not in st.secrets:
        raise Exception("Service Account belum diset")

    info = dict(st.secrets["gcp_service_account"])

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)

    return gspread.authorize(creds)



def sheet_get_worksheet_and_header():
    gc = gsheets_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.worksheet(SHEET_NAME)
    header = ws.row_values(1)
    header = [norm(h) for h in header]
    return ws, header


def sheet_push_stock_from_db(df_inventory: pd.DataFrame):

    ws, header = sheet_get_worksheet_and_header()

    try:
        col_itemsku = header.index("Item SKU") + 1
        col_stock = header.index("Stock") + 1
    except ValueError:
        raise Exception(f"Header sheet tidak cocok. Header terbaca: {header}")

    # map item sku -> row
    itemsku_col = ws.col_values(col_itemsku)

    row_map = {}
    for i, v in enumerate(itemsku_col[1:], start=2):
        v = str(v).strip()
        if v:
            row_map[v] = i

    updates = []

    for _, r in df_inventory.iterrows():

        sku = str(r["item_sku"]).strip()

        if sku not in row_map:
            continue

        row = row_map[sku]

        new_stock = int(r["qty"])

        cell = gspread.Cell(row, col_stock, new_stock)

        updates.append(cell)

    if updates:
        ws.update_cells(updates)

    return len(updates)



# =========================================================
# SYNC CONTROLLERS
# =========================================================
def do_pull_sheet_to_app():
    df_master = sheet_pull_master()
    ins, upd, stock_set = db_upsert_from_master(df_master)
    return ins, upd, stock_set


def do_push_app_to_sheet_stock():
    df_inv = db_get_inventory()
    pushed = sheet_push_stock_from_db(df_inv)
    return pushed


# =========================================================
# UI
# =========================================================
migrate_schema()

# Auto refresh loop (does not block)
try:
    st.autorefresh(interval=AUTO_PULL_SECONDS * 1000, key="ronary_autopull")
except Exception:
    pass

# Session state defaults
st.session_state.setdefault("sync_ok", True)
st.session_state.setdefault("sync_msg", "Not synced yet")
st.session_state.setdefault("local_dirty_stock", False)
st.session_state.setdefault("auto_push_after_local", False)
st.session_state.setdefault("low_stock_threshold", LOW_STOCK_THRESHOLD_DEFAULT)

st.title("Ronary Inventory System")

# Top controls
c1, c2, c3, c4 = st.columns([2, 2, 2, 2])

with c1:
    st.write(f"Auto PULL interval: **{AUTO_PULL_SECONDS}s**")

with c2:
    force_pull = st.button("⬇️ Force PULL (Sheet → App)")

with c3:
    force_push = st.button("⬆️ Push Stock (App → Sheet)")

with c4:
    st.session_state["auto_push_after_local"] = st.toggle(
        "Auto PUSH after local change",
        value=st.session_state["auto_push_after_local"],
        help="Jika ON, setelah Add/Remove stock, app akan otomatis push stock ke sheet (butuh Service Account)."
    )

syncing = st.empty()
status_box = st.empty()

# Decide when to pull
# IMPORTANT: jangan auto-pull kalau ada perubahan lokal (local_dirty_stock),
# karena itu akan overwrite stok lokal dari sheet.
should_pull = False

if force_pull:
    should_pull = True
elif not st.session_state.get("local_dirty_stock", False):
    should_pull = True

# PULL (Sheet -> App)
if should_pull:
    try:
        syncing.info("Syncing... (PULL from Google Sheet)")
        ins, upd, stock_set = do_pull_sheet_to_app()
        st.session_state["sync_ok"] = True
        st.session_state["sync_msg"] = f"PULL OK | products inserted={ins}, updated={upd}, stock_set={stock_set}"
    except Exception as e:
        st.session_state["sync_ok"] = False
        st.session_state["sync_msg"] = f"PULL FAILED: {e}"
    finally:
        syncing.empty()

# AUTO PUSH if local changed
if st.session_state["auto_push_after_local"] and st.session_state["local_dirty_stock"]:
    try:
        syncing.info("Syncing... (AUTO PUSH stock to Google Sheet)")
        pushed = do_push_app_to_sheet_stock()
        st.session_state["sync_ok"] = True
        st.session_state["sync_msg"] = f"AUTO PUSH OK | rows_pushed={pushed}"
        st.session_state["local_dirty_stock"] = False
    except Exception as e:
        st.session_state["sync_ok"] = False
        st.session_state["sync_msg"] = f"AUTO PUSH FAILED: {e}"
    finally:
        syncing.empty()

# FORCE PUSH button
if force_push:
    try:
        syncing.info("Syncing... (FORCE PUSH stock to Google Sheet)")
        pushed = do_push_app_to_sheet_stock()
        st.session_state["sync_ok"] = True
        st.session_state["sync_msg"] = f"FORCE PUSH OK | rows_pushed={pushed}"
        st.session_state["local_dirty_stock"] = False
    except Exception as e:
        import traceback
        st.session_state["sync_ok"] = False
        st.session_state["sync_msg"] = f"FORCE PUSH FAILED: {str(e)}"
        st.error(traceback.format_exc())

    finally:
        syncing.empty()

# Status indicator
if st.session_state["sync_ok"]:
    status_box.success(st.session_state["sync_msg"])
else:
    status_box.error(st.session_state["sync_msg"])
    st.info("Catatan: PUSH (App → Sheet) butuh Service Account di Streamlit secrets. PULL hanya butuh sheet share Viewer.")

# Sidebar navigation
menu = st.sidebar.radio("Menu", ["Dashboard", "Add Stock", "Remove Stock", "Movement History", "Settings"])

df = db_get_inventory()

# Settings
if menu == "Settings":
    st.subheader("Settings")
    st.session_state["low_stock_threshold"] = st.number_input(
        "Low stock threshold",
        min_value=0,
        max_value=999,
        value=int(st.session_state["low_stock_threshold"])
    )

    st.markdown("### Google Sheet writeback (App → Sheet)")
    st.write("Untuk bisa PUSH, kamu perlu Service Account + share Google Sheet ke email service account sebagai Editor.")
    st.code(
        """# .streamlit/secrets.toml (Streamlit Cloud)
[gcp_service_account]
type = "service_account"
project_id = "..."
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
client_email = "...@....iam.gserviceaccount.com"
client_id = "..."
token_uri = "https://oauth2.googleapis.com/token"
""",
        language="toml"
    )
    st.write("Pastikan requirements.txt ada: gspread, google-auth")
    st.stop()

# Dashboard
if menu == "Dashboard":
    st.subheader("Dashboard")

    a, b, c, d = st.columns(4)
    a.metric("Total Units", total_units)
    b.metric("Inventory Value (Revenue x Qty)", inventory_value)
    c.metric("Profit Potential ((Revenue-HPP) x Qty)", profit_potential)
    d.metric(f"Low Stock (≤ {low_thr})", low_count)

    # Filters
    st.markdown("### Inventory")
    f1, f2, f3 = st.columns([2, 1, 1])
    with f1:
        q = st.text_input("Search (product/item/color/size/vendor/base sku/item sku)")
    with f2:
        vendor_list = ["(All)"] + sorted([x for x in df["vendor"].dropna().unique().tolist()])
        vendor_sel = st.selectbox("Vendor", vendor_list)
    with f3:
        only_low = st.checkbox("Only low stock")

    view = df.copy()
    if q:
        ql = q.lower().strip()
        mask = (
            view["product_name"].astype(str).str.lower().str.contains(ql, na=False) |
            view["item_name"].astype(str).str.lower().str.contains(ql, na=False) |
            view["color"].astype(str).str.lower().str.contains(ql, na=False) |
            view["size"].astype(str).str.lower().str.contains(ql, na=False) |
            view["vendor"].astype(str).str.lower().str.contains(ql, na=False) |
            view["base_sku"].astype(str).str.lower().str.contains(ql, na=False) |
            view["item_sku"].astype(str).str.lower().str.contains(ql, na=False)
        )
        view = view[mask]

    if vendor_sel != "(All)":
        view = view[view["vendor"] == vendor_sel]

    if only_low:
        view = view[view["qty"] <= low_thr]

    # nice ordering
    show_cols = [
        "base_sku", "product_name", "item_name", "size", "color", "vendor",
        "cost", "price", "qty", "profit", "item_sku", "stock_updated_at"
    ]

    # =========================================================
    # SUMMARY BERDASARKAN FILTER (FIX)
    # =========================================================
    
    summary_df = view.copy()
    
    total_units = int(summary_df["qty"].sum()) if not summary_df.empty else 0
    inventory_value = int((summary_df["price"] * summary_df["qty"]).sum()) if not summary_df.empty else 0
    profit_potential = int(((summary_df["price"] - summary_df["cost"]) * summary_df["qty"]).sum()) if not summary_df.empty else 0
    low_count = int((summary_df["qty"] <= low_thr).sum()) if not summary_df.empty else 0
    
    a, b, c, d = st.columns(4)
    a.metric("Total Units", total_units)
    b.metric("Inventory Value (Revenue x Qty)", inventory_value)
    c.metric("Profit Potential ((Revenue-HPP) x Qty)", profit_potential)
    d.metric(f"Low Stock (≤ {low_thr})", low_count)

    st.dataframe(view[show_cols], use_container_width=True)

# Add stock
elif menu == "Add Stock":
    st.subheader("Add Stock (Local DB)")
    if df.empty:
        st.warning("Inventory kosong. Pastikan PULL berhasil dari Google Sheet.")
    else:
        sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty to add", min_value=1, max_value=100000, value=1)
        reason = st.text_input("Reason (optional)", value="RESTOCK")
        if st.button("✅ Add"):
            db_adjust_stock(sku, int(qty), movement="IN", reason=reason.strip() or "IN")
            st.session_state["sync_msg"] = "LOCAL CHANGE OK | waiting push or auto-push"
            st.success("Stock updated locally.")
            st.rerun()

# Remove stock
elif menu == "Remove Stock":
    st.subheader("Remove Stock (Local DB)")
    if df.empty:
        st.warning("Inventory kosong. Pastikan PULL berhasil dari Google Sheet.")
    else:
        sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        cur_qty = int(df.loc[df["item_sku"] == sku, "qty"].iloc[0]) if sku else 0
        st.caption(f"Current qty: {cur_qty}")

        qty = st.number_input("Qty to remove", min_value=1, max_value=100000, value=1)
        reason = st.text_input("Reason (optional)", value="SOLD")

        if st.button("✅ Remove"):
            # allow negative; but usually you want to prevent below zero
            if cur_qty - int(qty) < 0:
                st.error("Tidak bisa remove melebihi stok saat ini (qty jadi negatif).")
            else:
                db_adjust_stock(sku, -int(qty), movement="OUT", reason=reason.strip() or "OUT")
                st.session_state["sync_msg"] = "LOCAL CHANGE OK | waiting push or auto-push"
                st.success("Stock updated locally.")
                st.rerun()

# Movement history
elif menu == "Movement History":
    st.subheader("Movement History")
    limit = st.slider("Rows to show", min_value=50, max_value=2000, value=500, step=50)
    mv = db_get_movements(limit=limit)
    st.dataframe(mv, use_container_width=True)

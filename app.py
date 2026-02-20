import streamlit as st
import sqlite3
import pandas as pd
import requests
from io import StringIO
import datetime as dt
import re
from typing import Dict, Optional, Tuple

# =====================================================
# CONFIG
# =====================================================

DB_FILE = "ronary_inventory.db"

GOOGLE_SHEET_ID = "1r4Gmtlfh7WPwprRuKTY7K8FbUUC7yboZeb83BjEIDT4"
SHEET_PRODUCTS = "Final Master Product"
SHEET_PRICE = "Master Price"

CACHE_TTL_SECONDS = 300  # 5 minutes


# =====================================================
# DB HELPERS
# =====================================================

def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def get_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [r[1] for r in rows]  # name
    except Exception:
        return []


def ensure_table_schema() -> None:
    """
    Auto-migrate schema.
    If existing tables are incompatible (old columns), we rebuild safely.
    """
    conn = get_conn()

    # --- desired schemas
    desired_products_cols = [
        "item_sku", "base_sku", "product_name", "item_name",
        "size", "color", "vendor", "cost", "price", "created_at"
    ]
    desired_stock_cols = ["item_sku", "qty", "updated_at"]
    desired_movements_cols = ["id", "ts", "item_sku", "movement", "qty", "reason"]

    # PRODUCTS
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
    else:
        cols = [c.lower() for c in get_columns(conn, "products")]
        # if table looks totally different, rebuild
        if "item_sku" not in cols:
            # backup then rebuild
            conn.execute("ALTER TABLE products RENAME TO products_backup")
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
            # best-effort migrate from backup if it had 'sku'
            backup_cols = [c.lower() for c in get_columns(conn, "products_backup")]
            if "sku" in backup_cols:
                conn.execute("""
                INSERT OR IGNORE INTO products(item_sku, base_sku, product_name, created_at)
                SELECT sku, sku, COALESCE(product_name,''), COALESCE(created_at,'')
                FROM products_backup
                """)
        else:
            # add missing columns
            for col in desired_products_cols:
                if col.lower() not in cols:
                    # choose type
                    col_type = "REAL" if col in ("cost", "price") else "TEXT"
                    if col == "created_at":
                        col_type = "TEXT"
                    conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")

    # STOCK
    if not table_exists(conn, "stock"):
        conn.execute("""
        CREATE TABLE stock (
            item_sku TEXT PRIMARY KEY,
            qty INTEGER,
            updated_at TEXT
        )
        """)
    else:
        cols = [c.lower() for c in get_columns(conn, "stock")]
        # if old stock schema (e.g., sku, location, low_stock_threshold), rebuild
        if "item_sku" not in cols or "qty" not in cols:
            conn.execute("ALTER TABLE stock RENAME TO stock_backup")
            conn.execute("""
            CREATE TABLE stock (
                item_sku TEXT PRIMARY KEY,
                qty INTEGER,
                updated_at TEXT
            )
            """)
            backup_cols = [c.lower() for c in get_columns(conn, "stock_backup")]
            # best-effort migrate
            if "item_sku" in backup_cols and "qty" in backup_cols:
                conn.execute("""
                INSERT OR IGNORE INTO stock(item_sku, qty, updated_at)
                SELECT item_sku, COALESCE(qty,0), COALESCE(updated_at,'')
                FROM stock_backup
                """)
            elif "sku" in backup_cols and "qty" in backup_cols:
                conn.execute("""
                INSERT OR IGNORE INTO stock(item_sku, qty, updated_at)
                SELECT sku, COALESCE(qty,0), COALESCE(updated_at,'')
                FROM stock_backup
                """)
            elif "sku" in backup_cols and "location" in backup_cols and "qty" in backup_cols:
                # sum across locations
                conn.execute("""
                INSERT OR IGNORE INTO stock(item_sku, qty, updated_at)
                SELECT sku, SUM(COALESCE(qty,0)), COALESCE(MAX(updated_at),'')
                FROM stock_backup
                GROUP BY sku
                """)
        else:
            # add missing updated_at if needed
            for col in desired_stock_cols:
                if col.lower() not in cols:
                    col_type = "INTEGER" if col == "qty" else "TEXT"
                    conn.execute(f"ALTER TABLE stock ADD COLUMN {col} {col_type}")

    # MOVEMENTS
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
    else:
        cols = [c.lower() for c in get_columns(conn, "movements")]
        # add missing cols only (no rebuild)
        for col in desired_movements_cols:
            if col.lower() not in cols:
                if col == "id":
                    continue
                col_type = "INTEGER" if col == "qty" else "TEXT"
                conn.execute(f"ALTER TABLE movements ADD COLUMN {col} {col_type}")

    conn.commit()
    conn.close()


# =====================================================
# GOOGLE SHEETS (ROBUST)
# =====================================================

def sheet_url(sheet: str) -> str:
    sheet = sheet.replace(" ", "%20")
    return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={sheet}"


def normalize_col(col: str) -> str:
    """
    Aggressive normalization:
    - strip spaces + NBSP + BOM
    - lower
    - keep only a-z0-9
    """
    if col is None:
        return ""
    s = str(col)
    s = s.replace("\ufeff", "")        # BOM
    s = s.replace("\u00a0", " ")       # NBSP
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9]", "", s)    # keep alnum only
    return s


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_sheet_raw(sheet: str) -> pd.DataFrame:
    url = sheet_url(sheet)
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise Exception(f"Cannot load sheet '{sheet}' (HTTP {r.status_code}). Make sure sharing is 'Anyone with the link - Viewer'.")
    df = pd.read_csv(StringIO(r.text))
    return df


def build_col_index(df: pd.DataFrame) -> Dict[str, str]:
    """
    Returns map: normalized_name -> original_name
    """
    idx = {}
    for c in df.columns:
        idx[normalize_col(c)] = c
    return idx


def clean_rp_to_number(x) -> float:
    s = str(x) if x is not None else ""
    s = s.replace("\ufeff", "").replace("\u00a0", " ")
    s = s.strip()
    s = re.sub(r"[Rr][Pp]\s?", "", s)
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"[^0-9\-]", "", s)
    if s == "" or s == "-":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_master() -> Tuple[pd.DataFrame, Dict[str, list[str]]]:
    """
    Load + normalize master products & price.
    Return (df_master, debug_info)
    """
    df_p = load_sheet_raw(SHEET_PRODUCTS)
    df_pr = load_sheet_raw(SHEET_PRICE)

    # Build column indices
    idx_p = build_col_index(df_p)
    idx_pr = build_col_index(df_pr)

    debug = {
        "products_columns": list(df_p.columns),
        "price_columns": list(df_pr.columns),
    }

    # Find needed columns in Products
    # Expected headers in your simplified sheet:
    # Product Name, Item Name, Size Name, Warna Name, Vendor Name, SKU, Item SKU, Stock
    col_item_sku = idx_p.get("itemsku") or idx_p.get("itemkode")  # fallback
    col_base_sku = idx_p.get("sku")
    col_stock = idx_p.get("stock")

    if not col_item_sku:
        # fail-safe: try "item sku" variations that normalize differently
        # (this should rarely happen because normalize_col("Item SKU") -> "itemsku")
        raise Exception(
            "Item SKU column not found from sheet export. "
            f"Detected columns: {list(df_p.columns)}"
        )

    # Map optional columns
    col_product_name = idx_p.get("productname")
    col_item_name = idx_p.get("itemname")
    col_size = idx_p.get("sizename")
    col_color = idx_p.get("warnaname")
    col_vendor = idx_p.get("vendorname")

    # Create normalized products df
    out = pd.DataFrame()
    out["item_sku"] = df_p[col_item_sku].astype(str).str.strip()
    out["base_sku"] = df_p[col_base_sku].astype(str).str.strip() if col_base_sku else ""
    out["product_name"] = df_p[col_product_name].astype(str).str.strip() if col_product_name else ""
    out["item_name"] = df_p[col_item_name].astype(str).str.strip() if col_item_name else ""
    out["size"] = df_p[col_size].astype(str).str.strip() if col_size else ""
    out["color"] = df_p[col_color].astype(str).str.strip() if col_color else ""
    out["vendor"] = df_p[col_vendor].astype(str).str.strip() if col_vendor else ""

    if col_stock:
        out["stock"] = pd.to_numeric(df_p[col_stock], errors="coerce").fillna(0).astype(int)
    else:
        out["stock"] = 0

    # Price sheet expected: SKU, HPP, Revenue, ...
    col_pr_base_sku = idx_pr.get("sku")
    col_cost = idx_pr.get("hpp")
    col_price = idx_pr.get("revenue")

    if not col_pr_base_sku:
        raise Exception(f"Master Price sheet missing 'SKU'. Detected columns: {list(df_pr.columns)}")

    pr = pd.DataFrame()
    pr["base_sku"] = df_pr[col_pr_base_sku].astype(str).str.strip()
    pr["cost"] = df_pr[col_cost].apply(clean_rp_to_number) if col_cost else 0.0
    pr["price"] = df_pr[col_price].apply(clean_rp_to_number) if col_price else 0.0

    df = out.merge(pr, on="base_sku", how="left")
    df["cost"] = df["cost"].fillna(0.0)
    df["price"] = df["price"].fillna(0.0)

    # drop blank item_sku rows
    df = df[df["item_sku"].astype(str).str.strip() != ""].copy()

    return df, debug


# =====================================================
# SYNC (NON-CRASHING)
# =====================================================

def upsert_master_to_db(df: pd.DataFrame) -> Tuple[int, int]:
    conn = get_conn()
    inserted = 0
    updated = 0

    for _, r in df.iterrows():
        item_sku = str(r.get("item_sku", "")).strip()
        if not item_sku:
            continue

        exists = conn.execute(
            "SELECT 1 FROM products WHERE item_sku=?",
            (item_sku,),
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
                str(r.get("base_sku", "")).strip(),
                str(r.get("product_name", "")).strip(),
                str(r.get("item_name", "")).strip(),
                str(r.get("size", "")).strip(),
                str(r.get("color", "")).strip(),
                str(r.get("vendor", "")).strip(),
                float(r.get("cost", 0.0) or 0.0),
                float(r.get("price", 0.0) or 0.0),
                item_sku
            ))
            updated += 1
        else:
            conn.execute("""
                INSERT INTO products(item_sku, base_sku, product_name, item_name, size, color, vendor, cost, price, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item_sku,
                str(r.get("base_sku", "")).strip(),
                str(r.get("product_name", "")).strip(),
                str(r.get("item_name", "")).strip(),
                str(r.get("size", "")).strip(),
                str(r.get("color", "")).strip(),
                str(r.get("vendor", "")).strip(),
                float(r.get("cost", 0.0) or 0.0),
                float(r.get("price", 0.0) or 0.0),
                dt.datetime.now().isoformat()
            ))

            conn.execute("""
                INSERT OR IGNORE INTO stock(item_sku, qty, updated_at)
                VALUES (?, ?, ?)
            """, (
                item_sku,
                int(r.get("stock", 0) or 0),
                dt.datetime.now().isoformat()
            ))
            inserted += 1

    conn.commit()
    conn.close()
    return inserted, updated


def sync_on_startup() -> Tuple[bool, str, Optional[Dict[str, list[str]]]]:
    """
    Never crash the app. If sync fails, return error string and keep app running.
    """
    try:
        df, debug = load_master()
        ins, upd = upsert_master_to_db(df)
        return True, f"Auto-sync OK (inserted {ins}, updated {upd}).", debug
    except Exception as e:
        return False, f"Auto-sync FAILED: {e}", None


# =====================================================
# INVENTORY OPS
# =====================================================

def add_stock(item_sku: str, qty: int, reason: str = "MANUAL") -> None:
    conn = get_conn()
    conn.execute(
        "UPDATE stock SET qty=COALESCE(qty,0)+?, updated_at=? WHERE item_sku=?",
        (qty, dt.datetime.now().isoformat(), item_sku),
    )
    conn.execute(
        "INSERT INTO movements(ts, item_sku, movement, qty, reason) VALUES (?, ?, 'IN', ?, ?)",
        (dt.datetime.now().isoformat(), item_sku, qty, reason),
    )
    conn.commit()
    conn.close()


def remove_stock(item_sku: str, qty: int, reason: str = "SOLD") -> None:
    conn = get_conn()
    conn.execute(
        "UPDATE stock SET qty=COALESCE(qty,0)-?, updated_at=? WHERE item_sku=?",
        (qty, dt.datetime.now().isoformat(), item_sku),
    )
    conn.execute(
        "INSERT INTO movements(ts, item_sku, movement, qty, reason) VALUES (?, ?, 'OUT', ?, ?)",
        (dt.datetime.now().isoformat(), item_sku, qty, reason),
    )
    conn.commit()
    conn.close()


def get_inventory() -> pd.DataFrame:
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
            (p.price - p.cost) AS profit,
            s.qty
        FROM products p
        JOIN stock s ON p.item_sku = s.item_sku
        ORDER BY p.product_name, p.color, p.size
    """, conn)
    conn.close()
    return df


def get_movements() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT id, ts, item_sku, movement, qty, reason
        FROM movements
        ORDER BY id DESC
    """, conn)
    conn.close()
    return df


# =====================================================
# STARTUP (MIGRATE + AUTO-SYNC)
# =====================================================

ensure_table_schema()
sync_ok, sync_msg, sync_debug = sync_on_startup()


# =====================================================
# UI
# =====================================================

st.set_page_config(layout="wide")
st.title("Ronary Inventory System")

# Show sync status (non-blocking)
if sync_ok:
    st.caption(sync_msg)
else:
    st.warning(sync_msg)
    with st.expander("Debug: columns detected from Google Sheets export"):
        if sync_debug is None:
            st.write("No debug payload (sync failed before collecting).")
        else:
            st.write("Final Master Product columns:")
            st.write(sync_debug.get("products_columns", []))
            st.write("Master Price columns:")
            st.write(sync_debug.get("price_columns", []))
    st.info("Tip: Pastikan Google Sheet setting: Share → Anyone with link → Viewer. "
            "Dan pastikan header row ada di baris pertama (row 1).")

menu = st.sidebar.selectbox("Menu", ["Dashboard", "Add Stock", "Remove Stock", "Movement History"])

df = get_inventory()

if menu == "Dashboard":
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Units", int(df["qty"].sum()) if not df.empty else 0)
    c2.metric("Total Value", int((df["price"] * df["qty"]).sum()) if not df.empty else 0)
    c3.metric("Total Profit Potential", int((df["profit"] * df["qty"]).sum()) if not df.empty else 0)
    st.dataframe(df, use_container_width=True)

elif menu == "Add Stock":
    if df.empty:
        st.info("Inventory masih kosong. Auto-sync harus berhasil dulu.")
    else:
        item_sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty", min_value=1, value=1)
        reason = st.text_input("Reason", value="MANUAL")
        if st.button("Add"):
            add_stock(item_sku, int(qty), reason=reason.strip() or "MANUAL")
            st.rerun()

elif menu == "Remove Stock":
    if df.empty:
        st.info("Inventory masih kosong. Auto-sync harus berhasil dulu.")
    else:
        item_sku = st.selectbox("Item SKU", df["item_sku"].tolist())
        qty = st.number_input("Qty", min_value=1, value=1)
        reason = st.text_input("Reason", value="SOLD")
        if st.button("Remove"):
            remove_stock(item_sku, int(qty), reason=reason.strip() or "SOLD")
            st.rerun()

elif menu == "Movement History":
    st.dataframe(get_movements(), use_container_width=True)

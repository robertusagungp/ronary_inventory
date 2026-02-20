
---

## 5) `app.py` (FULL APP)

```python
import sqlite3
import datetime as dt
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

# =========================
# CONFIG
# =========================
APP_TITLE = "Ronary Inventory System"
DB_FILE = "ronary_inventory.db"

DEFAULT_LOCATIONS = ["STUDIO", "GUDANG", "TOKOPEDIA", "SHOPEE"]
DEFAULT_REASONS_IN = ["PRODUCTION", "RESTOCK", "RETURN_IN", "ADJUST_IN"]
DEFAULT_REASONS_OUT = ["SOLD", "DAMAGED", "SAMPLE", "RETURN_OUT", "ADJUST_OUT"]

# =========================
# DB HELPERS
# =========================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Products master
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        sku TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT,
        color TEXT,
        size TEXT,
        cost REAL DEFAULT 0,
        price REAL DEFAULT 0,
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL
    );
    """)

    # Locations
    cur.execute("""
    CREATE TABLE IF NOT EXISTS locations (
        location TEXT PRIMARY KEY
    );
    """)

    # Stock per SKU per location (composite PK)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stock (
        sku TEXT NOT NULL,
        location TEXT NOT NULL,
        qty INTEGER NOT NULL DEFAULT 0,
        low_stock_threshold INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (sku, location),
        FOREIGN KEY (sku) REFERENCES products(sku) ON DELETE CASCADE,
        FOREIGN KEY (location) REFERENCES locations(location) ON DELETE CASCADE
    );
    """)

    # Movements log (append-only)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        sku TEXT NOT NULL,
        movement_type TEXT NOT NULL,           -- IN / OUT / ADJUST / TRANSFER
        location_from TEXT,
        location_to TEXT,
        qty INTEGER NOT NULL,                  -- positive integer
        reason TEXT,
        notes TEXT,
        FOREIGN KEY (sku) REFERENCES products(sku) ON DELETE CASCADE
    );
    """)

    conn.commit()

    # Seed default locations if not exist
    for loc in DEFAULT_LOCATIONS:
        cur.execute("INSERT OR IGNORE INTO locations(location) VALUES (?);", (loc,))
    conn.commit()
    conn.close()

def now_iso() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")

def run_query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

def exec_sql(sql: str, params: tuple = ()) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    conn.close()

def exec_many(sql: str, rows: list[tuple]) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    conn.close()

# =========================
# BUSINESS LOGIC
# =========================
def normalize_sku(sku: str) -> str:
    return (sku or "").strip().upper().replace(" ", "-")

def sku_exists(sku: str) -> bool:
    df = run_query_df("SELECT sku FROM products WHERE sku = ?;", (sku,))
    return len(df) > 0

def location_exists(loc: str) -> bool:
    df = run_query_df("SELECT location FROM locations WHERE location = ?;", (loc,))
    return len(df) > 0

def ensure_stock_row(sku: str, location: str) -> None:
    # create stock row if absent
    exec_sql("""
        INSERT OR IGNORE INTO stock(sku, location, qty, low_stock_threshold, updated_at)
        VALUES (?, ?, 0, 0, ?);
    """, (sku, location, now_iso()))

def get_stock_qty(sku: str, location: str) -> int:
    df = run_query_df("""
        SELECT qty FROM stock WHERE sku = ? AND location = ?;
    """, (sku, location))
    if df.empty:
        return 0
    return int(df.iloc[0]["qty"])

def set_low_stock_threshold(sku: str, location: str, threshold: int) -> None:
    ensure_stock_row(sku, location)
    exec_sql("""
        UPDATE stock SET low_stock_threshold = ?, updated_at = ?
        WHERE sku = ? AND location = ?;
    """, (int(threshold), now_iso(), sku, location))

def add_product(
    sku: str,
    product_name: str,
    category: str,
    color: str,
    size: str,
    cost: float,
    price: float,
    is_active: bool
) -> None:
    created_at = now_iso()
    exec_sql("""
        INSERT INTO products(sku, product_name, category, color, size, cost, price, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        sku,
        product_name.strip(),
        (category or "").strip(),
        (color or "").strip(),
        (size or "").strip(),
        float(cost or 0),
        float(price or 0),
        1 if is_active else 0,
        created_at
    ))

    # Ensure stock rows for default locations
    for loc in get_locations():
        ensure_stock_row(sku, loc)

def update_product(
    sku: str,
    product_name: str,
    category: str,
    color: str,
    size: str,
    cost: float,
    price: float,
    is_active: bool
) -> None:
    exec_sql("""
        UPDATE products
        SET product_name = ?, category = ?, color = ?, size = ?, cost = ?, price = ?, is_active = ?
        WHERE sku = ?;
    """, (
        product_name.strip(),
        (category or "").strip(),
        (color or "").strip(),
        (size or "").strip(),
        float(cost or 0),
        float(price or 0),
        1 if is_active else 0,
        sku
    ))

def add_location(loc: str) -> None:
    loc = (loc or "").strip().upper()
    if not loc:
        return
    exec_sql("INSERT OR IGNORE INTO locations(location) VALUES (?);", (loc,))
    # Create missing stock rows for existing products
    df_skus = run_query_df("SELECT sku FROM products;")
    for sku in df_skus["sku"].tolist():
        ensure_stock_row(sku, loc)

def get_locations() -> list[str]:
    df = run_query_df("SELECT location FROM locations ORDER BY location;")
    return df["location"].tolist()

def log_movement(
    sku: str,
    movement_type: str,
    qty: int,
    location_from: Optional[str] = None,
    location_to: Optional[str] = None,
    reason: str = "",
    notes: str = ""
) -> None:
    exec_sql("""
        INSERT INTO movements(ts, sku, movement_type, location_from, location_to, qty, reason, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        now_iso(),
        sku,
        movement_type,
        location_from,
        location_to,
        int(qty),
        (reason or "").strip(),
        (notes or "").strip()
    ))

def apply_in(sku: str, location: str, qty: int, reason: str, notes: str) -> Tuple[bool, str]:
    if qty <= 0:
        return False, "Qty must be > 0."
    ensure_stock_row(sku, location)
    exec_sql("""
        UPDATE stock SET qty = qty + ?, updated_at = ?
        WHERE sku = ? AND location = ?;
    """, (int(qty), now_iso(), sku, location))
    log_movement(sku, "IN", qty, None, location, reason, notes)
    return True, "Stock added."

def apply_out(sku: str, location: str, qty: int, reason: str, notes: str) -> Tuple[bool, str]:
    if qty <= 0:
        return False, "Qty must be > 0."
    ensure_stock_row(sku, location)
    current = get_stock_qty(sku, location)
    if current - qty < 0:
        return False, f"Insufficient stock. Current: {current}, requested OUT: {qty}."
    exec_sql("""
        UPDATE stock SET qty = qty - ?, updated_at = ?
        WHERE sku = ? AND location = ?;
    """, (int(qty), now_iso(), sku, location))
    log_movement(sku, "OUT", qty, location, None, reason, notes)
    return True, "Stock deducted."

def apply_adjust(sku: str, location: str, new_qty: int, reason: str, notes: str) -> Tuple[bool, str]:
    if new_qty < 0:
        return False, "New qty cannot be negative."
    ensure_stock_row(sku, location)
    current = get_stock_qty(sku, location)
    delta = new_qty - current
    exec_sql("""
        UPDATE stock SET qty = ?, updated_at = ?
        WHERE sku = ? AND location = ?;
    """, (int(new_qty), now_iso(), sku, location))
    # log as ADJUST with qty=abs(delta)
    log_movement(sku, "ADJUST", abs(delta), location, location, reason, f"{notes} | prev={current} new={new_qty} delta={delta}")
    return True, f"Adjusted from {current} to {new_qty} (delta {delta})."

def apply_transfer(sku: str, loc_from: str, loc_to: str, qty: int, reason: str, notes: str) -> Tuple[bool, str]:
    if loc_from == loc_to:
        return False, "From and To locations must be different."
    if qty <= 0:
        return False, "Qty must be > 0."
    ensure_stock_row(sku, loc_from)
    ensure_stock_row(sku, loc_to)
    current = get_stock_qty(sku, loc_from)
    if current - qty < 0:
        return False, f"Insufficient stock at {loc_from}. Current: {current}, requested TRANSFER: {qty}."

    # subtract from source
    exec_sql("""
        UPDATE stock SET qty = qty - ?, updated_at = ?
        WHERE sku = ? AND location = ?;
    """, (int(qty), now_iso(), sku, loc_from))

    # add to destination
    exec_sql("""
        UPDATE stock SET qty = qty + ?, updated_at = ?
        WHERE sku = ? AND location = ?;
    """, (int(qty), now_iso(), sku, loc_to))

    log_movement(sku, "TRANSFER", qty, loc_from, loc_to, reason, notes)
    return True, "Transfer completed."

# =========================
# DATA VIEWS
# =========================
def df_products(active_only: bool = False) -> pd.DataFrame:
    if active_only:
        return run_query_df("""
            SELECT sku, product_name, category, color, size, cost, price, is_active, created_at
            FROM products
            WHERE is_active = 1
            ORDER BY created_at DESC;
        """)
    return run_query_df("""
        SELECT sku, product_name, category, color, size, cost, price, is_active, created_at
        FROM products
        ORDER BY created_at DESC;
    """)

def df_stock() -> pd.DataFrame:
    return run_query_df("""
        SELECT
          s.sku,
          p.product_name,
          p.category,
          p.color,
          p.size,
          s.location,
          s.qty,
          s.low_stock_threshold,
          s.updated_at
        FROM stock s
        JOIN products p ON p.sku = s.sku
        ORDER BY p.product_name, s.location;
    """)

def df_movements(limit: int = 500) -> pd.DataFrame:
    return run_query_df(f"""
        SELECT id, ts, sku, movement_type, location_from, location_to, qty, reason, notes
        FROM movements
        ORDER BY id DESC
        LIMIT {int(limit)};
    """)

def df_low_stock() -> pd.DataFrame:
    return run_query_df("""
        SELECT
          s.sku,
          p.product_name,
          s.location,
          s.qty,
          s.low_stock_threshold
        FROM stock s
        JOIN products p ON p.sku = s.sku
        WHERE s.low_stock_threshold > 0
          AND s.qty <= s.low_stock_threshold
        ORDER BY (s.low_stock_threshold - s.qty) DESC;
    """)

def df_dead_stock(days: int = 60) -> pd.DataFrame:
    # SKUs with no OUT movement in last X days (or never sold)
    cutoff = (dt.datetime.now() - dt.timedelta(days=days)).replace(microsecond=0).isoformat(sep=" ")
    return run_query_df("""
        WITH last_out AS (
          SELECT sku, MAX(ts) AS last_sold_ts
          FROM movements
          WHERE movement_type = 'OUT' AND reason = 'SOLD'
          GROUP BY sku
        )
        SELECT
          p.sku,
          p.product_name,
          p.category,
          p.color,
          p.size,
          COALESCE(l.last_sold_ts, 'NEVER') AS last_sold_ts
        FROM products p
        LEFT JOIN last_out l ON l.sku = p.sku
        WHERE (l.last_sold_ts IS NULL OR l.last_sold_ts < ?)
          AND p.is_active = 1
        ORDER BY last_sold_ts ASC;
    """, (cutoff,))

# =========================
# CSV IMPORT/EXPORT
# =========================
def download_df_button(df: pd.DataFrame, filename: str, label: str) -> None:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label=label, data=csv, file_name=filename, mime="text/csv")

def import_products_csv(df: pd.DataFrame) -> Tuple[int, int, list[str]]:
    # expects columns: sku, product_name, category, color, size, cost, price, is_active(optional)
    required = {"sku", "product_name"}
    missing = required - set(df.columns.str.lower())
    if missing:
        return 0, 0, [f"Missing required columns: {sorted(list(missing))}"]

    # normalize columns (case-insensitive)
    colmap = {c.lower(): c for c in df.columns}
    def g(col: str) -> pd.Series:
        return df[colmap[col]] if col in colmap else pd.Series([None]*len(df))

    inserted = 0
    updated = 0
    errors: list[str] = []

    for i in range(len(df)):
        sku = normalize_sku(str(g("sku").iloc[i]))
        name = str(g("product_name").iloc[i]).strip()
        if not sku or not name or name.lower() == "nan":
            errors.append(f"Row {i+1}: invalid sku/name.")
            continue

        category = str(g("category").iloc[i]).strip() if "category" in colmap else ""
        color = str(g("color").iloc[i]).strip() if "color" in colmap else ""
        size = str(g("size").iloc[i]).strip() if "size" in colmap else ""
        cost = float(g("cost").iloc[i]) if "cost" in colmap and pd.notna(g("cost").iloc[i]) else 0.0
        price = float(g("price").iloc[i]) if "price" in colmap and pd.notna(g("price").iloc[i]) else 0.0
        is_active = True
        if "is_active" in colmap and pd.notna(g("is_active").iloc[i]):
            val = str(g("is_active").iloc[i]).strip().lower()
            is_active = val in ("1", "true", "yes", "y")

        try:
            if sku_exists(sku):
                update_product(sku, name, category, color, size, cost, price, is_active)
                updated += 1
            else:
                add_product(sku, name, category, color, size, cost, price, is_active)
                inserted += 1
        except Exception as e:
            errors.append(f"Row {i+1} sku={sku}: {e}")

    return inserted, updated, errors

def import_stock_csv(df: pd.DataFrame) -> Tuple[int, list[str]]:
    # expects columns: sku, location, qty, low_stock_threshold(optional)
    required = {"sku", "location", "qty"}
    missing = required - set(df.columns.str.lower())
    if missing:
        return 0, [f"Missing required columns: {sorted(list(missing))}"]

    colmap = {c.lower(): c for c in df.columns}
    def g(col: str) -> pd.Series:
        return df[colmap[col]] if col in colmap else pd.Series([None]*len(df))

    applied = 0
    errors: list[str] = []

    for i in range(len(df)):
        sku = normalize_sku(str(g("sku").iloc[i]))
        loc = str(g("location").iloc[i]).strip().upper()
        if not sku_exists(sku):
            errors.append(f"Row {i+1}: SKU not found: {sku}")
            continue
        if not location_exists(loc):
            # auto-create location
            add_location(loc)

        try:
            qty = int(g("qty").iloc[i])
            if qty < 0:
                errors.append(f"Row {i+1}: qty cannot be negative.")
                continue
            ensure_stock_row(sku, loc)
            exec_sql("""
                UPDATE stock SET qty = ?, updated_at = ?
                WHERE sku = ? AND location = ?;
            """, (qty, now_iso(), sku, loc))

            if "low_stock_threshold" in colmap and pd.notna(g("low_stock_threshold").iloc[i]):
                thr = int(g("low_stock_threshold").iloc[i])
                set_low_stock_threshold(sku, loc, thr)

            applied += 1
        except Exception as e:
            errors.append(f"Row {i+1} sku={sku} loc={loc}: {e}")

    return applied, errors

# =========================
# UI HELPERS
# =========================
def sku_picker(active_only=True) -> Tuple[str, pd.DataFrame]:
    dfp = df_products(active_only=active_only)
    if dfp.empty:
        return "", dfp
    dfp["label"] = dfp["sku"] + " — " + dfp["product_name"].fillna("")
    label = st.selectbox("SKU", dfp["label"].tolist())
    sku = label.split(" — ")[0].strip()
    return sku, dfp

def product_quick_view(sku: str) -> None:
    dfp = run_query_df("""
        SELECT sku, product_name, category, color, size, cost, price, is_active
        FROM products WHERE sku = ?;
    """, (sku,))
    if dfp.empty:
        return
    row = dfp.iloc[0].to_dict()
    st.caption(
        f"**{row['product_name']}** | {row.get('category','')} | {row.get('color','')} | {row.get('size','')} "
        f"| cost={row.get('cost',0)} | price={row.get('price',0)} | active={bool(row.get('is_active',1))}"
    )

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    with st.expander("Filters", expanded=False):
        q = st.text_input("Search (SKU / name / category / color / size)", "")
        locs = sorted(df["location"].unique().tolist()) if "location" in df.columns else []
        pick_locs = st.multiselect("Location", options=locs, default=locs if locs else [])
        active_only = st.checkbox("Active products only (where possible)", value=False)

    out = df.copy()
    if q.strip():
        qq = q.strip().lower()
        cols = [c for c in ["sku", "product_name", "category", "color", "size", "location"] if c in out.columns]
        mask = False
        for c in cols:
            mask = mask | out[c].astype(str).str.lower().str.contains(qq, na=False)
        out = out[mask]

    if "location" in out.columns and pick_locs:
        out = out[out["location"].isin(pick_locs)]

    if active_only and "is_active" in out.columns:
        out = out[out["is_active"] == 1]

    return out

# =========================
# PAGE: DASHBOARD
# =========================
def page_dashboard():
    st.header("Dashboard")

    df_s = df_stock()
    df_l = df_low_stock()

    total_units = int(df_s["qty"].sum()) if not df_s.empty else 0
    active_products = int(run_query_df("SELECT COUNT(*) AS n FROM products WHERE is_active=1;")["n"].iloc[0])
    sku_count = int(run_query_df("SELECT COUNT(*) AS n FROM products;")["n"].iloc[0])
    oos = int((df_s["qty"] == 0).sum()) if not df_s.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Units (All Locations)", total_units)
    c2.metric("Active Products", active_products)
    c3.metric("Total SKUs", sku_count)
    c4.metric("Out of Stock rows (SKU+Loc)", oos)

    st.subheader("Low Stock Alerts (qty <= threshold)")
    if df_l.empty:
        st.info("No low-stock alerts. (Set thresholds per SKU+location in Stock Manager.)")
    else:
        st.dataframe(df_l, use_container_width=True)

    st.subheader("Current Stock (SKU + Location)")
    df_view = apply_filters(df_s)
    st.dataframe(df_view, use_container_width=True)

# =========================
# PAGE: PRODUCT MANAGER
# =========================
def page_products():
    st.header("Products (Master Data)")

    tab_add, tab_edit, tab_view = st.tabs(["Add Product", "Edit Product", "View Products"])

    with tab_add:
        st.subheader("Add new product")
        c1, c2 = st.columns(2)

        with c1:
            sku_raw = st.text_input("SKU (unique)", placeholder="RNRY-TSHIRT-TENCEL-BLACK-M")
            product_name = st.text_input("Product Name", placeholder="Tencel Tee")
            category = st.text_input("Category", placeholder="TSHIRT / POLO / WAFFLE / etc")

        with c2:
            color = st.text_input("Color", placeholder="BLACK")
            size = st.text_input("Size", placeholder="S / M / L / XL")
            cost = st.number_input("Cost", min_value=0.0, value=0.0, step=1000.0)
            price = st.number_input("Price", min_value=0.0, value=0.0, step=1000.0)

        is_active = st.checkbox("Active", value=True)
        sku = normalize_sku(sku_raw)

        with st.expander("SKU Helper (optional)", expanded=False):
            st.caption("Quick SKU builder (optional). Produces a SKU pattern you can copy.")
            b1, b2, b3, b4, b5 = st.columns(5)
            with b1:
                pfx = st.text_input("Prefix", "RNRY")
            with b2:
                pcat = st.text_input("Type", "TSHIRT")
            with b3:
                pmat = st.text_input("Material", "TENCEL")
            with b4:
                pcol = st.text_input("Color", "BLACK")
            with b5:
                psz = st.text_input("Size", "M")
            suggested = normalize_sku(f"{pfx}-{pcat}-{pmat}-{pcol}-{psz}")
            st.code(suggested)

        if st.button("Add Product", type="primary"):
            if not sku or len(sku) < 3:
                st.error("SKU invalid.")
            elif not product_name.strip():
                st.error("Product name is required.")
            elif sku_exists(sku):
                st.error("SKU already exists. Use Edit tab.")
            else:
                try:
                    add_product(sku, product_name, category, color, size, cost, price, is_active)
                    st.success(f"Added product: {sku}")
                except Exception as e:
                    st.error(f"Failed to add product: {e}")

    with tab_edit:
        st.subheader("Edit existing product")
        sku_sel, _dfp = sku_picker(active_only=False)
        if not sku_sel:
            st.info("No products yet.")
        else:
            dfp = run_query_df("""
                SELECT sku, product_name, category, color, size, cost, price, is_active
                FROM products WHERE sku = ?;
            """, (sku_sel,))
            row = dfp.iloc[0].to_dict()

            product_name = st.text_input("Product Name", value=row.get("product_name", ""))
            category = st.text_input("Category", value=row.get("category", ""))
            color = st.text_input("Color", value=row.get("color", ""))
            size = st.text_input("Size", value=row.get("size", ""))
            cost = st.number_input("Cost", min_value=0.0, value=float(row.get("cost", 0) or 0), step=1000.0)
            price = st.number_input("Price", min_value=0.0, value=float(row.get("price", 0) or 0), step=1000.0)
            is_active = st.checkbox("Active", value=bool(row.get("is_active", 1)))

            if st.button("Save Changes"):
                try:
                    update_product(sku_sel, product_name, category, color, size, cost, price, is_active)
                    st.success("Saved.")
                except Exception as e:
                    st.error(f"Failed: {e}")

    with tab_view:
        st.subheader("Products table")
        dfp = df_products(active_only=False)
        dfp = apply_filters(dfp) if not dfp.empty else dfp
        st.dataframe(dfp, use_container_width=True)
        if not dfp.empty:
            download_df_button(dfp, "products_export.csv", "Download products CSV")

# =========================
# PAGE: STOCK MANAGER
# =========================
def page_stock():
    st.header("Stock Manager")

    tab_in, tab_out, tab_adjust, tab_transfer, tab_thresholds = st.tabs(
        ["Stock IN", "Stock OUT", "Adjust", "Transfer", "Low-stock Thresholds"]
    )

    locations = get_locations()

    with tab_in:
        st.subheader("Add stock (IN)")
        sku, _ = sku_picker(active_only=True)
        if sku:
            product_quick_view(sku)
            loc = st.selectbox("Location (TO)", locations, index=0)
            qty = st.number_input("Qty", min_value=1, value=1, step=1)
            reason = st.selectbox("Reason", DEFAULT_REASONS_IN)
            notes = st.text_input("Notes (optional)", "")
            if st.button("Apply IN", type="primary"):
                ok, msg = apply_in(sku, loc, int(qty), reason, notes)
                (st.success if ok else st.error)(msg)

    with tab_out:
        st.subheader("Deduct stock (OUT)")
        sku, _ = sku_picker(active_only=True)
        if sku:
            product_quick_view(sku)
            loc = st.selectbox("Location (FROM)", locations, index=0)
            current = get_stock_qty(sku, loc)
            st.caption(f"Current stock at {loc}: **{current}**")
            qty = st.number_input("Qty", min_value=1, value=1, step=1)
            reason = st.selectbox("Reason", DEFAULT_REASONS_OUT)
            notes = st.text_input("Notes (optional)", "")
            if st.button("Apply OUT", type="primary"):
                ok, msg = apply_out(sku, loc, int(qty), reason, notes)
                (st.success if ok else st.error)(msg)

    with tab_adjust:
        st.subheader("Set stock to an exact number (ADJUST)")
        sku, _ = sku_picker(active_only=True)
        if sku:
            product_quick_view(sku)
            loc = st.selectbox("Location", locations, index=0)
            current = get_stock_qty(sku, loc)
            st.caption(f"Current stock at {loc}: **{current}**")
            new_qty = st.number_input("New qty", min_value=0, value=int(current), step=1)
            reason = st.selectbox("Reason", ["STOCKTAKE", "ADJUSTMENT", "CORRECTION"])
            notes = st.text_input("Notes (optional)", "")
            if st.button("Apply ADJUST", type="primary"):
                ok, msg = apply_adjust(sku, loc, int(new_qty), reason, notes)
                (st.success if ok else st.error)(msg)

    with tab_transfer:
        st.subheader("Move stock between locations (TRANSFER)")
        sku, _ = sku_picker(active_only=True)
        if sku:
            product_quick_view(sku)
            c1, c2 = st.columns(2)
            with c1:
                loc_from = st.selectbox("From", locations, index=0)
                stock_from = get_stock_qty(sku, loc_from)
                st.caption(f"Stock at {loc_from}: **{stock_from}**")
            with c2:
                # default to second location if possible
                idx_to = 1 if len(locations) > 1 else 0
                loc_to = st.selectbox("To", locations, index=idx_to)
            qty = st.number_input("Qty", min_value=1, value=1, step=1)
            reason = st.selectbox("Reason", ["REBALANCE", "FULFILLMENT", "MOVE"])
            notes = st.text_input("Notes (optional)", "")
            if st.button("Apply TRANSFER", type="primary"):
                ok, msg = apply_transfer(sku, loc_from, loc_to, int(qty), reason, notes)
                (st.success if ok else st.error)(msg)

    with tab_thresholds:
        st.subheader("Low-stock thresholds (per SKU + location)")
        st.caption("If qty <= threshold, it will appear on Dashboard alerts.")
        sku, _ = sku_picker(active_only=True)
        if sku:
            product_quick_view(sku)
            loc = st.selectbox("Location", locations, index=0)
            ensure_stock_row(sku, loc)
            df_thr = run_query_df("""
                SELECT qty, low_stock_threshold
                FROM stock WHERE sku = ? AND location = ?;
            """, (sku, loc))
            qty_now = int(df_thr.iloc[0]["qty"])
            thr_now = int(df_thr.iloc[0]["low_stock_threshold"])
            st.caption(f"Current qty: **{qty_now}** | Current threshold: **{thr_now}**")

            new_thr = st.number_input("Set threshold", min_value=0, value=int(thr_now), step=1)
            if st.button("Save Threshold"):
                set_low_stock_threshold(sku, loc, int(new_thr))
                st.success("Threshold saved.")

# =========================
# PAGE: MOVEMENTS / HISTORY
# =========================
def page_history():
    st.header("Movement History")

    limit = st.slider("Rows to show", min_value=50, max_value=2000, value=500, step=50)
    dfm = df_movements(limit=limit)

    if dfm.empty:
        st.info("No movements logged yet.")
        return

    with st.expander("Filters", expanded=False):
        q = st.text_input("Search (sku/reason/notes)", "")
        types = sorted(dfm["movement_type"].unique().tolist())
        pick_types = st.multiselect("Movement types", options=types, default=types)
        date_from = st.date_input("Date from", value=None)
        date_to = st.date_input("Date to", value=None)

    view = dfm.copy()

    if q.strip():
        qq = q.strip().lower()
        mask = (
            view["sku"].astype(str).str.lower().str.contains(qq, na=False)
            | view["reason"].astype(str).str.lower().str.contains(qq, na=False)
            | view["notes"].astype(str).str.lower().str.contains(qq, na=False)
        )
        view = view[mask]

    if pick_types:
        view = view[view["movement_type"].isin(pick_types)]

    # date filter (ts format: "YYYY-MM-DD HH:MM:SS")
    if date_from:
        view = view[view["ts"].astype(str).str[:10] >= str(date_from)]
    if date_to:
        view = view[view["ts"].astype(str).str[:10] <= str(date_to)]

    st.dataframe(view, use_container_width=True)
    download_df_button(view, "movements_export.csv", "Download movements CSV")

# =========================
# PAGE: ANALYTICS
# =========================
def page_analytics():
    st.header("Analytics")

    st.subheader("Dead Stock (no SOLD in last N days)")
    days = st.slider("Days", min_value=7, max_value=365, value=60, step=1)
    dfd = df_dead_stock(days=days)
    if dfd.empty:
        st.success("No dead stock detected under this rule.")
    else:
        st.dataframe(dfd, use_container_width=True)
        download_df_button(dfd, f"dead_stock_{days}d.csv", "Download dead stock CSV")

    st.subheader("Sales velocity (based on SOLD OUT logs)")
    st.caption("This uses movement_type=OUT and reason=SOLD. If you don't consistently log SOLD, velocity will be inaccurate.")
    window = st.slider("Window (days)", min_value=7, max_value=180, value=30, step=1)
    cutoff = (dt.datetime.now() - dt.timedelta(days=window)).replace(microsecond=0).isoformat(sep=" ")

    dfv = run_query_df("""
        SELECT sku, COUNT(*) AS sold_events, SUM(qty) AS units_sold
        FROM movements
        WHERE movement_type='OUT' AND reason='SOLD' AND ts >= ?
        GROUP BY sku
        ORDER BY units_sold DESC;
    """, (cutoff,))

    if dfv.empty:
        st.info("No SOLD data in the selected window.")
    else:
        dfp = df_products(active_only=False)[["sku", "product_name", "category", "color", "size"]]
        out = dfv.merge(dfp, on="sku", how="left")
        out["units_per_day"] = out["units_sold"] / float(window)
        st.dataframe(out, use_container_width=True)
        download_df_button(out, f"velocity_{window}d.csv", "Download velocity CSV")

# =========================
# PAGE: DATA TOOLS (IMPORT/EXPORT)
# =========================
def page_data_tools():
    st.header("Data Tools (CSV Import / Export)")

    st.subheader("Export current data")
    dfp = df_products(active_only=False)
    dfs = df_stock()
    dfm = df_movements(limit=5000)

    c1, c2, c3 = st.columns(3)
    with c1:
        if not dfp.empty:
            download_df_button(dfp, "products_export.csv", "Download products_export.csv")
    with c2:
        if not dfs.empty:
            download_df_button(dfs, "stock_export.csv", "Download stock_export.csv")
    with c3:
        if not dfm.empty:
            download_df_button(dfm, "movements_export.csv", "Download movements_export.csv")

    st.divider()
    st.subheader("Import Products CSV (upsert)")
    st.caption("Required columns: sku, product_name. Optional: category,color,size,cost,price,is_active")
    up_prod = st.file_uploader("Upload products CSV", type=["csv"], key="prod_csv")

    if up_prod is not None:
        try:
            df = pd.read_csv(up_prod)
            st.dataframe(df.head(50), use_container_width=True)
            if st.button("Import products CSV", type="primary"):
                ins, upd, errs = import_products_csv(df)
                st.success(f"Imported products: inserted={ins}, updated={upd}")
                if errs:
                    st.warning("Some rows had issues:")
                    st.write(errs[:50])
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")

    st.divider()
    st.subheader("Import Stock CSV (set absolute qty)")
    st.caption("Required columns: sku, location, qty. Optional: low_stock_threshold")
    up_stock = st.file_uploader("Upload stock CSV", type=["csv"], key="stock_csv")

    if up_stock is not None:
        try:
            df = pd.read_csv(up_stock)
            st.dataframe(df.head(50), use_container_width=True)
            if st.button("Import stock CSV", type="primary"):
                applied, errs = import_stock_csv(df)
                st.success(f"Applied stock rows: {applied}")
                if errs:
                    st.warning("Some rows had issues:")
                    st.write(errs[:50])
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")

    st.divider()
    st.subheader("Locations Manager")
    st.caption("Add new stock locations (e.g., 'EXHIBITION', 'RESELLER-A'). Existing SKUs will get stock rows automatically.")
    new_loc = st.text_input("New location name", "")
    if st.button("Add location"):
        if not new_loc.strip():
            st.error("Location cannot be empty.")
        else:
            add_location(new_loc)
            st.success(f"Added location: {new_loc.strip().upper()}")
    st.write("Current locations:", ", ".join(get_locations()))

# =========================
# APP BOOT
# =========================
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    init_db()

    st.title(APP_TITLE)
    st.caption("Inventory app (Streamlit + SQLite). Multi-location stock + movement logs + alerts + CSV tools.")

    menu = st.sidebar.radio(
        "Menu",
        ["Dashboard", "Products", "Stock Manager", "Movement History", "Analytics", "Data Tools"],
        index=0
    )

    st.sidebar.divider()
    st.sidebar.caption("Tips")
    st.sidebar.write("- Set low-stock thresholds for alerts.")
    st.sidebar.write("- Always log SOLD with Stock OUT reason=SOLD for accurate analytics.")
    st.sidebar.write("- Use Data Tools to import/export CSV as backup.")

    if menu == "Dashboard":
        page_dashboard()
    elif menu == "Products":
        page_products()
    elif menu == "Stock Manager":
        page_stock()
    elif menu == "Movement History":
        page_history()
    elif menu == "Analytics":
        page_analytics()
    elif menu == "Data Tools":
        page_data_tools()

if __name__ == "__main__":
    main()

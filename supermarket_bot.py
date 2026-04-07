"""
Supermarket Sales Analysis Telegram Bot
スーパー売上レポート自動分析ボット
"""

import os
import re
import io
import csv
import json
import sqlite3
import asyncio
import logging
import pathlib
import calendar
from datetime import datetime, timedelta, time as dtime, timezone
from typing import Optional

# ローカル開発時に .env を自動読み込み（本番環境では無視）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx
import anthropic
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, TypeHandler, CallbackQueryHandler,
    filters, ContextTypes
)

# ─── Logging ───────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────
BOT_TOKEN     = os.environ.get('TELEGRAM_BOT_TOKEN', '')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
DB_PATH       = os.environ.get('DB_PATH', '/app/data/sales_data.db')
# Comma-separated chat IDs that share the same store data
_raw_ids = os.environ.get('STORE_GROUP_IDS', '')
STORE_GROUP_IDS = [int(x.strip()) for x in _raw_ids.split(',') if x.strip()] if _raw_ids else []

# Auto weekly report target group (set WEEKLY_REPORT_CHAT_ID in Railway env vars)
_weekly_chat_raw = os.environ.get('WEEKLY_REPORT_CHAT_ID', '')
WEEKLY_REPORT_CHAT_ID = int(_weekly_chat_raw.strip()) if _weekly_chat_raw.strip() else 0

# Manager Telegram IDs for direct notifications (format: "Name:ID,Name:ID")
_manager_ids_raw = os.environ.get('MANAGER_IDS', '')
MANAGER_IDS: dict[str, int] = {}
for _item in _manager_ids_raw.split(','):
    _item = _item.strip()
    if ':' in _item:
        _name, _tid = _item.rsplit(':', 1)
        try:
            MANAGER_IDS[_name.strip()] = int(_tid.strip())
        except ValueError:
            pass

# Group that receives shift schedule confirmation replies
# If not set, falls back to WEEKLY_REPORT_CHAT_ID
_schedule_reply_raw = os.environ.get('SCHEDULE_REPLY_CHAT_ID', '')
SCHEDULE_REPLY_CHAT_ID = int(_schedule_reply_raw.strip()) if _schedule_reply_raw.strip() else 0

# Owner's personal chat ID for private reports (set OWNER_CHAT_ID in Railway env vars)
_owner_chat_raw = os.environ.get('OWNER_CHAT_ID', '').strip()
try:
    OWNER_CHAT_ID = int(_owner_chat_raw) if _owner_chat_raw else 0
except ValueError:
    OWNER_CHAT_ID = 0
    print(f"WARNING: OWNER_CHAT_ID='{_owner_chat_raw}' is not a numeric ID — must be a number, not a username")

# Philippines Time (UTC+8) — used for scheduling
PHT = timezone(timedelta(hours=8))

# Brave Search API for procurement recommendations
BRAVE_SEARCH_API_KEY = os.environ.get('BRAVE_SEARCH_API_KEY', '')

# UTAK POS credentials for auto-sync
UTAK_EMAIL    = os.environ.get('UTAK_EMAIL', '')
UTAK_PASSWORD = os.environ.get('UTAK_PASSWORD', '')

# DB directory auto-create
pathlib.Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

# ─── Database ──────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS supermarket_sales (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            date              TEXT,
            store             TEXT,
            submitted_by      TEXT,
            cash_sale         REAL DEFAULT 0,
            card_sale         REAL DEFAULT 0,
            qr_ph             REAL DEFAULT 0,
            maya              REAL DEFAULT 0,
            grab              REAL DEFAULT 0,
            foodpanda         REAL DEFAULT 0,
            graveyard         REAL DEFAULT 0,
            morning           REAL DEFAULT 0,
            afternoon         REAL DEFAULT 0,
            discounts         REAL DEFAULT 0,
            wastage           REAL DEFAULT 0,
            total             REAL DEFAULT 0,
            monthly_total     REAL DEFAULT 0,
            cash_drawer       REAL DEFAULT 0,
            transaction_count INTEGER DEFAULT 0,
            salary            REAL DEFAULT 0,
            inventory         REAL DEFAULT 0,
            other_expense     REAL DEFAULT 0,
            cashbox           REAL DEFAULT 0,
            for_deposit       REAL DEFAULT 0,
            comment           TEXT DEFAULT '',
            raw_text          TEXT,
            chat_id           INTEGER,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, store, chat_id)
        )
    ''')
    # 既存DBにcommentカラムがない場合は追加（マイグレーション）
    try:
        c.execute('ALTER TABLE supermarket_sales ADD COLUMN comment TEXT DEFAULT ""')
        conn.commit()
        logger.info("Migrated: added 'comment' column to supermarket_sales")
    except Exception:
        pass  # カラムが既に存在する場合は無視
    # Bot message log table
    c.execute('''
        CREATE TABLE IF NOT EXISTS bot_messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    INTEGER,
            message_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id   INTEGER PRIMARY KEY,
            translate INTEGER DEFAULT 0
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS sales_targets (
            chat_id     INTEGER,
            target_type TEXT,
            amount      REAL,
            PRIMARY KEY (chat_id, target_type)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS shift_schedules (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            shift       TEXT NOT NULL,
            manager     TEXT,
            chat_id     INTEGER,
            UNIQUE(date, shift, chat_id)
        )
    ''')
    # Migration: add new columns if they don't exist yet
    for col, definition in [
        ('foodpanda',             'REAL DEFAULT 0'),
        ('cash_drawer',           'REAL DEFAULT 0'),
        ('cat_instant_food',      'REAL DEFAULT 0'),
        ('cat_seasoning',         'REAL DEFAULT 0'),
        ('cat_grabmart',          'REAL DEFAULT 0'),
        ('cat_frozen_item',       'REAL DEFAULT 0'),
        ('cat_personal_care',     'REAL DEFAULT 0'),
        ('cat_beverage',          'REAL DEFAULT 0'),
        ('cat_snacks_candies',    'REAL DEFAULT 0'),
        ('cat_chilled_item',      'REAL DEFAULT 0'),
        ('cat_medicine',          'REAL DEFAULT 0'),
        ('cat_bento',             'REAL DEFAULT 0'),
        ('cat_rice_noodle_bread', 'REAL DEFAULT 0'),
        ('cat_grabfood',          'REAL DEFAULT 0'),
        ('cat_rte',               'REAL DEFAULT 0'),
        ('cat_ice_cream',         'REAL DEFAULT 0'),
        ('cat_bath_item',         'REAL DEFAULT 0'),
    ]:
        try:
            c.execute(f'ALTER TABLE supermarket_sales ADD COLUMN {col} {definition}')
        except Exception:
            pass  # column already exists
    c.execute('''
        CREATE TABLE IF NOT EXISTS procurement_settings (
            chat_id        INTEGER PRIMARY KEY,
            weekly_budget  REAL DEFAULT 0,
            restock_day    INTEGER DEFAULT 1,
            auto_send      INTEGER DEFAULT 1,
            last_sent_date TEXT DEFAULT ''
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS order_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            ordered_at  TEXT NOT NULL,
            category    TEXT NOT NULL,
            item_name   TEXT NOT NULL,
            unit_price  INTEGER NOT NULL,
            qty         INTEGER NOT NULL,
            total       INTEGER NOT NULL,
            source      TEXT DEFAULT '定番'
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS fixed_items (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            category    TEXT NOT NULL DEFAULT 'その他',
            item_name   TEXT NOT NULL,
            unit_price  INTEGER DEFAULT 0,
            min_qty     INTEGER NOT NULL,
            UNIQUE(chat_id, item_name)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            item_name   TEXT NOT NULL,
            category    TEXT NOT NULL DEFAULT 'その他',
            qty         INTEGER NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL,
            UNIQUE(chat_id, item_name)
        )
    ''')
    # UTAK POS data tables
    c.execute('''
        CREATE TABLE IF NOT EXISTS utak_inventory (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id       INTEGER NOT NULL,
            imported_at   TEXT NOT NULL,
            category      TEXT NOT NULL,
            item_name     TEXT NOT NULL,
            option        TEXT DEFAULT '',
            beginning     REAL DEFAULT 0,
            added         REAL DEFAULT 0,
            deducted      REAL DEFAULT 0,
            ending        REAL DEFAULT 0,
            inv_value     REAL DEFAULT 0,
            UNIQUE(chat_id, imported_at, category, item_name, option)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS utak_sales (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id       INTEGER NOT NULL,
            sale_date     TEXT NOT NULL,
            sale_time     TEXT DEFAULT '',
            transaction_id TEXT DEFAULT '',
            receipt_no    TEXT DEFAULT '',
            total         REAL DEFAULT 0,
            payment_type  TEXT DEFAULT '',
            category      TEXT NOT NULL,
            item_name     TEXT NOT NULL,
            option        TEXT DEFAULT '',
            qty           REAL DEFAULT 0,
            price_per_unit REAL DEFAULT 0,
            gross_price   REAL DEFAULT 0,
            cost          REAL DEFAULT 0,
            cashier       TEXT DEFAULT ''
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

# ─── Shift schedule ────────────────────────────────────────
def is_manpower_schedule(text: str) -> bool:
    return bool(re.search(r'manpower\s+schedule', text, re.IGNORECASE))

def parse_manpower_schedule(text: str) -> dict:
    """Return {'date': 'YYYY-MM-DD', 'graveyard': name, 'morning': name, 'afternoon': name}."""
    result = {}
    # Date
    date_m = re.search(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+(\d{1,2})\s*,?\s*(\d{4})', text, re.IGNORECASE
    )
    if date_m:
        try:
            result['date'] = datetime.strptime(
                f"{date_m.group(1)} {date_m.group(2)} {date_m.group(3)}", '%B %d %Y'
            ).strftime('%Y-%m-%d')
        except ValueError:
            result['date'] = datetime.now().strftime('%Y-%m-%d')
    else:
        result['date'] = datetime.now().strftime('%Y-%m-%d')
    # Manager/OIC per shift
    for shift_key, patterns in [
        ('graveyard', [
            r'graveyard[\s\S]{0,300}?(?:OIC|Team Lead|Manager)[^:\n]*:\s*([A-Za-z ]+)',
        ]),
        ('morning', [
            r'morning[\s\S]{0,300}?(?:OIC|Team Lead|Manager)[^:\n]*:\s*([A-Za-z ]+)',
        ]),
        ('afternoon', [
            r'afternoon[\s\S]{0,300}?(?:OIC|Team Lead|Manager)[^:\n]*:\s*([A-Za-z ]+)',
        ]),
    ]:
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                name = m.group(1).strip().split('\n')[0].strip()
                result[shift_key] = name
                break
    return result

def save_shift_schedule(parsed: dict, chat_id: int):
    conn = get_conn()
    c = conn.cursor()
    date = parsed.get('date', datetime.now().strftime('%Y-%m-%d'))
    for shift in ('graveyard', 'morning', 'afternoon'):
        manager = parsed.get(shift)
        if manager:
            c.execute(
                'INSERT OR REPLACE INTO shift_schedules (date, shift, manager, chat_id) VALUES (?,?,?,?)',
                (date, shift, manager, chat_id)
            )
    conn.commit()
    conn.close()

def get_last_shift_manager(date: str, chat_id: int) -> Optional[str]:
    """Return the afternoon shift manager name for the given date."""
    conn = get_conn()
    c = conn.cursor()
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    c.execute(
        f'SELECT manager FROM shift_schedules WHERE chat_id IN ({placeholders}) AND date=? AND shift=?',
        (*ids, date, 'afternoon')
    )
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def find_manager_id(name: str) -> Optional[int]:
    """Match a manager name to a Telegram ID from MANAGER_IDS.
    Case-insensitive. Matches if either string is a substring of the other,
    or if any word-pair is a prefix match (handles 'Vince' ↔ 'Vincente').
    """
    name_lower = name.lower()
    name_parts = name_lower.split()
    for key, tid in MANAGER_IDS.items():
        key_lower = key.lower()
        key_parts = key_lower.split()
        # Substring match in either direction
        if key_lower in name_lower or name_lower in key_lower:
            return tid
        # Word-level prefix match (e.g., "vince" matches "vincente")
        if any(
            kp.startswith(np) or np.startswith(kp)
            for kp in key_parts for np in name_parts
        ):
            return tid
    return None

# ─── Parser ────────────────────────────────────────────────
def _num(text: str, field: str) -> float:
    pattern = rf'{re.escape(field)}\s*:?\s*[₱Pp]?\s*([\d,]+\.?\d*)'
    m = re.search(pattern, text, re.IGNORECASE)
    return float(m.group(1).replace(',', '')) if m else 0.0

def _cat_num(text: str, field: str) -> float:
    pattern = rf'{re.escape(field)}\s*[–—-]+\s*[₱Pp]?\s*([\d,]+\.?\d*)'
    m = re.search(pattern, text, re.IGNORECASE)
    return float(m.group(1).replace(',', '')) if m else 0.0

# ─── Target helpers ────────────────────────────────────────
def get_target(chat_id: int, target_type: str) -> float:
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT amount FROM sales_targets WHERE chat_id=? AND target_type=?', (chat_id, target_type))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0.0

def set_target(chat_id: int, target_type: str, amount: float):
    conn = get_conn()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO sales_targets (chat_id, target_type, amount) VALUES (?,?,?)',
              (chat_id, target_type, amount))
    conn.commit()
    conn.close()

def delete_target(chat_id: int, target_type: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute('DELETE FROM sales_targets WHERE chat_id=? AND target_type=?', (chat_id, target_type))
    conn.commit()
    conn.close()

def get_target_any(chat_id: int, target_type: str) -> float:
    """Like get_target but searches all linked chat IDs (STORE_GROUP_IDS)."""
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    c.execute(f'SELECT amount FROM sales_targets WHERE chat_id IN ({placeholders}) AND target_type=? LIMIT 1',
              (*ids, target_type))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0.0

def get_daily_target(chat_id: int, date_str: str) -> float:
    """Return the day-of-week-specific daily target, falling back to generic 'daily'."""
    try:
        wd = datetime.strptime(date_str, '%Y-%m-%d').weekday()  # 0=Mon … 6=Sun
    except ValueError:
        wd = datetime.now(PHT).weekday()
    if wd <= 3:   target_type = 'daily_mon_thu'   # Mon-Thu
    elif wd == 4: target_type = 'daily_fri'        # Fri
    else:         target_type = 'daily_sat_sun'    # Sat-Sun
    v = get_target_any(chat_id, target_type)
    return v if v > 0 else get_target_any(chat_id, 'daily')

# ─── Procurement settings helpers ─────────────────────────
WEEKDAY_NAMES_JA = ['月曜', '火曜', '水曜', '木曜', '金曜', '土曜', '日曜']
WEEKDAY_NAMES_EN = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

def get_procurement_settings(chat_id: int) -> dict:
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    c.execute(f'SELECT weekly_budget, restock_day, auto_send, last_sent_date FROM procurement_settings WHERE chat_id IN ({placeholders}) LIMIT 1', ids)
    row = c.fetchone()
    conn.close()
    if row:
        return {'weekly_budget': row[0], 'restock_day': row[1], 'auto_send': bool(row[2]), 'last_sent_date': row[3]}
    return {'weekly_budget': 0.0, 'restock_day': 1, 'auto_send': True, 'last_sent_date': ''}

def set_procurement_budget(chat_id: int, amount: float):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO procurement_settings (chat_id, weekly_budget)
                 VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET weekly_budget=excluded.weekly_budget''',
              (chat_id, amount))
    conn.commit()
    conn.close()

def set_restock_day(chat_id: int, day: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO procurement_settings (chat_id, restock_day)
                 VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET restock_day=excluded.restock_day''',
              (chat_id, day))
    conn.commit()
    conn.close()

def update_last_sent_date(chat_id: int, date_str: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute('UPDATE procurement_settings SET last_sent_date=? WHERE chat_id=?', (date_str, chat_id))
    conn.commit()
    conn.close()

# ─── Order history helpers ─────────────────────────────────
def save_order_history(chat_id: int, approved_categories: list):
    """Save finalized order items to order_history table."""
    today = datetime.now(PHT).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    for cat in approved_categories:
        cat_name = cat.get('name', '')
        for item in cat.get('items', []):
            if item.get('qty', 0) <= 0:
                continue
            qty = item['qty']
            price = item.get('unit_price', 0)
            c.execute(
                'INSERT INTO order_history (chat_id, ordered_at, category, item_name, unit_price, qty, total, source) VALUES (?,?,?,?,?,?,?,?)',
                (chat_id, today, cat_name, item['name'], price, qty, price * qty, item.get('source', '定番'))
            )
    conn.commit()
    conn.close()

def get_order_history_summary(chat_id: int, n: int = 3) -> str:
    """Return a text summary of the last N order dates for AI context."""
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    c.execute(f'''
        SELECT DISTINCT ordered_at FROM order_history
        WHERE chat_id IN ({placeholders})
        ORDER BY ordered_at DESC LIMIT ?
    ''', (*ids, n))
    dates = [row[0] for row in c.fetchall()]
    if not dates:
        conn.close()
        return "（注文履歴なし）"
    lines = []
    for date in dates:
        c.execute(f'''
            SELECT category, item_name, qty, unit_price, total FROM order_history
            WHERE chat_id IN ({placeholders}) AND ordered_at=?
            ORDER BY category, item_name
        ''', (*ids, date))
        rows = c.fetchall()
        total = sum(r[4] for r in rows)
        items_str = ', '.join(f"{r[1]}×{r[2]}" for r in rows[:8])
        if len(rows) > 8:
            items_str += f" 他{len(rows)-8}品"
        lines.append(f"  [{date}] 合計¥{total:,} — {items_str}")
    conn.close()
    return "\n".join(lines)

def get_order_history_records(chat_id: int, days: int = 90) -> list:
    """Return raw order history rows for CSV export."""
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    c.execute(f'''
        SELECT ordered_at, category, item_name, unit_price, qty, total, source
        FROM order_history
        WHERE chat_id IN ({placeholders}) AND ordered_at >= ?
        ORDER BY ordered_at DESC, category, item_name
    ''', (*ids, since))
    rows = [{'ordered_at': r[0], 'category': r[1], 'item_name': r[2],
             'unit_price': r[3], 'qty': r[4], 'total': r[5], 'source': r[6]}
            for r in c.fetchall()]
    conn.close()
    return rows

# ─── Fixed items helpers ───────────────────────────────────
def add_fixed_item(chat_id: int, item_name: str, min_qty: int, unit_price: int = 0, category: str = 'その他'):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO fixed_items (chat_id, category, item_name, unit_price, min_qty)
                 VALUES (?,?,?,?,?)
                 ON CONFLICT(chat_id, item_name) DO UPDATE SET
                     min_qty=excluded.min_qty, unit_price=excluded.unit_price, category=excluded.category''',
              (chat_id, category, item_name, unit_price, min_qty))
    conn.commit()
    conn.close()

def get_fixed_items(chat_id: int) -> list:
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    c.execute(f'SELECT category, item_name, unit_price, min_qty FROM fixed_items WHERE chat_id IN ({placeholders}) ORDER BY category, item_name', ids)
    rows = [{'category': r[0], 'item_name': r[1], 'unit_price': r[2], 'min_qty': r[3]} for r in c.fetchall()]
    conn.close()
    return rows

def delete_fixed_item(chat_id: int, item_name: str) -> bool:
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    c.execute(f'DELETE FROM fixed_items WHERE chat_id IN ({placeholders}) AND item_name=?', (*ids, item_name))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

# ─── Inventory helpers ─────────────────────────────────────
def add_inventory(chat_id: int, item_name: str, category: str, qty_delta: int):
    """Add (or subtract) qty from inventory. Creates record if not exists."""
    now = datetime.now(PHT).strftime('%Y-%m-%d %H:%M')
    conn = get_conn()
    c = conn.cursor()
    c.execute('''INSERT INTO inventory (chat_id, item_name, category, qty, updated_at)
                 VALUES (?,?,?,?,?)
                 ON CONFLICT(chat_id, item_name) DO UPDATE SET
                     qty = MAX(0, qty + excluded.qty),
                     updated_at = excluded.updated_at''',
              (chat_id, item_name, category, qty_delta, now))
    conn.commit()
    conn.close()

def get_inventory(chat_id: int) -> list:
    ids = get_chat_ids(chat_id)
    conn = get_conn()
    c = conn.cursor()
    placeholders = ','.join('?' * len(ids))
    c.execute(f'SELECT item_name, category, qty, updated_at FROM inventory WHERE chat_id IN ({placeholders}) ORDER BY category, item_name', ids)
    rows = [{'item_name': r[0], 'category': r[1], 'qty': r[2], 'updated_at': r[3]} for r in c.fetchall()]
    conn.close()
    return rows

# ─── UTAK POS data helpers ────────────────────────────────
def _parse_float(val: str) -> float:
    """CSV値を数値に変換。空文字やエラーは0.0。"""
    try:
        return float(val.strip().replace(',', '')) if val and val.strip() else 0.0
    except (ValueError, TypeError):
        return 0.0

def _normalize_category(raw: str) -> str:
    """'01 FROZEN ITEM' → 'FROZEN ITEM' のように番号プレフィックスを除去。"""
    import re as _re
    return _re.sub(r'^\d+\s+', '', raw.strip())

def detect_utak_csv_type(header: list[str]) -> str:
    """CSVヘッダーからinventory or transactionsを判定。"""
    h = [c.lower().strip() for c in header]
    if 'beginning' in h or 'inventory value' in h:
        return 'inventory'
    if 'transaction id' in h or 'receipt no.' in h:
        return 'transactions'
    return 'unknown'

def import_utak_inventory_csv(chat_id: int, rows: list[dict]) -> int:
    """UTAK在庫CSVをDBにインポート。戻り値は取り込み件数。"""
    now = datetime.now(PHT).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    # 同日の既存データを削除して上書き
    c.execute('DELETE FROM utak_inventory WHERE chat_id=? AND imported_at=?', (chat_id, now))
    count = 0
    for r in rows:
        cat = _normalize_category(r.get('Category', ''))
        item = r.get('Title', '').strip()
        if not cat or not item:
            continue
        option = r.get('Option', '').strip()
        ending = _parse_float(r.get('End', ''))
        # 在庫0かつ値もない行はスキップ（UTAKの空行）
        inv_val = _parse_float(r.get('Inventory Value', ''))
        if ending == 0 and inv_val == 0 and not r.get('Beginning', '').strip():
            continue
        c.execute('''INSERT OR REPLACE INTO utak_inventory
                     (chat_id, imported_at, category, item_name, option, beginning, added, deducted, ending, inv_value)
                     VALUES (?,?,?,?,?,?,?,?,?,?)''',
                  (chat_id, now, cat, item, option,
                   _parse_float(r.get('Beginning', '')),
                   _parse_float(r.get('Added', '')),
                   _parse_float(r.get('Deducted', '')),
                   ending, inv_val))
        count += 1
    conn.commit()
    conn.close()
    return count

def import_utak_sales_csv(chat_id: int, rows: list[dict]) -> int:
    """UTAK売上CSVをDBにインポート。戻り値は取り込み件数。"""
    conn = get_conn()
    c = conn.cursor()
    count = 0
    for r in rows:
        cat = r.get('Category', '').strip()
        item = r.get('Item', '').strip()
        if not cat or not item:
            continue
        # 日付をYYYY-MM-DD形式に変換
        raw_date = r.get('Date', '').strip()
        try:
            sale_date = datetime.strptime(raw_date, '%d %b %Y').strftime('%Y-%m-%d')
        except Exception:
            sale_date = raw_date
        qty = _parse_float(r.get('Quantity', ''))
        if qty == 0:
            continue
        c.execute('''INSERT INTO utak_sales
                     (chat_id, sale_date, sale_time, transaction_id, receipt_no, total,
                      payment_type, category, item_name, option, qty, price_per_unit, gross_price, cost, cashier)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (chat_id, sale_date, r.get('Time', '').strip(),
                   r.get('Transaction ID', '').strip(), r.get('Receipt No.', '').strip(),
                   _parse_float(r.get('Total', '')), r.get('Payment Type', '').strip(),
                   cat, item, r.get('Option', '').strip(), qty,
                   _parse_float(r.get('Price per Unit', '')),
                   _parse_float(r.get('Gross Price', '')),
                   _parse_float(r.get('Cost', '')),
                   r.get('Cashier', '').strip()))
        count += 1
    conn.commit()
    conn.close()
    return count

def get_utak_low_stock(chat_id: int, threshold: int = 3) -> list[dict]:
    """在庫が少ない（ending <= threshold）かつ売れている商品を取得。"""
    conn = get_conn()
    c = conn.cursor()
    # 最新のインポート日を取得
    c.execute('SELECT MAX(imported_at) FROM utak_inventory WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    if not row or not row[0]:
        conn.close()
        return []
    latest = row[0]
    c.execute('''SELECT category, item_name, option, ending, inv_value
                 FROM utak_inventory
                 WHERE chat_id=? AND imported_at=? AND ending > 0 AND ending <= ?
                 ORDER BY ending ASC, category''',
              (chat_id, latest, threshold))
    items = [{'category': r[0], 'item_name': r[1], 'option': r[2], 'ending': r[3], 'inv_value': r[4]}
             for r in c.fetchall()]
    conn.close()
    return items

def get_utak_out_of_stock(chat_id: int) -> list[dict]:
    """在庫切れ（ending <= 0）かつ過去に在庫があった商品。"""
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT MAX(imported_at) FROM utak_inventory WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    if not row or not row[0]:
        conn.close()
        return []
    latest = row[0]
    c.execute('''SELECT category, item_name, option, ending
                 FROM utak_inventory
                 WHERE chat_id=? AND imported_at=? AND ending <= 0 AND beginning > 0
                 ORDER BY category, item_name''',
              (chat_id, latest))
    items = [{'category': r[0], 'item_name': r[1], 'option': r[2], 'ending': r[3]}
             for r in c.fetchall()]
    conn.close()
    return items

def get_utak_sales_top(chat_id: int, days: int = 7, limit: int = 30) -> list[dict]:
    """過去N日の売上トップ商品。"""
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    c.execute('''SELECT category, item_name, SUM(qty) as total_qty,
                        SUM(gross_price) as total_sales, COUNT(*) as txn_count
                 FROM utak_sales
                 WHERE chat_id=? AND sale_date>=?
                 GROUP BY category, item_name
                 ORDER BY total_qty DESC
                 LIMIT ?''', (chat_id, since, limit))
    items = [{'category': r[0], 'item_name': r[1], 'total_qty': r[2],
              'total_sales': r[3], 'txn_count': r[4]}
             for r in c.fetchall()]
    conn.close()
    return items

def get_utak_inventory_summary(chat_id: int) -> str:
    """最新UTAKインベントリのカテゴリ別サマリーをテキストで返す。"""
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT MAX(imported_at) FROM utak_inventory WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    if not row or not row[0]:
        conn.close()
        return "UTAK在庫データなし"
    latest = row[0]
    c.execute('''SELECT category,
                        COUNT(*) as items,
                        SUM(CASE WHEN ending > 0 THEN 1 ELSE 0 END) as in_stock,
                        SUM(CASE WHEN ending <= 0 AND beginning > 0 THEN 1 ELSE 0 END) as out_of_stock,
                        SUM(CASE WHEN ending > 0 AND ending <= 3 THEN 1 ELSE 0 END) as low_stock,
                        SUM(inv_value) as total_value
                 FROM utak_inventory
                 WHERE chat_id=? AND imported_at=?
                 GROUP BY category
                 ORDER BY total_value DESC''',
              (chat_id, latest))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return "UTAK在庫データなし"
    lines = [f"📦 UTAK在庫サマリー（{latest}）\n━━━━━━━━━━━━━━━━━━━"]
    total_items = 0
    total_low = 0
    total_out = 0
    total_val = 0
    for r in rows:
        cat, items, in_stock, out_of_stock, low_stock, val = r
        total_items += in_stock
        total_low += low_stock
        total_out += out_of_stock
        total_val += val or 0
        alerts = []
        if out_of_stock > 0:
            alerts.append(f"🔴{out_of_stock}品切")
        if low_stock > 0:
            alerts.append(f"🟡{low_stock}残少")
        alert_str = f" ({', '.join(alerts)})" if alerts else ""
        val_str = f"₱{val:,.0f}" if val else ""
        lines.append(f"【{cat}】{in_stock}品 {val_str}{alert_str}")
    lines.append(f"\n合計: {total_items}品在庫 | 🔴{total_out}品切 | 🟡{total_low}残少 | ₱{total_val:,.0f}")
    return "\n".join(lines)

def get_dead_stock(chat_id: int, days: int = 14) -> list[dict]:
    """在庫あり（ending > 0）だが過去N日間売れていない商品を検出。"""
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT MAX(imported_at) FROM utak_inventory WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    if not row or not row[0]:
        conn.close()
        return []
    latest = row[0]
    # 在庫ある商品
    c.execute('''SELECT category, item_name, option, ending, inv_value
                 FROM utak_inventory WHERE chat_id=? AND imported_at=? AND ending > 0''',
              (chat_id, latest))
    in_stock = {(r[0], r[1]): {'category': r[0], 'item_name': r[1], 'option': r[2],
                                'stock': r[3], 'inv_value': r[4]} for r in c.fetchall()}
    # 過去N日に売れた商品
    c.execute('''SELECT DISTINCT category, item_name FROM utak_sales
                 WHERE chat_id=? AND sale_date>=?''', (chat_id, since))
    sold = {(r[0], r[1]) for r in c.fetchall()}
    conn.close()
    dead = [v for k, v in in_stock.items() if k not in sold]
    dead.sort(key=lambda x: x.get('inv_value', 0) or 0, reverse=True)
    return dead

def get_online_vs_store_sales(chat_id: int, days: int = 7) -> dict:
    """GrabMart/GrabFood/FoodPanda vs 店舗売上を比較。"""
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    online_cats = ('GRABMART', 'GRABFOOD', 'FOODPANDA')
    c.execute(f'''SELECT category, SUM(qty) as total_qty, SUM(gross_price) as total_sales, COUNT(*) as txn_count
                 FROM utak_sales WHERE chat_id=? AND sale_date>=?
                 GROUP BY category''', (chat_id, since))
    rows = c.fetchall()
    conn.close()
    online = {'qty': 0, 'sales': 0, 'txn': 0, 'by_platform': {}}
    store = {'qty': 0, 'sales': 0, 'txn': 0}
    for cat, qty, sales, txn in rows:
        cat_upper = cat.upper().strip()
        # Check if category name starts with an online platform
        is_online = any(cat_upper.startswith(p) or cat_upper == p for p in online_cats)
        if is_online:
            platform = cat_upper.split()[0] if ' ' in cat_upper else cat_upper
            online['qty'] += qty or 0
            online['sales'] += sales or 0
            online['txn'] += txn or 0
            if platform not in online['by_platform']:
                online['by_platform'][platform] = {'qty': 0, 'sales': 0}
            online['by_platform'][platform]['qty'] += qty or 0
            online['by_platform'][platform]['sales'] += sales or 0
        else:
            store['qty'] += qty or 0
            store['sales'] += sales or 0
            store['txn'] += txn or 0
    return {'online': online, 'store': store, 'days': days}

def get_hourly_sales(chat_id: int, days: int = 7) -> list[dict]:
    """時間帯別の売上集計。"""
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    c.execute('''SELECT sale_time, category, item_name, SUM(qty) as total_qty, SUM(gross_price) as total_sales
                 FROM utak_sales WHERE chat_id=? AND sale_date>=? AND sale_time != ''
                 GROUP BY sale_time, category, item_name
                 ORDER BY total_qty DESC''', (chat_id, since))
    raw = c.fetchall()
    conn.close()
    # Parse time into hour buckets
    import re as _re
    hourly = {}  # hour -> {sales, qty, top_items}
    for time_str, cat, item, qty, sales in raw:
        # Parse "9:11am", "12:28pm" etc
        m = _re.match(r'(\d{1,2}):(\d{2})\s*(am|pm)', time_str.strip().lower())
        if not m:
            continue
        h = int(m.group(1))
        ampm = m.group(3)
        if ampm == 'pm' and h != 12:
            h += 12
        elif ampm == 'am' and h == 12:
            h = 0
        if h not in hourly:
            hourly[h] = {'hour': h, 'sales': 0, 'qty': 0, 'items': {}}
        hourly[h]['sales'] += sales or 0
        hourly[h]['qty'] += qty or 0
        key = item
        hourly[h]['items'][key] = hourly[h]['items'].get(key, 0) + (qty or 0)
    # Convert to list with top items
    result = []
    for h in sorted(hourly.keys()):
        data = hourly[h]
        top = sorted(data['items'].items(), key=lambda x: x[1], reverse=True)[:3]
        result.append({
            'hour': h,
            'label': f"{h}:00" if h >= 10 else f" {h}:00",
            'sales': data['sales'],
            'qty': data['qty'],
            'top_items': top,
        })
    return result

def get_frequently_bought_together(chat_id: int, days: int = 14, min_count: int = 3) -> list[dict]:
    """同じトランザクションで一緒に買われた商品ペアを検出。"""
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    # Get items per transaction
    c.execute('''SELECT transaction_id, item_name FROM utak_sales
                 WHERE chat_id=? AND sale_date>=? AND transaction_id != ''
                 ORDER BY transaction_id''', (chat_id, since))
    txn_items: dict[str, list[str]] = {}
    for txn_id, item in c.fetchall():
        txn_items.setdefault(txn_id, []).append(item)
    conn.close()
    # Count pairs
    from collections import Counter
    pair_count = Counter()
    for txn_id, items in txn_items.items():
        unique = list(set(items))
        if len(unique) < 2:
            continue
        for i in range(len(unique)):
            for j in range(i + 1, len(unique)):
                pair = tuple(sorted([unique[i], unique[j]]))
                pair_count[pair] += 1
    results = []
    for (a, b), count in pair_count.most_common(20):
        if count >= min_count:
            results.append({'item_a': a, 'item_b': b, 'count': count})
    return results

def get_utak_reorder_list(chat_id: int) -> list[dict]:
    """在庫 × 売上速度で仕入れ優先度リストを生成。各商品に days_until_stockout を計算。"""
    conn = get_conn()
    c = conn.cursor()
    # 最新在庫
    c.execute('SELECT MAX(imported_at) FROM utak_inventory WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    if not row or not row[0]:
        conn.close()
        return []
    latest = row[0]
    c.execute('''SELECT category, item_name, option, ending, inv_value
                 FROM utak_inventory WHERE chat_id=? AND imported_at=?''',
              (chat_id, latest))
    inv_map = {}
    for r in c.fetchall():
        key = (r[0], r[1])
        inv_map[key] = {'category': r[0], 'item_name': r[1], 'option': r[2],
                        'stock': r[3], 'inv_value': r[4]}
    # 過去14日の売上速度（1日あたり）
    since = (datetime.now(PHT) - timedelta(days=14)).strftime('%Y-%m-%d')
    c.execute('''SELECT category, item_name, SUM(qty) as total_qty, COUNT(DISTINCT sale_date) as days_sold
                 FROM utak_sales WHERE chat_id=? AND sale_date>=?
                 GROUP BY category, item_name''', (chat_id, since))
    sales_map = {}
    for r in c.fetchall():
        key = (r[0], r[1])
        total_qty = r[2]
        daily_rate = total_qty / 14.0
        sales_map[key] = {'total_qty': total_qty, 'daily_rate': daily_rate, 'days_sold': r[3]}
    conn.close()
    # Merge and calculate
    results = []
    for key, inv in inv_map.items():
        sale = sales_map.get(key, {'total_qty': 0, 'daily_rate': 0, 'days_sold': 0})
        stock = inv['stock']
        daily_rate = sale['daily_rate']
        if daily_rate > 0 and stock > 0:
            days_left = stock / daily_rate
        elif daily_rate > 0 and stock <= 0:
            days_left = 0  # already out
        else:
            days_left = 999  # not selling, no urgency
        # Priority: 🔴 urgent (<=2 days), 🟡 warning (<=7 days), 🟢 normal
        if days_left <= 2:
            priority = '🔴'
            priority_score = 0
        elif days_left <= 7:
            priority = '🟡'
            priority_score = 1
        elif stock <= 0 and sale['total_qty'] > 0:
            priority = '🔴'
            priority_score = 0
        else:
            priority = '🟢'
            priority_score = 2
        # Only include items that are actually selling and need reorder
        if sale['total_qty'] > 0 and (stock <= 0 or days_left <= 14):
            results.append({
                **inv,
                'total_sold_14d': sale['total_qty'],
                'daily_rate': daily_rate,
                'days_left': days_left,
                'priority': priority,
                'priority_score': priority_score,
            })
    results.sort(key=lambda x: (x['priority_score'], x['days_left']))
    return results

async def generate_utak_reorder_ai(chat_id: int) -> str:
    """UTAK在庫+売上データからAIで仕入れ提案を生成。"""
    loop = asyncio.get_event_loop()
    low_stock = get_utak_low_stock(chat_id, threshold=5)
    out_of_stock = get_utak_out_of_stock(chat_id)
    top_sellers = get_utak_sales_top(chat_id, days=7)
    if not low_stock and not out_of_stock and not top_sellers:
        return "UTAKデータが不足しています。まず在庫CSV・売上CSVをボットに送ってください。"
    # Build context
    ctx_parts = []
    if out_of_stock:
        lines = ["【在庫切れ商品】"]
        for it in out_of_stock[:30]:
            opt = f" ({it['option']})" if it.get('option') else ""
            lines.append(f"- {it['item_name']}{opt}（{it['category']}）")
        ctx_parts.append("\n".join(lines))
    if low_stock:
        lines = ["【在庫残少商品（5個以下）】"]
        for it in low_stock[:30]:
            opt = f" ({it['option']})" if it.get('option') else ""
            lines.append(f"- {it['item_name']}{opt}（{it['category']}）: 残{it['ending']:.0f}個")
        ctx_parts.append("\n".join(lines))
    if top_sellers:
        lines = ["【過去7日の売上トップ商品】"]
        for it in top_sellers[:20]:
            lines.append(f"- {it['item_name']}（{it['category']}）: {it['total_qty']:.0f}個売上, ₱{it['total_sales']:,.0f}")
        ctx_parts.append("\n".join(lines))
    data_text = "\n\n".join(ctx_parts)
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    try:
        resp = await asyncio.wait_for(
            client.messages.create(
                model="claude-opus-4-5",
                max_tokens=2000,
                system=(
                    "あなたは「みどりのマート」（フィリピンにある日本食品スーパー）の在庫管理アドバイザーです。\n"
                    "以下のUTAK POSデータを分析し、仕入れが必要な商品リストを作成してください。\n"
                    "優先度をつけて、理由も簡潔に説明してください。\n"
                    "- 🔴 緊急（在庫切れ＋売れ筋）\n"
                    "- 🟡 要注意（在庫残少＋売れ筋）\n"
                    "- 🟢 通常補充\n"
                    "日本語で回答してください。"
                ),
                messages=[{"role": "user", "content": data_text}],
            ),
            timeout=45,
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.error(f"UTAK reorder AI failed: {e}")
        return f"AI分析エラー: {e}"

def parse_weekday(text: str) -> int:
    t = text.lower().strip()
    for i, name in enumerate(WEEKDAY_NAMES_JA):
        if name in t:
            return i
    for i, name in enumerate(WEEKDAY_NAMES_EN):
        if name.lower() in t:
            return i
    short_ja = ['月', '火', '水', '木', '金', '土', '日']
    for i, ch in enumerate(short_ja):
        if ch in t and '曜' not in t:
            return i
    return -1

def is_supermarket_report(text: str) -> bool:
    t = text.lower()
    check_map = {
        'cash sale':      'cash sale' in t,
        'for deposit':    'for deposit' in t,
        'maya':           'maya' in t,
        'card/credit':    'card sale' in t or 'credit' in t,
        'previous sales': 'previous sales' in t,
        'morning':        'morning' in t or 'graveyard' in t or ' gy ' in t or t.startswith('gy'),
        'transaction':    'transaction' in t,
        'date today':     'date today' in t,
    }
    matched = sum(check_map.values())
    if matched < 4:
        logger.info(f"is_supermarket_report: {matched}/8 matched — {[k for k,v in check_map.items() if not v]} missing")
    return matched >= 4

def parse_report(text: str) -> dict:
    d = {}

    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    d['store'] = lines[0] if lines else 'Unknown Store'

    m = re.search(r'This is (\w+) from', text, re.IGNORECASE)
    d['submitted_by'] = m.group(1) if m else 'Staff'

    m = re.search(r'DATE TODAY\s*:?\s*(.+)', text, re.IGNORECASE)
    if m:
        raw_line = m.group(1).strip()
        # Extract just the date token — handles /, -, . separators and month names
        date_extract = re.search(
            r'(\d{1,2}[/\-.]\d{1,2}[/\-.]\d{4}'
            r'|\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2}'
            r'|\d{4}-\d{2}-\d{2}'
            r'|[A-Za-z]+\.?\s+\d{1,2},?\s*\d{4})',
            raw_line, re.IGNORECASE
        )
        raw_date = date_extract.group(1).strip() if date_extract else raw_line
        today = datetime.now()
        # Smart MM/DD vs DD/MM disambiguation for slash-separated dates:
        # when both interpretations are valid (e.g. 03/10/2026), pick the one
        # closest to today to avoid silently saving the wrong month.
        _slash = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', raw_date)
        if _slash:
            _a, _b, _yr = int(_slash.group(1)), int(_slash.group(2)), int(_slash.group(3))
            _candidates = []
            if 1 <= _a <= 12 and 1 <= _b <= 31:  # MM/DD/YYYY
                try:
                    _candidates.append(datetime(_yr, _a, _b))
                except ValueError:
                    pass
            if 1 <= _b <= 12 and 1 <= _a <= 31:  # DD/MM/YYYY
                try:
                    _dt = datetime(_yr, _b, _a)
                    if not _candidates or _dt != _candidates[0]:
                        _candidates.append(_dt)
                except ValueError:
                    pass
            if _candidates:
                _best = min(_candidates, key=lambda dt: abs((dt - today).days))
                d['date'] = _best.strftime('%Y-%m-%d')
                logger.info(f"Slash date disambiguated to {d['date']} (candidates: {[c.strftime('%Y-%m-%d') for c in _candidates]})")
        if 'date' not in d:
            for fmt in (
                '%m/%d/%Y', '%m/%d/%y',
                '%m-%d-%Y', '%m-%d-%y',
                '%m.%d.%Y',
                '%B %d, %Y', '%B %d %Y',
                '%B. %d, %Y', '%b %d, %Y', '%b %d %Y',
                '%d/%m/%Y', '%d/%m/%y',
                '%d-%m-%Y', '%d-%m-%y',
                '%d.%m.%Y',
                '%Y-%m-%d',
            ):
                try:
                    d['date'] = datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue
            else:
                d['date'] = raw_date
                logger.warning(f"Date parse failed: '{raw_date}' from line: '{raw_line}'")
        logger.info(f"Date extracted: '{d.get('date')}' | raw='{raw_date}' | line='{raw_line}'")
    else:
        d['date'] = datetime.now().strftime('%Y-%m-%d')

    d['previous_sales'] = _num(text, 'PREVIOUS SALES')
    d['cash_sale']   = _num(text, 'CASH SALE')
    d['card_sale']   = _num(text, 'CREDIT/CARD SALE') or _num(text, 'CREDIT CARD SALE') or _num(text, 'CARD SALE')
    d['qr_ph']       = _num(text, 'QR PH')
    d['maya']        = _num(text, 'MAYA')
    d['grab']        = _num(text, 'Grab')
    d['foodpanda']   = _num(text, 'Foodpanda')

    gv = re.search(r'(?:Grave\s*yard(?:\s*shift)?|GY(?:\s*shift)?)\s*:?\s*[₱]?\s*([\d,]+\.?\d*)', text, re.IGNORECASE)
    d['graveyard']   = float(gv.group(1).replace(',', '')) if gv else 0.0

    d['morning']     = _num(text, 'Morning shift')
    d['afternoon']   = _num(text, 'Afternoon Shift')
    d['discounts']   = _num(text, 'Discounts')
    d['wastage']     = _num(text, 'Wastage/Disposal')
    # 全体TOTAL：行頭から数スペース以内のTOTAL（カテゴリのダッシュ区切り小計と区別）
    m_total = re.search(r'^\s{0,6}TOTAL\s*:?\s*[₱Pp]?\s*([\d,]+\.?\d*)', text, re.MULTILINE | re.IGNORECASE)
    d['total'] = float(m_total.group(1).replace(',', '')) if m_total else 0.0

    # Monthly total: MONTHLY SALES or MARCH SALES etc.
    m = re.search(r'(?:MONTHLY|JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+SALES\s*:?\s*[₱]?\s*([\d,]+\.?\d*)', text, re.IGNORECASE)
    d['monthly_total'] = float(m.group(1).replace(',', '')) if m else 0.0

    # Cash in drawer: CASH DRAWER (ignore text in parentheses)
    tc = re.search(r'CASH DRAWER\s*:?\s*[₱]?\s*([\d,]+)', text, re.IGNORECASE)
    d['cash_drawer'] = float(tc.group(1).replace(',', '')) if tc else 0.0

    d['transaction_count'] = int(_num(text, 'Transaction count'))
    d['salary']        = _num(text, 'EMPLOYEE SALARY PER DAY')
    d['inventory']     = _num(text, 'INVENTORY SUPPLIES')
    d['other_expense'] = _num(text, 'OTHER EXPENSE')
    d['cashbox']       = _num(text, 'CASHBOX CASH')
    d['for_deposit']   = _num(text, 'FOR DEPOSIT')

    d['cat_instant_food']     = _cat_num(text, 'INSTANT FOOD')
    d['cat_seasoning']        = _cat_num(text, 'SEASONING')
    d['cat_grabmart']         = _cat_num(text, 'GRABMART')
    d['cat_frozen_item']      = _cat_num(text, 'FROZEN ITEM')
    d['cat_personal_care']    = _cat_num(text, 'PERSONAL CARE')
    d['cat_beverage']         = _cat_num(text, 'BEVERAGE')
    d['cat_snacks_candies']   = _cat_num(text, 'SNACKS & CANDIES')
    d['cat_chilled_item']     = _cat_num(text, 'CHILLED ITEM')
    d['cat_medicine']         = _cat_num(text, 'MEDICINE')
    d['cat_bento']            = _cat_num(text, 'BENTO')
    d['cat_rice_noodle_bread']= _cat_num(text, 'RICE NOODLE BREAD')
    d['cat_grabfood']         = _cat_num(text, 'GRABFOOD')
    d['cat_rte']              = _cat_num(text, 'RTE')
    d['cat_ice_cream']        = _cat_num(text, 'ICE CREAM')
    d['cat_bath_item']        = _cat_num(text, 'BATH ITEM')

    # コメント欄（COMENT / COMMENT どちらも対応）
    m_comment = re.search(r'CO[MN]{1,2}ENT\s*:?\s*(.+)', text, re.IGNORECASE)
    d['comment'] = m_comment.group(1).strip() if m_comment else ''

    return d

# ─── DB helpers ────────────────────────────────────────────
def save_record(data: dict, raw_text: str, chat_id: int):
    conn = get_conn()
    c = conn.cursor()
    # Normalize store name: if a record already exists for this (date, chat_id),
    # reuse the stored store name so ON CONFLICT(date, store, chat_id) triggers correctly
    c.execute('SELECT store FROM supermarket_sales WHERE date=? AND chat_id=?',
              (data['date'], chat_id))
    _existing_store = c.fetchone()
    if _existing_store:
        data['store'] = _existing_store[0]
    # Cross-chat deduplication for STORE_GROUP_IDS linked groups
    # 同じ日付のレコードが他のリンクグループに存在する場合は削除して新しい方で上書き
    ids = get_chat_ids(chat_id)
    if len(ids) > 1:
        placeholders_dup = ','.join('?' * len(ids))
        c.execute(
            f'SELECT chat_id FROM supermarket_sales WHERE date=? AND chat_id IN ({placeholders_dup}) AND chat_id != ?',
            (data['date'], *ids, chat_id)
        )
        dup = c.fetchone()
        if dup:
            c.execute('DELETE FROM supermarket_sales WHERE date=? AND chat_id=?',
                      (data['date'], dup[0]))
    # Detect overwrite: warn if a record for this date already exists
    c.execute('SELECT id FROM supermarket_sales WHERE date=? AND store=? AND chat_id=?',
              (data['date'], data['store'], chat_id))
    _existing = c.fetchone()
    if _existing:
        logger.warning(f"Overwriting existing record: {data.get('date')} / {data.get('store')} / chat={chat_id}")

    try:
        c.execute('''
            INSERT INTO supermarket_sales
            (date, store, submitted_by, cash_sale, card_sale, qr_ph, maya, grab,
             foodpanda, graveyard, morning, afternoon, discounts, wastage, total,
             monthly_total, cash_drawer, transaction_count, salary, inventory,
             other_expense, cashbox, for_deposit, comment,
             cat_instant_food, cat_seasoning, cat_grabmart, cat_frozen_item,
             cat_personal_care, cat_beverage, cat_snacks_candies, cat_chilled_item,
             cat_medicine, cat_bento, cat_rice_noodle_bread, cat_grabfood,
             cat_rte, cat_ice_cream, cat_bath_item,
             raw_text, chat_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, store, chat_id) DO UPDATE SET
                submitted_by=excluded.submitted_by,
                cash_sale=excluded.cash_sale,
                card_sale=excluded.card_sale,
                qr_ph=excluded.qr_ph,
                maya=excluded.maya,
                grab=excluded.grab,
                foodpanda=excluded.foodpanda,
                graveyard=excluded.graveyard,
                morning=excluded.morning,
                afternoon=excluded.afternoon,
                discounts=excluded.discounts,
                wastage=excluded.wastage,
                total=excluded.total,
                monthly_total=excluded.monthly_total,
                cash_drawer=excluded.cash_drawer,
                transaction_count=excluded.transaction_count,
                salary=excluded.salary,
                inventory=excluded.inventory,
                other_expense=excluded.other_expense,
                cashbox=excluded.cashbox,
                for_deposit=excluded.for_deposit,
                comment=excluded.comment,
                cat_instant_food=excluded.cat_instant_food,
                cat_seasoning=excluded.cat_seasoning,
                cat_grabmart=excluded.cat_grabmart,
                cat_frozen_item=excluded.cat_frozen_item,
                cat_personal_care=excluded.cat_personal_care,
                cat_beverage=excluded.cat_beverage,
                cat_snacks_candies=excluded.cat_snacks_candies,
                cat_chilled_item=excluded.cat_chilled_item,
                cat_medicine=excluded.cat_medicine,
                cat_bento=excluded.cat_bento,
                cat_rice_noodle_bread=excluded.cat_rice_noodle_bread,
                cat_grabfood=excluded.cat_grabfood,
                cat_rte=excluded.cat_rte,
                cat_ice_cream=excluded.cat_ice_cream,
                cat_bath_item=excluded.cat_bath_item,
                raw_text=excluded.raw_text,
                created_at=CURRENT_TIMESTAMP
        ''', (
            data['date'], data['store'], data['submitted_by'],
            data['cash_sale'], data['card_sale'], data['qr_ph'], data['maya'],
            data['grab'], data.get('foodpanda', 0), data['graveyard'],
            data['morning'], data['afternoon'], data['discounts'], data['wastage'],
            data['total'], data['monthly_total'], data.get('cash_drawer', 0),
            data['transaction_count'], data['salary'], data['inventory'],
            data['other_expense'], data['cashbox'], data['for_deposit'],
            data.get('comment', ''),
            data.get('cat_instant_food', 0), data.get('cat_seasoning', 0),
            data.get('cat_grabmart', 0), data.get('cat_frozen_item', 0),
            data.get('cat_personal_care', 0), data.get('cat_beverage', 0),
            data.get('cat_snacks_candies', 0), data.get('cat_chilled_item', 0),
            data.get('cat_medicine', 0), data.get('cat_bento', 0),
            data.get('cat_rice_noodle_bread', 0), data.get('cat_grabfood', 0),
            data.get('cat_rte', 0), data.get('cat_ice_cream', 0),
            data.get('cat_bath_item', 0),
            raw_text, chat_id
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info(f"Saved record: {data.get('date')} / {data.get('store')} / chat={chat_id}")

def get_previous(date: str, store: str, chat_id: int) -> Optional[dict]:
    conn = get_conn()
    c = conn.cursor()
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    c.execute(f'''
        SELECT * FROM supermarket_sales
        WHERE date < ? AND store = ? AND chat_id IN ({placeholders})
        ORDER BY date DESC LIMIT 1
    ''', (date, store, *ids))
    row = c.fetchone()
    col_names = [d[0] for d in c.description]
    conn.close()
    if not row:
        return None
    return dict(zip(col_names, row))

def get_chat_ids(chat_id: int) -> list:
    """Return all linked chat IDs, or just the given one if not configured."""
    if STORE_GROUP_IDS and chat_id in STORE_GROUP_IDS:
        return STORE_GROUP_IDS
    return [chat_id]

def get_records(chat_id: int, store: str = None, days: int = 30) -> list:
    conn = get_conn()
    c = conn.cursor()
    since = (datetime.now(PHT) - timedelta(days=days)).strftime('%Y-%m-%d')
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    if store:
        c.execute(f'''SELECT * FROM supermarket_sales
                     WHERE chat_id IN ({placeholders}) AND store=? AND date>=?
                     ORDER BY date ASC''', (*ids, store, since))
    else:
        c.execute(f'''SELECT * FROM supermarket_sales
                     WHERE chat_id IN ({placeholders}) AND date>=?
                     ORDER BY date ASC''', (*ids, since))
    rows = c.fetchall()
    col_names = [d[0] for d in c.description]
    conn.close()
    return [dict(zip(col_names, r)) for r in rows]

def get_last_week_records(chat_id: int):
    today = datetime.now(PHT)
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    start = last_monday.strftime('%Y-%m-%d')
    end   = last_sunday.strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    # 同じ日付が複数グループに保存されている場合は最新(id最大)を1件だけ取得
    c.execute(f'''SELECT * FROM supermarket_sales
                 WHERE id IN (
                     SELECT MAX(id) FROM supermarket_sales
                     WHERE chat_id IN ({placeholders}) AND date>=? AND date<=?
                     GROUP BY date
                 )
                 ORDER BY date ASC''', (*ids, start, end))
    rows = c.fetchall()
    col_names = [d[0] for d in c.description]
    conn.close()
    return [dict(zip(col_names, r)) for r in rows], start, end

def get_month_records(chat_id: int, year: int = None, month: int = None):
    today = datetime.now()
    y = year or today.year
    m = month or today.month
    start = f'{y:04d}-{m:02d}-01'
    last_day = calendar.monthrange(y, m)[1]
    end = min(f'{y:04d}-{m:02d}-{last_day:02d}', today.strftime('%Y-%m-%d'))
    conn = get_conn()
    c = conn.cursor()
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    # 同じ日付が複数グループに保存されている場合は最新(id最大)を1件だけ取得
    c.execute(f'''SELECT * FROM supermarket_sales
                 WHERE id IN (
                     SELECT MAX(id) FROM supermarket_sales
                     WHERE chat_id IN ({placeholders}) AND date>=? AND date<=?
                     GROUP BY date
                 )
                 ORDER BY date ASC''', (*ids, start, end))
    rows = c.fetchall()
    col_names = [d[0] for d in c.description]
    conn.close()
    return [dict(zip(col_names, r)) for r in rows], start, end

def get_staff_performance(chat_id: int, year: int, month: int) -> list:
    """Return per-staff stats for the given month, sorted by total sales desc."""
    records, _, _ = get_month_records(chat_id, year, month)
    stats: dict[str, dict] = {}
    for r in records:
        name = r.get('submitted_by') or 'Unknown'
        if name not in stats:
            stats[name] = {'reports': 0, 'total': 0.0, 'best': 0.0, 'best_date': ''}
        s = stats[name]
        s['reports'] += 1
        s['total']   += r['total']
        if r['total'] > s['best']:
            s['best']      = r['total']
            s['best_date'] = r['date']
    result = []
    for name, s in stats.items():
        result.append({
            'name':      name,
            'reports':   s['reports'],
            'total':     s['total'],
            'avg':       s['total'] / s['reports'] if s['reports'] > 0 else 0,
            'best':      s['best'],
            'best_date': s['best_date'],
        })
    return sorted(result, key=lambda x: x['total'], reverse=True)

def delete_record_db(date: str, store: str, chat_id: int) -> bool:
    conn = get_conn()
    c = conn.cursor()
    c.execute('DELETE FROM supermarket_sales WHERE date=? AND store=? AND chat_id=?',
              (date, store, chat_id))
    deleted = c.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def delete_latest_db(chat_id: int) -> Optional[dict]:
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT date, store FROM supermarket_sales WHERE chat_id=? ORDER BY created_at DESC LIMIT 1',
              (chat_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    date, store = row
    c.execute('DELETE FROM supermarket_sales WHERE date=? AND store=? AND chat_id=?',
              (date, store, chat_id))
    conn.commit()
    conn.close()
    return {'date': date, 'store': store}

def save_bot_message(chat_id: int, message_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute('INSERT INTO bot_messages (chat_id, message_id) VALUES (?, ?)', (chat_id, message_id))
    conn.commit()
    conn.close()

def get_bot_messages(chat_id: int, limit: int = None, hours: int = None) -> list:
    conn = get_conn()
    c = conn.cursor()
    if hours:
        since = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
        c.execute('SELECT message_id FROM bot_messages WHERE chat_id=? AND created_at >= ? ORDER BY created_at DESC', (chat_id, since))
    elif limit:
        c.execute('SELECT message_id FROM bot_messages WHERE chat_id=? ORDER BY created_at DESC LIMIT ?', (chat_id, limit))
    else:
        c.execute('SELECT message_id FROM bot_messages WHERE chat_id=? ORDER BY created_at DESC', (chat_id,))
    rows = [r[0] for r in c.fetchall()]
    conn.close()
    return rows

def delete_bot_message_db(chat_id: int, message_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute('DELETE FROM bot_messages WHERE chat_id=? AND message_id=?', (chat_id, message_id))
    conn.commit()
    conn.close()

def get_translate_mode(chat_id: int) -> bool:
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT translate FROM chat_settings WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    conn.close()
    return bool(row[0]) if row else False

def set_translate_mode(chat_id: int, enabled: bool):
    conn = get_conn()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO chat_settings (chat_id, translate) VALUES (?, ?)',
              (chat_id, 1 if enabled else 0))
    conn.commit()
    conn.close()

async def translate_text(text: str) -> str:
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    prompt = (
        "Translate the following text. "
        "If Japanese, translate to English. If English, translate to Japanese. "
        "Return translation only, no explanation.\n\nText: " + text
    )
    resp = await client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.content[0].text.strip()

async def ai_chat(text: str) -> str:
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    resp = await client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1000,
        system="あなたは「みどりのマート」のマネジメントです。店舗運営・売上・スタッフ管理などについて、マネージャーの立場で実践的にアドバイスしてください。ユーザーが書いた言語と同じ言語で回答し、簡潔に答えてください。",
        messages=[{"role": "user", "content": text}]
    )
    return resp.content[0].text.strip()


# ─── Procurement: Web search + AI recommendation ─────────
async def get_trend_fallback() -> list[dict]:
    """Brave API未設定時にClaudeの知識ベースでトレンド商品を返す。"""
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    now = datetime.now(PHT)
    try:
        resp = await asyncio.wait_for(
            client.messages.create(
                model="claude-opus-4-5",
                max_tokens=800,
                messages=[{"role": "user", "content": (
                    f"現在{now.year}年{now.month}月です。"
                    "今の日本でSNS（TikTok・Instagram・X）でバズっている食品・スナック・飲料・カップ麺・お菓子などを"
                    "具体的な商品名を挙げて教えてください。"
                    "また、ドン・キホーテで最近売れ筋の食品商品も教えてください。"
                    "さらに、フィリピン在住の日本食ファンに人気の日本食品・お菓子も教えてください。"
                    "各カテゴリ5件程度、商品名と簡単な説明を箇条書きで返してください。"
                )}]
            ),
            timeout=30,
        )
        text = resp.content[0].text.strip()
        return [{"category": "SNSバズ（AIフォールバック）", "title": "Claudeによるトレンド情報", "description": text, "url": ""}]
    except Exception as e:
        logger.error(f"Trend fallback failed: {e}")
        return []

async def search_trending_products() -> list[dict]:
    """Search for trending Japanese products using Brave Search API."""
    if not BRAVE_SEARCH_API_KEY:
        logger.info("BRAVE_SEARCH_API_KEY not set — using Claude fallback for trends")
        return await get_trend_fallback()
    queries = [
        ("日本スーパー", "ドンキホーテ おすすめ食品 スナック ランキング 2026"),
        ("ドンキホーテ", "ドンキホーテ 激安 食品 人気商品 お菓子 売れ筋"),
        ("SNSバズ", "TikTok Instagram バズり飯 食品 スナック お菓子 話題 2026"),
        ("フィリピン人気", "Japanese snacks Filipino favorites Philippines TikTok popular 2026"),
    ]
    results = []
    async with httpx.AsyncClient(timeout=15.0) as client:
        async def _search(category: str, query: str):
            try:
                resp = await client.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    params={"q": query, "count": 5},
                    headers={"Accept": "application/json", "Accept-Encoding": "gzip",
                             "X-Subscription-Token": BRAVE_SEARCH_API_KEY},
                )
                resp.raise_for_status()
                data = resp.json()
                for item in data.get("web", {}).get("results", [])[:5]:
                    results.append({
                        "category": category,
                        "title": item.get("title", ""),
                        "description": item.get("description", ""),
                        "url": item.get("url", ""),
                    })
            except Exception as e:
                logger.error(f"Brave search failed for '{category}': {e}")
        await asyncio.gather(*[_search(cat, q) for cat, q in queries])
    if not results:
        logger.info("Brave search returned no results — using Claude fallback")
        return await get_trend_fallback()
    return results

def get_category_sales_summary(chat_id: int, days: int = 30) -> str:
    """Aggregate category sales for the past N days and return a text summary."""
    records = get_records(chat_id, days=days)
    if not records:
        return "売上データなし"
    cat_cols = [k for k in records[0].keys() if k.startswith('cat_')]
    totals = {col: sum(r.get(col, 0) or 0 for r in records) for col in cat_cols}
    grand = sum(totals.values())
    half = len(records) // 2
    first_half = records[:half] if half > 0 else records
    second_half = records[half:] if half > 0 else records
    lines = [f"過去{days}日のカテゴリ別売上（{len(records)}日分）:"]
    sorted_cats = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    for col, total in sorted_cats:
        if total <= 0:
            continue
        pct = total / grand * 100 if grand > 0 else 0
        label = CAT_LABELS.get(col, col.replace('cat_', '').replace('_', ' ').title())
        first_sum = sum(r.get(col, 0) or 0 for r in first_half)
        second_sum = sum(r.get(col, 0) or 0 for r in second_half)
        if first_sum > 0:
            trend_pct = (second_sum - first_sum) / first_sum * 100
            trend = f"↑{trend_pct:.0f}%" if trend_pct > 5 else (f"↓{abs(trend_pct):.0f}%" if trend_pct < -5 else "→横ばい")
        else:
            trend = "—"
        lines.append(f"  {label}: ₱{total:,.0f} ({pct:.1f}%) {trend}")
    return "\n".join(lines)

async def generate_procurement_recommendation(chat_id: int, budget: float) -> str:
    """Generate AI-powered procurement recommendation using web trends + store data."""
    loop = asyncio.get_event_loop()
    search_results, sales_summary, order_history_text = await asyncio.gather(
        search_trending_products(),
        loop.run_in_executor(None, get_category_sales_summary, chat_id, 30),
        loop.run_in_executor(None, get_order_history_summary, chat_id, 3),
    )
    fixed_items = get_fixed_items(chat_id)
    # UTAK在庫データも取得
    utak_low = get_utak_low_stock(chat_id, threshold=5)
    utak_out = get_utak_out_of_stock(chat_id)
    search_text = ""
    if search_results:
        for cat in ["日本スーパー", "ドンキホーテ", "SNSバズ", "フィリピン人気"]:
            items = [r for r in search_results if r["category"] == cat]
            if items:
                search_text += f"\n【{cat}】\n"
                for r in items:
                    search_text += f"- {r['title']}: {r['description'][:150]}\n"
    else:
        search_text = "（Web検索結果なし — Brave Search APIキー未設定または検索失敗）"
    fixed_items_text = ""
    if fixed_items:
        fixed_items_text = "【固定アイテム（必ず提案に含めること）】\n"
        for fi in fixed_items:
            price_str = f"¥{fi['unit_price']:,}" if fi.get('unit_price') else "価格未設定"
            fixed_items_text += f"- {fi['item_name']}（{fi['category']}）: 最低{fi['min_qty']}個, {price_str}\n"
    now = datetime.now(PHT)
    system_prompt = (
        "あなたは「みどりのマート」（フィリピンにある日本食品スーパー）の仕入れアドバイザーです。\n"
        "日本から商品を仕入れてフィリピンで販売しています。\n"
        "以下の情報を基に、今週の仕入れ提案をJSON形式で作成してください。\n\n"
        "必ず以下のJSON形式のみを返してください（説明文不要）:\n"
        '{\n'
        '  "categories": [\n'
        '    {\n'
        '      "name": "カテゴリ名",\n'
        '      "budget": カテゴリ予算（円・整数）,\n'
        '      "reason": "このカテゴリの仕入れ理由（1文）",\n'
        '      "items": [\n'
        '        {"name": "商品名", "unit_price": 単価（円・整数）, "qty": 数量（整数）, "source": "定番" or "トレンド", "note": "補足（任意）"}\n'
        '      ]\n'
        '    }\n'
        '  ],\n'
        '  "summary": "全体コメント（季節性・トレンドを踏まえた総括）"\n'
        '}\n\n'
        "注意事項:\n"
        f"- 予算総額は ¥{budget:,.0f}（日本円）です。各カテゴリのbudgetの合計が予算総額以内になるようにしてください\n"
        f"- 各商品の unit_price × qty の合計がカテゴリbudget以内になるようにしてください\n"
        "- 日本で仕入れてフィリピンで販売する前提です\n"
        f"- 季節性（現在{now.month}月）を考慮してください\n"
        "- 5〜8カテゴリ、各カテゴリ2〜5商品程度\n"
        + ("- 固定アイテムは必ずいずれかのカテゴリに含め、指定の最低数量を守ってください\n" if fixed_items else "")
        + "- JSON以外のテキストは一切含めないでください"
    )
    # UTAK在庫状況テキスト
    utak_text = ""
    if utak_out:
        utak_text += "【UTAK POS: 在庫切れ商品（優先仕入れ）】\n"
        for it in utak_out[:15]:
            opt = f" ({it['option']})" if it.get('option') else ""
            utak_text += f"- {it['item_name']}{opt}（{it['category']}）\n"
        utak_text += "\n"
    if utak_low:
        utak_text += "【UTAK POS: 在庫残少商品（5個以下）】\n"
        for it in utak_low[:15]:
            opt = f" ({it['option']})" if it.get('option') else ""
            utak_text += f"- {it['item_name']}{opt}（{it['category']}）: 残{it['ending']:.0f}個\n"
        utak_text += "\n"
    user_msg = (
        f"【店舗売上データ（過去30日）】\n{sales_summary}\n\n"
        + (f"【過去の注文履歴】\n{order_history_text}\n\n" if order_history_text and order_history_text != "注文履歴なし" else "")
        + (f"{fixed_items_text}\n" if fixed_items_text else "")
        + (utak_text if utak_text else "")
        + f"【日本のトレンド商品情報（ウェブ検索結果）】\n{search_text}\n\n"
        f"上記を踏まえ、今週の仕入れ提案を ¥{budget:,.0f} の予算内でJSON形式で作成してください。"
    )
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
    try:
        resp = await asyncio.wait_for(
            client.messages.create(
                model="claude-opus-4-5",
                max_tokens=3000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_msg}],
            ),
            timeout=60,
        )
        raw = resp.content[0].text.strip()
        # Extract JSON from possible markdown code block
        if raw.startswith('```'):
            raw = re.sub(r'^```\w*\n?', '', raw)
            raw = re.sub(r'\n?```$', '', raw)
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"Procurement AI JSON parse failed: {e}\nRaw: {raw[:500]}")
        return None
    except Exception as e:
        logger.error(f"Procurement AI generation failed: {e}")
        return None

# ─── Procurement: proposal storage & approval flow ────────
# In-memory storage for pending proposals {chat_id: proposal_data}
_pending_proposals: dict[int, dict] = {}

def format_proposal_message(proposal: dict, chat_id: int) -> str:
    """Format a structured proposal into a readable Telegram message."""
    lines = []
    grand_total = 0
    for ci, cat in enumerate(proposal.get('categories', [])):
        cat_name = cat['name']
        cat_budget = cat.get('budget', 0)
        cat_reason = cat.get('reason', '')
        items = cat.get('items', [])
        cat_actual = sum(it.get('unit_price', 0) * it.get('qty', 0) for it in items)
        grand_total += cat_actual
        status = _pending_proposals.get(chat_id, {}).get('status', {})
        cat_status = status.get(ci, 'pending')
        icon = {'approved': '✅', 'rejected': '❌', 'pending': '⏳'}.get(cat_status, '⏳')
        lines.append(f"\n{icon} 【{cat_name}】予算: ¥{cat_budget:,} | 小計: ¥{cat_actual:,}")
        if cat_reason:
            lines.append(f"   {cat_reason}")
        for ii, item in enumerate(items):
            name = item['name']
            price = item.get('unit_price', 0)
            qty = item.get('qty', 0)
            total = price * qty
            source = item.get('source', '')
            note = item.get('note', '')
            src_icon = '🔄' if source == '定番' else '🆕'
            note_str = f" ({note})" if note else ""
            lines.append(f"   {src_icon} {name}: ¥{price:,} × {qty}個 = ¥{total:,}{note_str}")
    lines.append(f"\n━━━━━━━━━━━━━━━━━━━")
    lines.append(f"💰 合計: ¥{grand_total:,}")
    budget = _pending_proposals.get(chat_id, {}).get('budget', 0)
    if budget > 0:
        remaining = budget - grand_total
        lines.append(f"📊 予算: ¥{budget:,} | 残り: ¥{remaining:,}")
    summary = proposal.get('summary', '')
    if summary:
        lines.append(f"\n💡 {summary}")
    return "\n".join(lines)

def make_category_keyboard(proposal: dict, chat_id: int) -> InlineKeyboardMarkup:
    """Create inline keyboard with approve/reject buttons for each category."""
    buttons = []
    status = _pending_proposals.get(chat_id, {}).get('status', {})
    for ci, cat in enumerate(proposal.get('categories', [])):
        cat_status = status.get(ci, 'pending')
        cat_name = cat['name']
        if cat_status == 'pending':
            buttons.append([
                InlineKeyboardButton(f"✅ {cat_name}", callback_data=f"proc_approve_{ci}"),
                InlineKeyboardButton(f"❌", callback_data=f"proc_reject_{ci}"),
                InlineKeyboardButton(f"📝 数量変更", callback_data=f"proc_edit_{ci}"),
            ])
        elif cat_status == 'approved':
            buttons.append([
                InlineKeyboardButton(f"✅ {cat_name} (承認済)", callback_data=f"proc_undo_{ci}"),
            ])
        elif cat_status == 'rejected':
            buttons.append([
                InlineKeyboardButton(f"❌ {cat_name} (却下)", callback_data=f"proc_undo_{ci}"),
            ])
    # Bottom action buttons
    all_decided = all(status.get(i, 'pending') != 'pending' for i in range(len(proposal.get('categories', []))))
    if all_decided:
        buttons.append([InlineKeyboardButton("📋 確定して注文リスト出力", callback_data="proc_finalize")])
    buttons.append([InlineKeyboardButton("✅ 全て承認", callback_data="proc_approve_all"),
                    InlineKeyboardButton("🔄 リセット", callback_data="proc_reset")])
    return InlineKeyboardMarkup(buttons)

def make_item_keyboard(cat_index: int, items: list) -> InlineKeyboardMarkup:
    """Create inline keyboard for editing item quantities within a category."""
    buttons = []
    for ii, item in enumerate(items):
        name = item['name'][:12]
        qty = item.get('qty', 0)
        buttons.append([
            InlineKeyboardButton(f"➖", callback_data=f"proc_qty_{cat_index}_{ii}_dec"),
            InlineKeyboardButton(f"{name}: {qty}個", callback_data=f"proc_qty_{cat_index}_{ii}_info"),
            InlineKeyboardButton(f"➕", callback_data=f"proc_qty_{cat_index}_{ii}_inc"),
        ])
    buttons.append([InlineKeyboardButton("🔙 戻る", callback_data=f"proc_back_{cat_index}")])
    return InlineKeyboardMarkup(buttons)

async def handle_procurement_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle all procurement-related inline button callbacks."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = query.data

    if chat_id not in _pending_proposals:
        await query.edit_message_text("⚠️ 提案データが見つかりません。再度「仕入れ提案」を実行してください。")
        return

    prop = _pending_proposals[chat_id]
    proposal = prop['proposal']
    status = prop['status']

    if data.startswith('proc_approve_') and not data.startswith('proc_approve_all'):
        ci = int(data.split('_')[-1])
        status[ci] = 'approved'
    elif data.startswith('proc_reject_'):
        ci = int(data.split('_')[-1])
        status[ci] = 'rejected'
    elif data.startswith('proc_undo_'):
        ci = int(data.split('_')[-1])
        status[ci] = 'pending'
    elif data == 'proc_approve_all':
        for i in range(len(proposal.get('categories', []))):
            status[i] = 'approved'
    elif data == 'proc_reset':
        for i in range(len(proposal.get('categories', []))):
            status[i] = 'pending'
    elif data.startswith('proc_edit_'):
        ci = int(data.split('_')[-1])
        cat = proposal['categories'][ci]
        items = cat.get('items', [])
        cat_name = cat['name']
        item_lines = []
        for ii, item in enumerate(items):
            item_lines.append(f"{item['name']}: ¥{item.get('unit_price',0):,} × {item.get('qty',0)}個 = ¥{item.get('unit_price',0)*item.get('qty',0):,}")
        text = f"📝 【{cat_name}】数量変更\n━━━━━━━━━━━━━━━━━━━\n" + "\n".join(item_lines)
        keyboard = make_item_keyboard(ci, items)
        await query.edit_message_text(text=text, reply_markup=keyboard)
        return
    elif data.startswith('proc_qty_'):
        parts = data.split('_')
        ci = int(parts[2])
        ii = int(parts[3])
        action = parts[4]
        cat = proposal['categories'][ci]
        items = cat.get('items', [])
        if action == 'inc':
            items[ii]['qty'] = items[ii].get('qty', 0) + 1
        elif action == 'dec':
            items[ii]['qty'] = max(0, items[ii].get('qty', 0) - 1)
        elif action == 'info':
            return
        cat_name = cat['name']
        item_lines = []
        cat_total = 0
        for it in items:
            t = it.get('unit_price', 0) * it.get('qty', 0)
            cat_total += t
            item_lines.append(f"{it['name']}: ¥{it.get('unit_price',0):,} × {it.get('qty',0)}個 = ¥{t:,}")
        text = f"📝 【{cat_name}】数量変更  小計: ¥{cat_total:,}\n━━━━━━━━━━━━━━━━━━━\n" + "\n".join(item_lines)
        keyboard = make_item_keyboard(ci, items)
        await query.edit_message_text(text=text, reply_markup=keyboard)
        return
    elif data.startswith('proc_back_'):
        ci = int(data.split('_')[-1])
        # Return to main proposal view
        text = format_proposal_message(proposal, chat_id)
        keyboard = make_category_keyboard(proposal, chat_id)
        await query.edit_message_text(text=text, reply_markup=keyboard)
        return
    elif data == 'proc_finalize':
        # Generate final order list with only approved categories
        lines = ["📋 最終注文リスト\n━━━━━━━━━━━━━━━━━━━"]
        grand_total = 0
        approved_cats = []
        for ci, cat in enumerate(proposal.get('categories', [])):
            if status.get(ci) != 'approved':
                continue
            cat_name = cat['name']
            items = cat.get('items', [])
            cat_total = sum(it.get('unit_price', 0) * it.get('qty', 0) for it in items if it.get('qty', 0) > 0)
            grand_total += cat_total
            lines.append(f"\n【{cat_name}】¥{cat_total:,}")
            for item in items:
                if item.get('qty', 0) > 0:
                    t = item['unit_price'] * item['qty']
                    lines.append(f"  {item['name']}: {item['qty']}個 (¥{t:,})")
            approved_cats.append(cat)
        lines.append(f"\n━━━━━━━━━━━━━━━━━━━")
        lines.append(f"💰 注文合計: ¥{grand_total:,}")
        budget = prop.get('budget', 0)
        if budget > 0:
            lines.append(f"📊 予算: ¥{budget:,} | 残り: ¥{budget - grand_total:,}")
        confirmed_at = datetime.now(PHT).strftime('%Y-%m-%d %H:%M')
        lines.append(f"\n✅ 注文リスト確定 ({confirmed_at})")
        await query.edit_message_text(text="\n".join(lines))
        # 注文履歴をDBに保存
        save_order_history(chat_id, approved_cats)
        # 在庫に自動加算
        for cat in approved_cats:
            cat_name = cat.get('name', 'その他')
            for item in cat.get('items', []):
                if item.get('qty', 0) > 0:
                    add_inventory(chat_id, item['name'], cat_name, item['qty'])
        # CSV自動送信
        today_str = datetime.now(PHT).strftime('%Y%m%d')
        csv_buf = io.StringIO()
        csv_buf.write('\ufeff')  # UTF-8 BOM
        writer = csv.writer(csv_buf)
        writer.writerow(['注文日', 'カテゴリ', '商品名', '単価(¥)', '数量', '合計(¥)', '種別'])
        ordered_at = datetime.now(PHT).strftime('%Y-%m-%d')
        for cat in approved_cats:
            cat_name = cat.get('name', '')
            for item in cat.get('items', []):
                if item.get('qty', 0) > 0:
                    writer.writerow([
                        ordered_at, cat_name, item['name'],
                        item.get('unit_price', 0), item['qty'],
                        item.get('unit_price', 0) * item['qty'],
                        item.get('source', '定番')
                    ])
        csv_bytes = csv_buf.getvalue().encode('utf-8-sig')
        await query.message.reply_document(
            document=io.BytesIO(csv_bytes),
            filename=f"order_{today_str}.csv",
            caption=f"📋 注文CSV ({confirmed_at})"
        )
        del _pending_proposals[chat_id]
        return

    # Update main view
    text = format_proposal_message(proposal, chat_id)
    keyboard = make_category_keyboard(proposal, chat_id)
    await query.edit_message_text(text=text, reply_markup=keyboard)

# ─── Procurement: new command handlers ────────────────────────────────────────

async def cmd_order_history_csv(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """注文履歴CSVを送信するコマンド。"""
    chat_id = update.effective_chat.id
    records = get_order_history_records(chat_id, days=90)
    if not records:
        msg = await update.message.reply_text("📋 注文履歴がありません。")
        save_bot_message(chat_id, msg.message_id)
        return
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(['注文日', 'カテゴリ', '商品名', '単価(¥)', '数量', '合計(¥)', '種別'])
    for r in records:
        writer.writerow([r['ordered_at'], r['category'], r['item_name'],
                         r['unit_price'], r['qty'], r['total'], r.get('source', '定番')])
    csv_bytes = csv_buf.getvalue().encode('utf-8-sig')
    today_str = datetime.now(PHT).strftime('%Y%m%d')
    sent = await update.message.reply_document(
        document=io.BytesIO(csv_bytes),
        filename=f"order_history_{today_str}.csv",
        caption=f"📋 過去90日の注文履歴 ({len(records)}件)"
    )
    save_bot_message(chat_id, sent.message_id)

async def cmd_add_fixed_item(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    """固定アイテムを追加。例: 固定アイテム追加 カップ麺 20個 ¥150"""
    chat_id = update.effective_chat.id
    # Parse: 固定アイテム追加 <商品名> <数量>個 [¥<単価>] [<カテゴリ>]
    import re as _re
    m = _re.search(r'固定アイテム追加\s+(.+?)\s+(\d+)個?(?:\s+[¥￥](\d+))?(?:\s+(.+))?$', text)
    if not m:
        msg = await update.message.reply_text(
            "❌ 形式エラー。例:\n`固定アイテム追加 カップ麺 20個 ¥150`\n`固定アイテム追加 お茶 30個 ¥200 飲み物`",
            parse_mode='Markdown'
        )
        save_bot_message(chat_id, msg.message_id)
        return
    item_name = m.group(1).strip()
    min_qty = int(m.group(2))
    unit_price = int(m.group(3)) if m.group(3) else 0
    category = m.group(4).strip() if m.group(4) else 'その他'
    add_fixed_item(chat_id, item_name, min_qty, unit_price, category)
    price_str = f"¥{unit_price:,}" if unit_price else "価格未設定"
    msg = await update.message.reply_text(
        f"✅ 固定アイテム登録\n商品: {item_name}\nカテゴリ: {category}\n最低数量: {min_qty}個\n単価: {price_str}"
    )
    save_bot_message(chat_id, msg.message_id)

async def cmd_list_fixed_items(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """固定アイテム一覧を表示。"""
    chat_id = update.effective_chat.id
    items = get_fixed_items(chat_id)
    if not items:
        msg = await update.message.reply_text("📋 固定アイテムが登録されていません。\n「固定アイテム追加 商品名 数量個 ¥単価」で登録できます。")
        save_bot_message(chat_id, msg.message_id)
        return
    lines = ["📋 固定アイテム一覧\n━━━━━━━━━━━━━━━━━━━"]
    for fi in items:
        price_str = f"¥{fi['unit_price']:,}" if fi.get('unit_price') else "価格未設定"
        lines.append(f"• {fi['item_name']}（{fi['category']}）: 最低{fi['min_qty']}個 / {price_str}")
    lines.append(f"\n合計: {len(items)}件")
    msg = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, msg.message_id)

async def cmd_delete_fixed_item(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    """固定アイテムを削除。例: 固定アイテム削除 カップ麺"""
    chat_id = update.effective_chat.id
    import re as _re
    m = _re.search(r'固定アイテム削除\s+(.+)', text)
    if not m:
        msg = await update.message.reply_text("❌ 形式エラー。例: `固定アイテム削除 カップ麺`", parse_mode='Markdown')
        save_bot_message(chat_id, msg.message_id)
        return
    item_name = m.group(1).strip()
    deleted = delete_fixed_item(chat_id, item_name)
    if deleted:
        msg = await update.message.reply_text(f"✅ 「{item_name}」を固定アイテムから削除しました。")
    else:
        msg = await update.message.reply_text(f"❌ 「{item_name}」が見つかりません。「固定アイテム一覧」で確認してください。")
    save_bot_message(chat_id, msg.message_id)

async def cmd_inventory(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """在庫一覧を表示。"""
    chat_id = update.effective_chat.id
    items = get_inventory(chat_id)
    if not items:
        msg = await update.message.reply_text("📦 在庫データがありません。\n仕入れ確定時に自動で在庫が登録されます。")
        save_bot_message(chat_id, msg.message_id)
        return
    by_cat: dict = {}
    for it in items:
        cat = it.get('category', 'その他')
        by_cat.setdefault(cat, []).append(it)
    lines = ["📦 在庫一覧\n━━━━━━━━━━━━━━━━━━━"]
    for cat, cat_items in sorted(by_cat.items()):
        lines.append(f"\n【{cat}】")
        for it in cat_items:
            lines.append(f"  • {it['item_name']}: {it['qty']}個（更新: {it['updated_at'][:10]}）")
    msg = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, msg.message_id)

async def cmd_update_inventory(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    """在庫を手動更新。例: 在庫更新 カップ麺 -10"""
    chat_id = update.effective_chat.id
    import re as _re
    m = _re.search(r'在庫更新\s+(.+?)\s+([+-]?\d+)', text)
    if not m:
        msg = await update.message.reply_text("❌ 形式エラー。例:\n`在庫更新 カップ麺 -10`（10個売れた）\n`在庫更新 カップ麺 +5`（5個追加）", parse_mode='Markdown')
        save_bot_message(chat_id, msg.message_id)
        return
    item_name = m.group(1).strip()
    delta = int(m.group(2))
    add_inventory(chat_id, item_name, 'その他', delta)
    sign = "+" if delta >= 0 else ""
    msg = await update.message.reply_text(f"✅ 在庫更新: {item_name} {sign}{delta}個")
    save_bot_message(chat_id, msg.message_id)

# ─── Alerts ────────────────────────────────────────────────
def _has_graveyard_shift(date_str: str) -> bool:
    """金〜土（weekday 4-5）のみGraveyardシフトあり。日曜は8:00〜12:00のみ"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').weekday() in (4, 5)
    except Exception:
        return False  # 日付不明の場合はGYシフトなしとみなす

def _is_sunday(date_str: str) -> bool:
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').weekday() == 6
    except Exception:
        return False

def check_alerts(data: dict, prev: Optional[dict]) -> list:
    alerts = []
    total = data['total'] if data['total'] > 0 else 1
    has_gy = _has_graveyard_shift(data['date'])

    if prev and prev['total'] > 0:
        pct = (data['total'] - prev['total']) / prev['total'] * 100
        if pct <= -15:
            alerts.append(f"⚠️ 前日比{pct:+.1f}%：要因確認を推奨（天候/イベント影響？）")

        if prev.get('transaction_count', 0) > 0:
            tx_pct = (data['transaction_count'] - prev['transaction_count']) / prev['transaction_count'] * 100
            if tx_pct <= -20:
                alerts.append(f"👥 客数減{tx_pct:+.1f}%：プロモーション検討を推奨")

        # GYアラートは金〜日のみ（月〜木はGraveyardシフトなし）
        if has_gy and prev.get('graveyard', 0) > 0:
            g_pct = (data['graveyard'] - prev['graveyard']) / prev['graveyard'] * 100
            if g_pct <= -30:
                alerts.append(f"🌙 Graveyard売上急減{g_pct:+.1f}%：夜間需要の変動確認")

    if data['wastage'] / total * 100 > 3:
        alerts.append(f"⚠️ 廃棄率{data['wastage']/total*100:.1f}%：発注量見直しを検討")

    if data['cash_sale'] / total * 100 > 50:
        alerts.append(f"💵 現金比率{data['cash_sale']/total*100:.1f}%：集金タイミングの確認")

    return alerts

# ─── Claude comment ────────────────────────────────────────
async def generate_ai_comment(data: dict, prev: Optional[dict]) -> str:
    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
        total = data['total'] if data['total'] > 0 else 1
        shift_total = data['morning'] + data['afternoon'] + data['graveyard']
        comp = ""
        if prev and prev['total'] > 0:
            pct = (data['total'] - prev['total']) / prev['total'] * 100
            comp = f"前日比: {pct:+.1f}%"

        has_gy = _has_graveyard_shift(data['date'])
        is_sun = _is_sunday(data['date'])
        if has_gy:
            shift_note = "【シフト体制】月〜木: Morning・Afternoonの2シフトのみ。金〜土: Morning・Afternoon・Graveyardの3シフト。日曜: Morningのみ（8:00〜12:00）。※本日はGraveyardシフトあり（金〜土）。"
        elif is_sun:
            shift_note = "【シフト体制】月〜木: Morning・Afternoonの2シフトのみ。金〜土: Morning・Afternoon・Graveyardの3シフト。日曜: Morningのみ（8:00〜12:00）。※本日は日曜日のため、Morningシフトのみ（8:00〜12:00）。GY・Afternoon売上0は正常です。"
        else:
            shift_note = "【シフト体制】月〜木: Morning・Afternoonの2シフトのみ。金〜土: Morning・Afternoon・Graveyardの3シフト。日曜: Morningのみ（8:00〜12:00）。※本日はGraveyardシフトなし（月〜木）のため、GY売上0は正常です。"

        # トップカテゴリ（上位3件）
        cat_total = sum(data.get(k, 0) for _, k in CAT_LABELS)
        if cat_total > 0:
            top_cats = sorted(
                [(label, data.get(key, 0)) for label, key in CAT_LABELS if data.get(key, 0) > 0],
                key=lambda x: x[1], reverse=True
            )[:3]
            cat_note = "トップカテゴリ: " + ", ".join(f"{l} {v/cat_total*100:.0f}%" for l, v in top_cats)
        else:
            cat_note = ""

        prompt = f"""売上データを分析し、{data['submitted_by']}さんへの短いコメントを3点、日本語の箇条書きで生成してください。
ポジティブな点と改善提案を含めてください。コメントのみ返答してください。
{shift_note}

売上: ₱{total:,.0f} | 取引: {data['transaction_count']}件 | 平均: ₱{total/max(data['transaction_count'],1):,.0f}
現金比率: {data['cash_sale']/total*100:.1f}% | Grab: {data['grab']/total*100:.1f}% | 廃棄率: {data['wastage']/total*100:.1f}%
Morning: {data['morning']/shift_total*100 if shift_total>0 else 0:.1f}% | Afternoon: {data['afternoon']/shift_total*100 if shift_total>0 else 0:.1f}% | Graveyard: {data['graveyard']/shift_total*100 if shift_total>0 else 0:.1f}%
{comp}
{cat_note}"""

        resp = await client.messages.create(
            model="claude-opus-4-5",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.error(f"AI comment error: {e}")
        return "・本日もレポートありがとうございます！\n・データを確認しました。"

# ─── Category labels ───────────────────────────────────────
CAT_LABELS = [
    ('Instant Food',      'cat_instant_food'),
    ('Seasoning',         'cat_seasoning'),
    ('GrabMart',          'cat_grabmart'),
    ('Frozen Item',       'cat_frozen_item'),
    ('Personal Care',     'cat_personal_care'),
    ('Beverage',          'cat_beverage'),
    ('Snacks & Candies',  'cat_snacks_candies'),
    ('Chilled Item',      'cat_chilled_item'),
    ('Medicine',          'cat_medicine'),
    ('Bento',             'cat_bento'),
    ('Rice/Noodle/Bread', 'cat_rice_noodle_bread'),
    ('GrabFood',          'cat_grabfood'),
    ('RTE',               'cat_rte'),
    ('Ice Cream',         'cat_ice_cream'),
    ('Bath Item',         'cat_bath_item'),
]

# ─── Format daily report ───────────────────────────────────
def format_daily_report(data: dict, prev: Optional[dict], comments: str, alerts: list, daily_target: float = 0.0, monthly_target: float = 0.0, chat_id: int = 0) -> str:
    total = data['total'] if data['total'] > 0 else 1
    shift_total = data['morning'] + data['afternoon'] + data['graveyard']
    avg_tx = total / data['transaction_count'] if data['transaction_count'] > 0 else 0

    def pct(v): return v / total * 100
    def spct(v): return v / shift_total * 100 if shift_total > 0 else 0

    comp_line = ""
    if prev and prev['total'] > 0:
        diff = total - prev['total']
        p = diff / prev['total'] * 100
        comp_line = f"📊 前日比: {p:+.1f}% ({diff:+,.0f}₱)\n"

    alert_block = ""
    if alerts:
        alert_block = "\n🚨 アラート\n" + "\n".join(f"・{a}" for a in alerts) + "\n"

    cat_lines = ""
    cat_total = sum(data.get(k, 0) for _, k in CAT_LABELS)
    if cat_total > 0:
        cat_rows = "\n".join(
            f"  {label:<20} ₱{data.get(key, 0):>10,.0f} ({data.get(key, 0)/cat_total*100:.1f}%)"
            for label, key in CAT_LABELS
            if data.get(key, 0) > 0
        )
        cat_block = f"\n【カテゴリ別売上】\n{cat_rows}\n"
    else:
        cat_block = ""

    foodpanda_line = ""
    if data.get('foodpanda', 0) > 0:
        foodpanda_line = f"\n🐼 Foodpanda: ₱{data['foodpanda']:>10,.0f} ({pct(data['foodpanda']):.1f}%)"

    prev_line = ""
    if data.get('previous_sales', 0) > 0:
        prev_line = f"\n📊 前日売上: ₱{data['previous_sales']:,.0f}"

    monthly_line = ""
    _monthly_cum = data['monthly_total']
    if _monthly_cum <= 0 and monthly_target > 0:
        # Fallback: sum from DB for current month
        try:
            _y, _m = int(data['date'][:4]), int(data['date'][5:7])
            _recs, _, _ = get_month_records(chat_id, _y, _m)
            _monthly_cum = sum(r['total'] for r in _recs)
        except Exception:
            pass
    if _monthly_cum > 0:
        if monthly_target > 0:
            m_ach    = _monthly_cum / monthly_target * 100
            m_filled = min(int(m_ach // 10), 10)
            m_bar    = "🟩" * m_filled + "⬜" * (10 - m_filled)
            monthly_line = f"\n⭐️ 月間累計: ₱{_monthly_cum:,.0f}  🎯{m_ach:.1f}% {m_bar}"
        else:
            monthly_line = f"\n⭐️ 月間累計: ₱{_monthly_cum:,.0f}"

    target_line = ""
    if daily_target > 0:
        ach = data['total'] / daily_target * 100
        filled = min(int(ach // 10), 10)
        bar = "🟩" * filled + "⬜" * (10 - filled)
        target_line = f"\n🎯 日次目標達成率: {ach:.1f}% {bar}\n   ₱{data['total']:,.0f} / 目標 ₱{daily_target:,.0f}"

    report = f"""🏪 {data['store']} - 日次分析レポート{monthly_line}{prev_line}
📅 {data['date']}（{data['submitted_by']}さん提出）
━━━━━━━━━━━━━━━━━━━━━━
💰 売上総額: ₱{total:,.0f}
👥 取引件数: {data['transaction_count']}件（平均単価: ₱{avg_tx:,.0f}）

【決済内訳】
💵 現金:    ₱{data['cash_sale']:>10,.0f} ({pct(data['cash_sale']):.1f}%)
💳 カード:  ₱{data['card_sale']:>10,.0f} ({pct(data['card_sale']):.1f}%)
📱 QR PH:   ₱{data['qr_ph']:>10,.0f} ({pct(data['qr_ph']):.1f}%)
📱 MAYA:    ₱{data['maya']:>10,.0f} ({pct(data['maya']):.1f}%)
🚗 Grab:    ₱{data['grab']:>10,.0f} ({pct(data['grab']):.1f}%){foodpanda_line}

【シフト別】
🌅 Morning:   ₱{data['morning']:>10,.0f} ({spct(data['morning']):.1f}%)
🌆 Afternoon: ₱{data['afternoon']:>10,.0f} ({spct(data['afternoon']):.1f}%)
🌙 Graveyard: ₱{data['graveyard']:>10,.0f} ({spct(data['graveyard']):.1f}%)
━━━━━━━━━━━━━━━━━━━━━━
⚠️ 控除・損失
値引き: ₱{data['discounts']:,.0f}  |  廃棄: ₱{data['wastage']:,.0f} ({data['wastage']/total*100:.1f}%)

【経費】
人件費: ₱{data['salary']:,.0f} | 仕入: ₱{data['inventory']:,.0f} | その他: ₱{data['other_expense']:,.0f}
━━━━━━━━━━━━━━━━━━━━━━
{comp_line}💵 レジ現金: ₱{data.get('cash_drawer', 0):,.0f}
🏦 入金予定: ₱{data['for_deposit']:,.0f}
{alert_block}{cat_block}
💡 {data['submitted_by']}さんへのコメント
{comments}{target_line}""".strip()
    if data.get('comment'):
        report += f"\n\n✏️ {data['comment']}"
    return report

# ─── Chart generators ──────────────────────────────────────
def make_trend_chart(records: list, title: str = "Sales Trend") -> io.BytesIO:
    dates  = [r['date'] for r in records]
    totals = [r['total'] for r in records]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(len(dates)), totals, 'o-', color='#2E86AB', linewidth=2, markersize=5)
    ax.fill_between(range(len(dates)), totals, alpha=0.15, color='#2E86AB')
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha='right', fontsize=8)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    ax.set_ylabel('Sales (₱)', fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'₱{x:,.0f}'))
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close()
    return buf

def make_payment_chart(records: list) -> io.BytesIO:
    labels = ['Cash', 'Card', 'QR PH', 'MAYA', 'Grab', 'Foodpanda']
    totals = [
        sum(r['cash_sale'] for r in records),
        sum(r['card_sale'] for r in records),
        sum(r['qr_ph']     for r in records),
        sum(r['maya']      for r in records),
        sum(r['grab']      for r in records),
        sum(r.get('foodpanda', 0) for r in records),
    ]
    pairs = [(l, v) for l, v in zip(labels, totals) if v > 0]
    if not pairs:
        return io.BytesIO()
    labels, totals = zip(*pairs)
    colors = ['#F18F01', '#C73E1D', '#3B1F2B', '#44BBA4', '#E94F37', '#6A0572']
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(totals, labels=labels, colors=colors[:len(labels)],
           autopct='%1.1f%%', startangle=140,
           wedgeprops=dict(edgecolor='white', linewidth=1.5))
    ax.set_title('Payment Method Breakdown', fontsize=14, fontweight='bold', pad=16)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close()
    return buf

def make_shift_chart(records: list) -> io.BytesIO:
    dates     = [r['date']      for r in records]
    morning   = [r['morning']   for r in records]
    afternoon = [r['afternoon'] for r in records]
    grave     = [r['graveyard'] for r in records]
    x = np.arange(len(dates))
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x, morning,   0.6, label='Morning',   color='#FFBE0B')
    ax.bar(x, afternoon, 0.6, bottom=morning,    label='Afternoon', color='#FB5607')
    ax.bar(x, grave,     0.6,
           bottom=[m+a for m,a in zip(morning, afternoon)],
           label='Graveyard', color='#3A86FF')
    ax.set_xticks(x)
    ax.set_xticklabels(dates, rotation=45, ha='right', fontsize=8)
    ax.set_title('Sales by Shift (Stacked)', fontsize=14, fontweight='bold', pad=12)
    ax.set_ylabel('Sales (₱)', fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f'₱{v:,.0f}'))
    ax.legend(loc='upper left')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close()
    return buf

def make_category_chart(records: list) -> io.BytesIO:
    cat_data = [
        (lbl, sum(r.get(key, 0) for r in records))
        for lbl, key in CAT_LABELS
    ]
    cat_data = [(l, v) for l, v in cat_data if v > 0]
    if not cat_data:
        return io.BytesIO()
    cat_data.sort(key=lambda x: x[1])  # ascending for horizontal bar (smallest at top)
    labels = [l for l, _ in cat_data]
    values = [v for _, v in cat_data]
    total  = sum(values)
    height = max(4, len(labels) * 0.55)
    fig, ax = plt.subplots(figsize=(9, height))
    colors = plt.cm.RdYlGn(np.linspace(0.3, 0.9, len(labels)))  # type: ignore[attr-defined]
    bars = ax.barh(labels, values, color=colors)
    ax.set_title('Category Sales Breakdown', fontsize=13, fontweight='bold')
    ax.set_xlabel('Sales (₱)')
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'₱{x:,.0f}'))
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + total * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f'₱{val:,.0f} ({val/total*100:.1f}%)',
            va='center', fontsize=8
        )
    ax.margins(x=0.25)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

# ─── Commands ──────────────────────────────────────────────
async def _send_weekly_report(bot, chat_id: int, records: list, label: str = "今週（直近7日）"):
    if not records:
        sent = await bot.send_message(chat_id=chat_id, text=f"📭 {label}のデータがありません。")
        save_bot_message(chat_id, sent.message_id)
        return

    # Build the "previous week" comparison set from a broader 14-day window,
    # excluding dates that appear in the current records set.
    current_dates = {r['date'] for r in records}
    prev_records = get_records(chat_id, days=14)
    prev_week = [r for r in prev_records if r['date'] not in current_dates]

    # ── Basic totals ──
    n = len(records)
    total_sum   = sum(r['total']             for r in records)
    cash_sum    = sum(r['cash_sale']         for r in records)
    card_sum    = sum(r['card_sale']         for r in records)
    qr_sum      = sum(r['qr_ph']            for r in records)
    maya_sum    = sum(r['maya']              for r in records)
    grab_sum    = sum(r['grab']             for r in records)
    fp_sum      = sum(r.get('foodpanda', 0) for r in records)
    morning_s   = sum(r['morning']           for r in records)
    afternoon_s = sum(r['afternoon']         for r in records)
    grave_s     = sum(r['graveyard']         for r in records)
    wastage_s   = sum(r['wastage']           for r in records)
    discount_s  = sum(r['discounts']         for r in records)
    salary_s    = sum(r['salary']            for r in records)
    inventory_s = sum(r['inventory']         for r in records)
    other_s     = sum(r['other_expense']     for r in records)
    deposit_s   = sum(r['for_deposit']       for r in records)
    cashbox_s   = sum(r.get('cash_drawer',0) for r in records)
    tx_sum      = sum(r['transaction_count'] for r in records)

    prev_total    = sum(r['total']     for r in prev_week)
    prev_cash     = sum(r['cash_sale'] for r in prev_week)
    prev_grab     = sum(r['grab']      for r in prev_week)
    prev_card     = sum(r['card_sale'] for r in prev_week)
    prev_qr_maya  = sum(r['qr_ph'] + r['maya'] for r in prev_week)

    def pct(v, base): return v / base * 100 if base > 0 else 0
    def wow(curr, prev): return (curr - prev) / prev * 100 if prev > 0 else 0
    def trend(curr, prev_v, base_curr, base_prev):
        c = pct(curr, base_curr)
        p = pct(prev_v, base_prev)
        if base_prev == 0: return "─"
        return "↑" if c > p else ("↓" if c < p else "→")

    avg_tx = total_sum / tx_sum if tx_sum > 0 else 0
    wow_pct = wow(total_sum, prev_total)
    wow_str = f"{wow_pct:+.1f}%" if prev_total > 0 else "データなし"

    # ── Best/worst days ──
    best  = max(records, key=lambda r: r['total'])
    worst = min(records, key=lambda r: r['total'])

    # ── Day-of-week breakdown ──
    dow_map = {0:'月',1:'火',2:'水',3:'木',4:'金',5:'土',6:'日'}
    daily_rows = ""
    prev_total_day = 0
    for r in records:
        try:
            d = datetime.strptime(r['date'], '%Y-%m-%d')
            dow = dow_map[d.weekday()]
        except Exception:
            dow = "─"
        diff = r['total'] - prev_total_day
        diff_str = f"{diff:+,.0f}" if prev_total_day > 0 else "─"
        note = "🏆" if r['date'] == best['date'] else ("📉" if r['date'] == worst['date'] else "")
        daily_rows += f"  {dow} {r['date']}  ₱{r['total']:>10,.0f}  {diff_str:>10}  {note}\n"
        prev_total_day = r['total']

    # ── Shift analysis ──
    shift_total = morning_s + afternoon_s + grave_s
    shift_rows = ""
    for name, val in [('Morning', morning_s), ('Afternoon', afternoon_s), ('Graveyard', grave_s)]:
        shift_rows += f"  {name:<12} {n}日  ₱{val:>10,.0f}  {pct(val,shift_total):>5.1f}%  ₱{val/n:>8,.0f}/日\n"

    # ── Payment analysis ──
    pay_rows = ""
    prev_qr = sum(r['qr_ph'] for r in prev_week)
    prev_maya = sum(r['maya'] for r in prev_week)
    for name, val, prev_val in [
        ('現金',         cash_sum,          prev_cash),
        ('カード',       card_sum,          prev_card),
        ('QR PH',        qr_sum,            prev_qr),
        ('MAYA',         maya_sum,          prev_maya),
        ('Grab',         grab_sum,          prev_grab),
        ('Foodpanda',    fp_sum,            0),
    ]:
        if val == 0 and prev_val == 0: continue
        wow_pay = f"{wow(val, prev_val):+.1f}%" if prev_val > 0 else "─"
        tr = trend(val, prev_val, total_sum, prev_total)
        pay_rows += f"  {name:<12} ₱{val:>10,.0f}  {pct(val,total_sum):>5.1f}%  {wow_pay:>7}  {tr}\n"

    # ── Cost analysis ──
    total_cost = salary_s + inventory_s + discount_s + wastage_s + other_s
    gross_profit = total_sum - total_cost
    cost_rows = f"""  仕入れ      ₱{inventory_s:>10,.0f}
  人件費      ₱{salary_s:>10,.0f}
  値引き      ₱{discount_s:>10,.0f}  ({pct(discount_s,total_sum):.1f}%)
  廃棄・損失  ₱{wastage_s:>10,.0f}  ({pct(wastage_s,total_sum):.1f}%)
  その他      ₱{other_s:>10,.0f}
  ────────────────────────
  総経費      ₱{total_cost:>10,.0f}
  粗利        ₱{gross_profit:>10,.0f}  ({pct(gross_profit,total_sum):.1f}%)"""

    # ── KPI ──
    wast_pct  = pct(wastage_s, total_sum)
    disc_pct  = pct(discount_s, total_sum)
    cash_pct  = pct(cash_sum, total_sum)
    wast_eval = "✅" if wast_pct < 3 else "⚠️"
    disc_eval = "✅" if disc_pct < 2 else "⚠️"
    cash_eval = "✅" if cash_pct < 50 else "⚠️"

    # ── Weekday vs weekend ──
    # Fixed: weekday() < 5 means Mon-Fri (0-4); weekday >= 5 means Sat-Sun.
    # Original code used < 4 which incorrectly put Friday in the weekend bucket.
    weekday_recs = []
    weekend_recs = []
    for r in records:
        try:
            d = datetime.strptime(r['date'], '%Y-%m-%d')
            if d.weekday() < 5:
                weekday_recs.append(r)
            else:
                weekend_recs.append(r)
        except Exception:
            pass
    wd_avg = sum(r['total'] for r in weekday_recs) / len(weekday_recs) if weekday_recs else 0
    we_avg = sum(r['total'] for r in weekend_recs) / len(weekend_recs) if weekend_recs else 0

    # ── Category breakdown with WoW trend ──
    cat_weekly_data = [
        (label, sum(r.get(key, 0) for r in records), sum(r.get(key, 0) for r in prev_week))
        for label, key in CAT_LABELS
    ]
    cat_weekly_data = [(l, c, p) for l, c, p in cat_weekly_data if c > 0 or p > 0]
    cat_weekly_sum = sum(c for _, c, _ in cat_weekly_data)
    if cat_weekly_data:
        sorted_cats = sorted(cat_weekly_data, key=lambda x: x[1], reverse=True)
        cat_weekly_rows = "\n".join(
            f"  {label:<22} ₱{curr:>10,.0f}  ({pct(curr,cat_weekly_sum):>5.1f}%)  {f'{wow(curr,prev):+.1f}%' if prev > 0 else '─':>7}"
            for label, curr, prev in sorted_cats
        )
    else:
        cat_weekly_rows = "  (データなし / No data)"

    # ── Period ──
    start_date = records[0]['date']
    end_date   = records[-1]['date']
    store_name = records[0]['store']

    report = f"""📋 週次レポート - {label}
━━━━━━━━━━━━━━━━━━━━━━

【1. 基本情報】
📅 期間: {start_date} 〜 {end_date}
🏪 店舗: {store_name}
📆 営業日数: {n}日
🤖 提出者: 売上分析ボット

━━━━━━━━━━━━━━━━━━━━━━
【2. 売上サマリー】
💰 週間総売上:  ₱{total_sum:,.0f}
📊 前週比:      {wow_str}
👥 総取引件数:  {tx_sum}件
💡 平均客単価:  ₱{avg_tx:,.0f}

━━━━━━━━━━━━━━━━━━━━━━
【3. 日別売上推移】
  曜日 日付         総売上       前日比
{daily_rows}
  🏆 最高: {best['date']} ₱{best['total']:,.0f}
  📉 最低: {worst['date']} ₱{worst['total']:,.0f}

━━━━━━━━━━━━━━━━━━━━━━
【4. シフト別分析】
  シフト       日数  売上合計        比率    平均/日
{shift_rows}
━━━━━━━━━━━━━━━━━━━━━━
【5. 決済方法別分析】
  方法         週間合計        比率   前週比  トレンド
{pay_rows}
━━━━━━━━━━━━━━━━━━━━━━
【6. 原価・経費分析】
{cost_rows}

━━━━━━━━━━━━━━━━━━━━━━
【7. 現金管理】
💵 週間現金売上:  ₱{cash_sum:,.0f}
🏦 週間預金予定:  ₱{deposit_s:,.0f}
🗄️ レジ残高合計:  ₱{cashbox_s:,.0f}

━━━━━━━━━━━━━━━━━━━━━━
【8. KPI】
  週間売上    ₱{total_sum:,.0f}
  平均客単価  ₱{avg_tx:,.0f}
  廃棄率      {wast_pct:.1f}%  {wast_eval}
  値引き率    {disc_pct:.1f}%  {disc_eval}
  現金比率    {cash_pct:.1f}%  {cash_eval}

━━━━━━━━━━━━━━━━━━━━━━
【9. 曜日別パターン分析】
  月〜金平均: ₱{wd_avg:,.0f}
  土〜日平均: ₱{we_avg:,.0f}
  {'週末の方が高い📈' if we_avg > wd_avg else '平日の方が高い📊'}

━━━━━━━━━━━━━━━━━━━━━━
【10. カテゴリ別売上】
  カテゴリ               週間合計        比率     前週比
{cat_weekly_rows}"""

    # ── Weekly target comparison ──
    weekly_target = get_target_any(chat_id, 'weekly')
    if weekly_target > 0:
        w_ach = total_sum / weekly_target * 100
        w_filled = min(int(w_ach // 10), 10)
        w_bar = "🟩" * w_filled + "⬜" * (10 - w_filled)
        report += f"\n\n🎯 週次目標達成率: {w_ach:.1f}% {w_bar}\n   ₱{total_sum:,.0f} / 目標 ₱{weekly_target:,.0f}"

    sent = await bot.send_message(chat_id=chat_id, text=report)
    save_bot_message(chat_id, sent.message_id)

    # ── English version (sections 1-10) ──
    eng_report = f"""📋 Weekly Report - {label}
━━━━━━━━━━━━━━━━━━━━━━

[1. Basic Info]
📅 Period: {start_date} - {end_date}
🏪 Store: {store_name}
📆 Operating Days: {n} days
🤖 Submitted by: Sales Analysis Bot

━━━━━━━━━━━━━━━━━━━━━━
[2. Sales Summary]
💰 Weekly Total:   ₱{total_sum:,.0f}
📊 vs Last Week:   {wow_str}
👥 Total Transactions: {tx_sum}
💡 Avg Spend/Customer: ₱{avg_tx:,.0f}

━━━━━━━━━━━━━━━━━━━━━━
[3. Daily Sales]
  Day  Date          Total         vs Prev
{daily_rows}
  🏆 Best:  {best['date']} ₱{best['total']:,.0f}
  📉 Worst: {worst['date']} ₱{worst['total']:,.0f}

━━━━━━━━━━━━━━━━━━━━━━
[4. Shift Analysis]
  Shift        Days  Total          Share   Avg/Day
{shift_rows}
━━━━━━━━━━━━━━━━━━━━━━
[5. Payment Method Analysis]
  Method        Weekly Total    Share   WoW     Trend
{pay_rows}
━━━━━━━━━━━━━━━━━━━━━━
[6. Cost & Expense Analysis]
  Inventory    ₱{inventory_s:>10,.0f}
  Labor        ₱{salary_s:>10,.0f}
  Discounts    ₱{discount_s:>10,.0f}  ({pct(discount_s,total_sum):.1f}%)
  Wastage      ₱{wastage_s:>10,.0f}  ({pct(wastage_s,total_sum):.1f}%)
  Other        ₱{other_s:>10,.0f}
  ──────────────────────────
  Total Cost   ₱{total_cost:>10,.0f}
  Gross Profit ₱{gross_profit:>10,.0f}  ({pct(gross_profit,total_sum):.1f}%)

━━━━━━━━━━━━━━━━━━━━━━
[7. Cash Management]
💵 Weekly Cash Sales:    ₱{cash_sum:,.0f}
🏦 Weekly Deposit Plan:  ₱{deposit_s:,.0f}
🗄️ Register Balance:     ₱{cashbox_s:,.0f}

━━━━━━━━━━━━━━━━━━━━━━
[8. KPIs]
  Weekly Sales       ₱{total_sum:,.0f}
  Avg Spend          ₱{avg_tx:,.0f}
  Wastage Rate       {wast_pct:.1f}%  {wast_eval}
  Discount Rate      {disc_pct:.1f}%  {disc_eval}
  Cash Ratio         {cash_pct:.1f}%  {cash_eval}

━━━━━━━━━━━━━━━━━━━━━━
[9. Weekday Pattern Analysis]
  Mon-Fri Avg: ₱{wd_avg:,.0f}
  Sat-Sun Avg: ₱{we_avg:,.0f}
  {'Weekends outperform weekdays 📈' if we_avg > wd_avg else 'Weekdays outperform weekends 📊'}

━━━━━━━━━━━━━━━━━━━━━━
[10. Category Breakdown]
  Category              Weekly Total    Share    WoW
{cat_weekly_rows}"""

    if weekly_target > 0:
        eng_report += f"\n\n🎯 Weekly Target Achievement: {w_ach:.1f}% {w_bar}\n   ₱{total_sum:,.0f} / Target ₱{weekly_target:,.0f}"

    sent_en = await bot.send_message(chat_id=chat_id, text=eng_report)
    save_bot_message(chat_id, sent_en.message_id)

    # ── AI action items ──
    try:
        prompt = f"""以下の週次売上データを分析し、来週のアクション項目を優先度付きで3〜5点、日本語で生成してください。
各項目を「【優先度: 高/中/低】アクション内容（期限の目安）」の形式で出力してください。アクション項目のみ返答してください。

週間売上: ₱{total_sum:,.0f} | 前週比: {wow_str} | 平均客単価: ₱{avg_tx:,.0f}
廃棄率: {wast_pct:.1f}% | 値引き率: {disc_pct:.1f}% | 現金比率: {cash_pct:.1f}%
最高売上日: {best['date']} ₱{best['total']:,.0f} | 最低売上日: {worst['date']} ₱{worst['total']:,.0f}
粗利: ₱{gross_profit:,.0f} ({pct(gross_profit,total_sum):.1f}%)"""
        prompt_en = f"""Analyze the following weekly sales data and generate 3-5 prioritized action items for next week.
Format each as: [Priority: High/Medium/Low] Action item (deadline suggestion)
Return action items only.

Weekly Sales: ₱{total_sum:,.0f} | WoW: {wow_str} | Avg Spend: ₱{avg_tx:,.0f}
Wastage Rate: {wast_pct:.1f}% | Discount Rate: {disc_pct:.1f}% | Cash Ratio: {cash_pct:.1f}%
Best Day: {best['date']} ₱{best['total']:,.0f} | Worst Day: {worst['date']} ₱{worst['total']:,.0f}
Gross Profit: ₱{gross_profit:,.0f} ({pct(gross_profit,total_sum):.1f}%)"""

        async def _call_ai():
            _client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
            r_jp = await _client.messages.create(model="claude-opus-4-5", max_tokens=500,
                                                  messages=[{"role": "user", "content": prompt}])
            r_en = await _client.messages.create(model="claude-opus-4-5", max_tokens=500,
                                                  messages=[{"role": "user", "content": prompt_en}])
            return r_jp.content[0].text.strip(), r_en.content[0].text.strip()

        text_jp, text_en = await asyncio.wait_for(
            _call_ai(), timeout=60.0
        )
        sent2 = await bot.send_message(chat_id=chat_id, text=f"【11. 来週のアクション項目】\n{text_jp}")
        save_bot_message(chat_id, sent2.message_id)
        sent2_en = await bot.send_message(chat_id=chat_id, text=f"[11. Action Items for Next Week]\n{text_en}")
        save_bot_message(chat_id, sent2_en.message_id)
    except asyncio.TimeoutError:
        logger.warning("Weekly AI action items timed out after 60s")
    except Exception as e:
        logger.error(f"Weekly AI error: {e}")

    # ── Charts ──
    buf1 = make_trend_chart(records, "Weekly Sales Trend")
    m1 = await bot.send_photo(chat_id=chat_id, photo=buf1, caption="【12a】日別売上推移")
    save_bot_message(chat_id, m1.message_id)

    buf2 = make_shift_chart(records)
    m2 = await bot.send_photo(chat_id=chat_id, photo=buf2, caption="【12b】シフト別売上構成")
    save_bot_message(chat_id, m2.message_id)

    buf3 = make_payment_chart(records)
    if buf3.getbuffer().nbytes > 0:
        m3 = await bot.send_photo(chat_id=chat_id, photo=buf3, caption="【12c】決済方法別比率")
        save_bot_message(chat_id, m3.message_id)

    # Category breakdown chart
    buf_cat = make_category_chart(records)
    if buf_cat.getbuffer().nbytes > 0:
        m_cat = await bot.send_photo(chat_id=chat_id, photo=buf_cat, caption="【12d】カテゴリ別売上構成")
        save_bot_message(chat_id, m_cat.message_id)

    # Weekday avg bar chart
    try:
        dow_labels = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
        dow_avgs = []
        for i in range(7):
            day_recs = [r for r in records if datetime.strptime(r['date'],'%Y-%m-%d').weekday() == i]
            dow_avgs.append(sum(r['total'] for r in day_recs) / len(day_recs) if day_recs else 0)
        fig, ax = plt.subplots(figsize=(8, 4))
        max_avg = max(dow_avgs) if any(dow_avgs) else 0
        colors = ['#4CAF50' if v == max_avg else '#2196F3' for v in dow_avgs]
        ax.bar(dow_labels, dow_avgs, color=colors)
        ax.set_title('Avg Sales by Day of Week', fontsize=13, fontweight='bold')
        ax.set_ylabel('Sales (₱)')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f'₱{x:,.0f}'))
        ax.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        buf4 = io.BytesIO()
        plt.savefig(buf4, format='png', dpi=150)
        buf4.seek(0)
        plt.close()
        m4 = await bot.send_photo(chat_id=chat_id, photo=buf4, caption="【12e】曜日別平均売上")
        save_bot_message(chat_id, m4.message_id)
    except Exception as e:
        logger.error(f"Weekday chart error: {e}")


async def cmd_weekly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lookup_chat = STORE_GROUP_IDS[0] if STORE_GROUP_IDS else chat_id
    records, start, end = get_last_week_records(lookup_chat)
    await _send_weekly_report(ctx.bot, chat_id, records, label=f"先週（{start} 〜 {end}）")


async def cmd_monthly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    today = datetime.now()
    records, month_start, month_end = get_month_records(chat_id)
    month_label = today.strftime('%Y年%m月')
    if not records:
        sent = await update.message.reply_text(f"📭 {month_label}のデータがまだありません。")
        save_bot_message(chat_id, sent.message_id)
        return
    total_sum      = sum(r['total'] for r in records)
    n              = len(records)
    days_elapsed   = today.day
    days_in_month  = calendar.monthrange(today.year, today.month)[1]
    days_remaining = days_in_month - days_elapsed
    projected      = (total_sum / days_elapsed * days_in_month) if days_elapsed > 0 else 0
    monthly_target = get_target_any(chat_id, 'monthly')
    if monthly_target > 0:
        m_ach    = total_sum / monthly_target * 100
        m_filled = min(int(m_ach // 10), 10)
        m_bar    = "🟩" * m_filled + "⬜" * (10 - m_filled)
        needed   = max(monthly_target - total_sum, 0)
        target_block = (
            f"\n\n🎯 月間目標達成率: {m_ach:.1f}% {m_bar}"
            f"\n   ₱{total_sum:,.0f} / 目標 ₱{monthly_target:,.0f}"
            f"\n   残り {days_remaining}日で ₱{needed:,.0f} 必要"
        )
    else:
        target_block = ""
    # Category breakdown
    cat_monthly = [
        (label, sum(r.get(key, 0) for r in records))
        for label, key in CAT_LABELS
    ]
    cat_monthly = sorted([(l, v) for l, v in cat_monthly if v > 0], key=lambda x: x[1], reverse=True)
    cat_sum = sum(v for _, v in cat_monthly)
    if cat_monthly:
        cat_rows = "\n".join(
            f"  {label:<22} ₱{val:>10,.0f}  ({val/cat_sum*100:.1f}%)"
            for label, val in cat_monthly
        )
        cat_block = f"\n\n【カテゴリ別売上】\n{cat_rows}"
    else:
        cat_block = ""
    text = f"""📅 月次レポート - {month_label}（{month_start} 〜 {month_end}）
━━━━━━━━━━━━━━━━━━━
💰 月間売上合計: ₱{total_sum:,.0f}
📊 日平均: ₱{total_sum/n:,.0f}
📆 営業日数: {n}日 / {days_elapsed}日経過
📈 月末予測: ₱{projected:,.0f}{target_block}{cat_block}"""
    s1 = await update.message.reply_text(text)
    save_bot_message(chat_id, s1.message_id)
    s2 = await update.message.reply_photo(photo=make_trend_chart(records, f"Monthly Sales Trend ({month_label})"), caption="📈 Sales Trend")
    save_bot_message(chat_id, s2.message_id)
    s3 = await update.message.reply_photo(photo=make_shift_chart(records), caption="📊 Sales by Shift")
    save_bot_message(chat_id, s3.message_id)
    buf_cat = make_category_chart(records)
    if buf_cat.getbuffer().nbytes > 0:
        s4 = await update.message.reply_photo(photo=buf_cat, caption="🗂️ Category Breakdown")
        save_bot_message(chat_id, s4.message_id)

async def cmd_compare(update: Update, ctx: ContextTypes.DEFAULT_TYPE, mode: str = 'payment'):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=30)
    if not records:
        sent = await update.message.reply_text("📭 データがまだありません。")
        save_bot_message(chat_id, sent.message_id)
        return
    if mode == 'shift':
        s = await update.message.reply_photo(photo=make_shift_chart(records), caption="📊 Shift Comparison (Last 30 days)")
    else:
        s = await update.message.reply_photo(photo=make_payment_chart(records), caption="💳 Payment Comparison (Last 30 days)")
    save_bot_message(chat_id, s.message_id)

async def cmd_trend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=30)
    if not records:
        sent = await update.message.reply_text("📭 データがまだありません。")
        save_bot_message(chat_id, sent.message_id)
        return
    first_half  = records[:len(records)//2]
    second_half = records[len(records)//2:]
    avg1 = sum(r['total'] for r in first_half)  / max(len(first_half), 1)
    avg2 = sum(r['total'] for r in second_half) / max(len(second_half), 1)
    trend_pct = (avg2 - avg1) / avg1 * 100 if avg1 > 0 else 0
    emoji = "📈" if trend_pct >= 0 else "📉"
    text = f"""{emoji} トレンド分析（過去30日）
━━━━━━━━━━━━━━━━━━━
前半平均: ₱{avg1:,.0f}
後半平均: ₱{avg2:,.0f}
変化率: {trend_pct:+.1f}%

最高売上: ₱{max(r['total'] for r in records):,.0f}（{max(records, key=lambda r: r['total'])['date']}）
最低売上: ₱{min(r['total'] for r in records):,.0f}（{min(records, key=lambda r: r['total'])['date']}）"""
    s1 = await update.message.reply_text(text)
    save_bot_message(chat_id, s1.message_id)
    s2 = await update.message.reply_photo(photo=make_trend_chart(records, "30-Day Trend"))
    save_bot_message(chat_id, s2.message_id)

async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=90)
    if not records:
        await update.message.reply_text("📭 エクスポートできるデータがありません。")
        return
    fields = ['date','store','submitted_by','cash_sale','card_sale','qr_ph',
              'maya','grab','foodpanda','graveyard','morning','afternoon',
              'discounts','wastage','total','monthly_total','cash_drawer',
              'transaction_count','salary','inventory','other_expense','cashbox','for_deposit',
              'cat_instant_food','cat_seasoning','cat_grabmart','cat_frozen_item',
              'cat_personal_care','cat_beverage','cat_snacks_candies','cat_chilled_item',
              'cat_medicine','cat_bento','cat_rice_noodle_bread','cat_grabfood',
              'cat_rte','cat_ice_cream','cat_bath_item']
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(records)
    output.seek(0)
    filename = f"sales_{datetime.now().strftime('%Y%m%d')}.csv"
    sent = await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode('utf-8-sig')),
        filename=filename,
        caption=f"📊 Sales CSV（直近90日 / {len(records)}件）"
    )
    save_bot_message(chat_id, sent.message_id)


async def cmd_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    date_match = re.search(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})', text)
    if date_match:
        raw   = date_match.group(1).replace('/', '-')
        parts = raw.split('-')
        date_str = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        conn = get_conn()
        c = conn.cursor()
        c.execute('SELECT store FROM supermarket_sales WHERE date=? AND chat_id=? LIMIT 1',
                  (date_str, chat_id))
        row = c.fetchone()
        conn.close()
        if not row:
            await update.message.reply_text(f"⚠️ {date_str} のレポートは見つかりませんでした。")
            return
        if delete_record_db(date_str, row[0], chat_id):
            await update.message.reply_text(f"🗑️ {row[0]} の {date_str} を削除しました。")
        else:
            await update.message.reply_text("⚠️ 削除に失敗しました。")
    else:
        result = delete_latest_db(chat_id)
        if result:
            await update.message.reply_text(f"🗑️ 最新レポートを削除しました。\n📅 {result['date']} / {result['store']}")
        else:
            await update.message.reply_text("⚠️ 削除できるレポートが見つかりませんでした。")

async def cmd_delete_bot_messages(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    t = text.lower()

    # Determine how many to delete
    if '全部' in t or 'all' in t:
        message_ids = get_bot_messages(chat_id)
    elif '24時間' in t or '今日' in t:
        message_ids = get_bot_messages(chat_id, hours=24)
    else:
        m = re.search(r'(\d+)\s*件', text)
        if m:
            message_ids = get_bot_messages(chat_id, limit=int(m.group(1)))
        else:
            message_ids = get_bot_messages(chat_id, hours=24)  # デフォルト：24時間以内

    if not message_ids:
        await update.message.reply_text("🤖 削除できるメッセージがありません。")
        return

    deleted = 0
    failed = 0
    for mid in message_ids:
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=mid)
            delete_bot_message_db(chat_id, mid)
            deleted += 1
        except Exception:
            failed += 1  # 48時間超えなど
            delete_bot_message_db(chat_id, mid)

    msg = await update.message.reply_text(f"🗑️ {deleted}件のメッセージを削除しました。" + (f"（{failed}件は削除不可：48時間超）" if failed else ""))
    save_bot_message(chat_id, msg.message_id)

async def cmd_strategy(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=14)
    if not records:
        reply = await ai_chat(text)
        sent = await update.message.reply_text(reply)
        save_bot_message(chat_id, sent.message_id)
        return

    total_sum  = sum(r['total'] for r in records)
    avg        = total_sum / len(records)
    best       = max(records, key=lambda r: r['total'])
    worst      = min(records, key=lambda r: r['total'])
    wastage_pct = sum(r['wastage'] for r in records) / total_sum * 100 if total_sum else 0
    cash_pct    = sum(r['cash_sale'] for r in records) / total_sum * 100 if total_sum else 0
    grab_pct    = sum(r['grab'] for r in records) / total_sum * 100 if total_sum else 0
    daily_lines = "\n".join(
        f"  {r['date']} ₱{r['total']:,.0f}（取引{r['transaction_count']}件）"
        for r in records
    )

    data_context = f"""【みどりのマート 直近{len(records)}日の売上データ】
総売上: ₱{total_sum:,.0f} | 日平均: ₱{avg:,.0f}
最高日: {best['date']} ₱{best['total']:,.0f}
最低日: {worst['date']} ₱{worst['total']:,.0f}
廃棄率: {wastage_pct:.1f}% | 現金比率: {cash_pct:.1f}% | Grab比率: {grab_pct:.1f}%
シフト体制: 月〜木は2シフト（Morning/Afternoon）、金〜日は3シフト（+Graveyard）

日別実績:
{daily_lines}"""

    try:
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
        resp = await client.messages.create(
            model="claude-opus-4-5",
            max_tokens=1000,
            system="あなたは「みどりのマート」のマネジメントです。提供された実際の売上データを根拠に、具体的で実践的なアドバイスをしてください。ユーザーが書いた言語で回答してください。",
            messages=[{"role": "user", "content": f"{data_context}\n\n質問: {text}"}]
        )
        sent = await update.message.reply_text(resp.content[0].text.strip())
        save_bot_message(chat_id, sent.message_id)
    except Exception as e:
        logger.error(f"Strategy AI error: {e}")
        await update.message.reply_text("⚠️ 分析中にエラーが発生しました。しばらくしてからもう一度お試しください。")

async def cmd_db_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """オーナー専用: DB状態の診断情報を返す"""
    user_id = update.effective_user.id if update.effective_user else 0
    if OWNER_CHAT_ID and user_id != OWNER_CHAT_ID:
        await update.message.reply_text("このコマンドはオーナーのみ使用できます。")
        return
    chat_id = update.effective_chat.id
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    conn = get_conn()
    c = conn.cursor()
    # 総レコード数
    c.execute(f'SELECT COUNT(*) FROM supermarket_sales WHERE chat_id IN ({placeholders})', ids)
    total = c.fetchone()[0]
    # 最新5件
    c.execute(f'''SELECT date, chat_id, store, total FROM supermarket_sales
                 WHERE chat_id IN ({placeholders})
                 ORDER BY id DESC LIMIT 5''', ids)
    recent = c.fetchall()
    # 同日付重複
    c.execute(f'''SELECT date, COUNT(*) as cnt FROM supermarket_sales
                 WHERE chat_id IN ({placeholders})
                 GROUP BY date HAVING cnt > 1 ORDER BY date DESC''', ids)
    dupes = c.fetchall()
    conn.close()
    lines = [f"📊 DB診断\n総レコード数: {total}件\n"]
    lines.append("📅 最新5件:")
    for row in recent:
        lines.append(f"  {row[0]} | chat={row[1]} | ₱{row[3]:,.0f}")
    if dupes:
        lines.append(f"\n⚠️ 同日付重複 {len(dupes)}件:")
        for d in dupes:
            lines.append(f"  {d[0]}: {d[1]}件")
    else:
        lines.append("\n✅ 重複なし")
    await update.message.reply_text("\n".join(lines))

async def cmd_fix_duplicates(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """オーナー専用: 同じ日付の重複レコードを削除（最新を残す）
    同一chat_id内の重複 + リンクグループ間の同日付重複 の両方を処理する"""
    user_id = update.effective_user.id if update.effective_user else 0
    if OWNER_CHAT_ID and user_id != OWNER_CHAT_ID:
        await update.message.reply_text("このコマンドはオーナーのみ使用できます。")
        return
    chat_id = update.effective_chat.id
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    conn = get_conn()
    c = conn.cursor()
    # 同じ日付で複数レコード（異なるchat_idも含む）を検索
    c.execute(f'''
        SELECT date, COUNT(*) as cnt
        FROM supermarket_sales
        WHERE chat_id IN ({placeholders})
        GROUP BY date
        HAVING cnt > 1
        ORDER BY date DESC
    ''', ids)
    dupes = c.fetchall()
    if not dupes:
        await update.message.reply_text("✅ 重複なし。DBはきれいです。")
        conn.close()
        return
    total_deleted = 0
    lines = ["🗑️ 重複レコードを削除しました:\n"]
    for date, cnt in dupes:
        # 同じ日付の全レコードを取得し、最新(id最大)だけ残す
        c.execute(f'''
            SELECT id FROM supermarket_sales
            WHERE date=? AND chat_id IN ({placeholders})
            ORDER BY id DESC
        ''', (date, *ids))
        rows = c.fetchall()
        delete_ids = [r[0] for r in rows[1:]]
        placeholders2 = ','.join('?' * len(delete_ids))
        c.execute(f'DELETE FROM supermarket_sales WHERE id IN ({placeholders2})', delete_ids)
        total_deleted += len(delete_ids)
        lines.append(f"  {date}: {cnt}件 → 1件に整理")
    conn.commit()
    conn.close()
    lines.append(f"\n合計 {total_deleted} 件削除しました。")
    await update.message.reply_text("\n".join(lines))

async def cmd_last_week(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records, start, end = get_last_week_records(chat_id)
    if not records:
        sent = await update.message.reply_text(f"📭 先週（{start} 〜 {end}）のデータがありません。")
        save_bot_message(chat_id, sent.message_id)
        return
    # Reuse cmd_weekly logic but with last week's records
    await _send_weekly_report(ctx.bot, chat_id, records, label=f"先週（{start} 〜 {end}）")

# ─── Natural language intent detection ────────────────────
_ADVISORY_KEYWORDS = [
    '教えて', 'アドバイス', '提案', '戦略', 'どうすれば', 'どうしたら',
    'どう思う', 'どうしよう', '改善', 'おすすめ', 'ヒント', 'どうやって',
    'どうすべき', 'どう対応', 'どう考え', 'どうすると', 'どうしたい',
    'advice', 'suggest', 'strategy', 'recommend', 'how to', 'how should',
]
_DATA_KEYWORDS = [
    '先週', '今週', '今月', '売上', 'レポート', 'データ', '実績', '結果',
    'last week', 'this week', 'sales', 'report',
]

def detect_intent(text: str) -> Optional[str]:
    t = text.lower()
    # 質問・相談 + データ参照 → strategyとしてデータ付きAI回答
    if any(k in t for k in _ADVISORY_KEYWORDS) and any(k in t for k in _DATA_KEYWORDS):
        return 'strategy'
    # 質問・相談のみ → 通常AIチャット
    if any(k in t for k in _ADVISORY_KEYWORDS):
        return None
    if any(k in t for k in ['先週', 'last week', '前週']):
        return 'last_week'
    if any(k in t for k in ['今週', 'weekly', 'ウィークリー', '週次', '週レポ']):
        return 'weekly'
    if any(k in t for k in ['今月', 'monthly', 'マンスリー', '月次', '月レポ']):
        return 'monthly'
    if any(k in t for k in ['シフト比較', 'shift比較', 'shift compare', 'compare shift']):
        return 'compare_shift'
    if any(k in t for k in ['決済比較', 'payment比較', 'payment compare', 'compare payment']):
        return 'compare_payment'
    if any(k in t for k in ['トレンド', 'trend', '傾向', '推移']):
        return 'trend'
    if any(k in t for k in ['csv', 'export', 'エクスポート', 'ダウンロード']):
        return 'export'
    if any(k in t for k in ['翻訳開始', 'translate on']) or (
            '翻訳' in t and any(k in t for k in ['開始', 'スタート', 'start', 'はじめ', 'オン', ' on'])):
        return 'translate_on'
    if any(k in t for k in ['翻訳終了', 'translate off']) or (
            '翻訳' in t and any(k in t for k in ['終了', 'ストップ', 'stop', 'やめ', 'オフ', 'off'])):
        return 'translate_off'

    if any(k in t for k in ['重複修正', '重複削除', 'fix_duplicates', 'fix duplicates']):
        return 'fix_duplicates'
    if any(k in t for k in ['db診断', 'db状態', 'db status', 'データベース診断']):
        return 'db_status'
    if any(k in t for k in ['削除', 'delete', '消して', '取り消し']):
        if any(k in t for k in ['メッセージ', 'ボット', 'bot', '発言', '全部', '件']):
            return 'delete_bot'
        return 'delete'
    if any(k in t for k in ['ヘルプ', 'help', '使い方', 'コマンド']):
        return 'help'
    if '目標設定' in t or '目標を設定' in t or (('目標' in t or 'target' in t) and re.search(r'\d', t)):
        return 'set_target'
    if '目標' in t and any(k in t for k in ['削除', 'リセット', '取り消し', 'クリア', 'なし', 'reset', 'clear', 'delete']):
        return 'reset_target'
    if '目標確認' in t or '目標を見' in t or ('目標' in t and ('確認' in t or '見せ' in t or 'show' in t or '教えて' in t)):
        return 'view_target'
    if re.search(r'\d{1,2}[/月]\d{1,2}', t) and any(k in t for k in ['確認', 'データ', 'check', '記録', '見せ', '見て']):
        return 'check_date'
    # Procurement intents
    if any(k in t for k in ['仕入れ', '仕入', 'procurement', '発注提案', '入荷提案']):
        if any(k in t for k in ['予算', 'budget']):
            return 'set_procurement_budget'
        if any(k in t for k in ['曜日', '日に', '日を', 'day', 'restock']):
            return 'set_restock_day'
        if any(k in t for k in ['設定確認', '設定を確認', 'settings', '確認']):
            return 'view_procurement_settings'
        return 'procurement'
    # Fixed item intents
    if '固定アイテム' in text or 'fixed item' in t:
        if any(k in text for k in ['追加', '登録', 'add']):
            return 'fixed_item_add'
        if any(k in text for k in ['一覧', 'リスト', 'list', '見せ', '確認']):
            return 'fixed_item_list'
        if any(k in text for k in ['削除', 'delete', '消して', '取り消し']):
            return 'fixed_item_delete'
        return 'fixed_item_list'
    # Inventory intents
    if any(k in text for k in ['在庫確認', '在庫一覧', '在庫リスト']):
        return 'inventory_check'
    if '在庫更新' in text:
        return 'inventory_update'
    # Order history CSV
    if any(k in t for k in ['注文履歴', '注文csv', '注文 csv', 'order history', 'order csv']):
        return 'order_history_csv'
    # UTAK intents
    if any(k in t for k in ['在庫分析', 'utak分析', '仕入れ分析', 'reorder', '発注分析']):
        return 'utak_analysis'
    if any(k in t for k in ['utak在庫', 'utak stock', '在庫サマリー', 'pos在庫']):
        return 'utak_stock'
    if any(k in t for k in ['売れ筋', 'bestseller', 'ベストセラー', '売上ランキング', '人気商品']):
        return 'utak_bestsellers'
    if any(k in t for k in ['死に筋', 'dead stock', 'デッドストック', '売れ残り', '不良在庫']):
        return 'dead_stock'
    if any(k in t for k in ['オンライン売上', 'online sales', 'grab分析', 'grab売上', 'オンライン比較']):
        return 'online_sales'
    if any(k in t for k in ['時間帯', 'hourly', '時間別', 'ピーク時間', 'peak hour']):
        return 'hourly_sales'
    if any(k in t for k in ['セット販売', 'bundle', 'バンドル', '一緒に買', 'bought together', 'ペア']):
        return 'bundle_suggestions'
    return None

def is_bot_mentioned(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    msg = update.message
    if msg.chat.type == 'private':
        return True
    if msg.entities:
        for entity in msg.entities:
            if entity.type == 'mention':
                mentioned = msg.text[entity.offset:entity.offset + entity.length]
                if ctx.bot.username and mentioned == f"@{ctx.bot.username}":
                    return True
    return False

HELP_TEXT = """🤖 話しかけてくれてありがとう！

📊 「今週のレポート」— 週次サマリー
📅 「今月のまとめ」— 月次グラフ
🔀 「シフト比較」— シフト別グラフ
💳 「決済比較」— 決済方法別グラフ
📈 「トレンド見せて」— 過去30日分析
📁 「CSVダウンロード」— データ出力

🎯 「日次目標を20000に設定」— 日次目標設定
🎯 「週次目標150000」— 週次目標設定
🎯 「月間目標600000」— 月間目標設定
🎯 「目標確認」— 現在の目標を表示

🗑️ 「最新レポートを削除」— 最新データ削除
🗑️ 「2026-03-08のレポートを削除」— 日付指定削除

📦 「仕入れ提案」— AI仕入れ推薦
📦 「仕入れ予算を50000に設定」— 週間予算設定
📦 「仕入れ日を火曜に設定」— 仕入れ曜日設定
📦 「仕入れ設定確認」— 現在の設定を表示

📌 「固定アイテム追加 カップ麺 20個 ¥150」— 必ず仕入れる商品を登録
📌 「固定アイテム一覧」— 固定アイテム一覧表示
📌 「固定アイテム削除 カップ麺」— 固定アイテムを削除

📦 「在庫確認」— 現在の在庫一覧
📦 「在庫更新 カップ麺 -10」— 在庫を手動で更新
📋 「注文履歴」— 過去90日の注文CSVを取得

📤 UTAKのCSVファイルを送信 — 在庫・売上を自動取り込み
📊 「売れ筋」— UTAK売上ランキング
📦 「UTAK在庫」— 在庫サマリー表示
🔍 「在庫分析」— AI仕入れ提案
⚠️ 「死に筋」— 売れ残り商品リスト
📱 「オンライン売上」— Grab vs 店舗比較
⏰ 「時間帯」— 時間帯別売上分析
🛒 「セット販売」— 一緒に買われる商品ペア"""

# ─── Main message handler ──────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    text = update.message.text or update.message.caption or ''
    if not text:
        return
    chat_id = update.effective_chat.id
    user    = update.effective_user
    logger.info(f"MSG received | chat={chat_id} | user={user.username or user.id} | len={len(text)} | preview={text[:60].replace(chr(10),' ')!r}")

    # 0) シフトスケジュールの自動検知
    if is_manpower_schedule(text):
        parsed = parse_manpower_schedule(text)
        save_shift_schedule(parsed, chat_id)
        # Only reply in the designated group (or WEEKLY_REPORT_CHAT_ID as fallback)
        reply_chat = SCHEDULE_REPLY_CHAT_ID or WEEKLY_REPORT_CHAT_ID
        if reply_chat == 0 or chat_id == reply_chat:
            lines = [f"📋 シフトスケジュールを記録しました（{parsed.get('date', '?')}）"]
            for shift, label in [('graveyard', 'Graveyard'), ('morning', 'Morning'), ('afternoon', 'Afternoon')]:
                if parsed.get(shift):
                    lines.append(f"  {label}: {parsed[shift]}")
            sent = await update.message.reply_text("\n".join(lines))
            save_bot_message(chat_id, sent.message_id)
        return

    # 1) 売上レポートの自動検知
    _is_report = is_supermarket_report(text)
    logger.info(f"is_supermarket_report={_is_report} | chat={chat_id}")
    if _is_report:
        try:
            data     = parse_report(text)
            # Validate parsed date
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', data['date']):
                await update.message.reply_text(
                    f"⚠️ 日付の読み取りに失敗しました: '{data['date']}'\n"
                    "DATE TODAY の日付フォーマットを確認してください（例: 03/09/2026）"
                )
                return
            try:
                _pd = datetime.strptime(data['date'], '%Y-%m-%d')
                _diff = (datetime.now() - _pd).days
                if _diff < -1 or _diff > 30:
                    await update.message.reply_text(
                        f"⚠️ 日付確認: {data['date']} が本日と{_diff}日ずれています。\n"
                        "正しい日付かご確認ください。（処理は続行します）"
                    )
            except ValueError:
                pass
            # total と主要フィールドがすべて0の場合
            if data['total'] == 0 and data['cash_sale'] == 0 and data['for_deposit'] == 0:
                # 本文に3桁以上の数字があればパース失敗→マネージャーに通知
                if re.search(r'\d{3,}', text):
                    await update.message.reply_text(
                        "⚠️ レポートを検知しましたが、数値の読み取りに失敗しました。\n"
                        "以下を確認してください：\n"
                        "・TOTAL の行頭にスペースが多すぎないか\n"
                        "・CASH SALE / FOR DEPOSIT の表記が正しいか\n"
                        "・DATE TODAY の日付フォーマット（例: 03/11/2026）\n\n"
                        "問題が解決しない場合はレポートのテキストをそのまま送ってください。"
                    )
                # 数字もなければ空テンプレート→無視
                return
            # 重複チェック：同じ日付のレコードが既に存在するか確認
            _existing_raw = None
            try:
                _conn = get_conn()
                _row = _conn.execute(
                    'SELECT raw_text FROM supermarket_sales WHERE date=? AND chat_id=?',
                    (data['date'], chat_id)
                ).fetchone()
                if _row:
                    _existing_raw = _row[0]
            except Exception:
                pass
            finally:
                try:
                    _conn.close()
                except Exception:
                    pass
            # 同じ内容（空白・改行を正規化して比較）→ スキップ
            def _normalize(t): return re.sub(r'\s+', ' ', (t or '').strip().lower())
            if _existing_raw and _normalize(_existing_raw) == _normalize(text):
                logger.info(f"Duplicate report skipped (same content) | date={data['date']} | chat={chat_id}")
                return  # 無言でスキップ
            await update.message.reply_text(f"🔍 レポートを受信しました（日付: {data['date']}）。分析中...")
            # Save FIRST — before any DB reads or blocking calls that could fail
            try:
                save_record(data, text, chat_id)
            except Exception as e:
                logger.error(f"save_record error: {e}", exc_info=True)
            try:
                prev = get_previous(data['date'], data['store'], chat_id)
            except Exception as e:
                logger.error(f"get_previous error: {e}", exc_info=True)
                prev = None
            try:
                await check_sales_anomaly(ctx.bot, chat_id, data.get('date', ''), data.get('total', 0))
            except Exception as e:
                logger.error(f"check_sales_anomaly error: {e}", exc_info=True)
            try:
                alerts = check_alerts(data, prev)
            except Exception as e:
                logger.error(f"check_alerts error: {e}", exc_info=True)
                alerts = []
            try:
                comments = await asyncio.wait_for(
                    generate_ai_comment(data, prev),
                    timeout=60.0
                )
            except asyncio.TimeoutError:
                logger.warning("generate_ai_comment timed out after 60s")
                comments = "（AI分析タイムアウト）"
            except Exception as e:
                logger.error(f"generate_ai_comment error: {e}", exc_info=True)
                comments = "（AI分析スキップ）"
            try:
                daily_target   = get_daily_target(chat_id, data.get('date', ''))
                monthly_target = get_target_any(chat_id, 'monthly')
                reply = format_daily_report(data, prev, comments, alerts, daily_target, monthly_target, chat_id)
            except Exception as e:
                logger.error(f"format_daily_report error: {e}", exc_info=True)
                reply = (
                    f"📊 {data.get('date','?')} 売上レポート\n"
                    f"売上合計: ₱{data.get('total', 0):,.0f}\n"
                    f"（詳細フォーマットエラー: {e}）"
                )
            # Trim if over Telegram's 4096 char limit
            if len(reply) > 4096:
                reply = reply[:4090] + "\n…"
            sent = await update.message.reply_text(reply)
            save_bot_message(chat_id, sent.message_id)
        except Exception as e:
            logger.error(f"Report error: {e}", exc_info=True)
            try:
                await update.message.reply_text(f"⚠️ 分析中にエラーが発生しました: {str(e)}")
            except Exception:
                pass
        return

    # 2) Translation mode
    if get_translate_mode(chat_id) and not is_supermarket_report(text):
        if any(k in text.lower() for k in ['翻訳終了', 'translate off']):
            set_translate_mode(chat_id, False)
            sent = await update.message.reply_text("🌐 Translation mode OFF.")
            save_bot_message(chat_id, sent.message_id)
            return
        # @メンション付きのコマンドは翻訳せずコマンド処理に回す
        if is_bot_mentioned(update, ctx) and detect_intent(text) is not None:
            pass  # fall through to command processing
        else:
            try:
                translated = await translate_text(text)
                sent = await update.message.reply_text(f"🌐 {translated}")
                save_bot_message(chat_id, sent.message_id)
            except Exception as e:
                logger.error(f"Translation error: {e}")
            return

    # 3) Only respond when bot is mentioned
    if not is_bot_mentioned(update, ctx):
        return

    intent = detect_intent(text)
    # レポート系コマンドが検出されても、質問・相談の形なら strategy に上書き
    if intent in ('last_week', 'weekly', 'monthly', 'trend', 'compare_shift', 'compare_payment'):
        t = text.lower()
        is_question = (
            any(k in t for k in _ADVISORY_KEYWORDS)
            or '?' in t or '？' in t
            or bool(re.search(r'[かな][？?]?\s*$', t.strip()))
        )
        if is_question:
            intent = 'strategy'
    if   intent == 'strategy':         await cmd_strategy(update, ctx, text)
    elif intent == 'last_week':        await cmd_last_week(update, ctx)
    elif intent == 'weekly':           await cmd_weekly(update, ctx)
    elif intent == 'monthly':          await cmd_monthly(update, ctx)
    elif intent == 'compare_shift':    await cmd_compare(update, ctx, 'shift')
    elif intent == 'compare_payment':  await cmd_compare(update, ctx, 'payment')
    elif intent == 'trend':            await cmd_trend(update, ctx)
    elif intent == 'export':           await cmd_export(update, ctx)
    elif intent == 'db_status':        await cmd_db_status(update, ctx)
    elif intent == 'fix_duplicates':   await cmd_fix_duplicates(update, ctx)
    elif intent == 'delete':           await cmd_delete(update, ctx, text)
    elif intent == 'delete_bot':       await cmd_delete_bot_messages(update, ctx, text)
    elif intent == 'translate_on':
        set_translate_mode(chat_id, True)
        sent = await update.message.reply_text("🌐 Translation mode ON! All messages will be auto-translated.\nSend '翻訳終了' to stop.")
        save_bot_message(chat_id, sent.message_id)
    elif intent == 'translate_off':
        set_translate_mode(chat_id, False)
        sent = await update.message.reply_text("🌐 Translation mode OFF.")
        save_bot_message(chat_id, sent.message_id)
    elif intent == 'help':
        sent = await update.message.reply_text(HELP_TEXT)
        save_bot_message(chat_id, sent.message_id)
    elif intent == 'set_target':     await cmd_set_target(update, ctx, text)
    elif intent == 'reset_target':   await cmd_reset_target(update, ctx, text)
    elif intent == 'view_target':    await cmd_view_target(update, ctx)
    elif intent == 'check_date':     await cmd_check_date(update, ctx, text)
    elif intent == 'procurement':              await cmd_procurement(update, ctx)
    elif intent == 'set_procurement_budget':   await cmd_set_procurement_budget(update, ctx, text)
    elif intent == 'set_restock_day':          await cmd_set_restock_day(update, ctx, text)
    elif intent == 'view_procurement_settings': await cmd_view_procurement_settings(update, ctx)
    elif intent == 'fixed_item_add':           await cmd_add_fixed_item(update, ctx, text)
    elif intent == 'fixed_item_list':          await cmd_list_fixed_items(update, ctx)
    elif intent == 'fixed_item_delete':        await cmd_delete_fixed_item(update, ctx, text)
    elif intent == 'inventory_check':          await cmd_inventory(update, ctx)
    elif intent == 'inventory_update':         await cmd_update_inventory(update, ctx, text)
    elif intent == 'order_history_csv':        await cmd_order_history_csv(update, ctx)
    elif intent == 'utak_analysis':           await cmd_utak_analysis(update, ctx)
    elif intent == 'utak_stock':              await cmd_utak_stock(update, ctx)
    elif intent == 'utak_bestsellers':        await cmd_utak_bestsellers(update, ctx)
    elif intent == 'dead_stock':              await cmd_dead_stock(update, ctx)
    elif intent == 'online_sales':            await cmd_online_sales(update, ctx)
    elif intent == 'hourly_sales':            await cmd_hourly_sales(update, ctx)
    elif intent == 'bundle_suggestions':      await cmd_bundle_suggestions(update, ctx)
    else:
        try:
            reply_text = await ai_chat(text)
            sent = await update.message.reply_text(reply_text)
            save_bot_message(chat_id, sent.message_id)
        except Exception as e:
            logger.error(f"AI chat error: {e}")
            await update.message.reply_text(HELP_TEXT)

async def _dm_managers(bot, message: str):
    """Send a direct message to all registered managers. Silently skips failures."""
    for name, tid in MANAGER_IDS.items():
        try:
            await bot.send_message(chat_id=tid, text=message)
        except Exception as e:
            logger.warning(f"DM to {name} ({tid}) failed: {e}")

async def check_sales_anomaly(bot, chat_id: int, date_str: str, total: float):
    """Alert group + DM managers if today's sales deviate ±30% from same weekday last week."""
    try:
        report_date      = datetime.strptime(date_str, '%Y-%m-%d')
        same_day_last_wk = (report_date - timedelta(days=7)).strftime('%Y-%m-%d')
        conn = get_conn()
        c    = conn.cursor()
        ids  = get_chat_ids(chat_id)
        placeholders = ','.join('?' * len(ids))
        c.execute(
            f'SELECT total FROM supermarket_sales WHERE chat_id IN ({placeholders}) AND date=?',
            (*ids, same_day_last_wk)
        )
        row = c.fetchone()
        conn.close()
        if not row or row[0] == 0:
            return
        prev_total = row[0]
        diff_pct   = (total - prev_total) / prev_total * 100
        if abs(diff_pct) < 30:
            return
        direction = "📈 高い" if diff_pct > 0 else "📉 低い"
        dow_jp = ['月', '火', '水', '木', '金', '土', '日'][report_date.weekday()]
        group_msg = (
            f"⚠️ 売上異常アラート（{dow_jp}曜日）\n"
            f"本日 {date_str}：₱{total:,.0f}\n"
            f"先週同曜日 {same_day_last_wk}：₱{prev_total:,.0f}\n"
            f"→ {direction}（{diff_pct:+.1f}%）"
        )
        await bot.send_message(chat_id=chat_id, text=group_msg)
        dm_msg = (
            f"⚠️ Sales Anomaly Alert\n"
            f"Today ({date_str}): ₱{total:,.0f}\n"
            f"Same day last week ({same_day_last_wk}): ₱{prev_total:,.0f}\n"
            f"Difference: {diff_pct:+.1f}% ({direction.split()[1]})"
        )
        await _dm_managers(bot, dm_msg)
    except Exception as e:
        logger.error(f"check_sales_anomaly error: {e}")

# ─── Scheduled jobs ────────────────────────────────────────

async def auto_monthly_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs daily at 8:00 AM PHT — on 1st of month, sends previous month's report to group."""
    now = datetime.now(PHT)
    if now.day != 1:
        return
    if not WEEKLY_REPORT_CHAT_ID:
        return
    # Previous month
    prev_month = now.month - 1 or 12
    prev_year  = now.year if now.month > 1 else now.year - 1
    month_label = datetime(prev_year, prev_month, 1).strftime('%Y年%m月')
    try:
        lookup_chat = STORE_GROUP_IDS[0] if STORE_GROUP_IDS else WEEKLY_REPORT_CHAT_ID
        records, start, end = get_month_records(lookup_chat, prev_year, prev_month)
        if not records:
            await ctx.bot.send_message(chat_id=WEEKLY_REPORT_CHAT_ID,
                                       text=f"📭 {month_label}のデータがありません。")
            return
        total_sum     = sum(r['total'] for r in records)
        n             = len(records)
        days_in_month = calendar.monthrange(prev_year, prev_month)[1]
        monthly_target = get_target_any(WEEKLY_REPORT_CHAT_ID, 'monthly')
        target_line = ""
        if monthly_target > 0:
            ach     = total_sum / monthly_target * 100
            filled  = min(int(ach // 10), 10)
            bar     = "🟩" * filled + "⬜" * (10 - filled)
            target_line = f"\n🎯 目標達成率: {ach:.1f}% {bar}\n   ₱{total_sum:,.0f} / 目標 ₱{monthly_target:,.0f}"
        cat_data = sorted(
            [(lbl, sum(r.get(key, 0) for r in records)) for lbl, key in CAT_LABELS if sum(r.get(key, 0) for r in records) > 0],
            key=lambda x: x[1], reverse=True
        )
        cat_sum = sum(v for _, v in cat_data)
        cat_rows = "\n".join(
            f"  {lbl:<22} ₱{val:>10,.0f}  ({val/cat_sum*100:.1f}%)"
            for lbl, val in cat_data[:5]
        ) if cat_data else "  (データなし)"
        report = (
            f"📅 {month_label} 月次レポート（自動）\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"💰 月間売上合計: ₱{total_sum:,.0f}\n"
            f"📊 日平均: ₱{total_sum/n:,.0f}\n"
            f"📆 営業日数: {n}日 / {days_in_month}日{target_line}\n\n"
            f"【TOP5 カテゴリ】\n{cat_rows}"
        )
        await ctx.bot.send_message(chat_id=WEEKLY_REPORT_CHAT_ID, text=report)
        buf = make_trend_chart(records, f"Monthly Sales Trend ({month_label})")
        await ctx.bot.send_photo(chat_id=WEEKLY_REPORT_CHAT_ID, photo=buf, caption="📈 Monthly Trend")
        buf_cat = make_category_chart(records)
        if buf_cat.getbuffer().nbytes > 0:
            await ctx.bot.send_photo(chat_id=WEEKLY_REPORT_CHAT_ID, photo=buf_cat, caption="🗂️ Category Breakdown")
        logger.info(f"auto_monthly_report_job: sent {month_label} report to {WEEKLY_REPORT_CHAT_ID}")
    except Exception as e:
        logger.error(f"auto_monthly_report_job failed: {e}")

async def auto_staff_performance_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs daily at 8:00 AM PHT — on 1st of month, sends staff performance to OWNER_CHAT_ID."""
    now = datetime.now(PHT)
    if now.day != 1:
        return
    if not OWNER_CHAT_ID or not WEEKLY_REPORT_CHAT_ID:
        return
    prev_month = now.month - 1 or 12
    prev_year  = now.year if now.month > 1 else now.year - 1
    month_label = datetime(prev_year, prev_month, 1).strftime('%Y年%m月')
    try:
        staff = get_staff_performance(WEEKLY_REPORT_CHAT_ID, prev_year, prev_month)
        if not staff:
            await ctx.bot.send_message(chat_id=OWNER_CHAT_ID,
                                       text=f"📭 {month_label}のスタッフデータがありません。")
            return
        total_month = sum(s['total'] for s in staff)
        lines = [f"👥 {month_label} スタッフ別パフォーマンス\n━━━━━━━━━━━━━━━━━━━"]
        medals = ["🥇", "🥈", "🥉"]
        for i, s in enumerate(staff):
            medal = medals[i] if i < 3 else "  "
            share = s['total'] / total_month * 100 if total_month > 0 else 0
            lines.append(
                f"{medal} {s['name']}\n"
                f"   提出: {s['reports']}回 | 合計: ₱{s['total']:,.0f} ({share:.1f}%)\n"
                f"   日平均: ₱{s['avg']:,.0f} | 最高: ₱{s['best']:,.0f}（{s['best_date']}）"
            )
        await ctx.bot.send_message(chat_id=OWNER_CHAT_ID, text="\n\n".join(lines))
        logger.info(f"auto_staff_performance_job: sent {month_label} to owner {OWNER_CHAT_ID}")
    except Exception as e:
        logger.error(f"auto_staff_performance_job failed: {e}")

async def auto_weekly_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs every Monday 8:00 AM PHT — sends previous Mon–Sun report to WEEKLY_REPORT_CHAT_ID."""
    if not WEEKLY_REPORT_CHAT_ID:
        logger.warning("auto_weekly_report_job: WEEKLY_REPORT_CHAT_ID not set, skipping")
        return
    try:
        # Use store group for record lookup; WEEKLY_REPORT_CHAT_ID may differ from the store group
        lookup_chat = STORE_GROUP_IDS[0] if STORE_GROUP_IDS else WEEKLY_REPORT_CHAT_ID
        records, start, end = get_last_week_records(lookup_chat)
        if not records:
            logger.info(f"auto_weekly_report_job: no records for {start} - {end}, skipping")
            return
        logger.info(f"auto_weekly_report_job: sending report for {start} - {end} to {WEEKLY_REPORT_CHAT_ID}")
        await _send_weekly_report(ctx.bot, WEEKLY_REPORT_CHAT_ID, records, label=f"先週（{start} 〜 {end}）自動レポート")
    except Exception as e:
        logger.error(f"auto_weekly_report_job failed: {e}")

async def auto_procurement_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs daily at 20:00 PHT — if tomorrow is restock day, sends procurement recommendations."""
    now = datetime.now(PHT)
    tomorrow = (now + timedelta(days=1)).weekday()
    today_str = now.strftime('%Y-%m-%d')
    target_chats = []
    if WEEKLY_REPORT_CHAT_ID:
        target_chats.append(WEEKLY_REPORT_CHAT_ID)
    if OWNER_CHAT_ID and OWNER_CHAT_ID not in target_chats:
        target_chats.append(OWNER_CHAT_ID)
    for chat_id in target_chats:
        try:
            settings = get_procurement_settings(chat_id)
            if not settings['auto_send'] or settings['weekly_budget'] <= 0:
                continue
            if tomorrow != settings['restock_day']:
                continue
            if settings['last_sent_date'] == today_str:
                continue
            budget = settings['weekly_budget']
            proposal = await generate_procurement_recommendation(chat_id, budget)
            if proposal is None:
                logger.warning(f"auto_procurement_job: AI generation failed for {chat_id}")
                continue
            day_name = WEEKDAY_NAMES_JA[settings['restock_day']]
            num_cats = len(proposal.get('categories', []))
            _pending_proposals[chat_id] = {
                'proposal': proposal,
                'budget': budget,
                'status': {i: 'pending' for i in range(num_cats)},
            }
            header = f"📦🔔 明日は仕入れ日（{day_name}）です！\n予算: ¥{budget:,.0f}\n"
            text = header + format_proposal_message(proposal, chat_id)
            keyboard = make_category_keyboard(proposal, chat_id)
            sent = await ctx.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
            save_bot_message(chat_id, sent.message_id)
            update_last_sent_date(chat_id, today_str)
            logger.info(f"auto_procurement_job: sent recommendation to {chat_id}")
        except Exception as e:
            logger.error(f"auto_procurement_job failed for {chat_id}: {e}")

# ─── Target commands ────────────────────────────────────────
async def cmd_set_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    # Try to find amount after ₱/¥, or after 目標/target keyword, or after を particle
    amount_m = (
        re.search(r'[₱¥]\s*([\d,]+(?:\.\d+)?)', text) or
        re.search(r'(?:目標|target)\D{0,15}?([\d,]+(?:\.\d+)?)', text, re.IGNORECASE) or
        re.search(r'を\s*([\d,]+(?:\.\d+)?)', text)
    )
    if not amount_m:
        sent = await update.message.reply_text(
            "💡 目標設定の例:\n「日次目標を20000に設定」\n「週次目標150000」"
        )
        save_bot_message(chat_id, sent.message_id)
        return
    amount = float(amount_m.group(1).replace(',', ''))
    t = text.lower()
    # Day-of-week specific targets
    if any(k in text for k in ['月〜木', '月木', '月曜', '平日']) or re.search(r'月.{0,3}木', text):
        set_target(chat_id, 'daily_mon_thu', amount)
        sent = await update.message.reply_text(f"✅ 日次目標（月〜木）を ₱{amount:,.0f} に設定しました。")
    elif any(k in text for k in ['金曜', '金日', '金のみ', 'friday']) or re.search(r'^金', text):
        set_target(chat_id, 'daily_fri', amount)
        sent = await update.message.reply_text(f"✅ 日次目標（金曜日）を ₱{amount:,.0f} に設定しました。")
    elif any(k in text for k in ['土日', '週末', '土曜', '日曜', 'weekend', 'saturday', 'sunday']):
        set_target(chat_id, 'daily_sat_sun', amount)
        sent = await update.message.reply_text(f"✅ 日次目標（土・日）を ₱{amount:,.0f} に設定しました。")
    elif any(k in text for k in ['月間', '月次']) or ('月' in text and 'month' not in t and '月〜' not in text):
        set_target(chat_id, 'monthly', amount)
        sent = await update.message.reply_text(f"✅ 月間目標を ₱{amount:,.0f} に設定しました。")
    elif '週' in text or 'week' in t:
        set_target(chat_id, 'weekly', amount)
        sent = await update.message.reply_text(f"✅ 週次目標を ₱{amount:,.0f} に設定しました。")
    else:
        set_target(chat_id, 'daily', amount)
        sent = await update.message.reply_text(f"✅ 日次目標を ₱{amount:,.0f} に設定しました。")
    save_bot_message(chat_id, sent.message_id)

async def cmd_view_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    daily       = get_target(chat_id, 'daily')
    mon_thu     = get_target(chat_id, 'daily_mon_thu')
    fri         = get_target(chat_id, 'daily_fri')
    sat_sun     = get_target(chat_id, 'daily_sat_sun')
    weekly      = get_target(chat_id, 'weekly')
    monthly     = get_target(chat_id, 'monthly')
    lines = ["🎯 現在の売上目標"]
    if mon_thu > 0 or fri > 0 or sat_sun > 0:
        lines.append(f"日次目標（月〜木）: {f'₱{mon_thu:,.0f}' if mon_thu > 0 else '未設定'}")
        lines.append(f"日次目標（金）:     {f'₱{fri:,.0f}'     if fri     > 0 else '未設定'}")
        lines.append(f"日次目標（土・日）: {f'₱{sat_sun:,.0f}' if sat_sun > 0 else '未設定'}")
    else:
        lines.append(f"日次目標: {f'₱{daily:,.0f}' if daily > 0 else '未設定'}")
    lines.append(f"週次目標: {f'₱{weekly:,.0f}'  if weekly  > 0 else '未設定'}")
    lines.append(f"月間目標: {f'₱{monthly:,.0f}' if monthly > 0 else '未設定'}")
    sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

async def cmd_reset_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    t = text.lower()
    if '月' in text or 'month' in t:
        delete_target(chat_id, 'monthly')
        sent = await update.message.reply_text("🗑️ 月間目標を削除しました。")
    elif '週' in text or 'week' in t:
        delete_target(chat_id, 'weekly')
        sent = await update.message.reply_text("🗑️ 週次目標を削除しました。")
    else:
        delete_target(chat_id, 'daily')
        sent = await update.message.reply_text("🗑️ 日次目標を削除しました。")
    save_bot_message(chat_id, sent.message_id)

async def cmd_check_date(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    """Show stored record for a specific date. Usage: '3/13のデータを確認'"""
    chat_id = update.effective_chat.id
    # Extract date from message
    m = re.search(r'(\d{1,2})[/月](\d{1,2})', text)
    if not m:
        sent = await update.message.reply_text("📅 日付を指定してください。例：「3/13のデータ確認」")
        save_bot_message(chat_id, sent.message_id)
        return
    month, day = int(m.group(1)), int(m.group(2))
    year = datetime.now().year
    date_str = f'{year:04d}-{month:02d}-{day:02d}'
    conn = get_conn()
    c = conn.cursor()
    ids = STORE_GROUP_IDS if STORE_GROUP_IDS else [chat_id]
    placeholders = ','.join('?' * len(ids))
    c.execute(f'SELECT date, store, submitted_by, total, chat_id FROM supermarket_sales '
              f'WHERE chat_id IN ({placeholders}) AND date=? ORDER BY created_at DESC',
              (*ids, date_str))
    rows = c.fetchall()
    conn.close()
    if not rows:
        sent = await update.message.reply_text(f"📭 {date_str} のデータはDBに存在しません。\nレポートが正常に保存されていない可能性があります。")
    else:
        lines = [f"✅ {date_str} のDBデータ:"]
        for r in rows:
            lines.append(f"  店舗: {r[1]} | 提出者: {r[2]} | 合計: ₱{r[3]:,.0f} | chat_id: {r[4]}")
        sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

# ─── Procurement commands ─────────────────────────────────
async def cmd_procurement(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Generate and send procurement recommendation with approval buttons."""
    chat_id = update.effective_chat.id
    settings = get_procurement_settings(chat_id)
    budget = settings['weekly_budget']
    if budget <= 0:
        sent = await update.message.reply_text(
            "⚠️ 仕入れ予算が設定されていません。\n"
            "先に予算を設定してください:\n"
            "例: 「仕入れ予算を50000に設定」"
        )
        save_bot_message(chat_id, sent.message_id)
        return
    waiting = await update.message.reply_text("⏳ 仕入れ提案を生成中...\n（トレンド検索 + AI分析、30秒ほどお待ちください）")
    save_bot_message(chat_id, waiting.message_id)
    proposal = await generate_procurement_recommendation(chat_id, budget)
    if proposal is None:
        sent = await ctx.bot.send_message(chat_id=chat_id, text="⚠️ 仕入れ提案の生成に失敗しました。しばらく経ってから再度お試しください。")
        save_bot_message(chat_id, sent.message_id)
        return
    # Store proposal for approval flow
    num_cats = len(proposal.get('categories', []))
    _pending_proposals[chat_id] = {
        'proposal': proposal,
        'budget': budget,
        'status': {i: 'pending' for i in range(num_cats)},
    }
    day_name = WEEKDAY_NAMES_JA[settings['restock_day']]
    header = f"📦 今週の仕入れ提案（予算: ¥{budget:,.0f}）\n仕入れ日: {day_name}\n"
    text = header + format_proposal_message(proposal, chat_id)
    keyboard = make_category_keyboard(proposal, chat_id)
    sent = await ctx.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    save_bot_message(chat_id, sent.message_id)

async def cmd_set_procurement_budget(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    """Set weekly procurement budget."""
    chat_id = update.effective_chat.id
    nums = re.findall(r'[\d,]+(?:\.\d+)?', text.replace(',', ''))
    if not nums:
        sent = await update.message.reply_text("⚠️ 金額が読み取れませんでした。\n例: 「仕入れ予算を50000に設定」")
        save_bot_message(chat_id, sent.message_id)
        return
    amount = float(nums[0].replace(',', ''))
    set_procurement_budget(chat_id, amount)
    sent = await update.message.reply_text(f"✅ 週間仕入れ予算を ¥{amount:,.0f} に設定しました。")
    save_bot_message(chat_id, sent.message_id)

async def cmd_set_restock_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    """Set restock day of the week."""
    chat_id = update.effective_chat.id
    day = parse_weekday(text)
    if day < 0:
        sent = await update.message.reply_text(
            "⚠️ 曜日が読み取れませんでした。\n"
            "例: 「仕入れ日を火曜日に設定」"
        )
        save_bot_message(chat_id, sent.message_id)
        return
    set_restock_day(chat_id, day)
    day_name = WEEKDAY_NAMES_JA[day]
    prev_day = WEEKDAY_NAMES_JA[(day - 1) % 7]
    sent = await update.message.reply_text(
        f"✅ 仕入れ日を{day_name}に設定しました。\n"
        f"毎週{prev_day}の20:00に仕入れ提案を自動送信します。"
    )
    save_bot_message(chat_id, sent.message_id)

async def cmd_view_procurement_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show current procurement settings."""
    chat_id = update.effective_chat.id
    settings = get_procurement_settings(chat_id)
    day_name = WEEKDAY_NAMES_JA[settings['restock_day']]
    prev_day = WEEKDAY_NAMES_JA[(settings['restock_day'] - 1) % 7]
    budget_str = f"¥{settings['weekly_budget']:,.0f}" if settings['weekly_budget'] > 0 else "未設定"
    auto_str = f"ON（{prev_day} 20:00）" if settings['auto_send'] else "OFF"
    sent = await update.message.reply_text(
        f"📦 仕入れ設定\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"週間予算: {budget_str}\n"
        f"仕入れ日: {day_name}\n"
        f"自動送信: {auto_str}\n"
        f"最終送信: {settings['last_sent_date'] or 'なし'}"
    )
    save_bot_message(chat_id, sent.message_id)

# ─── UTAK CSV upload & analysis handlers ──────────────────
async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """CSVファイルがアップロードされたら自動でUTAKデータとして取り込む。"""
    doc = update.message.document
    if not doc or not doc.file_name:
        return
    fname = doc.file_name.lower()
    if not fname.endswith('.csv'):
        return
    chat_id = update.effective_chat.id
    # Download file
    file = await ctx.bot.get_file(doc.file_id)
    buf = io.BytesIO()
    await file.download_to_memory(buf)
    buf.seek(0)
    # Detect encoding (BOM)
    raw = buf.read()
    text = raw.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        sent = await update.message.reply_text("⚠️ CSVファイルにデータがありません。")
        save_bot_message(chat_id, sent.message_id)
        return
    csv_type = detect_utak_csv_type(list(rows[0].keys()))
    if csv_type == 'inventory':
        count = import_utak_inventory_csv(chat_id, rows)
        summary = get_utak_inventory_summary(chat_id)
        low = get_utak_low_stock(chat_id, threshold=5)
        out = get_utak_out_of_stock(chat_id)
        msg_lines = [f"✅ UTAK在庫データ取り込み完了（{count}品目）\n"]
        msg_lines.append(summary)
        if out:
            msg_lines.append(f"\n🔴 在庫切れ（売れ筋）: {len(out)}品")
            for it in out[:10]:
                opt = f" ({it['option']})" if it.get('option') else ""
                msg_lines.append(f"  • {it['item_name']}{opt}")
            if len(out) > 10:
                msg_lines.append(f"  ...他{len(out)-10}品")
        if low:
            msg_lines.append(f"\n🟡 在庫残少（5個以下）: {len(low)}品")
            for it in low[:10]:
                opt = f" ({it['option']})" if it.get('option') else ""
                msg_lines.append(f"  • {it['item_name']}{opt}: 残{it['ending']:.0f}個")
            if len(low) > 10:
                msg_lines.append(f"  ...他{len(low)-10}品")
        msg_lines.append("\n💡「在庫分析」で仕入れ提案を生成できます")
        sent = await update.message.reply_text("\n".join(msg_lines))
        save_bot_message(chat_id, sent.message_id)
    elif csv_type == 'transactions':
        count = import_utak_sales_csv(chat_id, rows)
        # Quick summary
        top = get_utak_sales_top(chat_id, days=1, limit=10)
        msg_lines = [f"✅ UTAK売上データ取り込み完了（{count}件）\n"]
        if top:
            msg_lines.append("📊 本日の売れ筋トップ10:")
            for i, it in enumerate(top, 1):
                msg_lines.append(f"  {i}. {it['item_name']}（{it['category']}）: {it['total_qty']:.0f}個 / ₱{it['total_sales']:,.0f}")
        msg_lines.append("\n💡「在庫分析」で仕入れ提案を生成できます")
        sent = await update.message.reply_text("\n".join(msg_lines))
        save_bot_message(chat_id, sent.message_id)
    else:
        sent = await update.message.reply_text("⚠️ UTAKのCSV形式を認識できませんでした。\nInventory CSV または Transactions Details CSV をお送りください。")
        save_bot_message(chat_id, sent.message_id)

async def cmd_utak_analysis(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """UTAK在庫+売上データをAI分析して仕入れ提案を生成。"""
    chat_id = update.effective_chat.id
    sent = await update.message.reply_text("🔄 UTAKデータを分析中...")
    save_bot_message(chat_id, sent.message_id)
    result = await generate_utak_reorder_ai(chat_id)
    await sent.edit_text(result)

async def cmd_utak_stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """UTAKの在庫サマリーを表示。"""
    chat_id = update.effective_chat.id
    summary = get_utak_inventory_summary(chat_id)
    sent = await update.message.reply_text(summary)
    save_bot_message(chat_id, sent.message_id)

async def cmd_utak_bestsellers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """UTAK売上データから売れ筋ランキングを表示。"""
    chat_id = update.effective_chat.id
    top = get_utak_sales_top(chat_id, days=7, limit=20)
    if not top:
        sent = await update.message.reply_text("📊 売上データがありません。UTAKのTransactions CSVを送ってください。")
        save_bot_message(chat_id, sent.message_id)
        return
    lines = ["📊 過去7日の売れ筋トップ20\n━━━━━━━━━━━━━━━━━━━"]
    for i, it in enumerate(top, 1):
        lines.append(f"{i}. {it['item_name']}（{it['category']}）\n   {it['total_qty']:.0f}個 / ₱{it['total_sales']:,.0f}")
    sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

async def cmd_dead_stock(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """14日以上売れていない在庫あり商品を表示。"""
    chat_id = update.effective_chat.id
    dead = get_dead_stock(chat_id, days=14)
    if not dead:
        sent = await update.message.reply_text("✅ 死に筋商品はありません（全商品が過去14日で売上あり）。")
        save_bot_message(chat_id, sent.message_id)
        return
    total_val = sum(it.get('inv_value', 0) or 0 for it in dead)
    lines = [f"⚠️ 死に筋アラート（14日以上売上なし）: {len(dead)}品\n在庫金額合計: ₱{total_val:,.0f}\n━━━━━━━━━━━━━━━━━━━"]
    by_cat: dict = {}
    for it in dead:
        by_cat.setdefault(it['category'], []).append(it)
    for cat, items in sorted(by_cat.items()):
        cat_val = sum(it.get('inv_value', 0) or 0 for it in items)
        lines.append(f"\n【{cat}】{len(items)}品 / ₱{cat_val:,.0f}")
        for it in items[:5]:
            opt = f" ({it['option']})" if it.get('option') else ""
            val = f" ₱{it['inv_value']:,.0f}" if it.get('inv_value') else ""
            lines.append(f"  • {it['item_name']}{opt}: {it['stock']:.0f}個{val}")
        if len(items) > 5:
            lines.append(f"  ...他{len(items)-5}品")
    lines.append(f"\n💡 値下げ・セット販売・廃棄を検討してください")
    sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

async def cmd_online_sales(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """GrabMart/GrabFood vs 店舗売上を比較。"""
    chat_id = update.effective_chat.id
    data = get_online_vs_store_sales(chat_id, days=7)
    store = data['store']
    online = data['online']
    total_sales = store['sales'] + online['sales']
    if total_sales == 0:
        sent = await update.message.reply_text("📊 売上データがありません。UTAKのTransactions CSVを送ってください。")
        save_bot_message(chat_id, sent.message_id)
        return
    online_pct = online['sales'] / total_sales * 100 if total_sales > 0 else 0
    store_pct = store['sales'] / total_sales * 100 if total_sales > 0 else 0
    lines = [f"📊 Online vs Store Sales (Past 7 Days)\n━━━━━━━━━━━━━━━━━━━"]
    lines.append(f"\n🏪 Store: ₱{store['sales']:,.0f} ({store_pct:.0f}%) | {store['qty']:.0f} items")
    lines.append(f"📱 Online: ₱{online['sales']:,.0f} ({online_pct:.0f}%) | {online['qty']:.0f} items")
    if online['by_platform']:
        lines.append(f"\n📱 Online Breakdown:")
        for platform, pdata in sorted(online['by_platform'].items(), key=lambda x: x[1]['sales'], reverse=True):
            lines.append(f"  • {platform}: ₱{pdata['sales']:,.0f} / {pdata['qty']:.0f} items")
    lines.append(f"\n💰 Total: ₱{total_sales:,.0f}")
    sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

async def cmd_hourly_sales(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """時間帯別売れ筋を表示。"""
    chat_id = update.effective_chat.id
    hourly = get_hourly_sales(chat_id, days=7)
    if not hourly:
        sent = await update.message.reply_text("📊 売上データがありません。")
        save_bot_message(chat_id, sent.message_id)
        return
    lines = ["⏰ Sales by Hour (Past 7 Days)\n━━━━━━━━━━━━━━━━━━━"]
    max_sales = max(h['sales'] for h in hourly) if hourly else 1
    for h in hourly:
        bar_len = int(h['sales'] / max_sales * 10) if max_sales > 0 else 0
        bar = '█' * bar_len + '░' * (10 - bar_len)
        top_str = ', '.join(f"{name}({qty:.0f})" for name, qty in h['top_items'][:2])
        lines.append(f"{h['label']} {bar} ₱{h['sales']:,.0f}")
        if top_str:
            lines.append(f"       Top: {top_str}")
    # Find peak hours
    peak = sorted(hourly, key=lambda x: x['sales'], reverse=True)[:3]
    peak_labels = ', '.join(h['label'].strip() for h in peak)
    lines.append(f"\n🔥 Peak hours: {peak_labels}")
    sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

async def cmd_bundle_suggestions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """一緒に買われている商品ペアを表示。"""
    chat_id = update.effective_chat.id
    pairs = get_frequently_bought_together(chat_id, days=14, min_count=2)
    if not pairs:
        sent = await update.message.reply_text("📊 セット販売データが不足しています。もう少し売上データが溜まるとペアが見えてきます。")
        save_bot_message(chat_id, sent.message_id)
        return
    lines = ["🛒 Frequently Bought Together (Past 14 Days)\n━━━━━━━━━━━━━━━━━━━"]
    for i, p in enumerate(pairs, 1):
        lines.append(f"{i}. {p['item_a']}\n   + {p['item_b']}\n   → {p['count']} times together")
    lines.append(f"\n💡 Consider bundle discounts for these pairs!")
    sent = await update.message.reply_text("\n".join(lines))
    save_bot_message(chat_id, sent.message_id)

# ─── UTAK auto-sync job ───────────────────────────────────
async def utak_auto_sync(context):
    """毎日自動でUTAKからCSVをダウンロードし、DBに取り込む。"""
    if not UTAK_EMAIL or not UTAK_PASSWORD:
        logger.info("UTAK credentials not set — skipping auto-sync")
        return
    chat_id = OWNER_CHAT_ID or WEEKLY_REPORT_CHAT_ID
    if not chat_id:
        logger.warning("No chat_id for UTAK sync notification")
        return
    logger.info("UTAK auto-sync starting...")
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(accept_downloads=True)
            page = await ctx.new_page()
            # Login
            await page.goto('https://utak.io/login', timeout=30000)
            await asyncio.sleep(3)
            await page.fill('input[type=text]', UTAK_EMAIL)
            await page.fill('input[type=password]', UTAK_PASSWORD)
            await page.click('button:has-text("Log in")')
            await asyncio.sleep(8)
            results = []
            # Download Inventory CSV
            try:
                await page.click('a[href="/inventory"]')
                await asyncio.sleep(6)
                async with page.expect_download(timeout=30000) as dl_info:
                    await page.click('button:has-text("Download")')
                download = await dl_info.value
                inv_path = '/tmp/utak_inventory_auto.csv'
                await download.save_as(inv_path)
                with open(inv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                inv_count = import_utak_inventory_csv(chat_id, rows)
                results.append(f"📦 在庫: {inv_count}品目")
                logger.info(f"UTAK inventory sync: {inv_count} items")
            except Exception as e:
                results.append(f"❌ 在庫取得失敗: {e}")
                logger.error(f"UTAK inventory sync failed: {e}")
            # Download Transactions CSV
            try:
                await page.click('a[href="/transactions"]')
                await asyncio.sleep(6)
                async with page.expect_download(timeout=30000) as dl_info:
                    await page.click('button:has-text("Download")')
                download = await dl_info.value
                txn_path = '/tmp/utak_transactions_auto.csv'
                await download.save_as(txn_path)
                with open(txn_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                txn_count = import_utak_sales_csv(chat_id, rows)
                results.append(f"💰 売上: {txn_count}件")
                logger.info(f"UTAK sales sync: {txn_count} items")
            except Exception as e:
                results.append(f"❌ 売上取得失敗: {e}")
                logger.error(f"UTAK sales sync failed: {e}")
            await browser.close()
        # Notify
        now = datetime.now(PHT).strftime('%Y-%m-%d %H:%M')
        summary = get_utak_inventory_summary(chat_id)
        low = get_utak_low_stock(chat_id, threshold=5)
        msg = f"🔄 UTAK自動同期完了（{now}）\n" + "\n".join(results)
        if low:
            msg += f"\n\n🟡 在庫残少: {len(low)}品"
            for it in low[:5]:
                msg += f"\n  • {it['item_name']}: 残{it['ending']:.0f}個"
        await context.bot.send_message(chat_id=chat_id, text=msg)
    except Exception as e:
        logger.error(f"UTAK auto-sync failed: {e}")
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ UTAK自動同期エラー: {e}")
        except Exception:
            pass

def _is_tuesday_before_1st_or_3rd_wednesday() -> bool:
    """今日が第1水曜または第3水曜の前日（火曜）かどうか判定。"""
    today = datetime.now(PHT).date()
    if today.weekday() != 1:  # 1 = Tuesday
        return False
    wednesday = today + timedelta(days=1)
    # 第何週の水曜日か: (day-1)//7 + 1
    week_num = (wednesday.day - 1) // 7 + 1
    return week_num in (1, 3)

REORDER_CHAT_ID = -4845840580

async def auto_reorder_job(context):
    """第1・第3水曜の前日火曜に自動仕入れリストを生成・送信。"""
    if not _is_tuesday_before_1st_or_3rd_wednesday():
        return
    chat_id = REORDER_CHAT_ID
    if not chat_id:
        return
    logger.info("Auto reorder: generating procurement list...")
    # まずUTAKデータを最新に同期（もしcredentialがあれば）
    if UTAK_EMAIL and UTAK_PASSWORD:
        await utak_auto_sync(context)
    reorder = get_utak_reorder_list(chat_id)
    if not reorder:
        await context.bot.send_message(chat_id=chat_id, text="📋 仕入れリスト自動生成: UTAKデータが不足しています。")
        return
    # Build message (English for manager)
    now = datetime.now(PHT)
    tomorrow = now + timedelta(days=1)
    week_num = (tomorrow.day - 1) // 7 + 1
    ordinal = {1: '1st', 2: '2nd', 3: '3rd'}.get(week_num, f'{week_num}th')
    lines = [f"📋 Reorder List for {ordinal} Wednesday {tomorrow.strftime('%m/%d')}\n━━━━━━━━━━━━━━━━━━━"]
    urgent = [r for r in reorder if r['priority'] == '🔴']
    warning = [r for r in reorder if r['priority'] == '🟡']
    normal = [r for r in reorder if r['priority'] == '🟢']
    if urgent:
        lines.append(f"\n🔴 URGENT (out of stock soon): {len(urgent)} items")
        for it in urgent[:20]:
            stock_str = f"{it['stock']:.0f} left" if it['stock'] > 0 else "OUT OF STOCK"
            lines.append(f"  • {it['item_name']} ({it['category']})")
            lines.append(f"    {stock_str} | {it['daily_rate']:.1f}/day | {it['days_left']:.0f} days left")
    if warning:
        lines.append(f"\n🟡 WARNING (runs out within 7 days): {len(warning)} items")
        for it in warning[:20]:
            lines.append(f"  • {it['item_name']} ({it['category']})")
            lines.append(f"    {it['stock']:.0f} left | {it['daily_rate']:.1f}/day | {it['days_left']:.0f} days left")
    if normal:
        lines.append(f"\n🟢 Regular restock: {len(normal)} items")
        for it in normal[:15]:
            lines.append(f"  • {it['item_name']}: {it['stock']:.0f} left | {it['daily_rate']:.1f}/day")
        if len(normal) > 15:
            lines.append(f"  ...and {len(normal)-15} more (see CSV)")
    lines.append(f"\nTotal: 🔴{len(urgent)} + 🟡{len(warning)} + 🟢{len(normal)} = {len(reorder)} items")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    # CSV (English headers)
    csv_buf = io.StringIO()
    csv_buf.write('\ufeff')
    writer = csv.writer(csv_buf)
    writer.writerow(['Priority', 'Category', 'Item', 'Current Stock', 'Daily Sales', 'Days Left', '14-Day Total Sales'])
    for it in reorder:
        writer.writerow([it['priority'], it['category'], it['item_name'],
                         f"{it['stock']:.0f}", f"{it['daily_rate']:.1f}",
                         f"{it['days_left']:.0f}" if it['days_left'] < 999 else '-',
                         f"{it['total_sold_14d']:.0f}"])
    csv_bytes = csv_buf.getvalue().encode('utf-8-sig')
    date_str = tomorrow.strftime('%Y%m%d')
    await context.bot.send_document(
        chat_id=chat_id,
        document=io.BytesIO(csv_bytes),
        filename=f"reorder_{date_str}.csv",
        caption=f"📋 Reorder List CSV ({ordinal} Wed {tomorrow.strftime('%m/%d')})"
    )
    logger.info(f"Auto reorder sent: {len(reorder)} items")

# ─── Main ──────────────────────────────────────────────────
def main():
    init_db()

    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set")
    if not ANTHROPIC_KEY:
        raise ValueError("ANTHROPIC_API_KEY is not set")

    app = Application.builder().token(BOT_TOKEN).build()

    # Diagnostic: log ALL incoming updates before any handler processes them
    async def _log_raw_update(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id if update.effective_chat else 'N/A'
        msg = update.effective_message
        msg_type = type(msg).__name__ if msg else 'None'
        text_preview = (msg.text or msg.caption or '')[:40] if msg else ''
        logger.info(f"RAW UPDATE | chat={chat_id} | msg_type={msg_type} | preview={text_preview!r}")
    app.add_handler(TypeHandler(Update, _log_raw_update), group=-1)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(handle_procurement_callback, pattern=r'^proc_'))

    # Schedule auto weekly report every Monday 8:00 AM PHT (UTC+8 = UTC 0:00)
    if WEEKLY_REPORT_CHAT_ID and app.job_queue:
        app.job_queue.run_daily(
            auto_weekly_report_job,
            time=dtime(8, 0, tzinfo=PHT),
            days=(1,),  # PTB v20+: cron scheme where 1 = Monday
            name='auto_weekly_report',
        )
        logger.info(f"Weekly auto-report scheduled: Monday 08:00 PHT → chat_id={WEEKLY_REPORT_CHAT_ID}")
        # Monthly auto-report + staff performance (fire daily at 8am, jobs check if day==1)
        app.job_queue.run_daily(
            auto_monthly_report_job,
            time=dtime(8, 0, tzinfo=PHT),
            name='auto_monthly_report',
        )
        app.job_queue.run_daily(
            auto_staff_performance_job,
            time=dtime(8, 0, tzinfo=PHT),
            name='auto_staff_performance',
        )
        logger.info("Monthly report + staff performance scheduled: 1st of month 08:00 PHT")
        # Procurement auto-recommendation: daily 20:00 PHT, sends if tomorrow is restock day
        app.job_queue.run_daily(
            auto_procurement_job,
            time=dtime(20, 0, tzinfo=PHT),
            name='auto_procurement',
        )
        logger.info("Procurement auto-recommendation scheduled: daily 20:00 PHT check")
        # UTAK auto-sync: daily 01:00 PHT (after midnight close)
        if UTAK_EMAIL and UTAK_PASSWORD:
            app.job_queue.run_daily(
                utak_auto_sync,
                time=dtime(1, 0, tzinfo=PHT),
                name='utak_auto_sync',
            )
            logger.info("UTAK auto-sync scheduled: daily 01:00 PHT")
        else:
            logger.info("UTAK credentials not set — auto-sync disabled")
        # Auto reorder list: daily 20:00 PHT check (fires only on Tuesdays before 1st/3rd Wednesday)
        app.job_queue.run_daily(
            auto_reorder_job,
            time=dtime(20, 0, tzinfo=PHT),
            name='auto_reorder',
        )
        logger.info("Auto reorder scheduled: Tue 20:00 PHT before 1st/3rd Wed")
    elif not WEEKLY_REPORT_CHAT_ID:
        logger.info("WEEKLY_REPORT_CHAT_ID not set — auto weekly report disabled")

    logger.info("Bot started.")

    _railway_domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')
    _webhook_url    = os.environ.get('WEBHOOK_URL', f'https://{_railway_domain}' if _railway_domain else '')
    _port           = int(os.environ.get('PORT', 8080))

    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()

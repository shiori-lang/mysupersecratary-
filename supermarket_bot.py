"""
Supermarket Sales Analysis Telegram Bot
スーパー売上レポート自動分析ボット
"""

import os
import re
import io
import csv
import sqlite3
import asyncio
import logging
import pathlib
import calendar
from datetime import datetime, timedelta, time as dtime, timezone
from typing import Optional

import anthropic
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from telegram import Update
from telegram.ext import (
    Application, MessageHandler,
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

# Philippines Time (UTC+8) — used for scheduling
PHT = timezone(timedelta(hours=8))

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
            raw_text          TEXT,
            chat_id           INTEGER,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, store, chat_id)
        )
    ''')
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
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

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

def get_daily_target(chat_id: int, date_str: str) -> float:
    """Return the day-of-week-specific daily target, falling back to generic 'daily'."""
    try:
        wd = datetime.strptime(date_str, '%Y-%m-%d').weekday()  # 0=Mon … 6=Sun
    except ValueError:
        wd = datetime.now().weekday()
    if wd <= 3:   target_type = 'daily_mon_thu'   # Mon-Thu
    elif wd == 4: target_type = 'daily_fri'        # Fri
    else:         target_type = 'daily_sat_sun'    # Sat-Sun
    v = get_target(chat_id, target_type)
    return v if v > 0 else get_target(chat_id, 'daily')

def is_supermarket_report(text: str) -> bool:
    t = text.lower()
    checks = [
        'cash sale' in t,
        'for deposit' in t,
        'maya' in t,
        'card sale' in t or 'credit' in t,
        'previous sales' in t,
        'morning' in t,
        'transaction' in t,
        'date today' in t,
    ]
    return sum(checks) >= 4

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

    gv = re.search(r'Grave\s*yard(?:\s*shift)?\s*:?\s*[₱]?\s*([\d,]+\.?\d*)', text, re.IGNORECASE)
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

    return d

# ─── DB helpers ────────────────────────────────────────────
def save_record(data: dict, raw_text: str, chat_id: int):
    conn = get_conn()
    c = conn.cursor()
    # Cross-chat deduplication for STORE_GROUP_IDS linked groups
    # 同じ日付・店舗のレコードが他のリンクグループに存在する場合は削除して新しい方で上書き
    ids = get_chat_ids(chat_id)
    if len(ids) > 1:
        placeholders_dup = ','.join('?' * len(ids))
        c.execute(
            f'SELECT chat_id FROM supermarket_sales WHERE date=? AND store=? AND chat_id IN ({placeholders_dup}) AND chat_id != ?',
            (data['date'], data['store'], *ids, chat_id)
        )
        dup = c.fetchone()
        if dup:
            c.execute('DELETE FROM supermarket_sales WHERE date=? AND store=? AND chat_id=?',
                      (data['date'], data['store'], dup[0]))
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
             other_expense, cashbox, for_deposit,
             cat_instant_food, cat_seasoning, cat_grabmart, cat_frozen_item,
             cat_personal_care, cat_beverage, cat_snacks_candies, cat_chilled_item,
             cat_medicine, cat_bento, cat_rice_noodle_bread, cat_grabfood,
             cat_rte, cat_ice_cream, cat_bath_item,
             raw_text, chat_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
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
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    start = last_monday.strftime('%Y-%m-%d')
    end   = last_sunday.strftime('%Y-%m-%d')
    conn = get_conn()
    c = conn.cursor()
    ids = get_chat_ids(chat_id)
    placeholders = ','.join('?' * len(ids))
    c.execute(f'''SELECT * FROM supermarket_sales
                 WHERE chat_id IN ({placeholders}) AND date>=? AND date<=?
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
    c.execute(f'''SELECT * FROM supermarket_sales
                 WHERE chat_id IN ({placeholders}) AND date>=? AND date<=?
                 ORDER BY date ASC''', (*ids, start, end))
    rows = c.fetchall()
    col_names = [d[0] for d in c.description]
    conn.close()
    return [dict(zip(col_names, r)) for r in rows], start, end

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

def get_bot_messages(chat_id: int, limit: int = None) -> list:
    conn = get_conn()
    c = conn.cursor()
    if limit:
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

def translate_text(text: str) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    prompt = (
        "Translate the following text. "
        "If Japanese, translate to English. If English, translate to Japanese. "
        "Return translation only, no explanation.\n\nText: " + text
    )
    resp = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.content[0].text.strip()

def ai_chat(text: str) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    resp = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1000,
        system="あなたは「みどりのマート」のマネジメントです。店舗運営・売上・スタッフ管理などについて、マネージャーの立場で実践的にアドバイスしてください。ユーザーが書いた言語と同じ言語で回答し、簡潔に答えてください。",
        messages=[{"role": "user", "content": text}]
    )
    return resp.content[0].text.strip()


# ─── Alerts ────────────────────────────────────────────────
def _has_graveyard_shift(date_str: str) -> bool:
    """金〜日（weekday 4-6）のみGraveyardシフトあり"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').weekday() >= 4
    except Exception:
        return False  # 日付不明の場合はGYシフトなしとみなす

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
def generate_ai_comment(data: dict, prev: Optional[dict]) -> str:
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        total = data['total'] if data['total'] > 0 else 1
        shift_total = data['morning'] + data['afternoon'] + data['graveyard']
        comp = ""
        if prev and prev['total'] > 0:
            pct = (data['total'] - prev['total']) / prev['total'] * 100
            comp = f"前日比: {pct:+.1f}%"

        has_gy = _has_graveyard_shift(data['date'])
        shift_note = (
            "【シフト体制】月〜木: Morning・Afternoonの2シフトのみ。金〜日: Morning・Afternoon・Graveyardの3シフト。"
            + ("※本日はGraveyardシフトあり（金〜日）。" if has_gy else "※本日はGraveyardシフトなし（月〜木）のため、GY売上0は正常です。")
        )

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

        resp = client.messages.create(
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
def format_daily_report(data: dict, prev: Optional[dict], comments: str, alerts: list, daily_target: float = 0.0, monthly_target: float = 0.0) -> str:
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
    if data['monthly_total'] > 0:
        if monthly_target > 0:
            m_ach    = data['monthly_total'] / monthly_target * 100
            m_filled = min(int(m_ach // 10), 10)
            m_bar    = "🟩" * m_filled + "⬜" * (10 - m_filled)
            monthly_line = f"\n⭐️ 月間累計: ₱{data['monthly_total']:,.0f}  🎯{m_ach:.1f}% {m_bar}"
        else:
            monthly_line = f"\n⭐️ 月間累計: ₱{data['monthly_total']:,.0f}"

    target_line = ""
    if daily_target > 0:
        ach = data['total'] / daily_target * 100
        filled = min(int(ach // 10), 10)
        bar = "🟩" * filled + "⬜" * (10 - filled)
        target_line = f"\n🎯 日次目標達成率: {ach:.1f}% {bar}\n   ₱{data['total']:,.0f} / 目標 ₱{daily_target:,.0f}"

    return f"""🏪 {data['store']} - 日次分析レポート{monthly_line}{prev_line}
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
    weekly_target = get_target(chat_id, 'weekly')
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
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
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

        resp_en = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt_en}]
        )
        resp = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        sent2 = await bot.send_message(chat_id=chat_id, text=f"【11. 来週のアクション項目】\n{resp.content[0].text.strip()}")
        save_bot_message(chat_id, sent2.message_id)
        sent2_en = await bot.send_message(chat_id=chat_id, text=f"[11. Action Items for Next Week]\n{resp_en.content[0].text.strip()}")
        save_bot_message(chat_id, sent2_en.message_id)
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
    records = get_records(chat_id, days=7)
    await _send_weekly_report(ctx.bot, chat_id, records, label="今週（直近7日）")


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
    monthly_target = get_target(chat_id, 'monthly')
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
    else:
        m = re.search(r'(\d+)\s*件', text)
        limit = int(m.group(1)) if m else 1
        message_ids = get_bot_messages(chat_id, limit=limit)

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
        reply = ai_chat(text)
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
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        resp = client.messages.create(
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
🗑️ 「2026-03-08のレポートを削除」— 日付指定削除"""

# ─── Main message handler ──────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text    = update.message.text
    chat_id = update.effective_chat.id

    # 1) 売上レポートの自動検知
    if is_supermarket_report(text):
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
            await update.message.reply_text(f"🔍 レポートを受信しました（日付: {data['date']}）。分析中...")
            prev         = get_previous(data['date'], data['store'], chat_id)
            save_record(data, text, chat_id)
            await check_sales_anomaly(update.get_bot(), chat_id, data.get('date', ''), data.get('total', 0))
            alerts       = check_alerts(data, prev)
            comments     = generate_ai_comment(data, prev)
            daily_target   = get_daily_target(chat_id, data.get('date', ''))
            monthly_target = get_target(chat_id, 'monthly')
            reply          = format_daily_report(data, prev, comments, alerts, daily_target, monthly_target)
            sent = await update.message.reply_text(reply)
            save_bot_message(chat_id, sent.message_id)
        except Exception as e:
            logger.error(f"Report error: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 分析中にエラーが発生しました: {str(e)}")
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
                translated = translate_text(text)
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
    else:
        try:
            reply_text = ai_chat(text)
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
async def missing_report_reminder_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs daily at 00:15 PHT — alerts if no report submitted for yesterday."""
    if not WEEKLY_REPORT_CHAT_ID:
        return
    # At 00:15 the calendar has just rolled over, so check yesterday
    yesterday = (datetime.now(PHT) - timedelta(days=1)).strftime('%Y-%m-%d')
    conn  = get_conn()
    c     = conn.cursor()
    ids   = get_chat_ids(WEEKLY_REPORT_CHAT_ID)
    placeholders = ','.join('?' * len(ids))
    c.execute(
        f'SELECT COUNT(*) FROM supermarket_sales WHERE chat_id IN ({placeholders}) AND date=?',
        (*ids, yesterday)
    )
    count = c.fetchone()[0]
    conn.close()
    if count > 0:
        logger.info(f"missing_report_reminder: report exists for {yesterday}, skipping")
        return
    logger.info(f"missing_report_reminder: no report for {yesterday}, sending alerts")
    group_msg = (
        f"⚠️ レポート未提出リマインダー\n"
        f"昨日（{yesterday}）の売上レポートがまだ提出されていません。\n"
        f"担当マネージャーは早急に提出をお願いします。"
    )
    try:
        await ctx.bot.send_message(chat_id=WEEKLY_REPORT_CHAT_ID, text=group_msg)
    except Exception as e:
        logger.error(f"missing_report_reminder group message error: {e}")
    await _dm_managers(
        ctx.bot,
        f"⚠️ Reminder: Yesterday's ({yesterday}) sales report has not been submitted yet. "
        f"Please submit it as soon as possible!"
    )

async def auto_weekly_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Runs every Monday 8:00 AM PHT — sends previous Mon–Sun report to WEEKLY_REPORT_CHAT_ID."""
    if not WEEKLY_REPORT_CHAT_ID:
        logger.warning("auto_weekly_report_job: WEEKLY_REPORT_CHAT_ID not set, skipping")
        return
    try:
        records, start, end = get_last_week_records(WEEKLY_REPORT_CHAT_ID)
        if not records:
            logger.info(f"auto_weekly_report_job: no records for {start} - {end}, skipping")
            return
        logger.info(f"auto_weekly_report_job: sending report for {start} - {end} to {WEEKLY_REPORT_CHAT_ID}")
        await _send_weekly_report(ctx.bot, WEEKLY_REPORT_CHAT_ID, records, label=f"先週（{start} 〜 {end}）自動レポート")
    except Exception as e:
        logger.error(f"auto_weekly_report_job failed: {e}")

# ─── Target commands ────────────────────────────────────────
async def cmd_set_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    # Try to find amount after ₱/¥, or after 目標/target keyword, or after を particle
    amount_m = (
        re.search(r'[₱¥]\s*([\d,]+(?:\.\d+)?)', text) or
        re.search(r'(?:目標|target)\D{0,15}?(\d[\d,]+(?:\.\d+)?)', text, re.IGNORECASE) or
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

# ─── Main ──────────────────────────────────────────────────
def main():
    init_db()

    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set")
    if not ANTHROPIC_KEY:
        raise ValueError("ANTHROPIC_API_KEY is not set")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Schedule auto weekly report every Monday 8:00 AM PHT (UTC+8 = UTC 0:00)
    if WEEKLY_REPORT_CHAT_ID and app.job_queue:
        app.job_queue.run_daily(
            auto_weekly_report_job,
            time=dtime(8, 0, tzinfo=PHT),
            days=(0,),  # 0 = Monday
            name='auto_weekly_report',
        )
        logger.info(f"Weekly auto-report scheduled: Monday 08:00 PHT → chat_id={WEEKLY_REPORT_CHAT_ID}")
        app.job_queue.run_daily(
            missing_report_reminder_job,
            time=dtime(0, 15, tzinfo=PHT),
            name='missing_report_reminder',
        )
        logger.info(f"Missing report reminder scheduled: daily 00:15 PHT → chat_id={WEEKLY_REPORT_CHAT_ID}")
    elif not WEEKLY_REPORT_CHAT_ID:
        logger.info("WEEKLY_REPORT_CHAT_ID not set — auto weekly report disabled")

    logger.info("Bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()

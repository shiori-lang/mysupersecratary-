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
from datetime import datetime, timedelta
from typing import Optional

import anthropic
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, CommandHandler,
    filters, ContextTypes
)

# ─── Logging ───────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────
BOT_TOKEN       = os.environ.get('TELEGRAM_BOT_TOKEN', '')
ANTHROPIC_KEY   = os.environ.get('ANTHROPIC_API_KEY', '')
DB_PATH         = os.environ.get('DB_PATH', 'sales_data.db')

# ─── Database ──────────────────────────────────────────────
def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS supermarket_sales (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            date             TEXT,
            store            TEXT,
            submitted_by     TEXT,
            cash_sale        REAL DEFAULT 0,
            card_sale        REAL DEFAULT 0,
            qr_ph            REAL DEFAULT 0,
            maya             REAL DEFAULT 0,
            grab             REAL DEFAULT 0,
            graveyard        REAL DEFAULT 0,
            morning          REAL DEFAULT 0,
            afternoon        REAL DEFAULT 0,
            discounts        REAL DEFAULT 0,
            wastage          REAL DEFAULT 0,
            total            REAL DEFAULT 0,
            monthly_total    REAL DEFAULT 0,
            transaction_count INTEGER DEFAULT 0,
            salary           REAL DEFAULT 0,
            inventory        REAL DEFAULT 0,
            other_expense    REAL DEFAULT 0,
            cashbox          REAL DEFAULT 0,
            for_deposit      REAL DEFAULT 0,
            raw_text         TEXT,
            chat_id          INTEGER,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

# ─── Parser ────────────────────────────────────────────────
def _num(text: str, field: str) -> float:
    """Extract number after a field label."""
    pattern = rf'{re.escape(field)}\s*:?\s*[₱Pp]?\s*([\d,]+\.?\d*)'
    m = re.search(pattern, text, re.IGNORECASE)
    return float(m.group(1).replace(',', '')) if m else 0.0

def is_supermarket_report(text: str) -> bool:
    keywords = ['PREVIOUS SALES', 'CASH SALE', 'CREDIT/CARD SALE',
                'MAYA', 'Grave yard', 'FOR DEPOSIT']
    return sum(1 for kw in keywords if kw.lower() in text.lower()) >= 4

def parse_report(text: str) -> dict:
    d = {}

    # Store name (first non-empty line)
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    d['store'] = lines[0] if lines else 'Unknown Store'

    # Submitter
    m = re.search(r'This is (\w+) from', text, re.IGNORECASE)
    d['submitted_by'] = m.group(1) if m else 'Staff'

    # Date
    m = re.search(r'DATE TODAY\s*:?\s*(.+)', text, re.IGNORECASE)
    if m:
        raw_date = m.group(1).strip()
        # Try to parse common formats
        for fmt in ('%m/%d/%Y', '%B %d, %Y', '%d/%m/%Y', '%Y-%m-%d'):
            try:
                d['date'] = datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d')
                break
            except ValueError:
                continue
        else:
            d['date'] = raw_date  # keep as-is if unparseable
    else:
        d['date'] = datetime.now().strftime('%Y-%m-%d')

    d['cash_sale']   = _num(text, 'CASH SALE')
    d['card_sale']   = _num(text, 'CREDIT/CARD SALE')
    d['qr_ph']       = _num(text, 'QR PH')
    d['maya']        = _num(text, 'MAYA')
    d['grab']        = _num(text, 'Grab')
    d['graveyard']   = _num(text, 'Grave yard shift')
    d['morning']     = _num(text, 'Morning shift')
    d['afternoon']   = _num(text, 'Afternoon Shift')
    d['discounts']   = _num(text, 'Discounts')
    d['wastage']     = _num(text, 'Wastage/Disposal')
    d['total']       = _num(text, 'TOTAL')

    # Monthly section – "TOTAL CASH" under MARCH SALES block
    m = re.search(r'TOTAL CASH\s*:?\s*[₱]?\s*([\d,]+)', text, re.IGNORECASE)
    d['monthly_total'] = float(m.group(1).replace(',', '')) if m else 0.0

    d['transaction_count'] = int(_num(text, 'Transaction count'))
    d['salary']        = _num(text, 'EMPLOYEE SALARY PER DAY')
    d['inventory']     = _num(text, 'INVENTORY SUPPLIES')
    d['other_expense'] = _num(text, 'OTHER EXPENSE')
    d['cashbox']       = _num(text, 'CASHBOX CASH')
    d['for_deposit']   = _num(text, 'FOR DEPOSIT')

    return d

# ─── DB helpers ────────────────────────────────────────────
def save_record(data: dict, raw_text: str, chat_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        INSERT INTO supermarket_sales
        (date, store, submitted_by, cash_sale, card_sale, qr_ph, maya, grab,
         graveyard, morning, afternoon, discounts, wastage, total,
         monthly_total, transaction_count, salary, inventory, other_expense,
         cashbox, for_deposit, raw_text, chat_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        data['date'], data['store'], data['submitted_by'],
        data['cash_sale'], data['card_sale'], data['qr_ph'], data['maya'],
        data['grab'], data['graveyard'], data['morning'], data['afternoon'],
        data['discounts'], data['wastage'], data['total'],
        data['monthly_total'], data['transaction_count'],
        data['salary'], data['inventory'], data['other_expense'],
        data['cashbox'], data['for_deposit'], raw_text, chat_id
    ))
    conn.commit()
    conn.close()

def get_previous(date: str, store: str, chat_id: int) -> Optional[dict]:
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        SELECT * FROM supermarket_sales
        WHERE date < ? AND store = ? AND chat_id = ?
        ORDER BY date DESC LIMIT 1
    ''', (date, store, chat_id))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    cols = [d[0] for d in c.description] if c.description else []
    # rebuild description from known schema order
    col_names = ['id','date','store','submitted_by','cash_sale','card_sale',
                 'qr_ph','maya','grab','graveyard','morning','afternoon',
                 'discounts','wastage','total','monthly_total',
                 'transaction_count','salary','inventory','other_expense',
                 'cashbox','for_deposit','raw_text','chat_id','created_at']
    return dict(zip(col_names, row))

def get_records(chat_id: int, store: str = None, days: int = 30) -> list:
    conn = get_conn()
    c = conn.cursor()
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    if store:
        c.execute('''SELECT * FROM supermarket_sales
                     WHERE chat_id=? AND store=? AND date>=?
                     ORDER BY date ASC''', (chat_id, store, since))
    else:
        c.execute('''SELECT * FROM supermarket_sales
                     WHERE chat_id=? AND date>=?
                     ORDER BY date ASC''', (chat_id, since))
    rows = c.fetchall()
    conn.close()
    col_names = ['id','date','store','submitted_by','cash_sale','card_sale',
                 'qr_ph','maya','grab','graveyard','morning','afternoon',
                 'discounts','wastage','total','monthly_total',
                 'transaction_count','salary','inventory','other_expense',
                 'cashbox','for_deposit','raw_text','chat_id','created_at']
    return [dict(zip(col_names, r)) for r in rows]

# ─── Alerts ────────────────────────────────────────────────
def check_alerts(data: dict, prev: Optional[dict]) -> list:
    alerts = []
    total = data['total'] or 1

    if prev and prev['total'] > 0:
        pct = (data['total'] - prev['total']) / prev['total'] * 100
        if pct <= -15:
            alerts.append(f"⚠️ 前日比{pct:+.1f}%：要因確認を推奨（天候/イベント影響？）")

        if prev.get('transaction_count', 0) > 0:
            tx_pct = (data['transaction_count'] - prev['transaction_count']) / prev['transaction_count'] * 100
            if tx_pct <= -20:
                alerts.append(f"👥 客数減{tx_pct:+.1f}%：プロモーション検討を推奨")

        if prev.get('graveyard', 0) > 0:
            g_pct = (data['graveyard'] - prev['graveyard']) / prev['graveyard'] * 100
            if g_pct <= -30:
                alerts.append(f"🌙 Graveyard売上急減{g_pct:+.1f}%：夜間需要の変動確認")

    wastage_pct = data['wastage'] / total * 100
    if wastage_pct > 3:
        alerts.append(f"⚠️ 廃棄率{wastage_pct:.1f}%：発注量見直しを検討")

    cash_pct = data['cash_sale'] / total * 100
    if cash_pct > 50:
        alerts.append(f"💵 現金比率{cash_pct:.1f}%：集金タイミングの確認")

    return alerts

# ─── Claude コメント生成 ────────────────────────────────────
def generate_ai_comment(data: dict, prev: Optional[dict]) -> str:
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        total = data['total'] or 1
        shift_total = data['morning'] + data['afternoon'] + data['graveyard']

        comp = ""
        if prev and prev['total'] > 0:
            pct = (data['total'] - prev['total']) / prev['total'] * 100
            comp = f"前日比: {pct:+.1f}%"

        prompt = f"""売上データを分析し、{data['submitted_by']}さんへの短いコメントを3点、日本語の箇条書きで生成してください。
ポジティブな点と改善提案を含めてください。コメントのみ返答してください。

売上: ₱{total:,.0f} | 取引: {data['transaction_count']}件 | 平均: ₱{total/max(data['transaction_count'],1):,.0f}
現金比率: {data['cash_sale']/total*100:.1f}% | Grab: {data['grab']/total*100:.1f}% | 廃棄率: {data['wastage']/total*100:.1f}%
Morning: {data['morning']/shift_total*100 if shift_total>0 else 0:.1f}% | Afternoon: {data['afternoon']/shift_total*100 if shift_total>0 else 0:.1f}% | Graveyard: {data['graveyard']/shift_total*100 if shift_total>0 else 0:.1f}%
{comp}"""

        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.error(f"AI comment error: {e}")
        return "・本日もレポートありがとうございます！\n・データを確認しました。"

# ─── Format reply ──────────────────────────────────────────
def format_daily_report(data: dict, prev: Optional[dict], comments: str, alerts: list) -> str:
    total = data['total'] or 1
    shift_total = data['morning'] + data['afternoon'] + data['graveyard']
    avg_tx = total / data['transaction_count'] if data['transaction_count'] > 0 else 0

    def pct(v): return v / total * 100
    def spct(v): return v / shift_total * 100 if shift_total > 0 else 0

    comp_line = ""
    if prev and prev['total'] > 0:
        diff = total - prev['total']
        p = diff / prev['total'] * 100
        comp_line = f"📊 前日比: {p:+.1f}% ({diff:+,.0f}₱)"

    alert_block = ""
    if alerts:
        alert_block = "\n🚨 アラート\n" + "\n".join(f"・{a}" for a in alerts) + "\n"

    msg = f"""🏪 {data['store']} - 日次分析レポート
📅 {data['date']}（{data['submitted_by']}さん提出）
━━━━━━━━━━━━━━━━━━━━━━
💰 売上総額: ₱{total:,.0f}
👥 取引件数: {data['transaction_count']}件（平均単価: ₱{avg_tx:,.0f}）

【決済内訳】
💵 現金:    ₱{data['cash_sale']:>10,.0f} ({pct(data['cash_sale']):.1f}%)
💳 カード:  ₱{data['card_sale']:>10,.0f} ({pct(data['card_sale']):.1f}%)
📱 QR PH:   ₱{data['qr_ph']:>10,.0f} ({pct(data['qr_ph']):.1f}%)
📱 MAYA:    ₱{data['maya']:>10,.0f} ({pct(data['maya']):.1f}%)
🚗 Grab:    ₱{data['grab']:>10,.0f} ({pct(data['grab']):.1f}%)

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
{comp_line}
📈 月間累計: ₱{data['monthly_total']:,.0f}
{alert_block}
💡 {data['submitted_by']}さんへのコメント
{comments}"""
    return msg.strip()

# ─── Chart generators ──────────────────────────────────────
def make_trend_chart(records: list, title: str = "売上トレンド") -> io.BytesIO:
    dates  = [r['date'] for r in records]
    totals = [r['total'] for r in records]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, totals, 'o-', color='#2E86AB', linewidth=2, markersize=5)
    ax.fill_between(range(len(dates)), totals, alpha=0.15, color='#2E86AB')
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha='right', fontsize=8)
    ax.set_title(title, fontsize=14, fontweight='bold', pad=12)
    ax.set_ylabel('売上 (₱)', fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'₱{x:,.0f}'))
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close()
    return buf

def make_payment_chart(records: list) -> io.BytesIO:
    labels = ['現金', 'カード', 'QR PH', 'MAYA', 'Grab']
    totals = [
        sum(r['cash_sale'] for r in records),
        sum(r['card_sale'] for r in records),
        sum(r['qr_ph']    for r in records),
        sum(r['maya']     for r in records),
        sum(r['grab']     for r in records),
    ]
    colors = ['#F18F01', '#C73E1D', '#3B1F2B', '#44BBA4', '#E94F37']

    fig, ax = plt.subplots(figsize=(7, 7))
    wedges, texts, autotexts = ax.pie(
        totals, labels=labels, colors=colors,
        autopct='%1.1f%%', startangle=140,
        wedgeprops=dict(edgecolor='white', linewidth=1.5)
    )
    for t in autotexts:
        t.set_fontsize(9)
    ax.set_title('決済方法別内訳', fontsize=14, fontweight='bold', pad=16)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close()
    return buf

def make_shift_chart(records: list) -> io.BytesIO:
    dates    = [r['date'] for r in records]
    morning  = [r['morning']   for r in records]
    afternoon= [r['afternoon'] for r in records]
    grave    = [r['graveyard'] for r in records]

    x = np.arange(len(dates))
    w = 0.6

    fig, ax = plt.subplots(figsize=(10, 5))
    b1 = ax.bar(x, morning,   w, label='Morning',   color='#FFBE0B')
    b2 = ax.bar(x, afternoon, w, bottom=morning,    label='Afternoon', color='#FB5607')
    b3 = ax.bar(x, grave,     w,
                bottom=[m+a for m,a in zip(morning, afternoon)],
                label='Graveyard', color='#3A86FF')

    ax.set_xticks(x)
    ax.set_xticklabels(dates, rotation=45, ha='right', fontsize=8)
    ax.set_title('シフト別売上（積み上げ）', fontsize=14, fontweight='bold', pad=12)
    ax.set_ylabel('売上 (₱)', fontsize=10)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v,_: f'₱{v:,.0f}'))
    ax.legend(loc='upper left')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    buf.seek(0)
    plt.close()
    return buf

# ─── Commands ──────────────────────────────────────────────
async def cmd_weekly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=7)
    if not records:
        await update.message.reply_text("📭 過去7日のデータがまだありません。")
        return

    total_sum  = sum(r['total'] for r in records)
    avg_daily  = total_sum / len(records)
    cash_sum   = sum(r['cash_sale'] for r in records)
    card_sum   = sum(r['card_sale'] for r in records)
    grab_sum   = sum(r['grab']      for r in records)
    morning_s  = sum(r['morning']   for r in records)
    afternoon_s= sum(r['afternoon'] for r in records)
    grave_s    = sum(r['graveyard'] for r in records)

    text = f"""📅 週次サマリー（直近7日）
━━━━━━━━━━━━━━━━━━━
💰 総売上:    ₱{total_sum:,.0f}
📊 日平均:    ₱{avg_daily:,.0f}

【決済別】
💵 現金: ₱{cash_sum:,.0f} ({cash_sum/total_sum*100:.1f}%)
💳 カード: ₱{card_sum:,.0f} ({card_sum/total_sum*100:.1f}%)
🚗 Grab: ₱{grab_sum:,.0f} ({grab_sum/total_sum*100:.1f}%)

【シフト別】
🌅 Morning:   ₱{morning_s:,.0f}
🌆 Afternoon: ₱{afternoon_s:,.0f}
🌙 Graveyard: ₱{grave_s:,.0f}"""

    await update.message.reply_text(text)

    # Send chart
    buf = make_trend_chart(records, "週次売上トレンド")
    await update.message.reply_photo(photo=buf, caption="📈 週次トレンド")

async def cmd_monthly(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=30)
    if not records:
        await update.message.reply_text("📭 過去30日のデータがまだありません。")
        return

    total_sum = sum(r['total'] for r in records)
    days_count = len(records)

    text = f"""📅 月次レポート（直近30日）
━━━━━━━━━━━━━━━━━━━
💰 総売上: ₱{total_sum:,.0f}
📊 日平均: ₱{total_sum/days_count:,.0f}
📆 営業日数: {days_count}日"""

    await update.message.reply_text(text)

    # Trend + Shift chart
    buf_trend = make_trend_chart(records, "月次売上トレンド（折れ線）")
    await update.message.reply_photo(photo=buf_trend, caption="📈 売上推移")

    buf_shift = make_shift_chart(records)
    await update.message.reply_photo(photo=buf_shift, caption="📊 シフト別積み上げ")

async def cmd_compare(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Usage: /compare shift  or  /compare payment"""
    chat_id = update.effective_chat.id
    args    = ctx.args
    mode    = args[0].lower() if args else 'payment'

    records = get_records(chat_id, days=30)
    if not records:
        await update.message.reply_text("📭 データがまだありません。")
        return

    if mode == 'shift':
        buf = make_shift_chart(records)
        await update.message.reply_photo(photo=buf, caption="📊 シフト別比較（直近30日）")
    else:
        buf = make_payment_chart(records)
        await update.message.reply_photo(photo=buf, caption="💳 決済方法別比較（直近30日）")

async def cmd_trend(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=30)
    if not records:
        await update.message.reply_text("📭 データがまだありません。")
        return

    # Simple trend analysis
    first_half = records[:len(records)//2]
    second_half= records[len(records)//2:]
    avg1 = sum(r['total'] for r in first_half)  / max(len(first_half), 1)
    avg2 = sum(r['total'] for r in second_half) / max(len(second_half), 1)
    trend_pct = (avg2 - avg1) / avg1 * 100 if avg1 > 0 else 0
    trend_emoji = "📈" if trend_pct >= 0 else "📉"

    text = f"""{trend_emoji} トレンド分析（過去30日）
━━━━━━━━━━━━━━━━━━━
前半平均: ₱{avg1:,.0f}
後半平均: ₱{avg2:,.0f}
変化率: {trend_pct:+.1f}%

最高売上: ₱{max(r['total'] for r in records):,.0f}（{max(records, key=lambda r: r['total'])['date']}）
最低売上: ₱{min(r['total'] for r in records):,.0f}（{min(records, key=lambda r: r['total'])['date']}）"""

    await update.message.reply_text(text)
    buf = make_trend_chart(records, "過去30日トレンド")
    await update.message.reply_photo(photo=buf)

async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=90)
    if not records:
        await update.message.reply_text("📭 エクスポートできるデータがありません。")
        return

    output = io.StringIO()
    fields = ['date','store','submitted_by','cash_sale','card_sale','qr_ph',
              'maya','grab','graveyard','morning','afternoon','discounts',
              'wastage','total','transaction_count','salary','inventory',
              'other_expense','cashbox','for_deposit']
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(records)

    output.seek(0)
    filename = f"sales_export_{datetime.now().strftime('%Y%m%d')}.csv"
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode('utf-8-sig')),
        filename=filename,
        caption=f"📊 売上データCSV（直近90日 / {len(records)}件）"
    )

async def cmd_english(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Translate the most recent report to English."""
    chat_id = update.effective_chat.id
    records = get_records(chat_id, days=7)
    if not records:
        await update.message.reply_text("📭 Recent report not found.")
        return

    latest = records[-1]
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        prompt = f"""Generate a professional English daily sales report based on this data:
Store: {latest['store']} | Date: {latest['date']} | Submitted by: {latest['submitted_by']}
Total Sales: ₱{latest['total']:,.0f} | Transactions: {latest['transaction_count']}
Cash: ₱{latest['cash_sale']:,.0f} | Card: ₱{latest['card_sale']:,.0f} | Grab: ₱{latest['grab']:,.0f}
Morning: ₱{latest['morning']:,.0f} | Afternoon: ₱{latest['afternoon']:,.0f} | Graveyard: ₱{latest['graveyard']:,.0f}
Discounts: ₱{latest['discounts']:,.0f} | Wastage: ₱{latest['wastage']:,.0f}
Salary: ₱{latest['salary']:,.0f} | Inventory: ₱{latest['inventory']:,.0f}

Format it as a clear, concise business report with emojis."""
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        await update.message.reply_text(resp.content[0].text.strip())
    except Exception as e:
        logger.error(f"English cmd error: {e}")
        await update.message.reply_text("⚠️ Translation failed. Please try again.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    help_text = """🤖 スーパー売上分析ボット

【自動機能】
レポートをGCに投稿すると自動で分析＆返信します

【コマンド一覧】
/weekly    — 今週のシフト別・決済別サマリー
/monthly   — 今月の推移グラフ
/compare [shift|payment] — 比較グラフ
/trend     — 過去30日トレンド分析
/export    — CSVダウンロード
/english   — 直近レポートの英語版生成
/help      — このヘルプを表示"""
    await update.message.reply_text(help_text)

# ─── Main message handler ──────────────────────────────────
async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text    = update.message.text
    chat_id = update.effective_chat.id

    if not is_supermarket_report(text):
        return  # Not a report, ignore

    await update.message.reply_text("🔍 レポートを受信しました。分析中...")

    try:
        data = parse_report(text)
        prev = get_previous(data['date'], data['store'], chat_id)

        # Save to DB
        save_record(data, text, chat_id)

        # Check alerts
        alerts = check_alerts(data, prev)

        # Generate AI comment
        comments = generate_ai_comment(data, prev)

        # Format and send reply
        reply = format_daily_report(data, prev, comments, alerts)
        await update.message.reply_text(reply)

    except Exception as e:
        logger.error(f"Report processing error: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 分析中にエラーが発生しました: {str(e)}")

# ─── Main ──────────────────────────────────────────────────
def main():
    init_db()

    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set")
    if not ANTHROPIC_KEY:
        raise ValueError("ANTHROPIC_API_KEY is not set")

    app = Application.builder().token(BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("weekly",  cmd_weekly))
    app.add_handler(CommandHandler("monthly", cmd_monthly))
    app.add_handler(CommandHandler("compare", cmd_compare))
    app.add_handler(CommandHandler("trend",   cmd_trend))
    app.add_handler(CommandHandler("export",  cmd_export))
    app.add_handler(CommandHandler("english", cmd_english))
    app.add_handler(CommandHandler("help",    cmd_help))

    # Message handler (auto-detect reports)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        handle_message
    ))

    logger.info("🤖 Supermarket Bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()

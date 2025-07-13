import logging
import os
import sqlite3
import csv
import random
import re
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple, Optional, Dict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from functools import wraps
import traceback
import html
import json
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

# --- Configuration & Constants ---
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_USER_IDS = [5134940733, 8074969502]  # List of allowed user IDs
MESSAGE_DELETION_DELAY = 300  # 5 minutes in seconds
ITEMS_PER_PAGE = 5  # For paginated keyboards

# --- Personality ---
STARTUP_MESSAGES = [
    "Powered up and ready to judge your spending habits.", "I have been summoned. Let's make some money moves.",
    "The financial overlord is online. Try to impress me.",
]
MANUAL_TEXT = (f"<b>🤖 {random.choice(STARTUP_MESSAGES)}</b>\n\n"
               f"<b>📋 COMMAND CENTER</b>\n"
               f"<i>Your financial empire awaits...</i>\n\n"
               
               f"<b>🎯 Goals & Debts</b>\n"
               f"<code>new goal</code> - Set a savings target\n"
               f"<code>new debt</code> - Track debt to pay off\n"
               f"<code>view all</code> - See all goals/debts\n"
               f"<code>delete</code> - Remove goals/debts\n\n"
               
               f"<b>💳 Payment Tracking</b>\n"
               f"<code>new payment</code> - Track ongoing payments\n"
               f"<code>add payment</code> - Log payment made\n"
               f"<code>view payments</code> - See all payments\n"
               f"<code>payment progress</code> - Check payment status\n"
               f"<code>delete payment</code> - Remove payment tracker\n\n"
               
               f"<b>💰 Money Moves</b>\n"
               f"<code>add</code> - Log savings/payments\n"
               f"<code>progress</code> - Check goal progress\n\n"
               
               f"<b>💸 Smart Expense Tracking</b>\n"
               f"<code>add expense</code> - Record spending\n"
               f"<code>expense report</code> - View spending analysis\n"
               f"<code>expense compare</code> - Compare periods\n"
               f"<code>set budget</code> - Create spending limits\n"
               f"<code>budget status</code> - Check budget health\n\n"
               
               f"<b>🏦 Asset Management</b>\n"
               f"<code>add asset</code> - Track investments\n"
               f"<code>update asset</code> - Modify values\n"
               f"<code>view assets</code> - Portfolio overview\n"
               f"<code>delete asset</code> - Remove assets\n"
               f"<code>view all assets</code> - Detailed breakdown\n\n"
               
               f"<b>🔔 Smart Reminders</b>\n"
               f"<code>add reminder</code> - Custom notifications\n"
               f"<code>view reminders</code> - See all alarms\n"
               f"<code>set reminder</code> - Daily savings nudge\n\n"
               
               f"<b>📊 Analytics & More</b>\n"
               f"<code>financial dashboard</code> - Complete overview\n"
               f"<code>trends</code> - Spending patterns\n"
               f"<code>export</code> - Download your data\n\n"
               
               f"<b>⚠️ Nuclear Options</b>\n"
               f"<code>erase all</code> - <i>Delete everything</i>\n"
               f"<code>cancel</code> - Abort current action")

# --- States for ConversationHandler ---
(GOAL_NAME, GOAL_AMOUNT, GOAL_CURRENCY,
 ADD_SAVINGS_GOAL, ADD_SAVINGS_AMOUNT,
 DELETE_GOAL_CONFIRM, REMINDER_TIME,
 DEBT_NAME, DEBT_AMOUNT, DEBT_CURRENCY,
 PROGRESS_GOAL_SELECT, EXPENSE_AMOUNT, EXPENSE_REASON, EXPENSE_CURRENCY, EXPENSE_CATEGORY,
 ASSET_NAME, ASSET_AMOUNT, ASSET_CURRENCY, ASSET_TYPE,
 UPDATE_ASSET_SELECT, UPDATE_ASSET_AMOUNT, DELETE_ASSET_SELECT,
 BUDGET_CATEGORY, BUDGET_AMOUNT, BUDGET_CURRENCY, BUDGET_PERIOD,
 RECURRING_NAME, RECURRING_AMOUNT, RECURRING_CURRENCY, RECURRING_TYPE, RECURRING_CATEGORY, RECURRING_FREQUENCY,
 REMINDER_TITLE, REMINDER_MESSAGE, REMINDER_FREQUENCY,
 PAYMENT_NAME, PAYMENT_RECIPIENT, PAYMENT_TARGET, PAYMENT_CURRENCY, PAYMENT_AMOUNT, PAYMENT_FREQUENCY,
 ADD_PAYMENT_SELECT, ADD_PAYMENT_AMOUNT, DELETE_PAYMENT_SELECT, PAYMENT_PROGRESS_SELECT) = range(45)

# --- Logging ---
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Database & Persistent Storage ---
# Define the persistent data directory for Render
DATA_DIR = "/data"
DB_PATH = os.path.join(DATA_DIR, "savings_bot.db")

def db_connect():
    """Establishes a database connection to the persistent disk."""
    # Ensure the data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            name TEXT NOT NULL UNIQUE, target_amount REAL NOT NULL,
            current_amount REAL DEFAULT 0, currency TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'goal', notified_90_percent BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS savings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, goal_id INTEGER NOT NULL,
            amount REAL NOT NULL, saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (goal_id) REFERENCES goals (id) ON DELETE CASCADE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            amount REAL NOT NULL, currency TEXT NOT NULL,
            reason TEXT NOT NULL, category TEXT DEFAULT 'other',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            name TEXT NOT NULL, amount REAL NOT NULL, currency TEXT NOT NULL,
            asset_type TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            category TEXT NOT NULL, amount REAL NOT NULL, currency TEXT NOT NULL,
            period TEXT NOT NULL DEFAULT 'monthly', current_spent REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recurring_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            name TEXT NOT NULL, amount REAL NOT NULL, currency TEXT NOT NULL,
            type TEXT NOT NULL, category TEXT DEFAULT 'other',
            frequency TEXT NOT NULL, next_due DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            title TEXT NOT NULL, message TEXT, reminder_time TIME NOT NULL,
            frequency TEXT NOT NULL DEFAULT 'daily', is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            name TEXT NOT NULL UNIQUE, target_amount REAL NOT NULL,
            current_amount REAL DEFAULT 0, currency TEXT NOT NULL,
            payment_amount REAL NOT NULL, payment_frequency TEXT NOT NULL DEFAULT 'monthly',
            recipient TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, payment_id INTEGER NOT NULL,
            amount REAL NOT NULL, paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (payment_id) REFERENCES payments (id) ON DELETE CASCADE
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")

# --- UI Formatting & Pagination (No changes from original) ---
def fmt_progress_bar(percentage: float, length: int = 10) -> str:
    if percentage >= 100: return "[🏆🏆🏆🏆🏆🏆🏆🏆🏆]"
    filled_length = int(length * percentage / 100)
    bar = '🟩' * filled_length + '⬛️' * (length - filled_length)
    return f"[{bar}]"

def fmt_goal_list(goals: List[Tuple]) -> str:
    if not goals: return "Your financial dashboard is a blank canvas. Use `new goal` or `new debt` to start."
    message = "Alright, here's the current state of your financial empire:\n\n"
    for goal in goals:
        goal_id, name, target, current, currency, goal_type, _ = goal
        progress_percent = (current / target) * 100 if target > 0 else 0
        progress_bar = fmt_progress_bar(progress_percent)
        remaining = target - current
        if goal_type == 'goal':
            message += (f"🎯 **{name.upper()}** (Goal)\n`{progress_bar} {progress_percent:.1f}%`\n"
                        f"   - **Saved:** `{current:,.2f} / {target:,.2f} {currency}`\n"
                        f"   - **Needs:** `{remaining:,.2f} {currency}`\n\n")
        elif goal_type == 'debt':
            message += (f"⛓️ **{name.upper()}** (Debt)\n`{progress_bar} {progress_percent:.1f}% Paid Off`\n"
                        f"   - **Paid:** `{current:,.2f} / {target:,.2f} {currency}`\n"
                        f"   - **Remaining Debt:** `{remaining:,.2f} {currency}`\n\n")
    return message

def fmt_single_goal_progress(goal: Tuple, recent_transactions: List[Tuple]) -> str:
    goal_id, name, target, current, currency, goal_type, _ = goal
    progress_percent = (current / target) * 100 if target > 0 else 0
    header_emoji = "🎯" if goal_type == 'goal' else "⛓️"
    title = f"{header_emoji} **Progress Report: {name.upper()}**\n"
    animated_bar = fmt_progress_bar(progress_percent, length=15)
    summary = (f"`{animated_bar} {progress_percent:.1f}%`\n\n"
               f"  - **Target:** `{target:,.2f} {currency}`\n"
               f"  - **{'Saved' if goal_type == 'goal' else 'Paid'}:** `{current:,.2f} {currency}`\n"
               f"  - **Remaining:** `{target - current:,.2f} {currency}`\n")
    transactions_log = "\n**Recent Activity:**\n"
    if not recent_transactions:
        transactions_log += "_No recent transactions found._"
    else:
        for trans in recent_transactions:
            amount, date_str = trans
            date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            formatted_date = date_obj.strftime('%b %d, %Y')
            transactions_log += f"`  - {amount:,.2f} {currency} on {formatted_date}`\n"
    return title + summary + transactions_log

def generate_paginated_keyboard(items: List[Tuple], prefix: str, page: int = 0) -> InlineKeyboardMarkup:
    """Creates a paginated inline keyboard."""
    keyboard = []
    start_index = page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE

    for item in items[start_index:end_index]:
        item_id, name, _, _, currency, goal_type, _ = item
        emoji = "🎯" if goal_type == 'goal' else "⛓️"
        button = InlineKeyboardButton(f"{emoji} {name} ({currency})", callback_data=f"{prefix}_{item_id}")
        keyboard.append([button])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"nav_{prefix}_{page - 1}"))
    if end_index < len(items):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"nav_{prefix}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    return InlineKeyboardMarkup(keyboard)

def generate_asset_keyboard(assets: List[Tuple], prefix: str, page: int = 0) -> InlineKeyboardMarkup:
    """Creates a paginated inline keyboard for assets."""
    keyboard = []
    start_index = page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE

    for asset in assets[start_index:end_index]:
        asset_id, name, amount, currency, asset_type, _, _ = asset
        type_emojis = {
            'cash': '💵', 'crypto': '₿', 'stocks': '📈', 'bonds': '🏛️',
            'real_estate': '🏠', 'commodities': '🥇', 'other': '💼'
        }
        emoji = type_emojis.get(asset_type.lower(), '💼')
        formatted_amount = fmt_currency_amount(amount, currency)
        button = InlineKeyboardButton(f"{emoji} {name} ({formatted_amount})", callback_data=f"{prefix}_{asset_id}")
        keyboard.append([button])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"nav_{prefix}_{page - 1}"))
    if end_index < len(assets):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"nav_{prefix}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    return InlineKeyboardMarkup(keyboard)

# --- Database Access Functions (No changes from original) ---
def get_user_goals_and_debts(user_id: int) -> List[Tuple]:
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, target_amount, current_amount, currency, type, notified_90_percent FROM goals WHERE user_id = ?", (user_id,))
    goals = cursor.fetchall()
    conn.close()
    return goals

def get_goal_by_id(goal_id: int) -> Optional[Tuple]:
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, target_amount, current_amount, currency, type, notified_90_percent FROM goals WHERE id = ?", (goal_id,))
    goal = cursor.fetchone()
    conn.close()
    return goal

def get_recent_transactions(goal_id: int, limit: int = 5) -> List[Tuple]:
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("SELECT amount, saved_at FROM savings WHERE goal_id = ? ORDER BY saved_at DESC LIMIT ?", (goal_id, limit))
    transactions = cursor.fetchall()
    conn.close()
    return transactions

def delete_goal_from_db(goal_id: int):
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM goals WHERE id = ?", (goal_id,))
    conn.commit()
    conn.close()

def erase_all_data():
    """Erase all data from the database - goals, debts, savings, expenses, assets, budgets, reminders, and payments"""
    conn = db_connect()
    cursor = conn.cursor()
    try:
        # Delete all data from all tables (order matters due to foreign keys)
        cursor.execute("DELETE FROM savings")
        cursor.execute("DELETE FROM expenses") 
        cursor.execute("DELETE FROM assets")
        cursor.execute("DELETE FROM budgets")
        cursor.execute("DELETE FROM recurring_transactions")
        cursor.execute("DELETE FROM reminders")
        cursor.execute("DELETE FROM payment_history")
        cursor.execute("DELETE FROM payments")
        cursor.execute("DELETE FROM goals")
        conn.commit()
        logger.info("All data erased from database")
        return True
    except Exception as e:
        logger.error(f"Error erasing data: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# --- Payment Management Functions ---
def get_user_payments(user_id: int) -> List[Tuple]:
    """Get all payments for a user"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, target_amount, current_amount, currency, payment_amount, 
               payment_frequency, recipient, created_at
        FROM payments 
        WHERE user_id = ?
        ORDER BY name
    """, (user_id,))
    payments = cursor.fetchall()
    conn.close()
    return payments

def get_payment_by_id(payment_id: int) -> Optional[Tuple]:
    """Get a specific payment by ID"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, target_amount, current_amount, currency, payment_amount, 
               payment_frequency, recipient, created_at
        FROM payments 
        WHERE id = ?
    """, (payment_id,))
    payment = cursor.fetchone()
    conn.close()
    return payment

def get_payment_history(payment_id: int, limit: int = 10) -> List[Tuple]:
    """Get recent payment history for a specific payment"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT amount, paid_at 
        FROM payment_history 
        WHERE payment_id = ? 
        ORDER BY paid_at DESC 
        LIMIT ?
    """, (payment_id, limit))
    history = cursor.fetchall()
    conn.close()
    return history

def delete_payment_from_db(payment_id: int):
    """Delete a payment and its history"""
    conn = db_connect()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM payments WHERE id = ?", (payment_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error deleting payment: {e}")
        return False
    finally:
        conn.close()

# --- Expense & Asset Helper Functions ---
def get_expenses_by_period(user_id: int, period: str) -> List[Tuple]:
    """Get expenses for a specific period (today, week, month, all)"""
    conn = db_connect()
    cursor = conn.cursor()
    
    if period == 'today':
        cursor.execute("""
            SELECT amount, currency, reason, created_at 
            FROM expenses 
            WHERE user_id = ? AND DATE(created_at) = DATE('now')
            ORDER BY created_at DESC
        """, (user_id,))
    elif period == 'week':
        cursor.execute("""
            SELECT amount, currency, reason, created_at 
            FROM expenses 
            WHERE user_id = ? AND DATE(created_at) >= DATE('now', '-7 days')
            ORDER BY created_at DESC
        """, (user_id,))
    elif period == 'month':
        cursor.execute("""
            SELECT amount, currency, reason, created_at 
            FROM expenses 
            WHERE user_id = ? AND DATE(created_at) >= DATE('now', '-30 days')
            ORDER BY created_at DESC
        """, (user_id,))
    else:  # all
        cursor.execute("""
            SELECT amount, currency, reason, created_at 
            FROM expenses 
            WHERE user_id = ?
            ORDER BY created_at DESC
        """, (user_id,))
    
    expenses = cursor.fetchall()
    conn.close()
    return expenses

def get_expense_totals_by_currency(user_id: int, period: str) -> Dict[str, float]:
    """Get total expenses grouped by currency for a period"""
    expenses = get_expenses_by_period(user_id, period)
    totals = {}
    for amount, currency, _, _, _ in expenses:  # Added category field
        totals[currency] = totals.get(currency, 0) + amount
    return totals

# --- Budget Management Functions ---
def get_user_budgets(user_id: int) -> List[Tuple]:
    """Get all budgets for a user"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, category, amount, currency, period, current_spent, created_at, updated_at
        FROM budgets 
        WHERE user_id = ?
        ORDER BY category
    """, (user_id,))
    budgets = cursor.fetchall()
    conn.close()
    return budgets

def update_budget_spending(user_id: int, category: str, amount: float, currency: str):
    """Update budget spending when expense is added"""
    conn = db_connect()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE budgets 
            SET current_spent = current_spent + ?, updated_at = CURRENT_TIMESTAMP 
            WHERE user_id = ? AND category = ? AND currency = ?
        """, (amount, user_id, category, currency))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error updating budget spending: {e}")
        return False
    finally:
        conn.close()

def check_budget_alerts(user_id: int, category: str, currency: str) -> Optional[str]:
    """Check if budget limit is exceeded and return alert message"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT category, amount, current_spent, currency
        FROM budgets 
        WHERE user_id = ? AND category = ? AND currency = ?
    """, (user_id, category, currency))
    budget = cursor.fetchone()
    conn.close()
    
    if not budget:
        return None
        
    category, limit, spent, currency = budget
    percentage = (spent / limit) * 100 if limit > 0 else 0
    
    if percentage >= 100:
        return f"🚨 <b>BUDGET EXCEEDED!</b> 🚨\n<code>{category}</code> budget blown by {percentage-100:.1f}%"
    elif percentage >= 80:
        return f"⚠️ <b>Budget Warning!</b>\n<code>{category}</code> at {percentage:.1f}% ({fmt_currency_amount(limit-spent, currency)} left)"
    
    return None

# --- Enhanced Expense Categories ---
EXPENSE_CATEGORIES = {
    'food': '🍽️ Food & Dining',
    'transport': '🚗 Transportation', 
    'shopping': '🛍️ Shopping',
    'bills': '💡 Bills & Utilities',
    'entertainment': '🎬 Entertainment',
    'health': '🏥 Healthcare',
    'education': '📚 Education',
    'travel': '✈️ Travel',
    'gifts': '🎁 Gifts',
    'other': '📦 Other'
}

# --- Payment Formatting Functions ---
def fmt_payment_list(payments: List[Tuple]) -> str:
    """Format payment list with progress tracking"""
    if not payments:
        return "<b>💳 Payment Tracker</b>\n\n<i>No payments being tracked yet. Use </i><code>new payment</code><i> to start.</i>"
    
    message = "<b>💳 Payment Tracker</b>\n<i>Ongoing payment obligations</i>\n\n"
    
    for payment in payments:
        payment_id, name, target, current, currency, payment_amt, frequency, recipient, created = payment
        progress_percent = (current / target) * 100 if target > 0 else 0
        remaining = max(0, target - current)  # Don't show negative remaining
        
        # Payment status
        if current >= target:
            status = "✅ Target Reached"
            progress_bar = fmt_progress_bar(100, length=8)
        else:
            status = f"{progress_percent:.1f}% Complete"
            progress_bar = fmt_progress_bar(progress_percent, length=8)
        
        payments_made = int(current / payment_amt) if payment_amt > 0 else 0
        
        message += f"💳 <b>{name.upper()}</b>\n"
        message += f"   To: <i>{recipient}</i>\n"
        message += f"   <code>{progress_bar} {status}</code>\n"
        message += f"   • Paid: <code>{fmt_currency_amount(current, currency)}</code> of <code>{fmt_currency_amount(target, currency)}</code>\n"
        
        if remaining > 0:
            message += f"   • Remaining: <code>{fmt_currency_amount(remaining, currency)}</code>\n"
        else:
            message += f"   • <b>Target exceeded by:</b> <code>{fmt_currency_amount(current - target, currency)}</code>\n"
        
        message += f"   • Payments: <code>{payments_made}</code> × <code>{fmt_currency_amount(payment_amt, currency)}</code> {frequency}\n\n"
    
    return message

def fmt_payment_progress(payment: Tuple, recent_payments: List[Tuple]) -> str:
    """Format detailed payment progress"""
    payment_id, name, target, current, currency, payment_amt, frequency, recipient, created = payment
    progress_percent = (current / target) * 100 if target > 0 else 0
    
    header = f"💳 <b>Payment Progress: {name.upper()}</b>\n"
    header += f"<i>Paying {recipient}</i>\n\n"
    
    # Progress visualization
    if current >= target:
        animated_bar = fmt_progress_bar(100, length=15)
        status_line = f"<code>{animated_bar} ✅ TARGET REACHED!</code>\n\n"
    else:
        animated_bar = fmt_progress_bar(progress_percent, length=15)
        status_line = f"<code>{animated_bar} {progress_percent:.1f}%</code>\n\n"
    
    # Payment details
    payments_made = int(current / payment_amt) if payment_amt > 0 else 0
    remaining = max(0, target - current)
    
    details = f"<b>📊 Payment Summary:</b>\n"
    details += f"  • Target Amount: <code>{fmt_currency_amount(target, currency)}</code>\n"
    details += f"  • Total Paid: <code>{fmt_currency_amount(current, currency)}</code>\n"
    
    if remaining > 0:
        details += f"  • Remaining: <code>{fmt_currency_amount(remaining, currency)}</code>\n"
        payments_left = remaining / payment_amt if payment_amt > 0 else 0
        details += f"  • Est. Payments Left: <code>{payments_left:.0f}</code>\n"
    else:
        details += f"  • <b>Overpaid by:</b> <code>{fmt_currency_amount(current - target, currency)}</code>\n"
    
    details += f"  • Payment Size: <code>{fmt_currency_amount(payment_amt, currency)}</code> {frequency}\n"
    details += f"  • Payments Made: <code>{payments_made}</code>\n\n"
    
    # Recent payments
    history = "<b>📝 Recent Payments:</b>\n"
    if not recent_payments:
        history += "<i>No payments recorded yet.</i>"
    else:
        for amount, date_str in recent_payments[:5]:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            formatted_date = date_obj.strftime('%b %d, %Y')
            history += f"  • <code>{fmt_currency_amount(amount, currency)}</code> on {formatted_date}\n"
    
    return header + status_line + details + history

def get_user_assets(user_id: int) -> List[Tuple]:
    """Get all assets for a user"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, amount, currency, asset_type, created_at, updated_at
        FROM assets 
        WHERE user_id = ?
        ORDER BY asset_type, name
    """, (user_id,))
    assets = cursor.fetchall()
    conn.close()
    return assets

def get_asset_by_id(asset_id: int) -> Optional[Tuple]:
    """Get a specific asset by ID"""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, amount, currency, asset_type, created_at, updated_at
        FROM assets 
        WHERE id = ?
    """, (asset_id,))
    asset = cursor.fetchone()
    conn.close()
    return asset

def update_asset_amount(asset_id: int, amount_change: float, operation: str) -> bool:
    """Update asset amount by adding or subtracting"""
    conn = db_connect()
    cursor = conn.cursor()
    
    try:
        if operation == 'add':
            cursor.execute("""
                UPDATE assets 
                SET amount = amount + ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            """, (amount_change, asset_id))
        elif operation == 'subtract':
            cursor.execute("""
                UPDATE assets 
                SET amount = amount - ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            """, (amount_change, asset_id))
        else:
            return False
            
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating asset: {e}")
        return False
    finally:
        conn.close()

def delete_asset_from_db(asset_id: int) -> bool:
    """Delete an asset by ID"""
    conn = db_connect()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logger.error(f"Error deleting asset: {e}")
        return False
    finally:
        conn.close()

def fmt_currency_amount(amount: float, currency: str) -> str:
    """Format currency amounts with proper symbols and formatting"""
    currency_symbols = {
        'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥', 'CNY': '¥',
        'BTC': '₿', 'ETH': 'Ξ', 'ADA': '₳', 'DOT': '●', 'SOL': '◎',
        'TONE': '🎵', 'NGN': '₦', 'GHS': '₵'
    }
    
    symbol = currency_symbols.get(currency.upper(), currency.upper())
    
    if currency.upper() in ['BTC', 'ETH']:
        return f"{symbol}{amount:.8f}"
    elif amount >= 1000000:
        return f"{symbol}{amount/1000000:.2f}M"
    elif amount >= 1000:
        return f"{symbol}{amount/1000:.1f}K"
    else:
        return f"{symbol}{amount:,.2f}"

def fmt_expense_report(expenses: List[Tuple], period: str) -> str:
    """Format expense report with nice formatting"""
    if not expenses:
        return f"<b>📊 Expense Report ({period.title()})</b>\n\n💸 <i>No expenses recorded for this period. Living frugally, I see!</i>"
    
    # Group by currency and category
    totals = {}
    category_totals = {}
    expense_lines = []
    
    for amount, currency, reason, category, created_at in expenses:
        totals[currency] = totals.get(currency, 0) + amount
        
        if category not in category_totals:
            category_totals[category] = {}
        category_totals[category][currency] = category_totals[category].get(currency, 0) + amount
        
        date_obj = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
        formatted_date = date_obj.strftime('%b %d')
        category_emoji = EXPENSE_CATEGORIES.get(category, '📦 Other').split(' ')[0]
        expense_lines.append(f"  • <code>{fmt_currency_amount(amount, currency)}</code> - {reason} {category_emoji} <i>({formatted_date})</i>")
    
    # Build report with better formatting
    report = f"<b>📊 Expense Report ({period.title()})</b>\n\n"
    
    # Summary by currency
    report += "<b>💰 Total Spending:</b>\n"
    for currency, total in totals.items():
        report += f"  <code>{fmt_currency_amount(total, currency)}</code>\n"
    
    # Summary by category
    report += f"\n<b>🏷️ By Category:</b>\n"
    for category, amounts in category_totals.items():
        category_name = EXPENSE_CATEGORIES.get(category, f'📦 {category.title()}')
        report += f"  {category_name}: "
        for currency, amount in amounts.items():
            report += f"<code>{fmt_currency_amount(amount, currency)}</code> "
        report += "\n"
    
    report += f"\n<b>📝 Recent Transactions ({len(expenses)}):</b>\n"
    for line in expense_lines[:10]:  # Show max 10 recent transactions
        report += line + "\n"
    
    if len(expenses) > 10:
        report += f"  <i>... and {len(expenses) - 10} more transactions</i>\n"
    
    return report

def fmt_expense_comparison(current_totals: Dict[str, float], previous_totals: Dict[str, float], period: str) -> str:
    """Format expense comparison between periods"""
    if not current_totals and not previous_totals:
        return f"📈 **Expense Comparison**\n\nNo data for comparison in {period} periods."
    
    comparison = f"📈 **Expense Comparison ({period.title()})**\n\n"
    
    all_currencies = set(current_totals.keys()) | set(previous_totals.keys())
    
    for currency in sorted(all_currencies):
        current = current_totals.get(currency, 0)
        previous = previous_totals.get(currency, 0)
        
        if previous == 0 and current > 0:
            change_text = "🆕 New spending"
        elif current == 0 and previous > 0:
            change_text = "✅ No spending (was spending before)"
        elif current == previous:
            change_text = "➖ No change"
        else:
            diff = current - previous
            percentage = (diff / previous * 100) if previous > 0 else 0
            if diff > 0:
                change_text = f"📈 +{fmt_currency_amount(abs(diff), currency)} ({percentage:+.1f}%)"
            else:
                change_text = f"📉 -{fmt_currency_amount(abs(diff), currency)} ({percentage:+.1f}%)"
        
        comparison += f"**{currency}:**\n"
        comparison += f"  Current: {fmt_currency_amount(current, currency)}\n"
        comparison += f"  Previous: {fmt_currency_amount(previous, currency)}\n"
        comparison += f"  Change: {change_text}\n\n"
    
    return comparison

def fmt_asset_summary(assets: List[Tuple]) -> str:
    """Format asset summary with nice formatting"""
    if not assets:
        return "🏦 **Asset Portfolio**\n\n💰 Your vault is empty. Time to start building wealth!"
    
    # Group by asset type and currency
    by_type = {}
    totals_by_currency = {}
    
    for asset_id, name, amount, currency, asset_type, created_at, updated_at in assets:
        if asset_type not in by_type:
            by_type[asset_type] = []
        by_type[asset_type].append((name, amount, currency))
        totals_by_currency[currency] = totals_by_currency.get(currency, 0) + amount
    
    summary = "🏦 **Asset Portfolio**\n\n"
    
    # Total summary
    summary += "💎 **Total Value:**\n"
    for currency, total in sorted(totals_by_currency.items()):
        summary += f"  {fmt_currency_amount(total, currency)}\n"
    
    summary += "\n📊 **By Category:**\n"
    
    type_emojis = {
        'cash': '💵', 'crypto': '₿', 'stocks': '📈', 'bonds': '🏛️',
        'real_estate': '🏠', 'commodities': '🥇', 'other': '💼'
    }
    
    for asset_type, type_assets in by_type.items():
        emoji = type_emojis.get(asset_type.lower(), '💼')
        summary += f"\n{emoji} **{asset_type.title()}:**\n"
        
        for name, amount, currency in type_assets:
            summary += f"  • {name}: {fmt_currency_amount(amount, currency)}\n"
    
    return summary

# --- PDF Generation ---
async def delete_message_later(context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.delete_message(chat_id=context.job.chat_id, message_id=context.job.data['message_id'])
    except BadRequest as e:
        if "message to delete not found" not in e.message:
            logger.warning(f"Could not delete message {context.job.data['message_id']}: {e}")

async def send_and_delete(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    try:
        if update.message: # Check if update.message exists (it might not for callback queries)
            await update.message.delete()
    except BadRequest as e:
        logger.warning(f"Could not delete user's message {update.message.message_id if update.message else 'N/A'}: {e}")
    sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text=text, **kwargs)
    context.job_queue.run_once(delete_message_later, MESSAGE_DELETION_DELAY, data={'message_id': sent_message.message_id}, chat_id=update.effective_chat.id)

def restricted(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id not in ALLOWED_USER_IDS:
            await (update.message or update.callback_query).reply_text("⛔️ Access Denied. I'm a one-person bot. And you're not that person.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


# --- Command & Conversation Handlers (Largely unchanged) ---

@restricted
async def erase_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Erase all data from the database"""
    try:
        # Delete user's message first
        if update.message:
            try:
                await update.message.delete()
            except BadRequest as e:
                logger.warning(f"Could not delete user's message: {e}")
        
        success = erase_all_data()
        
        if success:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="💀 **NUCLEAR OPTION ACTIVATED** 💀\n\n"
                     "All your financial data has been completely erased.\n"
                     "Goals, debts, savings, expenses, assets - all gone.\n\n"
                     "Hope you're ready to start fresh! 🔥"
            )
        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ **ERROR**: Failed to erase data. Something went wrong."
            )
    except Exception as e:
        logger.error(f"Error in erase_all command: {e}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ **ERROR**: An unexpected error occurred while trying to erase data."
        )

@restricted
async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Brewing up your financial reports...")
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("SELECT g.name, g.type, g.target_amount, g.currency, s.amount, s.saved_at FROM goals g JOIN savings s ON g.id = s.goal_id WHERE g.user_id = ? ORDER BY g.name, s.saved_at", (update.effective_user.id,))
    records = cursor.fetchall()
    goals_summary = get_user_goals_and_debts(update.effective_user.id)
    conn.close()

    if not records:
        await update.message.reply_text("Nothing to export.")
        return

    # Generate CSV in memory
    csv_output = StringIO()
    csv_writer = csv.writer(csv_output)
    csv_writer.writerow(["Name", "Type", "Target", "Currency", "Amount Paid/Saved", "Date"])
    # Convert records to list of lists for csv.writerows
    csv_records_for_export = [[r[0], r[1], f"{r[2]:,.2f}", r[3], f"{r[4]:,.2f}", datetime.strptime(r[5], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')] for r in records]
    csv_writer.writerows(csv_records_for_export)
    csv_output.seek(0)
    
    # Convert StringIO to BytesIO for the document
    csv_bytes = StringIO(csv_output.getvalue()).read().encode('utf-8')
    await update.message.reply_document(document=csv_bytes, filename=f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", caption="Here's your data in CSV format.")

    # Define PDF path within the persistent directory
    pdf_path = os.path.join(DATA_DIR, f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
    
    # Calculate summaries
    totals_saved: Dict[str, float] = {}
    totals_paid: Dict[str, float] = {}
    for record in records:
        _name, type, _target, currency, amount, _date = record
        if type == 'goal':
            totals_saved[currency] = totals_saved.get(currency, 0) + amount
        elif type == 'debt':
            totals_paid[currency] = totals_paid.get(currency, 0) + amount
            
    total_goals = sum(1 for g in goals_summary if g[5] == 'goal')
    total_debts = sum(1 for g in goals_summary if g[5] == 'debt')
    
    summary_data = [["Stat", "Value"], ["Total Savings Goals", str(total_goals)], ["Total Debts", str(total_debts)]]
    if totals_saved:
        summary_data.append(["--- Total Saved ---", ""])
        for currency, total in totals_saved.items():
            summary_data.append([f"Total Saved ({currency})", f"{total:,.2f}"])
    if totals_paid:
        summary_data.append(["--- Total Debt Paid ---", ""])
        for currency, total in totals_paid.items():
            summary_data.append([f"Total Debt Paid ({currency})", f"{total:,.2f}"])
            
    # Generate and send PDF
    try:
        generate_pdf_report(records, summary_data, pdf_path)
        with open(pdf_path, 'rb') as pdf_file:
            await update.message.reply_document(document=pdf_file, filename=os.path.basename(pdf_path), caption="And the fancy PDF version.")
    except Exception as e:
        logger.error(f"Failed to generate or send PDF: {e}")
        await update.message.reply_text("I managed the CSV, but the PDF maker threw a tantrum.")
    finally:
        # Clean up the generated PDF file from the persistent disk
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

@restricted
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_and_delete(update, context, MANUAL_TEXT, parse_mode='HTML')

@restricted
async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_and_delete(update, context, f"<b>❓ Unknown Command</b>\n\nI don't know what '<code>{update.message.text}</code>' means. Stick to the script.\n\n" + MANUAL_TEXT, parse_mode='HTML')

@restricted
async def new_goal_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await send_and_delete(update, context, "🚀 A new dream, huh? Let's give it a name.")
    return GOAL_NAME
async def get_goal_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['goal_name'] = update.message.text
    await send_and_delete(update, context, f"'{context.user_data['goal_name']}'. Sounds expensive. How much?")
    return GOAL_AMOUNT
async def get_goal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['goal_amount'] = float(update.message.text)
        await send_and_delete(update, context, "Currency? (e.g., USD, TONE)")
        return GOAL_CURRENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Try again.")
        return GOAL_AMOUNT
async def get_goal_currency_and_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    currency = update.message.text.upper()
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO goals (user_id, name, target_amount, currency, type) VALUES (?, ?, ?, ?, ?)", (update.effective_user.id, context.user_data['goal_name'], context.user_data['goal_amount'], currency, 'goal'))
        conn.commit()
        await send_and_delete(update, context, f"✅ Goal set. Don't let '{context.user_data['goal_name']}' become a forgotten dream.")
    except sqlite3.IntegrityError:
        await send_and_delete(update, context, "You already have something with that name. Try a more creative name.")
    finally:
        if conn: conn.close()
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def new_debt_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await send_and_delete(update, context, "⛓️ Facing the music? Name this debt.")
    return DEBT_NAME
async def get_debt_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['debt_name'] = update.message.text
    await send_and_delete(update, context, f"'{context.user_data['debt_name']}'. Oof. Total damage?")
    return DEBT_AMOUNT
async def get_debt_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['debt_amount'] = float(update.message.text)
        await send_and_delete(update, context, "Currency?")
        return DEBT_CURRENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Try again.")
        return DEBT_AMOUNT
async def get_debt_currency_and_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    currency = update.message.text.upper()
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO goals (user_id, name, target_amount, currency, type) VALUES (?, ?, ?, ?, ?)", (update.effective_user.id, context.user_data['debt_name'], context.user_data['debt_amount'], currency, 'debt'))
        conn.commit()
        await send_and_delete(update, context, f"✅ Debt logged. Let's start chipping away at '{context.user_data['debt_name']}'.")
    except sqlite3.IntegrityError:
        await send_and_delete(update, context, "Already tracking a debt with that name. One crisis at a time.")
    finally:
        if conn: conn.close()
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def view_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    goals = get_user_goals_and_debts(update.message.from_user.id)
    message = fmt_goal_list(goals)
    await send_and_delete(update, context, message, parse_mode='Markdown')

async def paginated_list_start(update: Update, context: ContextTypes.DEFAULT_TYPE, prefix: str, state: int) -> int:
    # Use update.effective_chat.id for send_message instead of update.message.chat_id
    # as update.message might not exist for callback queries.
    chat_id = update.effective_chat.id

    try:
        # If the update is a message, delete it. If it's a callback query, it's already "answered" or being edited.
        if update.message:
            await update.message.delete()
    except BadRequest as e:
        logger.warning(f"Could not delete user's message {update.message.message_id if update.message else 'N/A'}: {e}")

    goals = get_user_goals_and_debts(update.effective_user.id)
    if not goals:
        await context.bot.send_message(chat_id=chat_id, text="You have nothing to select from. Create a goal or debt first.")
        return ConversationHandler.END
    
    reply_markup = generate_paginated_keyboard(goals, prefix=prefix, page=0)
    await context.bot.send_message(chat_id=chat_id, text="Which one are we looking at?", reply_markup=reply_markup)
    return state

@restricted
async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    logger.info(f"add_start: Received message: '{update.message.text}'")
    return await paginated_list_start(update, context, prefix="add_to", state=ADD_SAVINGS_GOAL)

@restricted
async def delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await paginated_list_start(update, context, prefix="delete", state=DELETE_GOAL_CONFIRM)

@restricted
async def progress_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await paginated_list_start(update, context, prefix="progress", state=PROGRESS_GOAL_SELECT)

async def navigate_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()  # Acknowledge the callback query

    try:
        # The data is in the format "nav_{prefix}_{page}".
        # We remove the "nav_" part and then split from the right to reliably get the page number.
        data_payload = query.data[4:]  # Removes "nav_"
        prefix, page_str = data_payload.rsplit('_', 1)
        page = int(page_str)
    except (ValueError, IndexError) as e:
        logger.error(f"Could not parse page number from callback_data: '{query.data}'. Error: {e}")
        await query.edit_message_text(text="Error processing navigation. Please try again.")
        return  # Return None to stay in the current state

    goals = get_user_goals_and_debts(query.from_user.id)
    reply_markup = generate_paginated_keyboard(goals, prefix=prefix, page=page)

    try:
        await query.edit_message_reply_markup(reply_markup)
    except BadRequest as e:
        # This can happen if the keyboard content is identical. It's not a critical error.
        if 'Message is not modified' not in str(e):
             logger.warning(f"Failed to edit message reply markup for navigation: {e}")
             await query.edit_message_text(text="Could not update the list. Please try again.")

    # Return None to stay in the current state, allowing for more pagination or selection.
    return None
async def select_goal_for_adding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    goal_id = int(query.data.split("_")[-1])
    context.user_data['selected_goal_id'] = goal_id
    goal = get_goal_by_id(goal_id)
    if not goal:
        await query.edit_message_text(text="Error: Goal not found. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

    action = "saving for" if goal[5] == 'goal' else "paying off"
    await query.edit_message_text(text=f"How much are you {action} '{goal[1]}'? ({goal[4]})")
    logger.info(f"select_goal_for_adding: User selected goal_id {goal_id} for adding.")
    return ADD_SAVINGS_AMOUNT

async def get_amount_and_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    logger.info(f"get_amount_and_save: Received amount text: {update.message.text}")
    try:
        amount = float(update.message.text)
        goal_id = context.user_data.get('selected_goal_id')

        if goal_id is None:
            logger.error("get_amount_and_save: selected_goal_id not found in user_data. Conversation state likely lost.")
            await send_and_delete(update, context, "It seems I forgot which goal we were talking about. Please start the `add` command again.")
            context.user_data.clear()
            return ConversationHandler.END

        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO savings (goal_id, amount) VALUES (?, ?)", (goal_id, amount))
        cursor.execute("UPDATE goals SET current_amount = current_amount + ? WHERE id = ?", (amount, goal_id))
        conn.commit()
        
        goal = get_goal_by_id(goal_id)
        if not goal:
            await send_and_delete(update, context, "Successfully recorded, but couldn't retrieve goal details.")
            conn.close()
            context.user_data.clear()
            return ConversationHandler.END

        name, target, current, currency, type, notified = goal[1], goal[2], goal[3], goal[4], goal[5], goal[6]
        await send_and_delete(update, context, f"✅ Roger that. {amount:,.2f} {currency} logged for '{name}'.")
        
        progress_percent = (current / target) * 100 if target > 0 else 0
        if type == 'goal' and progress_percent >= 100:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"🎉 **GOAL REACHED!** 🎉\nYou hit your target for '{name}'.")
        elif type == 'goal' and progress_percent >= 90 and not notified:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"🔥 **Almost there!** Over 90% of the way to '{name}'.")
            cursor.execute("UPDATE goals SET notified_90_percent = 1 WHERE id = ?", (goal_id,)); conn.commit()
        elif type == 'debt' and progress_percent >= 100:
             await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ **DEBT CLEARED!** ✅\nYou paid off '{name}'. You are free.")
        
        conn.close()
        context.user_data.clear()
        logger.info(f"get_amount_and_save: Amount {amount} saved for goal {goal_id}.")
        return ConversationHandler.END
    except ValueError:
        logger.warning(f"get_amount_and_save: Invalid amount input '{update.message.text}'.")
        await send_and_delete(update, context, "That's not a valid number. Please enter a numerical amount.")
        # Do not end conversation here, allow user to retry entering amount
        return ADD_SAVINGS_AMOUNT # Stay in the same state
    except KeyError:
        logger.error("get_amount_and_save: 'selected_goal_id' not found in context.user_data. Likely lost conversation state.")
        await send_and_delete(update, context, "It seems I lost track of which goal you were adding to. Please start the `add` command again.")
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_amount_and_save: {e}", exc_info=True)
        await send_and_delete(update, context, "An unexpected error occurred while saving. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END


async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    goal_id = int(query.data.split("_")[-1])
    goal = get_goal_by_id(goal_id)
    if goal:
        delete_goal_from_db(goal_id)
        await query.edit_message_text(text=f"Gone. '{goal[1]}' has been vanquished.")
    else:
        await query.edit_message_text(text="Goal not found or already deleted.")
    return ConversationHandler.END

async def show_goal_progress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    goal_id = int(query.data.split("_")[-1])
    goal = get_goal_by_id(goal_id)
    if not goal:
        await query.edit_message_text(text="Error: Goal not found. Please try again.")
        return ConversationHandler.END
    recent_transactions = get_recent_transactions(goal_id)
    progress_message = fmt_single_goal_progress(goal, recent_transactions)
    await query.edit_message_text(text=progress_message, parse_mode='Markdown')
    return ConversationHandler.END

@restricted
async def set_reminder_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await send_and_delete(update, context, "You need me to nag you? What time daily? (e.g., '09:00', '21:30' in 24h format)")
    return REMINDER_TIME

async def set_reminder_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        user_time_str = update.message.text
        user_time = datetime.strptime(user_time_str, '%H:%M').time()
        chat_id = update.effective_chat.id
        # Remove any existing jobs for this chat_id before creating a new one
        for job in context.job_queue.get_jobs_by_name(str(chat_id)):
            job.schedule_removal()
        context.job_queue.run_daily(reminder_callback, time=user_time, chat_id=chat_id, name=str(chat_id))
        await send_and_delete(update, context, f"Done. Expect a poke from me daily at {user_time.strftime('%H:%M')}.")
        return ConversationHandler.END
    except ValueError:
        await send_and_delete(update, context, "Not a valid time. Use HH:MM format.")
        return REMINDER_TIME
async def reminder_callback(context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id=context.job.chat_id, text="🔔 Reminder: Your goals won't meet themselves. Did you save today?")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Determine the appropriate method to respond based on the update type
    if update.callback_query:
        await update.callback_query.answer() # Acknowledge the callback query
        await update.callback_query.edit_message_text(text="Fine, whatever. Mission aborted.")
    elif update.message:
        # Delete user's message and send a response that also gets deleted
        await send_and_delete(update, context, "Fine, whatever. Mission aborted.")
    else:
        # Fallback if neither message nor callback_query exists (unlikely but good for robustness)
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Fine, whatever. Mission aborted.")

    context.user_data.clear()
    return ConversationHandler.END

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    message = (f"An exception was raised:\n<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}"
               f"</pre>\n\n<pre>{html.escape(tb_string)}</pre>")
    if isinstance(update, Update) and hasattr(update, 'message') and update.message:
        await update.message.reply_text("Looks like I tripped over a bug. Try again, I guess.")
    elif isinstance(update, Update) and hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.answer("Looks like I tripped over a bug. Try again, I guess.")
        await update.callback_query.edit_message_text("Looks like I tripped over a bug. Try again, I guess.")
    logger.error(message)

# --- Expense Tracking Handlers ---
@restricted
async def add_expense_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await send_and_delete(update, context, "💸 Time to face the music. How much did you spend?")
    return EXPENSE_AMOUNT

async def get_expense_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['expense_amount'] = float(update.message.text)
        await send_and_delete(update, context, "Currency? (e.g., USD, EUR, BTC)")
        return EXPENSE_CURRENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Try again.")
        return EXPENSE_AMOUNT

async def get_expense_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['expense_currency'] = update.message.text.upper()
    
    # Show category options with emojis
    categories_text = "What category is this expense?\n\n"
    for key, value in EXPENSE_CATEGORIES.items():
        categories_text += f"<code>{key}</code> - {value}\n"
    
    await send_and_delete(update, context, categories_text, parse_mode='HTML')
    return EXPENSE_CATEGORY

async def get_expense_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    category = update.message.text.lower()
    if category not in EXPENSE_CATEGORIES:
        category = 'other'
    
    context.user_data['expense_category'] = category
    category_name = EXPENSE_CATEGORIES[category]
    await send_and_delete(update, context, f"Great! {category_name}\n\nWhat was this expense for? (describe the purchase)")
    return EXPENSE_REASON

async def save_expense(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Delete user's message first
    if update.message:
        try:
            await update.message.delete()
        except BadRequest as e:
            logger.warning(f"Could not delete user's message: {e}")
    
    reason = update.message.text
    amount = context.user_data['expense_amount']
    currency = context.user_data['expense_currency']
    category = context.user_data.get('expense_category', 'other')
    
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO expenses (user_id, amount, currency, reason, category) VALUES (?, ?, ?, ?, ?)",
            (update.effective_user.id, amount, currency, reason, category)
        )
        conn.commit()
        conn.close()
        
        # Update budget spending
        update_budget_spending(update.effective_user.id, category, amount, currency)
        
        # Check for budget alerts
        budget_alert = check_budget_alerts(update.effective_user.id, category, currency)
        
        formatted_amount = fmt_currency_amount(amount, currency)
        category_name = EXPENSE_CATEGORIES.get(category, f'📦 {category.title()}')
        
        response = f"<b>💸 Expense Recorded!</b>\n\n"
        response += f"<code>{formatted_amount}</code> - {reason}\n"
        response += f"Category: {category_name}"
        
        await send_and_delete(update, context, response, parse_mode='HTML')
        
        # Send budget alert if needed
        if budget_alert:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=budget_alert,
                parse_mode='HTML'
            )
        
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error saving expense: {e}")
        await send_and_delete(update, context, "❌ Error saving expense. Try again.")
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def expense_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    # Get expenses for different periods
    today_expenses = get_expenses_by_period(user_id, 'today')
    week_expenses = get_expenses_by_period(user_id, 'week')
    month_expenses = get_expenses_by_period(user_id, 'month')
    
    # Format reports
    today_report = fmt_expense_report(today_expenses, 'today')
    week_report = fmt_expense_report(week_expenses, 'week')
    
    # Send reports
    await send_and_delete(update, context, today_report, parse_mode='HTML')
    await send_and_delete(update, context, week_report, parse_mode='HTML')

@restricted
async def expense_compare(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    # Get current and previous week totals
    current_week = get_expense_totals_by_currency(user_id, 'week')
    
    # Get previous week (14-7 days ago)
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT amount, currency
        FROM expenses 
        WHERE user_id = ? AND DATE(created_at) >= DATE('now', '-14 days') 
        AND DATE(created_at) < DATE('now', '-7 days')
    """, (user_id,))
    previous_week_data = cursor.fetchall()
    conn.close()
    
    previous_week = {}
    for amount, currency in previous_week_data:
        previous_week[currency] = previous_week.get(currency, 0) + amount
    
    comparison = fmt_expense_comparison(current_week, previous_week, 'week')
    await send_and_delete(update, context, comparison, parse_mode='HTML')

# --- Budget Management Handlers ---
@restricted
async def set_budget_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    categories_text = "<b>💰 Set Budget Limit</b>\n\nWhich category?\n\n"
    for key, value in EXPENSE_CATEGORIES.items():
        categories_text += f"<code>{key}</code> - {value}\n"
    
    await send_and_delete(update, context, categories_text, parse_mode='HTML')
    return BUDGET_CATEGORY

async def get_budget_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    category = update.message.text.lower()
    if category not in EXPENSE_CATEGORIES:
        category = 'other'
    
    context.user_data['budget_category'] = category
    category_name = EXPENSE_CATEGORIES[category]
    await send_and_delete(update, context, f"Setting budget for {category_name}\n\nHow much can you spend per month?")
    return BUDGET_AMOUNT

async def get_budget_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['budget_amount'] = float(update.message.text)
        await send_and_delete(update, context, "Currency? (e.g., USD, EUR)")
        return BUDGET_CURRENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Try again.")
        return BUDGET_AMOUNT

async def get_budget_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['budget_currency'] = update.message.text.upper()
    await send_and_delete(update, context, "Budget period?\n\n<code>weekly</code> - Weekly limit\n<code>monthly</code> - Monthly limit", parse_mode='HTML')
    return BUDGET_PERIOD

async def save_budget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        try:
            await update.message.delete()
        except BadRequest as e:
            logger.warning(f"Could not delete user's message: {e}")
    
    period = update.message.text.lower()
    if period not in ['weekly', 'monthly']:
        period = 'monthly'
    
    category = context.user_data['budget_category']
    amount = context.user_data['budget_amount']
    currency = context.user_data['budget_currency']
    
    try:
        conn = db_connect()
        cursor = conn.cursor()
        
        # Check if budget already exists, update if it does
        cursor.execute(
            "SELECT id FROM budgets WHERE user_id = ? AND category = ? AND currency = ?",
            (update.effective_user.id, category, currency)
        )
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute(
                "UPDATE budgets SET amount = ?, period = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (amount, period, existing[0])
            )
            action = "updated"
        else:
            cursor.execute(
                "INSERT INTO budgets (user_id, category, amount, currency, period) VALUES (?, ?, ?, ?, ?)",
                (update.effective_user.id, category, amount, currency, period)
            )
            action = "created"
        
        conn.commit()
        conn.close()
        
        formatted_amount = fmt_currency_amount(amount, currency)
        category_name = EXPENSE_CATEGORIES.get(category, f'📦 {category.title()}')
        
        response = f"<b>💰 Budget {action.title()}!</b>\n\n"
        response += f"{category_name}: <code>{formatted_amount}</code> per {period[:-2]}\n"
        response += f"<i>I'll warn you when you hit 80% of this limit.</i>"
        
        await send_and_delete(update, context, response, parse_mode='HTML')
        
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error saving budget: {e}")
        await send_and_delete(update, context, "❌ Error saving budget. Try again.")
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def budget_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    budgets = get_user_budgets(update.effective_user.id)
    
    if not budgets:
        message = "<b>💰 Budget Status</b>\n\n<i>No budgets set yet. Use </i><code>set budget</code><i> to create spending limits.</i>"
    else:
        message = "<b>💰 Budget Dashboard</b>\n\n"
        
        for budget_id, category, limit, currency, period, spent, created_at, updated_at in budgets:
            category_name = EXPENSE_CATEGORIES.get(category, f'📦 {category.title()}')
            percentage = (spent / limit) * 100 if limit > 0 else 0
            remaining = limit - spent
            
            # Status emoji based on spending
            if percentage >= 100:
                status = "🚨"
            elif percentage >= 80:
                status = "⚠️"
            elif percentage >= 50:
                status = "🟡"
            else:
                status = "🟢"
            
            message += f"{status} <b>{category_name}</b>\n"
            message += f"  Budget: <code>{fmt_currency_amount(limit, currency)}</code> per {period[:-2]}\n"
            message += f"  Spent: <code>{fmt_currency_amount(spent, currency)}</code> ({percentage:.1f}%)\n"
            message += f"  Remaining: <code>{fmt_currency_amount(remaining, currency)}</code>\n\n"
    
    await send_and_delete(update, context, message, parse_mode='HTML')

# --- Payment Tracking Handlers ---
@restricted
async def new_payment_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await send_and_delete(update, context, "<b>💳 New Payment Tracker</b>\n\nWhat should we call this payment? (e.g., 'Car Loan', 'House Payment', 'Friend Loan')", parse_mode='HTML')
    return PAYMENT_NAME

async def get_payment_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['payment_name'] = update.message.text
    await send_and_delete(update, context, f"<b>Payment:</b> <i>{context.user_data['payment_name']}</i>\n\nWho are you paying? (recipient name)", parse_mode='HTML')
    return PAYMENT_RECIPIENT

async def get_payment_recipient(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['payment_recipient'] = update.message.text
    await send_and_delete(update, context, f"<b>Paying:</b> <i>{context.user_data['payment_recipient']}</i>\n\nWhat's the total amount you need to pay? (initial capital/debt)", parse_mode='HTML')
    return PAYMENT_TARGET

async def get_payment_target(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['payment_target'] = float(update.message.text)
        await send_and_delete(update, context, "Currency? (e.g., USD, EUR)")
        return PAYMENT_CURRENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Enter the total amount to pay:")
        return PAYMENT_TARGET

async def get_payment_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['payment_currency'] = update.message.text.upper()
    target = context.user_data['payment_target']
    currency = context.user_data['payment_currency']
    await send_and_delete(update, context, f"<b>Total:</b> <code>{fmt_currency_amount(target, currency)}</code>\n\nHow much do you pay each time?", parse_mode='HTML')
    return PAYMENT_AMOUNT

async def get_payment_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['payment_amount'] = float(update.message.text)
        await send_and_delete(update, context, "How often do you make this payment?\n\n<code>weekly</code> - Every week\n<code>monthly</code> - Every month\n<code>quarterly</code> - Every 3 months\n<code>yearly</code> - Every year", parse_mode='HTML')
        return PAYMENT_FREQUENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Enter the payment amount:")
        return PAYMENT_AMOUNT

async def save_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        try:
            await update.message.delete()
        except BadRequest as e:
            logger.warning(f"Could not delete user's message: {e}")
    
    frequency = update.message.text.lower()
    if frequency not in ['weekly', 'monthly', 'quarterly', 'yearly']:
        frequency = 'monthly'
    
    name = context.user_data['payment_name']
    recipient = context.user_data['payment_recipient']
    target = context.user_data['payment_target']
    currency = context.user_data['payment_currency']
    amount = context.user_data['payment_amount']
    
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO payments (user_id, name, target_amount, currency, payment_amount, payment_frequency, recipient) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (update.effective_user.id, name, target, currency, amount, frequency, recipient))
        conn.commit()
        conn.close()
        
        response = f"<b>💳 Payment Tracker Created!</b>\n\n"
        response += f"<b>Payment:</b> {name}\n"
        response += f"<b>To:</b> {recipient}\n"
        response += f"<b>Total:</b> <code>{fmt_currency_amount(target, currency)}</code>\n"
        response += f"<b>Payment:</b> <code>{fmt_currency_amount(amount, currency)}</code> {frequency}\n\n"
        response += f"<i>Use </i><code>add payment</code><i> to log payments made!</i>"
        
        await send_and_delete(update, context, response, parse_mode='HTML')
        
        context.user_data.clear()
        return ConversationHandler.END
    except sqlite3.IntegrityError:
        await send_and_delete(update, context, "❌ You already have a payment with that name. Choose a different name.", parse_mode='HTML')
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error saving payment: {e}")
        await send_and_delete(update, context, "❌ Error creating payment tracker. Try again.", parse_mode='HTML')
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def view_payments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    payments = get_user_payments(update.effective_user.id)
    message = fmt_payment_list(payments)
    await send_and_delete(update, context, message, parse_mode='HTML')

@restricted
async def add_payment_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await payment_list_start(update, context, prefix="add_payment", state=ADD_PAYMENT_SELECT)

async def payment_list_start(update: Update, context: ContextTypes.DEFAULT_TYPE, prefix: str, state: int) -> int:
    chat_id = update.effective_chat.id

    try:
        if update.message:
            await update.message.delete()
    except BadRequest as e:
        logger.warning(f"Could not delete user's message: {e}")

    payments = get_user_payments(update.effective_user.id)
    if not payments:
        await context.bot.send_message(chat_id=chat_id, text="<b>💳 No Payments Found</b>\n\n<i>Create a payment tracker first with </i><code>new payment</code>", parse_mode='HTML')
        return ConversationHandler.END
    
    reply_markup = generate_payment_keyboard(payments, prefix=prefix, page=0)
    await context.bot.send_message(chat_id=chat_id, text="<b>💳 Select Payment:</b>", reply_markup=reply_markup, parse_mode='HTML')
    return state

def generate_payment_keyboard(payments: List[Tuple], prefix: str, page: int = 0) -> InlineKeyboardMarkup:
    """Creates a paginated inline keyboard for payments."""
    keyboard = []
    start_index = page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE

    for payment in payments[start_index:end_index]:
        payment_id, name, target, current, currency, payment_amt, frequency, recipient, created = payment
        progress = (current / target) * 100 if target > 0 else 0
        
        if current >= target:
            emoji = "✅"
        elif progress >= 50:
            emoji = "🟡"
        else:
            emoji = "💳"
            
        button = InlineKeyboardButton(f"{emoji} {name} → {recipient}", callback_data=f"{prefix}_{payment_id}")
        keyboard.append([button])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"nav_{prefix}_{page - 1}"))
    if end_index < len(payments):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"nav_{prefix}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    return InlineKeyboardMarkup(keyboard)

async def select_payment_for_adding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    payment_id = int(query.data.split("_")[-1])
    context.user_data['selected_payment_id'] = payment_id
    payment = get_payment_by_id(payment_id)
    if not payment:
        await query.edit_message_text(text="❌ Error: Payment not found.", parse_mode='HTML')
        context.user_data.clear()
        return ConversationHandler.END

    _, name, target, current, currency, payment_amt, frequency, recipient, _ = payment
    
    await query.edit_message_text(
        text=f"<b>💳 Recording Payment</b>\n\n"
             f"<b>Payment:</b> {name}\n"
             f"<b>To:</b> {recipient}\n"
             f"<b>Suggested:</b> <code>{fmt_currency_amount(payment_amt, currency)}</code>\n\n"
             f"How much did you pay?",
        parse_mode='HTML'
    )
    return ADD_PAYMENT_AMOUNT

async def get_payment_amount_and_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        amount = float(update.message.text)
        payment_id = context.user_data.get('selected_payment_id')

        if payment_id is None:
            await send_and_delete(update, context, "❌ Lost track of which payment we were recording.", parse_mode='HTML')
            context.user_data.clear()
            return ConversationHandler.END

        conn = db_connect()
        cursor = conn.cursor()
        
        # Add to payment history
        cursor.execute("INSERT INTO payment_history (payment_id, amount) VALUES (?, ?)", (payment_id, amount))
        
        # Update current amount in payments table
        cursor.execute("UPDATE payments SET current_amount = current_amount + ? WHERE id = ?", (amount, payment_id))
        
        conn.commit()
        
        payment = get_payment_by_id(payment_id)
        if payment:
            _, name, target, current, currency, payment_amt, frequency, recipient, _ = payment
            progress = (current / target) * 100 if target > 0 else 0
            
            response = f"<b>✅ Payment Recorded!</b>\n\n"
            response += f"<code>{fmt_currency_amount(amount, currency)}</code> paid to {recipient}\n\n"
            response += f"<b>Progress:</b> <code>{fmt_currency_amount(current, currency)}</code> / <code>{fmt_currency_amount(target, currency)}</code> ({progress:.1f}%)\n"
            
            if current >= target:
                response += f"\n🎉 <b>TARGET REACHED!</b> Payment continues tracking."
            
            await send_and_delete(update, context, response, parse_mode='HTML')
        
        conn.close()
        context.user_data.clear()
        return ConversationHandler.END
        
    except ValueError:
        await send_and_delete(update, context, "❌ That's not a valid number. Enter the payment amount:", parse_mode='HTML')
        return ADD_PAYMENT_AMOUNT
    except Exception as e:
        logger.error(f"Error saving payment: {e}")
        await send_and_delete(update, context, "❌ Error recording payment. Try again.", parse_mode='HTML')
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def payment_progress_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await payment_list_start(update, context, prefix="payment_progress", state=PAYMENT_PROGRESS_SELECT)

async def show_payment_progress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    payment_id = int(query.data.split("_")[-1])
    payment = get_payment_by_id(payment_id)
    if not payment:
        await query.edit_message_text(text="❌ Error: Payment not found.")
        return ConversationHandler.END
    
    recent_payments = get_payment_history(payment_id)
    progress_message = fmt_payment_progress(payment, recent_payments)
    await query.edit_message_text(text=progress_message, parse_mode='HTML')
    return ConversationHandler.END

@restricted
async def delete_payment_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await payment_list_start(update, context, prefix="delete_payment", state=DELETE_PAYMENT_SELECT)

async def confirm_payment_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    payment_id = int(query.data.split("_")[-1])
    payment = get_payment_by_id(payment_id)
    if payment:
        success = delete_payment_from_db(payment_id)
        if success:
            await query.edit_message_text(text=f"<b>🗑️ Payment Deleted</b>\n\n<i>'{payment[1]}' tracking removed.</i>", parse_mode='HTML')
        else:
            await query.edit_message_text(text="❌ Error deleting payment.")
    else:
        await query.edit_message_text(text="❌ Payment not found.")
    return ConversationHandler.END

# --- Financial Dashboard ---
@restricted
async def financial_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    
    # Get all data
    goals = get_user_goals_and_debts(user_id)
    assets = get_user_assets(user_id)
    budgets = get_user_budgets(user_id)
    today_expenses = get_expenses_by_period(user_id, 'today')
    week_expenses = get_expenses_by_period(user_id, 'week')
    
    # Calculate totals
    goal_progress = 0
    debt_progress = 0
    total_assets_usd = 0
    today_spending = 0
    week_spending = 0
    
    for goal in goals:
        if goal[5] == 'goal':
            goal_progress += (goal[3] / goal[2]) * 100 if goal[2] > 0 else 0
        else:
            debt_progress += (goal[3] / goal[2]) * 100 if goal[2] > 0 else 0
    
    for asset in assets:
        if asset[3] == 'USD':  # Only count USD assets for simplicity
            total_assets_usd += asset[2]
    
    for expense in today_expenses:
        if expense[1] == 'USD':
            today_spending += expense[0]
    
    for expense in week_expenses:
        if expense[1] == 'USD':
            week_spending += expense[0]
    
    # Build dashboard
    dashboard = f"<b>📊 FINANCIAL DASHBOARD</b>\n"
    dashboard += f"<i>Your complete financial overview</i>\n\n"
    
    # Quick stats
    dashboard += f"<b>🎯 Goals & Debts ({len(goals)})</b>\n"
    if goals:
        avg_goal_progress = goal_progress / len([g for g in goals if g[5] == 'goal']) if any(g[5] == 'goal' for g in goals) else 0
        avg_debt_progress = debt_progress / len([g for g in goals if g[5] == 'debt']) if any(g[5] == 'debt' for g in goals) else 0
        dashboard += f"  Avg Goal Progress: <code>{avg_goal_progress:.1f}%</code>\n"
        if any(g[5] == 'debt' for g in goals):
            dashboard += f"  Avg Debt Paid: <code>{avg_debt_progress:.1f}%</code>\n"
    else:
        dashboard += f"  <i>No goals set yet</i>\n"
    
    dashboard += f"\n<b>🏦 Assets ({len(assets)})</b>\n"
    if assets:
        dashboard += f"  Portfolio Value: <code>${total_assets_usd:,.2f}</code>\n"
    else:
        dashboard += f"  <i>No assets tracked</i>\n"
    
    dashboard += f"\n<b>💸 Spending</b>\n"
    dashboard += f"  Today: <code>${today_spending:,.2f}</code>\n"
    dashboard += f"  This Week: <code>${week_spending:,.2f}</code>\n"
    
    # Budget alerts
    budget_alerts = 0
    for budget in budgets:
        percentage = (budget[5] / budget[2]) * 100 if budget[2] > 0 else 0
        if percentage >= 80:
            budget_alerts += 1
    
    dashboard += f"\n<b>💰 Budgets ({len(budgets)})</b>\n"
    if budget_alerts > 0:
        dashboard += f"  ⚠️ <code>{budget_alerts}</code> budget(s) need attention\n"
    elif budgets:
        dashboard += f"  ✅ All budgets healthy\n"
    else:
        dashboard += f"  <i>No budgets set</i>\n"
    
    # Quick actions
    dashboard += f"\n<b>⚡ Quick Actions</b>\n"
    dashboard += f"<code>add expense</code> - Record spending\n"
    dashboard += f"<code>add</code> - Save towards goal\n"
    dashboard += f"<code>budget status</code> - Check limits\n"
    dashboard += f"<code>view all</code> - See all goals\n"
    
    await send_and_delete(update, context, dashboard, parse_mode='HTML')

# --- Asset Tracking Handlers ---
@restricted
async def add_asset_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await send_and_delete(update, context, "🏦 Building wealth, I see. What's the asset name? (e.g., 'Savings Account', 'Bitcoin Wallet')")
    return ASSET_NAME

async def get_asset_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['asset_name'] = update.message.text
    await send_and_delete(update, context, f"How much {context.user_data['asset_name']} do you have?")
    return ASSET_AMOUNT

async def get_asset_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['asset_amount'] = float(update.message.text)
        await send_and_delete(update, context, "Currency? (e.g., USD, BTC, ETH)")
        return ASSET_CURRENCY
    except ValueError:
        await send_and_delete(update, context, "That's not a number. Try again.")
        return ASSET_AMOUNT

async def get_asset_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['asset_currency'] = update.message.text.upper()
    await send_and_delete(update, context, "Asset type? (cash, crypto, stocks, bonds, real_estate, commodities, other)")
    return ASSET_TYPE

async def save_asset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Delete user's message first
    if update.message:
        try:
            await update.message.delete()
        except BadRequest as e:
            logger.warning(f"Could not delete user's message: {e}")
    
    asset_type = update.message.text.lower()
    name = context.user_data['asset_name']
    amount = context.user_data['asset_amount']
    currency = context.user_data['asset_currency']
    
    try:
        conn = db_connect()
        cursor = conn.cursor()
        
        # Check if asset already exists, update if it does
        cursor.execute(
            "SELECT id FROM assets WHERE user_id = ? AND name = ? AND currency = ?",
            (update.effective_user.id, name, currency)
        )
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute(
                "UPDATE assets SET amount = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (amount, existing[0])
            )
            action = "updated"
        else:
            cursor.execute(
                "INSERT INTO assets (user_id, name, amount, currency, asset_type) VALUES (?, ?, ?, ?, ?)",
                (update.effective_user.id, name, amount, currency, asset_type)
            )
            action = "added"
        
        conn.commit()
        conn.close()
        
        formatted_amount = fmt_currency_amount(amount, currency)
        await send_and_delete(update, context, f"🏦 Asset {action}: {name} - {formatted_amount}")
        
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error saving asset: {e}")
        await send_and_delete(update, context, "Error saving asset. Try again.")
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def view_assets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    assets = get_user_assets(update.effective_user.id)
    summary = fmt_asset_summary(assets)
    await send_and_delete(update, context, summary, parse_mode='Markdown')

@restricted
async def asset_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    assets = get_user_assets(update.effective_user.id)
    summary = fmt_asset_summary(assets)
    await send_and_delete(update, context, summary, parse_mode='Markdown')

@restricted
async def update_asset_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        if update.message:
            await update.message.delete()
    except BadRequest as e:
        logger.warning(f"Could not delete user's message: {e}")

    assets = get_user_assets(update.effective_user.id)
    if not assets:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="🏦 No assets found. Use `add asset` to create one first.")
        return ConversationHandler.END
    
    reply_markup = generate_asset_keyboard(assets, prefix="update_asset", page=0)
    await context.bot.send_message(chat_id=update.effective_chat.id, text="💼 Which asset do you want to update?", reply_markup=reply_markup)
    return UPDATE_ASSET_SELECT

async def navigate_asset_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        data_payload = query.data[4:]  # Removes "nav_"
        prefix, page_str = data_payload.rsplit('_', 1)
        page = int(page_str)
    except (ValueError, IndexError) as e:
        logger.error(f"Could not parse page number from callback_data: '{query.data}'. Error: {e}")
        await query.edit_message_text(text="Error processing navigation. Please try again.")
        return

    assets = get_user_assets(query.from_user.id)
    reply_markup = generate_asset_keyboard(assets, prefix=prefix, page=page)

    try:
        await query.edit_message_reply_markup(reply_markup)
    except BadRequest as e:
        if 'Message is not modified' not in str(e):
             logger.warning(f"Failed to edit message reply markup for navigation: {e}")
             await query.edit_message_text(text="Could not update the list. Please try again.")

async def select_asset_for_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    asset_id = int(query.data.split("_")[-1])
    context.user_data['selected_asset_id'] = asset_id
    
    asset = get_asset_by_id(asset_id)
    if not asset:
        await query.edit_message_text(text="❌ Error: Asset not found. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

    asset_id, name, amount, currency, asset_type, _, _ = asset
    formatted_amount = fmt_currency_amount(amount, currency)
    
    await query.edit_message_text(
        text=f"💼 **{name}** ({asset_type.title()})\n"
             f"Current: {formatted_amount}\n\n"
             f"Enter the amount to add (+) or subtract (-):\n"
             f"Examples: `+500`, `-250`, `+0.5`"
    )
    return UPDATE_ASSET_AMOUNT

async def process_asset_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        # Delete user's message first
        if update.message:
            try:
                await update.message.delete()
            except BadRequest as e:
                logger.warning(f"Could not delete user's message: {e}")
        
        amount_text = update.message.text.strip()
        asset_id = context.user_data.get('selected_asset_id')

        if asset_id is None:
            await send_and_delete(update, context, "❌ Lost track of which asset we were updating. Please start again.")
            context.user_data.clear()
            return ConversationHandler.END

        # Parse the amount and operation
        if amount_text.startswith('+'):
            operation = 'add'
            amount = float(amount_text[1:])
        elif amount_text.startswith('-'):
            operation = 'subtract'
            amount = float(amount_text[1:])
        else:
            # If no sign, assume it's an addition
            operation = 'add'
            amount = float(amount_text)

        if amount <= 0:
            await send_and_delete(update, context, "❌ Amount must be greater than 0. Try again.")
            return UPDATE_ASSET_AMOUNT

        # Get asset details before update
        asset = get_asset_by_id(asset_id)
        if not asset:
            await send_and_delete(update, context, "❌ Asset not found. Please try again.")
            context.user_data.clear()
            return ConversationHandler.END

        old_amount = asset[2]
        name = asset[1]
        currency = asset[3]
        asset_type = asset[4]

        # Update the asset
        success = update_asset_amount(asset_id, amount, operation)
        
        if success:
            # Get updated asset details
            updated_asset = get_asset_by_id(asset_id)
            new_amount = updated_asset[2]
            
            # Format the response
            old_formatted = fmt_currency_amount(old_amount, currency)
            new_formatted = fmt_currency_amount(new_amount, currency)
            change_formatted = fmt_currency_amount(amount, currency)
            
            operation_symbol = "+" if operation == 'add' else "-"
            operation_text = "Added" if operation == 'add' else "Subtracted"
            
            type_emojis = {
                'cash': '💵', 'crypto': '₿', 'stocks': '📈', 'bonds': '🏛️',
                'real_estate': '🏠', 'commodities': '🥇', 'other': '💼'
            }
            emoji = type_emojis.get(asset_type.lower(), '💼')
            
            response = (f"✅ **Asset Updated Successfully!**\n\n"
                       f"{emoji} **{name}** ({asset_type.title()})\n"
                       f"Previous: `{old_formatted}`\n"
                       f"Change: `{operation_symbol}{change_formatted}`\n"
                       f"**New Total: `{new_formatted}`**")
            
            await send_and_delete(update, context, response, parse_mode='Markdown')
        else:
            await send_and_delete(update, context, "❌ Failed to update asset. Please try again.")

        context.user_data.clear()
        return ConversationHandler.END

    except ValueError:
        await send_and_delete(update, context, "❌ Invalid amount format. Use +100, -50, or just 100")
        return UPDATE_ASSET_AMOUNT
    except Exception as e:
        logger.error(f"Error in process_asset_update: {e}")
        await send_and_delete(update, context, "❌ An error occurred. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

@restricted
async def delete_asset_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        if update.message:
            await update.message.delete()
    except BadRequest as e:
        logger.warning(f"Could not delete user's message: {e}")

    assets = get_user_assets(update.effective_user.id)
    if not assets:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="🏦 No assets found to delete. Use `add asset` to create one first.")
        return ConversationHandler.END
    
    reply_markup = generate_asset_keyboard(assets, prefix="delete_asset", page=0)
    await context.bot.send_message(chat_id=update.effective_chat.id, text="🗑️ Which asset do you want to delete?", reply_markup=reply_markup)
    return DELETE_ASSET_SELECT

async def navigate_delete_asset_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        data_payload = query.data[4:]  # Removes "nav_"
        prefix, page_str = data_payload.rsplit('_', 1)
        page = int(page_str)
    except (ValueError, IndexError) as e:
        logger.error(f"Could not parse page number from callback_data: '{query.data}'. Error: {e}")
        await query.edit_message_text(text="Error processing navigation. Please try again.")
        return

    assets = get_user_assets(query.from_user.id)
    reply_markup = generate_asset_keyboard(assets, prefix=prefix, page=page)

    try:
        await query.edit_message_reply_markup(reply_markup)
    except BadRequest as e:
        if 'Message is not modified' not in str(e):
             logger.warning(f"Failed to edit message reply markup for navigation: {e}")
             await query.edit_message_text(text="Could not update the list. Please try again.")

async def confirm_asset_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    asset_id = int(query.data.split("_")[-1])
    asset = get_asset_by_id(asset_id)
    
    if not asset:
        await query.edit_message_text(text="❌ Error: Asset not found.")
        return ConversationHandler.END

    asset_id, name, amount, currency, asset_type, _, _ = asset
    formatted_amount = fmt_currency_amount(amount, currency)
    
    type_emojis = {
        'cash': '💵', 'crypto': '₿', 'stocks': '📈', 'bonds': '🏛️',
        'real_estate': '🏠', 'commodities': '🥇', 'other': '💼'
    }
    emoji = type_emojis.get(asset_type.lower(), '💼')
    
    # Delete the asset
    success = delete_asset_from_db(asset_id)
    
    if success:
        await query.edit_message_text(
            text=f"🗑️ **Asset Deleted Successfully!**\n\n"
                 f"{emoji} **{name}** ({asset_type.title()})\n"
                 f"Value: `{formatted_amount}`\n\n"
                 f"💀 Gone forever. Hope you don't regret this."
        )
    else:
        await query.edit_message_text(text="❌ Failed to delete asset. Please try again.")
    
    return ConversationHandler.END

@restricted
async def view_all_assets_detailed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show a detailed view of all assets with creation/update dates"""
    assets = get_user_assets(update.effective_user.id)
    
    if not assets:
        message = "🏦 **Complete Asset Portfolio**\n\n💰 Your vault is completely empty. Time to start building wealth!"
    else:
        # Group by asset type and currency for totals
        by_type = {}
        totals_by_currency = {}
        
        for asset_id, name, amount, currency, asset_type, created_at, updated_at in assets:
            if asset_type not in by_type:
                by_type[asset_type] = []
            by_type[asset_type].append((name, amount, currency, created_at, updated_at))
            totals_by_currency[currency] = totals_by_currency.get(currency, 0) + amount
        
        message = "🏦 **Complete Asset Portfolio**\n\n"
        
        # Total summary
        message += "💎 **Portfolio Value:**\n"
        for currency, total in sorted(totals_by_currency.items()):
            message += f"  {fmt_currency_amount(total, currency)}\n"
        
        message += f"\n📊 **Assets by Category ({len(assets)} total):**\n"
        
        type_emojis = {
            'cash': '💵', 'crypto': '₿', 'stocks': '📈', 'bonds': '🏛️',
            'real_estate': '🏠', 'commodities': '🥇', 'other': '💼'
        }
        
        for asset_type, type_assets in by_type.items():
            emoji = type_emojis.get(asset_type.lower(), '💼')
            message += f"\n{emoji} **{asset_type.title()}:**\n"
            
            for name, amount, currency, created_at, updated_at in type_assets:
                formatted_amount = fmt_currency_amount(amount, currency)
                
                # Parse dates
                created_date = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S').strftime('%b %d, %Y')
                updated_date = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S').strftime('%b %d, %Y')
                
                message += f"  • **{name}**: `{formatted_amount}`\n"
                if created_at != updated_at:
                    message += f"    📅 Created: {created_date} | 🔄 Updated: {updated_date}\n"
                else:
                    message += f"    📅 Created: {created_date}\n"
        
        # Add portfolio insights
        total_value_usd = sum(total for currency, total in totals_by_currency.items() if currency == 'USD')
        if total_value_usd > 0:
            message += f"\n💡 **Insights:**\n"
            message += f"  • USD Portfolio Value: {fmt_currency_amount(total_value_usd, 'USD')}\n"
            message += f"  • Asset Categories: {len(by_type)}\n"
            message += f"  • Most Common Type: {max(by_type.keys(), key=lambda k: len(by_type[k]))}\n"
    
    await send_and_delete(update, context, message, parse_mode='Markdown')

def main() -> None:
    init_db()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).connect_timeout(30).read_timeout(30).build()
    application.add_error_handler(error_handler)
    
    # Regex patterns are case-insensitive
    conv_handler_new_goal = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^new goal$', re.IGNORECASE)), new_goal_start)],
        states={
            GOAL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_goal_name)],
            GOAL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_goal_amount)],
            GOAL_CURRENCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_goal_currency_and_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_new_debt = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^new debt$', re.IGNORECASE)), new_debt_start)],
        states={
            DEBT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_debt_name)],
            DEBT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_debt_amount)],
            DEBT_CURRENCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_debt_currency_and_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_add = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^\s*add\s*$', re.IGNORECASE)), add_start)],
        states={
            ADD_SAVINGS_GOAL: [
                CallbackQueryHandler(navigate_menu, pattern="^nav_add_to_"),
                CallbackQueryHandler(select_goal_for_adding, pattern="^add_to_"),
            ],
            ADD_SAVINGS_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_amount_and_save)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_delete = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^delete$', re.IGNORECASE)), delete_start)],
        states={
            DELETE_GOAL_CONFIRM: [
                CallbackQueryHandler(navigate_menu, pattern="^nav_delete_"),
                CallbackQueryHandler(confirm_delete, pattern="^delete_"),
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_progress = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^progress$', re.IGNORECASE)), progress_start)],
        states={
            PROGRESS_GOAL_SELECT: [
                CallbackQueryHandler(navigate_menu, pattern="^nav_progress_"),
                CallbackQueryHandler(show_goal_progress, pattern="^progress_"),
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_reminder = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^set reminder$', re.IGNORECASE)), set_reminder_start)],
        states={REMINDER_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_reminder_time)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_add_expense = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^add expense$', re.IGNORECASE)), add_expense_start)],
        states={
            EXPENSE_AMOUNT: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_expense_amount)
            ],
            EXPENSE_CURRENCY: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_expense_currency)
            ],
            EXPENSE_CATEGORY: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_expense_category)
            ],
            EXPENSE_REASON: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_expense)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_set_budget = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^set budget$', re.IGNORECASE)), set_budget_start)],
        states={
            BUDGET_CATEGORY: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_budget_category)
            ],
            BUDGET_AMOUNT: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_budget_amount)
            ],
            BUDGET_CURRENCY: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_budget_currency)
            ],
            BUDGET_PERIOD: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_budget)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_add_asset = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^add asset$', re.IGNORECASE)), add_asset_start)],
        states={
            ASSET_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_asset_name)],
            ASSET_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_asset_amount)],
            ASSET_CURRENCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_asset_currency)],
            ASSET_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_asset)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_update_asset = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^update asset$', re.IGNORECASE)), update_asset_start)],
        states={
            UPDATE_ASSET_SELECT: [
                CallbackQueryHandler(navigate_asset_menu, pattern="^nav_update_asset_"),
                CallbackQueryHandler(select_asset_for_update, pattern="^update_asset_"),
            ],
            UPDATE_ASSET_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_asset_update)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_delete_asset = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^delete asset$', re.IGNORECASE)), delete_asset_start)],
        states={
            DELETE_ASSET_SELECT: [
                CallbackQueryHandler(navigate_delete_asset_menu, pattern="^nav_delete_asset_"),
                CallbackQueryHandler(confirm_asset_delete, pattern="^delete_asset_"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_new_payment = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^new payment$', re.IGNORECASE)), new_payment_start)],
        states={
            PAYMENT_NAME: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_payment_name)
            ],
            PAYMENT_RECIPIENT: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_payment_recipient)
            ],
            PAYMENT_TARGET: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_payment_target)
            ],
            PAYMENT_CURRENCY: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_payment_currency)
            ],
            PAYMENT_AMOUNT: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_payment_amount)
            ],
            PAYMENT_FREQUENCY: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_payment)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_add_payment = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^add payment$', re.IGNORECASE)), add_payment_start)],
        states={
            ADD_PAYMENT_SELECT: [
                CallbackQueryHandler(select_payment_for_adding, pattern="^add_payment_"),
            ],
            ADD_PAYMENT_AMOUNT: [
                MessageHandler(filters.Regex(re.compile(r'^cancel$', re.IGNORECASE)), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_payment_amount_and_save)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_payment_progress = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^payment progress$', re.IGNORECASE)), payment_progress_start)],
        states={
            PAYMENT_PROGRESS_SELECT: [
                CallbackQueryHandler(show_payment_progress, pattern="^payment_progress_"),
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    conv_handler_delete_payment = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(re.compile(r'^delete payment$', re.IGNORECASE)), delete_payment_start)],
        states={
            DELETE_PAYMENT_SELECT: [
                CallbackQueryHandler(confirm_payment_delete, pattern="^delete_payment_"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler_new_goal)
    application.add_handler(conv_handler_new_debt)
    application.add_handler(conv_handler_add)
    application.add_handler(conv_handler_delete)
    application.add_handler(conv_handler_progress)
    application.add_handler(conv_handler_reminder)
    application.add_handler(conv_handler_add_expense)
    application.add_handler(conv_handler_set_budget)
    application.add_handler(conv_handler_add_asset)
    application.add_handler(conv_handler_update_asset)
    application.add_handler(conv_handler_delete_asset)
    application.add_handler(conv_handler_new_payment)
    application.add_handler(conv_handler_add_payment)
    application.add_handler(conv_handler_payment_progress)
    application.add_handler(conv_handler_delete_payment)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^view all$', re.IGNORECASE)), view_all))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^export$', re.IGNORECASE)), export_data))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^expense report$', re.IGNORECASE)), expense_report))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^expense compare$', re.IGNORECASE)), expense_compare))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^view assets$', re.IGNORECASE)), view_assets))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^asset summary$', re.IGNORECASE)), asset_summary))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^view all assets$', re.IGNORECASE)), view_all_assets_detailed))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^budget status$', re.IGNORECASE)), budget_status))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^financial dashboard$', re.IGNORECASE)), financial_dashboard))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^view payments$', re.IGNORECASE)), view_payments))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^erase all$', re.IGNORECASE)), erase_all))
    application.add_handler(CommandHandler("cancel", cancel))
    
    # Move unknown_command to the very end and make it more specific
    # Only catch messages that don't match any of our known patterns
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & 
        ~filters.Regex(re.compile(r'^\s*(add|new goal|new debt|view all|delete|progress|export|set reminder|add expense|expense report|expense compare|add asset|update asset|delete asset|view assets|view all assets|asset summary|set budget|budget status|financial dashboard|new payment|add payment|view payments|payment progress|delete payment|erase all)\s*$', re.IGNORECASE)), 
        unknown_command
    ))

    logger.info("Snarky Savings Bot is online...")
    application.run_polling()


if __name__ == "__main__":
    main()
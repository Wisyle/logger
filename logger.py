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
ALLOWED_USER_ID = 5134940733  # Replace with your actual user ID
MESSAGE_DELETION_DELAY = 300  # 5 minutes in seconds
ITEMS_PER_PAGE = 5  # For paginated keyboards

# --- Personality ---
STARTUP_MESSAGES = [
    "Powered up and ready to judge your spending habits.", "I have been summoned. Let's make some money moves.",
    "The financial overlord is online. Try to impress me.",
]
MANUAL_TEXT = (f"**{random.choice(STARTUP_MESSAGES)}**\n\nHere's the command deck. Let's make some magic happen (or at least track it).\n\n"
               "🎯 **Goals & Debts**\n  - `new goal`\n  - `new debt`\n  - `view all`\n  - `delete`\n\n"
               "💰 **Money Moves**\n  - `add`\n  - `progress`\n\n"
               "🛠️ **Utilities**\n  - `set reminder`\n  - `export`\n  - `cancel`")

# --- States for ConversationHandler ---
(GOAL_NAME, GOAL_AMOUNT, GOAL_CURRENCY,
 ADD_SAVINGS_GOAL, ADD_SAVINGS_AMOUNT,
 DELETE_GOAL_CONFIRM, REMINDER_TIME,
 DEBT_NAME, DEBT_AMOUNT, DEBT_CURRENCY,
 PROGRESS_GOAL_SELECT) = range(11)

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

# --- PDF Generation ---
def generate_pdf_report(records: List[Tuple], summary_data: List[List[str]], pdf_path: str):
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []
    elements.append(Paragraph("Savings & Debts Report", styles['Title']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("Summary", styles['h2']))
    summary_table = Table(summary_data, colWidths=[2.5 * inch, 2.5 * inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey), ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12), ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('SPAN', (0, 0), (1, 0)),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 24))
    elements.append(Paragraph("Transaction History", styles['h2']))
    header = ["Name", "Type", "Amount", "Currency", "Date"]
    table_data = [header] + [[r[0], r[1], f"{r[4]:,.2f}", r[3], datetime.strptime(r[5], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')] for r in records]
    records_table = Table(table_data, colWidths=[2*inch, 0.8*inch, 1*inch, 1*inch, 1.5*inch])
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue), ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ])
    for i in range(1, len(table_data)):
        bg_color = colors.lightblue if i % 2 == 0 else colors.beige
        style.add('BACKGROUND', (0, i), (-1, i), bg_color)
    records_table.setStyle(style)
    elements.append(records_table)
    doc.build(elements)


# --- Message Handling & Decorators (No changes from original) ---
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
        if update.effective_user.id != ALLOWED_USER_ID:
            await (update.message or update.callback_query).reply_text("⛔️ Access Denied. I'm a one-person bot. And you're not that person.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


# --- Command & Conversation Handlers (Largely unchanged) ---

@restricted
async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Brewing up your financial reports...")
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute("SELECT g.name, g.type, g.target_amount, g.currency, s.amount, s.saved_at FROM goals g JOIN savings s ON g.id = s.goal_id WHERE g.user_id = ? ORDER BY g.name, s.saved_at", (ALLOWED_USER_ID,))
    records = cursor.fetchall()
    goals_summary = get_user_goals_and_debts(ALLOWED_USER_ID)
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
    await send_and_delete(update, context, MANUAL_TEXT, parse_mode='Markdown')

@restricted
async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_and_delete(update, context, f"I don't know what '{update.message.text}' means. Stick to the script.\n\n" + MANUAL_TEXT, parse_mode='Markdown')

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
        cursor.execute("INSERT INTO goals (user_id, name, target_amount, currency, type) VALUES (?, ?, ?, ?, ?)", (ALLOWED_USER_ID, context.user_data['goal_name'], context.user_data['goal_amount'], currency, 'goal'))
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
        cursor.execute("INSERT INTO goals (user_id, name, target_amount, currency, type) VALUES (?, ?, ?, ?, ?)", (ALLOWED_USER_ID, context.user_data['debt_name'], context.user_data['debt_amount'], currency, 'debt'))
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

    application.add_handler(conv_handler_new_goal)
    application.add_handler(conv_handler_new_debt)
    application.add_handler(conv_handler_add)
    application.add_handler(conv_handler_delete)
    application.add_handler(conv_handler_progress)
    application.add_handler(conv_handler_reminder)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^view all$', re.IGNORECASE)), view_all))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r'^export$', re.IGNORECASE)), export_data))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_command))

    logger.info("Snarky Savings Bot is online...")
    application.run_polling()


if __name__ == "__main__":
    main()
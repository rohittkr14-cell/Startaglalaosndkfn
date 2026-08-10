import asyncio
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import FloodWait
from pyrogram.types import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Message

# ======================= CONFIG =======================
API_ID = int(os.environ.get("API_ID", 37893084 ))            # from my.telegram.org
API_HASH = os.environ.get("API_HASH", "853a6c0f3be11009f667bc153244452e")    # from my.telegram.org
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8642917919:AAHR1U_FtsL_X9XtUROTvpXc6lgjNLDz83M") # from @BotFather
BOT_USERNAME = os.environ.get("BOT_USERNAME", "Star4tagbot")  # without @

# Only these user IDs can use /broadcast (comma-separated in env var)
ADMIN_IDS = [
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "7691071175").split(",") if x.strip()
]

TAG_BATCH = 5        # Telegram limit: only 5 mentions notify per message
SEND_DELAY = 3.0     # seconds between messages (smooth, avoids flood)
PORT = int(os.environ.get("PORT", 8080))   # fake port for hosting platforms
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

# Trigger: @all, #all or /all at the start of a message
TRIGGER_RE = re.compile(r"^(?:[@#/]all)(.*)$", re.IGNORECASE | re.DOTALL)
# ======================================================

app = Client(
    "star_all_tag_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60,   # Pyrogram auto-sleeps on flood waits up to 60s
)

# chat_id -> {"starter": user_id, "task": asyncio.Task}
running_tasks = {}


# ---------------- PostgreSQL database ----------------
def get_conn():
    """Connect to Postgres (SSH-style SSL first, fallback plain)."""
    try:
        return psycopg2.connect(DATABASE_URL, sslmode="require")
    except psycopg2.OperationalError:
        return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS settings "
            "(chat_id TEXT PRIMARY KEY, only_admins INTEGER NOT NULL DEFAULT 0)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS groups "
            "(chat_id TEXT PRIMARY KEY, title TEXT)"
        )
    conn.commit()
    conn.close()


def get_setting(chat_id: int) -> bool:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT only_admins FROM settings WHERE chat_id = %s", (str(chat_id),))
        row = cur.fetchone()
    conn.close()
    return bool(row and row[0])


def set_setting(chat_id: int, only_admins: bool):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO settings (chat_id, only_admins) VALUES (%s, %s) "
            "ON CONFLICT (chat_id) DO UPDATE SET only_admins = EXCLUDED.only_admins",
            (str(chat_id), 1 if only_admins else 0),
        )
    conn.commit()
    conn.close()


def add_group(chat_id: int, title: str):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO groups (chat_id, title) VALUES (%s, %s) "
            "ON CONFLICT (chat_id) DO NOTHING",
            (str(chat_id), title or "Unknown"),
        )
    conn.commit()
    conn.close()


def remove_group(chat_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM groups WHERE chat_id = %s", (str(chat_id),))
    conn.commit()
    conn.close()


def get_all_groups():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT chat_id, title FROM groups")
        rows = cur.fetchall()
    conn.close()
    return rows


# ---------------- Fake port (dummy web server) ----------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def run_http_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    print(f"[+] Fake port server running on port {PORT}")
    server.serve_forever()


threading.Thread(target=run_http_server, daemon=True).start()


# ---------------- Helpers ----------------
def mention(user) -> str:
    name = (user.first_name or "User").replace("[", "").replace("]", "")
    return f"[{name}](tg://user?id={user.id})"


async def safe_send(chat_id: int, text: str, retries: int = 5):
    """Send a message. If Telegram says FloodWait, wait it out and retry — never crashes."""
    for _ in range(retries):
        try:
            return await app.send_message(chat_id, text)
        except FloodWait as e:
            print(f"[*] FloodWait {e.value}s — waiting...")
            await asyncio.sleep(e.value)
    raise FloodWait(f"Still flooded after {retries} retries")


async def get_members(chat_id: int):
    """Full member list via bot account (MTProto). Group must be a supergroup."""
    members = []
    async for member in app.get_chat_members(chat_id):
        user = member.user
        if user and not user.is_bot and not user.is_deleted:
            members.append(user)
    return members


async def is_admin_user(chat_id: int, user_id: int) -> bool:
    try:
        member = await app.get_chat_member(chat_id, user_id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False


def send_batches(chat_id: int, members: list, extra: str = ""):
    """Build batch texts: 5 mentions per message. First batch gets extra text if given."""
    batches = []
    total = len(members)
    for i in range(0, total, TAG_BATCH):
        chunk = members[i:i + TAG_BATCH]
        mentions = " ".join(mention(u) for u in chunk)
        if extra and i == 0:
            batches.append(f"{extra}\n\n{mentions}")   # first batch: text + mentions
        else:
            batches.append(mentions)                    # rest: just mentions
    return batches


async def tag_once(chat_id: int, members: list, extra: str):
    """Single pass — tags everyone once, first message includes the user's text."""
    try:
        for text in send_batches(chat_id, members, extra):
            await safe_send(chat_id, text)
            await asyncio.sleep(SEND_DELAY)
    except Exception as e:
        print(f"[!] Tagging error: {e}")


async def tag_task(chat_id: int, members: list):
    """Continuous loop — tags everyone, then restarts, until /stopall. No text."""
    try:
        while True:
            for text in send_batches(chat_id, members):
                await safe_send(chat_id, text)
                await asyncio.sleep(SEND_DELAY)
            # Full cycle done → start again from the top
    except asyncio.CancelledError:
        raise  # /stopall cancels → clean exit
    except Exception as e:
        print(f"[!] Tagging error: {e}")
    finally:
        running_tasks.pop(chat_id, None)


def get_add_group_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{BOT_USERNAME}?startgroup=start")]
    ])


# ---------------- Startup: commands menu + DB + group backfill ----------------
@app.on_startup()
async def startup(client):
    init_db()
    await client.set_bot_commands([
        BotCommand("all", "Tag all members"),
        BotCommand("start", "Show help"),
        BotCommand("stopall", "Stop the tagging"),
        BotCommand("onlyadmins", "Bot works only for admins"),
        BotCommand("noonlyadmins", "Bot works for everyone"),
        BotCommand("broadcast", "Broadcast message to all groups (admin only)"),
    ])
    print("[+] Commands menu set")

    # Backfill: store all groups the bot is currently in
    try:
        count = 0
        async for dialog in app.get_dialogs():
            chat = dialog.chat
            if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
                add_group(chat.id, getattr(chat, "title", None) or "Unknown")
                count += 1
        print(f"[+] Backfilled {count} groups from dialogs")
    except Exception as e:
        print(f"[!] Dialog backfill failed: {e}")


# Track when the bot is added to / removed from a group
@app.on_chat_member_updated()
async def track_group(client, event):
    try:
        new = event.new_chat_member
        if not new or not new.user or not new.user.is_self:
            return
        chat = event.chat
        if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        if new.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
            add_group(chat.id, getattr(chat, "title", None) or "Unknown")
            print(f"[+] Bot added to group: {getattr(chat, 'title', 'Unknown')} ({chat.id})")
        else:
            remove_group(chat.id)
            print(f"[-] Bot removed from group: {chat.id}")
    except Exception as e:
        print(f"[!] track_group error: {e}")


# ---------------- Handlers ----------------
@app.on_message(filters.command("start") & filters.private)
async def start_private(client, message: Message):
    await message.reply_text(
        "👋 Welcome to **Star All tag Bot**!\n\n"
        "Add me to a group, then use these triggers:\n\n"
        "`@all <text>` — tag all members once, with your text\n"
        "`@all` — tag all members continuously (no text) until `/stopall`\n"
        "`#all` and `/all` work the same way\n\n"
        "**Commands (in group):**\n"
        "`/stopall` — stop the tagging (only starter or admin)\n"
        "`/onlyadmins` — bot works only for admins\n"
        "`/noonlyadmins` — bot works for everyone\n\n"
        "**Admin commands:**\n"
        "`/broadcast <message>` — send to all groups where the bot is",
        reply_markup=get_add_group_keyboard(),
    )


@app.on_message(filters.command("start") & filters.group)
async def start_group(client, message: Message):
    """/start in group → just shows help, does NOT start tagging."""
    await message.reply_text(
        "Send `@all <text>` to tag all members once, "
        "or `@all` alone to tag continuously until `/stopall`."
    )


@app.on_message(filters.group & filters.text & filters.regex(TRIGGER_RE))
async def all_trigger(client, message: Message):
    """@all / #all / /all → tag everyone. With text = once, without = continuous loop."""
    chat_id = message.chat.id
    user_id = message.from_user.id

    if chat_id in running_tasks:
        await message.reply_text("⏳ Tagging is already running here. Use /stopall to stop it.")
        return

    if get_setting(chat_id) and not await is_admin_user(chat_id, user_id):
        await message.reply_text("🔒 Only admins can use this bot here.")
        return

    m = TRIGGER_RE.match(message.text)
    extra = (m.group(1) or "").strip() if m else ""

    try:
        members = await get_members(chat_id)
    except FloodWait as e:
        await message.reply_text(f"⏳ Too many requests. Try again in {e.value}s.")
        return
    except Exception as e:
        await message.reply_text(f"❌ Could not fetch member list: {e}")
        return

    if not members:
        await message.reply_text(
            "❌ No members found. Make sure the group is a **supergroup** "
            "(Group Info → Convert to Supergroup)."
        )
        return

    me = await app.get_me()
    members = [u for u in members if u.id != me.id]

    if extra:
        await tag_once(chat_id, members, extra)
    else:
        task = asyncio.create_task(tag_task(chat_id, members))
        running_tasks[chat_id] = {"starter": user_id, "task": task}


@app.on_message(filters.command("stopall") & filters.group)
async def stop_all(client, message: Message):
    chat_id = message.chat.id
    entry = running_tasks.get(chat_id)
    if not entry:
        await message.reply_text("❌ No tagging is running in this group.")
        return

    user = message.from_user
    is_starter = entry["starter"] == user.id
    is_admin = await is_admin_user(chat_id, user.id)
    if not (is_starter or is_admin):
        await message.reply_text("🔒 Only the person who started it or an admin can stop it.")
        return

    entry["task"].cancel()
    await message.reply_text("⏹ Tagging stopped.")


@app.on_message(filters.command("onlyadmins") & filters.group)
async def only_admins_cmd(client, message: Message):
    chat_id = message.chat.id
    if not await is_admin_user(chat_id, message.from_user.id):
        await message.reply_text("🔒 Only admins can change this setting.")
        return
    set_setting(chat_id, True)
    await message.reply_text("✅ Bot now works **only for admins**.")


@app.on_message(filters.command("noonlyadmins") & filters.group)
async def no_only_admins_cmd(client, message: Message):
    chat_id = message.chat.id
    if not await is_admin_user(chat_id, message.from_user.id):
        await message.reply_text("🔒 Only admins can change this setting.")
        return
    set_setting(chat_id, False)
    await message.reply_text("✅ Bot now works **for everyone**.")


@app.on_message(filters.command("broadcast"))
async def broadcast_cmd(client, message: Message):
    """Admin-only: send a message to every group the bot is in."""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.reply_text("🔒 Only bot admins can use /broadcast.")
        return

    parts = message.text.split(" ", 1)
    broadcast_text = parts[1].strip() if len(parts) > 1 else ""

    if not broadcast_text and message.reply_to_message and message.reply_to_message.text:
        broadcast_text = message.reply_to_message.text

    if not broadcast_text:
        await message.reply_text(
            "Usage: `/broadcast <message>`\n"
            "or reply to any message with `/broadcast`."
        )
        return

    groups = get_all_groups()
    if not groups:
        await message.reply_text(
            "❌ No groups found in database.\n"
            "Bot stores groups automatically when added. Re-add it to groups if needed."
        )
        return

    await message.reply_text(f"📢 Broadcasting to **{len(groups)}** groups...")

    ok = 0
    failed = 0
    for chat_id, title in groups:
        try:
            await safe_send(int(chat_id), broadcast_text)
            ok += 1
        except Exception as e:
            failed += 1
            print(f"[!] Broadcast failed to {title} ({chat_id}): {e}")

    await message.reply_text(f"✅ **Broadcast finished!**\nSent: {ok}\nFailed: {failed}")


@app.on_message(filters.command("clone") & filters.private)
async def clone_cmd(client, message: Message):
    await message.reply_text(
        "📋 **How to clone this bot:**\n\n"
        "1. Create a new bot via @BotFather (name: Star All tag Bot, your own username)\n"
        "2. `/setprivacy` → **Disable**\n"
        "3. Get `api_id` / `api_hash` from my.telegram.org\n"
        "4. Set env vars: `API_ID`, `API_HASH`, `BOT_TOKEN`, `BOT_USERNAME`, `ADMIN_IDS`\n"
        "5. `pip install -r requirements.txt`\n"
        "6. `python bot.py`\n\n"
        "Done! Add the bot to a group and send `@all`. 🎉",
        reply_markup=get_add_group_keyboard(),
    )


if __name__ == "__main__":
    print("⭐ Star All tag Bot is running...")
    app.run()
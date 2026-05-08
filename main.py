import asyncio
import os
import re
import logging
from pyrogram import Client, filters, enums
from pyrogram.errors import RPCError, UserNotParticipant, FloodWait
from pyrogram.types import Message
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import GroupCallConfig, MediaStream
from pymongo import MongoClient
from datetime import datetime, timedelta
from functools import wraps

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
LOG_GROUP_ID = int(os.environ.get("LOG_GROUP_ID", 0))

ALLOWED_GROUP = -1001952511944
JOIN_REQUIRED_MSG = "You must join the allowed group before using /getip."

ALLOWED_MEMBER_STATUSES = (
    enums.ChatMemberStatus.MEMBER,
    enums.ChatMemberStatus.ADMINISTRATOR,
    enums.ChatMemberStatus.OWNER,
)

RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

db_client = MongoClient(MONGO_URI)
db = db_client["getip_bot"]
users_db = db["users"]
assistants_db = db["assistants"]
invite_links_db = db["invite_links"]
rate_limit_db = db["rate_limits"]

bot = Client("bot_wrapper", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

assistants = []
calls = {}


# ── Helpers ──────────────────────────────────────────────────────────────────

async def send_log(text: str):
    if not LOG_GROUP_ID:
        return
    try:
        await bot.send_message(LOG_GROUP_ID, text, disable_web_page_preview=True)
    except FloodWait as e:
        logger.warning(f"FloodWait on log send: sleeping {e.value}s")
        await asyncio.sleep(e.value)
        try:
            await bot.send_message(LOG_GROUP_ID, text, disable_web_page_preview=True)
        except Exception as ex:
            logger.error(f"Log send retry failed: {ex}")
    except Exception as e:
        logger.error(f"Failed to send log: {e}")


async def with_floodwait(coro, retries=3):
    for attempt in range(retries):
        try:
            return await coro
        except FloodWait as e:
            wait = e.value + 2
            logger.warning(f"FloodWait {e.value}s (attempt {attempt+1}/{retries}), sleeping {wait}s")
            await asyncio.sleep(wait)
        except Exception as e:
            raise e
    raise RuntimeError(f"Failed after {retries} retries due to FloodWait")


def get_user_tag(m: Message) -> str:
    u = m.from_user
    name = f"{u.first_name or ''} {u.last_name or ''}".strip() or "Unknown"
    mention = f"[{name}](tg://user?id={u.id})"
    username = f"@{u.username}" if u.username else "no username"
    return f"{mention} (`{u.id}` | {username})"


def get_chat_tag(m: Message) -> str:
    if m.chat.type == enums.ChatType.PRIVATE:
        return "Private Chat"
    title = m.chat.title or "Unknown"
    username = f"@{m.chat.username}" if m.chat.username else str(m.chat.id)
    return f"{title} ({username})"


# ── Rate Limit ────────────────────────────────────────────────────────────────

def check_rate_limit(user_id: int) -> tuple[bool, int]:
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=RATE_LIMIT_WINDOW)
    record = rate_limit_db.find_one({"_id": user_id})

    if not record:
        rate_limit_db.insert_one({"_id": user_id, "requests": [now]})
        return True, RATE_LIMIT_MAX - 1

    requests = [r for r in record.get("requests", []) if r > window_start]
    remaining = RATE_LIMIT_MAX - len(requests)

    if remaining <= 0:
        oldest = min(requests)
        retry_after = int((oldest + timedelta(seconds=RATE_LIMIT_WINDOW) - now).total_seconds()) + 1
        return False, retry_after

    requests.append(now)
    rate_limit_db.update_one({"_id": user_id}, {"$set": {"requests": requests}})
    return True, remaining - 1


def rate_limited(func):
    @wraps(func)
    async def wrapper(c: Client, m: Message, *args, **kwargs):
        if not m.from_user:
            return await func(c, m, *args, **kwargs)
        if m.from_user.id == ADMIN_ID:
            return await func(c, m, *args, **kwargs)
        allowed, value = check_rate_limit(m.from_user.id)
        if not allowed:
            return await m.reply(
                f"⏳ Rate limit exceeded. Try again in **{value}s**.\n"
                f"Max **{RATE_LIMIT_MAX}** requests per **{RATE_LIMIT_WINDOW}s**."
            )
        return await func(c, m, *args, **kwargs)
    return wrapper


# ── Force Subscribe ───────────────────────────────────────────────────────────

async def get_or_create_invite_link(chat_id: int) -> str:
    cached = invite_links_db.find_one({"_id": chat_id})
    now = datetime.utcnow()

    if cached:
        expire = cached.get("expire_date")
        if expire and expire > now:
            return cached["link"]

    expire_date = now + timedelta(days=7)
    link_obj = await with_floodwait(
        bot.create_chat_invite_link(chat_id, name="Force Sub Link", expire_date=expire_date)
    )
    invite_links_db.update_one(
        {"_id": chat_id},
        {"$set": {"link": link_obj.invite_link, "expire_date": expire_date}},
        upsert=True,
    )
    return link_obj.invite_link


async def check_force_sub(user_id: int) -> tuple[bool, str]:
    try:
        member = await with_floodwait(bot.get_chat_member(ALLOWED_GROUP, user_id))
        if member.status in ALLOWED_MEMBER_STATUSES:
            return True, ""
        invite_link = await get_or_create_invite_link(ALLOWED_GROUP)
        return False, invite_link
    except UserNotParticipant:
        invite_link = await get_or_create_invite_link(ALLOWED_GROUP)
        return False, invite_link
    except RPCError:
        return False, ""


# ── User DB ───────────────────────────────────────────────────────────────────

def check_user_limit(user_id: int) -> bool:
    user = users_db.find_one({"_id": user_id})
    if not user:
        users_db.insert_one({"_id": user_id, "limit": 5, "used": 0})
        return True
    return user["used"] < user["limit"]


def increment_user_usage(user_id: int):
    users_db.update_one({"_id": user_id}, {"$inc": {"used": 1}})


def get_user_stats(user_id: int) -> tuple[int, int]:
    user = users_db.find_one({"_id": user_id})
    if not user:
        return 0, 5
    return user.get("used", 0), user.get("limit", 5)


# ── Assistants ────────────────────────────────────────────────────────────────

async def load_assistants():
    global assistants, calls
    default_session = os.environ.get("SESSION_STRING")
    if default_session:
        app = Client("assistant_env", api_id=API_ID, api_hash=API_HASH, session_string=default_session)
        await app.start()
        assistants.append(app)
        calls[app] = PyTgCalls(app)
        await calls[app].start()

    for ast in assistants_db.find():
        app = Client(
            f"assistant_{ast['_id']}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=ast["session_string"],
        )
        await app.start()
        assistants.append(app)
        calls[app] = PyTgCalls(app)
        await calls[app].start()

    logger.info(f"Loaded {len(assistants)} assistant(s)")


# ── UDP Capture ───────────────────────────────────────────────────────────────

async def capture_udp_ip(timeout=20):
    proc = await asyncio.create_subprocess_exec(
        "tcpdump", "-i", "any", "udp", "-n", "-q", "-l",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    pattern = re.compile(r'IP (\d+\.\d+\.\d+\.\d+)\.(\d+)')
    private = ("10.", "172.", "192.168.", "127.")
    try:
        async def read():
            async for line in proc.stdout:
                line = line.decode()
                for match in pattern.finditer(line):
                    ip, port = match.group(1), match.group(2)
                    if not any(ip.startswith(p) for p in private):
                        return ip, port
            return None, None
        return await asyncio.wait_for(read(), timeout=timeout)
    except asyncio.TimeoutError:
        return None, None
    finally:
        try:
            proc.kill()
        except:
            pass


# ── Failure Message Helper ────────────────────────────────────────────────────

FAILURE_MSG = (
    "🔴 **Hum aapke us private group mein join nahi hain.**\n\n"
    "**Kriyapiya niche diye options try karein:**\n"
    "├ `/getip invite_link` — Invite link dale or approve kre fir dubara /getip chut_id dalne se join karke IP lein\n"
    "├ `/getip chut_id — Agar assistant already group mein ho\n"
    "└ `/getip chat_id session ` — Apna Pyrogram session use karein\n\n"
    "📌 **Apna session string kaise lein?**\n"
    "→ @ArchStringBot se apna session string generate karein\n\n"
    "🔒 _Hum aapka session store nahi karte. Isliye har baar fresh IP ke liye "
    "aapko apna logged-in session dena hoga. Dhanyavaad!_ 🙏"
)


# ── Handlers ──────────────────────────────────────────────────────────────────

@bot.on_message(filters.command("start"))
async def start_command(c: Client, m: Message):
    if not m.from_user:
        return

    user_tag = get_user_tag(m)
    chat_tag = get_chat_tag(m)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    first_name = m.from_user.first_name or "User"

    used, limit = get_user_stats(m.from_user.id)
    remaining = max(0, limit - used)

    logger.info(f"Bot started by {m.from_user.id} in {m.chat.id}")

    await send_log(
        f"🟢 **Bot Started**\n"
        f"👤 User: {user_tag}\n"
        f"💬 Chat: {chat_tag}\n"
        f"🕐 Time: `{now}`"
    )

    await m.reply(
        f"👋 **Hello, {first_name}!**\n\n"
        f"🔍 **What I Do:**\n"
        f"I reveal the **real IP address** of any Telegram group voice/video call "
     #   f"using live UDP packet sniffing.\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📋 **Commands:**\n"
        f"├ `/getip <chat_id>` — Get IP of a group call\n"
        f"├ `/getip <invite_link>` — Join & get IP\n"
        f"└ `/getip <chat_id> <session>` — Use your own session\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📊 **Your Stats:**\n"
        f"├ Used: `{used}` requests\n"
        f"├ Limit: `{limit}` requests\n"
        f"└ Remaining: `{remaining}` requests\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"⚠️ **Requirements:**\n"
        f"You must be a member of the required group to use this bot.\n\n"
        f"🛡️ _Use responsibly. Misuse may result in a ban._",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@bot.on_message(filters.command("approve") & filters.user(ADMIN_ID))
async def approve_user(c: Client, m: Message):
    parts = m.text.split()
    if len(parts) < 3:
        return await m.reply("Usage: /approve {user_id} {limit}")

    user_id = int(parts[1])
    limit = int(parts[2])
    users_db.update_one({"_id": user_id}, {"$set": {"limit": limit}}, upsert=True)

    logger.info(f"Admin approved user {user_id} with limit {limit}")
    await send_log(
        f"✅ **User Approved**\n"
        f"👤 By Admin: `{ADMIN_ID}`\n"
        f"🆔 Target User: `{user_id}`\n"
        f"🔢 New Limit: `{limit}`"
    )
    await m.reply(f"User `{user_id}` limit updated to `{limit}`.")


@bot.on_message(filters.command("getip"))
@rate_limited
async def getip_command(c: Client, m: Message):
    if not m.from_user:
        return await m.reply("Unable to verify your account for this command.")

    user_tag = get_user_tag(m)
    chat_tag = get_chat_tag(m)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    joined, invite_link = await check_force_sub(m.from_user.id)
    if not joined:
        logger.info(f"Force sub failed for {m.from_user.id}")
        await send_log(
            f"🚫 **Force Sub Failed**\n"
            f"👤 User: {user_tag}\n"
            f"💬 Chat: {chat_tag}\n"
            f"🕐 Time: `{now}`"
        )
        if invite_link:
            return await m.reply(
                f"You must join our group to use this bot.\n\n"
                f"👉 [Join Here]({invite_link})\n\n"
                f"After joining, send /getip again.",
                disable_web_page_preview=True,
            )
        return await m.reply("Unable to verify your group membership right now.")

    parts = m.text.split()
    if len(parts) < 2:
        return await m.reply("Usage: /getip <chat_id_or_invite_link> [optional_session_string]")

    target = parts[1]
    temp_session = parts[2] if len(parts) > 2 else None

    if not check_user_limit(m.from_user.id):
        await send_log(
            f"⛔ **Limit Exceeded**\n"
            f"👤 User: {user_tag}\n"
            f"🎯 Target: `{target}`\n"
            f"🕐 Time: `{now}`"
        )
        return await m.reply("You have exceeded your usage limit. Contact Admin.")

    logger.info(f"User {m.from_user.id} requested IP for {target}")
    await send_log(
        f"🔍 **GetIP Request**\n"
        f"👤 User: {user_tag}\n"
        f"💬 Chat: {chat_tag}\n"
        f"🎯 Target: `{target}`\n"
        f"🕐 Time: `{now}`"
    )

    if temp_session:
        user_app = Client("temp", api_id=API_ID, api_hash=API_HASH, session_string=temp_session)
        await user_app.start()
        user_call = PyTgCalls(user_app)
        await user_call.start()
        available_assistants = [(user_app, user_call)]
    else:
        available_assistants = [(app, calls[app]) for app in assistants]

    if not available_assistants:
        return await m.reply("No assistants are available right now.")

    success = False
    error_msg = ""
    status_msg = await m.reply("⏳ Attempting to get IP...")

    if "t.me/" in target or "telegram.me/" in target:
        for app, _ in available_assistants:
            try:
                await app.join_chat(target)
                target_chat = await app.get_chat(target)
                target = target_chat.id
                break
            except FloodWait as e:
                logger.warning(f"FloodWait joining chat: {e.value}s")
                await asyncio.sleep(e.value + 1)
            except Exception as e:
                error_msg = str(e)
                continue
    else:
        try:
            target = int(target)
        except ValueError:
            return await status_msg.edit("Invalid Chat ID or Invite Link.")

    for app, call in available_assistants:
        capture_task = asyncio.create_task(capture_udp_ip(timeout=20))
        await asyncio.sleep(0.5)

        try:
            await call.play(
                target,
                MediaStream("/dev/zero"),
                config=GroupCallConfig(auto_start=True),
            )

            ip, port = await capture_task
            if ip and port:
                await status_msg.edit(
                    f"✅ **Result**\n"
                    f"**Chat ID:** `{target}`\n"
                    f"**IP:** `{ip}`\n"
                    f"**Port:** `{port}`"
                )
                increment_user_usage(m.from_user.id)
                success = True
                await send_log(
                    f"✅ **IP Found**\n"
                    f"👤 User: {user_tag}\n"
                    f"🎯 Chat ID: `{target}`\n"
                    f"🌐 IP: `{ip}` | Port: `{port}`\n"
                    f"🕐 Time: `{now}`"
                )
            else:
                # ── No UDP detected — Hindi failure message ──
                await status_msg.edit(
                    f"⚠️ **IP Nahi Mila — `{target}`**\n\n"
                    + FAILURE_MSG
                )
                success = True
                await send_log(
                    f"⚠️ **No IP Found**\n"
                    f"👤 User: {user_tag}\n"
                    f"🎯 Chat ID: `{target}`\n"
                    f"🕐 Time: `{now}`"
                )

            try:
                await call.leave_call(target)
            except:
                pass
            break

        except FloodWait as e:
            capture_task.cancel()
            logger.warning(f"FloodWait on call.play: {e.value}s")
            await asyncio.sleep(e.value + 1)
            error_msg = f"FloodWait {e.value}s"
            continue
        except Exception as e:
            capture_task.cancel()
            error_msg = str(e)
            continue

    if temp_session:
        try:
            await user_app.stop()
        except:
            pass

    if not success:
        if any(code in error_msg for code in (
            "USER_NOT_PARTICIPANT",
            "CHAT_WRITE_FORBIDDEN",
            "CHANNEL_INVALID",
            "CHAT_ID_INVALID",
            "PEER_ID_INVALID",
        )):
            # ── Group/channel access error — Hindi failure message ──
            await status_msg.edit(
                f"❌ **Assistant Us Group Mein Nahi Hai!**\n\n"
                + FAILURE_MSG
            )
        else:
            await status_msg.edit(f"❌ Failed after retries: `{error_msg}`")

        await send_log(
            f"❌ **GetIP Failed**\n"
            f"👤 User: {user_tag}\n"
            f"🎯 Target: `{target}`\n"
            f"⚠️ Error: `{error_msg}`\n"
            f"🕐 Time: `{now}`"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    await bot.start()

    me = await bot.get_me()
    start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"Bot @{me.username} started at {start_time}")

    await send_log(
        f"🚀 **Bot Online**\n"
        f"🤖 Bot: @{me.username} (`{me.id}`)\n"
        f"🕐 Time: `{start_time}`\n"
        f"👑 Admin: `{ADMIN_ID}`"
    )

    await load_assistants()
    await idle()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())


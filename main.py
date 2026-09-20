import asyncio
import ipaddress
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

from cachetools import TTLCache
from dotenv import load_dotenv
from pyrogram import Client, enums, filters
from pyrogram.enums import ButtonStyle, MessageServiceType
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    KeyboardButtonRequestChat,
    Message,
    ReplyKeyboardMarkup,
)
from pymongo import MongoClient
from pytgcalls import PyTgCalls, idle
from pytgcalls.exceptions import NoActiveGroupCall
from pytgcalls.types import GroupCallConfig

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
LOG_GROUP_ID = int(os.environ.get("LOG_GROUP_ID", 0))
SESSION_STRING = os.environ.get("SESSION_STRING", "")
ALLOWED_GROUP = int(os.environ.get("ALLOWED_GROUP", 0))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

db = MongoClient(MONGO_URI)["getip_bot"]
users_db = db["users"]
assistants_db = db["assistants"]
invite_db = db["invites"]
rate_db = db["rates"]

OK_STATUS = (
    enums.ChatMemberStatus.MEMBER,
    enums.ChatMemberStatus.ADMINISTRATOR,
    enums.ChatMemberStatus.OWNER,
)

USER_BURST = (3, 60)
USER_HOURLY = (15, 3600)
CHAT_BURST = (10, 300)
GLOBAL_BURST = 60

SFU_NETS = [
    ipaddress.ip_network(x)
    for x in (
        "91.108.16.0/22",
        "91.108.20.0/22",
        "91.108.24.0/22",
        "149.154.160.0/20",
    )
]

SFU_REGION = {
    "91.108.16.0/22": "Amsterdam (EU)",
    "91.108.20.0/22": "Singapore (APAC)",
    "91.108.24.0/22": "Miami (US)",
    "149.154.160.0/20": "Telegram DC",
}

CACHE_TTL = 300
REQ_TIMEOUT = 30
CAPTURE_TIMEOUT = 4
WORKERS = 2
COOLDOWN = 60

bot = Client(
    "bot_wrapper",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

assistants = []
calls = {}

_sem = asyncio.Semaphore(1)
_inflight = {}
_cache = TTLCache(maxsize=500, ttl=CACHE_TTL)
_last_used = {}
_cooldown = {}
_global_hits = deque()
_queue = asyncio.Queue()
pending = {}

TG_RE = re.compile(r"IP (\d+\.\d+\.\d+\.\d+)\.(\d+) > (\d+\.\d+\.\d+\.\d+)\.(\d+)")
REDACT_RE = re.compile(r"[A-Za-z0-9_\-]{80,}")


def redact(s):
    return REDACT_RE.sub("[REDACTED]", str(s))


def now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def user_tag(m):
    u = m.from_user
    n = f"{u.first_name or ''} {u.last_name or ''}".strip() or "Unknown"
    return f"[{n}](tg://user?id={u.id}) (`{u.id}` | @{u.username or '—'})"


def log_async(text):
    if not LOG_GROUP_ID:
        return
    asyncio.create_task(_send_log(text))


async def _send_log(text):
    try:
        await bot.send_message(LOG_GROUP_ID, text, disable_web_page_preview=True)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        log.error(f"log: {e}")


async def floodwait(coro, n=3):
    for _ in range(n):
        try:
            return await coro
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
    raise RuntimeError("floodwait retries exhausted")


def bucket(key, limit, window):
    now = datetime.utcnow()
    start = now - timedelta(seconds=window)
    rec = rate_db.find_one({"_id": key})
    if not rec:
        rate_db.insert_one({"_id": key, "hits": [now]})
        return True, limit - 1
    hits = [h for h in rec.get("hits", []) if h > start]
    if len(hits) >= limit:
        oldest = min(hits)
        retry = int((oldest + timedelta(seconds=window) - now).total_seconds()) + 1
        return False, retry
    hits.append(now)
    rate_db.update_one({"_id": key}, {"$set": {"hits": hits}})
    return True, limit - len(hits)


def check_global():
    t = time.time()
    while _global_hits and t - _global_hits[0] > 60:
        _global_hits.popleft()
    if len(_global_hits) >= GLOBAL_BURST:
        return False
    _global_hits.append(t)
    return True


def rate_limited(func):
    @wraps(func)
    async def w(c, m, *a, **k):
        if not m.from_user or m.from_user.id == ADMIN_ID:
            return await func(c, m, *a, **k)
        ok, v = bucket(f"u:{m.from_user.id}", *USER_BURST)
        if not ok:
            return await m.reply(f"⏳ Rate limit. Try in **{v}s**.")
        ok, v = bucket(f"uh:{m.from_user.id}", *USER_HOURLY)
        if not ok:
            return await m.reply(f"⏳ Hourly limit. Try in **{v}s**.")
        return await func(c, m, *a, **k)

    return w


def user_has_quota(uid):
    u = users_db.find_one({"_id": uid})
    if not u:
        users_db.insert_one({"_id": uid, "limit": 5, "used": 0})
        return True
    return u["used"] < u["limit"]


def bump_usage(uid):
    users_db.update_one({"_id": uid}, {"$inc": {"used": 1}})


def user_stats(uid):
    u = users_db.find_one({"_id": uid})
    return (0, 5) if not u else (u.get("used", 0), u.get("limit", 5))


async def invite_link(cid):
    c = invite_db.find_one({"_id": cid})
    now = datetime.utcnow()
    if c and c.get("expire") and c["expire"] > now:
        return c["link"]
    exp = now + timedelta(days=7)
    obj = await floodwait(
        bot.create_chat_invite_link(cid, name="Force Sub", expire_date=exp)
    )
    invite_db.update_one(
        {"_id": cid},
        {"$set": {"link": obj.invite_link, "expire": exp}},
        upsert=True,
    )
    return obj.invite_link


async def force_sub(uid):
    if not ALLOWED_GROUP:
        return True, ""
    try:
        m = await floodwait(bot.get_chat_member(ALLOWED_GROUP, uid))
        if m.status in OK_STATUS:
            return True, ""
        return False, await invite_link(ALLOWED_GROUP)
    except UserNotParticipant:
        return False, await invite_link(ALLOWED_GROUP)
    except RPCError:
        return False, ""


async def load_assistants():
    if SESSION_STRING:
        a = Client(
            "assistant_env",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING,
        )
        await a.start()
        assistants.append(a)
        calls[a] = PyTgCalls(a)
        await calls[a].start()
    for ast in assistants_db.find():
        a = Client(
            f"assistant_{ast['_id']}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=ast["session_string"],
        )
        await a.start()
        assistants.append(a)
        calls[a] = PyTgCalls(a)
        await calls[a].start()
    log.info(f"loaded {len(assistants)} assistant(s)")


def pick_pool():
    now = time.time()
    ok = [a for a in assistants if a in calls and _cooldown.get(a, 0) < now]
    ok.sort(key=lambda a: _last_used.get(a, 0))
    return [(a, calls[a]) for a in ok]


async def health_loop():
    while True:
        for a in list(assistants):
            try:
                await a.get_me()
            except Exception:
                assistants.remove(a)
                calls.pop(a, None)
                try:
                    await a.stop()
                except Exception:
                    pass
        await asyncio.sleep(300)


def is_sfu(ip):
    try:
        a = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(a in n for n in SFU_NETS)


def region(ip):
    try:
        a = ipaddress.ip_address(ip)
    except ValueError:
        return "Unknown"
    for cidr, name in SFU_REGION.items():
        if a in ipaddress.ip_network(cidr):
            return name
    return "Unknown"


async def capture_ip(timeout=CAPTURE_TIMEOUT):
    async with _sem:
        p = await asyncio.create_subprocess_exec(
            "tcpdump",
            "-i",
            "any",
            "-n",
            "-q",
            "-l",
            "-c",
            "20",
            "-Q",
            "out",
            "-s",
            "96",
            "udp and not dst net 10.0.0.0/8 and not dst net 172.16.0.0/12 "
            "and not dst net 192.168.0.0/16 and not dst net 127.0.0.0/8",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:

            async def reader():
                async for raw in p.stdout:
                    m = TG_RE.search(raw.decode())
                    if m and is_sfu(m.group(3)):
                        return m.group(3), m.group(4)
                return None, None

            return await asyncio.wait_for(reader(), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return None, None
        finally:
            try:
                p.kill()
            except Exception:
                pass


async def do_capture(chat_id, pool):
    err = None
    for app, call in pool:
        try:
            task = asyncio.create_task(capture_ip())
            try:
                await call.play(chat_id, config=GroupCallConfig(auto_start=False))
            except NoActiveGroupCall:
                task.cancel()
                return None, None, None
            ip, port = await task
            try:
                await call.leave_call(chat_id)
            except Exception:
                pass
            _last_used[app] = time.time()
            return (ip, port, region(ip)) if ip else (None, None, None)
        except FloodWait as e:
            _cooldown[app] = time.time() + min(e.value, 120)
            err = f"FloodWait {e.value}s"
            await asyncio.sleep(min(e.value + 1, 10))
        except Exception as e:
            err = redact(str(e))
    raise RuntimeError(err or "all assistants failed")


async def capture_chat(chat_id):
    if chat_id in _inflight:
        return await _inflight[chat_id]
    fut = _loop.create_future()
    _inflight[chat_id] = fut
    try:
        pool = pick_pool()
        if not pool:
            raise RuntimeError("all assistants in cooldown")
        res = await do_capture(chat_id, pool)
        if not fut.done():
            fut.set_result(res)
        return res
    except Exception as e:
        if not fut.done():
            fut.set_exception(e)
        raise
    finally:
        _inflight.pop(chat_id, None)


async def worker():
    while True:
        chat_id, fut = await _queue.get()
        try:
            cached = _cache.get(chat_id)
            if cached:
                if not fut.done():
                    fut.set_result(cached)
                continue
            res = await capture_chat(chat_id)
            if res[0]:
                _cache[chat_id] = res
            if not fut.done():
                fut.set_result(res)
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
        finally:
            _queue.task_done()


async def enqueue(chat_id):
    cached = _cache.get(chat_id)
    if cached:
        return cached
    fut = _loop.create_future()
    await _queue.put((chat_id, fut))
    return await asyncio.wait_for(fut, timeout=REQ_TIMEOUT)


def start_workers():
    for _ in range(WORKERS):
        _loop.create_task(worker())


FAILURE = (
    "🔴 **Assistant us group mein nahi hai.**\n\n"
    "├ `/getip invite_link` — join & retry\n"
    "├ `/getip chat_id` — agar already member hai\n"
    "└ `/getip chat_id session` — apna session use karein\n\n"
    "📌 Session: @ArchStringBot se lein.\n🔒 Store nahi karte."
)


@bot.on_message(filters.command("start") & ~filters.private)
async def start_group(c, m):
    me = await c.get_me()
    await m.reply(f"👋 DM me: @{me.username}")


@bot.on_message(filters.command("start") & filters.private)
async def start_dm(c, m):
    if not m.from_user:
        return
    used, lim = user_stats(m.from_user.id)
    kb = ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    "📤 Share Chat",
                    request_chat=KeyboardButtonRequestChat(
                        button_id=1,
                        chat_is_channel=False,
                        chat_is_forum=None,
                        request_title=True,
                        request_username=True,
                        request_photo=True,
                    ),
                    style=ButtonStyle.PRIMARY,
                )
            ],
            [KeyboardButton("❓ Help", style=ButtonStyle.SUCCESS)],
        ],
        resize_keyboard=True,
        placeholder="Tap Share Chat…",
    )
    await m.reply(
        f"👋 **{m.from_user.first_name}**\n\n"
        f"used `{used}` / limit `{lim}` / left `{max(0, lim - used)}`\n\n"
        f"Tap **📤 Share Chat** to pick a group.",
        reply_markup=kb,
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@bot.on_message(filters.command("help") & filters.private)
async def help_dm(c, m):
    await m.reply(
        "**Commands**\n\n"
        "`/start` — share a group\n"
        "`/getip <chat_id>` — direct capture\n"
        "`/getip <invite_link>` — join & capture\n"
        "`/getip <chat_id> <session>` — use your session",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@bot.on_message(filters.command("approve") & filters.user(ADMIN_ID))
async def approve(c, m):
    p = m.text.split()
    if len(p) < 3:
        return await m.reply("Usage: /approve <uid> <limit>")
    users_db.update_one(
        {"_id": int(p[1])}, {"$set": {"limit": int(p[2])}}, upsert=True
    )
    await m.reply(f"User `{p[1]}` → `{p[2]}`")


@bot.on_message(filters.private & filters.service)
async def on_share(c, m):
    if m.service_type != MessageServiceType.CHAT_SHARED or not m.chat_shared:
        return
    if m.chat_shared.button_id != 1:
        return
    cs = m.chat_shared
    if not cs.chat:
        return await m.reply("⚠️ Could not read shared chat.")
    cid = cs.chat.id
    if not str(cid).startswith("-100"):
        cid = int(f"-100{abs(cid)}")
    title = cs.chat.title or "Unknown"

    log_async(f"📤 **Shared** {user_tag(m)} → `{cid}` ({title})")

    ok, link = await force_sub(m.from_user.id)
    if not ok:
        return await m.reply(
            f"Join first.\n👉 [Join Here]({link})",
            disable_web_page_preview=True,
        )
    if not user_has_quota(m.from_user.id):
        return await m.reply("Usage limit reached.")
    if not check_global():
        return await m.reply("🚦 Bot busy. Try in a minute.")

    pending[m.from_user.id] = {"chat_id": cid, "stage": None, "title": title}

    st = await m.reply(f"⏳ Checking `{title}`…")
    try:
        ip, port, reg = await enqueue(cid)
    except asyncio.TimeoutError:
        pending.pop(m.from_user.id, None)
        return await st.edit("⏱️ Timed out.")
    except NoActiveGroupCall:
        pending.pop(m.from_user.id, None)
        return await st.edit(f"⚠️ No active call in `{title}`.")
    except RuntimeError as e:
        return await _share_fallback(st, m, cid, title, str(e))

    if not ip:
        pending.pop(m.from_user.id, None)
        return await st.edit(f"⚠️ No active call in `{title}`.")

    bump_usage(m.from_user.id)
    pending.pop(m.from_user.id, None)
    await st.edit(
        f"✅ **Result**\n"
        f"**Chat:** `{title}`\n"
        f"**Chat ID:** `{cid}`\n"
        f"**IP:** `{ip}`\n"
        f"**Port:** `{port}`\n"
        f"**Region:** `{reg}`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


async def _share_fallback(st, m, cid, title, err):
    if any(
        x in err
        for x in (
            "USER_NOT_PARTICIPANT",
            "CHAT_WRITE_FORBIDDEN",
            "CHANNEL_INVALID",
            "CHAT_ID_INVALID",
            "PEER_ID_INVALID",
        )
    ):
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔗 Send Invite Link",
                        callback_data=f"inv:{m.from_user.id}",
                        style=ButtonStyle.PRIMARY,
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🔑 Use My Session",
                        callback_data=f"ses:{m.from_user.id}",
                        style=ButtonStyle.DANGER,
                    )
                ],
                [
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data=f"can:{m.from_user.id}",
                    )
                ],
            ]
        )
        return await st.edit(
            f"❌ **Assistant not in `{title}`**\n\nChoose:",
            reply_markup=kb,
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    return await st.edit(f"❌ Failed: `{redact(err)}`")


@bot.on_callback_query(filters.regex(r"^(inv|ses|can):(\d+)$") & filters.private)
async def cb(c, q):
    action, uid = q.data.split(":")
    uid = int(uid)
    if q.from_user.id != uid:
        return await q.answer("Not your button.", show_alert=True)
    st = pending.get(uid)
    if not st:
        return await q.answer("Session expired. Share again.", show_alert=True)
    if action == "can":
        pending.pop(uid, None)
        await q.message.edit("❌ Cancelled.")
        return await q.answer()
    if action == "inv":
        st["stage"] = "invite"
        await q.message.edit("🔗 Send invite link (`https://t.me/+…`).")
    else:
        st["stage"] = "session"
        await q.message.edit("🔑 Send Pyrogram session string.\n⚠️ Not stored.")
    return await q.answer()


@bot.on_message(
    filters.private
    & filters.text
    & ~filters.command(["start", "getip", "approve", "help"])
)
async def fallback_text(c, m):
    if not m.from_user:
        return
    st = pending.get(m.from_user.id)
    if not st or not st.get("stage"):
        return
    cid, title, stage = st["chat_id"], st.get("title", ""), st.pop("stage")

    if stage == "invite":
        link = m.text.strip()
        if "t.me/" not in link:
            st["stage"] = "invite"
            return await m.reply("❌ Invalid link.")
        stt = await m.reply("⏳ Joining…")
        app = assistants[0] if assistants else None
        if not app:
            pending.pop(m.from_user.id, None)
            return await stt.edit("No assistants online.")
        try:
            await app.join_chat(link)
        except Exception as e:
            st["stage"] = "invite"
            return await stt.edit(f"❌ Join failed: `{redact(str(e))}`")
        try:
            ip, port, reg = await enqueue(cid)
        except Exception as e:
            pending.pop(m.from_user.id, None)
            return await stt.edit(f"❌ {redact(str(e))}")
        pending.pop(m.from_user.id, None)
        if not ip:
            return await stt.edit("⚠️ Joined but no active call.")
        bump_usage(m.from_user.id)
        return await stt.edit(
            f"✅ **Result**\n**Chat:** `{title}`\n**Chat ID:** `{cid}`\n"
            f"**IP:** `{ip}`\n**Port:** `{port}`\n**Region:** `{reg}`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )

    if stage == "session":
        sess = m.text.strip()
        if len(sess) < 100:
            st["stage"] = "session"
            return await m.reply("❌ Invalid session string.")
        stt = await m.reply("⏳ Using session…")
        ua = uc = None
        try:
            ua = Client(
                f"temp_{m.from_user.id}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=sess,
            )
            await ua.start()
            uc = PyTgCalls(ua)
            await uc.start()
            task = asyncio.create_task(capture_ip())
            await uc.play(cid, config=GroupCallConfig(auto_start=False))
            ip, port = await task
            try:
                await uc.leave_call(cid)
            except Exception:
                pass
            if ip:
                bump_usage(m.from_user.id)
                await stt.edit(
                    f"✅ **Result**\n**Chat:** `{title}`\n**Chat ID:** `{cid}`\n"
                    f"**IP:** `{ip}`\n**Port:** `{port}`\n**Region:** `{region(ip)}`",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )
            else:
                await stt.edit("⚠️ No active call.")
        except Exception as e:
            await stt.edit(f"❌ {redact(str(e))}")
        finally:
            try:
                if uc:
                    await uc.leave_call(cid)
            except Exception:
                pass
            try:
                if ua:
                    await ua.stop()
            except Exception:
                pass
            for f in Path(".").glob(f"temp_{m.from_user.id}*.session*"):
                try:
                    f.unlink()
                except Exception:
                    pass
            pending.pop(m.from_user.id, None)


@bot.on_message(filters.command("getip"))
@rate_limited
async def getip_cmd(c, m):
    if not m.from_user:
        return
    parts = m.text.split()
    if len(parts) < 2:
        return await m.reply("Usage: `/getip <chat_id_or_link> [session]`")
    if not check_global():
        return await m.reply("🚦 Bot busy. Try in a minute.")
    ok, link = await force_sub(m.from_user.id)
    if not ok:
        return await m.reply(
            f"Join first.\n👉 [Join Here]({link})",
            disable_web_page_preview=True,
        )
    if not user_has_quota(m.from_user.id):
        return await m.reply("Usage limit reached.")

    target = parts[1]
    sess = parts[2] if len(parts) > 2 else None

    if "t.me/" in target:
        pool = pick_pool()
        if not pool:
            return await m.reply("No assistants online.")
        for app, _ in pool:
            try:
                await app.join_chat(target)
                target = (await app.get_chat(target)).id
                break
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except Exception:
                continue
        else:
            return await m.reply("Could not resolve link.")
    else:
        try:
            target = int(target)
        except ValueError:
            return await m.reply("Invalid chat id.")

    ok, v = bucket(f"c:{target}", *CHAT_BURST)
    if not ok:
        return await m.reply(f"⏳ Chat rate limit. Try in **{v}s**.")

    if sess:
        st = await m.reply("⏳ Using session…")
        ua = uc = None
        try:
            ua = Client(
                f"temp_{m.from_user.id}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=sess,
            )
            await ua.start()
            uc = PyTgCalls(ua)
            await uc.start()
            task = asyncio.create_task(capture_ip())
            await uc.play(target, config=GroupCallConfig(auto_start=False))
            ip, port = await task
            try:
                await uc.leave_call(target)
            except Exception:
                pass
            if not ip:
                return await st.edit("⚠️ No active call.")
            bump_usage(m.from_user.id)
            return await st.edit(
                f"✅ **Result**\n**Chat ID:** `{target}`\n**IP:** `{ip}`\n"
                f"**Port:** `{port}`\n**Region:** `{region(ip)}`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
        except Exception as e:
            return await st.edit(f"❌ {redact(str(e))}")
        finally:
            try:
                if uc:
                    await uc.leave_call(target)
            except Exception:
                pass
            try:
                if ua:
                    await ua.stop()
            except Exception:
                pass
            for f in Path(".").glob(f"temp_{m.from_user.id}*.session*"):
                try:
                    f.unlink()
                except Exception:
                    pass

    st = await m.reply("⏳ Capturing…")
    try:
        ip, port, reg = await enqueue(target)
    except asyncio.TimeoutError:
        return await st.edit("⏱️ Timed out.")
    except NoActiveGroupCall:
        return await st.edit("⚠️ No active call.")
    except RuntimeError as e:
        err = str(e)
        if any(
            x in err
            for x in (
                "USER_NOT_PARTICIPANT",
                "CHAT_WRITE_FORBIDDEN",
                "CHANNEL_INVALID",
                "CHAT_ID_INVALID",
                "PEER_ID_INVALID",
            )
        ):
            return await st.edit(f"❌ Assistant not in group.\n\n{FAILURE}")
        return await st.edit(f"❌ {redact(err)}")

    if not ip:
        return await st.edit("⚠️ No active call.")
    bump_usage(m.from_user.id)
    await st.edit(
        f"✅ **Result**\n**Chat ID:** `{target}`\n**IP:** `{ip}`\n"
        f"**Port:** `{port}`\n**Region:** `{reg}`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


async def main():
    await bot.start()
    me = await bot.get_me()
    log.info(f"bot @{me.username} up")
    log_async(f"🚀 **Bot Online** @{me.username}\n🕐 `{now_str()}`")
    await load_assistants()
    start_workers()
    _loop.create_task(health_loop())
    await idle()


if __name__ == "__main__":
    try:
        _loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        _loop.close()

import asyncio
import os
import re
from pyrogram import Client, filters
from pyrogram.errors import RPCError, UserNotParticipant
from pyrogram.types import Message
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import GroupCallConfig, MediaStream
from pymongo import MongoClient

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))

ALLOWED_GROUP = -1001952511944
JOIN_REQUIRED_MSG = "You must join the allowed group before using /getip."
ALLOWED_MEMBER_STATUSES = ("member", "administrator", "creator")

db_client = MongoClient(MONGO_URI)
db = db_client["getip_bot"]
users_db = db["users"]
assistants_db = db["assistants"]

bot = Client("bot_wrapper", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

assistants = []
calls = {}

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
        app = Client(f"assistant_{ast['_id']}", api_id=API_ID, api_hash=API_HASH, session_string=ast["session_string"])
        await app.start()
        assistants.append(app)
        calls[app] = PyTgCalls(app)
        await calls[app].start()

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
                for m in pattern.finditer(line):
                    ip, port = m.group(1), m.group(2)
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

def check_user_limit(user_id: int):
    user = users_db.find_one({"_id": user_id})
    if not user:
        user = {"_id": user_id, "limit": 5, "used": 0}
        users_db.insert_one(user)
    if user["used"] >= user["limit"]:
        return False
    return True

def increment_user_usage(user_id: int):
    users_db.update_one({"_id": user_id}, {"$inc": {"used": 1}})

def auth_filter(_, __, m: Message):
    return m.chat.type == "private" or m.chat.id == ALLOWED_GROUP

@bot.on_message(filters.command("approve") & filters.user(ADMIN_ID))
async def approve_user(c: Client, m: Message):
    parts = m.text.split()
    if len(parts) < 3:
        return await m.reply("Usage: /approve {user_id} {limit}")
    
    user_id = int(parts[1])
    limit = int(parts[2])
    
    users_db.update_one(
        {"_id": user_id},
        {"$set": {"limit": limit}},
        upsert=True
    )
    await m.reply(f"User {user_id} limit updated to {limit}.")

@bot.on_message(filters.command("getip") & filters.create(auth_filter))
async def getip_command(c: Client, m: Message):
    if not m.from_user:
        return await m.reply("Unable to verify your account for this command.")

    try:
        member = await bot.get_chat_member(ALLOWED_GROUP, m.from_user.id)
        if member.status not in ALLOWED_MEMBER_STATUSES:
            return await m.reply(JOIN_REQUIRED_MSG)
    except UserNotParticipant:
        return await m.reply(JOIN_REQUIRED_MSG)
    except RPCError:
        return await m.reply("Unable to verify your group membership right now.")

    parts = m.text.split()
    if len(parts) < 2:
        return await m.reply("Usage: /getip <chat_id_or_invite_link> [optional_session_string]")
    
    target = parts[1]
    temp_session = parts[2] if len(parts) > 2 else None

    if not check_user_limit(m.from_user.id):
        return await m.reply("You have exceeded your usage limit. Contact Admin.")

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
    status_msg = await m.reply("Attempting to get IP...")

    if "t.me/" in target or "telegram.me/" in target:
        for app, _ in available_assistants:
            try:
                await app.join_chat(target)
                target_chat = await app.get_chat(target)
                target = target_chat.id
                break
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
                config=GroupCallConfig(auto_start=True)
            )
            
            ip, port = await capture_task
            if ip and port:
                await status_msg.edit(f"**Chat ID:** `{target}`\n**IP:** `{ip}`\n**Port:** `{port}`")
                increment_user_usage(m.from_user.id)
                success = True
            else:
                await status_msg.edit(f"{target} no connection found.")
                success = True
                
            try:
                await call.leave_call(target)
            except:
                pass
            break
            
        except Exception as e:
            capture_task.cancel()
            error_msg = str(e)
            continue

    if temp_session:
        await user_app.stop()
        
    if not success:
        if "USER_NOT_PARTICIPANT" in error_msg or "CHAT_WRITE_FORBIDDEN" in error_msg:
             await status_msg.edit("Assistant is not in this private chat. Please provide an invite link:\n`/getip <invite_link>` or provide your own session file string:\n`/getip <chat_id> <session_string>`")
        else:
             await status_msg.edit(f"Failed to join after retries: {error_msg}")

async def main():
    await bot.start()
    await load_assistants()
    print("Bot Wrapper & Assistants are running...")
    await idle()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

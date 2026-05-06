import asyncio
import os
import subprocess
import re

from pyrogram import Client, filters
from pyrogram.types import Message
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import GroupCallConfig, MediaStream

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]

ALLOWED_USER = 5218610039

app = Client(
    "py-tgcalls",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)
call_py = PyTgCalls(app)


async def capture_udp_ip(timeout=15):
    proc = await asyncio.create_subprocess_exec(
        "tcpdump", "-i", "any", "udp", "-n", "-q", "-l",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    pattern = re.compile(r'IP (\d+\.\d+\.\d+\.\d+)\.(\d+) > ')
    private = ("10.", "172.", "192.168.", "127.")
    try:
        async def read():
            async for line in proc.stdout:
                line = line.decode()
                m = pattern.search(line)
                if m:
                    ip, port = m.group(1), m.group(2)
                    if not any(ip.startswith(p) for p in private):
                        return ip, port
            return None, None

        ip, port = await asyncio.wait_for(read(), timeout=timeout)
        return ip, port
    except asyncio.TimeoutError:
        return None, None
    finally:
        try:
            proc.kill()
        except Exception:
            pass


@app.on_message(filters.command("getip") & filters.user(ALLOWED_USER))
async def getip_handler(_: Client, message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("Usage: /getip <chat_id>")
    try:
        chat_id = int(parts[1])
    except ValueError:
        return await message.reply("Invalid chat ID.")

    try:
        await call_py.play(
            chat_id,
            MediaStream("/dev/zero"),
            config=GroupCallConfig(auto_start=True)
        )
    except Exception as e:
        return await message.reply(f"Failed to join: {e}")

    ip, port = await capture_udp_ip(timeout=15)
    if ip and port:
        await message.reply(f"{chat_id} {ip} {port}")
    else:
        await message.reply(f"{chat_id} no connection")

    try:
        await call_py.leave_call(chat_id)
    except Exception:
        pass


async def main():
    await app.start()
    await call_py.start()
    await idle()


asyncio.get_event_loop().run_until_complete(main())

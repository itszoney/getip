import asyncio
import os

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

PRIVATE_HEX = ("00000000", "7F", "0A", "AC1", "C0A8")


def get_udp_remote():
    for path in ("/proc/net/udp", "/proc/net/udp6"):
        try:
            with open(path) as f:
                for line in f:
                    if line.strip().startswith("sl"):
                        continue
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    remote = parts[2]
                    if ":" not in remote:
                        continue
                    ip_hex, port_hex = remote.split(":")
                    if ip_hex == "00000000":
                        continue
                    if any(ip_hex.startswith(p) for p in PRIVATE_HEX):
                        continue
                    if len(ip_hex) == 8:
                        ip = ".".join(str(int(ip_hex[i:i+2], 16)) for i in (6, 4, 2, 0))
                        port = int(port_hex, 16)
                        if port == 0:
                            continue
                        return ip, port
        except FileNotFoundError:
            continue
    return None, None


async def poll_udp(timeout=20, interval=0.5):
    for _ in range(int(timeout / interval)):
        ip, port = get_udp_remote()
        if ip:
            return ip, port
        await asyncio.sleep(interval)
    return None, None


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

    ip, port = await poll_udp(timeout=20, interval=0.5)
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

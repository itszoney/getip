import asyncio
import os

from pyrogram import Client, filters
from pyrogram.types import Message
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import GroupCallConfig

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]

app = Client(
    "py-tgcalls",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)
call_py = PyTgCalls(app)

PRIVATE_HEX = ("00000000", "7F", "0A", "AC1", "C0A8")
_connected = {}  # chat_id -> asyncio.Event


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


@call_py.on_update()
async def on_update(_, update):
    try:
        from ntgcalls import ConnectionState, CONNECTED
        if hasattr(update, 'chat_id') and hasattr(update, 'state'):
            if update.state == CONNECTED:
                if update.chat_id in _connected:
                    _connected[update.chat_id].set()
    except Exception:
        pass


@app.on_message(filters.command("getip"))
async def getip_handler(_: Client, message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("Usage: /getip <chat_id>")
    try:
        chat_id = int(parts[1])
    except ValueError:
        return await message.reply("Invalid chat ID.")

    event = asyncio.Event()
    _connected[chat_id] = event

    try:
        await call_py.play(
            chat_id,
            stream=None,
            config=GroupCallConfig(auto_start=True)
        )
    except Exception as e:
        _connected.pop(chat_id, None)
        return await message.reply(f"Failed to join: {e}")

    # wait for connected event or timeout
    try:
        await asyncio.wait_for(event.wait(), timeout=15)
        await message.reply("connected event fired, reading UDP...")
    except asyncio.TimeoutError:
        await message.reply("no connected event, reading UDP anyway...")

    _connected.pop(chat_id, None)

    ip, port = await poll_udp(timeout=10, interval=0.5)
    if ip and port:
        await message.reply(f"{chat_id} {ip} {port}")
    else:
        # dump raw for debug
        raw = ""
        for path in ("/proc/net/udp", "/proc/net/udp6"):
            try:
                with open(path) as f:
                    raw += f"=={path}==\n{f.read()}"
            except FileNotFoundError:
                pass
        await message.reply(f"no connection\n```\n{raw[:2000]}\n```")

    try:
        await call_py.leave_call(chat_id)
    except Exception:
        pass


async def main():
    await app.start()
    await call_py.start()
    await idle()


asyncio.get_event_loop().run_until_complete(main())

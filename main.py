from pyrogram import Client
from pyrogram import filters
from pyrogram.types import Message

from pytgcalls import filters as fl
from pytgcalls import idle
from pytgcalls import PyTgCalls
from pytgcalls.types import ChatUpdate
from pytgcalls.types import GroupCallParticipant
from pytgcalls.types import StreamEnded
from pytgcalls.types import Update
from pytgcalls.types import UpdatedGroupCallParticipant
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


def _from_proc():
    pid = os.getpid()
    try:
        with open(f"/proc/{pid}/net/udp", "r") as f:
            for line in f:
                if line.startswith("  sl"):
                    continue
                parts = line.split()
                remote = parts[2]
                ip_hex, port_hex = remote.split(":")
                if ip_hex[:2] == "7F":
                    continue
                ip = ".".join(str(int(ip_hex[i:i+2], 16)) for i in (0, 2, 4, 6))
                port = int(port_hex, 16)
                return ip, port
    except FileNotFoundError:
        pass
    return None, None


def _from_psutil():
    try:
        import psutil
    except ImportError:
        return None, None
    pid = os.getpid()
    try:
        proc = psutil.Process(pid)
        for conn in proc.connections(kind="udp"):
            if conn.raddr and not conn.raddr.ip.startswith(("127.", "0.", "192.168.", "10.", "172.")):
                return conn.raddr.ip, conn.raddr.port
    except Exception:
        pass
    return None, None


def _from_ss():
    pid = os.getpid()
    try:
        out = subprocess.check_output(
            ["ss", "-tunp"],
            stderr=subprocess.DEVNULL, text=True
        )
        for line in out.splitlines():
            if f"pid={pid}" in line:
                parts = line.split()
                if len(parts) >= 5:
                    peer = parts[4]
                    if peer == "*:*" or peer.startswith("127."):
                        continue
                    ip, port = peer.rsplit(":", 1)
                    try:
                        return ip, int(port)
                    except ValueError:
                        continue
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    return None, None


def _from_lsof():
    pid = os.getpid()
    try:
        out = subprocess.check_output(
            ["lsof", "-i", "UDP", "-a", "-p", str(pid), "-n", "-P"],
            stderr=subprocess.DEVNULL, text=True
        )
        for line in out.splitlines():
            if "UDP" in line and "->" in line:
                arrow_idx = line.index("->")
                remote = line[arrow_idx+2:].strip()
                ip, port = remote.rsplit(":", 1)
                if ip.startswith("127."):
                    continue
                try:
                    return ip, int(port)
                except ValueError:
                    continue
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    return None, None


def get_voice_remote_addr():
    methods = [
        _from_proc,
        _from_psutil,
        _from_ss,
        _from_lsof,
    ]
    for method in methods:
        ip, port = method()
        if ip:
            return ip, port
    return None, None


@app.on_message(filters.command("getip"))
async def getip_handler(_: Client, message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("Usage: /getip <chat_id>")
    try:
        chat_id = int(parts[1])
    except ValueError:
        return await message.reply("Invalid chat ID.")

    await call_py.join_group_call(chat_id)
    await asyncio.sleep(3)
    ip, port = get_voice_remote_addr()
    if ip and port:
        await message.reply(f"{chat_id} {ip} {port}")
    else:
        await message.reply(f"{chat_id} no connection")
    await call_py.leave_call(chat_id)


call_py.start()
idle()

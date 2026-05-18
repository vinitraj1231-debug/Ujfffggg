import os
import re
import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional, Deque, Dict, List

from telethon import TelegramClient, events
from pytgcalls import PyTgCalls, idle
from yt_dlp import YoutubeDL

# =========================
# CONFIG
# =========================
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ASSISTANT_SESSION = os.getenv("ASSISTANT_SESSION", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

BOT_NAME = os.getenv("BOT_NAME", "Elite VC Music Bot")

if not API_ID or not API_HASH or not BOT_TOKEN or not ASSISTANT_SESSION:
    raise SystemExit("Missing env vars: API_ID, API_HASH, BOT_TOKEN, ASSISTANT_SESSION")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("musicbot")

# =========================
# CLIENTS
# =========================
bot = TelegramClient("bot_session", API_ID, API_HASH)
assistant = TelegramClient("assistant_session", API_ID, API_HASH)
call = PyTgCalls(assistant)

# =========================
# DATA MODELS
# =========================
@dataclass
class Track:
    title: str
    webpage_url: str
    stream_url: str
    duration: Optional[int] = None
    requester: Optional[str] = None

queue_map: Dict[int, Deque[Track]] = defaultdict(deque)
current_map: Dict[int, Optional[Track]] = defaultdict(lambda: None)
loop_map: Dict[int, bool] = defaultdict(bool)
play_lock = asyncio.Lock()


# =========================
# HELPERS
# =========================
def _is_url(text: str) -> bool:
    return bool(re.match(r"^https?://", text.strip(), re.I))


def _human_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "Live/Unknown"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def ytdlp_opts(search: bool = False):
    base = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "nocheckcertificate": True,
        "ignoreerrors": True,
        "retries": 10,
        "geo_bypass": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios", "tv_embedded"],
            }
        },
    }
    if search:
        base["default_search"] = "ytsearch1"
    return base


def extract_track(query: str, requester: str = "unknown") -> Track:
    """
    Best-effort cookie-free extraction using yt-dlp + YouTube client fallback.
    """
    with YoutubeDL(ytdlp_opts(search=not _is_url(query))) as ydl:
        info = ydl.extract_info(query, download=False)

        # ytsearch1 returns entries
        if "entries" in info:
            info = next((e for e in info["entries"] if e), None)
            if not info:
                raise RuntimeError("No result found")

        stream_url = info.get("url")
        if not stream_url:
            raise RuntimeError("Could not resolve stream URL")

        title = info.get("title") or "Unknown title"
        webpage_url = info.get("webpage_url") or query
        duration = info.get("duration")

        return Track(
            title=title,
            webpage_url=webpage_url,
            stream_url=stream_url,
            duration=duration,
            requester=requester,
        )


async def ensure_playback(chat_id: int) -> bool:
    """
    Starts the next track if queue has items.
    """
    async with play_lock:
        if current_map[chat_id] is not None:
            return True

        if not queue_map[chat_id]:
            return False

        nxt = queue_map[chat_id].popleft()
        current_map[chat_id] = nxt
        call.play(chat_id, nxt.stream_url)
        return True


async def next_track(chat_id: int):
    """
    Skip current and move to next.
    """
    async with play_lock:
        if loop_map[chat_id] and current_map[chat_id] is not None:
            queue_map[chat_id].appendleft(current_map[chat_id])

        current_map[chat_id] = None

        if queue_map[chat_id]:
            nxt = queue_map[chat_id].popleft()
            current_map[chat_id] = nxt
            call.play(chat_id, nxt.stream_url)
            return nxt

        return None


async def maybe_call(method_name: str, *args):
    """
    Safe wrapper for PyTgCalls methods that may vary by version.
    """
    fn = getattr(call, method_name, None)
    if fn is None:
        return False
    res = fn(*args)
    if asyncio.iscoroutine(res):
        return await res
    return res


def panel_text(chat_id: int) -> str:
    cur = current_map[chat_id]
    qlen = len(queue_map[chat_id])
    loop_state = "ON" if loop_map[chat_id] else "OFF"

    if not cur:
        return (
            f"**{BOT_NAME}**\n\n"
            f"Status: `Idle`\n"
            f"Queue: `{qlen}`\n"
            f"Loop: `{loop_state}`\n"
        )

    return (
        f"**{BOT_NAME}**\n\n"
        f"Now Playing: [{cur.title}]({cur.webpage_url})\n"
        f"Duration: `{_human_duration(cur.duration)}`\n"
        f"Requested by: `{cur.requester}`\n"
        f"Queue: `{qlen}`\n"
        f"Loop: `{loop_state}`\n"
    )


# =========================
# BOT COMMANDS
# =========================
@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start_cmd(event):
    await event.reply(
        f"**{BOT_NAME}** is alive.\n\n"
        "Commands:\n"
        "`/play <song or url>`\n"
        "`/skip`\n"
        "`/pause`\n"
        "`/resume`\n"
        "`/stop`\n"
        "`/queue`\n"
        "`/loop on|off`\n"
    )


@bot.on(events.NewMessage(pattern=r"^/play(?:\s+(.+))?$"))
async def play_cmd(event):
    chat_id = event.chat_id
    query = event.pattern_match.group(1)

    if not query:
        return await event.reply("Usage: `/play <song name or url>`", parse_mode="md")

    requester = event.sender.username or event.sender.first_name or "user"

    msg = await event.reply("Searching and extracting stream...")

    try:
        track = await asyncio.to_thread(extract_track, query, requester)
    except Exception as e:
        return await msg.edit(f"Extraction failed: `{e}`", parse_mode="md")

    queue_map[chat_id].append(track)

    if current_map[chat_id] is None:
        try:
            await ensure_playback(chat_id)
            await msg.edit(
                f"Started playing: **{track.title}**\n"
                f"Duration: `{_human_duration(track.duration)}`"
            )
        except Exception as e:
            queue_map[chat_id].pop()
            current_map[chat_id] = None
            await msg.edit(f"Playback start failed: `{e}`", parse_mode="md")
    else:
        await msg.edit(
            f"Queued: **{track.title}**\n"
            f"Position: `{len(queue_map[chat_id])}`"
        )


@bot.on(events.NewMessage(pattern=r"^/skip$"))
async def skip_cmd(event):
    chat_id = event.chat_id
    if current_map[chat_id] is None and not queue_map[chat_id]:
        return await event.reply("Nothing to skip.")

    try:
        await maybe_call("stop", chat_id)
    except Exception:
        pass

    nxt = await next_track(chat_id)
    if nxt:
        await event.reply(f"Skipped. Now playing: **{nxt.title}**")
    else:
        await event.reply("Skipped. Queue is empty.")


@bot.on(events.NewMessage(pattern=r"^/pause$"))
async def pause_cmd(event):
    chat_id = event.chat_id
    ok = await maybe_call("pause", chat_id)
    if ok:
        await event.reply("Paused.")
    else:
        await event.reply("Pause method not available in this version of PyTgCalls.")


@bot.on(events.NewMessage(pattern=r"^/resume$"))
async def resume_cmd(event):
    chat_id = event.chat_id
    ok = await maybe_call("resume", chat_id)
    if ok:
        await event.reply("Resumed.")
    else:
        await event.reply("Resume method not available in this version of PyTgCalls.")


@bot.on(events.NewMessage(pattern=r"^/stop$"))
async def stop_cmd(event):
    chat_id = event.chat_id
    queue_map[chat_id].clear()
    current_map[chat_id] = None
    try:
        await maybe_call("stop", chat_id)
    except Exception:
        pass
    await event.reply("Stopped and queue cleared.")


@bot.on(events.NewMessage(pattern=r"^/queue$"))
async def queue_cmd(event):
    chat_id = event.chat_id
    q = queue_map[chat_id]
    cur = current_map[chat_id]

    if not cur and not q:
        return await event.reply("Queue is empty.")

    text = [panel_text(chat_id)]
    if q:
        text.append("\n**Up Next:**")
        for i, t in enumerate(list(q)[:10], start=1):
            text.append(f"`{i}.` {t.title} — `{_human_duration(t.duration)}`")
    await event.reply("\n".join(text), parse_mode="md")


@bot.on(events.NewMessage(pattern=r"^/loop(?:\s+(on|off))?$"))
async def loop_cmd(event):
    chat_id = event.chat_id
    state = (event.pattern_match.group(1) or "").lower()

    if state == "on":
        loop_map[chat_id] = True
    elif state == "off":
        loop_map[chat_id] = False
    else:
        loop_map[chat_id] = not loop_map[chat_id]

    await event.reply(f"Loop is now `{ 'ON' if loop_map[chat_id] else 'OFF' }`.", parse_mode="md")


@bot.on(events.NewMessage(pattern=r"^/panel$"))
async def panel_cmd(event):
    await event.reply(panel_text(event.chat_id), parse_mode="md")


# =========================
# AUTO-ADVANCE HOOK
# =========================
async def playback_watcher():
    """
    Lightweight fallback watcher:
    if current track ends and queue has items, move to the next one.
    This keeps the bot usable even if your installed PyTgCalls version
    doesn't expose a callback hook here.
    """
    while True:
        await asyncio.sleep(5)
        for chat_id in list(current_map.keys()):
            if current_map[chat_id] is None and queue_map[chat_id]:
                try:
                    await ensure_playback(chat_id)
                except Exception as e:
                    log.warning("Auto-play failed for %s: %s", chat_id, e)


# =========================
# MAIN
# =========================
async def main():
    await assistant.start(
        phone=None,
        bot_token=None,
        password=None,
        code_callback=None,
        force_sms=None,
        first_name=None,
        last_name=None,
        max_attempts=None,
        session=ASSISTANT_SESSION,
    )
    await bot.start(bot_token=BOT_TOKEN)

    call.start()

    log.info("Bot and assistant started.")
    asyncio.create_task(playback_watcher())

    await idle()


if __name__ == "__main__":
    asyncio.run(main())

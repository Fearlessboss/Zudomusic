#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Single-file Telegram Music Bot with clone mode.

SECURITY NOTE:
- Do NOT keep real bot tokens or string sessions hardcoded in public files.
- Since you already shared credentials in chat, rotate/revoke them before production use.
- Fill the placeholders below OR use environment variables.

Main features:
- /start stylish message with inline buttons
- /help and /commands with full command list
- /play with YouTube search via yt-dlp
- Queue, loop, pause, resume, skip, stop, mute, unmute, ping
- Owner-only /clone flow for launching same single file as a cloned bot
- Each clone can have custom support and owner username

Tested target stack:
- Python 3.10+
- pyrogram
- py-tgcalls
- yt-dlp
- ffmpeg (required on host machine)
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# =========================================================
# AUTO INSTALL PIP DEPENDENCIES (single-file convenience)
# =========================================================
REQUIRED_PACKAGES = {
    "pyrogram": "pyrogram",
    "tgcrypto": "tgcrypto",
    "pytgcalls": "py-tgcalls",
    "yt_dlp": "yt-dlp",
    "aiohttp": "aiohttp",
}


def ensure_python_packages() -> None:
    missing = [pip_name for mod, pip_name in REQUIRED_PACKAGES.items() if importlib.util.find_spec(mod) is None]
    if not missing:
        return
    print(f"[BOOT] Installing missing packages: {', '.join(missing)}")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", *missing])


ensure_python_packages()

import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, UserAlreadyParticipant
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, GroupCallConfig, MediaStream, StreamEnded
from yt_dlp import YoutubeDL


# =========================================================
# USER CONFIG - FILL THESE VALUES OR USE ENV VARIABLES
# =========================================================
API_ID = int(os.getenv("API_ID", "33628258"))
API_HASH = os.getenv("API_HASH", "0850762925b9c1715b9b122f7b753128")
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "8727045177:AAHBBXLTABA5BQPSlKrUlVtlYfzHv8YW7RA")
OWNER_ID = int(os.getenv("OWNER_ID", "7661825494"))
DEFAULT_ASSISTANT_SESSION = os.getenv("DEFAULT_ASSISTANT_SESSION", "BAIBIGIAq8OQHIQxDFA3LDgskQKAp3979G2EilIaWsBGu6yahWNA9tn_L4eB6UaNsp3ivZ0fx8KIE61qC0mfusNFHDi5N2JZPV0AwtSHxlCeMI4OI8aQ7vyq10HJhDzt_KtHXhrBrgNeorlRfoZRRtl7JSN31X6h84tDANtWrA5YteeuWKRaPTwiggRw86IkyV72DrVPnzFnAeb7xpzy9L7JE9Bw_l0Cddo3cZpDQbfY6QyPLICEsYPPFIC4-IULcUISDSpOvT32LBHj9LFWCy9VUcCi2H_YMGKL508pT2uwo9wSFuwE33MP1571DbhniOtYveG207Ir3TixGl0cGTpQaIkIswAAAAG1wb5UAA")
MASTER_SUPPORT_CHAT = os.getenv("MASTER_SUPPORT_CHAT", "@userbotsupportchat")
MASTER_OWNER_USERNAME = os.getenv("MASTER_OWNER_USERNAME", "@ITZ_ME_ADITYA_02")
NUBCODER_TOKEN = os.getenv("NUBCODER_TOKEN", "4HBcMS072p")

BOT_BRAND_NAME = os.getenv("BOT_BRAND_NAME", "ZUDO X MUSIC")
BOT_BRAND_TAGLINE = os.getenv("BOT_BRAND_TAGLINE", "Ultra Fast • No Lag • Voice Chat Player")

# =========================================================
# PATHS
# =========================================================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "musicbot_runtime"
CLONES_DIR = DATA_DIR / "clones"
LOGS_DIR = DATA_DIR / "logs"
PIDS_DIR = DATA_DIR / "pids"
for _p in [DATA_DIR, CLONES_DIR, LOGS_DIR, PIDS_DIR]:
    _p.mkdir(parents=True, exist_ok=True)


# =========================================================
# DATA MODELS
# =========================================================
@dataclass
class BotConfig:
    api_id: int
    api_hash: str
    bot_token: str
    owner_id: int
    assistant_session: str
    support_chat: str
    owner_username: str
    nubcoder_token: str = ""
    clone_mode: bool = False
    brand_name: str = BOT_BRAND_NAME
    tagline: str = BOT_BRAND_TAGLINE

    @property
    def bot_id(self) -> str:
        return self.bot_token.split(":", 1)[0] if ":" in self.bot_token else "unknown"


@dataclass
class Track:
    title: str
    stream_url: str
    webpage_url: str
    duration: int = 0
    requested_by: str = "Unknown"
    source: str = "YouTube"
    thumbnail: str = ""

    @property
    def pretty_duration(self) -> str:
        if not self.duration:
            return "Live/Unknown"
        m, s = divmod(self.duration, 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h:02d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"


@dataclass
class ChatState:
    current: Optional[Track] = None
    queue: List[Track] = field(default_factory=list)
    loop: bool = False
    paused: bool = False


# =========================================================
# HELPERS
# =========================================================
URL_RE = re.compile(r"^(https?://|www\.)", re.I)
TOKEN_RE = re.compile(r"^\d{7,12}:[A-Za-z0-9_-]{20,}$")


def is_url(text: str) -> bool:
    return bool(URL_RE.match(text.strip()))


def normalize_support(value: str) -> str:
    value = value.strip()
    if value.startswith("https://t.me/"):
        value = "@" + value.split("https://t.me/", 1)[1].strip("/")
    if value.startswith("t.me/"):
        value = "@" + value.split("t.me/", 1)[1].strip("/")
    if value and not value.startswith("@") and re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        value = "@" + value
    return value


def normalize_owner_username(value: str) -> str:
    value = value.strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1].strip("/")
    if value.startswith("t.me/"):
        value = value.split("t.me/", 1)[1].strip("/")
    if value and not value.startswith("@"):
        value = "@" + value
    return value


def mention_user(message: Message) -> str:
    user = message.from_user
    if not user:
        return "Unknown"
    name = user.first_name or "User"
    return f"[{name}](tg://user?id={user.id})"


def command_arg(message: Message) -> str:
    if not message.text:
        return ""
    parts = message.text.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def load_config(path: Path) -> BotConfig:
    return BotConfig(**json.loads(path.read_text(encoding="utf-8")))


def save_config(cfg: BotConfig, path: Path) -> None:
    path.write_text(json.dumps(asdict(cfg), indent=2, ensure_ascii=False), encoding="utf-8")


async def safe_send(message: Message, text: str, **kwargs):
    try:
        return await message.reply_text(text, **kwargs)
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        return await message.reply_text(text, **kwargs)


def sync_extract_track(query: str) -> Track:
    ydl_opts = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "default_search": "ytsearch1",
        "skip_download": True,
        "geo_bypass": True,
        "extract_flat": False,
    }

    source = query if is_url(query) else f"ytsearch1:{query}"
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(source, download=False)
        if info is None:
            raise ValueError("No result found.")
        if "entries" in info:
            entries = info.get("entries") or []
            info = next((x for x in entries if x), None)
            if not info:
                raise ValueError("No playable result found.")

        stream_url = info.get("url")
        webpage_url = info.get("webpage_url") or info.get("original_url") or query
        title = info.get("title") or "Unknown Title"
        duration = int(info.get("duration") or 0)
        source_name = info.get("extractor_key") or info.get("extractor") or "Media"
        thumb = info.get("thumbnail") or ""

        if not stream_url:
            raise ValueError("Playable audio URL not resolved.")

        return Track(
            title=title,
            stream_url=stream_url,
            webpage_url=webpage_url,
            duration=duration,
            source=source_name,
            thumbnail=thumb,
        )


class TelegramMusicBot:
    def __init__(self, config: BotConfig, config_path: Optional[Path] = None, is_master: bool = False):
        self.config = config
        self.config_path = config_path
        self.is_master = is_master
        self.bot = Client(
            name=f"bot_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            bot_token=config.bot_token,
            workdir=str(DATA_DIR),
            in_memory=False,
        )
        self.assistant = Client(
            name=f"assistant_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            session_string=config.assistant_session,
            workdir=str(DATA_DIR),
            in_memory=False,
            no_updates=True,
        )
        self.calls = PyTgCalls(self.assistant)
        self.states: Dict[int, ChatState] = {}
        self.chat_locks: Dict[int, asyncio.Lock] = {}
        self.clone_flow: Dict[int, Dict[str, str]] = {}
        self.bot_username: str = ""
        self.bot_name: str = config.brand_name

    def get_state(self, chat_id: int) -> ChatState:
        if chat_id not in self.states:
            self.states[chat_id] = ChatState()
        return self.states[chat_id]

    def get_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        return self.chat_locks[chat_id]

    async def resolve_track(self, query: str, requested_by: str) -> Track:
        track = await asyncio.to_thread(sync_extract_track, query)
        track.requested_by = requested_by
        return track

    def start_text(self) -> str:
        return (
            f"<b>✨ {escape_html(self.config.brand_name)}</b>\n"
            f"<i>{escape_html(self.config.tagline)}</i>\n\n"
            "🎧 High-speed voice chat music bot ready hai.\n"
            "🚀 YouTube search + direct stream + smart queue system.\n"
            "🎛 Stylish controls, inline buttons, powerful admin commands.\n\n"
            "<b>Quick Start:</b>\n"
            "• Group me add karo\n"
            "• Voice chat start karo\n"
            "• /play songname bhejo\n\n"
            f"<b>Support:</b> {escape_html(self.config.support_chat)}\n"
            f"<b>Owner:</b> {escape_html(self.config.owner_username)}"
        )

    def help_text(self) -> str:
        return (
            f"<b>🎵 {escape_html(self.config.brand_name)} — Full Help Panel</b>\n\n"
            "<b>Basic Commands</b>\n"
            "• /start - Stylish start panel\n"
            "• /help - Full help menu\n"
            "• /commands - Complete command list\n"
            "• /ping - Bot speed check\n"
            "• /alive - Bot online status\n\n"
            "<b>Music Commands</b>\n"
            "• /play songname - Search karke play karega\n"
            "• /play URL - Direct URL stream karega\n"
            "• /p - /play alias\n"
            "• /pause - Song pause\n"
            "• /resume - Song resume\n"
            "• /skip - Next song\n"
            "• /next - /skip alias\n"
            "• /stop - Playback stop\n"
            "• /end - /stop alias\n"
            "• /queue - Queue list\n"
            "• /q - /queue alias\n"
            "• /loop - Loop on/off\n"
            "• /shuffle - Queue shuffle\n"
            "• /clearqueue - Queue clear\n"
            "• /np - Current song\n"
            "• /now - /np alias\n"
            "• /mute - VC audio mute\n"
            "• /unmute - VC audio unmute\n\n"
            "<b>Owner Commands</b>\n"
            "• /clone - New clone bot launch flow\n"
            "• /cancel - Current clone flow cancel\n"
            "• /clones - Saved clone configs list\n\n"
            "<b>Notes</b>\n"
            "• Best use in groups / supergroups\n"
            "• Voice chat auto-start supported\n"
            "• Uses yt-dlp for fast searchable playback\n"
            "• ffmpeg host machine me installed hona chahiye\n"
        )

    def commands_text(self) -> str:
        return (
            "<b>📜 All Commands</b>\n\n"
            "/start\n/help\n/commands\n/ping\n/alive\n"
            "/play <song name or url>\n/p\n/pause\n/resume\n/skip\n/next\n/stop\n/end\n"
            "/queue\n/q\n/loop\n/shuffle\n/clearqueue\n/np\n/now\n/mute\n/unmute\n\n"
            "<b>Owner Only:</b>\n/clone\n/cancel\n/clones"
        )

    def start_keyboard(self) -> InlineKeyboardMarkup:
        add_url = f"https://t.me/{self.bot_username}?startgroup=true" if self.bot_username else "https://t.me"
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("➕ Add To Group", url=add_url),
                    InlineKeyboardButton("📚 Help", callback_data="panel_help"),
                ],
                [
                    InlineKeyboardButton("🎛 Commands", callback_data="panel_commands"),
                    InlineKeyboardButton("💬 Support", url=f"https://t.me/{self.config.support_chat.lstrip('@')}"),
                ],
                [
                    InlineKeyboardButton("👑 Owner", url=f"https://t.me/{self.config.owner_username.lstrip('@')}"),
                    InlineKeyboardButton("🏠 Home", callback_data="panel_home"),
                ],
            ]
        )

    async def is_admin(self, chat_id: int, user_id: int) -> bool:
        if user_id == self.config.owner_id:
            return True
        try:
            member = await self.bot.get_chat_member(chat_id, user_id)
            return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
        except Exception:
            return False

    async def ensure_assistant_in_chat(self, chat_id: int) -> None:
        try:
            invite_link = await self.bot.export_chat_invite_link(chat_id)
            try:
                await self.assistant.join_chat(invite_link)
            except UserAlreadyParticipant:
                pass
            except Exception:
                pass
        except Exception:
            pass

    async def play_track(self, chat_id: int, track: Track) -> None:
        await self.ensure_assistant_in_chat(chat_id)
        await self.calls.play(
            chat_id,
            MediaStream(track.stream_url, audio_parameters=AudioQuality.HIGH),
            config=GroupCallConfig(auto_start=True),
        )
        state = self.get_state(chat_id)
        state.current = track
        state.paused = False

    async def play_next(self, chat_id: int, announce_chat: bool = False, reason: str = "") -> None:
        async with self.get_lock(chat_id):
            state = self.get_state(chat_id)
            next_track: Optional[Track] = None
            if state.loop and state.current:
                next_track = state.current
            elif state.queue:
                next_track = state.queue.pop(0)
            else:
                state.current = None
                state.paused = False
                try:
                    await self.calls.leave_call(chat_id)
                except Exception:
                    pass
                return

            await self.play_track(chat_id, next_track)
            if announce_chat:
                try:
                    await self.bot.send_message(
                        chat_id,
                        (
                            f"<b>▶️ Now Playing</b>\n"
                            f"<b>Title:</b> <a href=\"{escape_html(next_track.webpage_url)}\">{escape_html(next_track.title)}</a>\n"
                            f"<b>Duration:</b> {escape_html(next_track.pretty_duration)}\n"
                            f"<b>Requested By:</b> {next_track.requested_by}\n"
                            + (f"<b>Reason:</b> {escape_html(reason)}\n" if reason else "")
                        ),
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass

    async def add_handlers(self) -> None:
        @self.calls.on_update()
        async def stream_updates(_, update):
            if isinstance(update, StreamEnded):
                await self.play_next(update.chat_id, announce_chat=True, reason="Previous stream ended")

        @self.bot.on_message(filters.command(["start"]) & filters.private)
        async def start_handler(_, message: Message):
            await safe_send(
                message,
                self.start_text(),
                reply_markup=self.start_keyboard(),
                disable_web_page_preview=True,
            )

        @self.bot.on_callback_query()
        async def panel_callbacks(_, query):
            data = query.data or ""
            if data == "panel_home":
                await query.message.edit_text(self.start_text(), reply_markup=self.start_keyboard(), disable_web_page_preview=True)
            elif data == "panel_help":
                await query.message.edit_text(self.help_text(), reply_markup=self.start_keyboard(), disable_web_page_preview=True)
            elif data == "panel_commands":
                await query.message.edit_text(self.commands_text(), reply_markup=self.start_keyboard(), disable_web_page_preview=True)
            await query.answer()

        @self.bot.on_message(filters.command(["help", "commands"]) & (filters.private | filters.group))
        async def help_handler(_, message: Message):
            text = self.help_text() if message.command and message.command[0].lower() == "help" else self.commands_text()
            await safe_send(message, text, disable_web_page_preview=True)

        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def ping_handler(_, message: Message):
            start = time.perf_counter()
            x = await safe_send(message, "<b>🏓 Pinging...</b>")
            taken = (time.perf_counter() - start) * 1000
            await x.edit_text(
                (
                    f"<b>⚡ {escape_html(self.config.brand_name)} is Online</b>\n"
                    f"<b>Latency:</b> <code>{taken:.2f} ms</code>\n"
                    f"<b>Mode:</b> {'Clone' if self.config.clone_mode else 'Master'}\n"
                    f"<b>Bot ID:</b> <code>{escape_html(self.config.bot_id)}</code>"
                )
            )

        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def play_handler(_, message: Message):
            query = command_arg(message)
            if not query:
                return await safe_send(message, "<b>❌ Usage:</b> <code>/play song name</code>")

            processing = await safe_send(message, f"<b>🔎 Searching:</b> <code>{escape_html(query)}</code>")
            try:
                track = await self.resolve_track(query, mention_user(message))
            except Exception as exc:
                return await processing.edit_text(f"<b>❌ Failed:</b> <code>{escape_html(str(exc))}</code>")

            state = self.get_state(message.chat.id)
            async with self.get_lock(message.chat.id):
                if state.current is None:
                    try:
                        await self.play_track(message.chat.id, track)
                    except Exception as exc:
                        return await processing.edit_text(
                            "<b>❌ VC playback failed.</b>\n"
                            f"<code>{escape_html(str(exc))}</code>\n\n"
                            "<b>Tip:</b> Group voice chat/video chat active hona chahiye."
                        )
                    return await processing.edit_text(
                        (
                            f"<b>▶️ Playing Now</b>\n"
                            f"<b>Title:</b> <a href=\"{escape_html(track.webpage_url)}\">{escape_html(track.title)}</a>\n"
                            f"<b>Duration:</b> {escape_html(track.pretty_duration)}\n"
                            f"<b>Source:</b> {escape_html(track.source)}\n"
                            f"<b>Requested By:</b> {track.requested_by}"
                        ),
                        disable_web_page_preview=True,
                    )
                state.queue.append(track)
                return await processing.edit_text(
                    (
                        f"<b>📥 Added To Queue</b>\n"
                        f"<b>Title:</b> <a href=\"{escape_html(track.webpage_url)}\">{escape_html(track.title)}</a>\n"
                        f"<b>Duration:</b> {escape_html(track.pretty_duration)}\n"
                        f"<b>Position:</b> <code>{len(state.queue)}</code>"
                    ),
                    disable_web_page_preview=True,
                )

        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def pause_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            try:
                await self.calls.pause(message.chat.id)
                self.get_state(message.chat.id).paused = True
                await safe_send(message, "<b>⏸ Playback paused.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Pause failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["resume"]) & filters.group)
        async def resume_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            try:
                await self.calls.resume(message.chat.id)
                self.get_state(message.chat.id).paused = False
                await safe_send(message, "<b>▶️ Playback resumed.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Resume failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["mute"]) & filters.group)
        async def mute_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            try:
                await self.calls.mute(message.chat.id)
                await safe_send(message, "<b>🔇 VC muted.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Mute failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["unmute"]) & filters.group)
        async def unmute_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            try:
                await self.calls.unmute(message.chat.id)
                await safe_send(message, "<b>🔊 VC unmuted.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Unmute failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def skip_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            state = self.get_state(message.chat.id)
            if not state.current and not state.queue:
                return await safe_send(message, "<b>❌ Queue empty hai.</b>")
            await safe_send(message, "<b>⏭ Skipping current track...</b>")
            state.current = None
            state.paused = False
            await self.play_next(message.chat.id, announce_chat=True, reason="Skipped by admin")

        @self.bot.on_message(filters.command(["stop", "end"]) & filters.group)
        async def stop_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            state = self.get_state(message.chat.id)
            state.queue.clear()
            state.current = None
            state.paused = False
            state.loop = False
            try:
                await self.calls.leave_call(message.chat.id)
            except Exception:
                pass
            await safe_send(message, "<b>⏹ Playback ended and queue cleared.</b>")

        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def queue_handler(_, message: Message):
            state = self.get_state(message.chat.id)
            if not state.current and not state.queue:
                return await safe_send(message, "<b>📭 Queue empty hai.</b>")
            lines = ["<b>🎶 Queue Panel</b>"]
            if state.current:
                lines.append(
                    f"\n<b>Now:</b> <a href=\"{escape_html(state.current.webpage_url)}\">{escape_html(state.current.title)}</a> "
                    f"({escape_html(state.current.pretty_duration)})"
                )
            if state.queue:
                lines.append("\n<b>Up Next:</b>")
                for i, track in enumerate(state.queue[:15], start=1):
                    lines.append(f"{i}. {escape_html(track.title)} — {escape_html(track.pretty_duration)}")
                if len(state.queue) > 15:
                    lines.append(f"... and {len(state.queue) - 15} more")
            lines.append(f"\n<b>Loop:</b> {'On' if state.loop else 'Off'}")
            await safe_send(message, "\n".join(lines), disable_web_page_preview=True)

        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def loop_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            arg = command_arg(message).lower()
            state = self.get_state(message.chat.id)
            if arg in {"on", "yes", "true", "1"}:
                state.loop = True
            elif arg in {"off", "no", "false", "0"}:
                state.loop = False
            else:
                state.loop = not state.loop
            await safe_send(message, f"<b>🔁 Loop {'enabled' if state.loop else 'disabled'}.</b>")

        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def shuffle_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            state = self.get_state(message.chat.id)
            if len(state.queue) < 2:
                return await safe_send(message, "<b>❌ Shuffle ke liye kam se kam 2 tracks chahiye.</b>")
            random.shuffle(state.queue)
            await safe_send(message, "<b>🔀 Queue shuffled.</b>")

        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def clearqueue_handler(_, message: Message):
            if not await self.is_admin(message.chat.id, message.from_user.id):
                return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
            state = self.get_state(message.chat.id)
            count = len(state.queue)
            state.queue.clear()
            await safe_send(message, f"<b>🧹 Cleared {count} queued tracks.</b>")

        @self.bot.on_message(filters.command(["np", "now"]) & filters.group)
        async def np_handler(_, message: Message):
            state = self.get_state(message.chat.id)
            if not state.current:
                return await safe_send(message, "<b>❌ Abhi kuch play nahi ho raha.</b>")
            await safe_send(
                message,
                (
                    f"<b>🎵 Now Playing</b>\n"
                    f"<b>Title:</b> <a href=\"{escape_html(state.current.webpage_url)}\">{escape_html(state.current.title)}</a>\n"
                    f"<b>Duration:</b> {escape_html(state.current.pretty_duration)}\n"
                    f"<b>Source:</b> {escape_html(state.current.source)}\n"
                    f"<b>Requested By:</b> {state.current.requested_by}\n"
                    f"<b>Loop:</b> {'On' if state.loop else 'Off'}\n"
                    f"<b>Paused:</b> {'Yes' if state.paused else 'No'}"
                ),
                disable_web_page_preview=True,
            )

        if self.is_master:
            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def clone_handler(_, message: Message):
                if message.from_user.id != self.config.owner_id:
                    return await safe_send(message, "<b>❌ Owner only command.</b>")
                self.clone_flow[message.from_user.id] = {"step": "bot_token"}
                await safe_send(
                    message,
                    (
                        "<b>🤖 Clone Setup Started</b>\n\n"
                        "Step 1/4: Naya bot token bhejo.\n"
                        "Example: <code>123456789:ABCDEF...</code>\n\n"
                        "Cancel karna ho to /cancel"
                    ),
                )

            @self.bot.on_message(filters.command(["cancel"]) & filters.private)
            async def cancel_handler(_, message: Message):
                if message.from_user.id != self.config.owner_id:
                    return
                self.clone_flow.pop(message.from_user.id, None)
                await safe_send(message, "<b>🛑 Current clone flow cancelled.</b>")

            @self.bot.on_message(filters.command(["clones"]) & filters.private)
            async def clones_handler(_, message: Message):
                if message.from_user.id != self.config.owner_id:
                    return await safe_send(message, "<b>❌ Owner only command.</b>")
                files = sorted(CLONES_DIR.glob("*.json"))
                if not files:
                    return await safe_send(message, "<b>📭 No clone configs found.</b>")
                lines = ["<b>📦 Saved Clones</b>"]
                for f in files[:50]:
                    try:
                        cfg = load_config(f)
                        lines.append(f"• <code>{escape_html(cfg.bot_id)}</code> - {escape_html(cfg.owner_username)} - {escape_html(cfg.support_chat)}")
                    except Exception:
                        lines.append(f"• <code>{escape_html(f.name)}</code>")
                await safe_send(message, "\n".join(lines))

            @self.bot.on_message(filters.private & filters.text & ~filters.command(["clone", "cancel", "clones", "start", "help", "commands", "ping", "alive"]))
            async def clone_flow_handler(_, message: Message):
                if message.from_user.id != self.config.owner_id:
                    return
                state = self.clone_flow.get(message.from_user.id)
                if not state:
                    return

                text = (message.text or "").strip()
                step = state.get("step")

                if step == "bot_token":
                    if not TOKEN_RE.match(text):
                        return await safe_send(message, "<b>❌ Invalid bot token.</b> Dobara bhejo.")
                    state["bot_token"] = text
                    state["step"] = "support"
                    return await safe_send(message, "<b>Step 2/4:</b> Support group username ya link bhejo.\nExample: <code>@userbotsupportchat</code>")

                if step == "support":
                    state["support_chat"] = normalize_support(text)
                    state["step"] = "owner_username"
                    return await safe_send(message, "<b>Step 3/4:</b> Owner username/link bhejo.\nExample: <code>@ITZ_ME_ADITYA_02</code>")

                if step == "owner_username":
                    state["owner_username"] = normalize_owner_username(text)
                    state["step"] = "session"
                    return await safe_send(message, "<b>Step 4/4:</b> Assistant string session bhejo ya <code>/default</code> likho.")

                if step == "session":
                    session_string = self.config.assistant_session if text.lower() == "/default" else text
                    if len(session_string) < 30:
                        return await safe_send(message, "<b>❌ Session string bahut short lag raha hai.</b>")

                    clone_cfg = BotConfig(
                        api_id=self.config.api_id,
                        api_hash=self.config.api_hash,
                        bot_token=state["bot_token"],
                        owner_id=self.config.owner_id,
                        assistant_session=session_string,
                        support_chat=state["support_chat"],
                        owner_username=state["owner_username"],
                        nubcoder_token=self.config.nubcoder_token,
                        clone_mode=True,
                        brand_name=self.config.brand_name,
                        tagline=self.config.tagline,
                    )

                    cfg_path = CLONES_DIR / f"{clone_cfg.bot_id}.json"
                    save_config(clone_cfg, cfg_path)
                    log_path = LOGS_DIR / f"{clone_cfg.bot_id}.log"
                    pid_path = PIDS_DIR / f"{clone_cfg.bot_id}.pid"

                    with open(log_path, "a", encoding="utf-8") as log_file:
                        proc = subprocess.Popen(
                            [sys.executable, str(Path(__file__).resolve()), "--config", str(cfg_path)],
                            stdout=log_file,
                            stderr=log_file,
                            start_new_session=True,
                        )
                    pid_path.write_text(str(proc.pid), encoding="utf-8")
                    self.clone_flow.pop(message.from_user.id, None)
                    return await safe_send(
                        message,
                        (
                            f"<b>✅ Clone launched successfully</b>\n"
                            f"<b>Bot ID:</b> <code>{escape_html(clone_cfg.bot_id)}</code>\n"
                            f"<b>Support:</b> {escape_html(clone_cfg.support_chat)}\n"
                            f"<b>Owner:</b> {escape_html(clone_cfg.owner_username)}\n"
                            f"<b>PID:</b> <code>{proc.pid}</code>\n"
                            f"<b>Config:</b> <code>{escape_html(str(cfg_path))}</code>"
                        ),
                    )

    async def start(self) -> None:
        if shutil.which("ffmpeg") is None:
            print("[WARN] ffmpeg not found in PATH. Install ffmpeg before using music playback.")

        await self.assistant.start()
        await self.bot.start()
        await self.calls.start()

        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name = me.first_name or self.config.brand_name

        await self.add_handlers()
        print(f"[RUNNING] {self.bot_name} | @{self.bot_username} | clone={self.config.clone_mode}")
        await idle()

    async def stop(self) -> None:
        try:
            await self.calls.stop()
        except Exception:
            pass
        try:
            await self.bot.stop()
        except Exception:
            pass
        try:
            await self.assistant.stop()
        except Exception:
            pass


async def main() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "--config":
        cfg = load_config(Path(sys.argv[2]))
        app = TelegramMusicBot(cfg, config_path=Path(sys.argv[2]), is_master=False)
    else:
        master_cfg = BotConfig(
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=MAIN_BOT_TOKEN,
            owner_id=OWNER_ID,
            assistant_session=DEFAULT_ASSISTANT_SESSION,
            support_chat=MASTER_SUPPORT_CHAT,
            owner_username=MASTER_OWNER_USERNAME,
            nubcoder_token=NUBCODER_TOKEN,
            clone_mode=False,
            brand_name=BOT_BRAND_NAME,
            tagline=BOT_BRAND_TAGLINE,
        )
        app = TelegramMusicBot(master_cfg, is_master=True)

    try:
        await app.start()
    finally:
        await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

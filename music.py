#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hardened single-file Telegram Music Bot.
Only runtime-generated folders are used; no extra source files required.

Main goals:
- single file bot + clone mode
- auto-restart supervisor on crash
- Docker friendly
- better error handling
- only runtime folders are created automatically
- optional local .env loading

Required ENV examples:
API_ID=12345
API_HASH=your_api_hash
MAIN_BOT_TOKEN=123456:ABC
OWNER_ID=123456789
DEFAULT_ASSISTANT_SESSION=your_pyrogram_string_session
MASTER_SUPPORT_CHAT=@supportchat
MASTER_OWNER_USERNAME=@owner
BOT_BRAND_NAME=My Music Bot
BOT_BRAND_TAGLINE=Fast VC Player
AUTO_INSTALL_DEPS=true

Optional:
RUNTIME_DIR=/app/runtime
CLONE_RESTART_DELAY=5
MAX_RESTART_DELAY=60
LOG_LEVEL=INFO
ENV_FILE=/app/.env
"""

from __future__ import annotations

import asyncio
import html
import importlib.util
import json
import logging
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


# =========================================================
# LOCAL .ENV LOADER
# =========================================================
def load_local_env() -> None:
    env_candidates = []
    custom_env = os.getenv("ENV_FILE", "").strip()
    if custom_env:
        env_candidates.append(Path(custom_env).expanduser())
    env_candidates.append(Path(__file__).resolve().with_name(".env"))

    env_path = next((p for p in env_candidates if p.exists() and p.is_file()), None)
    if not env_path:
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_local_env()


# =========================================================
# BOOTSTRAP
# =========================================================
REQUIRED_PACKAGES = {
    "pyrogram": "pyrogram>=2.0.106",
    "tgcrypto": "tgcrypto>=1.2.5",
    "pytgcalls": "py-tgcalls>=2.1.0",
    "yt_dlp": "yt-dlp>=2025.3.31",
}


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def ensure_python_packages() -> None:
    if not _env_bool("AUTO_INSTALL_DEPS", True):
        return
    missing = []
    for module_name, pip_name in REQUIRED_PACKAGES.items():
        if importlib.util.find_spec(module_name) is None:
            missing.append(pip_name)
    if not missing:
        return
    print(f"[BOOT] Installing missing packages: {', '.join(missing)}", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", "-U", *missing])


ensure_python_packages()

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, UserAlreadyParticipant
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from pytgcalls import PyTgCalls
from pytgcalls.types import StreamEnded
from yt_dlp import YoutubeDL


# =========================================================
# LOGGING
# =========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("musicbot")


# =========================================================
# CONFIG
# =========================================================
API_ID = int(os.getenv("API_ID", "0") or "0")
API_HASH = os.getenv("API_HASH", "")
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
DEFAULT_ASSISTANT_SESSION = os.getenv("DEFAULT_ASSISTANT_SESSION", "")
MASTER_SUPPORT_CHAT = os.getenv("MASTER_SUPPORT_CHAT", "@support")
MASTER_OWNER_USERNAME = os.getenv("MASTER_OWNER_USERNAME", "@owner")
NUBCODER_TOKEN = os.getenv("NUBCODER_TOKEN", "")
BOT_BRAND_NAME = os.getenv("BOT_BRAND_NAME", "ZUDO X MUSIC")
BOT_BRAND_TAGLINE = os.getenv("BOT_BRAND_TAGLINE", "Ultra Fast • No Lag • Voice Chat Player")

RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", str(Path(__file__).resolve().parent / "runtime"))).resolve()
DATA_DIR = RUNTIME_DIR
CLONES_DIR = DATA_DIR / "clones"
LOGS_DIR = DATA_DIR / "logs"
PIDS_DIR = DATA_DIR / "pids"
CACHE_DIR = DATA_DIR / "cache"
DOWNLOADS_DIR = DATA_DIR / "downloads"

for _p in [DATA_DIR, CLONES_DIR, LOGS_DIR, PIDS_DIR, CACHE_DIR, DOWNLOADS_DIR]:
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
        m, s = divmod(int(self.duration), 60)
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
    muted: bool = False


# =========================================================
# HELPERS
# =========================================================
URL_RE = re.compile(r"^(https?://|www\.)", re.I)
TOKEN_RE = re.compile(r"^\d{7,12}:[A-Za-z0-9_-]{20,}$")


def is_url(text: str) -> bool:
    return bool(URL_RE.match((text or "").strip()))


def escape_html(text: str) -> str:
    return html.escape(str(text or ""), quote=True)


def normalize_support(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("https://t.me/"):
        value = "@" + value.split("https://t.me/", 1)[1].strip("/")
    if value.startswith("http://t.me/"):
        value = "@" + value.split("http://t.me/", 1)[1].strip("/")
    if value.startswith("t.me/"):
        value = "@" + value.split("t.me/", 1)[1].strip("/")
    if value and not value.startswith("@") and re.fullmatch(r"[A-Za-z0-9_]{5,32}", value):
        value = "@" + value
    return value or "@support"


def normalize_owner_username(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1].strip("/")
    if value.startswith("http://t.me/"):
        value = value.split("http://t.me/", 1)[1].strip("/")
    if value.startswith("t.me/"):
        value = value.split("t.me/", 1)[1].strip("/")
    if value and not value.startswith("@"):
        value = "@" + value
    return value or "@owner"


def mention_user(message: Message) -> str:
    user = message.from_user
    if not user:
        return "Unknown"
    name = user.first_name or user.username or "User"
    return f"<a href=\"tg://user?id={user.id}\">{escape_html(name)}</a>"


def command_arg(message: Message) -> str:
    text = message.text or message.caption or ""
    parts = text.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def load_config(path: Path) -> BotConfig:
    return BotConfig(**json.loads(path.read_text(encoding="utf-8")))


def save_config(cfg: BotConfig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(cfg), indent=2, ensure_ascii=False), encoding="utf-8")


def validate_config(cfg: BotConfig) -> None:
    missing = []
    if not cfg.api_id:
        missing.append("API_ID")
    if not cfg.api_hash:
        missing.append("API_HASH")
    if not cfg.bot_token:
        missing.append("MAIN_BOT_TOKEN / clone bot_token")
    if not cfg.owner_id:
        missing.append("OWNER_ID")
    if not cfg.assistant_session:
        missing.append("DEFAULT_ASSISTANT_SESSION / clone assistant_session")
    if missing:
        raise ValueError("Missing required config: " + ", ".join(missing))


async def safe_send(message: Message, text: str, **kwargs):
    try:
        return await message.reply_text(text, **kwargs)
    except FloodWait as fw:
        await asyncio.sleep(getattr(fw, "value", 1))
        return await message.reply_text(text, **kwargs)
    except Exception:
        log.exception("safe_send failed")
        return None


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
        "nocheckcertificate": True,
        "cookiefile": None,
        "source_address": "0.0.0.0",
    }
    source = query if is_url(query) else f"ytsearch1:{query}"
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(source, download=False)
        if info is None:
            raise ValueError("No result found")
        if "entries" in info:
            entries = info.get("entries") or []
            info = next((x for x in entries if x), None)
            if not info:
                raise ValueError("No playable result found")
        stream_url = info.get("url")
        webpage_url = info.get("webpage_url") or info.get("original_url") or query
        title = info.get("title") or "Unknown Title"
        duration = int(info.get("duration") or 0)
        source_name = info.get("extractor_key") or info.get("extractor") or "Media"
        thumb = info.get("thumbnail") or ""
        if not stream_url:
            raise ValueError("Playable audio URL not resolved")
        return Track(
            title=title,
            stream_url=stream_url,
            webpage_url=webpage_url,
            duration=duration,
            source=source_name,
            thumbnail=thumb,
        )


# =========================================================
# BOT CORE
# =========================================================
class TelegramMusicBot:
    def __init__(self, config: BotConfig, config_path: Optional[Path] = None, is_master: bool = False):
        validate_config(config)
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
            parse_mode=ParseMode.HTML,
        )
        self.assistant = Client(
            name=f"assistant_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            session_string=config.assistant_session,
            workdir=str(DATA_DIR),
            in_memory=False,
            no_updates=True,
            parse_mode=ParseMode.HTML,
        )
        self.calls = PyTgCalls(self.assistant)
        self.states: Dict[int, ChatState] = {}
        self.chat_locks: Dict[int, asyncio.Lock] = {}
        self.clone_flow: Dict[int, Dict[str, str]] = {}
        self.bot_username: str = ""
        self.bot_name: str = config.brand_name
        self._stopping = False

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
            "• <code>/play songname</code> bhejo\n\n"
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
            "• Voice chat/video chat active hona chahiye\n"
            "• yt-dlp based search enabled\n"
            "• Dockerfile ffmpeg ke saath ready hai\n"
        )

    def commands_text(self) -> str:
        return (
            "<b>📜 All Commands</b>\n\n"
            "/start\n/help\n/commands\n/ping\n/alive\n"
            "/play &lt;song name or url&gt;\n/p\n/pause\n/resume\n/skip\n/next\n/stop\n/end\n"
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

    async def is_admin(self, chat_id: int, user_id: Optional[int]) -> bool:
        if not user_id:
            return False
        if user_id == self.config.owner_id:
            return True
        try:
            member = await self.bot.get_chat_member(chat_id, user_id)
            return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
        except Exception:
            log.exception("Failed to check admin")
            return False

    async def ensure_assistant_in_chat(self, chat_id: int) -> None:
        try:
            invite_link = await self.bot.export_chat_invite_link(chat_id)
            try:
                await self.assistant.join_chat(invite_link)
            except UserAlreadyParticipant:
                return
            except Exception:
                log.exception("Assistant join via invite failed")
        except Exception:
            log.exception("Export invite link failed")

    async def play_track(self, chat_id: int, track: Track) -> None:
        await self.ensure_assistant_in_chat(chat_id)
        try:
            await self.calls.play(chat_id, track.stream_url)
        except TypeError:
            await self.calls.play(chat_id, track.stream_url, stream_type="audio")
        state = self.get_state(chat_id)
        state.current = track
        state.paused = False
        state.muted = False

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
                state.muted = False
                try:
                    await self.calls.leave_call(chat_id)
                except Exception:
                    log.exception("leave_call failed")
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
                    log.exception("Now playing announcement failed")

    async def on_stream_end(self, chat_id: int) -> None:
        try:
            await self.play_next(chat_id, announce_chat=True, reason="Previous stream ended")
        except Exception:
            log.exception("on_stream_end failed")

    async def add_handlers(self) -> None:
        @self.calls.on_update()
        async def stream_updates(_, update):
            try:
                if isinstance(update, StreamEnded):
                    await self.on_stream_end(update.chat_id)
            except Exception:
                log.exception("Stream update handler failed")

        @self.bot.on_message(filters.command(["start"]) & (filters.private | filters.group))
        async def start_handler(_, message: Message):
            try:
                await safe_send(
                    message,
                    self.start_text(),
                    reply_markup=self.start_keyboard(),
                    disable_web_page_preview=True,
                )
            except Exception:
                log.exception("start_handler failed")

        @self.bot.on_callback_query()
        async def panel_callbacks(_, query):
            try:
                data = query.data or ""
                if data == "panel_home":
                    await query.message.edit_text(self.start_text(), reply_markup=self.start_keyboard(), disable_web_page_preview=True)
                elif data == "panel_help":
                    await query.message.edit_text(self.help_text(), reply_markup=self.start_keyboard(), disable_web_page_preview=True)
                elif data == "panel_commands":
                    await query.message.edit_text(self.commands_text(), reply_markup=self.start_keyboard(), disable_web_page_preview=True)
                await query.answer()
            except Exception:
                log.exception("panel_callbacks failed")

        @self.bot.on_message(filters.command(["help", "commands"]) & (filters.private | filters.group))
        async def help_handler(_, message: Message):
            try:
                cmd = (message.command[0].lower() if getattr(message, "command", None) else "help")
                text = self.help_text() if cmd == "help" else self.commands_text()
                await safe_send(message, text, disable_web_page_preview=True)
            except Exception:
                log.exception("help_handler failed")

        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def ping_handler(_, message: Message):
            try:
                started = time.perf_counter()
                x = await safe_send(message, "<b>🏓 Pinging...</b>")
                taken = (time.perf_counter() - started) * 1000
                if x:
                    await x.edit_text(
                        (
                            f"<b>⚡ {escape_html(self.config.brand_name)} is Online</b>\n"
                            f"<b>Latency:</b> <code>{taken:.2f} ms</code>\n"
                            f"<b>Mode:</b> {'Clone' if self.config.clone_mode else 'Master'}\n"
                            f"<b>Bot ID:</b> <code>{escape_html(self.config.bot_id)}</code>"
                        )
                    )
            except Exception:
                log.exception("ping_handler failed")

        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def play_handler(_, message: Message):
            try:
                query = command_arg(message)
                if not query:
                    return await safe_send(message, "<b>❌ Usage:</b> <code>/play song name</code>")

                processing = await safe_send(message, f"<b>🔎 Searching:</b> <code>{escape_html(query)}</code>")
                try:
                    track = await self.resolve_track(query, mention_user(message))
                except Exception as exc:
                    if processing:
                        return await processing.edit_text(f"<b>❌ Failed:</b> <code>{escape_html(str(exc))}</code>")
                    return

                state = self.get_state(message.chat.id)
                async with self.get_lock(message.chat.id):
                    if state.current is None:
                        try:
                            await self.play_track(message.chat.id, track)
                        except Exception as exc:
                            if processing:
                                return await processing.edit_text(
                                    "<b>❌ VC playback failed.</b>\n"
                                    f"<code>{escape_html(str(exc))}</code>\n\n"
                                    "<b>Tip:</b> Group voice chat/video chat active hona chahiye aur assistant ko permissions chahiye."
                                )
                            return
                        if processing:
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
                    if processing:
                        return await processing.edit_text(
                            (
                                f"<b>📥 Added To Queue</b>\n"
                                f"<b>Title:</b> <a href=\"{escape_html(track.webpage_url)}\">{escape_html(track.title)}</a>\n"
                                f"<b>Duration:</b> {escape_html(track.pretty_duration)}\n"
                                f"<b>Position:</b> <code>{len(state.queue)}</code>"
                            ),
                            disable_web_page_preview=True,
                        )
            except Exception:
                log.exception("play_handler failed")
                await safe_send(message, "<b>❌ Unexpected error while processing /play.</b>")

        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def pause_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                await self.calls.pause(message.chat.id)
                self.get_state(message.chat.id).paused = True
                await safe_send(message, "<b>⏸ Playback paused.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Pause failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["resume"]) & filters.group)
        async def resume_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                await self.calls.resume(message.chat.id)
                self.get_state(message.chat.id).paused = False
                await safe_send(message, "<b>▶️ Playback resumed.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Resume failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["mute"]) & filters.group)
        async def mute_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                await self.calls.mute(message.chat.id)
                self.get_state(message.chat.id).muted = True
                await safe_send(message, "<b>🔇 VC muted.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Mute failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["unmute"]) & filters.group)
        async def unmute_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                await self.calls.unmute(message.chat.id)
                self.get_state(message.chat.id).muted = False
                await safe_send(message, "<b>🔊 VC unmuted.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Unmute failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def skip_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                state = self.get_state(message.chat.id)
                if not state.current and not state.queue:
                    return await safe_send(message, "<b>❌ Queue empty hai.</b>")
                await safe_send(message, "<b>⏭ Skipping current track...</b>")
                state.current = None
                state.paused = False
                await self.play_next(message.chat.id, announce_chat=True, reason="Skipped by admin")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Skip failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["stop", "end"]) & filters.group)
        async def stop_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                state = self.get_state(message.chat.id)
                state.queue.clear()
                state.current = None
                state.paused = False
                state.loop = False
                state.muted = False
                try:
                    await self.calls.leave_call(message.chat.id)
                except Exception:
                    log.exception("leave_call failed in stop")
                await safe_send(message, "<b>⏹ Playback ended and queue cleared.</b>")
            except Exception as exc:
                await safe_send(message, f"<b>❌ Stop failed:</b> <code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def queue_handler(_, message: Message):
            try:
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
                lines.append(f"<b>Paused:</b> {'Yes' if state.paused else 'No'}")
                await safe_send(message, "\n".join(lines), disable_web_page_preview=True)
            except Exception:
                log.exception("queue_handler failed")

        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def loop_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
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
            except Exception:
                log.exception("loop_handler failed")

        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def shuffle_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                state = self.get_state(message.chat.id)
                if len(state.queue) < 2:
                    return await safe_send(message, "<b>❌ Shuffle ke liye kam se kam 2 tracks chahiye.</b>")
                random.shuffle(state.queue)
                await safe_send(message, "<b>🔀 Queue shuffled.</b>")
            except Exception:
                log.exception("shuffle_handler failed")

        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def clearqueue_handler(_, message: Message):
            try:
                if not await self.is_admin(message.chat.id, getattr(message.from_user, "id", None)):
                    return await safe_send(message, "<b>❌ Sirf admins use kar sakte hain.</b>")
                state = self.get_state(message.chat.id)
                count = len(state.queue)
                state.queue.clear()
                await safe_send(message, f"<b>🧹 Cleared {count} queued tracks.</b>")
            except Exception:
                log.exception("clearqueue_handler failed")

        @self.bot.on_message(filters.command(["np", "now"]) & filters.group)
        async def np_handler(_, message: Message):
            try:
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
                        f"<b>Paused:</b> {'Yes' if state.paused else 'No'}\n"
                        f"<b>Muted:</b> {'Yes' if state.muted else 'No'}"
                    ),
                    disable_web_page_preview=True,
                )
            except Exception:
                log.exception("np_handler failed")

        if self.is_master:
            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def clone_handler(_, message: Message):
                try:
                    if getattr(message.from_user, "id", None) != self.config.owner_id:
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
                except Exception:
                    log.exception("clone_handler failed")

            @self.bot.on_message(filters.command(["cancel"]) & filters.private)
            async def cancel_handler(_, message: Message):
                try:
                    if getattr(message.from_user, "id", None) != self.config.owner_id:
                        return
                    self.clone_flow.pop(message.from_user.id, None)
                    await safe_send(message, "<b>🛑 Current clone flow cancelled.</b>")
                except Exception:
                    log.exception("cancel_handler failed")

            @self.bot.on_message(filters.command(["clones"]) & filters.private)
            async def clones_handler(_, message: Message):
                try:
                    if getattr(message.from_user, "id", None) != self.config.owner_id:
                        return await safe_send(message, "<b>❌ Owner only command.</b>")
                    files = sorted(CLONES_DIR.glob("*.json"))
                    if not files:
                        return await safe_send(message, "<b>📭 No clone configs found.</b>")
                    lines = ["<b>📦 Saved Clones</b>"]
                    for f in files[:50]:
                        try:
                            cfg = load_config(f)
                            lines.append(
                                f"• <code>{escape_html(cfg.bot_id)}</code> - {escape_html(cfg.owner_username)} - {escape_html(cfg.support_chat)}"
                            )
                        except Exception:
                            lines.append(f"• <code>{escape_html(f.name)}</code>")
                    await safe_send(message, "\n".join(lines))
                except Exception:
                    log.exception("clones_handler failed")

            @self.bot.on_message(filters.private & filters.text)
            async def clone_flow_handler(_, message: Message):
                try:
                    if getattr(message.from_user, "id", None) != self.config.owner_id:
                        return
                    state = self.clone_flow.get(message.from_user.id)
                    if not state:
                        return

                    text = (message.text or "").strip()
                    step = state.get("step")

                    if text.lower() in {"/cancel", "/clone", "/clones"}:
                        return

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
                                stdin=subprocess.DEVNULL,
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
                                f"<b>PID:</b> <code>{proc.pid}</code>"
                            ),
                        )
                except Exception:
                    log.exception("clone_flow_handler failed")
                    await safe_send(message, "<b>❌ Clone create karte time error aaya.</b>")

    async def start(self) -> None:
        if shutil.which("ffmpeg") is None:
            log.warning("ffmpeg not found in PATH. Playback may fail.")

        await self.assistant.start()
        await self.bot.start()
        await self.calls.start()

        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name = me.first_name or self.config.brand_name

        await self.add_handlers()
        log.info("RUNNING | %s | @%s | clone=%s", self.bot_name, self.bot_username, self.config.clone_mode)
        await idle()

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        for name, coro in (
            ("calls.stop", self.calls.stop()),
            ("bot.stop", self.bot.stop()),
            ("assistant.stop", self.assistant.stop()),
        ):
            try:
                await coro
            except Exception:
                log.exception("%s failed", name)


# =========================================================
# SUPERVISOR
# =========================================================
async def run_once() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "--config":
        cfg = load_config(Path(sys.argv[2]).resolve())
        app = TelegramMusicBot(cfg, config_path=Path(sys.argv[2]).resolve(), is_master=False)
    else:
        master_cfg = BotConfig(
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=MAIN_BOT_TOKEN,
            owner_id=OWNER_ID,
            assistant_session=DEFAULT_ASSISTANT_SESSION,
            support_chat=normalize_support(MASTER_SUPPORT_CHAT),
            owner_username=normalize_owner_username(MASTER_OWNER_USERNAME),
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


async def supervisor() -> None:
    restart_delay = int(os.getenv("CLONE_RESTART_DELAY", "5") or "5")
    max_restart_delay = int(os.getenv("MAX_RESTART_DELAY", "60") or "60")
    attempt = 0

    while True:
        try:
            await run_once()
            log.warning("Bot stopped normally. Restarting in %ss.", restart_delay)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            attempt += 1
            log.error("Unhandled fatal error on attempt %s: %s", attempt, exc)
            traceback.print_exc()

        await asyncio.sleep(restart_delay)
        restart_delay = min(max_restart_delay, restart_delay + 5)


def _handle_signal(signum, frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    try:
        asyncio.run(supervisor())
    except KeyboardInterrupt:
        log.info("Shutdown requested, exiting...")

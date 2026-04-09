#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
from typing import Any, Dict, List, Optional, Tuple

# =========================================================
# LOCAL .ENV LOADER
# =========================================================

def load_local_env() -> None:
    candidates: List[Path] = []
    custom_env = os.getenv("ENV_FILE", "").strip()
    if custom_env:
        candidates.append(Path(custom_env).expanduser())
    candidates.append(Path(__file__).resolve().with_name(".env"))

    env_path = next((p for p in candidates if p.exists() and p.is_file()), None)
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
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_local_env()

# =========================================================
# BOOTSTRAP / AUTO INSTALL
# =========================================================

REQUIRED_PACKAGES = {
    "pyrogram": "pyrogram>=2.0.106",
    "tgcrypto": "tgcrypto>=1.2.5",
    "pytgcalls": "py-tgcalls>=2.2.0",
    "yt_dlp": "yt-dlp>=2025.3.31",
}


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def ensure_python_packages() -> None:
    if not env_bool("AUTO_INSTALL_DEPS", True):
        return

    missing = []
    for module_name, pip_name in REQUIRED_PACKAGES.items():
        if importlib.util.find_spec(module_name) is None:
            missing.append(pip_name)

    if not missing:
        return

    print(f"[BOOT] Installing missing packages: {', '.join(missing)}", flush=True)
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--no-cache-dir", "-U", *missing]
    )


ensure_python_packages()

# =========================================================
# SAFE IMPORTS
# =========================================================

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
import pyrogram.errors as pyro_errors

try:
    from pyrogram.errors import (
        FloodWait,
        UserAlreadyParticipant,
        UserNotParticipant,
        RPCError,
        Forbidden,
        BadRequest,
    )
except Exception:
    from pyrogram.errors import FloodWait, UserAlreadyParticipant  # type: ignore
    UserNotParticipant = Exception  # type: ignore
    RPCError = Exception  # type: ignore
    Forbidden = Exception  # type: ignore
    BadRequest = Exception  # type: ignore

if hasattr(pyro_errors, "GroupcallForbidden"):
    GroupcallForbidden = pyro_errors.GroupcallForbidden
else:
    class GroupcallForbidden(Forbidden):  # type: ignore
        ID = "GROUPCALL_FORBIDDEN"
        MESSAGE = "The group call is not accessible."
    pyro_errors.GroupcallForbidden = GroupcallForbidden  # type: ignore

from pytgcalls import PyTgCalls
from yt_dlp import YoutubeDL

AudioPiped = None
StreamEndedCompat = None
StreamAudioEndedCompat = None

try:
    from pytgcalls.types.input_stream import AudioPiped  # type: ignore
except Exception:
    try:
        from pytgcalls.types.input_stream.quality import AudioPiped  # type: ignore
    except Exception:
        AudioPiped = None

try:
    from pytgcalls.types import StreamEnded as StreamEndedCompat  # type: ignore
except Exception:
    StreamEndedCompat = None

try:
    from pytgcalls.types.stream import StreamAudioEnded as StreamAudioEndedCompat  # type: ignore
except Exception:
    StreamAudioEndedCompat = None

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
BOT_BRAND_NAME = os.getenv("BOT_BRAND_NAME", "ZUDO X MUSIC")
BOT_BRAND_TAGLINE = os.getenv("BOT_BRAND_TAGLINE", "Fast • Stable • Smooth VC Player")
NUBCODER_TOKEN = os.getenv("NUBCODER_TOKEN", "")

ROOT_RUNTIME_DIR = Path(
    os.getenv("RUNTIME_DIR", str(Path(__file__).resolve().parent / "runtime"))
).resolve()
ROOT_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

CLONES_DIR = ROOT_RUNTIME_DIR / "clones"
CLONES_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = ROOT_RUNTIME_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

PIDS_DIR = ROOT_RUNTIME_DIR / "pids"
PIDS_DIR.mkdir(parents=True, exist_ok=True)

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
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")

VOICE_CHAT_ERROR_MARKERS = {
    "GROUPCALL_FORBIDDEN",
    "GROUPCALL_ALREADY_STARTED",
    "GROUPCALL_NOT_FOUND",
    "CHAT_ADMIN_REQUIRED",
    "CHAT_ADMIN_INVITE_REQUIRED",
    "INVITE_HASH_EXPIRED",
    "PARTICIPANT_JOIN_MISSING",
    "PEER_ID_INVALID",
    "CHAT_WRITE_FORBIDDEN",
    "CHANNEL_PUBLIC_GROUP_NA",
    "CHAT_FORBIDDEN",
    "VOICE CHAT",
    "VIDEO CHAT",
    "NO ACTIVE GROUP CALL",
    "NOT IN CALL",
    "ALREADY ENDED",
    "JOIN AS PEER INVALID",
    "GROUPCALL_JOIN_MISSING",
    "CALL_PROTOCOL",
    "YOU MUST BE ADMIN",
    "ANONYMOUS ADMIN",
}

def is_url(text: str) -> bool:
    return bool(URL_RE.match((text or "").strip()))

def escape_html(text: str) -> str:
    return html.escape(str(text or ""), quote=True)

def normalize_support(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("https://t.me/"):
        value = "@" + value.split("https://t.me/", 1)[1].strip("/")
    elif value.startswith("http://t.me/"):
        value = "@" + value.split("http://t.me/", 1)[1].strip("/")
    elif value.startswith("t.me/"):
        value = "@" + value.split("t.me/", 1)[1].strip("/")
    elif value and not value.startswith("@") and USERNAME_RE.fullmatch(value):
        value = "@" + value
    return value or "@support"

def normalize_owner_username(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1].strip("/")
    elif value.startswith("http://t.me/"):
        value = value.split("http://t.me/", 1)[1].strip("/")
    elif value.startswith("t.me/"):
        value = value.split("t.me/", 1)[1].strip("/")
    if value and not value.startswith("@"):
        value = "@" + value
    return value or "@owner"

def mention_user(message: Message) -> str:
    user = message.from_user
    if not user:
        return "Unknown"
    name = user.first_name or user.username or "User"
    return escape_html(name)

def command_arg(message: Message) -> str:
    text = message.text or message.caption or ""
    parts = text.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""

def exc_text(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}".strip()

def is_voice_chat_error(exc: Exception) -> bool:
    text = exc_text(exc).upper()
    return any(marker in text for marker in VOICE_CHAT_ERROR_MARKERS)

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

def load_config(path: Path) -> BotConfig:
    return BotConfig(**json.loads(path.read_text(encoding="utf-8")))

def save_config(cfg: BotConfig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(cfg), indent=2, ensure_ascii=False), encoding="utf-8")

def is_admin_status(status: Any) -> bool:
    return status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}

def user_to_username(value: str) -> str:
    value = (value or "").strip()
    return value[1:].lower() if value.startswith("@") else value.lower()

def human_bool(value: bool) -> str:
    return "On" if value else "Off"

def pretty_uptime(seconds: int) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

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
# CORE BOT
# =========================================================

class TelegramMusicBot:
    def __init__(self, config: BotConfig, config_path: Optional[Path] = None, is_master: bool = False):
        validate_config(config)

        self.config = config
        self.config_path = config_path
        self.is_master = is_master
        self.start_time = time.time()

        self.bot_storage = ROOT_RUNTIME_DIR / f"bot_{config.bot_id}"
        self.bot_storage.mkdir(parents=True, exist_ok=True)

        self.data_dir = self.bot_storage / "data"
        self.cache_dir = self.bot_storage / "cache"
        self.downloads_dir = self.bot_storage / "downloads"
        self.settings_dir = self.bot_storage / "settings"
        self.runtime_dir = self.bot_storage / "runtime"
        self.local_logs_dir = self.bot_storage / "logs"

        for p in [self.data_dir, self.cache_dir, self.downloads_dir, self.settings_dir, self.runtime_dir, self.local_logs_dir]:
            p.mkdir(parents=True, exist_ok=True)

        self.settings_path = self.settings_dir / "settings.json"
        self.settings = self.load_settings()

        self.bot = Client(
            name=f"bot_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            bot_token=config.bot_token,
            workdir=str(self.data_dir),
            in_memory=False,
            parse_mode=ParseMode.HTML,
        )

        self.assistant = Client(
            name=f"assistant_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            session_string=config.assistant_session,
            workdir=str(self.data_dir),
            in_memory=False,
            no_updates=True,
            parse_mode=ParseMode.HTML,
        )

        self.calls = PyTgCalls(self.assistant)
        self.states: Dict[int, ChatState] = {}
        self.chat_locks: Dict[int, asyncio.Lock] = {}
        self.clone_flow: Dict[int, Dict[str, str]] = {}
        self.pending_start_photo: Dict[int, float] = {}

        self.bot_username: str = ""
        self.bot_name: str = config.brand_name
        self.bot_id_int: int = 0
        self.assistant_id: int = 0
        self.assistant_name: str = "Assistant"
        self._stopping = False

    # -----------------------------------------------------
    # PERSISTENT SETTINGS
    # -----------------------------------------------------

    def load_settings(self) -> Dict[str, Any]:
        if not self.settings_path.exists():
            return {"start_photo_file_id": ""}
        try:
            data = json.loads(self.settings_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {"start_photo_file_id": ""}
            data.setdefault("start_photo_file_id", "")
            return data
        except Exception:
            return {"start_photo_file_id": ""}

    def save_settings(self) -> None:
        self.settings_path.write_text(
            json.dumps(self.settings, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    # -----------------------------------------------------
    # STATE HELPERS
    # -----------------------------------------------------

    def get_state(self, chat_id: int) -> ChatState:
        if chat_id not in self.states:
            self.states[chat_id] = ChatState()
        return self.states[chat_id]

    def get_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        return self.chat_locks[chat_id]

    # -----------------------------------------------------
    # UI / TEXT
    # -----------------------------------------------------

    @property
    def support_url(self) -> str:
        return f"https://t.me/{self.config.support_chat.lstrip('@')}"

    @property
    def owner_url(self) -> str:
        return f"https://t.me/{self.config.owner_username.lstrip('@')}"

    @property
    def add_to_group_url(self) -> str:
        if self.bot_username:
            return f"https://t.me/{self.bot_username}?startgroup=true"
        return "https://t.me"

    def start_text(self) -> str:
        name = escape_html(self.config.brand_name)
        tagline = escape_html(self.config.tagline)
        owner = escape_html(self.config.owner_username)
        return (
            f"🎧 <b>{name}</b>\n"
            f"✨ <i>{tagline}</i>\n\n"
            f"◎ This is <b>{name}</b> — fast & powerful Telegram music bot.\n"
            f"◎ Smooth beats • stable & seamless music flow.\n"
            f"◎ Smart search, queue, controls and stylish inline panels.\n"
            f"◎ Click on <b>Help & Commands</b> to explore all modules.\n\n"
            f"👤 Owner: {owner}\n"
            f"💬 Support: {escape_html(self.config.support_chat)}"
        )

    def about_text(self) -> str:
        return (
            f"✨ <b>About {escape_html(self.config.brand_name)}</b>\n\n"
            f"• Smooth VC playback engine\n"
            f"• Better error handling\n"
            f"• Smart queue, loop, shuffle\n"
            f"• Inline help explorer\n"
            f"• Friendly admin/permission diagnostics\n"
            f"• Startup photo support with /setdp\n\n"
            f"Use in group:\n"
            f"1. Add bot + assistant\n"
            f"2. Start voice chat\n"
            f"3. Send /play song name"
        )

    def help_home_text(self) -> str:
        return (
            f"📚 <b>{escape_html(self.config.brand_name)} Help Panel</b>\n\n"
            f"Choose a section below and explore commands properly.\n"
            f"Everything is split into categories so it doesn't look messy.\n\n"
            f"Tip: In group, use /play song name"
        )

    def help_music_text(self) -> str:
        return (
            f"🎵 <b>Music Commands</b>\n\n"
            f"/play <i>song name / url</i> — search or direct play\n"
            f"/p — alias of /play\n"
            f"/pause — pause current song\n"
            f"/resume — resume paused song\n"
            f"/skip — skip current track\n"
            f"/next — alias of /skip\n"
            f"/stop — stop playback & clear current VC stream\n"
            f"/end — alias of /stop\n"
            f"/queue — show queue list\n"
            f"/q — alias of /queue\n"
            f"/np — now playing panel\n"
            f"/now — alias of /np"
        )

    def help_admin_text(self) -> str:
        return (
            f"🛠 <b>Admin Controls</b>\n\n"
            f"/loop — toggle loop mode\n"
            f"/loop on — enable loop\n"
            f"/loop off — disable loop\n"
            f"/shuffle — shuffle queue\n"
            f"/clearqueue — clear queued tracks\n"
            f"/mute — mute VC audio\n"
            f"/unmute — unmute VC audio\n"
            f"/ping — bot speed/status\n"
            f"/alive — bot online check\n\n"
            f"Note: Admin-only controls require group admin."
        )

    def help_extra_text(self) -> str:
        return (
            f"🧩 <b>Extra Info</b>\n\n"
            f"• Bot should be admin for smooth management.\n"
            f"• Assistant should be in group and ideally admin with voice chat rights.\n"
            f"• Voice chat/video chat must be active before /play.\n"
            f"• If group is private, invite access must work.\n"
            f"• If permissions were fixed later, just try /play again — bot rechecks fresh."
        )

    def shell_help_text(self) -> str:
        return (
            f"🔐 <b>Hidden Owner / Sudo Panel</b>\n\n"
            f"/shelp — hidden commands\n"
            f"/setdp — set startup photo\n"
            f"/removedp — remove startup photo\n"
            f"/clone — start new bot setup flow\n"
            f"/cancel — cancel current setup flow\n"
            f"/clones — list saved clone configs\n\n"
            f"Only allowed for configured owner."
        )

    def start_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ Add Me In Your Group ➕", url=self.add_to_group_url)],
                [
                    InlineKeyboardButton("👑 Owner", url=self.owner_url),
                    InlineKeyboardButton("📖 About", callback_data="nav_about"),
                ],
                [
                    InlineKeyboardButton("💬 Support", url=self.support_url),
                    InlineKeyboardButton("✨ Update", url=self.support_url),
                ],
                [InlineKeyboardButton("📚 Help And Commands", callback_data="nav_help_home")],
            ]
        )

    def help_home_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🎵 Music", callback_data="help_music"),
                    InlineKeyboardButton("🛠 Admin", callback_data="help_admin"),
                ],
                [
                    InlineKeyboardButton("🧩 Extra", callback_data="help_extra"),
                    InlineKeyboardButton("📖 About", callback_data="nav_about"),
                ],
                [
                    InlineKeyboardButton("🏠 Home", callback_data="nav_home"),
                    InlineKeyboardButton("❌ Close", callback_data="nav_close"),
                ],
            ]
        )

    def subpage_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⬅ Back", callback_data="nav_help_home"),
                    InlineKeyboardButton("🏠 Home", callback_data="nav_home"),
                ],
                [InlineKeyboardButton("❌ Close", callback_data="nav_close")],
            ]
        )

    def np_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⏸ Pause", callback_data="ctl_pause"),
                    InlineKeyboardButton("▶ Resume", callback_data="ctl_resume"),
                ],
                [
                    InlineKeyboardButton("⏭ Skip", callback_data="ctl_skip"),
                    InlineKeyboardButton("⏹ Stop", callback_data="ctl_stop"),
                ],
                [
                    InlineKeyboardButton("📜 Queue", callback_data="ctl_queue"),
                    InlineKeyboardButton("🔄 Refresh", callback_data="ctl_np"),
                ],
            ]
        )

    def queue_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🔀 Shuffle", callback_data="ctl_shuffle"),
                    InlineKeyboardButton("🧹 Clear", callback_data="ctl_clearqueue"),
                ],
                [
                    InlineKeyboardButton("🎵 Now Playing", callback_data="ctl_np"),
                    InlineKeyboardButton("🏠 Home", callback_data="nav_home"),
                ],
            ]
        )

    # -----------------------------------------------------
    # SAFE SEND / EDIT
    # -----------------------------------------------------

    async def safe_send(self, message: Message, text: str, **kwargs):
        try:
            return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
        except Exception:
            log.exception("safe_send failed")
            return None

    async def safe_edit_text(self, msg: Optional[Message], text: str, **kwargs):
        if not msg:
            return None
        try:
            return await msg.edit_text(text, disable_web_page_preview=True, **kwargs)
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            return await msg.edit_text(text, disable_web_page_preview=True, **kwargs)
        except Exception:
            log.exception("safe_edit_text failed")
            return None

    async def safe_edit_panel(self, msg: Optional[Message], text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
        if not msg:
            return None
        try:
            if getattr(msg, "photo", None):
                return await msg.edit_caption(caption=text, reply_markup=reply_markup)
            return await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            try:
                if getattr(msg, "photo", None):
                    return await msg.edit_caption(caption=text, reply_markup=reply_markup)
                return await msg.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
            except Exception:
                log.exception("safe_edit_panel retry failed")
                return None
        except Exception:
            log.exception("safe_edit_panel failed")
            return None

    async def send_start_panel(self, message: Message):
        photo_id = (self.settings.get("start_photo_file_id") or "").strip()
        if photo_id:
            try:
                return await message.reply_photo(
                    photo=photo_id,
                    caption=self.start_text(),
                    reply_markup=self.start_keyboard(),
                )
            except Exception:
                log.exception("send_start_panel photo failed; fallback text")
        return await self.safe_send(message, self.start_text(), reply_markup=self.start_keyboard())

    # -----------------------------------------------------
    # AUTH
    # -----------------------------------------------------

    def is_owner_username(self, username: str) -> bool:
        if not username:
            return False
        return user_to_username(username) == user_to_username(self.config.owner_username)

    def is_config_owner_user(self, message: Message) -> bool:
        user = message.from_user
        if not user:
            return False
        if user.id == self.config.owner_id:
            return True
        if user.username and self.is_owner_username(user.username):
            return True
        return False

    async def is_admin(self, chat_id: int, user_id: Optional[int]) -> bool:
        if not user_id:
            return False
        if user_id == self.config.owner_id:
            return True
        try:
            member = await self.bot.get_chat_member(chat_id, user_id)
            return is_admin_status(member.status)
        except Exception:
            log.exception("Failed to check admin")
            return False

    async def require_admin(self, message: Message) -> bool:
        ok = await self.is_admin(message.chat.id, getattr(message.from_user, "id", None))
        if not ok:
            await self.safe_send(message, "❌ Ye control sirf group admins use kar sakte hain.")
        return ok

    # -----------------------------------------------------
    # TRACK RESOLUTION
    # -----------------------------------------------------

    async def resolve_track(self, query: str, requested_by: str) -> Track:
        track = await asyncio.to_thread(sync_extract_track, query)
        track.requested_by = requested_by
        return track

    # -----------------------------------------------------
    # CHAT / RIGHTS / DIAGNOSTICS
    # -----------------------------------------------------

    async def bot_member_info(self, chat_id: int):
        try:
            return await self.bot.get_chat_member(chat_id, self.bot_id_int)
        except Exception:
            return None

    async def assistant_member_info(self, chat_id: int):
        try:
            return await self.bot.get_chat_member(chat_id, self.assistant_id)
        except Exception:
            return None

    async def build_join_link(self, chat_id: int) -> Tuple[Optional[str], Optional[str]]:
        """
        Return (link, reason_if_failed)
        """
        try:
            chat = await self.bot.get_chat(chat_id)
            if getattr(chat, "username", None):
                return f"https://t.me/{chat.username}", None
        except Exception:
            pass

        try:
            link = await self.bot.export_chat_invite_link(chat_id)
            if link:
                return link, None
        except Exception as exc:
            return None, exc_text(exc)

        return None, "No public username and invite link export failed"

    async def ensure_assistant_in_chat(self, chat_id: int) -> Tuple[bool, Optional[str]]:
        """
        Ensures assistant is inside group if possible.
        """
        member = await self.assistant_member_info(chat_id)
        if member:
            return True, None

        link, reason = await self.build_join_link(chat_id)
        if not link:
            return (
                False,
                "Assistant abhi group me nahi hai aur join link bhi generate nahi ho pa raha.\n"
                "Bot ko admin banao aur <b>Invite Users</b> permission do, ya group ko public username do."
                + (f"\n\nDebug: <code>{escape_html(reason or 'unknown')}</code>" if reason else "")
            )

        try:
            await self.assistant.join_chat(link)
            await asyncio.sleep(1.5)
            member = await self.assistant_member_info(chat_id)
            if member:
                return True, None
            return False, "Assistant join try hua tha, lekin confirm nahi ho paaya. Ek baar /play dobara try karo."
        except UserAlreadyParticipant:
            return True, None
        except Exception as exc:
            return (
                False,
                "Assistant group me join nahi ho paaya.\n"
                "Check karo ki link valid hai, bot/assistant pe restrictions nahi hain, aur group access open hai.\n\n"
                f"Reason: <code>{escape_html(str(exc))}</code>"
            )

    async def diagnose_voice_issue(self, chat_id: int, exc: Exception) -> str:
        text = exc_text(exc).upper()

        bot_member = await self.bot_member_info(chat_id)
        assistant_member = await self.assistant_member_info(chat_id)

        if not bot_member or not is_admin_status(getattr(bot_member, "status", None)):
            return (
                "Bot admin nahi hai, isliye main group ko properly manage nahi kar paa raha.\n"
                "Bot ko admin banao. Best rahega agar <b>Invite Users</b> aur basic management rights bhi do."
            )

        if "NO ACTIVE GROUP CALL" in text or "GROUPCALL_NOT_FOUND" in text or "VOICE CHAT" in text or "VIDEO CHAT" in text:
            return (
                "Abhi is group me active voice chat / video chat start nahi hai.\n"
                "Pehle voice chat start karo, phir <b>/play</b> use karo."
            )

        if "GROUPCALL_FORBIDDEN" in text or "ALREADY ENDED" in text:
            return (
                "Voice chat accessible nahi hai ya pichla call khatam ho chuka hai.\n"
                "Voice chat ko dubara start karke /play chalao."
            )

        if not assistant_member:
            link, reason = await self.build_join_link(chat_id)
            if not link:
                return (
                    "Assistant group me maujood nahi hai aur join link bhi nahi mil raha.\n"
                    "Bot ko <b>Invite Users</b> permission do ya group ko public banao.\n\n"
                    f"Reason: <code>{escape_html(reason or 'unknown')}</code>"
                )
            return (
                "Assistant abhi group me properly add/join nahi hua lag raha.\n"
                "Ab permissions sahi hain to ek baar <b>/play</b> dobara chalao."
            )

        if "CHAT_ADMIN_REQUIRED" in text or "YOU MUST BE ADMIN" in text:
            return (
                "Assistant ke paas required admin rights nahi hain.\n"
                "Assistant ko admin banao aur <b>Manage Voice Chats / Video Chats</b> rights do."
            )

        if "CHAT_ADMIN_INVITE_REQUIRED" in text or "INVITE_HASH_EXPIRED" in text:
            return (
                "Invite access me problem aa rahi hai.\n"
                "Bot ke paas invite/export access hona chahiye ya group public username ke saath reachable hona chahiye."
            )

        if "PARTICIPANT_JOIN_MISSING" in text or "GROUPCALL_JOIN_MISSING" in text:
            return (
                "Assistant voice chat me properly attach nahi ho paaya.\n"
                "VC active rakho, assistant rights check karo, aur /play dobara use karo."
            )

        return (
            "Playback start nahi ho paaya.\n"
            "Voice chat active rakho, bot + assistant rights check karo, phir dobara try karo.\n\n"
            f"Raw reason: <code>{escape_html(str(exc))}</code>"
        )

    # -----------------------------------------------------
    # PLAYBACK LAYER
    # -----------------------------------------------------

    async def _play_via_pytgcalls(self, chat_id: int, stream_url: str) -> None:
        attempts = []

        if hasattr(self.calls, "play"):
            if AudioPiped is not None:
                attempts.append(("play_audio_piped", lambda: self.calls.play(chat_id, AudioPiped(stream_url))))
            attempts.append(("play_direct_url", lambda: self.calls.play(chat_id, stream_url)))
            attempts.append(("play_direct_url_audio_kw", lambda: self.calls.play(chat_id, stream_url, stream_type="audio")))

        if hasattr(self.calls, "join_group_call") and AudioPiped is not None:
            attempts.append(("join_group_call_audio_piped", lambda: self.calls.join_group_call(chat_id, AudioPiped(stream_url))))

        last_error = None
        for label, fn in attempts:
            try:
                result = fn()
                if asyncio.iscoroutine(result):
                    await result
                log.info("Playback started using %s", label)
                return
            except TypeError as exc:
                last_error = exc
                continue
            except Exception as exc:
                last_error = exc
                break

        if last_error:
            raise last_error
        raise RuntimeError("No compatible playback method worked")

    async def play_track(self, chat_id: int, track: Track) -> None:
        ok, reason = await self.ensure_assistant_in_chat(chat_id)
        if not ok:
            raise RuntimeError(reason or "Assistant join issue")

        # Fresh attempt 1
        try:
            await self._play_via_pytgcalls(chat_id, track.stream_url)
        except Exception as first_exc:
            # self-heal attempt after rights fixed or stale call state
            try:
                if hasattr(self.calls, "leave_call"):
                    result = self.calls.leave_call(chat_id)
                    if asyncio.iscoroutine(result):
                        await result
                elif hasattr(self.calls, "leave_group_call"):
                    result = self.calls.leave_group_call(chat_id)
                    if asyncio.iscoroutine(result):
                        await result
            except Exception:
                pass

            await asyncio.sleep(1.2)
            ok, reason = await self.ensure_assistant_in_chat(chat_id)
            if not ok:
                raise RuntimeError(reason or "Assistant join issue") from first_exc

            try:
                await self._play_via_pytgcalls(chat_id, track.stream_url)
            except Exception as second_exc:
                friendly = await self.diagnose_voice_issue(chat_id, second_exc)
                raise RuntimeError(friendly) from second_exc

        state = self.get_state(chat_id)
        state.current = track
        state.paused = False
        state.muted = False

    async def leave_call_safely(self, chat_id: int):
        try:
            if hasattr(self.calls, "leave_call"):
                result = self.calls.leave_call(chat_id)
                if asyncio.iscoroutine(result):
                    await result
                return
        except Exception:
            pass

        try:
            if hasattr(self.calls, "leave_group_call"):
                result = self.calls.leave_group_call(chat_id)
                if asyncio.iscoroutine(result):
                    await result
        except Exception:
            pass

    async def pause_call_safely(self, chat_id: int):
        if hasattr(self.calls, "pause"):
            result = self.calls.pause(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        if hasattr(self.calls, "pause_stream"):
            result = self.calls.pause_stream(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        raise RuntimeError("Pause method unavailable in this build")

    async def resume_call_safely(self, chat_id: int):
        if hasattr(self.calls, "resume"):
            result = self.calls.resume(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        if hasattr(self.calls, "resume_stream"):
            result = self.calls.resume_stream(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        raise RuntimeError("Resume method unavailable in this build")

    async def mute_call_safely(self, chat_id: int):
        if hasattr(self.calls, "mute"):
            result = self.calls.mute(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        raise RuntimeError("Mute method unavailable in this build")

    async def unmute_call_safely(self, chat_id: int):
        if hasattr(self.calls, "unmute"):
            result = self.calls.unmute(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        raise RuntimeError("Unmute method unavailable in this build")

    # -----------------------------------------------------
    # NOW PLAYING / QUEUE PANEL
    # -----------------------------------------------------

    def now_playing_text(self, state: ChatState) -> str:
        if not state.current:
            return "❌ Abhi kuch play nahi ho raha."
        t = state.current
        return (
            f"🎵 <b>Now Playing</b>\n\n"
            f"🏷 <b>Title:</b> {escape_html(t.title)}\n"
            f"⏱ <b>Duration:</b> {escape_html(t.pretty_duration)}\n"
            f"🌐 <b>Source:</b> {escape_html(t.source)}\n"
            f"🙋 <b>Requested By:</b> {t.requested_by}\n"
            f"🔁 <b>Loop:</b> {human_bool(state.loop)}\n"
            f"⏸ <b>Paused:</b> {human_bool(state.paused)}\n"
            f"🔇 <b>Muted:</b> {human_bool(state.muted)}"
        )

    def queue_text(self, state: ChatState) -> str:
        if not state.current and not state.queue:
            return "📭 Queue empty hai."
        lines = ["📜 <b>Queue Panel</b>\n"]
        if state.current:
            lines.append(
                f"🎵 <b>Now:</b> {escape_html(state.current.title)} "
                f"(<code>{escape_html(state.current.pretty_duration)}</code>)"
            )
        if state.queue:
            lines.append("\n<b>Up Next:</b>")
            for i, track in enumerate(state.queue[:15], start=1):
                lines.append(
                    f"{i}. {escape_html(track.title)} — <code>{escape_html(track.pretty_duration)}</code>"
                )
            if len(state.queue) > 15:
                lines.append(f"... and {len(state.queue) - 15} more")
        lines.append(f"\n🔁 Loop: <b>{human_bool(state.loop)}</b>")
        lines.append(f"⏸ Paused: <b>{human_bool(state.paused)}</b>")
        return "\n".join(lines)

    # -----------------------------------------------------
    # NEXT / STREAM END
    # -----------------------------------------------------

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
                    await self.leave_call_safely(chat_id)
                except Exception:
                    pass
                return

            try:
                await self.play_track(chat_id, next_track)
            except Exception as exc:
                log.warning("play_next failed for chat %s: %s", chat_id, exc_text(exc))
                state.current = None
                state.paused = False
                state.muted = False
                if announce_chat:
                    try:
                        await self.bot.send_message(
                            chat_id,
                            f"❌ Next track play nahi ho saka.\n\n{escape_html(str(exc))}",
                        )
                    except Exception:
                        pass
                return

            if announce_chat:
                try:
                    text = (
                        f"▶️ <b>Now Playing</b>\n\n"
                        f"🏷 <b>Title:</b> {escape_html(next_track.title)}\n"
                        f"⏱ <b>Duration:</b> {escape_html(next_track.pretty_duration)}\n"
                        f"🙋 <b>Requested By:</b> {next_track.requested_by}"
                    )
                    if reason:
                        text += f"\n📝 <b>Reason:</b> {escape_html(reason)}"
                    await self.bot.send_message(
                        chat_id,
                        text,
                        disable_web_page_preview=True,
                        reply_markup=self.np_keyboard(),
                    )
                except Exception:
                    pass

    async def on_stream_end(self, chat_id: int) -> None:
        try:
            await self.play_next(chat_id, announce_chat=True, reason="Previous stream ended")
        except Exception:
            log.exception("on_stream_end failed")

    # -----------------------------------------------------
    # HANDLERS
    # -----------------------------------------------------

    async def add_handlers(self) -> None:
        @self.calls.on_update()
        async def stream_updates(_, update):
            try:
                name = type(update).__name__.lower()
                chat_id = getattr(update, "chat_id", None)
                if not chat_id:
                    return

                if StreamEndedCompat is not None and isinstance(update, StreamEndedCompat):
                    await self.on_stream_end(chat_id)
                    return

                if StreamAudioEndedCompat is not None and isinstance(update, StreamAudioEndedCompat):
                    await self.on_stream_end(chat_id)
                    return

                if "ended" in name:
                    await self.on_stream_end(chat_id)
            except Exception:
                log.exception("Stream update handler failed")

        # ---------------- START / HELP PANEL ----------------

        @self.bot.on_message(filters.command(["start"]) & (filters.private | filters.group))
        async def start_handler(_, message: Message):
            try:
                await self.send_start_panel(message)
            except Exception:
                log.exception("start_handler failed")

        @self.bot.on_message(filters.command(["help", "commands"]) & (filters.private | filters.group))
        async def help_handler(_, message: Message):
            try:
                photo_id = (self.settings.get("start_photo_file_id") or "").strip()
                if photo_id:
                    try:
                        await message.reply_photo(
                            photo=photo_id,
                            caption=self.help_home_text(),
                            reply_markup=self.help_home_keyboard(),
                        )
                        return
                    except Exception:
                        pass
                await self.safe_send(message, self.help_home_text(), reply_markup=self.help_home_keyboard())
            except Exception:
                log.exception("help_handler failed")

        @self.bot.on_message(filters.command(["about"]) & (filters.private | filters.group))
        async def about_handler(_, message: Message):
            try:
                await self.safe_send(message, self.about_text(), reply_markup=self.subpage_keyboard())
            except Exception:
                log.exception("about_handler failed")

        @self.bot.on_callback_query()
        async def callback_handler(_, query):
            try:
                data = query.data or ""

                # navigation
                if data == "nav_home":
                    await self.safe_edit_panel(query.message, self.start_text(), self.start_keyboard())
                    return await query.answer()

                if data == "nav_about":
                    await self.safe_edit_panel(query.message, self.about_text(), self.subpage_keyboard())
                    return await query.answer()

                if data == "nav_help_home":
                    await self.safe_edit_panel(query.message, self.help_home_text(), self.help_home_keyboard())
                    return await query.answer()

                if data == "help_music":
                    await self.safe_edit_panel(query.message, self.help_music_text(), self.subpage_keyboard())
                    return await query.answer()

                if data == "help_admin":
                    await self.safe_edit_panel(query.message, self.help_admin_text(), self.subpage_keyboard())
                    return await query.answer()

                if data == "help_extra":
                    await self.safe_edit_panel(query.message, self.help_extra_text(), self.subpage_keyboard())
                    return await query.answer()

                if data == "nav_close":
                    try:
                        await query.message.delete()
                    except Exception:
                        pass
                    return await query.answer("Closed")

                # control callbacks only in group
                if data.startswith("ctl_"):
                    if query.message.chat.type not in {"group", "supergroup"}:
                        return await query.answer("Ye control group ke liye hai.", show_alert=True)

                    user_id = getattr(query.from_user, "id", None)
                    if not await self.is_admin(query.message.chat.id, user_id):
                        return await query.answer("Sirf admins control use kar sakte hain.", show_alert=True)

                    chat_id = query.message.chat.id
                    state = self.get_state(chat_id)

                    if data == "ctl_pause":
                        try:
                            await self.pause_call_safely(chat_id)
                            state.paused = True
                            await query.answer("Playback paused")
                        except Exception as exc:
                            await query.answer((await self.diagnose_voice_issue(chat_id, exc))[:180], show_alert=True)
                        return

                    if data == "ctl_resume":
                        try:
                            await self.resume_call_safely(chat_id)
                            state.paused = False
                            await query.answer("Playback resumed")
                        except Exception as exc:
                            await query.answer((await self.diagnose_voice_issue(chat_id, exc))[:180], show_alert=True)
                        return

                    if data == "ctl_skip":
                        try:
                            if not state.current and not state.queue:
                                return await query.answer("Queue empty hai.", show_alert=True)
                            state.current = None
                            state.paused = False
                            await self.play_next(chat_id, announce_chat=True, reason="Skipped by admin")
                            return await query.answer("Skipped")
                        except Exception as exc:
                            return await query.answer(str(exc)[:180], show_alert=True)

                    if data == "ctl_stop":
                        try:
                            state.queue.clear()
                            state.current = None
                            state.paused = False
                            state.loop = False
                            state.muted = False
                            await self.leave_call_safely(chat_id)
                            try:
                                await self.safe_edit_panel(
                                    query.message,
                                    "⏹ <b>Playback ended</b>\n\nQueue clear kar di gayi hai.",
                                    self.queue_keyboard(),
                                )
                            except Exception:
                                pass
                            return await query.answer("Playback stopped")
                        except Exception as exc:
                            return await query.answer(str(exc)[:180], show_alert=True)

                    if data == "ctl_queue":
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer()

                    if data == "ctl_np":
                        await self.safe_edit_panel(query.message, self.now_playing_text(state), self.np_keyboard())
                        return await query.answer()

                    if data == "ctl_shuffle":
                        if len(state.queue) < 2:
                            return await query.answer("Shuffle ke liye kam se kam 2 queued tracks chahiye.", show_alert=True)
                        random.shuffle(state.queue)
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer("Queue shuffled")

                    if data == "ctl_clearqueue":
                        count = len(state.queue)
                        state.queue.clear()
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer(f"{count} tracks removed")

                await query.answer()
            except Exception:
                log.exception("callback_handler failed")
                try:
                    await query.answer("Something went wrong", show_alert=True)
                except Exception:
                    pass

        # ---------------- BASIC STATUS ----------------

        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def ping_handler(_, message: Message):
            try:
                started = time.perf_counter()
                x = await self.safe_send(message, "🏓 Checking bot speed...")
                taken = (time.perf_counter() - started) * 1000
                uptime = pretty_uptime(int(time.time() - self.start_time))
                active_chats = sum(1 for s in self.states.values() if s.current or s.queue)

                text = (
                    f"⚡ <b>{escape_html(self.config.brand_name)} is online</b>\n\n"
                    f"🏓 <b>Latency:</b> <code>{taken:.2f} ms</code>\n"
                    f"⏳ <b>Uptime:</b> <code>{escape_html(uptime)}</code>\n"
                    f"🎧 <b>Active Chats:</b> <code>{active_chats}</code>\n"
                    f"🤖 <b>Bot ID:</b> <code>{escape_html(self.config.bot_id)}</code>"
                )
                if x:
                    await self.safe_edit_text(x, text)
            except Exception:
                log.exception("ping_handler failed")

                # ---------------- PLAY / MUSIC COMMANDS ----------------

        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def play_handler(_, message: Message):
            try:
                query = command_arg(message)
                if not query:
                    return await self.safe_send(
                        message,
                        "❌ Usage:\n<code>/play song name</code>\nya\n<code>/play youtube_url</code>"
                    )

                processing = await self.safe_send(
                    message,
                    f"🔎 <b>Searching...</b>\n<code>{escape_html(query)}</code>"
                )

                try:
                    track = await self.resolve_track(query, mention_user(message))
                except Exception as exc:
                    return await self.safe_edit_text(
                        processing,
                        f"❌ Song resolve nahi ho paaya.\n\nReason: <code>{escape_html(str(exc))}</code>",
                    )

                state = self.get_state(message.chat.id)

                async with self.get_lock(message.chat.id):
                    if state.current is None:
                        try:
                            await self.play_track(message.chat.id, track)
                        except Exception as exc:
                            friendly = str(exc)
                            return await self.safe_edit_text(
                                processing,
                                f"❌ <b>Playback start nahi ho paaya.</b>\n\n{friendly}",
                            )

                        text = (
                            f"▶️ <b>Playing Now</b>\n\n"
                            f"🏷 <b>Title:</b> {escape_html(track.title)}\n"
                            f"⏱ <b>Duration:</b> {escape_html(track.pretty_duration)}\n"
                            f"🌐 <b>Source:</b> {escape_html(track.source)}\n"
                            f"🙋 <b>Requested By:</b> {track.requested_by}"
                        )
                        return await self.safe_edit_text(
                            processing,
                            text,
                            reply_markup=self.np_keyboard(),
                        )

                    state.queue.append(track)
                    return await self.safe_edit_text(
                        processing,
                        (
                            f"📥 <b>Added To Queue</b>\n\n"
                            f"🏷 <b>Title:</b> {escape_html(track.title)}\n"
                            f"⏱ <b>Duration:</b> {escape_html(track.pretty_duration)}\n"
                            f"📌 <b>Position:</b> <code>{len(state.queue)}</code>"
                        ),
                        reply_markup=self.queue_keyboard(),
                    )

            except Exception:
                log.exception("play_handler failed")
                await self.safe_send(message, "❌ /play process karte time unexpected error aaya.")

        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def pause_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.current:
                    return await self.safe_send(message, "❌ Abhi koi active playback nahi hai.")

                await self.pause_call_safely(message.chat.id)
                state.paused = True
                await self.safe_send(message, "⏸ <b>Playback paused.</b>")

            except Exception as exc:
                await self.safe_send(
                    message,
                    f"❌ Pause failed.\n\n{await self.diagnose_voice_issue(message.chat.id, exc)}"
                )

        @self.bot.on_message(filters.command(["resume"]) & filters.group)
        async def resume_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.current:
                    return await self.safe_send(message, "❌ Abhi koi paused playback nahi hai.")

                await self.resume_call_safely(message.chat.id)
                state.paused = False
                await self.safe_send(message, "▶️ <b>Playback resumed.</b>")

            except Exception as exc:
                await self.safe_send(
                    message,
                    f"❌ Resume failed.\n\n{await self.diagnose_voice_issue(message.chat.id, exc)}"
                )

        @self.bot.on_message(filters.command(["mute"]) & filters.group)
        async def mute_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.current:
                    return await self.safe_send(message, "❌ Abhi kuch play nahi ho raha.")

                await self.mute_call_safely(message.chat.id)
                state.muted = True
                await self.safe_send(message, "🔇 <b>VC muted.</b>")

            except Exception as exc:
                await self.safe_send(
                    message,
                    f"❌ Mute failed.\n\n{await self.diagnose_voice_issue(message.chat.id, exc)}"
                )

        @self.bot.on_message(filters.command(["unmute"]) & filters.group)
        async def unmute_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.current:
                    return await self.safe_send(message, "❌ Abhi kuch play nahi ho raha.")

                await self.unmute_call_safely(message.chat.id)
                state.muted = False
                await self.safe_send(message, "🔊 <b>VC unmuted.</b>")

            except Exception as exc:
                await self.safe_send(
                    message,
                    f"❌ Unmute failed.\n\n{await self.diagnose_voice_issue(message.chat.id, exc)}"
                )

        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def skip_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return

                state = self.get_state(message.chat.id)
                if not state.current and not state.queue:
                    return await self.safe_send(message, "❌ Queue empty hai.")

                await self.safe_send(message, "⏭ <b>Skipping current track...</b>")
                state.current = None
                state.paused = False
                state.muted = False
                await self.play_next(message.chat.id, announce_chat=True, reason="Skipped by admin")

            except Exception as exc:
                await self.safe_send(
                    message,
                    f"❌ Skip failed.\n\n{await self.diagnose_voice_issue(message.chat.id, exc)}"
                )

        @self.bot.on_message(filters.command(["stop", "end"]) & filters.group)
        async def stop_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return

                state = self.get_state(message.chat.id)
                state.queue.clear()
                state.current = None
                state.paused = False
                state.loop = False
                state.muted = False

                await self.leave_call_safely(message.chat.id)
                await self.safe_send(message, "⏹ <b>Playback stopped</b>\nQueue bhi clear kar di gayi hai.")

            except Exception as exc:
                await self.safe_send(message, f"❌ Stop failed.\n\n<code>{escape_html(str(exc))}</code>")

        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def queue_handler(_, message: Message):
            try:
                state = self.get_state(message.chat.id)
                await self.safe_send(message, self.queue_text(state), reply_markup=self.queue_keyboard())
            except Exception:
                log.exception("queue_handler failed")
                await self.safe_send(message, "❌ Queue panel load nahi ho paaya.")

        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def loop_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return

                arg = command_arg(message).lower().strip()
                state = self.get_state(message.chat.id)

                if arg in {"on", "yes", "true", "1"}:
                    state.loop = True
                elif arg in {"off", "no", "false", "0"}:
                    state.loop = False
                else:
                    state.loop = not state.loop

                await self.safe_send(
                    message,
                    f"🔁 <b>Loop { 'enabled' if state.loop else 'disabled' }.</b>"
                )

            except Exception:
                log.exception("loop_handler failed")
                await self.safe_send(message, "❌ Loop update nahi ho paaya.")

        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def shuffle_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return

                state = self.get_state(message.chat.id)
                if len(state.queue) < 2:
                    return await self.safe_send(
                        message,
                        "❌ Shuffle ke liye kam se kam 2 queued tracks chahiye."
                    )

                random.shuffle(state.queue)
                await self.safe_send(message, "🔀 <b>Queue shuffled.</b>")

            except Exception:
                log.exception("shuffle_handler failed")
                await self.safe_send(message, "❌ Shuffle nahi ho paaya.")

        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def clearqueue_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return

                state = self.get_state(message.chat.id)
                count = len(state.queue)
                state.queue.clear()
                await self.safe_send(message, f"🧹 <b>Cleared</b> <code>{count}</code> queued track(s).")

            except Exception:
                log.exception("clearqueue_handler failed")
                await self.safe_send(message, "❌ Queue clear nahi ho paayi.")

        @self.bot.on_message(filters.command(["np", "now"]) & filters.group)
        async def np_handler(_, message: Message):
            try:
                state = self.get_state(message.chat.id)
                await self.safe_send(message, self.now_playing_text(state), reply_markup=self.np_keyboard())
            except Exception:
                log.exception("np_handler failed")
                await self.safe_send(message, "❌ Now playing panel load nahi hua.")

        # ---------------- OWNER / HIDDEN COMMANDS ----------------

        @self.bot.on_message(filters.command(["shelp"]) & (filters.private | filters.group))
        async def shelp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return
                await self.safe_send(message, self.shell_help_text())
            except Exception:
                log.exception("shelp_handler failed")

        @self.bot.on_message(filters.command(["setdp"]) & filters.private)
        async def setdp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return await self.safe_send(message, "❌ Ye command sirf allowed owner use kar sakta hai.")

                self.pending_start_photo[message.from_user.id] = time.time()
                await self.safe_send(
                    message,
                    "🖼 <b>Startup photo set mode enabled.</b>\n\n"
                    "Ab mujhe <b>photo</b> bhejo.\n"
                    "Main usko start panel pe use karunga.\n\n"
                    "Cancel ke liye /cancel likh do."
                )
            except Exception:
                log.exception("setdp_handler failed")

        @self.bot.on_message(filters.command(["removedp"]) & filters.private)
        async def removedp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return await self.safe_send(message, "❌ Ye command sirf allowed owner use kar sakta hai.")

                self.settings["start_photo_file_id"] = ""
                self.save_settings()
                self.pending_start_photo.pop(message.from_user.id, None)
                await self.safe_send(message, "✅ Startup photo remove kar di gayi hai.")
            except Exception:
                log.exception("removedp_handler failed")

        @self.bot.on_message(filters.private & (filters.photo | filters.document))
        async def private_media_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return

                if message.from_user.id not in self.pending_start_photo:
                    return

                file_id = ""
                if message.photo:
                    file_id = message.photo[-1].file_id
                elif message.document and (message.document.mime_type or "").startswith("image/"):
                    file_id = message.document.file_id
                else:
                    return await self.safe_send(message, "❌ Sirf image/photo bhejo.")

                self.settings["start_photo_file_id"] = file_id
                self.save_settings()
                self.pending_start_photo.pop(message.from_user.id, None)

                await self.safe_send(
                    message,
                    "✅ <b>Startup photo saved successfully.</b>\nAb /start panel pe ye photo dikhegi."
                )
            except Exception:
                log.exception("private_media_handler failed")
                await self.safe_send(message, "❌ Photo save nahi ho paayi.")

        # ---------------- CLONE FLOW (PRIVATE ONLY) ----------------

        if self.is_master:
            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def clone_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return await self.safe_send(message, "❌ Owner only command.")

                    self.clone_flow[message.from_user.id] = {"step": "bot_token"}
                    await self.safe_send(
                        message,
                        "🤖 <b>New Bot Setup Started</b>\n\n"
                        "Step 1/4:\n"
                        "Naya <b>bot token</b> bhejo.\n\n"
                        "Example:\n<code>123456789:ABCDEF...</code>\n\n"
                        "Cancel ke liye /cancel"
                    )
                except Exception:
                    log.exception("clone_handler failed")

            @self.bot.on_message(filters.command(["cancel"]) & filters.private)
            async def cancel_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return

                    had_setup = message.from_user.id in self.clone_flow
                    self.clone_flow.pop(message.from_user.id, None)
                    self.pending_start_photo.pop(message.from_user.id, None)

                    if had_setup:
                        await self.safe_send(message, "🛑 Current setup cancelled.")
                    else:
                        await self.safe_send(message, "✅ Nothing pending now.")
                except Exception:
                    log.exception("cancel_handler failed")

            @self.bot.on_message(filters.command(["clones"]) & filters.private)
            async def clones_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return await self.safe_send(message, "❌ Owner only command.")

                    files = sorted(CLONES_DIR.glob("*.json"))
                    if not files:
                        return await self.safe_send(message, "📭 No saved bot configs found.")

                    lines = ["📦 <b>Saved Bot Configs</b>\n"]
                    for f in files[:50]:
                        try:
                            cfg = load_config(f)
                            lines.append(
                                f"• <code>{escape_html(cfg.bot_id)}</code> — "
                                f"{escape_html(cfg.owner_username)} — {escape_html(cfg.support_chat)}"
                            )
                        except Exception:
                            lines.append(f"• {escape_html(f.name)}")

                    await self.safe_send(message, "\n".join(lines))
                except Exception:
                    log.exception("clones_handler failed")

            @self.bot.on_message(filters.private & filters.text)
            async def clone_flow_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return

                    state = self.clone_flow.get(message.from_user.id)
                    if not state:
                        return

                    text = (message.text or "").strip()
                    step = state.get("step")

                    if text.lower() in {"/cancel", "/clone", "/clones", "/setdp", "/removedp"}:
                        return

                    if step == "bot_token":
                        if not TOKEN_RE.match(text):
                            return await self.safe_send(
                                message,
                                "❌ Invalid bot token lag raha hai.\nDobara sahi token bhejo."
                            )
                        state["bot_token"] = text
                        state["step"] = "support"
                        return await self.safe_send(
                            message,
                            "Step 2/4:\nSupport group username ya link bhejo.\n\n"
                            "Example:\n<code>@yoursupportchat</code>"
                        )

                    if step == "support":
                        state["support_chat"] = normalize_support(text)
                        state["step"] = "owner_username"
                        return await self.safe_send(
                            message,
                            "Step 3/4:\nOwner username ya link bhejo.\n\n"
                            "Example:\n<code>@YourUsername</code>"
                        )

                    if step == "owner_username":
                        state["owner_username"] = normalize_owner_username(text)
                        state["step"] = "session"
                        return await self.safe_send(
                            message,
                            "Step 4/4:\nAssistant string session bhejo.\n"
                            "Ya same default use karna hai to <code>/default</code> likho."
                        )

                    if step == "session":
                        session_string = self.config.assistant_session if text.lower() == "/default" else text
                        if len(session_string) < 40:
                            return await self.safe_send(
                                message,
                                "❌ Session string bahut short lag rahi hai.\nDobara valid session bhejo."
                            )

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

                        return await self.safe_send(
                            message,
                            "✅ <b>Bot launched successfully</b>\n\n"
                            f"🤖 <b>Bot ID:</b> <code>{escape_html(clone_cfg.bot_id)}</code>\n"
                            f"💬 <b>Support:</b> {escape_html(clone_cfg.support_chat)}\n"
                            f"👤 <b>Owner:</b> {escape_html(clone_cfg.owner_username)}\n"
                            f"🆔 <b>PID:</b> <code>{proc.pid}</code>"
                        )

                except Exception:
                    log.exception("clone_flow_handler failed")
                    await self.safe_send(message, "❌ New bot create karte time error aaya.")

    # -----------------------------------------------------
    # START / STOP
    # -----------------------------------------------------

    async def start(self) -> None:
        if shutil.which("ffmpeg") is None:
            log.warning("ffmpeg not found in PATH. Playback may fail.")

        await self.add_handlers()

        await self.assistant.start()
        assistant_me = await self.assistant.get_me()
        self.assistant_id = assistant_me.id
        self.assistant_name = assistant_me.first_name or "Assistant"

        await self.bot.start()
        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name = me.first_name or self.config.brand_name
        self.bot_id_int = me.id

        await self.calls.start()

        log.info("RUNNING | %s | @%s | bot_id=%s", self.bot_name, self.bot_username, self.config.bot_id)
        await idle()

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True

        for name, action in (
            ("calls.stop", getattr(self.calls, "stop", None)),
            ("bot.stop", getattr(self.bot, "stop", None)),
            ("assistant.stop", getattr(self.assistant, "stop", None)),
        ):
            try:
                if action:
                    result = action()
                    if asyncio.iscoroutine(result):
                        await result
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


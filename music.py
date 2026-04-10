#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════╗
║      ◉ TELEGRAM MUSIC BOT — FULL SCRIPT ◉       ║
║   Fast • Stable • Dynamic • Error-Friendly       ║
╚══════════════════════════════════════════════════╝
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
from typing import Any, Dict, List, Optional, Tuple

# ═══════════════════════════════════════════
#  LOCAL .ENV LOADER
# ═══════════════════════════════════════════

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

# ═══════════════════════════════════════════
#  BOOTSTRAP / AUTO INSTALL
# ═══════════════════════════════════════════

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
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", "-U", *missing])

ensure_python_packages()

# ═══════════════════════════════════════════
#  SAFE IMPORTS
# ═══════════════════════════════════════════

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
import pyrogram.errors as pyro_errors

try:
    from pyrogram.errors import (
        FloodWait, UserAlreadyParticipant, UserNotParticipant,
        RPCError, Forbidden, BadRequest,
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

# ═══════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("musicbot")

# ═══════════════════════════════════════════
#  CONFIG — ALL FROM ENV
# ═══════════════════════════════════════════

API_ID                  = int(os.getenv("API_ID", "0") or "0")
API_HASH                = os.getenv("API_HASH", "")
MAIN_BOT_TOKEN          = os.getenv("MAIN_BOT_TOKEN", "")
OWNER_ID                = int(os.getenv("OWNER_ID", "0") or "0")
DEFAULT_ASSISTANT_SESSION = os.getenv("DEFAULT_ASSISTANT_SESSION", "")
MASTER_SUPPORT_CHAT     = os.getenv("MASTER_SUPPORT_CHAT", "@support")
MASTER_OWNER_USERNAME   = os.getenv("MASTER_OWNER_USERNAME", "@owner")
# BOT_BRAND_NAME is intentionally NOT set here — bot fetches its own name at runtime
BOT_BRAND_TAGLINE       = os.getenv("BOT_BRAND_TAGLINE", "Fast • Stable • Smooth VC Player")
NUBCODER_TOKEN          = os.getenv("NUBCODER_TOKEN", "")

ROOT_RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", str(Path(__file__).resolve().parent / "runtime"))).resolve()
ROOT_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
CLONES_DIR = ROOT_RUNTIME_DIR / "clones";  CLONES_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR   = ROOT_RUNTIME_DIR / "logs";    LOGS_DIR.mkdir(parents=True, exist_ok=True)
PIDS_DIR   = ROOT_RUNTIME_DIR / "pids";    PIDS_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════
#  DATA MODELS
# ═══════════════════════════════════════════

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
    brand_name: str = ""           # fetched dynamically from Telegram
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
            return "Live / Unknown"
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

# ═══════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════

URL_RE      = re.compile(r"^(https?://|www\.)", re.I)
TOKEN_RE    = re.compile(r"^\d{7,12}:[A-Za-z0-9_-]{20,}$")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")

VOICE_CHAT_ERROR_MARKERS = {
    "GROUPCALL_FORBIDDEN", "GROUPCALL_ALREADY_STARTED", "GROUPCALL_NOT_FOUND",
    "CHAT_ADMIN_REQUIRED", "CHAT_ADMIN_INVITE_REQUIRED", "INVITE_HASH_EXPIRED",
    "PARTICIPANT_JOIN_MISSING", "PEER_ID_INVALID", "CHAT_WRITE_FORBIDDEN",
    "CHANNEL_PUBLIC_GROUP_NA", "CHAT_FORBIDDEN", "VOICE CHAT", "VIDEO CHAT",
    "NO ACTIVE GROUP CALL", "NOT IN CALL", "ALREADY ENDED", "JOIN AS PEER INVALID",
    "GROUPCALL_JOIN_MISSING", "CALL_PROTOCOL", "YOU MUST BE ADMIN", "ANONYMOUS ADMIN",
    "USER_BANNED_IN_CHANNEL",
}

def is_url(text: str) -> bool:
    return bool(URL_RE.match((text or "").strip()))

def escape_html(text: str) -> str:
    return html.escape(str(text or ""), quote=True)

def normalize_support(value: str) -> str:
    value = (value or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = "@" + value.split(prefix, 1)[1].strip("/")
            break
    elif value and not value.startswith("@") and USERNAME_RE.fullmatch(value):
        value = "@" + value
    return value or "@support"

def normalize_owner_username(value: str) -> str:
    value = (value or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = value.split(prefix, 1)[1].strip("/")
            break
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
    if not cfg.api_id:        missing.append("API_ID")
    if not cfg.api_hash:      missing.append("API_HASH")
    if not cfg.bot_token:     missing.append("MAIN_BOT_TOKEN / clone bot_token")
    if not cfg.owner_id:      missing.append("OWNER_ID")
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
    return "✅ ON" if value else "❌ OFF"

def pretty_uptime(seconds: int) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:  return f"{d}d {h}h {m}m"
    if h:  return f"{h}h {m}m {s}s"
    if m:  return f"{m}m {s}s"
    return f"{s}s"

# ═══════════════════════════════════════════
#  YT-DLP — FAST EXTRACTION
# ═══════════════════════════════════════════

def sync_extract_track(query: str) -> Track:
    ydl_opts = {
        "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "default_search": "ytsearch1",
        "skip_download": True,
        "geo_bypass": True,
        "extract_flat": False,
        "nocheckcertificate": True,
        "source_address": "0.0.0.0",
        # speed tweaks
        "socket_timeout": 8,
        "retries": 2,
        "fragment_retries": 2,
        "http_chunk_size": 10485760,
        "concurrent_fragment_downloads": 4,
        "buffersize": 32768,
        "youtube_include_dash_manifest": False,
    }
    source = query if is_url(query) else f"ytsearch1:{query}"
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(source, download=False)
        if info is None:
            raise ValueError("Koi result nahi mila.")
        if "entries" in info:
            entries = info.get("entries") or []
            info = next((x for x in entries if x), None)
            if not info:
                raise ValueError("Koi playable result nahi mila.")
        stream_url  = info.get("url")
        webpage_url = info.get("webpage_url") or info.get("original_url") or query
        title       = info.get("title") or "Unknown Title"
        duration    = int(info.get("duration") or 0)
        source_name = info.get("extractor_key") or info.get("extractor") or "Media"
        thumb       = info.get("thumbnail") or ""
        if not stream_url:
            raise ValueError("Audio URL resolve nahi ho paaya — yt-dlp returned no stream.")
        return Track(title=title, stream_url=stream_url, webpage_url=webpage_url,
                     duration=duration, source=source_name, thumbnail=thumb)

# ═══════════════════════════════════════════
#  STYLED UI HELPERS  (Senorita-style caps/symbols)
# ═══════════════════════════════════════════

def box(text: str) -> str:
    """Wrap text in a styled quote-block style line."""
    return f"◉ {text}"

def sep() -> str:
    return "•────────────────────•"

# ═══════════════════════════════════════════
#  CORE BOT CLASS
# ═══════════════════════════════════════════

class TelegramMusicBot:
    def __init__(self, config: BotConfig, config_path: Optional[Path] = None, is_master: bool = False):
        validate_config(config)

        self.config      = config
        self.config_path = config_path
        self.is_master   = is_master
        self.start_time  = time.time()

        self.bot_storage = ROOT_RUNTIME_DIR / f"bot_{config.bot_id}"
        self.bot_storage.mkdir(parents=True, exist_ok=True)

        self.settings_path = self.bot_storage / "settings.json"
        self.settings: Dict[str, Any] = self.load_settings()

        session_name = f"assistant_{config.bot_id}"
        workdir      = str(self.bot_storage)

        self.bot = Client(
            name=f"bot_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            bot_token=config.bot_token,
            workdir=workdir,
        )
        self.assistant = Client(
            name=session_name,
            api_id=config.api_id,
            api_hash=config.api_hash,
            session_string=config.assistant_session,
        )
        self.calls = PyTgCalls(self.assistant)

        self.states:    Dict[int, ChatState]  = {}
        self.chat_locks: Dict[int, asyncio.Lock] = {}
        self.clone_flow: Dict[int, Dict[str, Any]] = {}
        self.pending_start_photo: Dict[int, float]  = {}

        # filled at runtime from Telegram
        self.bot_username:    str = ""
        self.bot_name:        str = ""
        self.bot_id_int:      int = 0
        self.assistant_id:    int = 0
        self.assistant_username: str = ""
        self.assistant_name:  str = "Assistant"
        self._stopping:       bool = False

    # ─────────────────────────────────────
    #  SETTINGS
    # ─────────────────────────────────────

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
            json.dumps(self.settings, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # ─────────────────────────────────────
    #  STATE / LOCK HELPERS
    # ─────────────────────────────────────

    def get_state(self, chat_id: int) -> ChatState:
        if chat_id not in self.states:
            self.states[chat_id] = ChatState()
        return self.states[chat_id]

    def get_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        return self.chat_locks[chat_id]

    # ─────────────────────────────────────
    #  RUNTIME IDENTITY (uses fetched name)
    # ─────────────────────────────────────

    @property
    def display_name(self) -> str:
        return self.bot_name or self.config.brand_name or "Music Bot"

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

    # ─────────────────────────────────────
    #  UI TEXT  (Senorita-style, dynamic name)
    # ─────────────────────────────────────

    def start_text(self, user_name: str = "") -> str:
        n   = escape_html(self.display_name)
        tag = escape_html(self.config.tagline)
        greet = f"ʜᴇʏ ʙᴀʙʏ {escape_html(user_name)} 🎶" if user_name else f"ʜᴇʏ ʙᴀʙʏ 🎶"
        return (
            f"{greet}\n"
            f"{sep()}\n\n"
            f"{box(f'ᴛʜɪs ɪs {n.upper()} : ꜰᴀsᴛ &')}\n"
            f"ᴘᴏᴡᴇʀꜰᴜʟ ᴛɢ ᴍᴜsɪᴄ ʙᴏᴛ.\n\n"
            f"{box('sᴍᴏᴏᴛʜ ʙᴇᴀᴛs • sᴛᴀʙʟᴇ &')}\n"
            f"sᴇᴀᴍʟᴇss ᴍᴜsɪᴄ ꜰʟᴏᴡ.\n\n"
            f"{box('ɴᴇᴡ ᴠᴇʀsɪᴏɴ ᴡɪᴛʜ sᴜᴘᴇʀ ꜰᴀsᴛ')}\n"
            f"ʏᴏᴜᴛᴜʙᴇ ᴀᴘɪ ʙᴀsᴇᴅ.\n\n"
            f"{sep()}\n\n"
            f"{box('ᴄʟɪᴄᴋ ᴏɴ ᴛʜᴇ ʜᴇʟᴘ ʙᴜᴛᴛᴏɴ ᴛᴏ ɢᴇᴛ')}\n"
            f"ɪɴꜰᴏʀᴍᴀᴛɪᴏɴ ᴀʙᴏᴜᴛ ᴍʏ ᴍᴏᴅᴜʟᴇs\n"
            f"ᴀɴᴅ ᴄᴏᴍᴍᴀɴᴅs."
        )

    def about_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"✨ ᴀʙᴏᴜᴛ {n.upper()}\n"
            f"{sep()}\n\n"
            f"{box('sᴍᴏᴏᴛʜ ᴠᴄ ᴘʟᴀʏʙᴀᴄᴋ ᴇɴɢɪɴᴇ')}\n"
            f"{box('ꜰʀɪᴇɴᴅʟʏ ᴇʀʀᴏʀ ᴅɪᴀɢɴᴏsᴛɪᴄs')}\n"
            f"{box('sᴍᴀʀᴛ ǫᴜᴇᴜᴇ, ʟᴏᴏᴘ, sʜᴜꜰꜰʟᴇ')}\n"
            f"{box('ɪɴʟɪɴᴇ ʜᴇʟᴘ ᴇxᴘʟᴏʀᴇʀ')}\n"
            f"{box('sᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ sᴜᴘᴘᴏʀᴛ ᴠɪᴀ /setdp')}\n\n"
            f"{sep()}\n\n"
            f"ᴜsᴇ ɪɴ ɢʀᴏᴜᴘ:\n"
            f"1. ᴀᴅᴅ ʙᴏᴛ + ᴀssɪsᴛᴀɴᴛ\n"
            f"2. sᴛᴀʀᴛ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ\n"
            f"3. sᴇɴᴅ /play sᴏɴɢ ɴᴀᴍᴇ"
        )

    def help_home_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"📚 {n.upper()} ʜᴇʟᴘ ᴘᴀɴᴇʟ\n"
            f"{sep()}\n\n"
            f"ɴᴇᴄʜᴇ sᴇᴄᴛɪᴏɴ ᴄʜᴜɴᴏ ᴀᴜʀ\n"
            f"ᴄᴏᴍᴍᴀɴᴅs ᴇxᴘʟᴏʀᴇ ᴋᴀʀᴏ.\n\n"
            f"💡 ᴛɪᴘ: ɢʀᴏᴜᴘ ᴍᴇ /play sᴏɴɢ ɴᴀᴍᴇ"
        )

    def help_music_text(self) -> str:
        return (
            f"🎵 ᴍᴜsɪᴄ ᴄᴏᴍᴍᴀɴᴅs\n"
            f"{sep()}\n\n"
            f"『 /play  』→ sᴏɴɢ ɴᴀᴍᴇ / ᴜʀʟ sᴇ ᴘʟᴀʏ\n"
            f"『 /p     』→ /play ᴋᴀ sʜᴏʀᴛ ꜰᴏʀᴍ\n"
            f"『 /pause 』→ ᴘᴀᴜsᴇ ᴄᴜʀʀᴇɴᴛ sᴏɴɢ\n"
            f"『 /resume』→ ʀᴇsᴜᴍᴇ ᴘᴀᴜsᴇᴅ sᴏɴɢ\n"
            f"『 /skip  』→ sᴋɪᴘ ᴄᴜʀʀᴇɴᴛ ᴛʀᴀᴄᴋ\n"
            f"『 /next  』→ /skip ᴋᴀ ᴀʟɪᴀs\n"
            f"『 /stop  』→ ᴘʟᴀʏʙᴀᴄᴋ ʙɴᴅ ᴋᴀʀᴏ\n"
            f"『 /end   』→ /stop ᴋᴀ ᴀʟɪᴀs\n"
            f"『 /queue 』→ ǫᴜᴇᴜᴇ ʟɪsᴛ ᴅᴇᴋʜᴏ\n"
            f"『 /q     』→ /queue ᴋᴀ sʜᴏʀᴛ ꜰᴏʀᴍ\n"
            f"『 /np    』→ ɴᴏᴡ ᴘʟᴀʏɪɴɢ ᴘᴀɴᴇʟ\n"
            f"『 /now   』→ /np ᴋᴀ ᴀʟɪᴀs\n\n"
            f"{sep()}"
        )

    def help_admin_text(self) -> str:
        return (
            f"🛠 ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟs\n"
            f"{sep()}\n\n"
            f"『 /loop       』→ ʟᴏᴏᴘ ᴛᴏɢɢʟᴇ\n"
            f"『 /loop on    』→ ʟᴏᴏᴘ ᴇɴᴀʙʟᴇ\n"
            f"『 /loop off   』→ ʟᴏᴏᴘ ᴅɪsᴀʙʟᴇ\n"
            f"『 /shuffle    』→ ǫᴜᴇᴜᴇ sʜᴜꜰꜰʟᴇ\n"
            f"『 /clearqueue 』→ ǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ\n"
            f"『 /mute       』→ ᴠᴄ ᴍᴜᴛᴇ\n"
            f"『 /unmute     』→ ᴠᴄ ᴜɴᴍᴜᴛᴇ\n"
            f"『 /ping       』→ ʙᴏᴛ sᴘᴇᴇᴅ / sᴛᴀᴛᴜs\n"
            f"『 /alive      』→ ʙᴏᴛ ᴏɴʟɪɴᴇ ᴄʜᴇᴄᴋ\n\n"
            f"⚠️ ɴᴏᴛᴇ: ᴀᴅᴍɪɴ-ᴏɴʟʏ ᴄᴏɴᴛʀᴏʟs\n"
            f"{sep()}"
        )

    def help_extra_text(self) -> str:
        return (
            f"🧩 ᴇxᴛʀᴀ ɪɴꜰᴏ\n"
            f"{sep()}\n\n"
            f"{box('ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ sᴍᴏᴏᴛʜ ᴍɢᴍᴛ ᴋᴇ ʟɪᴇ.')}\n"
            f"{box('ᴀssɪsᴛᴀɴᴛ ɢʀᴏᴜᴘ ᴍᴇ ʜᴏɴᴀ ᴄʜᴀʜɪᴇ.')}\n"
            f"{box('/play sᴇ ᴘᴇʜʟᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ sᴛᴀʀᴛ ᴋᴀʀᴏ.')}\n"
            f"{box('ᴘʀɪᴠᴀᴛᴇ ɢʀᴏᴜᴘ ᴍᴇ ɪɴᴠɪᴛᴇ ʟɪɴᴋ ᴡᴏʀᴋ ᴋᴀʀᴇ.')}\n"
            f"{box('ᴘᴇʀᴍɪssɪᴏɴs ꜰɪx ᴋᴀʀɴᴇ ᴋᴇ ʙᴀᴀᴅ /play ʀᴇᴛʀʏ ᴋᴀʀᴏ.')}\n\n"
            f"{sep()}"
        )

    def shell_help_text(self) -> str:
        return (
            f"🔐 ʜɪᴅᴅᴇɴ ᴏᴡɴᴇʀ ᴘᴀɴᴇʟ\n"
            f"{sep()}\n\n"
            f"『 /shelp     』→ ʏᴇ ᴘᴀɴᴇʟ\n"
            f"『 /setdp     』→ sᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ sᴇᴛ\n"
            f"『 /removedp  』→ sᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ʀᴇᴍᴏᴠᴇ\n"
            f"『 /clone     』→ ɴᴀʏᴀ ʙᴏᴛ sᴇᴛᴜᴘ ꜰʟᴏᴡ\n"
            f"『 /cancel    』→ sᴇᴛᴜᴘ ᴄᴀɴᴄᴇʟ\n"
            f"『 /clones    』→ ꜱᴀᴠᴇᴅ ʙᴏᴛ ᴄᴏɴꜰɪɢs ʟɪsᴛ\n\n"
            f"⚠️ sɪʀꜰ ᴏᴡɴᴇʀ ᴋᴇ ʟɪᴇ.\n"
            f"{sep()}"
        )

    # ─────────────────────────────────────
    #  KEYBOARDS
    # ─────────────────────────────────────

    def start_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ ɪɴ ʏᴏᴜʀ ɢʀᴏᴜᴘ ➕", url=self.add_to_group_url)],
            [
                InlineKeyboardButton("👑 ᴏᴡɴᴇʀ", url=self.owner_url),
                InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="nav_about"),
            ],
            [
                InlineKeyboardButton("💬 sᴜᴘᴘᴏʀᴛ ↗", url=self.support_url),
                InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇ ↗", url=self.support_url),
            ],
            [InlineKeyboardButton("📚 ʜᴇʟᴘ ᴀɴᴅ ᴄᴏᴍᴍᴀɴᴅs", callback_data="nav_help_home")],
        ])

    def help_home_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🎵 ᴍᴜsɪᴄ", callback_data="help_music"),
                InlineKeyboardButton("🛠 ᴀᴅᴍɪɴ", callback_data="help_admin"),
            ],
            [
                InlineKeyboardButton("🧩 ᴇxᴛʀᴀ", callback_data="help_extra"),
                InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="nav_about"),
            ],
            [
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home"),
                InlineKeyboardButton("❌ ᴄʟᴏsᴇ", callback_data="nav_close"),
            ],
        ])

    def subpage_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⬅ ʙᴀᴄᴋ", callback_data="nav_help_home"),
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home"),
            ],
            [InlineKeyboardButton("❌ ᴄʟᴏsᴇ", callback_data="nav_close")],
        ])

    def np_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⏸ ᴘᴀᴜsᴇ",  callback_data="ctl_pause"),
                InlineKeyboardButton("▶ ʀᴇsᴜᴍᴇ", callback_data="ctl_resume"),
            ],
            [
                InlineKeyboardButton("⏭ sᴋɪᴘ",   callback_data="ctl_skip"),
                InlineKeyboardButton("⏹ sᴛᴏᴘ",   callback_data="ctl_stop"),
            ],
            [
                InlineKeyboardButton("📜 ǫᴜᴇᴜᴇ",    callback_data="ctl_queue"),
                InlineKeyboardButton("🔄 ʀᴇꜰʀᴇsʜ", callback_data="ctl_np"),
            ],
        ])

    def queue_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔀 sʜᴜꜰꜰʟᴇ", callback_data="ctl_shuffle"),
                InlineKeyboardButton("🧹 ᴄʟᴇᴀʀ",   callback_data="ctl_clearqueue"),
            ],
            [
                InlineKeyboardButton("🎵 ɴᴏᴡ ᴘʟᴀʏɪɴɢ", callback_data="ctl_np"),
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ",         callback_data="nav_home"),
            ],
        ])

    # ─────────────────────────────────────
    #  SAFE SEND / EDIT
    # ─────────────────────────────────────

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

    async def safe_edit_panel(self, msg: Optional[Message], text: str,
                               reply_markup: Optional[InlineKeyboardMarkup] = None):
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
        except Exception:
            log.exception("safe_edit_panel failed")
        return None

    async def send_start_panel(self, message: Message):
        user_name = ""
        if message.from_user:
            user_name = message.from_user.first_name or message.from_user.username or ""
        photo_id = (self.settings.get("start_photo_file_id") or "").strip()
        if photo_id:
            try:
                return await message.reply_photo(
                    photo=photo_id,
                    caption=self.start_text(user_name),
                    reply_markup=self.start_keyboard(),
                )
            except Exception:
                log.exception("send_start_panel photo failed; fallback text")
        return await self.safe_send(message, self.start_text(user_name), reply_markup=self.start_keyboard())

    # ─────────────────────────────────────
    #  AUTH
    # ─────────────────────────────────────

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
            await self.safe_send(message, "❌ ʏᴇ ᴄᴏɴᴛʀᴏʟ sɪʀꜰ ɢʀᴏᴜᴘ ᴀᴅᴍɪɴs ᴜsᴇ ᴋᴀʀ sᴀᴋᴛᴇ ʜᴀɪɴ.")
        return ok

    # ─────────────────────────────────────
    #  TRACK RESOLUTION  (threaded for speed)
    # ─────────────────────────────────────

    async def resolve_track(self, query: str, requested_by: str) -> Track:
        track = await asyncio.to_thread(sync_extract_track, query)
        track.requested_by = requested_by
        return track

    # ─────────────────────────────────────
    #  ASSISTANT / VC DIAGNOSTICS
    # ─────────────────────────────────────

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
        member = await self.assistant_member_info(chat_id)
        if member:
            # check if banned
            status = getattr(getattr(member, "status", None), "name", "")
            if "BANNED" in status.upper() or "KICKED" in status.upper():
                aname = escape_html(self.assistant_username or self.assistant_name)
                aid   = self.assistant_id
                return (
                    False,
                    f"⚠️ ᴀssɪsᴛᴀɴᴛ ʙᴀɴ ʜᴀɪ ᴛᴇʀᴇ ɢʀᴏᴜᴘ ᴍᴇ!\n\n"
                    f"👤 ᴀssɪsᴛᴀɴᴛ: @{aname}\n"
                    f"🆔 ᴜsᴇʀɪᴅ: {aid}\n\n"
                    f"ɪsᴋᴏ ᴘᴇʜʟᴇ ᴜɴʙᴀɴ ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."
                )
            return True, None

        link, reason = await self.build_join_link(chat_id)
        if not link:
            aname = escape_html(self.assistant_username or self.assistant_name)
            aid   = self.assistant_id
            return (
                False,
                f"⚠️ ᴀssɪsᴛᴀɴᴛ (@{aname} | {aid}) ɢʀᴏᴜᴘ ᴍᴇ ɴᴀʜɪ ʜᴀɪ.\n"
                f"ᴊᴏɪɴ ʟɪɴᴋ ʙʜɪ ɴᴀʜɪ ʙɴᴀ ʀᴀʜᴀ.\n\n"
                f"ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ᴀᴜʀ ɪɴᴠɪᴛᴇ ᴜsᴇʀs ᴘᴇʀᴍɪssɪᴏɴ ᴅᴏ."
                + (f"\n\nᴅᴇʙᴜɢ: {escape_html(reason or 'unknown')}" if reason else "")
            )

        try:
            await self.assistant.join_chat(link)
            await asyncio.sleep(1.5)
            member = await self.assistant_member_info(chat_id)
            if member:
                return True, None
            return False, "ᴀssɪsᴛᴀɴᴛ ᴊᴏɪɴ ʜᴏ ɢᴀʏᴀ ᴛʜᴀ, ᴘᴇʀ ᴄᴏɴꜰɪʀᴍ ɴᴀʜɪ ʜᴜᴀ. ᴇᴋ ʙᴀᴀʀ /play ʀᴇᴛʀʏ ᴋᴀʀᴏ."
        except UserAlreadyParticipant:
            return True, None
        except Exception as exc:
            err = str(exc).upper()
            aname = escape_html(self.assistant_username or self.assistant_name)
            aid   = self.assistant_id
            if "BANNED" in err or "KICKED" in err or "USER_BANNED_IN_CHANNEL" in err:
                return (
                    False,
                    f"⚠️ ᴀssɪsᴛᴀɴᴛ ɪs ʙᴀɴ ɪɴ ʏᴏᴜʀ ᴄʜᴀᴛ!\n\n"
                    f"👤 ᴀssɪsᴛᴀɴᴛ: @{aname}\n"
                    f"🆔 ᴜsᴇʀɪᴅ: {aid}\n\n"
                    f"ᴘᴇʜʟᴇ ɪsᴋᴏ ᴜɴʙᴀɴ ᴋᴀʀᴏ ᴛᴇʀᴇ ɢʀᴏᴜᴘ sᴇ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."
                )
            return (
                False,
                f"⚠️ ᴀssɪsᴛᴀɴᴛ (@{aname} | {aid}) ᴊᴏɪɴ ɴᴀʜɪ ʜᴏ ᴘᴀᴀʏᴀ.\n"
                f"ᴄʜᴇᴄᴋ ᴋᴀʀᴏ ᴋɪ ɢʀᴏᴜᴘ ᴀᴄᴄᴇssɪʙʟᴇ ʜᴀɪ.\n\n"
                f"ʀᴇᴀsᴏɴ: {escape_html(str(exc))}"
            )

    async def diagnose_voice_issue(self, chat_id: int, exc: Exception) -> str:
        text = exc_text(exc).upper()
        bot_member       = await self.bot_member_info(chat_id)
        assistant_member = await self.assistant_member_info(chat_id)

        if not bot_member or not is_admin_status(getattr(bot_member, "status", None)):
            return (
                "⚠️ ʙᴏᴛ ɢʀᴏᴜᴘ ᴍᴇ ᴀᴅᴍɪɴ ɴᴀʜɪ ʜᴀɪ.\n"
                "ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ, ꜰɪʀ /play ᴄʜᴀʟᴀᴏ."
            )
        if "NO ACTIVE GROUP CALL" in text or "GROUPCALL_NOT_FOUND" in text or "VOICE CHAT" in text or "VIDEO CHAT" in text:
            return (
                "⚠️ ᴀʙʜɪ ɢʀᴏᴜᴘ ᴍᴇ ᴋᴏɪ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴛɪᴠᴇ ɴᴀʜɪ ʜᴀɪ.\n"
                "ᴘᴇʜʟᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ sᴛᴀʀᴛ ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."
            )
        if "GROUPCALL_FORBIDDEN" in text or "ALREADY ENDED" in text:
            return (
                "⚠️ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴄᴇssɪʙʟᴇ ɴᴀʜɪ ʜᴀɪ ʏᴀ ᴘɪᴄʜʟᴀ ᴄᴀʟʟ ᴋʜᴀᴛᴀᴍ ʜᴏ ᴄʜᴜᴋᴀ ʜᴀɪ.\n"
                "ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴅᴜʙᴀʀᴀ sᴛᴀʀᴛ ᴋᴀʀᴏ ᴀᴜʀ /play ᴄʜᴀʟᴀᴏ."
            )
        if not assistant_member:
            aname = escape_html(self.assistant_username or self.assistant_name)
            aid   = self.assistant_id
            return (
                f"⚠️ ᴀssɪsᴛᴀɴᴛ (@{aname} | {aid}) ɢʀᴏᴜᴘ ᴍᴇ ɴᴀʜɪ ʜᴀɪ.\n"
                f"ɪsᴇ ᴍᴀɴᴜᴀʟʟʏ ᴀᴅᴅ ᴋᴀʀᴏ, ꜰɪʀ /play ᴄʜᴀʟᴀᴏ."
            )
        if "BANNED" in text or "KICKED" in text or "USER_BANNED_IN_CHANNEL" in text:
            aname = escape_html(self.assistant_username or self.assistant_name)
            aid   = self.assistant_id
            return (
                f"⚠️ ᴀssɪsᴛᴀɴᴛ ɪs ʙᴀɴ ɪɴ ʏᴏᴜʀ ᴄʜᴀᴛ!\n\n"
                f"👤 ᴀssɪsᴛᴀɴᴛ: @{aname}\n"
                f"🆔 ᴜsᴇʀɪᴅ: {aid}\n\n"
                f"ᴘᴇʜʟᴇ ɪsᴋᴏ ᴜɴʙᴀɴ ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."
            )
        if "CHAT_ADMIN_REQUIRED" in text or "YOU MUST BE ADMIN" in text:
            return (
                "⚠️ ᴀssɪsᴛᴀɴᴛ ᴋᴏ ᴠᴄ ᴊᴏɪɴ ᴋᴀʀɴᴇ ᴋᴇ ʟɪᴇ ᴀᴅᴍɪɴ ʀɪɢʜᴛs ᴄʜᴀʜɪᴇ.\n"
                "ᴀssɪsᴛᴀɴᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ."
            )
        return (
            "⚠️ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ sᴇ ᴊᴜᴅɴᴇ ᴍᴇ ᴅɪᴋᴋᴀᴛ ᴀᴀʏɪ.\n"
            "ᴄʜᴇᴄᴋ ᴋᴀʀᴏ ᴋɪ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴛɪᴠᴇ ʜᴀɪ ᴀᴜʀ ʙᴏᴛ/ᴀssɪsᴛᴀɴᴛ ᴋᴇ ᴘᴀs ʀɪɢʜᴛs ʜᴀɪɴ."
        )

    # ─────────────────────────────────────
    #  PYTGCALLS PLAY
    # ─────────────────────────────────────

    async def _play_via_pytgcalls(self, chat_id: int, stream_url: str) -> None:
        # try all known pytgcalls APIs (v1 / v2 / v3)
        for method_name in ("join_group_call", "play", "stream"):
            method = getattr(self.calls, method_name, None)
            if not method:
                continue
            try:
                if AudioPiped is not None:
                    result = method(chat_id, AudioPiped(stream_url))
                else:
                    result = method(chat_id, stream_url)
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception as exc:
                if is_voice_chat_error(exc):
                    raise
                continue
        raise RuntimeError("No compatible pytgcalls play method found.")

    async def play_track(self, chat_id: int, track: Track) -> None:
        try:
            await self._play_via_pytgcalls(chat_id, track.stream_url)
        except Exception as first_exc:
            if not is_voice_chat_error(first_exc):
                raise RuntimeError(
                    "⚠️ Audio play ɴᴀʜɪ ʜᴜᴀ.\n"
                    f"ʀᴇᴀsᴏɴ: {escape_html(str(first_exc))}"
                ) from first_exc

            await asyncio.sleep(1.2)
            ok, reason = await self.ensure_assistant_in_chat(chat_id)
            if not ok:
                raise RuntimeError(reason or "ᴀssɪsᴛᴀɴᴛ ᴊᴏɪɴ ɪssᴜᴇ") from first_exc
            try:
                await self._play_via_pytgcalls(chat_id, track.stream_url)
            except Exception as second_exc:
                friendly = await self.diagnose_voice_issue(chat_id, second_exc)
                raise RuntimeError(friendly) from second_exc

        state = self.get_state(chat_id)
        state.current = track
        state.paused  = False
        state.muted   = False

    # ─────────────────────────────────────
    #  CALL CONTROLS  (safe wrappers)
    # ─────────────────────────────────────

    async def leave_call_safely(self, chat_id: int):
        for method_name in ("leave_call", "leave_group_call"):
            method = getattr(self.calls, method_name, None)
            if method:
                try:
                    result = method(chat_id)
                    if asyncio.iscoroutine(result):
                        await result
                    return
                except Exception:
                    pass

    async def pause_call_safely(self, chat_id: int):
        for method_name in ("pause", "pause_stream"):
            method = getattr(self.calls, method_name, None)
            if method:
                result = method(chat_id)
                if asyncio.iscoroutine(result):
                    await result
                return
        raise RuntimeError("ᴘᴀᴜsᴇ ᴍᴇᴛʜᴏᴅ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ.")

    async def resume_call_safely(self, chat_id: int):
        for method_name in ("resume", "resume_stream"):
            method = getattr(self.calls, method_name, None)
            if method:
                result = method(chat_id)
                if asyncio.iscoroutine(result):
                    await result
                return
        raise RuntimeError("ʀᴇsᴜᴍᴇ ᴍᴇᴛʜᴏᴅ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ.")

    async def mute_call_safely(self, chat_id: int):
        method = getattr(self.calls, "mute", None)
        if method:
            result = method(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        raise RuntimeError("ᴍᴜᴛᴇ ᴍᴇᴛʜᴏᴅ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ.")

    async def unmute_call_safely(self, chat_id: int):
        method = getattr(self.calls, "unmute", None)
        if method:
            result = method(chat_id)
            if asyncio.iscoroutine(result):
                await result
            return
        raise RuntimeError("ᴜɴᴍᴜᴛᴇ ᴍᴇᴛʜᴏᴅ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ.")

    # ─────────────────────────────────────
    #  NOW PLAYING / QUEUE TEXT
    # ─────────────────────────────────────

    def now_playing_text(self, state: ChatState) -> str:
        if not state.current:
            return f"❌ ᴀʙʜɪ ᴋᴜᴄʜ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ ʀᴀʜᴀ."
        t = state.current
        return (
            f"🎵 ɴᴏᴡ ᴘʟᴀʏɪɴɢ\n"
            f"{sep()}\n\n"
            f"🏷 ᴛɪᴛʟᴇ   : {escape_html(t.title)}\n"
            f"⏱ ᴅᴜʀᴀᴛɪᴏɴ: {escape_html(t.pretty_duration)}\n"
            f"🌐 sᴏᴜʀᴄᴇ  : {escape_html(t.source)}\n"
            f"🙋 ʀᴇǫ ʙʏ  : {t.requested_by}\n\n"
            f"🔁 ʟᴏᴏᴘ   : {human_bool(state.loop)}\n"
            f"⏸ ᴘᴀᴜsᴇᴅ : {human_bool(state.paused)}\n"
            f"🔇 ᴍᴜᴛᴇᴅ  : {human_bool(state.muted)}\n\n"
            f"{sep()}"
        )

    def queue_text(self, state: ChatState) -> str:
        if not state.current and not state.queue:
            return f"📭 ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ."
        lines = [f"📜 ǫᴜᴇᴜᴇ ᴘᴀɴᴇʟ\n{sep()}\n"]
        if state.current:
            lines.append(
                f"🎵 ᴄᴜʀʀᴇɴᴛ: {escape_html(state.current.title)} "
                f"[{escape_html(state.current.pretty_duration)}]"
            )
        if state.queue:
            lines.append("\nᴜᴘ ɴᴇxᴛ:")
            for i, track in enumerate(state.queue[:15], start=1):
                lines.append(f"{i}. {escape_html(track.title)} — {escape_html(track.pretty_duration)}")
            if len(state.queue) > 15:
                lines.append(f"... ᴀɴᴅ {len(state.queue) - 15} ᴍᴏʀᴇ")
        lines.append(f"\n🔁 ʟᴏᴏᴘ  : {human_bool(state.loop)}")
        lines.append(f"⏸ ᴘᴀᴜsᴇᴅ: {human_bool(state.paused)}")
        lines.append(sep())
        return "\n".join(lines)

    # ─────────────────────────────────────
    #  PLAY NEXT / STREAM END
    # ─────────────────────────────────────

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
                state.paused  = False
                state.muted   = False
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
                state.paused  = False
                state.muted   = False
                if announce_chat:
                    try:
                        await self.bot.send_message(chat_id, f"❌ ɴᴇxᴛ ᴛʀᴀᴄᴋ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ sᴀᴋᴀ.\n\n{escape_html(str(exc))}")
                    except Exception:
                        pass
                return

            if announce_chat:
                try:
                    text = (
                        f"▶️ ɴᴏᴡ ᴘʟᴀʏɪɴɢ\n"
                        f"{sep()}\n\n"
                        f"🏷 {escape_html(next_track.title)}\n"
                        f"⏱ {escape_html(next_track.pretty_duration)}\n"
                        f"🙋 {next_track.requested_by}"
                    )
                    if reason:
                        text += f"\n📝 {escape_html(reason)}"
                    await self.bot.send_message(
                        chat_id, text,
                        disable_web_page_preview=True,
                        reply_markup=self.np_keyboard(),
                    )
                except Exception:
                    pass

    async def on_stream_end(self, chat_id: int) -> None:
        try:
            await self.play_next(chat_id, announce_chat=True, reason="ᴘʀᴇᴠɪᴏᴜs sᴛʀᴇᴀᴍ ᴇɴᴅᴇᴅ")
        except Exception:
            log.exception("on_stream_end failed")

    # ─────────────────────────────────────
    #  HANDLERS
    # ─────────────────────────────────────

    async def add_handlers(self) -> None:

        # ── stream end
        @self.calls.on_update()
        async def stream_updates(_, update):
            try:
                name    = type(update).__name__.lower()
                chat_id = getattr(update, "chat_id", None)
                if not chat_id:
                    return
                if StreamEndedCompat is not None and isinstance(update, StreamEndedCompat):
                    return await self.on_stream_end(chat_id)
                if StreamAudioEndedCompat is not None and isinstance(update, StreamAudioEndedCompat):
                    return await self.on_stream_end(chat_id)
                if "ended" in name:
                    await self.on_stream_end(chat_id)
            except Exception:
                log.exception("Stream update handler failed")

        # ── /start
        @self.bot.on_message(filters.command(["start"]) & (filters.private | filters.group))
        async def start_handler(_, message: Message):
            try:
                await self.send_start_panel(message)
            except Exception:
                log.exception("start_handler failed")

        # ── /help /commands
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

        # ── /about
        @self.bot.on_message(filters.command(["about"]) & (filters.private | filters.group))
        async def about_handler(_, message: Message):
            try:
                await self.safe_send(message, self.about_text(), reply_markup=self.subpage_keyboard())
            except Exception:
                log.exception("about_handler failed")

        # ── all callbacks
        @self.bot.on_callback_query()
        async def callback_handler(_, query):
            try:
                data = query.data or ""

                if data == "nav_home":
                    user_name = ""
                    if query.from_user:
                        user_name = query.from_user.first_name or query.from_user.username or ""
                    await self.safe_edit_panel(query.message, self.start_text(user_name), self.start_keyboard())
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
                    return await query.answer("ᴄʟᴏsᴇᴅ")

                # ── music control callbacks (group only)
                if data.startswith("ctl_"):
                    chat_type = getattr(getattr(query.message, "chat", None), "type", None)
                    if chat_type and str(chat_type).lower() not in {"group", "supergroup",
                                                                      "chattype.group", "chattype.supergroup"}:
                        return await query.answer("ʏᴇ ᴄᴏɴᴛʀᴏʟ ɢʀᴏᴜᴘ ᴋᴇ ʟɪᴇ ʜᴀɪ.", show_alert=True)

                    user_id = getattr(query.from_user, "id", None)
                    if not await self.is_admin(query.message.chat.id, user_id):
                        return await query.answer("sɪʀꜰ ᴀᴅᴍɪɴs ᴄᴏɴᴛʀᴏʟ ᴜsᴇ ᴋᴀʀ sᴀᴋᴛᴇ ʜᴀɪɴ.", show_alert=True)

                    chat_id = query.message.chat.id
                    state   = self.get_state(chat_id)

                    if data == "ctl_pause":
                        if state.paused:
                            return await query.answer("ᴘʟᴀʏʙᴀᴄᴋ ᴘᴇʜʟᴇ sᴇ ʜɪ ᴘᴀᴜsᴇᴅ ʜᴀɪ.", show_alert=True)
                        try:
                            await self.pause_call_safely(chat_id)
                            state.paused = True
                            await self.safe_edit_panel(query.message, self.now_playing_text(state), self.np_keyboard())
                            return await query.answer("⏸ ᴘᴀᴜsᴇᴅ")
                        except Exception as exc:
                            msg = await self.diagnose_voice_issue(chat_id, exc)
                            return await query.answer(msg[:200], show_alert=True)

                    if data == "ctl_resume":
                        if not state.paused:
                            return await query.answer("ᴘʟᴀʏʙᴀᴄᴋ ᴀʟʀᴇᴀᴅʏ ᴄʜᴀʟ ʀᴀʜᴀ ʜᴀɪ.", show_alert=True)
                        try:
                            await self.resume_call_safely(chat_id)
                            state.paused = False
                            await self.safe_edit_panel(query.message, self.now_playing_text(state), self.np_keyboard())
                            return await query.answer("▶️ ʀᴇsᴜᴍᴇᴅ")
                        except Exception as exc:
                            msg = await self.diagnose_voice_issue(chat_id, exc)
                            return await query.answer(msg[:200], show_alert=True)

                    if data == "ctl_skip":
                        if not state.current and not state.queue:
                            return await query.answer("ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ.", show_alert=True)
                        try:
                            state.loop    = False
                            state.current = None
                            state.paused  = False
                            await self.play_next(chat_id, announce_chat=True, reason="sᴋɪᴘᴘᴇᴅ ʙʏ ᴀᴅᴍɪɴ")
                            return await query.answer("⏭ sᴋɪᴘᴘᴇᴅ")
                        except Exception as exc:
                            return await query.answer(str(exc)[:200], show_alert=True)

                    if data == "ctl_stop":
                        try:
                            state.queue.clear()
                            state.current = None
                            state.paused  = False
                            state.loop    = False
                            state.muted   = False
                            await self.leave_call_safely(chat_id)
                            await self.safe_edit_panel(
                                query.message,
                                f"⏹ ᴘʟᴀʏʙᴀᴄᴋ ᴇɴᴅᴇᴅ\n\nǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ ᴋᴀʀ ᴅɪ ɢᴀʏɪ ʜᴀɪ.",
                                self.queue_keyboard(),
                            )
                            return await query.answer("⏹ sᴛᴏᴘᴘᴇᴅ")
                        except Exception as exc:
                            return await query.answer(str(exc)[:200], show_alert=True)

                    if data == "ctl_queue":
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer()

                    if data == "ctl_np":
                        await self.safe_edit_panel(query.message, self.now_playing_text(state), self.np_keyboard())
                        return await query.answer()

                    if data == "ctl_shuffle":
                        if len(state.queue) < 2:
                            return await query.answer("sʜᴜꜰꜰʟᴇ ᴋᴇ ʟɪᴇ ᴋᴀᴍ sᴇ ᴋᴀᴍ 2 ᴛʀᴀᴄᴋs ᴄʜᴀʜɪᴇ.", show_alert=True)
                        random.shuffle(state.queue)
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer("🔀 sʜᴜꜰꜰʟᴇᴅ!")

                    if data == "ctl_clearqueue":
                        count = len(state.queue)
                        state.queue.clear()
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer(f"🧹 {count} ᴛʀᴀᴄᴋs ʀᴇᴍᴏᴠᴇᴅ")

                await query.answer()

            except Exception:
                log.exception("callback_handler failed")
                try:
                    await query.answer("❌ sᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ.", show_alert=True)
                except Exception:
                    pass

        # ── /ping /alive
        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def ping_handler(_, message: Message):
            try:
                t0 = time.time()
                x  = await self.safe_send(message, "🏓 ᴘɪɴɢɪɴɢ...")
                taken = (time.time() - t0) * 1000
                uptime = pretty_uptime(int(time.time() - self.start_time))
                active_chats = sum(1 for s in self.states.values() if s.current)
                n = escape_html(self.display_name)
                text = (
                    f"🏓 {n.upper()} ɪs ᴏɴʟɪɴᴇ\n"
                    f"{sep()}\n\n"
                    f"⚡ ʟᴀᴛᴇɴᴄʏ  : {taken:.2f} ᴍs\n"
                    f"⏳ ᴜᴘᴛɪᴍᴇ  : {escape_html(uptime)}\n"
                    f"🎧 ᴀᴄᴛɪᴠᴇ  : {active_chats} ᴄʜᴀᴛs\n"
                    f"🤖 ʙᴏᴛ ɪᴅ  : {escape_html(self.config.bot_id)}\n\n"
                    f"{sep()}"
                )
                if x:
                    await self.safe_edit_text(x, text)
            except Exception:
                log.exception("ping_handler failed")

        # ── /play /p
        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def play_handler(_, message: Message):
            try:
                query = command_arg(message)
                if not query:
                    return await self.safe_send(
                        message,
                        "❌ ᴜsᴀɢᴇ:\n/play sᴏɴɢ ɴᴀᴍᴇ\ɴʏᴀ\n/play youtube_url"
                    )

                processing = await self.safe_send(
                    message,
                    f"🔎 sᴇᴀʀᴄʜɪɴɢ...\n{escape_html(query)}"
                )

                try:
                    track = await self.resolve_track(query, mention_user(message))
                except Exception as exc:
                    await self.safe_edit_text(
                        processing,
                        f"❌ sᴏɴɢ ɴᴀʜɪ ᴍɪʟᴀ — {escape_html(str(exc))}"
                    )
                    return

                async with self.get_lock(message.chat.id):
                    state = self.get_state(message.chat.id)

                    if state.current:
                        state.queue.append(track)
                        pos = len(state.queue)
                        await self.safe_edit_text(
                            processing,
                            f"📥 ǫᴜᴇᴜᴇᴅ ᴀᴛ #{pos}\n\n"
                            f"🏷 {escape_html(track.title)}\n"
                            f"⏱ {escape_html(track.pretty_duration)}"
                        )
                        return

                    await self.safe_edit_text(
                        processing,
                        f"⏳ ᴘʟᴀʏɪɴɢ...\n🏷 {escape_html(track.title)}"
                    )

                    try:
                        await self.play_track(message.chat.id, track)
                    except Exception as exc:
                        await self.safe_edit_text(
                            processing,
                            f"❌ ᴘʟᴀʏ ɴᴀʜɪ ʜᴜᴀ\n\n{escape_html(str(exc))}"
                        )
                        return

                try:
                    np_text = self.now_playing_text(self.get_state(message.chat.id))
                    await self.safe_edit_text(processing, np_text, reply_markup=self.np_keyboard())
                except Exception:
                    pass

            except Exception:
                log.exception("play_handler failed")
                await self.safe_send(message, "❌ /play ᴍᴇ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ.")

        # ── /pause
        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def pause_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if state.paused:
                    return await self.safe_send(message, "⏸ ᴘʟᴀʏʙᴀᴄᴋ ᴘᴇʜʟᴇ sᴇ ʜɪ ᴘᴀᴜsᴇᴅ ʜᴀɪ.")
                await self.pause_call_safely(message.chat.id)
                state.paused = True
                await self.safe_send(message, "⏸ ᴘʟᴀʏʙᴀᴄᴋ ᴘᴀᴜsᴇᴅ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ {escape_html(str(exc))}")

        # ── /resume
        @self.bot.on_message(filters.command(["resume"]) & filters.group)
        async def resume_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.paused:
                    return await self.safe_send(message, "▶️ ᴘʟᴀʏʙᴀᴄᴋ ᴀʟʀᴇᴀᴅʏ ᴄʜᴀʟ ʀᴀʜᴀ ʜᴀɪ.")
                await self.resume_call_safely(message.chat.id)
                state.paused = False
                await self.safe_send(message, "▶️ ᴘʟᴀʏʙᴀᴄᴋ ʀᴇsᴜᴍᴇᴅ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ {escape_html(str(exc))}")

        # ── /skip /next
        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def skip_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.current and not state.queue:
                    return await self.safe_send(message, "📭 ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ.")
                state.loop    = False
                state.current = None
                state.paused  = False
                await self.play_next(message.chat.id, announce_chat=True, reason="sᴋɪᴘᴘᴇᴅ")
            except Exception as exc:
                await self.safe_send(message, f"❌ sᴋɪᴘ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

        # ── /stop /end
        @self.bot.on_message(filters.command(["stop", "end"]) & filters.group)
        async def stop_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                state.queue.clear()
                state.current = None
                state.paused  = False
                state.loop    = False
                state.muted   = False
                await self.leave_call_safely(message.chat.id)
                await self.safe_send(message, "⏹ ᴘʟᴀʏʙᴀᴄᴋ ʙɴᴅ ᴋᴀʀ ᴅɪ ɢᴀʏɪ.\nǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ sᴛᴏᴘ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

        # ── /queue /q
        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def queue_handler(_, message: Message):
            try:
                state = self.get_state(message.chat.id)
                await self.safe_send(message, self.queue_text(state), reply_markup=self.queue_keyboard())
            except Exception:
                log.exception("queue_handler failed")
                await self.safe_send(message, "❌ ǫᴜᴇᴜᴇ ᴘᴀɴᴇʟ ʟᴏᴀᴅ ɴᴀʜɪ ʜᴜᴀ.")

        # ── /loop
        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def loop_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                arg   = command_arg(message).lower().strip()
                state = self.get_state(message.chat.id)
                if arg in {"on", "yes", "true", "1"}:
                    state.loop = True
                elif arg in {"off", "no", "false", "0"}:
                    state.loop = False
                else:
                    state.loop = not state.loop
                emoji = "🔁" if state.loop else "➡️"
                await self.safe_send(message, f"{emoji} ʟᴏᴏᴘ {'ᴇɴᴀʙʟᴇᴅ' if state.loop else 'ᴅɪsᴀʙʟᴇᴅ'}.")
            except Exception:
                log.exception("loop_handler failed")
                await self.safe_send(message, "❌ ʟᴏᴏᴘ ᴜᴘᴅᴀᴛᴇ ɴᴀʜɪ ʜᴜᴀ.")

        # ── /shuffle
        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def shuffle_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if len(state.queue) < 2:
                    return await self.safe_send(message, "🔀 sʜᴜꜰꜰʟᴇ ᴋᴇ ʟɪᴇ ᴋᴀᴍ sᴇ ᴋᴀᴍ 2 ᴛʀᴀᴄᴋs ᴄʜᴀʜɪᴇ.")
                random.shuffle(state.queue)
                await self.safe_send(message, f"🔀 ǫᴜᴇᴜᴇ sʜᴜꜰꜰʟᴇᴅ — {len(state.queue)} ᴛʀᴀᴄᴋs.")
            except Exception:
                log.exception("shuffle_handler failed")
                await self.safe_send(message, "❌ sʜᴜꜰꜰʟᴇ ɴᴀʜɪ ʜᴜᴀ.")

        # ── /clearqueue
        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def clearqueue_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                count = len(state.queue)
                state.queue.clear()
                await self.safe_send(message, f"🧹 {count} ᴛʀᴀᴄᴋ(s) ʀᴇᴍᴏᴠᴇᴅ ꜰʀᴏᴍ ǫᴜᴇᴜᴇ.")
            except Exception:
                log.exception("clearqueue_handler failed")
                await self.safe_send(message, "❌ ǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ ɴᴀʜɪ ʜᴜᴀ.")

        # ── /mute /unmute
        @self.bot.on_message(filters.command(["mute"]) & filters.group)
        async def mute_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                await self.mute_call_safely(message.chat.id)
                self.get_state(message.chat.id).muted = True
                await self.safe_send(message, "🔇 ᴠᴄ ᴍᴜᴛᴇᴅ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ ᴍᴜᴛᴇ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

        @self.bot.on_message(filters.command(["unmute"]) & filters.group)
        async def unmute_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                await self.unmute_call_safely(message.chat.id)
                self.get_state(message.chat.id).muted = False
                await self.safe_send(message, "🔊 ᴠᴄ ᴜɴᴍᴜᴛᴇᴅ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ ᴜɴᴍᴜᴛᴇ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

        # ── /np /now
        @self.bot.on_message(filters.command(["np", "now"]) & filters.group)
        async def np_handler(_, message: Message):
            try:
                state = self.get_state(message.chat.id)
                await self.safe_send(message, self.now_playing_text(state), reply_markup=self.np_keyboard())
            except Exception:
                log.exception("np_handler failed")
                await self.safe_send(message, "❌ ɴᴏᴡ ᴘʟᴀʏɪɴɢ ᴘᴀɴᴇʟ ʟᴏᴀᴅ ɴᴀʜɪ ʜᴜᴀ.")

        # ── /shelp (owner only)
        @self.bot.on_message(filters.command(["shelp"]) & (filters.private | filters.group))
        async def shelp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return
                await self.safe_send(message, self.shell_help_text())
            except Exception:
                log.exception("shelp_handler failed")

        # ── /setdp
        @self.bot.on_message(filters.command(["setdp"]) & filters.private)
        async def setdp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return await self.safe_send(message, "❌ ʏᴇ ᴄᴏᴍᴍᴀɴᴅ sɪʀꜰ ᴏᴡɴᴇʀ ᴜsᴇ ᴋᴀʀ sᴀᴋᴛᴀ ʜᴀɪ.")
                self.pending_start_photo[message.from_user.id] = time.time()
                await self.safe_send(
                    message,
                    "🖼 sᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ sᴇᴛ ᴍᴏᴅᴇ ᴇɴᴀʙʟᴇᴅ.\n\n"
                    "ᴀʙ ᴍᴜᴊʜᴇ ᴘʜᴏᴛᴏ ʙʜᴇᴊᴏ.\n"
                    "ᴄᴀɴᴄᴇʟ ᴋᴇ ʟɪᴇ /cancel ʟɪᴋʜᴏ."
                )
            except Exception:
                log.exception("setdp_handler failed")

        # ── /removedp
        @self.bot.on_message(filters.command(["removedp"]) & filters.private)
        async def removedp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return await self.safe_send(message, "❌ ʏᴇ ᴄᴏᴍᴍᴀɴᴅ sɪʀꜰ ᴏᴡɴᴇʀ ᴜsᴇ ᴋᴀʀ sᴀᴋᴛᴀ ʜᴀɪ.")
                self.settings["start_photo_file_id"] = ""
                self.save_settings()
                self.pending_start_photo.pop(message.from_user.id, None)
                await self.safe_send(message, "✅ sᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ʀᴇᴍᴏᴠᴇ ᴋᴀʀ ᴅɪ ɢᴀʏɪ ʜᴀɪ.")
            except Exception:
                log.exception("removedp_handler failed")

        # ── photo upload for /setdp
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
                    return await self.safe_send(message, "❌ sɪʀꜰ ɪᴍᴀɢᴇ/ᴘʜᴏᴛᴏ ʙʜᴇᴊᴏ.")
                self.settings["start_photo_file_id"] = file_id
                self.save_settings()
                self.pending_start_photo.pop(message.from_user.id, None)
                await self.safe_send(message, "✅ sᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ sᴀᴠᴇᴅ!\nᴀʙ /start ᴘᴀɴᴇʟ ᴘᴇ ʏᴇ ᴘʜᴏᴛᴏ ᴅɪᴋʜᴇɢɪ.")
            except Exception:
                log.exception("private_media_handler failed")
                await self.safe_send(message, "❌ ᴘʜᴏᴛᴏ sᴀᴠᴇ ɴᴀʜɪ ʜᴜᴀ.")

        # ── clone flow (master only)
        if self.is_master:

            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def clone_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return await self.safe_send(message, "❌ ᴏᴡɴᴇʀ ᴏɴʟʏ ᴄᴏᴍᴍᴀɴᴅ.")
                    self.clone_flow[message.from_user.id] = {"step": "bot_token"}
                    await self.safe_send(
                        message,
                        f"🤖 ɴᴇᴡ ʙᴏᴛ sᴇᴛᴜᴘ sᴛᴀʀᴛᴇᴅ\n"
                        f"{sep()}\n\n"
                        f"sᴛᴇᴘ 1/4:\nɴᴀʏᴀ ʙᴏᴛ ᴛᴏᴋᴇɴ ʙʜᴇᴊᴏ.\n\n"
                        f"ᴇxᴀᴍᴘʟᴇ:\n123456789:ABCDEF...\n\n"
                        f"ᴄᴀɴᴄᴇʟ ᴋᴇ ʟɪᴇ /cancel"
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
                        await self.safe_send(message, "🛑 ᴄᴜʀʀᴇɴᴛ sᴇᴛᴜᴘ ᴄᴀɴᴄᴇʟʟᴇᴅ.")
                    else:
                        await self.safe_send(message, "✅ ɴᴏᴛʜɪɴɢ ᴘᴇɴᴅɪɴɢ.")
                except Exception:
                    log.exception("cancel_handler failed")

            @self.bot.on_message(filters.command(["clones"]) & filters.private)
            async def clones_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return await self.safe_send(message, "❌ ᴏᴡɴᴇʀ ᴏɴʟʏ ᴄᴏᴍᴍᴀɴᴅ.")
                    files = sorted(CLONES_DIR.glob("*.json"))
                    if not files:
                        return await self.safe_send(message, "📭 ᴋᴏɪ sᴀᴠᴇᴅ ʙᴏᴛ ᴄᴏɴꜰɪɢ ɴᴀʜɪ ᴍɪʟᴀ.")
                    lines = [f"📦 sᴀᴠᴇᴅ ʙᴏᴛ ᴄᴏɴꜰɪɢs\n{sep()}\n"]
                    for f in files[:50]:
                        try:
                            cfg = load_config(f)
                            lines.append(f"• {escape_html(cfg.bot_id)} — {escape_html(cfg.owner_username)} — {escape_html(cfg.support_chat)}")
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
                    state_flow = self.clone_flow.get(message.from_user.id)
                    if not state_flow:
                        return
                    text = (message.text or "").strip()
                    step = state_flow.get("step")

                    if text.lower() in {"/cancel", "/clone", "/clones", "/setdp", "/removedp"}:
                        return

                    if step == "bot_token":
                        if not TOKEN_RE.match(text):
                            return await self.safe_send(message, "❌ ɪɴᴠᴀʟɪᴅ ʙᴏᴛ ᴛᴏᴋᴇɴ.\nᴅᴏʙᴀʀᴀ sᴀʜɪ ᴛᴏᴋᴇɴ ʙʜᴇᴊᴏ.")
                        state_flow["bot_token"] = text
                        state_flow["step"] = "support"
                        return await self.safe_send(
                            message,
                            f"sᴛᴇᴘ 2/4:\nsᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ ᴜsᴇʀɴᴀᴍᴇ ʏᴀ ʟɪɴᴋ ʙʜᴇᴊᴏ.\n\nᴇxᴀᴍᴘʟᴇ:\n@yoursupportchat"
                        )

                    if step == "support":
                        state_flow["support_chat"] = normalize_support(text)
                        state_flow["step"] = "owner_username"
                        return await self.safe_send(
                            message,
                            f"sᴛᴇᴘ 3/4:\nᴏᴡɴᴇʀ ᴜsᴇʀɴᴀᴍᴇ ʏᴀ ʟɪɴᴋ ʙʜᴇᴊᴏ.\n\nᴇxᴀᴍᴘʟᴇ:\n@YourUsername"
                        )

                    if step == "owner_username":
                        state_flow["owner_username"] = normalize_owner_username(text)
                        state_flow["step"] = "session"
                        return await self.safe_send(
                            message,
                            f"sᴛᴇᴘ 4/4:\nᴀssɪsᴛᴀɴᴛ sᴛʀɪɴɢ sᴇssɪᴏɴ ʙʜᴇᴊᴏ.\n"
                            f"ʏᴀ sᴀᴍᴇ ᴅᴇꜰᴀᴜʟᴛ ᴜsᴇ ᴋᴀʀɴᴀ ʜᴀɪ ᴛᴏ /default ʟɪᴋʜᴏ."
                        )

                    if step == "session":
                        session_string = (
                            self.config.assistant_session
                            if text.lower() == "/default"
                            else text
                        )
                        if len(session_string) < 50:
                            return await self.safe_send(message, "❌ sᴇssɪᴏɴ sᴛʀɪɴɢ ʙᴀʜᴜᴛ ᴄʜᴏᴛɪ ʟᴀɢ ʀᴀʜɪ ʜᴀɪ.\nᴅᴏʙᴀʀᴀ sʜᴀʀᴇ ᴋᴀʀᴏ.")

                        # verify assistant identity on new session
                        await self.safe_send(message, "⏳ ᴀssɪsᴛᴀɴᴛ ᴠᴇʀɪꜰʏ ᴋᴀʀ ʀᴀʜᴀ ʜᴜɴ...")
                        try:
                            temp_client = Client(
                                name=f"verify_{int(time.time())}",
                                api_id=self.config.api_id,
                                api_hash=self.config.api_hash,
                                session_string=session_string,
                            )
                            await temp_client.start()
                            asst_me = await temp_client.get_me()
                            asst_username = asst_me.username or "NoUsername"
                            asst_id       = asst_me.id
                            asst_name     = asst_me.first_name or "Assistant"
                            await temp_client.stop()
                            await self.safe_send(
                                message,
                                f"✅ ᴀssɪsᴛᴀɴᴛ ꜰᴇᴛᴄʜ ʜᴏ ɢᴀʏᴀ!\n\n"
                                f"👤 ɴᴀᴍᴇ    : {escape_html(asst_name)}\n"
                                f"🔗 ᴜsᴇʀɴᴀᴍᴇ: @{escape_html(asst_username)}\n"
                                f"🆔 ᴜsᴇʀɪᴅ  : {asst_id}\n\n"
                                f"ᴀssɪsᴛᴀɴᴛ sᴇᴛ ʜᴏ ɢᴀʏᴀ — ɪssᴇ ᴀᴘɴᴇ ɢʀᴏᴜᴘ ᴍᴇ ᴀᴅᴅ ᴋᴀʀᴏ ᴀᴜʀ ᴠᴄ sᴛᴀʀᴛ ᴋᴀʀᴏ."
                            )
                        except Exception as ve:
                            await self.safe_send(message, f"⚠️ sᴇssɪᴏɴ ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ ꜰᴀɪʟᴇᴅ: {escape_html(str(ve))}\nᴘʀᴏᴄᴇᴇᴅɪɴɢ ᴀɴʏᴡᴀʏ...")

                        clone_cfg = BotConfig(
                            api_id=self.config.api_id,
                            api_hash=self.config.api_hash,
                            bot_token=state_flow["bot_token"],
                            owner_id=self.config.owner_id,
                            assistant_session=session_string,
                            support_chat=state_flow["support_chat"],
                            owner_username=state_flow["owner_username"],
                            nubcoder_token=self.config.nubcoder_token,
                            clone_mode=True,
                        )
                        self.clone_flow.pop(message.from_user.id, None)

                        config_file = CLONES_DIR / f"{clone_cfg.bot_id}.json"
                        save_config(clone_cfg, config_file)

                        log_file = LOGS_DIR / f"{clone_cfg.bot_id}.log"
                        pid_file = PIDS_DIR / f"{clone_cfg.bot_id}.pid"

                        try:
                            proc = subprocess.Popen(
                                [sys.executable, __file__, "--config", str(config_file)],
                                stdout=open(str(log_file), "a"),
                                stderr=subprocess.STDOUT,
                                start_new_session=True,
                            )
                            pid_file.write_text(str(proc.pid))
                            await self.safe_send(
                                message,
                                f"🚀 ʙᴏᴛ ʟᴀᴜɴᴄʜ ʜᴏ ɢᴀʏᴀ!\n"
                                f"{sep()}\n\n"
                                f"🤖 ʙᴏᴛ ɪᴅ : {escape_html(clone_cfg.bot_id)}\n"
                                f"💬 sᴜᴘᴘᴏʀᴛ: {escape_html(clone_cfg.support_chat)}\n"
                                f"👤 ᴏᴡɴᴇʀ  : {escape_html(clone_cfg.owner_username)}\n"
                                f"🆔 ᴘɪᴅ    : {proc.pid}"
                            )
                        except Exception as pe:
                            await self.safe_send(message, f"❌ ʙᴏᴛ ʟᴀᴜɴᴄʜ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(pe))}")

                except Exception:
                    log.exception("clone_flow_handler failed")
                    await self.safe_send(message, "❌ ɴᴀʏᴀ ʙᴏᴛ ᴄʀᴇᴀᴛᴇ ᴋᴀʀᴛᴇ ᴛɪᴍᴇ ᴇʀʀᴏʀ ᴀᴀʏᴀ.")

    # ─────────────────────────────────────
    #  START / STOP
    # ─────────────────────────────────────

    async def start(self) -> None:
        if shutil.which("ffmpeg") is None:
            log.warning("ffmpeg not found in PATH. Playback may fail.")

        await self.add_handlers()

        # ── start assistant FIRST, keep its session (not default)
        await self.assistant.start()
        assistant_me = await self.assistant.get_me()
        self.assistant_id       = assistant_me.id
        self.assistant_name     = assistant_me.first_name or "Assistant"
        self.assistant_username = assistant_me.username or ""
        log.info(
            "ASSISTANT | @%s | id=%s | name=%s",
            self.assistant_username, self.assistant_id, self.assistant_name
        )

        # ── start bot, fetch its REAL name from Telegram
        await self.bot.start()
        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name     = me.first_name or ""
        self.bot_id_int   = me.id
        # store real name back into config so all UI uses it
        if self.bot_name:
            self.config.brand_name = self.bot_name

        await self.calls.start()

        log.info(
            "RUNNING | %s | @%s | bot_id=%s | assistant=@%s(%s)",
            self.bot_name, self.bot_username, self.config.bot_id,
            self.assistant_username, self.assistant_id,
        )
        await idle()

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        for name, action in (
            ("calls.stop",     getattr(self.calls,     "stop", None)),
            ("bot.stop",       getattr(self.bot,       "stop", None)),
            ("assistant.stop", getattr(self.assistant, "stop", None)),
        ):
            try:
                if action:
                    result = action()
                    if asyncio.iscoroutine(result):
                        await result
            except Exception:
                log.exception("%s failed", name)

# ═══════════════════════════════════════════
#  SUPERVISOR
# ═══════════════════════════════════════════

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
            tagline=BOT_BRAND_TAGLINE,
        )
        app = TelegramMusicBot(master_cfg, is_master=True)

    try:
        await app.start()
    finally:
        await app.stop()


async def supervisor() -> None:
    restart_delay     = int(os.getenv("CLONE_RESTART_DELAY", "5") or "5")
    max_restart_delay = int(os.getenv("MAX_RESTART_DELAY",  "60") or "60")
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
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    try:
        asyncio.run(supervisor())
    except KeyboardInterrupt:
        log.info("Shutdown requested, exiting...")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   ██╗   ██╗██╗  ████████╗██████╗  █████╗                    ║
║   ██║   ██║██║  ╚══██╔══╝██╔══██╗██╔══██╗                   ║
║   ██║   ██║██║     ██║   ██████╔╝███████║                   ║
║   ██║   ██║██║     ██║   ██╔══██╗██╔══██║                   ║
║   ╚██████╔╝███████╗██║   ██║  ██║██║  ██║                   ║
║    ╚═════╝ ╚══════╝╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝                   ║
║                                                              ║
║        ♫  TELEGRAM MUSIC BOT  —  ULTRA v3  ♫                ║
║        Fast  •  Stable  •  Zero Error  •  Smart             ║
╚══════════════════════════════════════════════════════════════╝
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
    "pyrogram":  "pyrogram>=2.0.106",
    "tgcrypto":  "tgcrypto>=1.2.5",
    "pytgcalls": "py-tgcalls>=2.2.0",
    "yt_dlp":    "yt-dlp>=2025.3.31",
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
    RPCError = Exception            # type: ignore
    Forbidden = Exception           # type: ignore
    BadRequest = Exception          # type: ignore

if hasattr(pyro_errors, "GroupcallForbidden"):
    GroupcallForbidden = pyro_errors.GroupcallForbidden
else:
    class GroupcallForbidden(Forbidden):  # type: ignore
        ID = "GROUPCALL_FORBIDDEN"
        MESSAGE = "The group call is not accessible."
    pyro_errors.GroupcallForbidden = GroupcallForbidden  # type: ignore

from pytgcalls import PyTgCalls
from yt_dlp import YoutubeDL

# ─────────────────────────────────────────────────────
#  PYTGCALLS STREAM TYPES — UNIVERSAL COMPAT IMPORT
#  Handles v1, v2, v3, v4+ of py-tgcalls automatically
# ─────────────────────────────────────────────────────

_AudioPiped        = None
_MediaStream       = None
_AudioStream       = None
_VideoStream       = None
_MediaType         = None

try:
    from pytgcalls.types import MediaStream as _MediaStream      # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import MediaStream as _MediaStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types import AudioStream as _AudioStream      # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import AudioStream as _AudioStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types import VideoStream as _VideoStream      # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import VideoStream as _VideoStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types import MediaType as _MediaType          # type: ignore
except ImportError:
    pass

try:
    from pytgcalls.types.input_stream import AudioPiped as _AudioPiped  # type: ignore
except ImportError:
    try:
        from pytgcalls.types.input_stream.quality import AudioPiped as _AudioPiped  # type: ignore
    except ImportError:
        pass

_StreamEndedCompat      = None
_StreamAudioEndedCompat = None

try:
    from pytgcalls.types import StreamEnded as _StreamEndedCompat       # type: ignore
except ImportError:
    pass

try:
    from pytgcalls.types.stream import StreamAudioEnded as _StreamAudioEndedCompat  # type: ignore
except ImportError:
    try:
        from pytgcalls.types import StreamAudioEnded as _StreamAudioEndedCompat     # type: ignore
    except ImportError:
        pass

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

API_ID                    = int(os.getenv("API_ID", "0") or "0")
API_HASH                  = os.getenv("API_HASH", "")
MAIN_BOT_TOKEN            = os.getenv("MAIN_BOT_TOKEN", "")
OWNER_ID                  = int(os.getenv("OWNER_ID", "0") or "0")
DEFAULT_ASSISTANT_SESSION = os.getenv("DEFAULT_ASSISTANT_SESSION", "")
MASTER_SUPPORT_CHAT       = os.getenv("MASTER_SUPPORT_CHAT", "@support")
MASTER_OWNER_USERNAME     = os.getenv("MASTER_OWNER_USERNAME", "@owner")
BOT_BRAND_TAGLINE         = os.getenv("BOT_BRAND_TAGLINE", "𝗙𝗮𝘀𝘁 • 𝗦𝘁𝗮𝗯𝗹𝗲 • 𝗦𝗺𝗼𝗼𝘁𝗵 𝗩𝗖 𝗣𝗹𝗮𝘆𝗲𝗿")
NUBCODER_TOKEN            = os.getenv("NUBCODER_TOKEN", "")

ROOT_RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", str(Path(__file__).resolve().parent / "runtime"))).resolve()
ROOT_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
CLONES_DIR = ROOT_RUNTIME_DIR / "clones"; CLONES_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR   = ROOT_RUNTIME_DIR / "logs";   LOGS_DIR.mkdir(parents=True, exist_ok=True)
PIDS_DIR   = ROOT_RUNTIME_DIR / "pids";   PIDS_DIR.mkdir(parents=True, exist_ok=True)

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
    brand_name: str = ""
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
    is_video: bool = False

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
    matched = False
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if value.startswith(prefix):
            value = "@" + value.split(prefix, 1)[1].strip("/")
            matched = True
            break
    if not matched and value and not value.startswith("@") and USERNAME_RE.fullmatch(value):
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
    if not cfg.api_id:             missing.append("API_ID")
    if not cfg.api_hash:           missing.append("API_HASH")
    if not cfg.bot_token:          missing.append("MAIN_BOT_TOKEN / clone bot_token")
    if not cfg.owner_id:           missing.append("OWNER_ID")
    if not cfg.assistant_session:  missing.append("DEFAULT_ASSISTANT_SESSION / clone assistant_session")
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
    return "✅ ᴏɴ" if value else "❌ ᴏꜰꜰ"

def pretty_uptime(seconds: int) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s   = divmod(rem, 60)
    if d:  return f"{d}d {h}h {m}m"
    if h:  return f"{h}h {m}m {s}s"
    if m:  return f"{m}m {s}s"
    return f"{s}s"

# ═══════════════════════════════════════════
#  YT-DLP — FAST EXTRACTION
# ═══════════════════════════════════════════

def sync_extract_track(query: str, want_video: bool = False) -> Track:
    if want_video:
        fmt = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
    else:
        fmt = "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best"

    ydl_opts = {
        "format": fmt,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "default_search": "ytsearch1",
        "skip_download": True,
        "geo_bypass": True,
        "extract_flat": False,
        "nocheckcertificate": True,
        "source_address": "0.0.0.0",
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
            raise ValueError("ᴋᴏɪ ʀᴇsᴜʟᴛ ɴᴀʜɪ ᴍɪʟᴀ.")
        if "entries" in info:
            entries = info.get("entries") or []
            info = next((x for x in entries if x), None)
            if not info:
                raise ValueError("ᴋᴏɪ ᴘʟᴀʏᴀʙʟᴇ ʀᴇsᴜʟᴛ ɴᴀʜɪ ᴍɪʟᴀ.")
        stream_url  = info.get("url")
        webpage_url = info.get("webpage_url") or info.get("original_url") or query
        title       = info.get("title") or "Unknown Title"
        duration    = int(info.get("duration") or 0)
        source_name = info.get("extractor_key") or info.get("extractor") or "Media"
        thumb       = info.get("thumbnail") or ""
        if not stream_url:
            raise ValueError("ꜱᴛʀᴇᴀᴍ ᴜʀʟ ʀᴇꜱᴏʟᴠᴇ ɴᴀʜɪ ʜᴜᴀ.")
        return Track(
            title=title, stream_url=stream_url, webpage_url=webpage_url,
            duration=duration, source=source_name, thumbnail=thumb,
            is_video=want_video
        )

# ═══════════════════════════════════════════
#  STYLED UI HELPERS
# ═══════════════════════════════════════════

def sep() -> str:
    return "•───────────────────────────────•"

def sep_thin() -> str:
    return "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"

def box(text: str) -> str:
    return f"  ◈  {text}"

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

        self.states:              Dict[int, ChatState]       = {}
        self.chat_locks:          Dict[int, asyncio.Lock]    = {}
        self.clone_flow:          Dict[int, Dict[str, Any]]  = {}
        self.pending_start_photo: Dict[int, float]           = {}

        # filled at runtime
        self.bot_username:       str  = ""
        self.bot_name:           str  = ""
        self.bot_id_int:         int  = 0
        self.assistant_id:       int  = 0
        self.assistant_username: str  = ""
        self.assistant_name:     str  = "Assistant"
        self._stopping:          bool = False

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
        try:
            tmp_path = self.settings_path.with_suffix(".tmp")
            tmp_path.write_text(
                json.dumps(self.settings, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            tmp_path.replace(self.settings_path)
            log.info("Settings saved: %s", self.settings_path)
        except Exception:
            log.exception("save_settings failed")
            try:
                self.settings_path.write_text(
                    json.dumps(self.settings, indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )
            except Exception:
                log.exception("save_settings fallback also failed")

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
    #  RUNTIME IDENTITY
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
    #  UI TEXT
    # ─────────────────────────────────────

    def start_text(self, user_name: str = "") -> str:
        n   = escape_html(self.display_name)
        tag = escape_html(self.config.tagline)
        greet = (
            f"  ʜᴇʏ <b>{escape_html(user_name)}</b> 👋"
            if user_name else
            "  ʜᴇʏ ᴛʜᴇʀᴇ 👋"
        )
        return (
            f"╔══════════════════════════╗\n"
            f"║   🎵  <b>{n.upper()}</b>  🎵   ║\n"
            f"╚══════════════════════════╝\n\n"
            f"{greet}\n"
            f"{sep()}\n\n"
            f"❝ <i>ᴍᴜsɪᴄ ɪs ᴛʜᴇ sʜᴏʀᴛʜᴀɴᴅ ᴏꜰ ᴇᴍᴏᴛɪᴏɴ.</i> ❞\n\n"
            f"{sep_thin()}\n\n"
            f"{box('<b>ꜱᴜᴘᴇʀ ꜰᴀꜱᴛ</b> ʏᴏᴜᴛᴜʙᴇ ᴘʟᴀʏʙᴀᴄᴋ')}\n"
            f"{box('<b>ꜱᴍᴀʀᴛ ǫᴜᴇᴜᴇ</b> + ʟᴏᴏᴘ + ꜱʜᴜꜰꜰʟᴇ')}\n"
            f"{box('<b>ɪɴꜱᴛᴀɴᴛ</b> ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴊᴏɪɴ')}\n"
            f"{box('<b>ꜱᴇᴀᴍʟᴇꜱꜱ</b> ᴍᴜʟᴛɪ-ɢʀᴏᴜᴘ ꜱᴜᴘᴘᴏʀᴛ')}\n\n"
            f"{sep_thin()}\n\n"
            f"  {tag}\n\n"
            f"{sep()}\n\n"
            f"  <b>🎧 /play</b> <i>sᴏɴɢ ɴᴀᴍᴇ</i>  →  ɪɴsᴛᴀɴᴛ ᴍᴜsɪᴄ!\n"
            f"  ᴄʟɪᴄᴋ <b>ʜᴇʟᴘ</b> ʙᴜᴛᴛᴏɴ ꜰᴏʀ ᴀʟʟ ᴄᴏᴍᴍᴀɴᴅs 👇"
        )

    def about_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"╔══════════════════════════╗\n"
            f"║  ✨  ᴀʙᴏᴜᴛ  <b>{n.upper()}</b>  ║\n"
            f"╚══════════════════════════╝\n\n"
            f"❝ <i>ᴡʜᴇʀᴇ ᴡᴏʀᴅs ꜰᴀɪʟ, ᴍᴜsɪᴄ sᴘᴇᴀᴋs.</i> ❞\n\n"
            f"{sep()}\n\n"
            f"{box('ꜱᴍᴏᴏᴛʜ ᴠᴄ ᴘʟᴀʏʙᴀᴄᴋ ᴇɴɢɪɴᴇ')}\n"
            f"{box('ꜰʀɪᴇɴᴅʟʏ ᴇʀʀᴏʀ ᴅɪᴀɢɴᴏꜱᴛɪᴄꜱ')}\n"
            f"{box('ꜱᴍᴀʀᴛ ǫᴜᴇᴜᴇ, ʟᴏᴏᴘ, ꜱʜᴜꜰꜰʟᴇ')}\n"
            f"{box('ɪɴʟɪɴᴇ ʜᴇʟᴘ ᴇxᴘʟᴏʀᴇʀ')}\n"
            f"{box('ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ꜱᴜᴘᴘᴏʀᴛ ᴠɪᴀ /setdp')}\n\n"
            f"{sep()}\n\n"
            f"<b>ɢʀᴏᴜᴘ ꜱᴇᴛᴜᴘ:</b>\n"
            f"  1️⃣  ʙᴏᴛ ᴀᴅᴅ ᴋᴀʀᴏ ɢʀᴏᴜᴘ ᴍᴇ\n"
            f"  2️⃣  ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ\n"
            f"  3️⃣  <b>/play</b> ꜱᴏɴɢ ɴᴀᴍᴇ ʟɪᴋʜᴏ 🎶"
        )

    def help_home_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"📚 <b>{n.upper()} ʜᴇʟᴘ ᴘᴀɴᴇʟ</b>\n"
            f"{sep()}\n\n"
            f"❝ <i>ᴛʜᴇ ʙᴇꜱᴛ ᴍᴜꜱɪᴄ ɪꜱ ᴛʜᴇ ᴏɴᴇ ᴛʜᴀᴛ ᴍᴀᴋᴇꜱ ʏᴏᴜ ᴅᴀɴᴄᴇ.</i> ❞\n\n"
            f"{sep_thin()}\n\n"
            f"  ɴᴇᴄʜᴇ ꜱᴇᴄᴛɪᴏɴ ᴄʜᴜɴᴏ ᴀᴜʀ ᴄᴏᴍᴍᴀɴᴅꜱ ᴇxᴘʟᴏʀᴇ ᴋᴀʀᴏ.\n\n"
            f"  💡 <b>ᴛɪᴘ:</b>  /play sᴏɴɢ ɴᴀᴍᴇ"
        )

    def help_music_text(self) -> str:
        return (
            f"🎵 <b>ᴍᴜꜱɪᴄ ᴄᴏᴍᴍᴀɴᴅꜱ</b>\n"
            f"{sep()}\n\n"
            f"  /play  <code>sᴏɴɢ ɴᴀᴍᴇ / ᴜʀʟ</code>  →  ᴀᴜᴅɪᴏ ᴘʟᴀʏ\n"
            f"  /vplay <code>sᴏɴɢ ɴᴀᴍᴇ / ᴜʀʟ</code>  →  ᴠɪᴅᴇᴏ ᴘʟᴀʏ ᴠᴄ ᴘᴇ\n"
            f"  /p     <code>sᴏɴɢ</code>  →  /play ᴋᴀ ꜱʜᴏʀᴛ ꜰᴏʀᴍ\n"
            f"  /pause   →  ᴘᴀᴜꜱᴇ ᴄᴜʀʀᴇɴᴛ ꜱᴏɴɢ\n"
            f"  /resume  →  ʀᴇꜱᴜᴍᴇ ᴘᴀᴜꜱᴇᴅ ꜱᴏɴɢ\n"
            f"  /skip    →  ꜱᴋɪᴘ ᴄᴜʀʀᴇɴᴛ ᴛʀᴀᴄᴋ\n"
            f"  /next    →  /skip ᴀʟɪᴀꜱ\n"
            f"  /stop    →  ᴘʟᴀʏʙᴀᴄᴋ ʙɴᴅ ᴋᴀʀᴏ\n"
            f"  /end     →  /stop ᴀʟɪᴀꜱ\n"
            f"  /queue   →  ǫᴜᴇᴜᴇ ʟɪꜱᴛ ᴅᴇᴋʜᴏ\n"
            f"  /q       →  /queue ᴀʟɪᴀꜱ\n"
            f"  /np      →  ɴᴏᴡ ᴘʟᴀʏɪɴɢ ᴘᴀɴᴇʟ\n"
            f"  /now     →  /np ᴀʟɪᴀꜱ\n"
            f"  /refresh →  ɴᴏᴡ ᴘʟᴀʏɪɴɢ ʀᴇꜰʀᴇꜱʜ\n\n"
            f"{sep()}"
        )

    def help_admin_text(self) -> str:
        return (
            f"🛠 <b>ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟꜱ</b>\n"
            f"{sep()}\n\n"
            f"  /loop        →  ʟᴏᴏᴘ ᴛᴏɢɢʟᴇ\n"
            f"  /loop on     →  ʟᴏᴏᴘ ᴇɴᴀʙʟᴇ\n"
            f"  /loop off    →  ʟᴏᴏᴘ ᴅɪꜱᴀʙʟᴇ\n"
            f"  /shuffle     →  ǫᴜᴇᴜᴇ ꜱʜᴜꜰꜰʟᴇ\n"
            f"  /clearqueue  →  ǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ\n"
            f"  /mute        →  ᴠᴄ ᴍᴜᴛᴇ\n"
            f"  /unmute      →  ᴠᴄ ᴜɴᴍᴜᴛᴇ\n"
            f"  /ping        →  ʙᴏᴛ ꜱᴘᴇᴇᴅ / ꜱᴛᴀᴛᴜꜱ\n"
            f"  /alive       →  ʙᴏᴛ ᴏɴʟɪɴᴇ ᴄʜᴇᴄᴋ\n\n"
            f"⚠️ <b>ɴᴏᴛᴇ:</b> ᴀᴅᴍɪɴ-ᴏɴʟʏ ᴄᴏɴᴛʀᴏʟꜱ\n"
            f"{sep()}"
        )

    def help_extra_text(self) -> str:
        return (
            f"🧩 <b>ᴇxᴛʀᴀ ɪɴꜰᴏ</b>\n"
            f"{sep()}\n\n"
            f"{box('ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ꜱᴍᴏᴏᴛʜ ᴍɢᴍᴛ ᴋᴇ ʟɪᴇ')}\n"
            f"{box('/play ꜱᴇ ᴘᴇʜʟᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ')}\n"
            f"{box('ᴘʀɪᴠᴀᴛᴇ ɢʀᴏᴜᴘ ᴍᴇ ɪɴᴠɪᴛᴇ ʟɪɴᴋ ᴡᴏʀᴋ ᴋᴀʀᴇ')}\n"
            f"{box('ᴘᴇʀᴍɪꜱꜱɪᴏɴꜱ ꜰɪx ᴋᴀʀɴᴇ ᴋᴇ ʙᴀᴀᴅ /play ʀᴇᴛʀʏ')}\n\n"
            f"{sep()}"
        )

    def shell_help_text(self) -> str:
        return (
            f"🔐 <b>ʜɪᴅᴅᴇɴ ᴏᴡɴᴇʀ ᴘᴀɴᴇʟ</b>\n"
            f"{sep()}\n\n"
            f"  /shelp    →  ʏᴇ ᴘᴀɴᴇʟ\n"
            f"  /setdp    →  ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ꜱᴇᴛ ᴋᴀʀᴏ\n"
            f"  /removedp →  ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ʜᴀᴛᴀᴏ\n"
            f"  /clone    →  ɴᴀʏᴀ ʙᴏᴛ ꜱᴇᴛᴜᴘ ꜰʟᴏᴡ\n"
            f"  /dclone   →  ʙᴏᴛ ꜱᴛᴏᴘ ᴋᴀʀᴏ\n"
            f"  /cancel   →  ꜱᴇᴛᴜᴘ ᴄᴀɴᴄᴇʟ\n"
            f"  /clones   →  ꜱᴀᴠᴇᴅ ʙᴏᴛ ᴄᴏɴꜰɪɢꜱ ʟɪꜱᴛ\n\n"
            f"⚠️ <b>ꜱɪʀꜰ ᴏᴡɴᴇʀ ᴋᴇ ʟɪᴇ.</b>\n"
            f"{sep()}"
        )

    # ─────────────────────────────────────
    #  KEYBOARDS  (NO refresh button anywhere)
    # ─────────────────────────────────────

    def start_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ ɪɴ ʏᴏᴜʀ ɢʀᴏᴜᴘ ➕", url=self.add_to_group_url)],
            [
                InlineKeyboardButton("👑 ᴏᴡɴᴇʀ",  url=self.owner_url),
                InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ",  callback_data="nav_about"),
            ],
            [
                InlineKeyboardButton("💬 ꜱᴜᴘᴘᴏʀᴛ ↗", url=self.support_url),
                InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇ ↗",  url=self.support_url),
            ],
            [InlineKeyboardButton("📚 ʜᴇʟᴘ & ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="nav_help_home")],
        ])

    def help_home_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🎵 ᴍᴜꜱɪᴄ",  callback_data="help_music"),
                InlineKeyboardButton("🛠 ᴀᴅᴍɪɴ",  callback_data="help_admin"),
            ],
            [
                InlineKeyboardButton("🧩 ᴇxᴛʀᴀ",  callback_data="help_extra"),
                InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ",  callback_data="nav_about"),
            ],
            [
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home"),
                InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="nav_close"),
            ],
        ])

    def subpage_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⬅ ʙᴀᴄᴋ", callback_data="nav_help_home"),
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home"),
            ],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="nav_close")],
        ])

    def np_keyboard(self) -> InlineKeyboardMarkup:
        # NO refresh button — use /refresh command instead
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⏸ ᴘᴀᴜꜱᴇ",  callback_data="ctl_pause"),
                InlineKeyboardButton("▶ ʀᴇꜱᴜᴍᴇ", callback_data="ctl_resume"),
            ],
            [
                InlineKeyboardButton("⏭ ꜱᴋɪᴘ",  callback_data="ctl_skip"),
                InlineKeyboardButton("⏹ ꜱᴛᴏᴘ",  callback_data="ctl_stop"),
            ],
            [
                InlineKeyboardButton("📜 ǫᴜᴇᴜᴇ", callback_data="ctl_queue"),
            ],
        ])

    def queue_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔀 ꜱʜᴜꜰꜰʟᴇ", callback_data="ctl_shuffle"),
                InlineKeyboardButton("🧹 ᴄʟᴇᴀʀ",   callback_data="ctl_clearqueue"),
            ],
            [
                InlineKeyboardButton("🎵 ɴᴏᴡ ᴘʟᴀʏɪɴɢ", callback_data="ctl_np"),
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ",        callback_data="nav_home"),
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
            try:
                return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
            except Exception:
                log.exception("safe_send retry failed")
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
            try:
                return await msg.edit_text(text, disable_web_page_preview=True, **kwargs)
            except Exception:
                pass
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
                pass
        except Exception:
            log.exception("safe_edit_panel failed")
        return None

    async def try_delete(self, message: Message) -> None:
        try:
            await message.delete()
        except Exception:
            pass

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
                log.warning("send_start_panel: photo send failed (file_id=%s), fallback to text", photo_id)
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
            return False

    async def require_admin(self, message: Message) -> bool:
        ok = await self.is_admin(message.chat.id, getattr(message.from_user, "id", None))
        if not ok:
            await self.safe_send(message, "❌ ʏᴇ ᴄᴏɴᴛʀᴏʟ ꜱɪʀꜰ <b>ɢʀᴏᴜᴘ ᴀᴅᴍɪɴꜱ</b> ᴜꜱᴇ ᴋᴀʀ ꜱᴀᴋᴛᴇ ʜᴀɪɴ.")
        return ok

    # ─────────────────────────────────────
    #  TRACK RESOLUTION
    # ─────────────────────────────────────

    async def resolve_track(self, query: str, requested_by: str, want_video: bool = False) -> Track:
        track = await asyncio.to_thread(sync_extract_track, query, want_video)
        track.requested_by = requested_by
        return track

    # ─────────────────────────────────────
    #  PEER CACHE WARMUP
    # ─────────────────────────────────────

    async def warm_peer_cache(self, chat_id: int) -> None:
        try:
            await self.assistant.get_chat(chat_id)
            return
        except KeyError:
            pass
        except Exception:
            pass

        try:
            chat = await self.bot.get_chat(chat_id)
            username = getattr(chat, "username", None)
            if username:
                try:
                    await self.assistant.get_chat(f"@{username}")
                    return
                except Exception:
                    pass
            link, _ = await self.build_join_link(chat_id)
            if link:
                try:
                    await self.assistant.get_chat(link)
                except Exception:
                    pass
        except Exception:
            pass

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
        """
        Instantly ensures assistant is in the group.
        - If already in: returns immediately (True, None)
        - If not in: auto-joins using invite link or public username
        - If bot has no invite rights: returns helpful error
        """
        member = await self.assistant_member_info(chat_id)
        if member:
            status = getattr(getattr(member, "status", None), "name", "")
            if "BANNED" in status.upper() or "KICKED" in status.upper():
                return (
                    False,
                    "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ʙᴀɴ ʜᴀɪ ɢʀᴏᴜᴘ ᴍᴇ!\n\n"
                    "ᴘᴇʜʟᴇ ᴜɴʙᴀɴ ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."
                )
            return True, None

        # Not in group — try to auto join
        link, reason = await self.build_join_link(chat_id)
        if not link:
            return (
                False,
                f"⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ɢʀᴏᴜᴘ ᴍᴇ ɴᴀʜɪ ʜᴀɪ.\n"
                f"ᴊᴏɪɴ ʟɪɴᴋ ʙʜɪ ɴᴀʜɪ ʙɴᴀ ʀᴀʜᴀ.\n\n"
                f"ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ᴀᴜʀ <b>ɪɴᴠɪᴛᴇ ᴜꜱᴇʀꜱ</b> ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴏ."
                + (f"\n\n<code>{escape_html(reason or 'unknown')}</code>" if reason else "")
            )

        try:
            await self.assistant.join_chat(link)
            # Warm cache immediately after join — no sleep needed
            await self.warm_peer_cache(chat_id)
            return True, None
        except UserAlreadyParticipant:
            await self.warm_peer_cache(chat_id)
            return True, None
        except Exception as exc:
            err = str(exc).upper()
            if "BANNED" in err or "KICKED" in err or "USER_BANNED_IN_CHANNEL" in err:
                return (
                    False,
                    "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ʙᴀɴ ʜᴀɪ ɢʀᴏᴜᴘ ᴍᴇ!\n\nᴘᴇʜʟᴇ ᴜɴʙᴀɴ ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."
                )
            return (
                False,
                f"⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ᴊᴏɪɴ ɴᴀʜɪ ʜᴏ ᴘᴀᴀʏᴀ.\n\n"
                f"ʀᴇᴀꜱᴏɴ: <code>{escape_html(str(exc))}</code>"
            )

    async def diagnose_voice_issue(self, chat_id: int, exc: Exception) -> str:
        text = exc_text(exc).upper()
        bot_member       = await self.bot_member_info(chat_id)
        assistant_member = await self.assistant_member_info(chat_id)

        if not bot_member or not is_admin_status(getattr(bot_member, "status", None)):
            return "⚠️ ʙᴏᴛ ɢʀᴏᴜᴘ ᴍᴇ <b>ᴀᴅᴍɪɴ</b> ɴᴀʜɪ ʜᴀɪ.\nʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ, ꜰɪʀ /play ᴄʜᴀʟᴀᴏ."

        if any(x in text for x in ("NO ACTIVE GROUP CALL", "GROUPCALL_NOT_FOUND", "VOICE CHAT", "VIDEO CHAT")):
            return "⚠️ ᴀʙʜɪ ɢʀᴏᴜᴘ ᴍᴇ ᴋᴏɪ <b>ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴛɪᴠᴇ</b> ɴᴀʜɪ ʜᴀɪ.\nᴘᴇʜʟᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."

        if any(x in text for x in ("GROUPCALL_FORBIDDEN", "ALREADY ENDED")):
            return "⚠️ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴄᴇꜱꜱɪʙʟᴇ ɴᴀʜɪ ʜᴀɪ.\nᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴅᴜʙᴀʀᴀ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ ᴀᴜʀ /play ᴄʜᴀʟᴀᴏ."

        if not assistant_member:
            return "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ɢʀᴏᴜᴘ ᴍᴇ ɴᴀʜɪ ʜᴀɪ.\nᴘʟᴇᴀꜱᴇ /play ᴅᴜʙᴀʀᴀ ᴄʜᴀʟᴀᴏ."

        if any(x in text for x in ("BANNED", "KICKED", "USER_BANNED_IN_CHANNEL")):
            return "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ <b>ʙᴀɴ</b> ʜᴀɪ ᴛᴇʀᴇ ɢʀᴏᴜᴘ ᴍᴇ!\n\nᴘᴇʜʟᴇ ɪꜱᴋᴏ <b>ᴜɴʙᴀɴ</b> ᴋᴀʀᴏ, ᴘʜɪʀ /play ᴄʜᴀʟᴀᴏ."

        if any(x in text for x in ("CHAT_ADMIN_REQUIRED", "YOU MUST BE ADMIN")):
            return "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ᴋᴏ ᴠᴄ ᴊᴏɪɴ ᴋᴀʀɴᴇ ᴋᴇ ʟɪᴇ <b>ᴀᴅᴍɪɴ ʀɪɢʜᴛꜱ</b> ᴄʜᴀʜɪᴇ."

        return (
            "⚠️ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴇ ᴊᴜᴅɴᴇ ᴍᴇ ᴅɪᴋᴋᴀᴛ ᴀᴀʏɪ.\n"
            "ᴄʜᴇᴄᴋ ᴋᴀʀᴏ ᴋɪ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴛɪᴠᴇ ʜᴀɪ ᴀᴜʀ ʙᴏᴛ ᴀᴅᴍɪɴ ʜᴀɪ."
        )

    # ─────────────────────────────────────
    #  PYTGCALLS PLAY — UNIVERSAL COMPAT
    # ─────────────────────────────────────

    def _build_stream_objects(self, stream_url: str, is_video: bool = False) -> list:
        objs = []

        if is_video:
            # Video stream objects
            if _VideoStream is not None:
                try:
                    objs.append(_VideoStream(stream_url))
                except Exception:
                    pass
            if _MediaStream is not None:
                if _MediaType is not None:
                    for attr in ("VIDEO", "video"):
                        media_type_val = getattr(_MediaType, attr, None)
                        if media_type_val is not None:
                            try:
                                objs.append(_MediaStream(stream_url, media_type=media_type_val))
                            except Exception:
                                pass
                            break
                try:
                    objs.append(_MediaStream(stream_url))
                except Exception:
                    pass
        else:
            # Audio stream objects (preferred)
            if _MediaStream is not None:
                if _MediaType is not None:
                    for attr in ("AUDIO", "audio"):
                        media_type_val = getattr(_MediaType, attr, None)
                        if media_type_val is not None:
                            try:
                                objs.append(_MediaStream(stream_url, media_type=media_type_val))
                            except Exception:
                                pass
                            break
                try:
                    objs.append(_MediaStream(stream_url))
                except Exception:
                    pass
            if _AudioStream is not None:
                try:
                    objs.append(_AudioStream(stream_url))
                except Exception:
                    pass
            if _AudioPiped is not None:
                try:
                    objs.append(_AudioPiped(stream_url))
                except Exception:
                    pass

        return objs

    async def _call_method(self, method, chat_id: int, stream_obj=None, raw_url: str = "") -> bool:
        try:
            if stream_obj is not None:
                result = method(chat_id, stream_obj)
            else:
                result = method(chat_id, raw_url)
            if asyncio.iscoroutine(result):
                await result
            return True
        except Exception as exc:
            if is_voice_chat_error(exc):
                raise
            return False

    async def _play_via_pytgcalls(self, chat_id: int, stream_url: str, is_video: bool = False) -> None:
        stream_objects = self._build_stream_objects(stream_url, is_video=is_video)
        methods_priority = ["play", "join_group_call", "stream"]
        last_exc: Optional[Exception] = None

        for method_name in methods_priority:
            method = getattr(self.calls, method_name, None)
            if method is None:
                continue
            for stream_obj in stream_objects:
                try:
                    ok = await self._call_method(method, chat_id, stream_obj=stream_obj)
                    if ok:
                        log.info("play success via %s + %s", method_name, type(stream_obj).__name__)
                        return
                except Exception as exc:
                    if is_voice_chat_error(exc):
                        raise
                    last_exc = exc
            try:
                ok = await self._call_method(method, chat_id, raw_url=stream_url)
                if ok:
                    log.info("play success via %s + raw_url", method_name)
                    return
            except Exception as exc:
                if is_voice_chat_error(exc):
                    raise
                last_exc = exc

        raise RuntimeError(
            f"ᴋᴏɪ ᴄᴏᴍᴘᴀᴛɪʙʟᴇ ᴘʟᴀʏ ᴍᴇᴛʜᴏᴅ ɴᴀʜɪ ᴍɪʟᴀ.\n"
            f"ᴄʜᴇᴄᴋ ᴋᴀʀᴏ: py-tgcalls ᴀᴜʀ ffmpeg ɪɴꜱᴛᴀʟ ʜᴇ.\n"
            f"ʟᴀꜱᴛ ᴇʀʀᴏʀ: {escape_html(str(last_exc))}"
        )

    async def play_track(self, chat_id: int, track: Track) -> None:
        """
        INSTANT PLAY — pre-joins assistant & warms cache in parallel with NO delay.
        Flow:
          1. Parallel: ensure_assistant_in_chat + warm_peer_cache
          2. Play immediately
          3. On VC error: already joined, so just diagnose
        """
        # Step 1: Parallel join + cache warm — INSTANT, no sleep
        join_task  = asyncio.ensure_future(self.ensure_assistant_in_chat(chat_id))
        cache_task = asyncio.ensure_future(self.warm_peer_cache(chat_id))
        await asyncio.gather(join_task, cache_task, return_exceptions=True)

        join_ok, join_reason = join_task.result() if not join_task.exception() else (False, str(join_task.exception()))
        if not join_ok:
            raise RuntimeError(join_reason or "ᴠᴄ ᴍᴇᴍʙᴇʀ ᴊᴏɪɴ ɴᴀʜɪ ʜᴜᴀ.")

        # Step 2: Play
        try:
            await self._play_via_pytgcalls(chat_id, track.stream_url, is_video=track.is_video)
        except Exception as first_exc:
            if not is_voice_chat_error(first_exc):
                raise RuntimeError(
                    f"⚠️ ᴀᴜᴅɪᴏ ᴘʟᴀʏ ɴᴀʜɪ ʜᴜᴀ.\n"
                    f"ʀᴇᴀꜱᴏɴ: {escape_html(str(first_exc))}"
                ) from first_exc

            # VC error after join — diagnose properly
            friendly = await self.diagnose_voice_issue(chat_id, first_exc)
            raise RuntimeError(friendly) from first_exc

        state = self.get_state(chat_id)
        state.current = track
        state.paused  = False
        state.muted   = False

    # ─────────────────────────────────────
    #  CALL CONTROLS — safe wrappers
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
        raise RuntimeError("ᴘᴀᴜꜱᴇ ᴍᴇᴛʜᴏᴅ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ.")

    async def resume_call_safely(self, chat_id: int):
        for method_name in ("resume", "resume_stream"):
            method = getattr(self.calls, method_name, None)
            if method:
                result = method(chat_id)
                if asyncio.iscoroutine(result):
                    await result
                return
        raise RuntimeError("ʀᴇꜱᴜᴍᴇ ᴍᴇᴛʜᴏᴅ ᴜɴᴀᴠᴀɪʟᴀʙʟᴇ.")

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
            return "❌ ᴀʙʜɪ ᴋᴜᴄʜ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ ʀᴀʜᴀ."
        t = state.current
        mode = "📹 ᴠɪᴅᴇᴏ" if t.is_video else "🎵 ᴀᴜᴅɪᴏ"
        return (
            f"🎵 <b>ɴᴏᴡ ᴘʟᴀʏɪɴɢ</b>\n"
            f"{sep()}\n\n"
            f"🏷 <b>ᴛɪᴛʟᴇ</b>   : {escape_html(t.title)}\n"
            f"⏱ <b>ᴅᴜʀᴀᴛɪᴏɴ</b>: {escape_html(t.pretty_duration)}\n"
            f"🌐 <b>ꜱᴏᴜʀᴄᴇ</b>  : {escape_html(t.source)}\n"
            f"🙋 <b>ʀᴇǫ ʙʏ</b>  : {t.requested_by}\n"
            f"📺 <b>ᴍᴏᴅᴇ</b>    : {mode}\n\n"
            f"{sep_thin()}\n\n"
            f"🔁 ʟᴏᴏᴘ  : {human_bool(state.loop)}\n"
            f"⏸ ᴘᴀᴜꜱᴇᴅ: {human_bool(state.paused)}\n"
            f"🔇 ᴍᴜᴛᴇᴅ : {human_bool(state.muted)}\n\n"
            f"{sep()}"
        )

    def queue_text(self, state: ChatState) -> str:
        if not state.current and not state.queue:
            return "📭 ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ."
        lines = [f"📜 <b>ǫᴜᴇᴜᴇ ᴘᴀɴᴇʟ</b>\n{sep()}\n"]
        if state.current:
            lines.append(
                f"🎵 <b>ᴄᴜʀʀᴇɴᴛ:</b> {escape_html(state.current.title)} "
                f"[{escape_html(state.current.pretty_duration)}]"
            )
        if state.queue:
            lines.append(f"\n<b>ᴜᴘ ɴᴇxᴛ:</b>")
            for i, track in enumerate(state.queue[:15], start=1):
                lines.append(f"  {i}. {escape_html(track.title)} — {escape_html(track.pretty_duration)}")
            if len(state.queue) > 15:
                lines.append(f"  ... ᴀɴᴅ {len(state.queue) - 15} ᴍᴏʀᴇ")
        lines.append(f"\n🔁 ʟᴏᴏᴘ  : {human_bool(state.loop)}")
        lines.append(f"⏸ ᴘᴀᴜꜱᴇᴅ: {human_bool(state.paused)}")
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
                        await self.bot.send_message(
                            chat_id,
                            f"❌ ɴᴇxᴛ ᴛʀᴀᴄᴋ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ ꜱᴀᴋᴀ.\n\n{escape_html(str(exc))}"
                        )
                    except Exception:
                        pass
                return

            if announce_chat:
                try:
                    text = (
                        f"▶️ <b>ɴᴏᴡ ᴘʟᴀʏɪɴɢ</b>\n"
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
            await self.play_next(chat_id, announce_chat=True, reason="ᴘʀᴇᴠɪᴏᴜꜱ ꜱᴛʀᴇᴀᴍ ᴇɴᴅᴇᴅ")
        except Exception:
            log.exception("on_stream_end failed")

    # ─────────────────────────────────────
    #  CORE PLAY HANDLER (shared by /play and /vplay)
    # ─────────────────────────────────────

    async def _handle_play(self, message: Message, query: str, want_video: bool = False) -> None:
        # Auto-delete the command message silently
        asyncio.ensure_future(self.try_delete(message))

        if not query:
            await self.safe_send(
                message,
                f"❓ <b>ᴜꜱᴀɢᴇ:</b>\n"
                f"  /{'vplay' if want_video else 'play'} <code>sᴏɴɢ ɴᴀᴍᴇ</code>\n"
                f"  /{'vplay' if want_video else 'play'} <code>youtube_url</code>"
            )
            return

        processing = await self.safe_send(
            message,
            f"🔎 <b>ꜱᴇᴀʀᴄʜɪɴɢ...</b>\n<code>{escape_html(query)}</code>"
        )

        # Parallel: resolve track + pre-join assistant — BOTH at same time = INSTANT
        try:
            track_task = asyncio.ensure_future(
                self.resolve_track(query, mention_user(message), want_video=want_video)
            )
            join_task = asyncio.ensure_future(
                self.ensure_assistant_in_chat(message.chat.id)
            )
            await asyncio.gather(track_task, join_task, return_exceptions=True)

            # Check track resolution
            if track_task.exception():
                await self.safe_edit_text(
                    processing,
                    f"❌ ꜱᴏɴɢ ɴᴀʜɪ ᴍɪʟᴀ\n\n<code>{escape_html(str(track_task.exception()))}</code>"
                )
                return
            track = track_task.result()

            # Check join result (non-fatal: we'll retry in play_track)
            join_ok, join_reason = (True, None)
            if not join_task.exception():
                join_ok, join_reason = join_task.result()

        except Exception as exc:
            await self.safe_edit_text(
                processing,
                f"❌ ꜱᴇᴀʀᴄʜ ᴇʀʀᴏʀ\n\n<code>{escape_html(str(exc))}</code>"
            )
            return

        async with self.get_lock(message.chat.id):
            state = self.get_state(message.chat.id)

            if state.current:
                state.queue.append(track)
                pos = len(state.queue)
                await self.safe_edit_text(
                    processing,
                    f"📥 <b>ǫᴜᴇᴜᴇᴅ ᴀᴛ #{pos}</b>\n\n"
                    f"🏷 {escape_html(track.title)}\n"
                    f"⏱ {escape_html(track.pretty_duration)}"
                )
                return

            await self.safe_edit_text(
                processing,
                f"⚡ ᴄᴏɴɴᴇᴄᴛɪɴɢ...\n🏷 <b>{escape_html(track.title)}</b>"
            )

            try:
                # play_track will use already-joined assistant (no re-join needed)
                await self.play_track(message.chat.id, track)
            except Exception as exc:
                await self.safe_edit_text(
                    processing,
                    f"❌ <b>ᴘʟᴀʏ ɴᴀʜɪ ʜᴜᴀ</b>\n\n{escape_html(str(exc))}"
                )
                return

        try:
            np_text = self.now_playing_text(self.get_state(message.chat.id))
            await self.safe_edit_text(processing, np_text, reply_markup=self.np_keyboard())
        except Exception:
            pass

    # ─────────────────────────────────────
    #  HANDLERS
    # ─────────────────────────────────────

    async def add_handlers(self) -> None:

        # ── Stream end handler (universal)
        @self.calls.on_update()
        async def stream_updates(_, update):
            try:
                name    = type(update).__name__.lower()
                chat_id = getattr(update, "chat_id", None)
                if not chat_id:
                    return
                if _StreamEndedCompat is not None and isinstance(update, _StreamEndedCompat):
                    return await self.on_stream_end(chat_id)
                if _StreamAudioEndedCompat is not None and isinstance(update, _StreamAudioEndedCompat):
                    return await self.on_stream_end(chat_id)
                if "ended" in name:
                    await self.on_stream_end(chat_id)
            except Exception:
                log.exception("stream_updates handler failed")

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

        # ── All callbacks
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
                    return await query.answer("ᴄʟᴏꜱᴇᴅ")

                # ── Music control callbacks (group only)
                if data.startswith("ctl_"):
                    chat_type = getattr(getattr(query.message, "chat", None), "type", None)
                    if chat_type and str(chat_type).lower() not in {
                        "group", "supergroup", "chattype.group", "chattype.supergroup"
                    }:
                        return await query.answer("ʏᴇ ᴄᴏɴᴛʀᴏʟ ɢʀᴏᴜᴘ ᴋᴇ ʟɪᴇ ʜᴀɪ.", show_alert=True)

                    user_id = getattr(query.from_user, "id", None)
                    if not await self.is_admin(query.message.chat.id, user_id):
                        return await query.answer("ꜱɪʀꜰ ᴀᴅᴍɪɴꜱ ᴄᴏɴᴛʀᴏʟ ᴜꜱᴇ ᴋᴀʀ ꜱᴀᴋᴛᴇ ʜᴀɪɴ.", show_alert=True)

                    chat_id = query.message.chat.id
                    state   = self.get_state(chat_id)

                    if data == "ctl_pause":
                        if state.paused:
                            return await query.answer("ᴘʟᴀʏʙᴀᴄᴋ ᴘᴇʜʟᴇ ꜱᴇ ʜɪ ᴘᴀᴜꜱᴇᴅ ʜᴀɪ.", show_alert=True)
                        try:
                            await self.pause_call_safely(chat_id)
                            state.paused = True
                            await self.safe_edit_panel(query.message, self.now_playing_text(state), self.np_keyboard())
                            return await query.answer("⏸ ᴘᴀᴜꜱᴇᴅ")
                        except Exception as exc:
                            return await query.answer((await self.diagnose_voice_issue(chat_id, exc))[:200], show_alert=True)

                    if data == "ctl_resume":
                        if not state.paused:
                            return await query.answer("ᴘʟᴀʏʙᴀᴄᴋ ᴀʟʀᴇᴀᴅʏ ᴄʜᴀʟ ʀᴀʜᴀ ʜᴀɪ.", show_alert=True)
                        try:
                            await self.resume_call_safely(chat_id)
                            state.paused = False
                            await self.safe_edit_panel(query.message, self.now_playing_text(state), self.np_keyboard())
                            return await query.answer("▶️ ʀᴇꜱᴜᴍᴇᴅ")
                        except Exception as exc:
                            return await query.answer((await self.diagnose_voice_issue(chat_id, exc))[:200], show_alert=True)

                    if data == "ctl_skip":
                        if not state.current and not state.queue:
                            return await query.answer("ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ.", show_alert=True)
                        try:
                            state.loop    = False
                            state.current = None
                            state.paused  = False
                            await self.play_next(chat_id, announce_chat=True, reason="ꜱᴋɪᴘᴘᴇᴅ ʙʏ ᴀᴅᴍɪɴ")
                            return await query.answer("⏭ ꜱᴋɪᴘᴘᴇᴅ")
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
                            return await query.answer("⏹ ꜱᴛᴏᴘᴘᴇᴅ")
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
                            return await query.answer("ꜱʜᴜꜰꜰʟᴇ ᴋᴇ ʟɪᴇ ᴋᴀᴍ ꜱᴇ ᴋᴀᴍ 2 ᴛʀᴀᴄᴋꜱ ᴄʜᴀʜɪᴇ.", show_alert=True)
                        random.shuffle(state.queue)
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer("🔀 ꜱʜᴜꜰꜰʟᴇᴅ!")

                    if data == "ctl_clearqueue":
                        count = len(state.queue)
                        state.queue.clear()
                        await self.safe_edit_panel(query.message, self.queue_text(state), self.queue_keyboard())
                        return await query.answer(f"🧹 {count} ᴛʀᴀᴄᴋꜱ ʀᴇᴍᴏᴠᴇᴅ")

                await query.answer()

            except Exception:
                log.exception("callback_handler failed")
                try:
                    await query.answer("❌ ꜱᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ.", show_alert=True)
                except Exception:
                    pass

        # ── /ping /alive
        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def ping_handler(_, message: Message):
            try:
                t0     = time.time()
                x      = await self.safe_send(message, "🏓 ᴘɪɴɢɪɴɢ...")
                taken  = (time.time() - t0) * 1000
                uptime = pretty_uptime(int(time.time() - self.start_time))
                active_chats = sum(1 for s in self.states.values() if s.current)
                n = escape_html(self.display_name)
                text = (
                    f"🏓 <b>{n.upper()} ɪꜱ ᴏɴʟɪɴᴇ</b>\n"
                    f"{sep()}\n\n"
                    f"⚡ ʟᴀᴛᴇɴᴄʏ   : <b>{taken:.2f} ᴍꜱ</b>\n"
                    f"⏳ ᴜᴘᴛɪᴍᴇ   : {escape_html(uptime)}\n"
                    f"🎧 ᴀᴄᴛɪᴠᴇ   : {active_chats} ᴄʜᴀᴛꜱ\n"
                    f"🤖 ʙᴏᴛ ɪᴅ   : <code>{escape_html(self.config.bot_id)}</code>\n\n"
                    f"{sep()}"
                )
                if x:
                    await self.safe_edit_text(x, text)
            except Exception:
                log.exception("ping_handler failed")

        # ── /play /p  — AUDIO
        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def play_handler(_, message: Message):
            try:
                await self._handle_play(message, command_arg(message), want_video=False)
            except Exception:
                log.exception("play_handler failed")
                await self.safe_send(message, "❌ /play ᴍᴇ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ.")

        # ── /vplay — VIDEO stream on VC
        @self.bot.on_message(filters.command(["vplay"]) & filters.group)
        async def vplay_handler(_, message: Message):
            try:
                await self._handle_play(message, command_arg(message), want_video=True)
            except Exception:
                log.exception("vplay_handler failed")
                await self.safe_send(message, "❌ /vplay ᴍᴇ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ.")

        # ── /refresh — manually refresh now playing panel
        @self.bot.on_message(filters.command(["refresh"]) & filters.group)
        async def refresh_handler(_, message: Message):
            try:
                asyncio.ensure_future(self.try_delete(message))
                state = self.get_state(message.chat.id)
                await self.safe_send(message, self.now_playing_text(state), reply_markup=self.np_keyboard())
            except Exception:
                log.exception("refresh_handler failed")

        # ── /pause
        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def pause_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if state.paused:
                    return await self.safe_send(message, "⏸ ᴘʟᴀʏʙᴀᴄᴋ ᴘᴇʜʟᴇ ꜱᴇ ʜɪ ᴘᴀᴜꜱᴇᴅ ʜᴀɪ.")
                await self.pause_call_safely(message.chat.id)
                state.paused = True
                await self.safe_send(message, "⏸ ᴘʟᴀʏʙᴀᴄᴋ ᴘᴀᴜꜱᴇᴅ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ ᴘᴀᴜꜱᴇ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

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
                await self.safe_send(message, "▶️ ᴘʟᴀʏʙᴀᴄᴋ ʀᴇꜱᴜᴍᴇᴅ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ ʀᴇꜱᴜᴍᴇ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

        # ── /skip /next
        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def skip_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if not state.current and not state.queue:
                    return await self.safe_send(message, "📭 ᴋᴜᴄʜ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ ʀᴀʜᴀ.")
                state.loop    = False
                state.current = None
                state.paused  = False
                await self.play_next(message.chat.id, announce_chat=True, reason="ꜱᴋɪᴘᴘᴇᴅ ʙʏ ᴀᴅᴍɪɴ")
            except Exception as exc:
                await self.safe_send(message, f"❌ ꜱᴋɪᴘ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

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
                await self.safe_send(message, "⏹ ᴘʟᴀʏʙᴀᴄᴋ ꜱᴛᴏᴘᴘᴇᴅ. ǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ.")
            except Exception as exc:
                await self.safe_send(message, f"❌ ꜱᴛᴏᴘ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(exc))}")

        # ── /queue /q
        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def queue_handler(_, message: Message):
            try:
                state = self.get_state(message.chat.id)
                await self.safe_send(message, self.queue_text(state), reply_markup=self.queue_keyboard())
            except Exception:
                log.exception("queue_handler failed")

        # ── /loop
        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def loop_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                arg = command_arg(message).lower()
                if arg == "on":
                    state.loop = True
                elif arg == "off":
                    state.loop = False
                else:
                    state.loop = not state.loop
                await self.safe_send(message, f"🔁 ʟᴏᴏᴘ: {human_bool(state.loop)}")
            except Exception:
                log.exception("loop_handler failed")

        # ── /shuffle
        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def shuffle_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                if len(state.queue) < 2:
                    return await self.safe_send(message, "❌ ꜱʜᴜꜰꜰʟᴇ ᴋᴇ ʟɪᴇ ᴋᴀᴍ ꜱᴇ ᴋᴀᴍ 2 ᴛʀᴀᴄᴋꜱ ᴄʜᴀʜɪᴇ.")
                random.shuffle(state.queue)
                await self.safe_send(message, f"🔀 ǫᴜᴇᴜᴇ ꜱʜᴜꜰꜰʟᴇᴅ! ({len(state.queue)} ᴛʀᴀᴄᴋꜱ)")
            except Exception:
                log.exception("shuffle_handler failed")

        # ── /clearqueue
        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def clearqueue_handler(_, message: Message):
            try:
                if not await self.require_admin(message):
                    return
                state = self.get_state(message.chat.id)
                count = len(state.queue)
                state.queue.clear()
                await self.safe_send(message, f"🧹 {count} ᴛʀᴀᴄᴋꜱ ᴄʟᴇᴀʀ ʜᴏ ɢᴀʏᴇ.")
            except Exception:
                log.exception("clearqueue_handler failed")

        # ── /mute
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

        # ── /unmute
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

        # ── /shelp (owner only)
        @self.bot.on_message(filters.command(["shelp"]) & (filters.private | filters.group))
        async def shelp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return
                await self.safe_send(message, self.shell_help_text())
            except Exception:
                log.exception("shelp_handler failed")

        # ── /setdp (owner only, private)
        @self.bot.on_message(filters.command(["setdp"]) & filters.private)
        async def setdp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return await self.safe_send(message, "❌ ʏᴇ ᴄᴏᴍᴍᴀɴᴅ ꜱɪʀꜰ <b>ᴏᴡɴᴇʀ</b> ᴜꜱᴇ ᴋᴀʀ ꜱᴀᴋᴛᴀ ʜᴀɪ.")
                self.pending_start_photo[message.from_user.id] = time.time()
                await self.safe_send(
                    message,
                    "🖼 <b>ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ꜱᴇᴛ ᴍᴏᴅᴇ ᴇɴᴀʙʟᴇᴅ</b>\n\n"
                    "ᴀʙ ᴍᴜᴊʜᴇ ᴘʜᴏᴛᴏ ʙʜᴇᴊᴏ.\n"
                    "ᴄᴀɴᴄᴇʟ ᴋᴇ ʟɪᴇ /cancel ʟɪᴋʜᴏ."
                )
            except Exception:
                log.exception("setdp_handler failed")

        # ── /removedp (owner only, private)
        @self.bot.on_message(filters.command(["removedp"]) & filters.private)
        async def removedp_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return await self.safe_send(message, "❌ ʏᴇ ᴄᴏᴍᴍᴀɴᴅ ꜱɪʀꜰ <b>ᴏᴡɴᴇʀ</b> ᴜꜱᴇ ᴋᴀʀ ꜱᴀᴋᴛᴀ ʜᴀɪ.")
                self.settings["start_photo_file_id"] = ""
                self.save_settings()
                self.pending_start_photo.pop(message.from_user.id, None)
                await self.safe_send(message, "✅ ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ʀᴇᴍᴏᴠᴇ ʜᴏ ɢᴀʏɪ.")
            except Exception:
                log.exception("removedp_handler failed")

        # ── Photo upload for /setdp
        @self.bot.on_message(filters.private & (filters.photo | filters.document))
        async def private_media_handler(_, message: Message):
            try:
                if not self.is_config_owner_user(message):
                    return
                if message.from_user.id not in self.pending_start_photo:
                    return

                file_id = ""
                if message.photo:
                    photo_obj = message.photo
                    if hasattr(photo_obj, "file_id"):
                        file_id = photo_obj.file_id
                    elif isinstance(photo_obj, (list, tuple)) and photo_obj:
                        file_id = photo_obj[-1].file_id
                elif message.document and (message.document.mime_type or "").startswith("image/"):
                    file_id = message.document.file_id
                else:
                    return await self.safe_send(message, "❌ ꜱɪʀꜰ <b>ɪᴍᴀɢᴇ/ᴘʜᴏᴛᴏ</b> ʙʜᴇᴊᴏ.")

                if not file_id:
                    return await self.safe_send(message, "❌ ꜰɪʟᴇ ɪᴅ ᴇxᴛʀᴀᴄᴛ ɴᴀʜɪ ʜᴜɪ. ᴅᴏʙᴀʀᴀ ꜱᴇɴᴅ ᴋᴀʀᴏ.")

                self.settings["start_photo_file_id"] = file_id
                self.save_settings()
                saved_id = self.settings.get("start_photo_file_id", "")
                self.pending_start_photo.pop(message.from_user.id, None)

                if saved_id == file_id:
                    await self.safe_send(
                        message,
                        f"✅ <b>ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ꜱᴀᴠᴇᴅ!</b>\n\n"
                        f"ꜰɪʟᴇ ɪᴅ: <code>{file_id[:30]}...</code>\n\n"
                        f"ᴀʙ /start ᴘᴀɴᴇʟ ᴘᴇ ʏᴇ ᴘʜᴏᴛᴏ ᴅɪᴋʜᴇɢɪ."
                    )
                else:
                    await self.safe_send(message, "⚠️ ꜱᴇᴛᴛɪɴɢꜱ ꜱᴀᴠᴇ ᴍᴇ ɪꜱꜱᴜᴇ. ᴅᴏʙᴀʀᴀ ᴛʀʏ ᴋᴀʀᴏ.")

            except Exception:
                log.exception("private_media_handler failed")
                await self.safe_send(message, "❌ ᴘʜᴏᴛᴏ ꜱᴀᴠᴇ ɴᴀʜɪ ʜᴜᴀ.")

        # ─────────────────────────────────────
        #  MASTER-ONLY COMMANDS
        # ─────────────────────────────────────

        if self.is_master:

            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def clone_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return
                    self.clone_flow[message.from_user.id] = {"step": "bot_token"}
                    await self.safe_send(
                        message,
                        f"🚀 <b>ɴᴀʏᴀ ʙᴏᴛ ꜱᴇᴛᴜᴘ</b>\n"
                        f"{sep()}\n\n"
                        f"<b>ꜱᴛᴇᴘ 1/4:</b>\nɴᴀʏᴇ ʙᴏᴛ ᴋᴀ ᴛᴏᴋᴇɴ ʙʜᴇᴊᴏ.\n\n"
                        f"ᴇxᴀᴍᴘʟᴇ:\n<code>123456789:ABCDEFGHIJ...</code>"
                    )
                except Exception:
                    log.exception("clone_handler failed")

            @self.bot.on_message(filters.command(["dclone"]) & filters.private)
            async def dclone_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return
                    token = command_arg(message).strip()
                    if not token:
                        return await self.safe_send(
                            message,
                            f"❓ <b>ᴜꜱᴀɢᴇ:</b>\n"
                            f"<code>/dclone 123456789:ABCDEF...</code>\n\n"
                            f"ʏᴀ /clones ꜱᴇ ᴀʟʟ ʙᴏᴛꜱ ᴅᴇᴋʜᴏ."
                        )

                    if not TOKEN_RE.match(token):
                        return await self.safe_send(message, "❌ ɪɴᴠᴀʟɪᴅ ᴛᴏᴋᴇɴ ꜰᴏʀᴍᴀᴛ.")

                    bot_id = token.split(":", 1)[0]
                    config_file = CLONES_DIR / f"{bot_id}.json"
                    pid_file    = PIDS_DIR   / f"{bot_id}.pid"

                    killed = False
                    pid_val = None

                    if pid_file.exists():
                        try:
                            pid_val = int(pid_file.read_text().strip())
                            try:
                                os.kill(pid_val, signal.SIGTERM)
                                await asyncio.sleep(1.5)
                                try:
                                    os.kill(pid_val, signal.SIGKILL)
                                except Exception:
                                    pass
                            except ProcessLookupError:
                                pass
                            killed = True
                        except Exception as pe:
                            log.warning("dclone: could not kill pid %s: %s", pid_val, pe)
                        finally:
                            try:
                                pid_file.unlink(missing_ok=True)
                            except Exception:
                                pass

                    config_removed = False
                    if config_file.exists():
                        try:
                            config_file.unlink()
                            config_removed = True
                        except Exception as ce:
                            log.warning("dclone: config remove failed: %s", ce)

                    if not killed and not config_removed:
                        return await self.safe_send(
                            message,
                            f"⚠️ ʙᴏᴛ <code>{bot_id}</code> ɴᴀʜɪ ᴍɪʟᴀ.\n"
                            f"ᴘʜɪʀ ꜱᴇ /clones ᴄʜᴇᴄᴋ ᴋᴀʀᴏ."
                        )

                    await self.safe_send(
                        message,
                        f"✅ <b>ʙᴏᴛ ꜱᴛᴏᴘ ʜᴏ ɢᴀʏᴀ!</b>\n\n"
                        f"🤖 ʙᴏᴛ ɪᴅ   : <code>{bot_id}</code>\n"
                        f"💀 ᴘʀᴏᴄᴇꜱꜱ  : {'ꜱᴛᴏᴘᴘᴇᴅ ✅' if killed else 'ɴᴏᴛ ꜰᴏᴜɴᴅ ⚠️'}\n"
                        f"📁 ᴄᴏɴꜰɪɢ   : {'ʀᴇᴍᴏᴠᴇᴅ ✅' if config_removed else 'ɴᴏᴛ ꜰᴏᴜɴᴅ ⚠️'}"
                    )

                except Exception:
                    log.exception("dclone_handler failed")
                    await self.safe_send(message, "❌ /dclone ᴍᴇ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ.")

            @self.bot.on_message(filters.command(["cancel"]) & filters.private)
            async def cancel_handler(_, message: Message):
                try:
                    if not self.is_config_owner_user(message):
                        return
                    had_setup = message.from_user.id in self.clone_flow
                    self.clone_flow.pop(message.from_user.id, None)
                    self.pending_start_photo.pop(message.from_user.id, None)
                    if had_setup:
                        await self.safe_send(message, "🛑 ᴄᴜʀʀᴇɴᴛ ꜱᴇᴛᴜᴘ ᴄᴀɴᴄᴇʟʟᴇᴅ.")
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
                        return await self.safe_send(message, "📭 ᴋᴏɪ ꜱᴀᴠᴇᴅ ʙᴏᴛ ᴄᴏɴꜰɪɢ ɴᴀʜɪ ᴍɪʟᴀ.")
                    lines = [f"📦 <b>ꜱᴀᴠᴇᴅ ʙᴏᴛ ᴄᴏɴꜰɪɢꜱ</b>\n{sep()}\n"]
                    for f in files[:50]:
                        try:
                            cfg     = load_config(f)
                            pid_f   = PIDS_DIR / f"{cfg.bot_id}.pid"
                            running = False
                            if pid_f.exists():
                                try:
                                    pid_v = int(pid_f.read_text().strip())
                                    os.kill(pid_v, 0)
                                    running = True
                                except Exception:
                                    pass
                            status = "🟢" if running else "🔴"
                            lines.append(
                                f"{status} <code>{escape_html(cfg.bot_id)}</code> — "
                                f"{escape_html(cfg.owner_username)} — "
                                f"{escape_html(cfg.support_chat)}"
                            )
                        except Exception:
                            lines.append(f"• {escape_html(f.name)}")
                    lines.append(f"\n{sep()}\n💡 /dclone &lt;token&gt; ꜱᴇ ꜱᴛᴏᴘ ᴋᴀʀᴏ")
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

                    skip_cmds = {
                        "/cancel", "/clone", "/clones", "/setdp",
                        "/removedp", "/dclone", "/shelp",
                    }
                    if text.lower() in skip_cmds:
                        return

                    if step == "bot_token":
                        if not TOKEN_RE.match(text):
                            return await self.safe_send(message, "❌ ɪɴᴠᴀʟɪᴅ ʙᴏᴛ ᴛᴏᴋᴇɴ.\nᴅᴏʙᴀʀᴀ ꜱᴀʜɪ ᴛᴏᴋᴇɴ ʙʜᴇᴊᴏ.")
                        state_flow["bot_token"] = text
                        state_flow["step"] = "support"
                        return await self.safe_send(
                            message,
                            f"<b>ꜱᴛᴇᴘ 2/4:</b>\nꜱᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ ᴜꜱᴇʀɴᴀᴍᴇ ʏᴀ ʟɪɴᴋ ʙʜᴇᴊᴏ.\n\n"
                            f"ᴇxᴀᴍᴘʟᴇ:\n<code>@yoursupportchat</code>"
                        )

                    if step == "support":
                        state_flow["support_chat"] = normalize_support(text)
                        state_flow["step"] = "owner_username"
                        return await self.safe_send(
                            message,
                            f"<b>ꜱᴛᴇᴘ 3/4:</b>\nᴏᴡɴᴇʀ ᴜꜱᴇʀɴᴀᴍᴇ ʏᴀ ʟɪɴᴋ ʙʜᴇᴊᴏ.\n\n"
                            f"ᴇxᴀᴍᴘʟᴇ:\n<code>@YourUsername</code>"
                        )

                    if step == "owner_username":
                        state_flow["owner_username"] = normalize_owner_username(text)
                        state_flow["step"] = "session"
                        return await self.safe_send(
                            message,
                            f"<b>ꜱᴛᴇᴘ 4/4:</b>\nꜱᴇꜱꜱɪᴏɴ ꜱᴛʀɪɴɢ ʙʜᴇᴊᴏ.\n"
                            f"ʏᴀ ꜱᴀᴍᴇ ᴅᴇꜰᴀᴜʟᴛ ᴜꜱᴇ ᴋᴀʀɴᴀ ʜᴀɪ ᴛᴏ <code>/default</code> ʟɪᴋʜᴏ."
                        )

                    if step == "session":
                        session_string = (
                            self.config.assistant_session
                            if text.lower() == "/default"
                            else text
                        )
                        if len(session_string) < 50:
                            return await self.safe_send(message, "❌ ꜱᴇꜱꜱɪᴏɴ ꜱᴛʀɪɴɢ ʙᴀʜᴜᴛ ᴄʜᴏᴛɪ ʟᴀɢ ʀᴀʜɪ ʜᴀɪ.")

                        await self.safe_send(message, "⏳ ᴠᴇʀɪꜰʏ ᴋᴀʀ ʀᴀʜᴀ ʜᴜɴ...")
                        try:
                            temp_client = Client(
                                name=f"verify_{int(time.time())}",
                                api_id=self.config.api_id,
                                api_hash=self.config.api_hash,
                                session_string=session_string,
                            )
                            await temp_client.start()
                            asst_me       = await temp_client.get_me()
                            asst_username = asst_me.username or "NoUsername"
                            asst_id       = asst_me.id
                            asst_name     = asst_me.first_name or "Assistant"
                            await temp_client.stop()
                            await self.safe_send(
                                message,
                                f"✅ <b>ꜱᴇꜱꜱɪᴏɴ ᴠᴇʀɪꜰɪᴇᴅ!</b>\n\n"
                                f"👤 ɴᴀᴍᴇ    : {escape_html(asst_name)}\n"
                                f"🔗 ᴜꜱᴇʀɴᴀᴍᴇ: @{escape_html(asst_username)}\n"
                                f"🆔 ᴜꜱᴇʀɪᴅ  : <code>{asst_id}</code>\n\n"
                                f"ꜱᴇᴛᴜᴘ ᴄᴏᴍᴘʟᴇᴛᴇ ʜᴏ ɢᴀʏᴀ — ʙᴏᴛ ʟᴀᴜɴᴄʜ ʜᴏ ʀʜᴀ ʜᴀɪ."
                            )
                        except Exception as ve:
                            await self.safe_send(
                                message,
                                f"⚠️ ꜱᴇꜱꜱɪᴏɴ ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ ꜰᴀɪʟᴇᴅ:\n"
                                f"<code>{escape_html(str(ve))}</code>\n\nᴘʀᴏᴄᴇᴇᴅɪɴɢ ᴀɴʏᴡᴀʏ..."
                            )

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
                                f"🚀 <b>ʙᴏᴛ ʟᴀᴜɴᴄʜ ʜᴏ ɢᴀʏᴀ!</b>\n"
                                f"{sep()}\n\n"
                                f"🤖 ʙᴏᴛ ɪᴅ : <code>{escape_html(clone_cfg.bot_id)}</code>\n"
                                f"💬 ꜱᴜᴘᴘᴏʀᴛ: {escape_html(clone_cfg.support_chat)}\n"
                                f"👤 ᴏᴡɴᴇʀ  : {escape_html(clone_cfg.owner_username)}\n"
                                f"🆔 ᴘɪᴅ    : <code>{proc.pid}</code>\n\n"
                                f"💡 ꜱᴛᴏᴘ ᴋᴀʀɴᴇ ᴋᴇ ʟɪᴇ:\n"
                                f"<code>/dclone {escape_html(clone_cfg.bot_token)}</code>"
                            )
                        except Exception as pe:
                            await self.safe_send(message, f"❌ ʙᴏᴛ ʟᴀᴜɴᴄʜ ɴᴀʜɪ ʜᴜᴀ: {escape_html(str(pe))}")

                except Exception:
                    log.exception("clone_flow_handler failed")
                    await self.safe_send(message, "❌ ꜱᴇᴛᴜᴘ ᴍᴇ ᴇʀʀᴏʀ ᴀᴀʏᴀ.")

    # ─────────────────────────────────────
    #  START / STOP
    # ─────────────────────────────────────

    async def start(self) -> None:
        if shutil.which("ffmpeg") is None:
            log.warning("ffmpeg not found in PATH. Audio playback may fail.")

        await self.add_handlers()

        await self.assistant.start()
        assistant_me = await self.assistant.get_me()
        self.assistant_id       = assistant_me.id
        self.assistant_name     = assistant_me.first_name or "Assistant"
        self.assistant_username = assistant_me.username or ""
        log.info(
            "ASSISTANT | @%s | id=%s | name=%s",
            self.assistant_username, self.assistant_id, self.assistant_name
        )

        await self.bot.start()
        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name     = me.first_name or ""
        self.bot_id_int   = me.id
        if self.bot_name:
            self.config.brand_name = self.bot_name

        try:
            await self.calls.start()
        except KeyError as ke:
            log.warning(
                "PyTgCalls startup: peer cache miss (%s). "
                "This is harmless — peer will be cached on first /play.",
                ke
            )
        except Exception:
            log.exception(
                "PyTgCalls start raised an error. "
                "Will attempt to continue — if play fails, restart the bot."
            )

        log.info(
            "RUNNING | %s | @%s | bot_id=%s",
            self.bot_name, self.bot_username, self.config.bot_id,
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
                log.exception("%s failed during shutdown", name)

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
        log.info("Shutdown requested. Goodbye! 🎵")

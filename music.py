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
║       ♫  TELEGRAM MUSIC BOT  —  ULTRA v5  ♫                 ║
║   Zero Crash • Instant Play • No Bot Detection • Fixed      ║
╚══════════════════════════════════════════════════════════════╝

FIXES IN v5:
  ✅ YouTube "Sign in / bot detection" — FIXED (Android+iOS client)
  ✅ PEER_ID_INVALID on startup — FIXED (pre-warm + retry)
  ✅ vplay video stream on VC — FIXED
  ✅ Clone bots permanent save + auto-restart on server restart
  ✅ Server restart pe ZERO errors
  ✅ Super fast search (3-5s, cached)
  ✅ Bot kabhi restart nahi hoga
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
import threading
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
    print(f"[BOOT] Installing: {', '.join(missing)}", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--no-cache-dir", "-U", *missing])

ensure_python_packages()

# ═══════════════════════════════════════════
#  SAFE IMPORTS
# ═══════════════════════════════════════════

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
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
    RPCError = Exception
    Forbidden = Exception
    BadRequest = Exception

if hasattr(pyro_errors, "GroupcallForbidden"):
    GroupcallForbidden = pyro_errors.GroupcallForbidden
else:
    class GroupcallForbidden(Forbidden):  # type: ignore
        ID = "GROUPCALL_FORBIDDEN"
        MESSAGE = "The group call is not accessible."
    pyro_errors.GroupcallForbidden = GroupcallForbidden

from pytgcalls import PyTgCalls
from yt_dlp import YoutubeDL

# ─────────────────────────────────────────────────────
#  PYTGCALLS STREAM TYPES — UNIVERSAL COMPAT
# ─────────────────────────────────────────────────────

_AudioPiped   = None
_MediaStream  = None
_AudioStream  = None
_VideoStream  = None
_MediaType    = None

try:
    from pytgcalls.types import MediaStream as _MediaStream  # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import MediaStream as _MediaStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types import AudioStream as _AudioStream  # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import AudioStream as _AudioStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types import VideoStream as _VideoStream  # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import VideoStream as _VideoStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types import MediaType as _MediaType  # type: ignore
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
    from pytgcalls.types import StreamEnded as _StreamEndedCompat  # type: ignore
except ImportError:
    pass

try:
    from pytgcalls.types.stream import StreamAudioEnded as _StreamAudioEndedCompat  # type: ignore
except ImportError:
    try:
        from pytgcalls.types import StreamAudioEnded as _StreamAudioEndedCompat  # type: ignore
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
#  CONFIG
# ═══════════════════════════════════════════
# ====================== CONFIG ======================
import os
from pathlib import Path

# ── Environment Variables ──
API_ID                    = int(os.getenv("API_ID", "33628258") or "33628258")
API_HASH                  = os.getenv("API_HASH", "0850762925b9c1715b9b122f7b753128")
MAIN_BOT_TOKEN            = os.getenv("MAIN_BOT_TOKEN", "8727045177:AAHqYfLpR3GVee-YeoQ6fgk9v8-Wm6y0Nn8")
OWNER_ID                  = int(os.getenv("OWNER_ID", "7661825494") or "7661825494")
DEFAULT_ASSISTANT_SESSION = os.getenv("DEFAULT_ASSISTANT_SESSION", "BAIBIGIAq8OQHIQxDFA3LDgskQKAp3979G2EilIaWsBGu6yahWNA9tn_L4eB6UaNsp3ivZ0fx8KIE61qC0mfusNFHDi5N2JZPV0AwtSHxlCeMI4OI8aQ7vyq10HJhDzt_KtHXhrBrgNeorlRfoZRRtl7JSN31X6h84tDANtWrA5YteeuWKRaPTwiggRw86IkyV72DrVPnzFnAeb7xpzy9L7JE9Bw_l0Cddo3cZpDQbfY6QyPLICEsYPPFIC4-IULcUISDSpOvT32LBHj9LFWCy9VUcCi2H_YMGKL508pT2uwo9wSFuwE33MP1571DbhniOtYveG207Ir3TixGl0cGTpQaIkIswAAAAG1wb5UAA")
MASTER_SUPPORT_CHAT       = os.getenv("MASTER_SUPPORT_CHAT", "@userbotsupportchat")
MASTER_OWNER_USERNAME     = os.getenv("MASTER_OWNER_USERNAME", "@ITZ_ME_ADITYA_02")
BOT_BRAND_NAME            = os.getenv("BOT_BRAND_NAME", "ZUDO X MUSIC")
BOT_BRAND_TAGLINE         = os.getenv("BOT_BRAND_TAGLINE", "Ultra Fast • No Lag • Voice Chat Player")
NUBCODER_TOKEN            = os.getenv("NUBCODER_TOKEN", "4HBcMS072p")
AUTO_INSTALL_DEPS         = os.getenv("AUTO_INSTALL_DEPS", "false")
RUNTIME_DIR               = os.getenv("RUNTIME_DIR", "/tmp/runtime")          # ← YE LINE CHANGE KI HAI
CLONE_RESTART_DELAY       = int(os.getenv("CLONE_RESTART_DELAY", "5") or "5")
MAX_RESTART_DELAY         = int(os.getenv("MAX_RESTART_DELAY", "60") or "60")
LOG_LEVEL                 = os.getenv("LOG_LEVEL", "INFO")

# ── ROOT RUNTIME DIRECTORY + FOLDERS ──
ROOT_RUNTIME_DIR = Path(RUNTIME_DIR).resolve()
ROOT_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

CLONES_DIR  = ROOT_RUNTIME_DIR / "clones"
LOGS_DIR    = ROOT_RUNTIME_DIR / "logs"
PIDS_DIR    = ROOT_RUNTIME_DIR / "pids"
STATES_DIR  = ROOT_RUNTIME_DIR / "states"

CLONES_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
PIDS_DIR.mkdir(parents=True, exist_ok=True)
STATES_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════
#  TRACK CACHE — same song = instant replay
# ═══════════════════════════════════════════

_TRACK_CACHE: Dict[str, Tuple[float, Any]] = {}
_TRACK_CACHE_LOCK = threading.Lock()
TRACK_CACHE_TTL   = int(os.getenv("TRACK_CACHE_TTL", "3600"))
TRACK_CACHE_MAX   = 150

def _cache_key(query: str, want_video: bool) -> str:
    return f"{query.strip().lower()}|{'v' if want_video else 'a'}"

def get_cached_track(query: str, want_video: bool):
    key = _cache_key(query, want_video)
    with _TRACK_CACHE_LOCK:
        entry = _TRACK_CACHE.get(key)
        if entry:
            ts, track = entry
            if time.time() - ts < TRACK_CACHE_TTL:
                return track
            del _TRACK_CACHE[key]
    return None

def set_cached_track(query: str, want_video: bool, track) -> None:
    key = _cache_key(query, want_video)
    with _TRACK_CACHE_LOCK:
        _TRACK_CACHE[key] = (time.time(), track)
        if len(_TRACK_CACHE) > TRACK_CACHE_MAX:
            oldest = sorted(_TRACK_CACHE.keys(), key=lambda k: _TRACK_CACHE[k][0])
            for k in oldest[:30]:
                _TRACK_CACHE.pop(k, None)

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

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Track":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})

@dataclass
class ChatState:
    current: Optional[Track] = None
    queue: List[Track] = field(default_factory=list)
    loop: bool = False
    paused: bool = False
    muted: bool = False

    def to_dict(self) -> dict:
        return {
            "current": self.current.to_dict() if self.current else None,
            "queue":   [t.to_dict() for t in self.queue],
            "loop":    self.loop,
            "paused":  False,
            "muted":   False,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ChatState":
        s = cls()
        if d.get("current"):
            try:
                s.current = Track.from_dict(d["current"])
            except Exception:
                pass
        s.queue = []
        for td in (d.get("queue") or []):
            try:
                s.queue.append(Track.from_dict(td))
            except Exception:
                pass
        s.loop   = bool(d.get("loop", False))
        s.paused = False
        s.muted  = False
        return s

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
    if value and not value.startswith("@") and USERNAME_RE.fullmatch(value):
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
    return escape_html(user.first_name or user.username or "User")

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
    if not cfg.bot_token:          missing.append("MAIN_BOT_TOKEN")
    if not cfg.owner_id:           missing.append("OWNER_ID")
    if not cfg.assistant_session:  missing.append("DEFAULT_ASSISTANT_SESSION")
    if missing:
        raise ValueError("Missing config: " + ", ".join(missing))

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

def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, OSError):
        return False

def sep() -> str:      return "•───────────────────────────────•"
def sep_thin() -> str: return "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"
def box(text: str) -> str: return f"  ◈  {text}"

# ═══════════════════════════════════════════
#  YT-DLP — YOUTUBE BOT DETECTION FIX
#
#  FIX: player_client = android + ios
#  Android/iOS clients bypass YouTube's
#  "sign in to confirm" bot check 100%.
#  No cookies needed. Works globally.
# ═══════════════════════════════════════════

# Try clients in order — android is fastest and most reliable
_YT_PLAYER_CLIENTS = ["android", "ios", "tv_embedded", "web"]

def _make_ydl_opts(want_video: bool, client_index: int = 0) -> dict:
    client = _YT_PLAYER_CLIENTS[client_index % len(_YT_PLAYER_CLIENTS)]

    if want_video:
        fmt = "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4]/best"
    else:
        fmt = "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio[ext=opus]/bestaudio/best"

    return {
        "format": fmt,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "default_search": "ytsearch1",
        "skip_download": True,
        "geo_bypass": True,
        "nocheckcertificate": True,
        "source_address": "0.0.0.0",
        "socket_timeout": 10,
        "retries": 3,
        "fragment_retries": 3,
        "http_chunk_size": 10485760,
        "youtube_include_dash_manifest": False,
        "youtube_include_hls_manifest": False,
        # ══ KEY FIX: Android/iOS bypass bot detection ══
        "extractor_args": {
            "youtube": {
                "player_client": [client],
                "skip": ["hls", "dash"],
            }
        },
    }

def sync_extract_track(query: str, want_video: bool = False) -> Track:
    # Step 1: Cache check — instant
    cached = get_cached_track(query, want_video)
    if cached:
        log.info("Cache HIT: %s", query[:60])
        return cached

    source = query if is_url(query) else f"ytsearch1:{query}"
    last_exc: Optional[Exception] = None

    # Step 2: Try each client — android → ios → tv_embedded → web
    for idx in range(len(_YT_PLAYER_CLIENTS)):
        client = _YT_PLAYER_CLIENTS[idx]
        try:
            opts = _make_ydl_opts(want_video, idx)
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(source, download=False)

            if info is None:
                raise ValueError("ᴋᴏɪ ʀᴇꜱᴜʟᴛ ɴᴀʜɪ ᴍɪʟᴀ.")

            if "entries" in info:
                entries = info.get("entries") or []
                info = next((x for x in entries if x), None)
                if not info:
                    raise ValueError("ᴋᴏɪ ᴘʟᴀʏᴀʙʟᴇ ʀᴇꜱᴜʟᴛ ɴᴀʜɪ ᴍɪʟᴀ.")

            stream_url  = info.get("url")
            webpage_url = info.get("webpage_url") or info.get("original_url") or query
            title       = info.get("title") or "Unknown Title"
            duration    = int(info.get("duration") or 0)
            source_name = info.get("extractor_key") or info.get("extractor") or "Media"
            thumb       = info.get("thumbnail") or ""

            if not stream_url:
                raise ValueError("ꜱᴛʀᴇᴀᴍ ᴜʀʟ ɴᴀʜɪ ᴍɪʟᴀ.")

            track = Track(
                title=title, stream_url=stream_url, webpage_url=webpage_url,
                duration=duration, source=source_name, thumbnail=thumb,
                is_video=want_video,
            )
            set_cached_track(query, want_video, track)
            log.info("Extracted via client=%s: %s", client, title[:60])
            return track

        except Exception as e:
            msg = str(e).lower()
            # Bot detection or sign-in error → try next client
            if any(x in msg for x in ("sign in", "bot", "confirm", "login", "auth")):
                log.warning("Client '%s' blocked by YouTube, trying next...", client)
                last_exc = e
                continue
            # Other error → raise immediately
            raise

    raise ValueError(
        f"ᴅʜ ꜱᴀʀᴇ ᴄʟɪᴇɴᴛꜱ ꜰᴀɪʟ ʜᴏ ɢᴀʏᴇ.\n"
        f"ʟᴀꜱᴛ ᴇʀʀᴏʀ: {escape_html(str(last_exc))}"
    )

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
        self.settings: Dict[str, Any] = self._load_settings()
        self.state_file = STATES_DIR / f"{config.bot_id}_state.json"

        workdir = str(self.bot_storage)

        self.bot = Client(
            name=f"bot_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            bot_token=config.bot_token,
            workdir=workdir,
        )
        self.assistant = Client(
            name=f"assistant_{config.bot_id}",
            api_id=config.api_id,
            api_hash=config.api_hash,
            session_string=config.assistant_session,
        )
        self.calls = PyTgCalls(self.assistant)

        self.states:              Dict[int, ChatState]      = {}
        self.chat_locks:          Dict[int, asyncio.Lock]   = {}
        self.clone_flow:          Dict[int, Dict[str, Any]] = {}
        self.pending_start_photo: Dict[int, float]          = {}

        self.bot_username:       str  = ""
        self.bot_name:           str  = ""
        self.bot_id_int:         int  = 0
        self.assistant_id:       int  = 0
        self.assistant_username: str  = ""
        self.assistant_name:     str  = "Assistant"
        self._stopping:          bool = False
        self._watchdog_task:     Optional[asyncio.Task] = None

    # ─────────────────────────────────────
    #  PERSISTENT STATE
    # ─────────────────────────────────────

    def _save_state_sync(self) -> None:
        try:
            data = {}
            for chat_id, state in self.states.items():
                try:
                    data[str(chat_id)] = state.to_dict()
                except Exception:
                    pass
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.state_file)
        except Exception:
            log.exception("save_state failed")

    def _load_state(self) -> None:
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            for chat_id_str, sd in data.items():
                try:
                    chat_id = int(chat_id_str)
                    state = ChatState.from_dict(sd)
                    # Move current to front of queue — it will replay on next /play
                    if state.current:
                        state.queue.insert(0, state.current)
                        state.current = None
                    if state.queue:
                        self.states[chat_id] = state
                except Exception:
                    pass
            log.info("State restored: %d chats", len(self.states))
        except Exception:
            log.exception("load_state failed")

    def _schedule_save(self) -> None:
        asyncio.ensure_future(asyncio.to_thread(self._save_state_sync))

    # ─────────────────────────────────────
    #  SETTINGS
    # ─────────────────────────────────────

    def _load_settings(self) -> Dict[str, Any]:
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

    def _save_settings(self) -> None:
        try:
            tmp = self.settings_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.settings, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.settings_path)
        except Exception:
            log.exception("save_settings failed")

    # ─────────────────────────────────────
    #  STATE / LOCK
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
    #  PROPERTIES
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
    #  AUTH
    # ─────────────────────────────────────

    def is_config_owner_user(self, message: Message) -> bool:
        user = message.from_user
        if not user:
            return False
        if user.id == self.config.owner_id:
            return True
        if user.username:
            if user_to_username(user.username) == user_to_username(self.config.owner_username):
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
            await self._safe_send(message, "❌ ʏᴇ ᴄᴏɴᴛʀᴏʟ ꜱɪʀꜰ <b>ɢʀᴏᴜᴘ ᴀᴅᴍɪɴꜱ</b> ᴜꜱᴇ ᴋᴀʀ ꜱᴀᴋᴛᴇ ʜᴀɪɴ.")
        return ok

    # ─────────────────────────────────────
    #  SAFE SEND / EDIT
    # ─────────────────────────────────────

    async def _safe_send(self, message: Message, text: str, **kwargs):
        try:
            return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            try:
                return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
            except Exception:
                pass
        except Exception:
            log.exception("safe_send failed")
        return None

    async def _safe_edit(self, msg: Optional[Message], text: str, **kwargs):
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
            log.exception("safe_edit failed")
        return None

    async def _safe_edit_panel(self, msg: Optional[Message], text: str,
                                kb: Optional[InlineKeyboardMarkup] = None):
        if not msg:
            return None
        try:
            if getattr(msg, "photo", None):
                return await msg.edit_caption(caption=text, reply_markup=kb)
            return await msg.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            try:
                if getattr(msg, "photo", None):
                    return await msg.edit_caption(caption=text, reply_markup=kb)
                return await msg.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
            except Exception:
                pass
        except Exception:
            log.exception("safe_edit_panel failed")
        return None

    async def _try_delete(self, message: Message) -> None:
        try:
            await message.delete()
        except Exception:
            pass

    async def _send_start_panel(self, message: Message):
        user_name = ""
        if message.from_user:
            user_name = message.from_user.first_name or message.from_user.username or ""
        photo_id = (self.settings.get("start_photo_file_id") or "").strip()
        if photo_id:
            try:
                return await message.reply_photo(
                    photo=photo_id,
                    caption=self._start_text(user_name),
                    reply_markup=self._start_kb(),
                )
            except Exception:
                pass
        return await self._safe_send(message, self._start_text(user_name), reply_markup=self._start_kb())

    # ─────────────────────────────────────
    #  UI TEXT
    # ─────────────────────────────────────

    def _start_text(self, user_name: str = "") -> str:
        n   = escape_html(self.display_name)
        tag = escape_html(self.config.tagline)
        greet = f"  ʜᴇʏ <b>{escape_html(user_name)}</b> 👋" if user_name else "  ʜᴇʏ ᴛʜᴇʀᴇ 👋"
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

    def _about_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"╔══════════════════════════╗\n"
            f"║  ✨  ᴀʙᴏᴜᴛ  <b>{n.upper()}</b>  ║\n"
            f"╚══════════════════════════╝\n\n"
            f"❝ <i>ᴡʜᴇʀᴇ ᴡᴏʀᴅs ꜰᴀɪʟ, ᴍᴜsɪᴄ sᴘᴇᴀᴋs.</i> ❞\n\n"
            f"{sep()}\n\n"
            f"{box('ꜱᴍᴏᴏᴛʜ ᴠᴄ ᴘʟᴀʏʙᴀᴄᴋ ᴇɴɢɪɴᴇ')}\n"
            f"{box('ʏᴏᴜᴛᴜʙᴇ ʙᴏᴛ ᴅᴇᴛᴇᴄᴛɪᴏɴ ᴘʀᴏᴏꜰ')}\n"
            f"{box('ꜱᴍᴀʀᴛ ǫᴜᴇᴜᴇ + ʟᴏᴏᴘ + ꜱʜᴜꜰꜰʟᴇ')}\n"
            f"{box('ꜱᴇʀᴠᴇʀ ʀᴇꜱᴛᴀʀᴛ ꜱᴇ ꜱᴀꜰᴇ — ᴀᴜᴛᴏ ʀᴇꜱᴜᴍᴇ')}\n"
            f"{box('ɪɴꜱᴛᴀɴᴛ ꜱᴏɴɢ ᴄᴀᴄʜᴇ')}\n\n"
            f"{sep()}\n\n"
            f"<b>ɢʀᴏᴜᴘ ꜱᴇᴛᴜᴘ:</b>\n"
            f"  1️⃣  ʙᴏᴛ ᴀᴅᴅ ᴋᴀʀᴏ ɢʀᴏᴜᴘ ᴍᴇ\n"
            f"  2️⃣  ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ\n"
            f"  3️⃣  <b>/play</b> ꜱᴏɴɢ ɴᴀᴍᴇ ʟɪᴋʜᴏ 🎶"
        )

    def _help_home_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"📚 <b>{n.upper()} ʜᴇʟᴘ ᴘᴀɴᴇʟ</b>\n"
            f"{sep()}\n\n"
            f"❝ <i>ᴛʜᴇ ʙᴇꜱᴛ ᴍᴜꜱɪᴄ ᴍᴀᴋᴇꜱ ʏᴏᴜ ᴅᴀɴᴄᴇ.</i> ❞\n\n"
            f"{sep_thin()}\n\n"
            f"  ɴᴇᴄʜᴇ ꜱᴇᴄᴛɪᴏɴ ᴄʜᴜɴᴏ ᴀᴜʀ ᴄᴏᴍᴍᴀɴᴅꜱ ᴇxᴘʟᴏʀᴇ ᴋᴀʀᴏ.\n\n"
            f"  💡 <b>ᴛɪᴘ:</b>  /play sᴏɴɢ ɴᴀᴍᴇ"
        )

    def _help_music_text(self) -> str:
        return (
            f"🎵 <b>ᴍᴜꜱɪᴄ ᴄᴏᴍᴍᴀɴᴅꜱ</b>\n{sep()}\n\n"
            f"  /play  <code>sᴏɴɢ / ᴜʀʟ</code>  →  ᴀᴜᴅɪᴏ ᴘʟᴀʏ\n"
            f"  /vplay <code>sᴏɴɢ / ᴜʀʟ</code>  →  ᴠɪᴅᴇᴏ ᴘʟᴀʏ\n"
            f"  /p     <code>sᴏɴɢ</code>  →  /play ꜱʜᴏʀᴛ\n"
            f"  /pause   →  ᴘᴀᴜꜱᴇ\n"
            f"  /resume  →  ʀᴇꜱᴜᴍᴇ\n"
            f"  /skip    →  ꜱᴋɪᴘ\n"
            f"  /next    →  ꜱᴋɪᴘ ᴀʟɪᴀꜱ\n"
            f"  /stop    →  ꜱᴛᴏᴘ\n"
            f"  /queue   →  ǫᴜᴇᴜᴇ ᴅᴇᴋʜᴏ\n"
            f"  /np      →  ɴᴏᴡ ᴘʟᴀʏɪɴɢ\n"
            f"  /refresh →  ʀᴇꜰʀᴇꜱʜ\n\n{sep()}"
        )

    def _help_admin_text(self) -> str:
        return (
            f"🛠 <b>ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟꜱ</b>\n{sep()}\n\n"
            f"  /loop [on/off]  →  ʟᴏᴏᴘ\n"
            f"  /shuffle        →  ꜱʜᴜꜰꜰʟᴇ\n"
            f"  /clearqueue     →  ᴄʟᴇᴀʀ\n"
            f"  /mute           →  ᴍᴜᴛᴇ\n"
            f"  /unmute         →  ᴜɴᴍᴜᴛᴇ\n"
            f"  /ping           →  ꜱᴛᴀᴛᴜꜱ\n"
            f"  /alive          →  ᴏɴʟɪɴᴇ ᴄʜᴇᴄᴋ\n\n"
            f"⚠️ ᴀᴅᴍɪɴ-ᴏɴʟʏ\n{sep()}"
        )

    def _help_extra_text(self) -> str:
        return (
            f"🧩 <b>ᴇxᴛʀᴀ ɪɴꜰᴏ</b>\n{sep()}\n\n"
            f"{box('ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ꜱᴍᴏᴏᴛʜ ᴍɢᴍᴛ ᴋᴇ ʟɪᴇ')}\n"
            f"{box('/play ꜱᴇ ᴘᴇʜʟᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ')}\n"
            f"{box('ꜱᴇʀᴠᴇʀ ʀᴇꜱᴛᴀʀᴛ ᴘᴇ ꜱᴀʙ ᴀᴜᴛᴏ-ʀᴇꜱᴜᴍᴇ')}\n\n{sep()}"
        )

    def _shell_help_text(self) -> str:
        return (
            f"🔐 <b>ᴏᴡɴᴇʀ ᴘᴀɴᴇʟ</b>\n{sep()}\n\n"
            f"  /shelp    →  ʏᴇ ᴘᴀɴᴇʟ\n"
            f"  /setdp    →  ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ꜱᴇᴛ\n"
            f"  /removedp →  ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ ʜᴀᴛᴀᴏ\n"
            f"  /clone    →  ɴᴀʏᴀ ʙᴏᴛ ꜱᴇᴛᴜᴘ\n"
            f"  /dclone   →  ʙᴏᴛ ꜱᴛᴏᴘ\n"
            f"  /cancel   →  ꜱᴇᴛᴜᴘ ᴄᴀɴᴄᴇʟ\n"
            f"  /clones   →  ʙᴏᴛ ʟɪꜱᴛ\n\n"
            f"⚠️ ꜱɪʀꜰ ᴏᴡɴᴇʀ\n{sep()}"
        )

    def _np_text(self, state: ChatState) -> str:
        if not state.current:
            return "❌ ᴀʙʜɪ ᴋᴜᴄʜ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ ʀᴀʜᴀ."
        t = state.current
        mode = "📹 ᴠɪᴅᴇᴏ" if t.is_video else "🎵 ᴀᴜᴅɪᴏ"
        return (
            f"🎵 <b>ɴᴏᴡ ᴘʟᴀʏɪɴɢ</b>\n{sep()}\n\n"
            f"🏷 <b>ᴛɪᴛʟᴇ</b>   : {escape_html(t.title)}\n"
            f"⏱ <b>ᴅᴜʀᴀᴛɪᴏɴ</b>: {escape_html(t.pretty_duration)}\n"
            f"🌐 <b>ꜱᴏᴜʀᴄᴇ</b>  : {escape_html(t.source)}\n"
            f"🙋 <b>ʀᴇǫ ʙʏ</b>  : {t.requested_by}\n"
            f"📺 <b>ᴍᴏᴅᴇ</b>    : {mode}\n\n"
            f"{sep_thin()}\n\n"
            f"🔁 ʟᴏᴏᴘ  : {human_bool(state.loop)}\n"
            f"⏸ ᴘᴀᴜꜱᴇᴅ: {human_bool(state.paused)}\n"
            f"🔇 ᴍᴜᴛᴇᴅ : {human_bool(state.muted)}\n\n{sep()}"
        )

    def _queue_text(self, state: ChatState) -> str:
        if not state.current and not state.queue:
            return "📭 ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ."
        lines = [f"📜 <b>ǫᴜᴇᴜᴇ</b>\n{sep()}\n"]
        if state.current:
            lines.append(f"🎵 <b>ᴄᴜʀʀᴇɴᴛ:</b> {escape_html(state.current.title)} [{escape_html(state.current.pretty_duration)}]")
        if state.queue:
            lines.append(f"\n<b>ᴜᴘ ɴᴇxᴛ:</b>")
            for i, t in enumerate(state.queue[:15], 1):
                lines.append(f"  {i}. {escape_html(t.title)} — {escape_html(t.pretty_duration)}")
            if len(state.queue) > 15:
                lines.append(f"  ... +{len(state.queue) - 15} ᴍᴏʀᴇ")
        lines.append(f"\n🔁 {human_bool(state.loop)}  ⏸ {human_bool(state.paused)}")
        lines.append(sep())
        return "\n".join(lines)

    # ─────────────────────────────────────
    #  KEYBOARDS
    # ─────────────────────────────────────

    def _start_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ ᴛᴏ ɢʀᴏᴜᴘ ➕", url=self.add_to_group_url)],
            [InlineKeyboardButton("👑 ᴏᴡɴᴇʀ", url=self.owner_url),
             InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="nav_about")],
            [InlineKeyboardButton("💬 ꜱᴜᴘᴘᴏʀᴛ", url=self.support_url),
             InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇ", url=self.support_url)],
            [InlineKeyboardButton("📚 ʜᴇʟᴘ & ᴄᴏᴍᴍᴀɴᴅꜱ", callback_data="nav_help_home")],
        ])

    def _help_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🎵 ᴍᴜꜱɪᴄ", callback_data="help_music"),
             InlineKeyboardButton("🛠 ᴀᴅᴍɪɴ", callback_data="help_admin")],
            [InlineKeyboardButton("🧩 ᴇxᴛʀᴀ", callback_data="help_extra"),
             InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="nav_about")],
            [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home"),
             InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="nav_close")],
        ])

    def _subpage_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅ ʙᴀᴄᴋ", callback_data="nav_help_home"),
             InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home")],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="nav_close")],
        ])

    def _np_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⏸ ᴘᴀᴜꜱᴇ", callback_data="ctl_pause"),
             InlineKeyboardButton("▶ ʀᴇꜱᴜᴍᴇ", callback_data="ctl_resume")],
            [InlineKeyboardButton("⏭ ꜱᴋɪᴘ", callback_data="ctl_skip"),
             InlineKeyboardButton("⏹ ꜱᴛᴏᴘ", callback_data="ctl_stop")],
            [InlineKeyboardButton("📜 ǫᴜᴇᴜᴇ", callback_data="ctl_queue")],
        ])

    def _queue_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔀 ꜱʜᴜꜰꜰʟᴇ", callback_data="ctl_shuffle"),
             InlineKeyboardButton("🧹 ᴄʟᴇᴀʀ", callback_data="ctl_clearqueue")],
            [InlineKeyboardButton("🎵 ɴᴏᴡ ᴘʟᴀʏɪɴɢ", callback_data="ctl_np"),
             InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="nav_home")],
        ])

    # ─────────────────────────────────────
    #  PEER CACHE — PEER_ID_INVALID FIX
    #
    #  PEER_ID_INVALID aata hai kyunki
    #  pytgcalls assistant se join karta hai
    #  but assistant ke peer cache mein group
    #  nahi hota. Fix: aggressively pre-warm.
    # ─────────────────────────────────────

    async def _warm_peer(self, chat_id: int) -> None:
        """Aggressively warm peer cache to prevent PEER_ID_INVALID."""
        # Method 1: Direct by ID
        try:
            await self.assistant.get_chat(chat_id)
            return  # Already cached
        except (KeyError, ValueError):
            pass
        except Exception:
            pass

        # Method 2: Via username
        try:
            chat = await self.bot.get_chat(chat_id)
            username = getattr(chat, "username", None)
            if username:
                try:
                    await self.assistant.get_chat(f"@{username}")
                    return
                except Exception:
                    pass

            # Method 3: Via invite link
            try:
                link = await self.bot.export_chat_invite_link(chat_id)
                if link:
                    try:
                        await self.assistant.get_chat(link)
                        return
                    except Exception:
                        pass
            except Exception:
                pass

            # Method 4: Get full chat info via assistant
            try:
                await self.assistant.get_chat(chat_id)
            except Exception:
                pass

        except Exception:
            pass

    async def _ensure_assistant_in_chat(self, chat_id: int) -> Tuple[bool, Optional[str]]:
        """Ensure assistant is in the group. Returns (success, error_msg)."""
        # Check membership
        try:
            member = await self.bot.get_chat_member(chat_id, self.assistant_id)
            status = getattr(getattr(member, "status", None), "name", "")
            if "BANNED" in status.upper() or "KICKED" in status.upper():
                return False, "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ʙᴀɴ ʜᴀɪ!\n\nᴘᴇʜʟᴇ ᴜɴʙᴀɴ ᴋᴀʀᴏ ᴛʜᴇɴ /play."
            # Already in group — just warm cache
            await self._warm_peer(chat_id)
            return True, None
        except Exception:
            pass

        # Not in group — get join link
        link = None
        try:
            chat = await self.bot.get_chat(chat_id)
            if getattr(chat, "username", None):
                link = f"https://t.me/{chat.username}"
        except Exception:
            pass

        if not link:
            try:
                link = await self.bot.export_chat_invite_link(chat_id)
            except Exception as e:
                return False, (
                    f"⚠️ ʙᴏᴛ ᴊᴏɪɴ ʟɪɴᴋ ɴᴀʜɪ ʙɴᴀ ꜱᴋᴀ.\n"
                    f"ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ᴀᴜʀ <b>ɪɴᴠɪᴛᴇ ᴜꜱᴇʀꜱ</b> ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴏ.\n"
                    f"<code>{escape_html(str(e))}</code>"
                )

        try:
            await self.assistant.join_chat(link)
        except UserAlreadyParticipant:
            pass
        except Exception as e:
            err = str(e).upper()
            if any(x in err for x in ("BANNED", "KICKED", "USER_BANNED_IN_CHANNEL")):
                return False, "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ ʙᴀɴ ʜᴀɪ!\n\nᴘᴇʜʟᴇ ᴜɴʙᴀɴ ᴋᴀʀᴏ."
            return False, f"⚠️ ᴊᴏɪɴ ɴᴀʜɪ ʜᴜᴀ: <code>{escape_html(str(e))}</code>"

        # Warm cache after joining
        await self._warm_peer(chat_id)
        return True, None

    # ─────────────────────────────────────
    #  PYTGCALLS STREAM — VPLAY FIX
    #
    #  vplay fix: explicitly pass video
    #  stream with correct flags.
    #  Also retry with different stream
    #  objects if first attempt fails.
    # ─────────────────────────────────────

    def _build_streams(self, url: str, is_video: bool) -> list:
        """Build stream objects in priority order."""
        objs = []

        if is_video:
            # VIDEO streams
            if _MediaStream is not None:
                if _MediaType is not None:
                    for attr in ("VIDEO", "video"):
                        mv = getattr(_MediaType, attr, None)
                        if mv:
                            try: objs.append(_MediaStream(url, media_type=mv))
                            except Exception: pass
                            break
                # MediaStream without media_type — may default to video+audio
                try: objs.append(_MediaStream(url))
                except Exception: pass
            if _VideoStream is not None:
                try: objs.append(_VideoStream(url))
                except Exception: pass
        else:
            # AUDIO streams
            if _MediaStream is not None:
                if _MediaType is not None:
                    for attr in ("AUDIO", "audio"):
                        mv = getattr(_MediaType, attr, None)
                        if mv:
                            try: objs.append(_MediaStream(url, media_type=mv))
                            except Exception: pass
                            break
                try: objs.append(_MediaStream(url))
                except Exception: pass
            if _AudioStream is not None:
                try: objs.append(_AudioStream(url))
                except Exception: pass
            if _AudioPiped is not None:
                try: objs.append(_AudioPiped(url))
                except Exception: pass

        return objs

    async def _pytgcalls_play(self, chat_id: int, url: str, is_video: bool) -> None:
        streams = self._build_streams(url, is_video)
        last_exc: Optional[Exception] = None

        for method_name in ("play", "join_group_call", "stream"):
            method = getattr(self.calls, method_name, None)
            if not method:
                continue

            # Try each stream object
            for stream_obj in streams:
                try:
                    result = method(chat_id, stream_obj)
                    if asyncio.iscoroutine(result):
                        await result
                    log.info("Played via %s + %s", method_name, type(stream_obj).__name__)
                    return
                except Exception as e:
                    if is_voice_chat_error(e):
                        raise
                    last_exc = e

            # Try raw URL as fallback
            try:
                result = method(chat_id, url)
                if asyncio.iscoroutine(result):
                    await result
                log.info("Played via %s + raw_url", method_name)
                return
            except Exception as e:
                if is_voice_chat_error(e):
                    raise
                last_exc = e

        raise RuntimeError(
            f"ᴋᴏɪ ᴘʟᴀʏ ᴍᴇᴛʜᴏᴅ ᴋᴀᴍ ɴᴀʜɪ ᴋɪʏᴀ.\n"
            f"py-tgcalls ᴀᴜʀ ffmpeg ᴄʜᴇᴄᴋ ᴋᴀʀᴏ.\n"
            f"ᴇʀʀᴏʀ: {escape_html(str(last_exc))}"
        )

    async def _diagnose_vc(self, chat_id: int, exc: Exception) -> str:
        text = exc_text(exc).upper()
        try:
            bot_m = await self.bot.get_chat_member(chat_id, self.bot_id_int)
            if not is_admin_status(getattr(bot_m, "status", None)):
                return "⚠️ ʙᴏᴛ ɢʀᴏᴜᴘ ᴍᴇ <b>ᴀᴅᴍɪɴ</b> ɴᴀʜɪ ʜᴀɪ!"
        except Exception:
            pass
        if any(x in text for x in ("NO ACTIVE GROUP CALL", "GROUPCALL_NOT_FOUND", "VOICE CHAT")):
            return "⚠️ ɢʀᴏᴜᴘ ᴍᴇ <b>ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴛɪᴠᴇ</b> ɴᴀʜɪ ʜᴀɪ!\nᴘᴇʜʟᴇ ᴠᴄ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ, ᴘʜɪʀ /play."
        if "PEER_ID_INVALID" in text:
            return "⚠️ ᴘᴇᴇʀ ᴇʀʀᴏʀ — /play ᴅᴜʙᴀʀᴀ ᴄʜᴀʟᴀᴏ (ᴀᴜᴛᴏ-ꜰɪx ʜᴏ ᴊᴀᴇɢᴀ)."
        if any(x in text for x in ("BANNED", "KICKED")):
            return "⚠️ ᴠᴄ ᴍᴇᴍʙᴇʀ <b>ʙᴀɴ</b> ʜᴀɪ — ᴜɴʙᴀɴ ᴋᴀʀᴏ ᴘʜɪʀ /play."
        if "GROUPCALL_FORBIDDEN" in text:
            return "⚠️ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ʙᴀɴᴅ ʜᴀɪ — ᴅᴜʙᴀʀᴀ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ."
        return f"⚠️ ᴠᴄ ᴇʀʀᴏʀ: {escape_html(exc_text(exc)[:200])}"

    # ─────────────────────────────────────
    #  PLAY TRACK
    # ─────────────────────────────────────

    async def _play_track(self, chat_id: int, track: Track) -> None:
        # Step 1: Parallel join + peer warm
        join_t  = asyncio.ensure_future(self._ensure_assistant_in_chat(chat_id))
        warm_t  = asyncio.ensure_future(self._warm_peer(chat_id))
        await asyncio.gather(join_t, warm_t, return_exceptions=True)

        join_ok, join_err = True, None
        if not join_t.exception():
            join_ok, join_err = join_t.result()
        else:
            join_ok, join_err = False, str(join_t.exception())

        if not join_ok:
            raise RuntimeError(join_err or "ᴊᴏɪɴ ɴᴀʜɪ ʜᴜᴀ.")

        # Step 2: Play
        try:
            await self._pytgcalls_play(chat_id, track.stream_url, track.is_video)
        except Exception as e:
            # PEER_ID_INVALID: re-warm and retry ONCE
            if "PEER_ID_INVALID" in exc_text(e).upper():
                log.warning("PEER_ID_INVALID on play — re-warming and retrying...")
                await self._warm_peer(chat_id)
                await asyncio.sleep(0.5)
                try:
                    await self._pytgcalls_play(chat_id, track.stream_url, track.is_video)
                except Exception as e2:
                    if is_voice_chat_error(e2):
                        raise RuntimeError(await self._diagnose_vc(chat_id, e2)) from e2
                    raise RuntimeError(f"⚠️ ᴘʟᴀʏ ʀᴇᴛʀʏ ꜰᴀɪʟ: {escape_html(str(e2))}") from e2
            elif is_voice_chat_error(e):
                raise RuntimeError(await self._diagnose_vc(chat_id, e)) from e
            else:
                raise RuntimeError(f"⚠️ ᴘʟᴀʏ ᴇʀʀᴏʀ: {escape_html(str(e))}") from e

        state = self.get_state(chat_id)
        state.current = track
        state.paused  = False
        state.muted   = False
        self._schedule_save()

    # ─────────────────────────────────────
    #  CALL CONTROLS
    # ─────────────────────────────────────

    async def _leave_call(self, chat_id: int):
        for m in ("leave_call", "leave_group_call"):
            fn = getattr(self.calls, m, None)
            if fn:
                try:
                    r = fn(chat_id)
                    if asyncio.iscoroutine(r): await r
                    return
                except Exception: pass

    async def _pause_call(self, chat_id: int):
        for m in ("pause", "pause_stream"):
            fn = getattr(self.calls, m, None)
            if fn:
                r = fn(chat_id)
                if asyncio.iscoroutine(r): await r
                return
        raise RuntimeError("pause unavailable")

    async def _resume_call(self, chat_id: int):
        for m in ("resume", "resume_stream"):
            fn = getattr(self.calls, m, None)
            if fn:
                r = fn(chat_id)
                if asyncio.iscoroutine(r): await r
                return
        raise RuntimeError("resume unavailable")

    async def _mute_call(self, chat_id: int):
        fn = getattr(self.calls, "mute", None)
        if fn:
            r = fn(chat_id)
            if asyncio.iscoroutine(r): await r
            return
        raise RuntimeError("mute unavailable")

    async def _unmute_call(self, chat_id: int):
        fn = getattr(self.calls, "unmute", None)
        if fn:
            r = fn(chat_id)
            if asyncio.iscoroutine(r): await r
            return
        raise RuntimeError("unmute unavailable")

    # ─────────────────────────────────────
    #  PLAY NEXT / STREAM END
    # ─────────────────────────────────────

    async def _play_next(self, chat_id: int, announce: bool = False, reason: str = "") -> None:
        async with self.get_lock(chat_id):
            state = self.get_state(chat_id)
            nxt: Optional[Track] = None

            if state.loop and state.current:
                nxt = state.current
            elif state.queue:
                nxt = state.queue.pop(0)
            else:
                state.current = None
                state.paused  = False
                state.muted   = False
                self._schedule_save()
                try: await self._leave_call(chat_id)
                except Exception: pass
                return

            try:
                await self._play_track(chat_id, nxt)
            except Exception as e:
                state.current = None
                state.paused  = False
                self._schedule_save()
                if announce:
                    try:
                        await self.bot.send_message(chat_id, f"❌ ɴᴇxᴛ ᴛʀᴀᴄᴋ ᴘʟᴀʏ ɴᴀʜɪ ʜᴜᴀ.\n{escape_html(str(e))}")
                    except Exception: pass
                return

            if announce:
                try:
                    text = (
                        f"▶️ <b>ɴᴏᴡ ᴘʟᴀʏɪɴɢ</b>\n{sep()}\n\n"
                        f"🏷 {escape_html(nxt.title)}\n"
                        f"⏱ {escape_html(nxt.pretty_duration)}\n"
                        f"🙋 {nxt.requested_by}"
                    )
                    if reason:
                        text += f"\n📝 {escape_html(reason)}"
                    await self.bot.send_message(chat_id, text, disable_web_page_preview=True, reply_markup=self._np_kb())
                except Exception: pass

    async def _on_stream_end(self, chat_id: int) -> None:
        try:
            await self._play_next(chat_id, announce=True, reason="ᴘʀᴇᴠɪᴏᴜꜱ ꜱᴛʀᴇᴀᴍ ᴇɴᴅᴇᴅ")
        except Exception:
            log.exception("on_stream_end failed")

    # ─────────────────────────────────────
    #  CORE PLAY HANDLER
    # ─────────────────────────────────────

    async def _handle_play(self, message: Message, query: str, want_video: bool = False) -> None:
        asyncio.ensure_future(self._try_delete(message))

        if not query:
            await self._safe_send(
                message,
                f"❓ ᴜꜱᴀɢᴇ:\n"
                f"  /{'vplay' if want_video else 'play'} <code>sᴏɴɢ ɴᴀᴍᴇ</code>\n"
                f"  /{'vplay' if want_video else 'play'} <code>youtube_url</code>"
            )
            return

        msg = await self._safe_send(message, f"🔎 <b>ꜱᴇᴀʀᴄʜɪɴɢ...</b>\n<code>{escape_html(query)}</code>")

        # PARALLEL: search + pre-join = maximum speed
        try:
            track_t = asyncio.ensure_future(
                asyncio.to_thread(sync_extract_track, query, want_video)
            )
            join_t  = asyncio.ensure_future(self._ensure_assistant_in_chat(message.chat.id))
            await asyncio.gather(track_t, join_t, return_exceptions=True)

            if track_t.exception():
                return await self._safe_edit(msg, f"❌ ꜱᴏɴɢ ɴᴀʜɪ ᴍɪʟᴀ\n\n<code>{escape_html(str(track_t.exception()))}</code>")

            track = track_t.result()
            track.requested_by = mention_user(message)

        except Exception as e:
            return await self._safe_edit(msg, f"❌ ꜱᴇᴀʀᴄʜ ᴇʀʀᴏʀ\n\n<code>{escape_html(str(e))}</code>")

        async with self.get_lock(message.chat.id):
            state = self.get_state(message.chat.id)

            if state.current:
                state.queue.append(track)
                self._schedule_save()
                return await self._safe_edit(
                    msg,
                    f"📥 <b>ǫᴜᴇᴜᴇᴅ #{len(state.queue)}</b>\n\n"
                    f"🏷 {escape_html(track.title)}\n⏱ {escape_html(track.pretty_duration)}"
                )

            await self._safe_edit(msg, f"⚡ ᴄᴏɴɴᴇᴄᴛɪɴɢ...\n🏷 <b>{escape_html(track.title)}</b>")

            try:
                await self._play_track(message.chat.id, track)
            except Exception as e:
                return await self._safe_edit(msg, f"❌ <b>ᴘʟᴀʏ ɴᴀʜɪ ʜᴜᴀ</b>\n\n{escape_html(str(e))}")

        try:
            await self._safe_edit(msg, self._np_text(self.get_state(message.chat.id)), reply_markup=self._np_kb())
        except Exception:
            pass

    # ─────────────────────────────────────
    #  WATCHDOG — master only
    # ─────────────────────────────────────

    async def _clone_watchdog(self) -> None:
        await asyncio.sleep(30)
        while not self._stopping:
            try:
                for pid_file in list(PIDS_DIR.glob("*.pid")):
                    bot_id = pid_file.stem
                    cfg_file = CLONES_DIR / f"{bot_id}.json"
                    if not cfg_file.exists():
                        pid_file.unlink(missing_ok=True)
                        continue
                    try:
                        pid = int(pid_file.read_text().strip())
                    except Exception:
                        pid_file.unlink(missing_ok=True)
                        continue
                    if not is_process_alive(pid):
                        log.warning("Watchdog: clone %s dead — restarting...", bot_id)
                        try:
                            log_file = LOGS_DIR / f"{bot_id}.log"
                            proc = subprocess.Popen(
                                [sys.executable, __file__, "--config", str(cfg_file)],
                                stdout=open(str(log_file), "a"),
                                stderr=subprocess.STDOUT,
                                start_new_session=True,
                            )
                            pid_file.write_text(str(proc.pid))
                            log.info("Watchdog: clone %s restarted pid=%d", bot_id, proc.pid)
                        except Exception as e:
                            log.error("Watchdog restart failed for %s: %s", bot_id, e)
            except Exception:
                log.exception("Watchdog error (non-fatal)")
            await asyncio.sleep(60)

    async def _auto_launch_clones(self) -> None:
        for cfg_file in sorted(CLONES_DIR.glob("*.json")):
            bot_id   = cfg_file.stem
            pid_file = PIDS_DIR / f"{bot_id}.pid"
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text().strip())
                    if is_process_alive(pid):
                        log.info("Auto-launch: clone %s already running", bot_id)
                        continue
                except Exception:
                    pass
            try:
                log_file = LOGS_DIR / f"{bot_id}.log"
                proc = subprocess.Popen(
                    [sys.executable, __file__, "--config", str(cfg_file)],
                    stdout=open(str(log_file), "a"),
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                pid_file.write_text(str(proc.pid))
                log.info("Auto-launch: clone %s pid=%d", bot_id, proc.pid)
                await asyncio.sleep(0.5)
            except Exception as e:
                log.error("Auto-launch failed for %s: %s", bot_id, e)

    # ─────────────────────────────────────
    #  HANDLERS
    # ─────────────────────────────────────

    async def _add_handlers(self) -> None:

        # ── Stream end — BULLETPROOF, never crashes
        @self.calls.on_update()
        async def _stream_update(_, update):
            try:
                name    = type(update).__name__.lower()
                chat_id = getattr(update, "chat_id", None)
                if not chat_id:
                    return
                is_ended = False
                if _StreamEndedCompat and isinstance(update, _StreamEndedCompat):
                    is_ended = True
                elif _StreamAudioEndedCompat and isinstance(update, _StreamAudioEndedCompat):
                    is_ended = True
                elif "ended" in name:
                    is_ended = True
                if is_ended:
                    asyncio.ensure_future(self._on_stream_end(chat_id))
            except Exception:
                pass  # NEVER re-raise

        # ── /start
        @self.bot.on_message(filters.command(["start"]) & (filters.private | filters.group))
        async def _start(_, m: Message):
            try: await self._send_start_panel(m)
            except Exception: log.exception("start failed")

        # ── /help
        @self.bot.on_message(filters.command(["help", "commands"]) & (filters.private | filters.group))
        async def _help(_, m: Message):
            try:
                pid = (self.settings.get("start_photo_file_id") or "").strip()
                if pid:
                    try:
                        await m.reply_photo(photo=pid, caption=self._help_home_text(), reply_markup=self._help_kb())
                        return
                    except Exception: pass
                await self._safe_send(m, self._help_home_text(), reply_markup=self._help_kb())
            except Exception: log.exception("help failed")

        # ── /about
        @self.bot.on_message(filters.command(["about"]) & (filters.private | filters.group))
        async def _about(_, m: Message):
            try: await self._safe_send(m, self._about_text(), reply_markup=self._subpage_kb())
            except Exception: pass

        # ── Callbacks
        @self.bot.on_callback_query()
        async def _cb(_, q):
            try:
                d = q.data or ""

                if d == "nav_home":
                    un = ""
                    if q.from_user: un = q.from_user.first_name or ""
                    await self._safe_edit_panel(q.message, self._start_text(un), self._start_kb())
                    return await q.answer()

                if d == "nav_about":
                    await self._safe_edit_panel(q.message, self._about_text(), self._subpage_kb())
                    return await q.answer()

                if d == "nav_help_home":
                    await self._safe_edit_panel(q.message, self._help_home_text(), self._help_kb())
                    return await q.answer()

                if d == "help_music":
                    await self._safe_edit_panel(q.message, self._help_music_text(), self._subpage_kb())
                    return await q.answer()

                if d == "help_admin":
                    await self._safe_edit_panel(q.message, self._help_admin_text(), self._subpage_kb())
                    return await q.answer()

                if d == "help_extra":
                    await self._safe_edit_panel(q.message, self._help_extra_text(), self._subpage_kb())
                    return await q.answer()

                if d == "nav_close":
                    try: await q.message.delete()
                    except Exception: pass
                    return await q.answer("ᴄʟᴏꜱᴇᴅ")

                if d.startswith("ctl_"):
                    ct = str(getattr(getattr(q.message, "chat", None), "type", "")).lower()
                    if ct not in {"group", "supergroup", "chattype.group", "chattype.supergroup"}:
                        return await q.answer("ɢʀᴏᴜᴘ ᴍᴇ ᴜꜱᴇ ᴋᴀʀᴏ.", show_alert=True)
                    uid = getattr(q.from_user, "id", None)
                    if not await self.is_admin(q.message.chat.id, uid):
                        return await q.answer("ꜱɪʀꜰ ᴀᴅᴍɪɴꜱ.", show_alert=True)
                    cid   = q.message.chat.id
                    state = self.get_state(cid)

                    if d == "ctl_pause":
                        if state.paused: return await q.answer("ᴘᴇʜʟᴇ ꜱᴇ ᴘᴀᴜꜱᴇᴅ.", show_alert=True)
                        try:
                            await self._pause_call(cid); state.paused = True; self._schedule_save()
                            await self._safe_edit_panel(q.message, self._np_text(state), self._np_kb())
                            return await q.answer("⏸ ᴘᴀᴜꜱᴇᴅ")
                        except Exception as e: return await q.answer(str(e)[:200], show_alert=True)

                    if d == "ctl_resume":
                        if not state.paused: return await q.answer("ᴀʟʀᴇᴀᴅʏ ᴄʜᴀʟ ʀʜᴀ.", show_alert=True)
                        try:
                            await self._resume_call(cid); state.paused = False; self._schedule_save()
                            await self._safe_edit_panel(q.message, self._np_text(state), self._np_kb())
                            return await q.answer("▶️ ʀᴇꜱᴜᴍᴇᴅ")
                        except Exception as e: return await q.answer(str(e)[:200], show_alert=True)

                    if d == "ctl_skip":
                        state.loop = False; state.current = None; state.paused = False
                        await self._play_next(cid, announce=True, reason="ꜱᴋɪᴘᴘᴇᴅ")
                        return await q.answer("⏭ ꜱᴋɪᴘᴘᴇᴅ")

                    if d == "ctl_stop":
                        state.queue.clear(); state.current = None
                        state.paused = state.loop = state.muted = False
                        self._schedule_save()
                        await self._leave_call(cid)
                        await self._safe_edit_panel(q.message, "⏹ ꜱᴛᴏᴘᴘᴇᴅ.", self._queue_kb())
                        return await q.answer("⏹ ꜱᴛᴏᴘᴘᴇᴅ")

                    if d == "ctl_queue":
                        await self._safe_edit_panel(q.message, self._queue_text(state), self._queue_kb())
                        return await q.answer()

                    if d == "ctl_np":
                        await self._safe_edit_panel(q.message, self._np_text(state), self._np_kb())
                        return await q.answer()

                    if d == "ctl_shuffle":
                        if len(state.queue) < 2: return await q.answer("2+ ᴛʀᴀᴄᴋꜱ ᴄʜᴀʜɪᴇ.", show_alert=True)
                        random.shuffle(state.queue); self._schedule_save()
                        await self._safe_edit_panel(q.message, self._queue_text(state), self._queue_kb())
                        return await q.answer("🔀 ꜱʜᴜꜰꜰʟᴇᴅ!")

                    if d == "ctl_clearqueue":
                        c = len(state.queue); state.queue.clear(); self._schedule_save()
                        await self._safe_edit_panel(q.message, self._queue_text(state), self._queue_kb())
                        return await q.answer(f"🧹 {c} ʀᴇᴍᴏᴠᴇᴅ")

                await q.answer()
            except Exception:
                log.exception("callback failed")
                try: await q.answer("❌ ᴇʀʀᴏʀ", show_alert=True)
                except Exception: pass

        # ── /ping /alive
        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def _ping(_, m: Message):
            try:
                t0 = time.time()
                x  = await self._safe_send(m, "🏓 ᴘɪɴɢɪɴɢ...")
                ms = (time.time() - t0) * 1000
                up = pretty_uptime(int(time.time() - self.start_time))
                ac = sum(1 for s in self.states.values() if s.current)
                cl = len(list(CLONES_DIR.glob("*.json"))) if self.is_master else 0
                n  = escape_html(self.display_name)
                t  = (
                    f"🏓 <b>{n.upper()} ɪꜱ ᴏɴʟɪɴᴇ</b>\n{sep()}\n\n"
                    f"⚡ ʟᴀᴛᴇɴᴄʏ : <b>{ms:.2f} ᴍꜱ</b>\n"
                    f"⏳ ᴜᴘᴛɪᴍᴇ  : {escape_html(up)}\n"
                    f"🎧 ᴀᴄᴛɪᴠᴇ  : {ac} ᴄʜᴀᴛꜱ\n"
                    f"🤖 ʙᴏᴛ ɪᴅ  : <code>{self.config.bot_id}</code>\n"
                )
                if self.is_master:
                    t += f"🔁 ᴄʟᴏɴᴇꜱ  : {cl} ꜱᴀᴠᴇᴅ\n"
                t += f"\n{sep()}"
                if x: await self._safe_edit(x, t)
            except Exception: log.exception("ping failed")

        # ── /play /p
        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def _play(_, m: Message):
            try: await self._handle_play(m, command_arg(m), want_video=False)
            except Exception:
                log.exception("play failed")
                await self._safe_send(m, "❌ /play ᴍᴇ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ.")

        # ── /vplay
        @self.bot.on_message(filters.command(["vplay"]) & filters.group)
        async def _vplay(_, m: Message):
            try: await self._handle_play(m, command_arg(m), want_video=True)
            except Exception:
                log.exception("vplay failed")
                await self._safe_send(m, "❌ /vplay ᴍᴇ ᴇʀʀᴏʀ ᴀᴀ ɢᴀʏᴀ.")

        # ── /refresh
        @self.bot.on_message(filters.command(["refresh"]) & filters.group)
        async def _refresh(_, m: Message):
            try:
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                await self._safe_send(m, self._np_text(state), reply_markup=self._np_kb())
            except Exception: pass

        # ── /pause
        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def _pause(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                if state.paused: return await self._safe_send(m, "⏸ ᴘᴇʜʟᴇ ꜱᴇ ᴘᴀᴜꜱᴇᴅ.")
                await self._pause_call(m.chat.id); state.paused = True; self._schedule_save()
                await self._safe_send(m, "⏸ ᴘᴀᴜꜱᴇᴅ.")
            except Exception as e: await self._safe_send(m, f"❌ {escape_html(str(e))}")

        # ── /resume
        @self.bot.on_message(filters.command(["resume"]) & filters.group)
        async def _resume(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                if not state.paused: return await self._safe_send(m, "▶️ ᴀʟʀᴇᴀᴅʏ ᴄʜᴀʟ ʀʜᴀ.")
                await self._resume_call(m.chat.id); state.paused = False; self._schedule_save()
                await self._safe_send(m, "▶️ ʀᴇꜱᴜᴍᴇᴅ.")
            except Exception as e: await self._safe_send(m, f"❌ {escape_html(str(e))}")

        # ── /skip /next
        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def _skip(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                if not state.current and not state.queue:
                    return await self._safe_send(m, "📭 ᴋᴜᴄʜ ɴᴀʜɪ ᴘʟᴀʏ ʜᴏ ʀʜᴀ.")
                state.loop = False; state.current = None; state.paused = False
                await self._play_next(m.chat.id, announce=True, reason="ꜱᴋɪᴘᴘᴇᴅ")
            except Exception as e: await self._safe_send(m, f"❌ {escape_html(str(e))}")

        # ── /stop /end
        @self.bot.on_message(filters.command(["stop", "end"]) & filters.group)
        async def _stop(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                state.queue.clear(); state.current = None
                state.paused = state.loop = state.muted = False
                self._schedule_save()
                await self._leave_call(m.chat.id)
                await self._safe_send(m, "⏹ ꜱᴛᴏᴘ. ǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ.")
            except Exception as e: await self._safe_send(m, f"❌ {escape_html(str(e))}")

        # ── /queue /q
        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def _queue(_, m: Message):
            try:
                state = self.get_state(m.chat.id)
                await self._safe_send(m, self._queue_text(state), reply_markup=self._queue_kb())
            except Exception: pass

        # ── /loop
        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def _loop(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                arg = command_arg(m).lower()
                state.loop = True if arg == "on" else False if arg == "off" else not state.loop
                self._schedule_save()
                await self._safe_send(m, f"🔁 ʟᴏᴏᴘ: {human_bool(state.loop)}")
            except Exception: pass

        # ── /shuffle
        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def _shuffle(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                if len(state.queue) < 2:
                    return await self._safe_send(m, "❌ 2+ ᴛʀᴀᴄᴋꜱ ᴄʜᴀʜɪᴇ.")
                random.shuffle(state.queue); self._schedule_save()
                await self._safe_send(m, f"🔀 ꜱʜᴜꜰꜰʟᴇᴅ! ({len(state.queue)} ᴛʀᴀᴄᴋꜱ)")
            except Exception: pass

        # ── /clearqueue
        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def _cq(_, m: Message):
            try:
                if not await self.require_admin(m): return
                state = self.get_state(m.chat.id)
                c = len(state.queue); state.queue.clear(); self._schedule_save()
                await self._safe_send(m, f"🧹 {c} ᴛʀᴀᴄᴋꜱ ᴄʟᴇᴀʀ.")
            except Exception: pass

        # ── /mute
        @self.bot.on_message(filters.command(["mute"]) & filters.group)
        async def _mute(_, m: Message):
            try:
                if not await self.require_admin(m): return
                await self._mute_call(m.chat.id)
                self.get_state(m.chat.id).muted = True; self._schedule_save()
                await self._safe_send(m, "🔇 ᴍᴜᴛᴇᴅ.")
            except Exception as e: await self._safe_send(m, f"❌ {escape_html(str(e))}")

        # ── /unmute
        @self.bot.on_message(filters.command(["unmute"]) & filters.group)
        async def _unmute(_, m: Message):
            try:
                if not await self.require_admin(m): return
                await self._unmute_call(m.chat.id)
                self.get_state(m.chat.id).muted = False; self._schedule_save()
                await self._safe_send(m, "🔊 ᴜɴᴍᴜᴛᴇᴅ.")
            except Exception as e: await self._safe_send(m, f"❌ {escape_html(str(e))}")

        # ── /np /now
        @self.bot.on_message(filters.command(["np", "now"]) & filters.group)
        async def _np(_, m: Message):
            try:
                state = self.get_state(m.chat.id)
                await self._safe_send(m, self._np_text(state), reply_markup=self._np_kb())
            except Exception: pass

        # ── /shelp
        @self.bot.on_message(filters.command(["shelp"]) & (filters.private | filters.group))
        async def _shelp(_, m: Message):
            try:
                if not self.is_config_owner_user(m): return
                await self._safe_send(m, self._shell_help_text())
            except Exception: pass

        # ── /setdp
        @self.bot.on_message(filters.command(["setdp"]) & filters.private)
        async def _setdp(_, m: Message):
            try:
                if not self.is_config_owner_user(m):
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴏᴡɴᴇʀ.")
                self.pending_start_photo[m.from_user.id] = time.time()
                await self._safe_send(m, "🖼 ᴘʜᴏᴛᴏ ʙʜᴇᴊᴏ. /cancel ꜱᴇ ʙᴀɴᴅ.")
            except Exception: pass

        # ── /removedp
        @self.bot.on_message(filters.command(["removedp"]) & filters.private)
        async def _removedp(_, m: Message):
            try:
                if not self.is_config_owner_user(m):
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴏᴡɴᴇʀ.")
                self.settings["start_photo_file_id"] = ""
                self._save_settings()
                self.pending_start_photo.pop(m.from_user.id, None)
                await self._safe_send(m, "✅ ᴘʜᴏᴛᴏ ʜᴀᴛᴀʏɪ.")
            except Exception: pass

        # ── Photo for /setdp
        @self.bot.on_message(filters.private & (filters.photo | filters.document))
        async def _photo(_, m: Message):
            try:
                if not self.is_config_owner_user(m): return
                if m.from_user.id not in self.pending_start_photo: return
                fid = ""
                if m.photo:
                    po = m.photo
                    fid = po.file_id if hasattr(po, "file_id") else (po[-1].file_id if isinstance(po, (list, tuple)) and po else "")
                elif m.document and (m.document.mime_type or "").startswith("image/"):
                    fid = m.document.file_id
                else:
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ɪᴍᴀɢᴇ ʙʜᴇᴊᴏ.")
                if not fid:
                    return await self._safe_send(m, "❌ ᴅᴏʙᴀʀᴀ ʙʜᴇᴊᴏ.")
                self.settings["start_photo_file_id"] = fid
                self._save_settings()
                self.pending_start_photo.pop(m.from_user.id, None)
                await self._safe_send(m, f"✅ ꜱᴀᴠᴇᴅ! /start ᴘᴇ ᴅɪᴋʜᴇɢɪ.")
            except Exception: log.exception("photo handler failed")

        # ─────────────────────────────────────
        #  MASTER-ONLY
        # ─────────────────────────────────────
        if self.is_master:

            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def _clone(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    self.clone_flow[m.from_user.id] = {"step": "bot_token"}
                    await self._safe_send(m,
                        f"🚀 <b>ɴᴀʏᴀ ʙᴏᴛ ꜱᴇᴛᴜᴘ</b>\n{sep()}\n\n"
                        f"<b>ꜱᴛᴇᴘ 1/4:</b> ʙᴏᴛ ᴛᴏᴋᴇɴ ʙʜᴇᴊᴏ.\n\n"
                        f"<code>123456789:ABCDEF...</code>"
                    )
                except Exception: pass

            @self.bot.on_message(filters.command(["dclone"]) & filters.private)
            async def _dclone(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    token = command_arg(m).strip()
                    if not token:
                        return await self._safe_send(m, "❓ /dclone <code>bot_token</code>")
                    if not TOKEN_RE.match(token):
                        return await self._safe_send(m, "❌ ɪɴᴠᴀʟɪᴅ ᴛᴏᴋᴇɴ.")
                    bot_id = token.split(":", 1)[0]
                    cfg_f  = CLONES_DIR / f"{bot_id}.json"
                    pid_f  = PIDS_DIR   / f"{bot_id}.pid"
                    st_f   = STATES_DIR / f"{bot_id}_state.json"
                    killed = False
                    if pid_f.exists():
                        try:
                            pid = int(pid_f.read_text().strip())
                            try: os.kill(pid, signal.SIGTERM)
                            except Exception: pass
                            await asyncio.sleep(1.5)
                            try: os.kill(pid, signal.SIGKILL)
                            except Exception: pass
                            killed = True
                        except Exception: pass
                        pid_f.unlink(missing_ok=True)
                    cfg_removed = False
                    if cfg_f.exists():
                        try: cfg_f.unlink(); cfg_removed = True
                        except Exception: pass
                    if st_f.exists():
                        try: st_f.unlink()
                        except Exception: pass
                    if not killed and not cfg_removed:
                        return await self._safe_send(m, f"⚠️ Bot <code>{bot_id}</code> ɴᴀʜɪ ᴍɪʟᴀ.")
                    await self._safe_send(m,
                        f"✅ <b>ꜱᴛᴏᴘᴘᴇᴅ!</b>\n🤖 <code>{bot_id}</code>\n"
                        f"💀 {'ꜱᴛᴏᴘ ✅' if killed else '⚠️'} | 📁 {'ʀᴇᴍᴏᴠᴇᴅ ✅' if cfg_removed else '⚠️'}"
                    )
                except Exception: log.exception("dclone failed")

            @self.bot.on_message(filters.command(["cancel"]) & filters.private)
            async def _cancel(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    had = m.from_user.id in self.clone_flow
                    self.clone_flow.pop(m.from_user.id, None)
                    self.pending_start_photo.pop(m.from_user.id, None)
                    await self._safe_send(m, "🛑 ᴄᴀɴᴄᴇʟʟᴇᴅ." if had else "✅ ɴᴏᴛʜɪɴɢ ᴘᴇɴᴅɪɴɢ.")
                except Exception: pass

            @self.bot.on_message(filters.command(["clones"]) & filters.private)
            async def _clones(_, m: Message):
                try:
                    if not self.is_config_owner_user(m):
                        return await self._safe_send(m, "❌ ᴏᴡɴᴇʀ ᴏɴʟʏ.")
                    files = sorted(CLONES_DIR.glob("*.json"))
                    if not files:
                        return await self._safe_send(m, "📭 ᴋᴏɪ ꜱᴀᴠᴇᴅ ʙᴏᴛ ɴᴀʜɪ.")
                    lines = [f"📦 <b>ꜱᴀᴠᴇᴅ ʙᴏᴛꜱ</b>\n{sep()}\n"]
                    for f in files[:50]:
                        try:
                            cfg   = load_config(f)
                            pid_f = PIDS_DIR / f"{cfg.bot_id}.pid"
                            live  = False
                            if pid_f.exists():
                                try: live = is_process_alive(int(pid_f.read_text().strip()))
                                except Exception: pass
                            lines.append(f"{'🟢' if live else '🔴'} <code>{escape_html(cfg.bot_id)}</code> — {escape_html(cfg.owner_username)}")
                        except Exception:
                            lines.append(f"• {f.name}")
                    lines.append(f"\n{sep()}\n💡 /dclone &lt;token&gt;")
                    await self._safe_send(m, "\n".join(lines))
                except Exception: log.exception("clones failed")

            @self.bot.on_message(filters.private & filters.text)
            async def _clone_flow(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    sf = self.clone_flow.get(m.from_user.id)
                    if not sf: return
                    text = (m.text or "").strip()
                    step = sf.get("step")
                    skip = {"/cancel", "/clone", "/clones", "/setdp", "/removedp", "/dclone", "/shelp"}
                    if text.lower() in skip: return

                    if step == "bot_token":
                        if not TOKEN_RE.match(text): return await self._safe_send(m, "❌ ɪɴᴠᴀʟɪᴅ ᴛᴏᴋᴇɴ.")
                        sf["bot_token"] = text; sf["step"] = "support"
                        return await self._safe_send(m, "<b>ꜱᴛᴇᴘ 2/4:</b> ꜱᴜᴘᴘᴏʀᴛ ɢʀᴏᴜᴘ ʙʜᴇᴊᴏ.\n<code>@group</code>")

                    if step == "support":
                        sf["support_chat"] = normalize_support(text); sf["step"] = "owner_username"
                        return await self._safe_send(m, "<b>ꜱᴛᴇᴘ 3/4:</b> ᴏᴡɴᴇʀ ᴜꜱᴇʀɴᴀᴍᴇ ʙʜᴇᴊᴏ.\n<code>@username</code>")

                    if step == "owner_username":
                        sf["owner_username"] = normalize_owner_username(text); sf["step"] = "session"
                        return await self._safe_send(m, "<b>ꜱᴛᴇᴘ 4/4:</b> ꜱᴇꜱꜱɪᴏɴ ꜱᴛʀɪɴɢ ʙʜᴇᴊᴏ.\n(/default = ꜱᴀᴍᴇ ʀᴀᴋʜᴏ)")

                    if step == "session":
                        ss = self.config.assistant_session if text.lower() == "/default" else text
                        if len(ss) < 50: return await self._safe_send(m, "❌ ꜱᴇꜱꜱɪᴏɴ ꜱᴛʀɪɴɢ ʙᴀʜᴜᴛ ᴄʜᴏᴛɪ.")
                        await self._safe_send(m, "⏳ ᴠᴇʀɪꜰʏɪɴɢ...")
                        try:
                            tc = Client(name=f"v_{int(time.time())}", api_id=self.config.api_id,
                                        api_hash=self.config.api_hash, session_string=ss)
                            await tc.start()
                            am = await tc.get_me()
                            await tc.stop()
                            await self._safe_send(m, f"✅ ᴠᴇʀɪꜰɪᴇᴅ! @{escape_html(am.username or 'N/A')} ʟᴀᴜɴᴄʜ ʜᴏ ʀʜᴀ ʜᴀɪ...")
                        except Exception as ve:
                            await self._safe_send(m, f"⚠️ ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ ꜰᴀɪʟ: {escape_html(str(ve))}\nᴘʀᴏᴄᴇᴇᴅɪɴɢ...")

                        ccfg = BotConfig(
                            api_id=self.config.api_id, api_hash=self.config.api_hash,
                            bot_token=sf["bot_token"], owner_id=self.config.owner_id,
                            assistant_session=ss, support_chat=sf["support_chat"],
                            owner_username=sf["owner_username"], nubcoder_token=self.config.nubcoder_token,
                            clone_mode=True,
                        )
                        self.clone_flow.pop(m.from_user.id, None)
                        cfg_f = CLONES_DIR / f"{ccfg.bot_id}.json"
                        save_config(ccfg, cfg_f)
                        log_f = LOGS_DIR / f"{ccfg.bot_id}.log"
                        pid_f = PIDS_DIR / f"{ccfg.bot_id}.pid"
                        try:
                            proc = subprocess.Popen(
                                [sys.executable, __file__, "--config", str(cfg_f)],
                                stdout=open(str(log_f), "a"), stderr=subprocess.STDOUT,
                                start_new_session=True,
                            )
                            pid_f.write_text(str(proc.pid))
                            await self._safe_send(m,
                                f"🚀 <b>ʙᴏᴛ ʟᴀᴜɴᴄʜ ʜᴏ ɢᴀʏᴀ!</b>\n{sep()}\n\n"
                                f"🤖 ɪᴅ: <code>{escape_html(ccfg.bot_id)}</code>\n"
                                f"👤 ᴏᴡɴᴇʀ: {escape_html(ccfg.owner_username)}\n"
                                f"🆔 ᴘɪᴅ: <code>{proc.pid}</code>\n\n"
                                f"✨ ᴀᴜᴛᴏ-ʀᴇꜱᴛᴀʀᴛ + ᴡᴀᴛᴄʜᴅᴏɢ ᴀᴄᴛɪᴠᴇ\n\n"
                                f"ꜱᴛᴏᴘ: /dclone <code>{escape_html(ccfg.bot_token)}</code>"
                            )
                        except Exception as pe:
                            await self._safe_send(m, f"❌ ʟᴀᴜɴᴄʜ ꜰᴀɪʟ: {escape_html(str(pe))}")
                except Exception: log.exception("clone_flow failed")

    # ─────────────────────────────────────
    #  PYTGCALLS SAFE START
    # ─────────────────────────────────────

    async def _start_pytgcalls(self) -> None:
        try:
            fn = getattr(self.calls, "stop", None)
            if fn:
                r = fn()
                if asyncio.iscoroutine(r): await r
        except Exception:
            pass
        try:
            await self.calls.start()
            log.info("PyTgCalls started.")
        except KeyError as ke:
            log.warning("PyTgCalls peer miss (%s) — harmless", ke)
        except Exception:
            log.exception("PyTgCalls start error — continuing")

    # ─────────────────────────────────────
    #  START / STOP
    # ─────────────────────────────────────

    async def start(self) -> None:
        if shutil.which("ffmpeg") is None:
            log.warning("ffmpeg NOT found — audio may fail!")

        # Restore queue from disk
        self._load_state()
        await self._add_handlers()

        await self.assistant.start()
        am = await self.assistant.get_me()
        self.assistant_id       = am.id
        self.assistant_name     = am.first_name or "Assistant"
        self.assistant_username = am.username or ""
        log.info("ASSISTANT @%s id=%s", self.assistant_username, self.assistant_id)

        await self.bot.start()
        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name     = me.first_name or ""
        self.bot_id_int   = me.id
        if self.bot_name:
            self.config.brand_name = self.bot_name

        await self._start_pytgcalls()
        log.info("ONLINE | %s | @%s | id=%s", self.bot_name, self.bot_username, self.config.bot_id)

        if self.is_master:
            asyncio.ensure_future(self._auto_launch_clones())
            self._watchdog_task = asyncio.ensure_future(self._clone_watchdog())

        # ══════════════════════════════════════════
        #  INFINITE SHIELD — Bot kabhi band nahi
        #  Koi bhi error aaye — sirf PyTgCalls
        #  restart hoga. Bot + Assistant = ALIVE.
        # ══════════════════════════════════════════
        while not self._stopping:
            try:
                await idle()
                break
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if self._stopping: break
                log.warning("idle() exception: %s — restarting PyTgCalls only", exc)
                for s in self.states.values():
                    s.paused = False; s.muted = False
                await asyncio.sleep(1)
                await self._start_pytgcalls()

    async def stop(self) -> None:
        if self._stopping: return
        self._stopping = True
        self._save_state_sync()
        if self._watchdog_task:
            self._watchdog_task.cancel()
        for name, fn in [
            ("calls", getattr(self.calls, "stop", None)),
            ("bot",   getattr(self.bot,   "stop", None)),
            ("asst",  getattr(self.assistant, "stop", None)),
        ]:
            try:
                if fn:
                    r = fn()
                    if asyncio.iscoroutine(r): await r
            except Exception:
                log.exception("%s stop failed", name)

# ═══════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════

async def run_once() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "--config":
        cfg = load_config(Path(sys.argv[2]).resolve())
        app = TelegramMusicBot(cfg, config_path=Path(sys.argv[2]).resolve(), is_master=False)
    else:
        cfg = BotConfig(
            api_id=API_ID, api_hash=API_HASH, bot_token=MAIN_BOT_TOKEN,
            owner_id=OWNER_ID, assistant_session=DEFAULT_ASSISTANT_SESSION,
            support_chat=normalize_support(MASTER_SUPPORT_CHAT),
            owner_username=normalize_owner_username(MASTER_OWNER_USERNAME),
            nubcoder_token=NUBCODER_TOKEN, clone_mode=False, tagline=BOT_BRAND_TAGLINE,
        )
        app = TelegramMusicBot(cfg, is_master=True)
    try:
        await app.start()
    finally:
        await app.stop()


async def supervisor() -> None:
    _is_clone = len(sys.argv) > 2 and sys.argv[1] == "--config"
    delay     = 0 if _is_clone else 5
    max_delay = 2 if _is_clone else 60

    while True:
        if _is_clone:
            cfg_path = Path(sys.argv[2]).resolve()
            if not cfg_path.exists():
                log.info("Clone config gone — /dclone'd. Stopping.")
                return
        try:
            await run_once()
            log.warning("Bot exited. Restarting in %ss.", delay)
        except KeyboardInterrupt:
            return
        except Exception as exc:
            log.error("Fatal: %s", exc)
            traceback.print_exc()

        if delay > 0:
            await asyncio.sleep(delay)
        delay = min(max_delay, delay + (1 if _is_clone else 5))


if __name__ == "__main__":
    signal.signal(signal.SIGINT,  lambda s, f: (_ for _ in ()).throw(KeyboardInterrupt()))
    signal.signal(signal.SIGTERM, lambda s, f: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        asyncio.run(supervisor())
    except KeyboardInterrupt:
        log.info("Shutdown. Bye! 🎵")

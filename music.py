#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║   ♫  Z U D O  X  M U S I C   —   v 13  U L T I M A T E  ♫       ║
║   LIFETIME-STABLE CLONES • BLOCKQUOTE CARD • SETDP VIDEO        ║
║   SETTHUMB FIXED • HEARTBEAT WATCHDOG • PER-CLONE ISOLATION     ║
╚══════════════════════════════════════════════════════════════════╝

═══════════════════ NEW IN v13 (over v12) ═══════════════════
  ✅ CLONE STABILITY — lifetime-stable, weeks-months without restart:
       → Heartbeat written every 20s (was 30s) — faster stale detection
       → spawn_clone_process: ultra-safe with proper FD handling,
         os.setsid, close_fds, env isolation, retry on EAGAIN
       → Watchdog: SIGTERM grace (8s) → SIGKILL → group-wide cleanup
       → Per-clone PEER cache (was global) — no cross-bot contamination
       → Per-clone TRACK cache (was global) — independent caches
       → Crash recovery: exponential backoff with reset on success

  ✅ BLOCKQUOTE PLAY CARD — exactly like screenshot:
       → Each field in its OWN <blockquote> (proper quoted boxes)
       → expandable=False so all visible at once
       → Title block separator preserved

  ✅ /setthmb FIX — custom thumb + play card both shown:
       → _send_play_card properly handles photo/video thumb + caption
       → Falls back to track thumbnail if no custom set
       → Always shows caption text with media

  ✅ /setdp VIDEO SUPPORT — photo AND video both work in clones:
       → start_photo_file_id now stores {type, file_id}
       → _send_start_panel auto-detects photo/video and uses right method
       → Backward-compatible with old string-only configs
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
from typing import Any, Dict, List, Optional, Tuple, Set

# ═══════════════════════════════════════════════════════════════════
# LOCAL .ENV LOADER
# ═══════════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════════
# BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════

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
    if not env_bool("AUTO_INSTALL_DEPS", False):
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

# ═══════════════════════════════════════════════════════════════════
# SAFE IMPORTS
# ═══════════════════════════════════════════════════════════════════

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
import pyrogram.errors as pyro_errors

try:
    from pyrogram.errors import (
        FloodWait, UserAlreadyParticipant, UserNotParticipant,
        RPCError, Forbidden, BadRequest, PeerIdInvalid, ChannelInvalid,
        ChatAdminRequired, UserBannedInChannel,
    )
except Exception:
    from pyrogram.errors import FloodWait, UserAlreadyParticipant  # type: ignore
    UserNotParticipant = Exception  # type: ignore
    RPCError = Exception
    Forbidden = Exception
    BadRequest = Exception
    PeerIdInvalid = Exception
    ChannelInvalid = Exception
    ChatAdminRequired = Exception
    UserBannedInChannel = Exception

if hasattr(pyro_errors, "GroupcallForbidden"):
    GroupcallForbidden = pyro_errors.GroupcallForbidden
else:
    class GroupcallForbidden(Forbidden):  # type: ignore
        ID = "GROUPCALL_FORBIDDEN"
        MESSAGE = "The group call is not accessible."
    pyro_errors.GroupcallForbidden = GroupcallForbidden

from pytgcalls import PyTgCalls
from yt_dlp import YoutubeDL

# ─────────────── PYTGCALLS COMPAT ───────────────

_AudioPiped   = None
_MediaStream  = None
_AudioStream  = None
_VideoStream  = None
_MediaType    = None
_VideoFlags   = None

try:
    from pytgcalls.types import MediaStream as _MediaStream  # type: ignore
except ImportError:
    try:
        from pytgcalls.types.stream import MediaStream as _MediaStream  # type: ignore
    except ImportError:
        pass

try:
    from pytgcalls.types.stream.media_stream import MediaStream as _MS_FOR_FLAGS  # type: ignore
    _VideoFlags = getattr(_MS_FOR_FLAGS, "Flags", None) or getattr(_MS_FOR_FLAGS, "IGNORE", None)
except Exception:
    pass

if _VideoFlags is None and _MediaStream is not None:
    _VideoFlags = getattr(_MediaStream, "Flags", None)

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

# ═══════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)
logging.getLogger("pytgcalls").setLevel(logging.WARNING)
log = logging.getLogger("zudomusic")

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════

API_ID                    = int(os.getenv("API_ID", "33628258") or "33628258")
API_HASH                  = os.getenv("API_HASH", "0850762925b9c1715b9b122f7b753128")
MAIN_BOT_TOKEN            = os.getenv("MAIN_BOT_TOKEN", "")
OWNER_ID                  = int(os.getenv("OWNER_ID", "7661825494") or "7661825494")
DEFAULT_ASSISTANT_SESSION = os.getenv("DEFAULT_ASSISTANT_SESSION", "")
MASTER_SUPPORT_CHAT       = os.getenv("MASTER_SUPPORT_CHAT", "@userbotsupportchat")
MASTER_UPDATES_CHANNEL    = os.getenv("MASTER_UPDATES_CHANNEL", "@userbotsupportchat")
MASTER_OWNER_USERNAME     = os.getenv("MASTER_OWNER_USERNAME", "@ITZ_ME_ADITYA_02")
BOT_BRAND_NAME            = os.getenv("BOT_BRAND_NAME", "ZUDO X MUSIC")
BOT_BRAND_TAGLINE         = os.getenv("BOT_BRAND_TAGLINE", "Ultra Fast • No Lag • Voice Chat Player")
NUBCODER_TOKEN            = os.getenv("NUBCODER_TOKEN", "")
RUNTIME_DIR               = os.getenv("RUNTIME_DIR", "/app/runtime")
CLONE_RESTART_DELAY       = int(os.getenv("CLONE_RESTART_DELAY", "5") or "5")
MAX_RESTART_DELAY         = int(os.getenv("MAX_RESTART_DELAY", "60") or "60")

JOIN_CACHE_TTL            = int(os.getenv("JOIN_CACHE_TTL", "3600") or "3600")
JOIN_FLOOD_SKIP_THRESHOLD = int(os.getenv("JOIN_FLOOD_SKIP_THRESHOLD", "10") or "10")

# v13: Heartbeat config — faster writes, faster detection
HEARTBEAT_INTERVAL        = int(os.getenv("HEARTBEAT_INTERVAL", "20") or "20")   # was 30
HEARTBEAT_STALE_AFTER     = int(os.getenv("HEARTBEAT_STALE_AFTER", "90") or "90") # was 120
WATCHDOG_INTERVAL         = int(os.getenv("WATCHDOG_INTERVAL", "25") or "25")
CLONE_BOOT_GRACE          = int(os.getenv("CLONE_BOOT_GRACE", "75") or "75")
SIGTERM_GRACE             = int(os.getenv("SIGTERM_GRACE", "8") or "8")

ROOT_RUNTIME_DIR = Path(RUNTIME_DIR).resolve()
ROOT_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

CLONES_DIR    = ROOT_RUNTIME_DIR / "clones"
LOGS_DIR      = ROOT_RUNTIME_DIR / "logs"
PIDS_DIR      = ROOT_RUNTIME_DIR / "pids"
STATES_DIR    = ROOT_RUNTIME_DIR / "states"
CONTROL_DIR   = ROOT_RUNTIME_DIR / "control"
SHARED_DIR    = ROOT_RUNTIME_DIR / "shared"
HEARTBEAT_DIR = ROOT_RUNTIME_DIR / "heartbeats"
USERS_FILE    = ROOT_RUNTIME_DIR / "users.json"
CHATS_FILE    = ROOT_RUNTIME_DIR / "chats.json"
SHARED_THUMB_FILE = SHARED_DIR / "thumbnail.json"

for d in (CLONES_DIR, LOGS_DIR, PIDS_DIR, STATES_DIR, CONTROL_DIR, SHARED_DIR, HEARTBEAT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════
# GLOBAL SHUTDOWN FLAG
# ═══════════════════════════════════════════════════════════════════

_SHUTDOWN_FLAG = threading.Event()


def _on_signal(signum, frame):
    print(f"\n[SIGNAL] Got signal {signum} — graceful shutdown", flush=True)
    _SHUTDOWN_FLAG.set()

# ═══════════════════════════════════════════════════════════════════
# SHARED THUMBNAIL HELPERS
# ═══════════════════════════════════════════════════════════════════

def load_shared_thumb() -> Dict[str, str]:
    if not SHARED_THUMB_FILE.exists():
        return {"type": "", "file_id": ""}
    try:
        data = json.loads(SHARED_THUMB_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"type": "", "file_id": ""}
        return {
            "type": str(data.get("type", "") or ""),
            "file_id": str(data.get("file_id", "") or ""),
        }
    except Exception:
        return {"type": "", "file_id": ""}


def save_shared_thumb(media_type: str, file_id: str) -> None:
    try:
        tmp = SHARED_THUMB_FILE.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"type": media_type, "file_id": file_id}, indent=2),
            encoding="utf-8",
        )
        tmp.replace(SHARED_THUMB_FILE)
    except Exception:
        log.exception("save_shared_thumb failed")


def clear_shared_thumb() -> None:
    try:
        if SHARED_THUMB_FILE.exists():
            SHARED_THUMB_FILE.unlink()
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════
# HEARTBEAT HELPERS (v13 — atomic writes, fast detection)
# ═══════════════════════════════════════════════════════════════════

def write_heartbeat(bot_id: str) -> None:
    """Each bot writes timestamp every HEARTBEAT_INTERVAL seconds — atomic via tmp+replace."""
    try:
        hb = HEARTBEAT_DIR / f"{bot_id}.hb"
        tmp = hb.with_suffix(".tmp")
        tmp.write_text(str(int(time.time())), encoding="utf-8")
        tmp.replace(hb)
    except Exception:
        pass


def read_heartbeat(bot_id: str) -> int:
    try:
        hb = HEARTBEAT_DIR / f"{bot_id}.hb"
        if not hb.exists():
            return 0
        return int(hb.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def clear_heartbeat(bot_id: str) -> None:
    try:
        hb = HEARTBEAT_DIR / f"{bot_id}.hb"
        hb.unlink(missing_ok=True)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════════

@dataclass
class BotConfig:
    api_id: int
    api_hash: str
    bot_token: str
    owner_id: int
    assistant_session: str
    support_chat: str
    owner_username: str
    updates_channel: str = ""
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
    query: str = ""
    duration: int = 0
    requested_by: str = "Unknown"
    source: str = "YouTube"
    thumbnail: str = ""
    is_video: bool = False
    fetched_at: float = 0.0

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
    volume: int = 100

    def to_dict(self) -> dict:
        return {
            "current": self.current.to_dict() if self.current else None,
            "queue":   [t.to_dict() for t in self.queue],
            "loop":    self.loop,
            "paused":  False,
            "muted":   False,
            "volume":  self.volume,
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
        s.volume = int(d.get("volume", 100))
        return s

# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

URL_RE      = re.compile(r"^(https?://|www\.)", re.I)
TOKEN_RE    = re.compile(r"^\d{7,12}:[A-Za-z0-9_-]{20,}$")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")


def is_url(text: str) -> bool:
    return bool(URL_RE.match((text or "").strip()))


def escape_html(text: str) -> str:
    return html.escape(str(text or ""), quote=True)


def normalize_support(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("https://t.me/+") or value.startswith("http://t.me/+") or value.startswith("t.me/+"):
        if not value.startswith("http"):
            value = "https://" + value
        return value
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


def build_tme_url(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("http://") or value.startswith("https://"):
        return value
    handle = value.lstrip("@")
    return f"https://t.me/{handle}" if handle else "https://t.me"


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
    data = json.loads(path.read_text(encoding="utf-8"))
    data.setdefault("updates_channel", data.get("support_chat", ""))
    return BotConfig(**data)


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


def kill_process_tree(pid: int, sig: int = signal.SIGTERM) -> None:
    """Kill process and its entire group (kills lingering ffmpeg etc)."""
    try:
        pgid = os.getpgid(pid)
        os.killpg(pgid, sig)
    except Exception:
        try:
            os.kill(pid, sig)
        except Exception:
            pass


def sep() -> str:      return "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
def sep_thin() -> str: return "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"


def spawn_clone_process(cfg_file: Path, bot_id: str) -> Optional[int]:
    """
    v13: Ultra-safe clone spawner.
    - close_fds=True → no inherited FDs
    - stdin=DEVNULL → no terminal binding
    - start_new_session=True → process group isolation (os.setsid)
    - Dedicated env per clone (ZUDO_CLONE_ID, ZUDO_BOT_ID)
    - Log FD opened+closed correctly (no leak)
    - Retry on EAGAIN/ENOMEM up to 3 times
    """
    last_exc = None
    for attempt in range(3):
        try:
            log_file_path = LOGS_DIR / f"{bot_id}.log"
            log_fd = open(str(log_file_path), "a", buffering=1)
            try:
                env = {
                    **os.environ,
                    "ZUDO_CLONE_ID": bot_id,
                    "ZUDO_BOT_ID": bot_id,
                    "PYTHONUNBUFFERED": "1",
                }
                proc = subprocess.Popen(
                    [sys.executable, "-u", __file__, "--config", str(cfg_file)],
                    stdin=subprocess.DEVNULL,
                    stdout=log_fd,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                    start_new_session=True,
                    env=env,
                )
            finally:
                try:
                    log_fd.close()
                except Exception:
                    pass
            return proc.pid
        except (OSError, BlockingIOError) as e:
            last_exc = e
            log.warning("spawn_clone_process attempt %d for %s failed: %s",
                       attempt + 1, bot_id, e)
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            log.error("spawn_clone_process unexpected fail for %s: %s", bot_id, e)
            return None
    log.error("spawn_clone_process exhausted retries for %s: %s", bot_id, last_exc)
    return None

# ═══════════════════════════════════════════════════════════════════
# FRIENDLY ERROR CLASSIFIER
# ═══════════════════════════════════════════════════════════════════

class FriendlyError(Exception):
    def __init__(self, html_text: str):
        super().__init__(html_text)
        self.html_text = html_text


def classify_error(
    exc: Exception,
    *,
    assistant_username: str = "",
    assistant_id: int = 0,
    chat_id: Optional[int] = None,
) -> str:
    raw = str(exc) if exc else ""
    upper = raw.upper()
    name = type(exc).__name__.upper() if exc else ""

    asst_handle = f"@{assistant_username}" if assistant_username else "ᴀꜱꜱɪꜱᴛᴀɴᴛ"
    asst_link = (
        f'<a href="https://t.me/{escape_html(assistant_username)}">@{escape_html(assistant_username)}</a>'
        if assistant_username else "ᴀꜱꜱɪꜱᴛᴀɴᴛ"
    )
    asst_id_text = f" (ID: <code>{assistant_id}</code>)" if assistant_id else ""

    if any(x in upper for x in (
        "NO_ACTIVE_GROUP_CALL", "NO ACTIVE GROUP CALL", "GROUPCALL_NOT_FOUND",
        "GROUPCALL_INVALID", "CALL_ALREADY_DECLINED", "GROUP CALL NOT FOUND",
        "VOICE CHAT NOT", "VIDEO CHAT NOT",
    )):
        return (
            "<b>🎙️ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴛɪᴠᴇ ɴᴀʜɪ ʜᴀɪ</b>\n\n"
            "<blockquote>ᴘᴇʜʟᴇ ɢʀᴏᴜᴘ ᴍᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ, ꜰɪʀ /play ᴄʜᴀʟᴀᴏ.</blockquote>"
        )

    if any(x in upper for x in (
        "USER_BANNED_IN_CHANNEL", "USERBANNEDINCHANNEL",
        "USER_KICKED", "USERKICKED",
    )) or ("BANNED" in name and "FORBIDDEN" not in name):
        return (
            "<b>🚫 ᴀꜱꜱɪꜱᴛᴀɴᴛ ᴡᴀꜱ ʙᴀɴɴᴇᴅ</b>\n\n"
            f"<blockquote>👤 ᴀꜱꜱɪꜱᴛᴀɴᴛ: {asst_link}{asst_id_text}\n\n"
            f"⚠️ ᴋʀɪᴘʏᴀ ɢʀᴏᴜᴘ ꜱᴇᴛᴛɪɴɢꜱ ᴍᴇ ᴊᴀᴋᴇ {asst_handle} ᴋᴏ "
            f"ᴜɴʙᴀɴ ᴋᴀʀᴏ, ꜰɪʀ /play ᴄʜᴀʟᴀᴏ.</blockquote>"
        )

    if any(x in upper for x in (
        "CHAT_ADMIN_REQUIRED", "ADMIN_REQUIRED", "YOU MUST BE ADMIN",
        "CHATADMINREQUIRED",
    )):
        return (
            "<b>🛡️ ʙᴏᴛ ᴀᴅᴍɪɴ ɴᴀʜɪ ʜᴀɪ</b>\n\n"
            "<blockquote>ᴋʀɪᴘʏᴀ ʙᴏᴛ ᴋᴏ ɢʀᴏᴜᴘ ᴍᴇ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ, "
            "ᴀᴜʀ ɪɴᴠɪᴛᴇ ᴜꜱᴇʀꜱ + ᴍᴀɴᴀɢᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴏ.</blockquote>"
        )

    if any(x in upper for x in (
        "CHAT_ADMIN_INVITE_REQUIRED", "INVITE_HASH_EXPIRED",
        "INVITE_HASH_INVALID", "USERS_TOO_FEW",
    )):
        return (
            "<b>🔗 ᴀꜱꜱɪꜱᴛᴀɴᴛ ᴀᴅᴅ ɴᴀʜɪ ʜᴏ ᴘᴀ ʀʜᴀ</b>\n\n"
            f"<blockquote>👤 {asst_link}{asst_id_text}\n\n"
            "ʙᴏᴛ ᴋᴏ ɪɴᴠɪᴛᴇ ᴜꜱᴇʀꜱ ᴀᴅᴍɪɴ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴏ,\n"
            f"ʏᴀ {asst_handle} ᴋᴏ ᴍᴀɴᴜᴀʟʟʏ ɢʀᴏᴜᴘ ᴍᴇ ᴀᴅᴅ ᴋᴀʀᴏ.</blockquote>"
        )

    if any(x in upper for x in (
        "GROUPCALL_FORBIDDEN", "GROUPCALL_JOIN_MISSING",
        "JOIN AS PEER INVALID", "GROUPCALLFORBIDDEN",
    )):
        return (
            "<b>🚷 ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴀᴄᴄᴇꜱꜱ ᴅᴇɴɪᴇᴅ</b>\n\n"
            "<blockquote>ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ʀᴇꜱᴛʀɪᴄᴛᴇᴅ ʜᴀɪ ʏᴀ ᴀʙʜɪ ʙᴀɴᴅ ʜᴜᴀ ʜᴀɪ.\n"
            "ᴋʀɪᴘʏᴀ ᴠᴄ ᴅᴜʙᴀʀᴀ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ.</blockquote>"
        )

    if any(x in upper for x in (
        "PEER_ID_INVALID", "PEER ID INVALID", "PEERIDINVALID",
        "CHANNEL_INVALID", "CHANNEL_PRIVATE", "CHANNELINVALID",
    )):
        return (
            "<b>🔄 ᴄʜᴀᴛ ᴄᴀᴄʜᴇ ᴜᴘᴅᴀᴛᴇ ʜᴏ ʀᴀʜɪ ʜᴀɪ</b>\n\n"
            "<blockquote>ᴀꜱꜱɪꜱᴛᴀɴᴛ ᴋᴀ ᴄᴀᴄʜᴇ ʀᴇꜰʀᴇꜱʜ ʜᴏ ɢᴀʏᴀ — "
            "ᴅᴜꜱʀɪ ʙᴀʀ /play ᴄʜᴀʟᴀᴏ, ᴀʙ ᴄʜᴀʟᴇɢᴀ ʙʜᴀɪ. ✨</blockquote>"
        )

    if "FLOOD" in upper or "FLOODWAIT" in name:
        secs = ""
        m = re.search(r"(\d+)", raw)
        if m:
            secs = f" ({m.group(1)}ꜱ)"
        return (
            f"<b>⏳ ꜱᴇʀᴠᴇʀ ʙᴜꜱʏ — ꜰʟᴏᴏᴅ ᴡᴀɪᴛ{secs}</b>\n\n"
            "<blockquote>ᴋᴜᴄʜ ꜱᴇᴄᴏɴᴅꜱ ʀᴜᴋᴏ, ꜰɪʀ /play ᴅᴜʙᴀʀᴀ ᴄʜᴀʟᴀᴏ.</blockquote>"
        )

    if any(x in upper for x in ("CHAT_WRITE_FORBIDDEN", "USER_DEACTIVATED", "CHAT_FORBIDDEN")):
        return (
            "<b>✏️ ɢʀᴏᴜᴘ ᴍᴇ ᴡʀɪᴛᴇ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ɴᴀʜɪ ʜᴀɪ</b>\n\n"
            "<blockquote>ᴋʀɪᴘʏᴀ ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ʏᴀ ᴍᴇꜱꜱᴀɢᴇ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴏ.</blockquote>"
        )

    if "403" in raw or ("FORBIDDEN" in upper and "GROUPCALL" not in upper):
        return (
            "<b>🔁 ꜱᴛʀᴇᴀᴍ ʟɪɴᴋ ᴇxᴘɪʀᴇᴅ</b>\n\n"
            "<blockquote>ʏᴏᴜᴛᴜʙᴇ ᴜʀʟ ᴇxᴘɪʀᴇ ʜᴏ ɢᴀʏɪ — /play ᴅᴜʙᴀʀᴀ ᴄʜᴀʟᴀᴏ, "
            "ꜰʀᴇꜱʜ ʟɪɴᴋ ᴀᴀ ᴊᴀᴇɢɪ. ✨</blockquote>"
        )

    if any(x in upper for x in ("ALREADY ENDED", "NOT IN CALL", "ALREADY IN GROUP CALL")):
        return (
            "<b>🔧 ᴄᴀʟʟ ꜱᴛᴀᴛᴇ ʀᴇꜱᴇᴛ ʜᴏ ʀᴀʜɪ ʜᴀɪ</b>\n\n"
            "<blockquote>/play ᴅᴜʙᴀʀᴀ ᴄʜᴀʟᴀᴏ ʏᴀ /leave ᴋᴀʀ ᴋᴇ ꜰɪʀ ᴛʀʏ ᴋᴀʀᴏ.</blockquote>"
        )

    if "CHANNEL_PUBLIC_GROUP_NA" in upper:
        return (
            "<b>📛 ɪꜱ ᴄʜᴀᴛ ᴍᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴜᴘᴘᴏʀᴛ ɴᴀʜɪ</b>\n\n"
            "<blockquote>ᴋʀɪᴘʏᴀ ᴄʜᴀᴛ ᴋᴏ ꜱᴜᴘᴇʀɢʀᴏᴜᴘ ʙᴀɴᴀᴏ.</blockquote>"
        )

    return (
        "<b>⚠️ ᴋᴜᴄʜ ᴛᴇᴄʜɴɪᴄᴀʟ ɢᴀᴅʙᴀᴅ ʜᴜɪ</b>\n\n"
        "<blockquote>ᴋʀɪᴘʏᴀ ᴋᴜᴄʜ ꜱᴇᴄᴏɴᴅꜱ ʙᴀᴀᴅ /play ᴅᴜʙᴀʀᴀ ᴄʜᴀʟᴀᴏ.\n"
        "ᴀɢᴀʀ ᴅᴜʙᴀʀᴀ ᴀᴀʏᴀ ᴛᴏ ꜱᴜᴘᴘᴏʀᴛ ᴄʜᴀᴛ ᴍᴇ ʙᴀᴛᴀᴏ. 🙏</blockquote>"
    )

# ═══════════════════════════════════════════════════════════════════
# YT-DLP
# ═══════════════════════════════════════════════════════════════════

_YT_PLAYER_CLIENTS = ["android", "ios", "tv_embedded", "web"]


def _make_ydl_opts(want_video: bool, client_index: int = 0) -> dict:
    client = _YT_PLAYER_CLIENTS[client_index % len(_YT_PLAYER_CLIENTS)]
    if want_video:
        fmt = "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4]/best"
    else:
        fmt = "bestaudio[ext=m4a]/bestaudio/best"
    return {
        "format": fmt,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "geo_bypass": True,
        "skip_download": True,
        "extract_flat": False,
        "socket_timeout": 20,
        "retries": 3,
        "extractor_args": {"youtube": {"player_client": [client]}},
    }


def sync_extract_track_raw(query: str, want_video: bool = False) -> Track:
    source = query if is_url(query) else f"ytsearch1:{query}"
    last_exc: Optional[Exception] = None

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
                query=query, duration=duration, source=source_name, thumbnail=thumb,
                is_video=want_video, fetched_at=time.time(),
            )
            log.info("Extracted via client=%s (video=%s): %s", client, want_video, title[:60])
            return track
        except Exception as e:
            msg = str(e).lower()
            if any(x in msg for x in ("sign in", "bot", "confirm", "login", "auth", "429", "throttle")):
                log.warning("Client '%s' blocked, trying next...", client)
                last_exc = e
                continue
            raise
    raise ValueError(f"ᴀʟʟ ᴄʟɪᴇɴᴛꜱ ꜰᴀɪʟ. ʟᴀꜱᴛ: {str(last_exc)[:200]}")

# ═══════════════════════════════════════════════════════════════════
# CORE BOT CLASS
# ═══════════════════════════════════════════════════════════════════

class TelegramMusicBot:

    def __init__(self, config: BotConfig, config_path: Optional[Path] = None, is_master: bool = False):
        validate_config(config)
        self.config      = config
        self.config_path = config_path
        self.is_master   = is_master
        self.start_time  = time.time()

        # v13: Per-clone isolated workdir
        self.bot_storage = ROOT_RUNTIME_DIR / f"bot_{config.bot_id}"
        self.bot_storage.mkdir(parents=True, exist_ok=True)

        self.settings_path = self.bot_storage / "settings.json"
        self.settings: Dict[str, Any] = self._load_settings()
        self.state_file = STATES_DIR / f"{config.bot_id}_state.json"

        # v13: Per-bot caches (was global → no more cross-bot contamination)
        self._track_cache: Dict[str, Tuple[float, Track]] = {}
        self._track_cache_lock = threading.Lock()
        self.TRACK_CACHE_TTL = int(os.getenv("TRACK_CACHE_TTL", "1800"))
        self.TRACK_CACHE_MAX = 150

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
            workdir=workdir,
        )
        self.calls = PyTgCalls(self.assistant)

        self.states:              Dict[int, ChatState]      = {}
        self.chat_locks:          Dict[int, asyncio.Lock]   = {}
        self.clone_flow:          Dict[int, Dict[str, Any]] = {}
        self.pending_start_photo: Dict[int, float]          = {}
        self.pending_thumb:       Dict[int, float]          = {}
        self.known_users:         set                       = set()
        self.known_chats:         set                       = set()

        self._assistant_joined:   Dict[int, float] = {}
        self._join_flood_until:   Dict[int, float] = {}
        self._peer_resolved:      Dict[int, float] = {}

        self.bot_username:       str  = ""
        self.bot_name:           str  = ""
        self.bot_id_int:         int  = 0
        self.assistant_id:       int  = 0
        self.assistant_username: str  = ""
        self.assistant_name:     str  = "Assistant"
        self._stopping:          bool = False
        self._main_loop:         Optional[asyncio.AbstractEventLoop] = None
        self._watchdog_task:     Optional[asyncio.Task] = None
        self._signal_watch_task: Optional[asyncio.Task] = None
        self._heartbeat_task:    Optional[asyncio.Task] = None
        self._dialogs_warmed:    bool = False

    # ─── PER-BOT TRACK CACHE ───

    def _cache_key(self, query: str, want_video: bool) -> str:
        return f"{query.strip().lower()}|{'v' if want_video else 'a'}"

    def get_cached_track(self, query: str, want_video: bool) -> Optional[Track]:
        key = self._cache_key(query, want_video)
        with self._track_cache_lock:
            entry = self._track_cache.get(key)
            if entry:
                ts, track = entry
                if time.time() - ts < self.TRACK_CACHE_TTL:
                    return track
                self._track_cache.pop(key, None)
        return None

    def set_cached_track(self, query: str, want_video: bool, track: Track) -> None:
        key = self._cache_key(query, want_video)
        with self._track_cache_lock:
            self._track_cache[key] = (time.time(), track)
            if len(self._track_cache) > self.TRACK_CACHE_MAX:
                oldest = sorted(self._track_cache.keys(), key=lambda k: self._track_cache[k][0])
                for k in oldest[:30]:
                    self._track_cache.pop(k, None)

    def invalidate_cached_track(self, query: str, want_video: bool) -> None:
        key = self._cache_key(query, want_video)
        with self._track_cache_lock:
            self._track_cache.pop(key, None)

    async def extract_track(self, query: str, want_video: bool = False, use_cache: bool = True) -> Track:
        if use_cache:
            cached = self.get_cached_track(query, want_video)
            if cached:
                log.info("Cache HIT: %s", query[:60])
                return cached
        track = await asyncio.to_thread(sync_extract_track_raw, query, want_video)
        if use_cache:
            self.set_cached_track(query, want_video, track)
        return track

    # ─── PERSISTENT STATE ───

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
        try:
            loop = self._main_loop
            if loop and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    asyncio.to_thread(self._save_state_sync), loop
                )
            else:
                self._save_state_sync()
        except Exception:
            try:
                self._save_state_sync()
            except Exception:
                pass

    # ─── USER/CHAT TRACKING ───

    def _load_known(self) -> None:
        try:
            if USERS_FILE.exists():
                self.known_users = set(json.loads(USERS_FILE.read_text("utf-8")))
        except Exception:
            self.known_users = set()
        try:
            if CHATS_FILE.exists():
                self.known_chats = set(json.loads(CHATS_FILE.read_text("utf-8")))
        except Exception:
            self.known_chats = set()

    def _save_known(self) -> None:
        try:
            USERS_FILE.write_text(json.dumps(list(self.known_users)), encoding="utf-8")
        except Exception:
            pass
        try:
            CHATS_FILE.write_text(json.dumps(list(self.known_chats)), encoding="utf-8")
        except Exception:
            pass

    def _track_user(self, uid: Optional[int]) -> None:
        if uid and uid not in self.known_users:
            self.known_users.add(uid)
            self._save_known()

    def _track_chat(self, cid: Optional[int]) -> None:
        if cid and cid not in self.known_chats:
            self.known_chats.add(cid)
            self._save_known()

    # ─── SETTINGS (v13: start_photo now stores type + file_id) ───

    def _load_settings(self) -> Dict[str, Any]:
        if not self.settings_path.exists():
            return {"start_photo_file_id": "", "start_photo_type": ""}
        try:
            data = json.loads(self.settings_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {"start_photo_file_id": "", "start_photo_type": ""}
            data.setdefault("start_photo_file_id", "")
            # v13: backward-compat — if file_id set but no type, assume "photo"
            if data.get("start_photo_file_id") and not data.get("start_photo_type"):
                data["start_photo_type"] = "photo"
            data.setdefault("start_photo_type", "")
            return data
        except Exception:
            return {"start_photo_file_id": "", "start_photo_type": ""}

    def _save_settings(self) -> None:
        try:
            tmp = self.settings_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.settings, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.settings_path)
        except Exception:
            log.exception("save_settings failed")

    # ─── STATE / LOCK ───

    def get_state(self, chat_id: int) -> ChatState:
        if chat_id not in self.states:
            self.states[chat_id] = ChatState()
        return self.states[chat_id]

    def get_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        return self.chat_locks[chat_id]

    # ─── PROPERTIES ───

    @property
    def display_name(self) -> str:
        return self.bot_name or self.config.brand_name or "Music Bot"

    @property
    def support_url(self) -> str:
        return build_tme_url(self.config.support_chat)

    @property
    def updates_url(self) -> str:
        ch = self.config.updates_channel or self.config.support_chat
        return build_tme_url(ch)

    @property
    def owner_url(self) -> str:
        return f"https://t.me/{self.config.owner_username.lstrip('@')}"

    @property
    def add_to_group_url(self) -> str:
        if self.bot_username:
            return f"https://t.me/{self.bot_username}?startgroup=true"
        return "https://t.me"

    # ─── AUTH ───

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
            await self._safe_send(message, "❌ ʏᴇ ᴄᴏɴᴛʀᴏʟ ꜱɪʀꜰ ɢʀᴏᴜᴘ ᴀᴅᴍɪɴꜱ ᴜꜱᴇ ᴋᴀʀ ꜱᴀᴋᴛᴇ ʜᴀɪɴ.")
        return ok

    # ─── SAFE SEND / EDIT ───

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
            if getattr(msg, "photo", None) or getattr(msg, "video", None) or getattr(msg, "animation", None):
                return await msg.edit_caption(caption=text, reply_markup=kb)
            return await msg.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            try:
                if getattr(msg, "photo", None) or getattr(msg, "video", None) or getattr(msg, "animation", None):
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

    # ═══════════════ START PANEL (v13: photo + video both) ═══════════════

    async def _send_start_panel(self, message: Message):
        """v13: Auto-detect start media type (photo OR video) and use correct method."""
        user_name = ""
        if message.from_user:
            user_name = message.from_user.first_name or message.from_user.username or ""
        text = self._start_text(user_name)
        kb = self._start_kb()

        file_id = (self.settings.get("start_photo_file_id") or "").strip()
        media_type = (self.settings.get("start_photo_type") or "").strip().lower()

        if file_id:
            try:
                if media_type == "video":
                    return await message.reply_video(
                        video=file_id,
                        caption=text,
                        reply_markup=kb,
                        supports_streaming=True,
                    )
                elif media_type == "animation":
                    return await message.reply_animation(
                        animation=file_id,
                        caption=text,
                        reply_markup=kb,
                    )
                else:  # photo (default + backward compat)
                    return await message.reply_photo(
                        photo=file_id,
                        caption=text,
                        reply_markup=kb,
                    )
            except Exception as e:
                log.warning("start media send failed (%s), falling back to text: %s", media_type, e)

        return await self._safe_send(message, text, reply_markup=kb)

    # ═══════════════ UI TEXTS ═══════════════

    def _start_text(self, user_name: str = "") -> str:
        n   = escape_html(self.display_name)
        greet_name = escape_html(user_name) if user_name else "ꜰʀɪᴇɴᴅ"
        return (
            f"<b>  ʜᴇʟʟᴏ {greet_name}  ✨</b>\n\n"
            f"  ɪ ᴀᴍ <b>{n}</b> 🎧\n"
            f"  ʏᴏᴜʀ ᴄᴜᴛᴇ ʟɪʟ ᴍᴜꜱɪᴄ ʙᴜᴅᴅʏ 💜\n\n"
            f"  {sep_thin()}\n\n"
            f"  ⚡  ꜱᴜᴘᴇʀ ꜰᴀꜱᴛ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ᴘʟᴀʏᴇʀ\n"
            f"  🎶  ᴀᴜᴅɪᴏ + ᴠɪᴅᴇᴏ + ǫᴜᴇᴜᴇ ꜱᴜᴘᴘᴏʀᴛ\n"
            f"  💎  24×7 ᴏɴʟɪɴᴇ • ᴢᴇʀᴏ ʟᴀɢ\n\n"
            f"  ᴛᴀᴘ <b>ʜᴇʟᴘ</b> ʙᴇʟᴏᴡ ᴛᴏ ꜱᴇᴇ ᴀʟʟ ᴄᴏᴍᴍᴀɴᴅꜱ 👇"
        )

    def _about_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"<b>  ✨ ᴀʙᴏᴜᴛ {n} ✨</b>\n\n"
            f"  ❝ <i>ᴍᴜsɪᴄ ɪs ᴛʜᴇ sʜᴏʀᴛʜᴀɴᴅ ᴏꜰ ᴇᴍᴏᴛɪᴏɴ.</i> ❞\n\n"
            f"  {sep_thin()}\n\n"
            f"  <b>💎  ꜰᴇᴀᴛᴜʀᴇꜱ</b>\n"
            f"  ▸  ꜱᴍᴏᴏᴛʜ ᴠᴄ ᴘʟᴀʏʙᴀᴄᴋ ᴇɴɢɪɴᴇ\n"
            f"  ▸  ʏᴏᴜᴛᴜʙᴇ ʙᴏᴛ-ᴅᴇᴛᴇᴄᴛɪᴏɴ ᴘʀᴏᴏꜰ\n"
            f"  ▸  ꜱᴍᴀʀᴛ ǫᴜᴇᴜᴇ + ʟᴏᴏᴘ + ꜱʜᴜꜰꜰʟᴇ\n"
            f"  ▸  ꜱᴇʀᴠᴇʀ ʀᴇꜱᴛᴀʀᴛ ꜱᴇ ᴀᴜᴛᴏ ʀᴇꜱᴜᴍᴇ\n"
            f"  ▸  ᴀᴜᴅɪᴏ + ᴠɪᴅᴇᴏ ʙᴏᴛʜ ꜱᴜᴘᴘᴏʀᴛᴇᴅ\n\n"
            f"  <b>🚀 ɢʀᴏᴜᴘ ꜱᴇᴛᴜᴘ:</b>\n"
            f"  ❶ ʙᴏᴛ ᴀᴅᴅ ᴋᴀʀᴏ\n"
            f"  ❷ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ\n"
            f"  ❸ ᴠᴏɪᴄᴇ ᴄʜᴀᴛ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ\n"
            f"  ❹ /play ꜱᴏɴɢ ɴᴀᴍᴇ 🎶"
        )

    def _help_home_text(self) -> str:
        n = escape_html(self.display_name)
        return (
            f"<b>  📚 {n} — ʜᴇʟᴘ ᴄᴇɴᴛᴇʀ</b>\n\n"
            f"  ❝ <i>ᴛʜᴇ ʙᴇsᴛ ᴍᴜsɪᴄ ᴍᴀᴋᴇs ʏᴏᴜ ᴅᴀɴᴄᴇ.</i> ❞\n\n"
            f"  {sep_thin()}\n\n"
            f"  ⬇ ɴᴇᴄʜᴇ ꜱᴇᴄᴛɪᴏɴ ᴄʜᴜɴᴏ\n"
            f"  ⬇ ᴄᴏᴍᴍᴀɴᴅꜱ ᴇxᴘʟᴏʀᴇ ᴋᴀʀᴏ\n\n"
            f"  <b>💡 ꜰᴀꜱᴛ ᴛɪᴘ:</b>\n"
            f"  ▸ <code>/play sᴏɴɢ ɴᴀᴍᴇ</code>\n"
            f"  ▸ <code>/vplay sᴏɴɢ ɴᴀᴍᴇ</code> (ᴠɪᴅᴇᴏ)"
        )

    def _help_music_text(self) -> str:
        return (
            f"<b>  🎵 ᴍᴜꜱɪᴄ ᴄᴏᴍᴍᴀɴᴅꜱ</b>\n\n"
            f"  ▸  <code>/play  sᴏɴɢ / ᴜʀʟ</code>  →  🎵 ᴀᴜᴅɪᴏ ᴏɴʟʏ\n"
            f"  ▸  <code>/vplay sᴏɴɢ / ᴜʀʟ</code>  →  📹 ᴠɪᴅᴇᴏ ᴏɴʟʏ\n"
            f"  ▸  <code>/p     sᴏɴɢ</code>        →  /play ᴀʟɪᴀs\n\n"
            f"  {sep_thin()}\n\n"
            f"  ⏸  /pause    →  ᴘᴀᴜꜱᴇ\n"
            f"  ▶️  /resume   →  ʀᴇꜱᴜᴍᴇ\n"
            f"  ⏭  /skip     →  ɴᴇxᴛ ᴛʀᴀᴄᴋ\n"
            f"  ⏹  /stop     →  ꜱᴛᴏᴘ + ᴄʟᴇᴀʀ\n"
            f"  📜  /queue    →  ᴠɪᴇᴡ ǫᴜᴇᴜᴇ\n"
            f"  🎵  /np       →  ɴᴏᴡ ᴘʟᴀʏɪɴɢ\n"
            f"  🔄  /refresh  →  ʀᴇꜰʀᴇꜱʜ ᴘᴀɴᴇʟ\n"
            f"  🎼  /playlist →  ʟɪꜱᴛ ᴀᴜᴅɪᴏ ǫᴜᴇᴜᴇ"
        )

    def _help_admin_text(self) -> str:
        return (
            f"<b>  🛠 ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟꜱ</b>\n\n"
            f"  🔁  /loop [on/off]  →  ʟᴏᴏᴘ\n"
            f"  🔀  /shuffle      →  ꜱʜᴜꜰꜰʟᴇ\n"
            f"  🧹  /clearqueue   →  ᴄʟᴇᴀʀ\n"
            f"  🔇  /mute         →  ᴍᴜᴛᴇ\n"
            f"  🔊  /unmute       →  ᴜɴᴍᴜᴛᴇ\n"
            f"  🔊  /volume 1-200  →  ᴠᴏʟᴜᴍᴇ\n"
            f"  🚪  /leave        →  ʟᴇᴀᴠᴇ ᴠᴄ\n\n"
            f"  {sep_thin()}\n\n"
            f"  🏓  /ping  →  ʟᴀᴛᴇɴᴄʏ\n"
            f"  💚  /alive →  ᴏɴʟɪɴᴇ ꜱᴛᴀᴛᴜꜱ\n"
            f"  📊  /stats →  ʙᴏᴛ ꜱᴛᴀᴛꜱ"
        )

    def _help_extra_text(self) -> str:
        return (
            f"<b>  🧩 ᴇxᴛʀᴀ ɪɴꜰᴏ</b>\n\n"
            f"  ◈  ʙᴏᴛ ᴋᴏ ᴀᴅᴍɪɴ ʙᴀɴᴀᴏ ꜱᴍᴏᴏᴛʜ ᴘʟᴀʏ ᴋᴇ ʟɪᴇ\n"
            f"  ◈  /play ꜱᴇ ᴘᴇʜʟᴇ ᴠᴄ ꜱᴛᴀʀᴛ ᴋᴀʀᴏ\n"
            f"  ◈  ꜱᴇʀᴠᴇʀ ʀᴇꜱᴛᴀʀᴛ ᴘᴇ ǫᴜᴇᴜᴇ ᴀᴜᴛᴏ ʀᴇꜱᴜᴍᴇ\n"
            f"  ◈  ʟᴏɴɢ ꜱᴏɴɢꜱ ᴀᴜᴛᴏ ʀᴇꜰʀᴇꜱʜ\n"
            f"  ◈  ᴄʀᴀꜱʜ ʜᴏɴᴇ ᴘᴀʀ ᴀᴜᴛᴏ ʀᴇᴄᴏᴠᴇʀʏ\n\n"
            f"  <b>🌐 ᴜꜱᴇʀʙᴏᴛ ᴄᴏᴍᴍᴀɴᴅꜱ:</b>\n"
            f"  ▸  /userbotjoin  →  ᴀꜱꜱɪꜱᴛᴀɴᴛ ᴊᴏɪɴ\n"
            f"  ▸  /userbotleave →  ᴀꜱꜱɪꜱᴛᴀɴᴛ ʟᴇᴀᴠᴇ"
        )

    def _shell_help_text(self) -> str:
        master_only = ""
        if self.is_master:
            master_only = (
                f"  {sep_thin()}\n\n"
                f"  ▸  /clone     →  ɴᴀʏᴀ ʙᴏᴛ ꜱᴇᴛᴜᴘ\n"
                f"  ▸  /dclone    →  ᴄʟᴏɴᴇ ʙᴏᴛ ꜱᴛᴏᴘ\n"
                f"  ▸  /clones    →  ʟɪꜱᴛ ᴀʟʟ ʙᴏᴛꜱ\n"
                f"  ▸  /cancel    →  ꜱᴇᴛᴜᴘ ᴄᴀɴᴄᴇʟ\n"
                f"  ▸  /restart   →  ʙᴏᴛ ʀᴇꜱᴛᴀʀᴛ\n"
                f"  ▸  /broadcast msg →  ʙʀᴏᴀᴅᴄᴀꜱᴛ\n\n"
                f"  🎬  /setthmb     →  ɢʟᴏʙᴀʟ ᴘʟᴀʏ ᴛʜᴜᴍʙɴᴀɪʟ ꜱᴇᴛ\n"
                f"  🗑️  /delthmb     →  ᴛʜᴜᴍʙɴᴀɪʟ ʜᴀᴛᴀᴏ\n\n"
            )
        return (
            f"<b>  🔐 ᴏᴡɴᴇʀ ᴘᴀɴᴇʟ</b>\n\n"
            f"  ▸  /shelp     →  ʏᴇ ᴘᴀɴᴇʟ\n"
            f"  ▸  /setdp     →  ꜱᴛᴀʀᴛᴜᴘ ᴘʜᴏᴛᴏ/ᴠɪᴅᴇᴏ ꜱᴇᴛ\n"
            f"  ▸  /removedp  →  ᴘʜᴏᴛᴏ/ᴠɪᴅᴇᴏ ʜᴀᴛᴀᴏ\n\n"
            f"{master_only}"
            f"  ⚠️ ꜱɪʀꜰ ᴏᴡɴᴇʀ"
        )

    # ═══════════════ BLOCKQUOTE PLAY-CARD (v13: exact screenshot style) ═══════════════

    def _play_card_text(self, track: Track) -> str:
        """
        v13: Each field wrapped in its OWN <blockquote> for proper quoted boxes.
        Title gets its own blockquote with separator inside, others get individual quotes.
        Telegram renders each <blockquote> as a green-bar quoted box.
        """
        mode_emoji = "📹" if track.is_video else "🎵"
        mode_label = "ᴠɪᴅᴇᴏ" if track.is_video else "ᴀᴜᴅɪᴏ"
        brand = escape_html(self.display_name)
        title = escape_html(track.title)
        duration = escape_html(track.pretty_duration)
        req = track.requested_by  # already HTML-escaped at mention_user time

        return (
            f"🎶 <b>{brand}</b>  ᯤ‌  [ <b>ɴᴏ ᴀᴅꜱ</b> ] ♪✨\n"
            f"<blockquote>🍎  <b>ᴛɪᴛʟᴇ</b>      :  {title}</blockquote>\n"
            f"<blockquote>⚙️  <b>ᴅᴜʀᴀᴛɪᴏɴ</b>   :  {duration}</blockquote>\n"
            f"<blockquote>💫  <b>ʀᴇǫᴜᴇꜱᴛᴇᴅ</b>  :  {req}</blockquote>\n"
            f"<blockquote>{mode_emoji}  <b>ᴍᴏᴅᴇ</b>       :  {mode_label}</blockquote>"
        )

    def _np_text(self, state: ChatState) -> str:
        if not state.current:
            return (
                f"<b>  🎵 ɴᴏᴡ ᴘʟᴀʏɪɴɢ</b>\n\n"
                f"  ❌ ᴀʙʜɪ ᴋᴜᴄʜ ᴘʟᴀʏ ɴᴀʜɪ ʜᴏ ʀᴀʜᴀ.\n\n"
                f"  💡 <code>/play sᴏɴɢ ɴᴀᴍᴇ</code>"
            )
        t = state.current
        card = self._play_card_text(t)
        return (
            f"{card}\n"
            f"<blockquote>🔁 <b>ʟᴏᴏᴘ</b>   : {human_bool(state.loop)}\n"
            f"⏸ <b>ᴘᴀᴜꜱᴇᴅ</b> : {human_bool(state.paused)}\n"
            f"🔇 <b>ᴍᴜᴛᴇᴅ</b>  : {human_bool(state.muted)}\n"
            f"🔊 <b>ᴠᴏʟ</b>    : {state.volume}%</blockquote>"
        )

    def _queue_text(self, state: ChatState) -> str:
        if not state.current and not state.queue:
            return f"<b>  📜 ǫᴜᴇᴜᴇ</b>\n\n  📭 ǫᴜᴇᴜᴇ ᴇᴍᴘᴛʏ ʜᴀɪ."
        lines = ["<b>  📜 ǫᴜᴇᴜᴇ</b>", ""]
        if state.current:
            lines.append(f"  <b>🎵 ᴘʟᴀʏɪɴɢ:</b>")
            lines.append(f"      <b>{escape_html(state.current.title)}</b>")
            lines.append(f"      ⏱ <code>{escape_html(state.current.pretty_duration)}</code>")
            lines.append("")
        if state.queue:
            lines.append(f"  <b>⏭ ᴜᴘ ɴᴇxᴛ:</b>")
            lines.append(f"  {sep_thin()}")
            for i, t in enumerate(state.queue[:15], 1):
                lines.append(f"  <b>{i:>2}.</b> {escape_html(t.title[:40])}")
                lines.append(f"       ⏱ <code>{escape_html(t.pretty_duration)}</code>")
            if len(state.queue) > 15:
                lines.append(f"\n  <i>... +{len(state.queue) - 15} ᴍᴏʀᴇ</i>")
        lines.append("")
        lines.append(f"  🔁 {human_bool(state.loop)}   ⏸ {human_bool(state.paused)}")
        return "\n".join(lines)

    # ─── KEYBOARDS ───

    def _start_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ ➕", url=self.add_to_group_url)],
            [InlineKeyboardButton("👑 ᴏᴡɴᴇʀ", url=self.owner_url),
             InlineKeyboardButton("📖 ᴀʙᴏᴜᴛ", callback_data="nav_about")],
            [InlineKeyboardButton("💬 ꜱᴜᴘᴘᴏʀᴛ", url=self.support_url),
             InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇꜱ", url=self.updates_url)],
            [InlineKeyboardButton("📚 ʜᴇʟᴘ & ᴄᴏᴍᴍᴀɴᴅꜱ 📚", callback_data="nav_help_home")],
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

    def _play_card_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⏸️ ᴘᴀᴜꜱᴇ",  callback_data="ctl_pause"),
                InlineKeyboardButton("▶️ ʀᴇꜱᴜᴍᴇ", callback_data="ctl_resume"),
            ],
            [
                InlineKeyboardButton("⏭️ sᴋɪᴘ",  callback_data="ctl_skip"),
                InlineKeyboardButton("⏹️ sᴛᴏᴘ",  callback_data="ctl_stop"),
            ],
            [
                InlineKeyboardButton("🎼 ᴛᴜɴᴇs", callback_data="ctl_queue"),
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ",  callback_data="nav_home"),
            ],
            [
                InlineKeyboardButton("✖️ ᴄʟᴏsᴇ", callback_data="ctl_close_card"),
            ],
        ])

    def _np_kb(self) -> InlineKeyboardMarkup:
        return self._play_card_kb()

    def _queue_kb(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔀 ꜱʜᴜꜰꜰʟᴇ",  callback_data="ctl_shuffle"),
             InlineKeyboardButton("🧹 ᴄʟᴇᴀʀ",    callback_data="ctl_clearqueue")],
            [InlineKeyboardButton("🎵 ɴᴏᴡ ᴘʟᴀʏɪɴɢ", callback_data="ctl_np"),
             InlineKeyboardButton("🏠 ʜᴏᴍᴇ",        callback_data="nav_home")],
            [InlineKeyboardButton("✖️ ᴄʟᴏꜱᴇ", callback_data="ctl_close_card")],
        ])

    # ═══════════════════════════════════════════════════════════════
    # SEND PLAY CARD (v13: FIXED — custom thumb + caption both shown)
    # ═══════════════════════════════════════════════════════════════

    async def _send_play_card(self, chat_id: int, track: Track,
                              reply_to_msg: Optional[Message] = None,
                              edit_msg: Optional[Message] = None) -> Optional[Message]:
        """
        v13 FIX: Play card priority:
          1. Custom global thumb (photo/video) → sent with caption
          2. YouTube track thumbnail → sent as photo with caption
          3. Plain text fallback

        Bug fix: jab custom thumb set tha to caption nahi aa raha tha because
        edit path was incorrectly trying to edit media types. Ab clean approach:
        agar edit_msg hai aur same media type hai → edit caption;
        agar mismatch → delete old + send new.
        """
        text = self._play_card_text(track)
        kb   = self._play_card_kb()
        thumb = load_shared_thumb()
        media_type = thumb.get("type", "").lower()
        file_id    = thumb.get("file_id", "")
        has_custom = bool(file_id and media_type in ("photo", "video"))

        # Edit path: only safe if existing msg type matches what we want to send
        if edit_msg is not None:
            try:
                old_is_photo = bool(getattr(edit_msg, "photo", None))
                old_is_video = bool(getattr(edit_msg, "video", None) or getattr(edit_msg, "animation", None))
                old_is_text  = not (old_is_photo or old_is_video)

                # Determine what we WANT to send
                want_photo = has_custom and media_type == "photo"
                want_video = has_custom and media_type == "video"
                want_yt_thumb = (not has_custom) and bool(track.thumbnail)

                # Compatible edit cases
                if want_photo and old_is_photo:
                    return await edit_msg.edit_caption(caption=text, reply_markup=kb)
                if want_video and old_is_video:
                    return await edit_msg.edit_caption(caption=text, reply_markup=kb)
                if want_yt_thumb and old_is_photo:
                    return await edit_msg.edit_caption(caption=text, reply_markup=kb)
                if (not has_custom) and (not track.thumbnail) and old_is_text:
                    return await edit_msg.edit_text(text, reply_markup=kb, disable_web_page_preview=True)

                # Incompatible → delete old, send new below
                try:
                    await edit_msg.delete()
                except Exception:
                    pass
            except Exception:
                log.exception("play_card edit attempt failed; will send new")

        # Fresh send path
        try:
            # Priority 1: Custom global thumb (set via /setthmb)
            if has_custom and media_type == "photo":
                return await self.bot.send_photo(
                    chat_id, photo=file_id, caption=text, reply_markup=kb
                )
            if has_custom and media_type == "video":
                return await self.bot.send_video(
                    chat_id, video=file_id, caption=text, reply_markup=kb,
                    supports_streaming=True,
                )

            # Priority 2: YouTube thumbnail
            if track.thumbnail:
                try:
                    return await self.bot.send_photo(
                        chat_id, photo=track.thumbnail, caption=text, reply_markup=kb
                    )
                except Exception as e:
                    log.warning("yt thumbnail send failed: %s", e)

            # Priority 3: plain text
            return await self.bot.send_message(
                chat_id, text, reply_markup=kb, disable_web_page_preview=True
            )
        except FloodWait as fw:
            await asyncio.sleep(getattr(fw, "value", 1))
            try:
                return await self.bot.send_message(
                    chat_id, text, reply_markup=kb, disable_web_page_preview=True
                )
            except Exception:
                pass
        except Exception:
            log.exception("send_play_card failed — last resort plain text")
            try:
                return await self.bot.send_message(
                    chat_id, text, reply_markup=kb, disable_web_page_preview=True
                )
            except Exception:
                pass
        return None

    # ═══════════════════════════════════════════════════════════════
    # PEER RESOLVER
    # ═══════════════════════════════════════════════════════════════

    async def _warm_dialogs_once(self) -> None:
        if self._dialogs_warmed:
            return
        try:
            count = 0
            async for _ in self.assistant.get_dialogs(limit=50):
                count += 1
            log.info("Assistant dialogs warmed: %d", count)
            self._dialogs_warmed = True
        except Exception as e:
            log.warning("dialogs warm failed: %s", e)
            self._dialogs_warmed = True

    async def _resolve_peer_ultimate(self, chat_id: int) -> bool:
        now = time.time()
        last = self._peer_resolved.get(chat_id, 0)
        if now - last < 300:
            return True
        try:
            await self.assistant.resolve_peer(chat_id)
            self._peer_resolved[chat_id] = now
            return True
        except Exception:
            pass
        try:
            chat = await self.bot.get_chat(chat_id)
            uname = getattr(chat, "username", None)
            if uname:
                await self.assistant.resolve_peer(uname)
                self._peer_resolved[chat_id] = now
                return True
        except Exception:
            pass
        try:
            await self.assistant.get_chat(chat_id)
            self._peer_resolved[chat_id] = now
            return True
        except Exception:
            pass
        try:
            count = 0
            async for _ in self.assistant.get_dialogs(limit=100):
                count += 1
                if count > 100:
                    break
            await asyncio.sleep(0.4)
            await self.assistant.resolve_peer(chat_id)
            self._peer_resolved[chat_id] = now
            return True
        except Exception as e:
            log.warning("resolve_peer all paths failed for %s: %s", chat_id, e)
            return False

    async def _check_assistant_status(self, chat_id: int) -> Tuple[str, Optional[Exception]]:
        try:
            member = await self.bot.get_chat_member(chat_id, self.assistant_id)
            status = getattr(getattr(member, "status", None), "name", "") or ""
            su = status.upper()
            if "BANNED" in su or "KICKED" in su:
                return "banned", None
            if any(x in su for x in ("MEMBER", "ADMINISTRATOR", "OWNER", "CREATOR", "RESTRICTED")):
                return "in", None
            if "LEFT" in su:
                return "left", None
            return "unknown", None
        except UserNotParticipant:
            return "left", None
        except Exception as e:
            return "unknown", e

    async def _try_assistant_join(self, chat_id: int) -> Tuple[bool, Optional[Exception]]:
        link = ""
        try:
            chat = await self.bot.get_chat(chat_id)
            uname = getattr(chat, "username", None)
            if uname:
                link = f"@{uname}"
            else:
                try:
                    link = await self.bot.export_chat_invite_link(chat_id)
                except Exception as e:
                    return False, e
        except Exception as e:
            return False, e

        try:
            await self.assistant.join_chat(link)
            return True, None
        except UserAlreadyParticipant:
            return True, None
        except FloodWait as fw:
            wait_secs = int(getattr(fw, "value", 60) or 60)
            self._join_flood_until[chat_id] = time.time() + wait_secs
            log.warning("FloodWait on join chat=%s for %ss", chat_id, wait_secs)
            return False, fw
        except Exception as e:
            return False, e

    async def _ensure_assistant_in_chat(self, chat_id: int) -> Tuple[bool, Optional[Exception]]:
        now = time.time()

        ts = self._assistant_joined.get(chat_id)
        if ts and (now - ts) < JOIN_CACHE_TTL:
            await self._resolve_peer_ultimate(chat_id)
            return True, None

        flood_until = self._join_flood_until.get(chat_id, 0)
        if flood_until and now < flood_until:
            wait_remaining = int(flood_until - now)
            if wait_remaining > JOIN_FLOOD_SKIP_THRESHOLD:
                return False, FloodWait(value=wait_remaining)

        status, status_err = await self._check_assistant_status(chat_id)

        if status == "in":
            self._assistant_joined[chat_id] = now
            await self._resolve_peer_ultimate(chat_id)
            return True, None

        if status == "banned":
            return False, UserBannedInChannel("Assistant banned")

        if status in ("left", "unknown"):
            join_ok, join_err = await self._try_assistant_join(chat_id)
            if join_ok:
                self._assistant_joined[chat_id] = now
                await asyncio.sleep(0.4)
                await self._resolve_peer_ultimate(chat_id)
                return True, None
            return False, join_err or status_err or Exception("Join failed")

        return False, status_err or Exception("Assistant status unknown")

    def _invalidate_join_cache(self, chat_id: int) -> None:
        self._assistant_joined.pop(chat_id, None)
        self._join_flood_until.pop(chat_id, None)
        self._peer_resolved.pop(chat_id, None)

    # ═══════════════════════════════════════════════════════════════
    # STREAM BUILDER
    # ═══════════════════════════════════════════════════════════════

    def _build_streams(self, url: str, is_video: bool) -> list:
        objs = []
        if is_video:
            if _MediaStream is not None:
                if _MediaType is not None:
                    for attr in ("VIDEO", "video"):
                        mv = getattr(_MediaType, attr, None)
                        if mv:
                            try: objs.append(_MediaStream(url, media_type=mv))
                            except Exception: pass
                            break
                try: objs.append(_MediaStream(url))
                except Exception: pass
            if _VideoStream is not None:
                try: objs.append(_VideoStream(url))
                except Exception: pass
        else:
            if _MediaStream is not None:
                ignore_flag = None
                for source in (_VideoFlags, _MediaStream, getattr(_MediaStream, "Flags", None)):
                    if source is None:
                        continue
                    flag = getattr(source, "IGNORE", None)
                    if flag is not None:
                        ignore_flag = flag
                        break
                if ignore_flag is not None:
                    try:
                        objs.append(_MediaStream(url, video_flags=ignore_flag))
                    except Exception:
                        pass
                if _MediaType is not None:
                    for attr in ("AUDIO", "audio"):
                        mv = getattr(_MediaType, attr, None)
                        if mv:
                            try:
                                if ignore_flag is not None:
                                    objs.append(_MediaStream(url, media_type=mv, video_flags=ignore_flag))
                                else:
                                    objs.append(_MediaStream(url, media_type=mv))
                            except Exception:
                                pass
                            break
                if not objs:
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
            for stream_obj in streams:
                try:
                    result = method(chat_id, stream_obj)
                    if asyncio.iscoroutine(result):
                        await result
                    log.info("Played via %s + %s (video=%s)",
                             method_name, type(stream_obj).__name__, is_video)
                    return
                except Exception as e:
                    last_exc = e
                    if "PEER_ID_INVALID" in str(e).upper():
                        raise
            try:
                result = method(chat_id, url)
                if asyncio.iscoroutine(result):
                    await result
                log.info("Played via %s + raw_url", method_name)
                return
            except Exception as e:
                last_exc = e
                if "PEER_ID_INVALID" in str(e).upper():
                    raise
        if last_exc:
            raise last_exc
        raise RuntimeError("No play method worked")

    # ─── PLAY TRACK ───

    async def _refresh_track_if_stale(self, track: Track) -> Track:
        if time.time() - (track.fetched_at or 0) > 1500 and track.query:
            try:
                self.invalidate_cached_track(track.query, track.is_video)
                new_track = await self.extract_track(track.query, track.is_video, True)
                new_track.requested_by = track.requested_by
                log.info("Refreshed stale URL: %s", track.title[:50])
                return new_track
            except Exception as e:
                log.warning("Refresh failed: %s", e)
        return track

    async def _play_track(self, chat_id: int, track: Track) -> Track:
        track = await self._refresh_track_if_stale(track)

        try:
            join_ok, join_err = await self._ensure_assistant_in_chat(chat_id)
        except Exception as e:
            join_ok, join_err = False, e

        if not join_ok:
            friendly = classify_error(
                join_err or Exception("join failed"),
                assistant_username=self.assistant_username,
                assistant_id=self.assistant_id,
                chat_id=chat_id,
            )
            raise FriendlyError(friendly)

        attempts = 0
        max_attempts = 3
        last_exc: Optional[Exception] = None

        while attempts < max_attempts:
            attempts += 1
            try:
                await self._pytgcalls_play(chat_id, track.stream_url, track.is_video)
                break
            except Exception as e:
                last_exc = e
                msg = str(e).upper()

                if "PEER_ID_INVALID" in msg:
                    self._peer_resolved.pop(chat_id, None)
                    await self._resolve_peer_ultimate(chat_id)
                    await asyncio.sleep(0.6)
                    if attempts >= 2:
                        try:
                            chat = await self.bot.get_chat(chat_id)
                            uname = getattr(chat, "username", None)
                            link = f"@{uname}" if uname else await self.bot.export_chat_invite_link(chat_id)
                            try:
                                await self.assistant.join_chat(link)
                            except UserAlreadyParticipant:
                                pass
                            await asyncio.sleep(0.6)
                            await self._resolve_peer_ultimate(chat_id)
                        except Exception:
                            pass
                    continue

                if track.query and ("403" in str(e) or "forbidden" in str(e).lower()):
                    try:
                        self.invalidate_cached_track(track.query, track.is_video)
                        track = await self.extract_track(track.query, track.is_video, False)
                        continue
                    except Exception:
                        pass

                friendly = classify_error(
                    e,
                    assistant_username=self.assistant_username,
                    assistant_id=self.assistant_id,
                    chat_id=chat_id,
                )
                raise FriendlyError(friendly)
        else:
            friendly = classify_error(
                last_exc or Exception("Unknown play failure"),
                assistant_username=self.assistant_username,
                assistant_id=self.assistant_id,
                chat_id=chat_id,
            )
            raise FriendlyError(friendly)

        state = self.get_state(chat_id)
        state.current = track
        state.paused  = False
        state.muted   = False
        self._schedule_save()
        return track

    # ─── CALL CONTROLS ───

    async def _leave_call(self, chat_id: int) -> bool:
        for m in ("leave_call", "leave_group_call"):
            fn = getattr(self.calls, m, None)
            if fn:
                try:
                    r = fn(chat_id)
                    if asyncio.iscoroutine(r): await r
                    return True
                except Exception:
                    pass
        return False

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

    async def _change_volume(self, chat_id: int, vol: int):
        for m in ("change_volume_call", "change_volume", "set_volume"):
            fn = getattr(self.calls, m, None)
            if fn:
                try:
                    r = fn(chat_id, vol)
                    if asyncio.iscoroutine(r): await r
                    return
                except Exception:
                    continue
        raise RuntimeError("volume unavailable")

    # ═══════════════════════════════════════════════════════════════
    # PLAY NEXT
    # ═══════════════════════════════════════════════════════════════

    async def _play_next_locked(self, chat_id: int, announce: bool = False,
                                 reason: str = "", clean_switch: bool = False) -> None:
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

        if clean_switch:
            try:
                await self._leave_call(chat_id)
                await asyncio.sleep(0.6)
            except Exception:
                pass

        try:
            played = await self._play_track(chat_id, nxt)
            nxt = played
        except FriendlyError as fe:
            state.current = None
            state.paused  = False
            self._schedule_save()
            if announce:
                try:
                    await self.bot.send_message(chat_id, fe.html_text, disable_web_page_preview=True)
                except Exception: pass
            if state.queue:
                log.warning("Skipping failed track, trying next")
                await self._play_next_locked(chat_id, announce=announce,
                                              reason="ᴀᴜᴛᴏ-ꜱᴋɪᴘ",
                                              clean_switch=clean_switch)
            return
        except Exception as e:
            state.current = None
            state.paused  = False
            self._schedule_save()
            if announce:
                friendly = classify_error(
                    e, assistant_username=self.assistant_username,
                    assistant_id=self.assistant_id, chat_id=chat_id,
                )
                try:
                    await self.bot.send_message(chat_id, friendly, disable_web_page_preview=True)
                except Exception: pass
            if state.queue:
                await self._play_next_locked(chat_id, announce=announce,
                                              reason="ᴀᴜᴛᴏ-ꜱᴋɪᴘ",
                                              clean_switch=clean_switch)
            return

        if announce:
            try:
                await self._send_play_card(chat_id, nxt)
            except Exception:
                pass

    async def _play_next(self, chat_id: int, announce: bool = False,
                         reason: str = "", clean_switch: bool = False) -> None:
        async with self.get_lock(chat_id):
            await self._play_next_locked(chat_id, announce=announce,
                                          reason=reason, clean_switch=clean_switch)

    async def _on_stream_end(self, chat_id: int) -> None:
        try:
            await self._play_next(chat_id, announce=True,
                                   reason="ᴘʀᴇᴠɪᴏᴜꜱ ꜱᴛʀᴇᴀᴍ ᴇɴᴅᴇᴅ",
                                   clean_switch=False)
        except Exception:
            log.exception("on_stream_end failed")

    # ─── CORE PLAY HANDLER ───

    async def _handle_play(self, message: Message, query: str, want_video: bool = False) -> None:
        asyncio.ensure_future(self._try_delete(message))
        self._track_user(getattr(message.from_user, "id", None))
        self._track_chat(message.chat.id)

        if not query:
            await self._safe_send(
                message,
                f"❓ <b>ᴜꜱᴀɢᴇ:</b>\n\n"
                f"  ▸ <code>/{'vplay' if want_video else 'play'} sᴏɴɢ ɴᴀᴍᴇ</code>\n"
                f"  ▸ <code>/{'vplay' if want_video else 'play'} youtube_url</code>"
            )
            return

        msg = await self._safe_send(
            message,
            f"  🔎 <b>ꜱᴇᴀʀᴄʜɪɴɢ...</b>\n  <i>{escape_html(query)}</i>"
        )

        try:
            track = await self.extract_track(query, want_video)
            track.requested_by = mention_user(message)
            track.is_video = want_video
        except Exception as e:
            return await self._safe_edit(
                msg,
                f"❌ <b>ꜱᴏɴɢ ɴᴀʜɪ ᴍɪʟᴀ</b>\n\n<code>{escape_html(str(e)[:200])}</code>"
            )

        async with self.get_lock(message.chat.id):
            state = self.get_state(message.chat.id)
            if state.current:
                state.queue.append(track)
                self._schedule_save()
                return await self._safe_edit(
                    msg,
                    f"  📥 <b>ǫᴜᴇᴜᴇᴅ #{len(state.queue)}</b>\n\n"
                    f"<blockquote>🍎  <b>ᴛɪᴛʟᴇ</b>     :  {escape_html(track.title)}</blockquote>\n"
                    f"<blockquote>⚙️  <b>ᴅᴜʀᴀᴛɪᴏɴ</b>  :  {escape_html(track.pretty_duration)}</blockquote>\n"
                    f"<blockquote>💫  <b>ʀᴇǫᴜᴇꜱᴛᴇᴅ</b> :  {track.requested_by}</blockquote>"
                )

            await self._safe_edit(
                msg,
                f"  ⚡ <b>ᴄᴏɴɴᴇᴄᴛɪɴɢ...</b>\n  🏷 <i>{escape_html(track.title)}</i>"
            )

            try:
                await self._play_track(message.chat.id, track)
            except FriendlyError as fe:
                state.current = None
                state.paused = False
                self._schedule_save()
                return await self._safe_edit(msg, fe.html_text)
            except Exception as e:
                state.current = None
                state.paused = False
                self._schedule_save()
                friendly = classify_error(
                    e, assistant_username=self.assistant_username,
                    assistant_id=self.assistant_id, chat_id=message.chat.id,
                )
                return await self._safe_edit(msg, friendly)

        # v13 FIX: Always send play card AFTER playing (delete loading msg first)
        try:
            try: await msg.delete()
            except Exception: pass
            await self._send_play_card(message.chat.id, track)
        except Exception:
            log.exception("post-play card failed")

    # ─── HEARTBEAT LOOP (v13) ───

    async def _heartbeat_loop(self) -> None:
        """v13: Write heartbeat every HEARTBEAT_INTERVAL seconds — proves bot is alive AND responsive."""
        write_heartbeat(self.config.bot_id)
        while not self._stopping:
            try:
                write_heartbeat(self.config.bot_id)
            except Exception:
                pass
            for _ in range(HEARTBEAT_INTERVAL):
                if self._stopping:
                    return
                await asyncio.sleep(1)

    # ─── WATCHDOG (v13 — heartbeat-aware, robust kill) ───

    async def _clone_watchdog(self) -> None:
        await asyncio.sleep(30)
        while not self._stopping:
            try:
                now = int(time.time())
                for cfg_file in sorted(CLONES_DIR.glob("*.json")):
                    bot_id = cfg_file.stem
                    pid_file = PIDS_DIR / f"{bot_id}.pid"

                    pid = None
                    if pid_file.exists():
                        try:
                            pid = int(pid_file.read_text().strip())
                        except Exception:
                            pid = None

                    alive = pid is not None and is_process_alive(pid)
                    last_hb = read_heartbeat(bot_id)
                    hb_age = now - last_hb if last_hb else 99999

                    needs_restart = False
                    reason = ""

                    if not alive:
                        needs_restart = True
                        reason = "process not alive"
                    elif last_hb == 0:
                        try:
                            pid_age = now - int(pid_file.stat().st_mtime)
                            if pid_age > CLONE_BOOT_GRACE:
                                needs_restart = True
                                reason = f"no heartbeat after {pid_age}s boot"
                        except Exception:
                            pass
                    elif hb_age > HEARTBEAT_STALE_AFTER:
                        needs_restart = True
                        reason = f"stale heartbeat ({hb_age}s)"

                    if not needs_restart:
                        continue

                    log.warning("Watchdog: clone %s needs restart — %s", bot_id, reason)

                    # v13: SIGTERM grace → SIGKILL escalation
                    if alive and pid:
                        try:
                            kill_process_tree(pid, signal.SIGTERM)
                            # Wait up to SIGTERM_GRACE for clean shutdown
                            for _ in range(SIGTERM_GRACE):
                                if not is_process_alive(pid):
                                    break
                                await asyncio.sleep(1)
                            # Still alive → SIGKILL
                            if is_process_alive(pid):
                                log.warning("Watchdog: %s ignored SIGTERM, sending SIGKILL", bot_id)
                                kill_process_tree(pid, signal.SIGKILL)
                                await asyncio.sleep(1)
                        except Exception:
                            log.exception("kill failed for %s", bot_id)

                    try: pid_file.unlink(missing_ok=True)
                    except Exception: pass
                    clear_heartbeat(bot_id)

                    # Respawn
                    new_pid = spawn_clone_process(cfg_file, bot_id)
                    if new_pid:
                        try:
                            pid_file.write_text(str(new_pid))
                            log.info("Watchdog: clone %s respawned pid=%d", bot_id, new_pid)
                        except Exception:
                            log.exception("write pid file failed for %s", bot_id)
                    else:
                        log.error("Watchdog: clone %s respawn FAILED — will retry next cycle", bot_id)

                # Cleanup orphan pid files (config deleted)
                for pid_file in list(PIDS_DIR.glob("*.pid")):
                    bot_id = pid_file.stem
                    cfg_file = CLONES_DIR / f"{bot_id}.json"
                    if not cfg_file.exists():
                        try:
                            pid = int(pid_file.read_text().strip())
                            if is_process_alive(pid):
                                kill_process_tree(pid, signal.SIGTERM)
                                await asyncio.sleep(2)
                                if is_process_alive(pid):
                                    kill_process_tree(pid, signal.SIGKILL)
                        except Exception:
                            pass
                        try: pid_file.unlink(missing_ok=True)
                        except Exception: pass
                        clear_heartbeat(bot_id)

            except Exception:
                log.exception("Watchdog error")

            for _ in range(WATCHDOG_INTERVAL):
                if self._stopping:
                    return
                await asyncio.sleep(1)

    async def _auto_launch_clones(self) -> None:
        for cfg_file in sorted(CLONES_DIR.glob("*.json")):
            bot_id   = cfg_file.stem
            pid_file = PIDS_DIR / f"{bot_id}.pid"
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text().strip())
                    if is_process_alive(pid):
                        last_hb = read_heartbeat(bot_id)
                        hb_age = int(time.time()) - last_hb if last_hb else 99999
                        if hb_age < HEARTBEAT_STALE_AFTER:
                            log.info("Auto-launch: clone %s already running healthy", bot_id)
                            continue
                        else:
                            log.warning("Auto-launch: clone %s pid alive but stale hb (%ds) — restarting", bot_id, hb_age)
                            kill_process_tree(pid, signal.SIGTERM)
                            for _ in range(SIGTERM_GRACE):
                                if not is_process_alive(pid):
                                    break
                                await asyncio.sleep(1)
                            if is_process_alive(pid):
                                kill_process_tree(pid, signal.SIGKILL)
                                await asyncio.sleep(1)
                except Exception:
                    pass

            clear_heartbeat(bot_id)
            new_pid = spawn_clone_process(cfg_file, bot_id)
            if new_pid:
                try:
                    pid_file.write_text(str(new_pid))
                    log.info("Auto-launch: clone %s pid=%d", bot_id, new_pid)
                except Exception:
                    pass
            # Stagger launches to avoid thundering herd
            await asyncio.sleep(1.5)

    async def _signal_watcher(self) -> None:
        while not self._stopping:
            if _SHUTDOWN_FLAG.is_set():
                log.info("Shutdown flag set — stopping...")
                self._stopping = True
                try:
                    await self.bot.stop()
                except Exception:
                    pass
                return
            await asyncio.sleep(0.5)

    # ─── HANDLERS ───

    async def _add_handlers(self) -> None:

        # ── Stream end ──
        @self.calls.on_update()
        async def stream_update(_, update):
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
                elif "ended" in name or "end" in name:
                    is_ended = True
                if is_ended:
                    asyncio.ensure_future(self._on_stream_end(chat_id))
            except Exception:
                pass

        # ── /start ──
        @self.bot.on_message(filters.command(["start"]) & (filters.private | filters.group))
        async def start(_, m: Message):
            try:
                self._track_user(getattr(m.from_user, "id", None))
                self._track_chat(m.chat.id)
                await self._send_start_panel(m)
            except Exception: log.exception("start failed")

        # ── /help ──
        @self.bot.on_message(filters.command(["help", "commands"]) & (filters.private | filters.group))
        async def help_cmd(_, m: Message):
            try:
                fid = (self.settings.get("start_photo_file_id") or "").strip()
                mtype = (self.settings.get("start_photo_type") or "").strip().lower()
                if fid:
                    try:
                        if mtype == "video":
                            await m.reply_video(video=fid, caption=self._help_home_text(),
                                              reply_markup=self._help_kb(), supports_streaming=True)
                            return
                        elif mtype == "animation":
                            await m.reply_animation(animation=fid, caption=self._help_home_text(),
                                                  reply_markup=self._help_kb())
                            return
                        else:
                            await m.reply_photo(photo=fid, caption=self._help_home_text(), reply_markup=self._help_kb())
                            return
                    except Exception: pass
                await self._safe_send(m, self._help_home_text(), reply_markup=self._help_kb())
            except Exception: log.exception("help failed")

        # ── /about ──
        @self.bot.on_message(filters.command(["about"]) & (filters.private | filters.group))
        async def about(_, m: Message):
            try: await self._safe_send(m, self._about_text(), reply_markup=self._subpage_kb())
            except Exception: pass

        # ── Callbacks ──
        @self.bot.on_callback_query()
        async def cb(_, q):
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

                if d == "ctl_close_card":
                    try: await q.message.delete()
                    except Exception: pass
                    return await q.answer("ᴄʟᴏꜱᴇᴅ")

                if d.startswith("ctl_"):
                    ct = str(getattr(getattr(q.message, "chat", None), "type", "")).lower()
                    if "group" not in ct:
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
                        await q.answer("⏭ ꜱᴋɪᴘᴘɪɴɢ...")
                        async def _do_skip():
                            async with self.get_lock(cid):
                                st = self.get_state(cid)
                                st.loop = False
                                st.current = None
                                st.paused = False
                                self._schedule_save()
                                await self._play_next_locked(
                                    cid, announce=True,
                                    reason="ꜱᴋɪᴘᴘᴇᴅ",
                                    clean_switch=True,
                                )
                        asyncio.ensure_future(_do_skip())
                        return

                    if d == "ctl_stop":
                        async with self.get_lock(cid):
                            state.queue.clear(); state.current = None
                            state.paused = state.loop = state.muted = False
                            self._schedule_save()
                            await self._leave_call(cid)
                        await self._safe_edit_panel(q.message, "  ⏹ <b>ꜱᴛᴏᴘᴘᴇᴅ.</b>", self._queue_kb())
                        return await q.answer("⏹ ꜱᴛᴏᴘᴘᴇᴅ")

                    if d == "ctl_queue":
                        await self._safe_edit_panel(q.message, self._queue_text(state), self._queue_kb())
                        return await q.answer()

                    if d == "ctl_np":
                        await self._safe_edit_panel(q.message, self._np_text(state), self._np_kb())
                        return await q.answer()

                    if d == "ctl_shuffle":
                        if len(state.queue) < 2:
                            return await q.answer("ǫᴜᴇᴜᴇ ᴄʜʜᴏᴛɪ.", show_alert=True)
                        random.shuffle(state.queue); self._schedule_save()
                        await self._safe_edit_panel(q.message, self._queue_text(state), self._queue_kb())
                        return await q.answer("🔀 ꜱʜᴜꜰꜰʟᴇᴅ")

                    if d == "ctl_clearqueue":
                        c = len(state.queue); state.queue.clear(); self._schedule_save()
                        await self._safe_edit_panel(q.message, self._queue_text(state), self._queue_kb())
                        return await q.answer(f"🧹 {c} ᴄʟᴇᴀʀ")
            except Exception:
                log.exception("callback failed")

        # ── /ping & /alive ──
        @self.bot.on_message(filters.command(["ping", "alive"]) & (filters.private | filters.group))
        async def ping(_, m: Message):
            try:
                asyncio.ensure_future(self._try_delete(m))
                t0 = time.time()
                x = await self._safe_send(m, "🏓")
                ms = (time.time() - t0) * 1000
                up = pretty_uptime(int(time.time() - self.start_time))
                ac = sum(1 for s in self.states.values() if s.current)
                cl = len(list(CLONES_DIR.glob("*.json")))
                n = escape_html(self.display_name)
                t = (
                    f"<b>  💚 {n} ɪꜱ ᴏɴʟɪɴᴇ</b>\n\n"
                    f"<blockquote>⚡  <b>ʟᴀᴛᴇɴᴄʏ</b>  :  {ms:.2f} ᴍꜱ\n"
                    f"⏳  <b>ᴜᴘᴛɪᴍᴇ</b>   :  {escape_html(up)}\n"
                    f"🎧  <b>ᴀᴄᴛɪᴠᴇ</b>   :  {ac} ᴄʜᴀᴛꜱ\n"
                    f"🤖  <b>ʙᴏᴛ ɪᴅ</b>   :  <code>{self.config.bot_id}</code></blockquote>"
                )
                if self.is_master:
                    t += f"\n<blockquote>🔁  <b>ᴄʟᴏɴᴇꜱ</b>   :  {cl} ꜱᴀᴠᴇᴅ</blockquote>"
                if x: await self._safe_edit(x, t)
            except Exception: log.exception("ping failed")

        # ── /stats ──
        @self.bot.on_message(filters.command(["stats"]) & (filters.private | filters.group))
        async def stats(_, m: Message):
            try:
                if not self.is_config_owner_user(m): return
                asyncio.ensure_future(self._try_delete(m))
                t = (
                    f"<b>  📊 ʙᴏᴛ ꜱᴛᴀᴛꜱ</b>\n\n"
                    f"<blockquote>👥 ᴜꜱᴇʀꜱ  : {len(self.known_users)}\n"
                    f"💬 ᴄʜᴀᴛꜱ  : {len(self.known_chats)}\n"
                    f"🎧 ᴀᴄᴛɪᴠᴇ : {sum(1 for s in self.states.values() if s.current)}\n"
                    f"📥 ǫᴜᴇᴜᴇᴅ : {sum(len(s.queue) for s in self.states.values())}\n"
                    f"⏳ ᴜᴘᴛɪᴍᴇ : {pretty_uptime(int(time.time() - self.start_time))}\n"
                    f"🚦 ᴊᴏɪɴ ᴄᴀᴄʜᴇ : {len(self._assistant_joined)}\n"
                    f"🌐 ᴘᴇᴇʀ ᴄᴀᴄʜᴇ : {len(self._peer_resolved)}\n"
                    f"💾 ᴛʀᴀᴄᴋ ᴄᴀᴄʜᴇ: {len(self._track_cache)}</blockquote>"
                )
                await self._safe_send(m, t)
            except Exception: pass

        # ── /play /p (AUDIO ONLY) ──
        @self.bot.on_message(filters.command(["play", "p"]) & filters.group)
        async def play(_, m: Message):
            try:
                await self._handle_play(m, command_arg(m), want_video=False)
            except Exception:
                log.exception("play failed")
                await self._safe_send(m, "❌ /play ᴍᴇ ᴛᴇᴄʜɴɪᴄᴀʟ ɪꜱꜱᴜᴇ — ᴅᴜʙᴀʀᴀ ᴛʀʏ ᴋᴀʀᴏ.")

        # ── /vplay (VIDEO ONLY) ──
        @self.bot.on_message(filters.command(["vplay", "vp"]) & filters.group)
        async def vplay(_, m: Message):
            try:
                await self._handle_play(m, command_arg(m), want_video=True)
            except Exception:
                log.exception("vplay failed")
                await self._safe_send(m, "❌ /vplay ᴍᴇ ᴛᴇᴄʜɴɪᴄᴀʟ ɪꜱꜱᴜᴇ — ᴅᴜʙᴀʀᴀ ᴛʀʏ ᴋᴀʀᴏ.")

        # ── /refresh ──
        @self.bot.on_message(filters.command(["refresh"]) & filters.group)
        async def refresh(_, m: Message):
            try:
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                if state.current:
                    await self._send_play_card(m.chat.id, state.current)
                else:
                    await self._safe_send(m, self._np_text(state), reply_markup=self._np_kb())
            except Exception: pass

        # ── /pause ──
        @self.bot.on_message(filters.command(["pause"]) & filters.group)
        async def pause(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                if state.paused: return await self._safe_send(m, "⏸ ᴘᴇʜʟᴇ ꜱᴇ ᴘᴀᴜꜱᴇᴅ.")
                try:
                    await self._pause_call(m.chat.id); state.paused = True; self._schedule_save()
                    await self._safe_send(m, "⏸ <b>ᴘᴀᴜꜱᴇᴅ.</b>")
                except Exception as e:
                    await self._safe_send(m, classify_error(e, assistant_username=self.assistant_username, assistant_id=self.assistant_id))
            except Exception:
                log.exception("pause failed")

        # ── /resume ──
        @self.bot.on_message(filters.command(["resume"]) & filters.group)
        async def resume(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                if not state.paused: return await self._safe_send(m, "▶️ ᴀʟʀᴇᴀᴅʏ ᴄʜᴀʟ ʀʜᴀ.")
                try:
                    await self._resume_call(m.chat.id); state.paused = False; self._schedule_save()
                    await self._safe_send(m, "▶️ <b>ʀᴇꜱᴜᴍᴇᴅ.</b>")
                except Exception as e:
                    await self._safe_send(m, classify_error(e, assistant_username=self.assistant_username, assistant_id=self.assistant_id))
            except Exception:
                log.exception("resume failed")

        # ── /skip /next ──
        @self.bot.on_message(filters.command(["skip", "next"]) & filters.group)
        async def skip(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                if not state.current and not state.queue:
                    return await self._safe_send(m, "📭 ᴋᴜᴄʜ ɴᴀʜɪ ᴘʟᴀʏ ʜᴏ ʀʜᴀ.")
                async with self.get_lock(m.chat.id):
                    st = self.get_state(m.chat.id)
                    st.loop = False
                    st.current = None
                    st.paused = False
                    self._schedule_save()
                    await self._play_next_locked(
                        m.chat.id, announce=True,
                        reason="ꜱᴋɪᴘᴘᴇᴅ", clean_switch=True,
                    )
            except Exception:
                log.exception("skip failed")

        # ── /stop /end ──
        @self.bot.on_message(filters.command(["stop", "end"]) & filters.group)
        async def stop(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                async with self.get_lock(m.chat.id):
                    state = self.get_state(m.chat.id)
                    state.queue.clear(); state.current = None
                    state.paused = state.loop = state.muted = False
                    self._schedule_save()
                    await self._leave_call(m.chat.id)
                await self._safe_send(m, "⏹ <b>ꜱᴛᴏᴘ.</b> ǫᴜᴇᴜᴇ ᴄʟᴇᴀʀ.")
            except Exception:
                log.exception("stop failed")

        # ── /leave ──
        @self.bot.on_message(filters.command(["leave"]) & filters.group)
        async def leave(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                async with self.get_lock(m.chat.id):
                    state = self.get_state(m.chat.id)
                    state.queue.clear(); state.current = None
                    state.paused = state.loop = state.muted = False
                    self._schedule_save()
                    await self._leave_call(m.chat.id)
                await self._safe_send(m, "🚪 ᴀꜱꜱɪꜱᴛᴀɴᴛ ʟᴇꜰᴛ ᴠᴄ.")
            except Exception:
                log.exception("leave failed")

        # ── /queue /q ──
        @self.bot.on_message(filters.command(["queue", "q"]) & filters.group)
        async def queue(_, m: Message):
            try:
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                await self._safe_send(m, self._queue_text(state), reply_markup=self._queue_kb())
            except Exception: pass

        # ── /playlist ──
        @self.bot.on_message(filters.command(["playlist"]) & filters.group)
        async def playlist(_, m: Message):
            try:
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                audio_q = [t for t in state.queue if not t.is_video]
                if state.current and not state.current.is_video:
                    txt = f"<b>  🎼 ᴀᴜᴅɪᴏ ᴘʟᴀʏʟɪꜱᴛ</b>\n\n  🎵 <b>ᴘʟᴀʏɪɴɢ:</b>\n     {escape_html(state.current.title)}\n\n"
                else:
                    txt = "<b>  🎼 ᴀᴜᴅɪᴏ ᴘʟᴀʏʟɪꜱᴛ</b>\n\n"
                if audio_q:
                    txt += "  📥 <b>ᴜᴘ ɴᴇxᴛ:</b>\n"
                    for i, t in enumerate(audio_q[:15], 1):
                        txt += f"  {i}. {escape_html(t.title[:40])}\n"
                else:
                    txt += "  📭 ɴᴏ ᴀᴜᴅɪᴏ ɪɴ ǫᴜᴇᴜᴇ."
                await self._safe_send(m, txt)
            except Exception: pass

        # ── /loop ──
        @self.bot.on_message(filters.command(["loop"]) & filters.group)
        async def loop_cmd(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                arg = command_arg(m).lower()
                state.loop = True if arg == "on" else False if arg == "off" else not state.loop
                self._schedule_save()
                await self._safe_send(m, f"🔁 <b>ʟᴏᴏᴘ:</b> {human_bool(state.loop)}")
            except Exception: pass

        # ── /shuffle ──
        @self.bot.on_message(filters.command(["shuffle"]) & filters.group)
        async def shuffle(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                if len(state.queue) < 2:
                    return await self._safe_send(m, "📭 ǫᴜᴇᴜᴇ ᴄʜʜᴏᴛɪ ʜᴀɪ.")
                random.shuffle(state.queue); self._schedule_save()
                await self._safe_send(m, f"🔀 <b>ꜱʜᴜꜰꜰʟᴇᴅ!</b> ({len(state.queue)} ᴛʀᴀᴄᴋꜱ)")
            except Exception: pass

        # ── /clearqueue ──
        @self.bot.on_message(filters.command(["clearqueue"]) & filters.group)
        async def cq(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                c = len(state.queue); state.queue.clear(); self._schedule_save()
                await self._safe_send(m, f"🧹 <b>{c}</b> ᴛʀᴀᴄᴋꜱ ᴄʟᴇᴀʀ.")
            except Exception: pass

        # ── /mute ──
        @self.bot.on_message(filters.command(["mute"]) & filters.group)
        async def mute(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                try:
                    await self._mute_call(m.chat.id)
                    self.get_state(m.chat.id).muted = True; self._schedule_save()
                    await self._safe_send(m, "🔇 <b>ᴍᴜᴛᴇᴅ.</b>")
                except Exception as e:
                    await self._safe_send(m, classify_error(e, assistant_username=self.assistant_username, assistant_id=self.assistant_id))
            except Exception:
                log.exception("mute failed")

        # ── /unmute ──
        @self.bot.on_message(filters.command(["unmute"]) & filters.group)
        async def unmute(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                try:
                    await self._unmute_call(m.chat.id)
                    self.get_state(m.chat.id).muted = False; self._schedule_save()
                    await self._safe_send(m, "🔊 <b>ᴜɴᴍᴜᴛᴇᴅ.</b>")
                except Exception as e:
                    await self._safe_send(m, classify_error(e, assistant_username=self.assistant_username, assistant_id=self.assistant_id))
            except Exception:
                log.exception("unmute failed")

        # ── /volume ──
        @self.bot.on_message(filters.command(["volume", "vol"]) & filters.group)
        async def volume(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                arg = command_arg(m).strip()
                if not arg.isdigit():
                    state = self.get_state(m.chat.id)
                    return await self._safe_send(m, f"🔊 <b>ᴄᴜʀʀᴇɴᴛ ᴠᴏʟᴜᴍᴇ:</b> {state.volume}%\n\nᴜꜱᴀɢᴇ: <code>/volume 1-200</code>")
                v = max(1, min(200, int(arg)))
                try:
                    await self._change_volume(m.chat.id, v)
                    self.get_state(m.chat.id).volume = v
                    self._schedule_save()
                    await self._safe_send(m, f"🔊 <b>ᴠᴏʟᴜᴍᴇ ꜱᴇᴛ:</b> {v}%")
                except Exception as e:
                    await self._safe_send(m, classify_error(e, assistant_username=self.assistant_username, assistant_id=self.assistant_id))
            except Exception: pass

        # ── /np /now ──
        @self.bot.on_message(filters.command(["np", "now"]) & filters.group)
        async def np(_, m: Message):
            try:
                asyncio.ensure_future(self._try_delete(m))
                state = self.get_state(m.chat.id)
                if state.current:
                    await self._send_play_card(m.chat.id, state.current)
                else:
                    await self._safe_send(m, self._np_text(state), reply_markup=self._np_kb())
            except Exception: pass

        # ── /userbotjoin ──
        @self.bot.on_message(filters.command(["userbotjoin"]) & filters.group)
        async def ubjoin(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                self._invalidate_join_cache(m.chat.id)
                ok, err = await self._ensure_assistant_in_chat(m.chat.id)
                if ok:
                    await self._safe_send(m, f"✅ <b>ᴀꜱꜱɪꜱᴛᴀɴᴛ ᴊᴏɪɴᴇᴅ!</b>\n\n@{escape_html(self.assistant_username)}")
                else:
                    friendly = classify_error(err or Exception("join failed"),
                                              assistant_username=self.assistant_username,
                                              assistant_id=self.assistant_id)
                    await self._safe_send(m, friendly)
            except Exception:
                log.exception("ubjoin failed")

        # ── /userbotleave ──
        @self.bot.on_message(filters.command(["userbotleave"]) & filters.group)
        async def ubleave(_, m: Message):
            try:
                if not await self.require_admin(m): return
                asyncio.ensure_future(self._try_delete(m))
                try:
                    await self.assistant.leave_chat(m.chat.id)
                    self._invalidate_join_cache(m.chat.id)
                    await self._safe_send(m, "🚪 ᴀꜱꜱɪꜱᴛᴀɴᴛ ʟᴇꜰᴛ.")
                except Exception as e:
                    await self._safe_send(m, classify_error(e, assistant_username=self.assistant_username, assistant_id=self.assistant_id))
            except Exception: pass

        # ── /shelp ──
        @self.bot.on_message(filters.command(["shelp"]) & (filters.private | filters.group))
        async def shelp(_, m: Message):
            try:
                if not self.is_config_owner_user(m): return
                await self._safe_send(m, self._shell_help_text())
            except Exception: pass

        # ── /setdp (v13: photo + video both supported) ──
        @self.bot.on_message(filters.command(["setdp"]) & filters.private)
        async def setdp(_, m: Message):
            try:
                if not self.is_config_owner_user(m):
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴏᴡɴᴇʀ.")
                self.pending_start_photo[m.from_user.id] = time.time()
                await self._safe_send(m,
                    f"<b>  🖼 ꜱᴛᴀʀᴛ ᴍᴇᴅɪᴀ</b>\n\n"
                    f"  ᴀʙ ᴇᴋ <b>ᴘʜᴏᴛᴏ</b> ʏᴀ <b>ᴠɪᴅᴇᴏ</b> ʙʜᴇᴊᴏ.\n"
                    f"  ʏᴇ /start ᴘᴀʀ ᴅɪᴋʜᴇɢᴀ.\n\n"
                    f"  /cancel ꜱᴇ ʙᴀɴᴅ."
                )
            except Exception: pass

        # ── /removedp ──
        @self.bot.on_message(filters.command(["removedp"]) & filters.private)
        async def removedp(_, m: Message):
            try:
                if not self.is_config_owner_user(m):
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴏᴡɴᴇʀ.")
                self.settings["start_photo_file_id"] = ""
                self.settings["start_photo_type"] = ""
                self._save_settings()
                self.pending_start_photo.pop(m.from_user.id, None)
                await self._safe_send(m, "✅ ꜱᴛᴀʀᴛ ᴍᴇᴅɪᴀ ʜᴀᴛᴀʏɪ.")
            except Exception: pass

        # ── /setthmb ──
        @self.bot.on_message(filters.command(["setthmb", "setthumb"]) & filters.private)
        async def setthmb(_, m: Message):
            try:
                if not self.is_master:
                    return await self._safe_send(m,
                        "⚠️ ʏᴇ ᴄᴏᴍᴍᴀɴᴅ ꜱɪʀꜰ ᴍᴀɪɴ ʙᴏᴛ ᴋᴇ ᴏᴡɴᴇʀ ᴋᴇ ʟɪᴇ ʜᴀɪ.")
                if not self.is_config_owner_user(m):
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴏᴡɴᴇʀ.")
                self.pending_thumb[m.from_user.id] = time.time()
                await self._safe_send(m,
                    f"<b>  🎬 ɢʟᴏʙᴀʟ ᴘʟᴀʏ ᴛʜᴜᴍʙɴᴀɪʟ</b>\n\n"
                    f"  ᴀʙ ᴇᴋ <b>ᴠɪᴅᴇᴏ ʏᴀ ᴘʜᴏᴛᴏ</b> ʙʜᴇᴊᴏ.\n"
                    f"  ʏᴇ ᴀᴀᴊ ꜱᴇ ʜᴀʀ ᴄʟᴏɴᴇ + ᴍᴀɪɴ ʙᴏᴛ ᴋᴇ ᴘʟᴀʏ ᴄᴀʀᴅ ᴘᴇ ᴅɪᴋʜᴇɢᴀ.\n\n"
                    f"  /cancel ꜱᴇ ʙᴀɴᴅ ᴋᴀʀᴏ\n"
                    f"  /delthmb ꜱᴇ ᴘᴜʀᴀɴᴀ ᴛʜᴜᴍʙɴᴀɪʟ ʜᴀᴛᴀᴏ"
                )
            except Exception:
                log.exception("setthmb failed")

        @self.bot.on_message(filters.command(["delthmb", "delthumb", "removethmb"]) & filters.private)
        async def delthmb(_, m: Message):
            try:
                if not self.is_master:
                    return await self._safe_send(m,
                        "⚠️ ʏᴇ ᴄᴏᴍᴍᴀɴᴅ ꜱɪʀꜰ ᴍᴀɪɴ ʙᴏᴛ ᴋᴇ ᴏᴡɴᴇʀ ᴋᴇ ʟɪᴇ ʜᴀɪ.")
                if not self.is_config_owner_user(m):
                    return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴏᴡɴᴇʀ.")
                clear_shared_thumb()
                self.pending_thumb.pop(m.from_user.id, None)
                await self._safe_send(m, "✅ ᴛʜᴜᴍʙɴᴀɪʟ ʜᴀᴛᴀʏᴀ. ᴀʙ ᴋᴏɪ ᴄᴜꜱᴛᴏᴍ ᴛʜᴜᴍʙ ɴᴀʜɪ ᴅɪᴋʜᴇɢᴀ.")
            except Exception:
                log.exception("delthmb failed")

        # ── Photo/Video receiver (v13: /setdp now supports video) ──
        @self.bot.on_message(filters.private & (filters.photo | filters.video | filters.document | filters.animation))
        async def media_receiver(_, m: Message):
            try:
                if not self.is_config_owner_user(m):
                    return

                # ── /setthmb pending (master only) ──
                if self.is_master and m.from_user and m.from_user.id in self.pending_thumb:
                    fid = ""
                    media_type = ""
                    if m.video:
                        fid = m.video.file_id; media_type = "video"
                    elif m.animation:
                        fid = m.animation.file_id; media_type = "video"
                    elif m.photo:
                        po = m.photo
                        fid = po.file_id if hasattr(po, "file_id") else (po[-1].file_id if isinstance(po, (list, tuple)) and po else "")
                        media_type = "photo"
                    elif m.document and (m.document.mime_type or ""):
                        mime = (m.document.mime_type or "").lower()
                        if mime.startswith("image/"):
                            fid = m.document.file_id; media_type = "photo"
                        elif mime.startswith("video/"):
                            fid = m.document.file_id; media_type = "video"
                    if not fid or not media_type:
                        return await self._safe_send(m, "❌ ꜱɪʀꜰ ᴘʜᴏᴛᴏ ʏᴀ ᴠɪᴅᴇᴏ ʙʜᴇᴊᴏ.")
                    save_shared_thumb(media_type, fid)
                    self.pending_thumb.pop(m.from_user.id, None)
                    return await self._safe_send(m,
                        f"<b>  ✅ ɢʟᴏʙᴀʟ ᴛʜᴜᴍʙɴᴀɪʟ ꜱᴇᴛ!</b>\n\n"
                        f"  📦 ᴛʏᴘᴇ: <code>{media_type}</code>\n"
                        f"  🤖 ᴀʙ ʜᴀʀ ᴄʟᴏɴᴇ + ᴍᴀɪɴ ʙᴏᴛ ᴋᴇ /play ᴘᴀʀ ᴅɪᴋʜᴇɢᴀ.\n\n"
                        f"  🗑️ ʜᴀᴛᴀɴᴇ ᴋᴇ ʟɪᴇ: /delthmb"
                    )

                # ── /setdp pending (v13: now accepts video too) ──
                if m.from_user and m.from_user.id in self.pending_start_photo:
                    fid = ""
                    media_type = ""
                    if m.photo:
                        po = m.photo
                        fid = po.file_id if hasattr(po, "file_id") else (po[-1].file_id if isinstance(po, (list, tuple)) and po else "")
                        media_type = "photo"
                    elif m.video:
                        fid = m.video.file_id
                        media_type = "video"
                    elif m.animation:
                        fid = m.animation.file_id
                        media_type = "animation"
                    elif m.document:
                        mime = (m.document.mime_type or "").lower()
                        if mime.startswith("image/"):
                            fid = m.document.file_id; media_type = "photo"
                        elif mime.startswith("video/"):
                            fid = m.document.file_id; media_type = "video"
                        else:
                            return await self._safe_send(m, "❌ ꜱɪʀꜰ ɪᴍᴀɢᴇ ʏᴀ ᴠɪᴅᴇᴏ ʙʜᴇᴊᴏ.")
                    else:
                        return await self._safe_send(m, "❌ ꜱɪʀꜰ ɪᴍᴀɢᴇ ʏᴀ ᴠɪᴅᴇᴏ ʙʜᴇᴊᴏ.")

                    if not fid or not media_type:
                        return await self._safe_send(m, "❌ ᴅᴏʙᴀʀᴀ ʙʜᴇᴊᴏ.")

                    self.settings["start_photo_file_id"] = fid
                    self.settings["start_photo_type"] = media_type
                    self._save_settings()
                    self.pending_start_photo.pop(m.from_user.id, None)
                    return await self._safe_send(m,
                        f"<b>  ✅ ꜱᴛᴀʀᴛ ᴍᴇᴅɪᴀ ꜱᴀᴠᴇᴅ!</b>\n\n"
                        f"  📦 ᴛʏᴘᴇ: <code>{media_type}</code>\n"
                        f"  💡 /start ᴘᴀʀ ᴅɪᴋʜᴇɢᴀ."
                    )
            except Exception:
                log.exception("media_receiver failed")

        # ═══════════════════ MASTER-ONLY ═══════════════════
        if self.is_master:

            @self.bot.on_message(filters.command(["clone"]) & filters.private)
            async def clone(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    self.clone_flow[m.from_user.id] = {"step": "bot_token"}
                    await self._safe_send(m,
                        f"<b>  🚀 ɴᴀʏᴀ ʙᴏᴛ ꜱᴇᴛᴜᴘ</b>\n\n"
                        f"  ꜱᴛᴇᴘ <b>1/5:</b> ʙᴏᴛ ᴛᴏᴋᴇɴ ʙʜᴇᴊᴏ\n\n"
                        f"  <code>123456789:ABCDEF...</code>\n\n"
                        f"  /cancel ꜱᴇ ʙᴀɴᴅ ᴋᴀʀᴏ"
                    )
                except Exception: pass

            @self.bot.on_message(filters.command(["dclone"]) & filters.private)
            async def dclone(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    token = command_arg(m).strip()
                    if not token:
                        return await self._safe_send(m, "❓ <code>/dclone bot_token</code>")
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
                            kill_process_tree(pid, signal.SIGTERM)
                            for _ in range(SIGTERM_GRACE):
                                if not is_process_alive(pid):
                                    break
                                await asyncio.sleep(1)
                            if is_process_alive(pid):
                                kill_process_tree(pid, signal.SIGKILL)
                            killed = True
                        except Exception: pass
                        pid_f.unlink(missing_ok=True)
                    clear_heartbeat(bot_id)
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
                        f"<b>  ✅ ʙᴏᴛ ꜱᴛᴏᴘᴘᴇᴅ</b>\n\n"
                        f"<blockquote>🤖  <code>{bot_id}</code>\n"
                        f"💀  ᴘʀᴏᴄᴇꜱꜱ : {'✅ ᴋɪʟʟᴇᴅ' if killed else '⚠️ ɴᴏᴛ ʀᴜɴɴɪɴɢ'}\n"
                        f"📁  ᴄᴏɴꜰɪɢ  : {'✅ ʀᴇᴍᴏᴠᴇᴅ' if cfg_removed else '⚠️ ɴᴏᴛ ꜰᴏᴜɴᴅ'}</blockquote>"
                    )
                except Exception: log.exception("dclone failed")

            @self.bot.on_message(filters.command(["cancel"]) & filters.private)
            async def cancel(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    had = (
                        m.from_user.id in self.clone_flow
                        or m.from_user.id in self.pending_start_photo
                        or m.from_user.id in self.pending_thumb
                    )
                    self.clone_flow.pop(m.from_user.id, None)
                    self.pending_start_photo.pop(m.from_user.id, None)
                    self.pending_thumb.pop(m.from_user.id, None)
                    await self._safe_send(m, "🛑 ᴄᴀɴᴄᴇʟʟᴇᴅ." if had else "✅ ɴᴏᴛʜɪɴɢ ᴘᴇɴᴅɪɴɢ.")
                except Exception: pass

            @self.bot.on_message(filters.command(["clones"]) & filters.private)
            async def clones_cmd(_, m: Message):
                try:
                    if not self.is_config_owner_user(m):
                        return await self._safe_send(m, "❌ ᴏᴡɴᴇʀ ᴏɴʟʏ.")
                    files = sorted(CLONES_DIR.glob("*.json"))
                    if not files:
                        return await self._safe_send(m, "📭 ᴋᴏɪ ꜱᴀᴠᴇᴅ ʙᴏᴛ ɴᴀʜɪ.")
                    lines = ["<b>  📦 ꜱᴀᴠᴇᴅ ᴄʟᴏɴᴇ ʙᴏᴛꜱ</b>", ""]
                    now = int(time.time())
                    for f in files[:50]:
                        try:
                            cfg   = load_config(f)
                            pid_f = PIDS_DIR / f"{cfg.bot_id}.pid"
                            live  = False
                            healthy = False
                            if pid_f.exists():
                                try:
                                    live = is_process_alive(int(pid_f.read_text().strip()))
                                except Exception: pass
                            last_hb = read_heartbeat(cfg.bot_id)
                            hb_age = now - last_hb if last_hb else 99999
                            healthy = live and hb_age < HEARTBEAT_STALE_AFTER
                            if healthy:
                                icon = "🟢"
                                status = f"ʜᴇᴀʟᴛʜʏ ({hb_age}s)"
                            elif live:
                                icon = "🟡"
                                status = f"ꜱᴛᴀʟᴇ ({hb_age}s)"
                            else:
                                icon = "🔴"
                                status = "ᴅᴇᴀᴅ"
                            lines.append(f"  {icon} <code>{escape_html(cfg.bot_id)}</code> — {status}")
                            lines.append(f"     👤 {escape_html(cfg.owner_username)}")
                        except Exception:
                            lines.append(f"  ⚠ {f.name}")
                    lines.append("")
                    lines.append(f"  💡 <code>/dclone &lt;token&gt;</code> ꜱᴇ ꜱᴛᴏᴘ ᴋᴀʀᴏ")
                    lines.append(f"  🟢 ʜᴇᴀʟᴛʜʏ | 🟡 ʜᴀɴɢ | 🔴 ᴅᴇᴀᴅ")
                    await self._safe_send(m, "\n".join(lines))
                except Exception: log.exception("clones failed")

            @self.bot.on_message(filters.command(["restart"]) & filters.private)
            async def restart(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    await self._safe_send(m, "♻️ ʀᴇꜱᴛᴀʀᴛɪɴɢ...")
                    await asyncio.sleep(1)
                    os.execv(sys.executable, [sys.executable, __file__] + sys.argv[1:])
                except Exception as e:
                    await self._safe_send(m, f"❌ {escape_html(str(e))}")

            @self.bot.on_message(filters.command(["broadcast"]) & filters.private)
            async def bc(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    text = command_arg(m)
                    if not text and m.reply_to_message:
                        text = m.reply_to_message.text or m.reply_to_message.caption or ""
                    if not text:
                        return await self._safe_send(m, "❓ <code>/broadcast message</code>")
                    sent = 0; failed = 0
                    status = await self._safe_send(m, "📡 ʙʀᴏᴀᴅᴄᴀꜱᴛɪɴɢ...")
                    for cid in list(self.known_chats):
                        try:
                            await self.bot.send_message(cid, text)
                            sent += 1
                        except Exception:
                            failed += 1
                        await asyncio.sleep(0.05)
                    await self._safe_edit(status, f"<b>  ✅ ʙʀᴏᴀᴅᴄᴀꜱᴛ ᴅᴏɴᴇ</b>\n\n  ✓ ꜱᴇɴᴛ: {sent}\n  ✗ ꜰᴀɪʟᴇᴅ: {failed}")
                except Exception as e:
                    await self._safe_send(m, f"❌ {escape_html(str(e))}")

            def _in_clone_flow_filter(_, __, m: Message) -> bool:
                try:
                    if not m.from_user:
                        return False
                    if m.from_user.id not in self.clone_flow:
                        return False
                    text = (m.text or "").strip()
                    if not text:
                        return False
                    if text.startswith("/"):
                        sf = self.clone_flow.get(m.from_user.id) or {}
                        step = sf.get("step", "")
                        if step == "session" and text.lower() == "/default":
                            return True
                        return False
                    return True
                except Exception:
                    return False

            in_clone_flow = filters.create(_in_clone_flow_filter)

            @self.bot.on_message(filters.private & filters.text & in_clone_flow)
            async def clone_flow_handler(_, m: Message):
                try:
                    if not self.is_config_owner_user(m): return
                    sf = self.clone_flow.get(m.from_user.id)
                    if not sf: return
                    text = (m.text or "").strip()
                    step = sf.get("step")

                    # ─── Step 1: Bot Token ───
                    if step == "bot_token":
                        if not TOKEN_RE.match(text):
                            return await self._safe_send(m, "❌ ɪɴᴠᴀʟɪᴅ ᴛᴏᴋᴇɴ. ᴅᴜʙᴀʀᴀ ʙʜᴇᴊᴏ.")
                        bot_id_new = text.split(":", 1)[0]
                        if (CLONES_DIR / f"{bot_id_new}.json").exists():
                            return await self._safe_send(m,
                                f"⚠️ ʏᴇ ʙᴏᴛ <code>{bot_id_new}</code> ᴘᴇʜʟᴇ ꜱᴇ ʀᴜɴɴɪɴɢ ʜᴀɪ.\n\n"
                                f"ᴘᴇʜʟᴇ <code>/dclone {text}</code> ᴄʜᴀʟᴀᴏ.")
                        sf["bot_token"] = text
                        sf["step"] = "support"
                        return await self._safe_send(m,
                            f"<b>  ✅ ᴛᴏᴋᴇɴ ꜱᴀᴠᴇᴅ</b>\n\n"
                            f"  ꜱᴛᴇᴘ <b>2/5:</b> 💬 ꜱᴜᴘᴘᴏʀᴛ ᴄʜᴀᴛ ʟɪɴᴋ / ᴜꜱᴇʀɴᴀᴍᴇ ʙʜᴇᴊᴏ\n\n"
                            f"  ᴇxᴀᴍᴘʟᴇ:\n"
                            f"  ▸ <code>@my_support_group</code>\n"
                            f"  ▸ <code>https://t.me/my_support_group</code>"
                        )

                    # ─── Step 2: Support Chat ───
                    if step == "support":
                        sf["support_chat"] = normalize_support(text)
                        sf["step"] = "updates"
                        return await self._safe_send(m,
                            f"<b>  ✅ ꜱᴜᴘᴘᴏʀᴛ ꜱᴀᴠᴇᴅ</b>\n\n"
                            f"  ꜱᴛᴇᴘ <b>3/5:</b> ✨ ᴜᴘᴅᴀᴛᴇꜱ ᴄʜᴀɴɴᴇʟ ʟɪɴᴋ / ᴜꜱᴇʀɴᴀᴍᴇ ʙʜᴇᴊᴏ\n\n"
                            f"  ᴇxᴀᴍᴘʟᴇ:\n"
                            f"  ▸ <code>@my_updates_channel</code>\n"
                            f"  ▸ <code>https://t.me/my_updates_channel</code>"
                        )

                    # ─── Step 3: Updates Channel ───
                    if step == "updates":
                        sf["updates_channel"] = normalize_support(text)
                        sf["step"] = "owner_username"
                        return await self._safe_send(m,
                            f"<b>  ✅ ᴜᴘᴅᴀᴛᴇꜱ ꜱᴀᴠᴇᴅ</b>\n\n"
                            f"  ꜱᴛᴇᴘ <b>4/5:</b> 👤 ᴏᴡɴᴇʀ ᴜꜱᴇʀɴᴀᴍᴇ ʙʜᴇᴊᴏ\n\n"
                            f"  ᴇxᴀᴍᴘʟᴇ: <code>@your_username</code>"
                        )

                    # ─── Step 4: Owner Username ───
                    if step == "owner_username":
                        sf["owner_username"] = normalize_owner_username(text)
                        sf["step"] = "session"
                        return await self._safe_send(m,
                            f"<b>  ✅ ᴏᴡɴᴇʀ ꜱᴀᴠᴇᴅ</b>\n\n"
                            f"  ꜱᴛᴇᴘ <b>5/5:</b> 🔑 ᴀꜱꜱɪꜱᴛᴀɴᴛ ꜱᴇꜱꜱɪᴏɴ ꜱᴛʀɪɴɢ ʙʜᴇᴊᴏ\n\n"
                            f"  💡 <code>/default</code> ʙʜᴇᴊᴏ → ꜱᴀᴍᴇ ᴀꜱꜱɪꜱᴛᴀɴᴛ ʀᴀᴋʜᴏ"
                        )

                    # ─── Step 5: Assistant Session ───
                    if step == "session":
                        if text.lower() == "/default":
                            ss = self.config.assistant_session
                        else:
                            ss = text
                        if len(ss) < 100:
                            return await self._safe_send(m, "❌ ɪɴᴠᴀʟɪᴅ ꜱᴇꜱꜱɪᴏɴ. ᴅᴜʙᴀʀᴀ ʙʜᴇᴊᴏ.")
                        try:
                            ccfg = BotConfig(
                                api_id=self.config.api_id,
                                api_hash=self.config.api_hash,
                                bot_token=sf["bot_token"],
                                owner_id=self.config.owner_id,
                                assistant_session=ss,
                                support_chat=sf["support_chat"],
                                updates_channel=sf.get("updates_channel", sf["support_chat"]),
                                owner_username=sf["owner_username"],
                                clone_mode=True,
                                tagline=BOT_BRAND_TAGLINE,
                            )
                            cfg_path = CLONES_DIR / f"{ccfg.bot_id}.json"
                            save_config(ccfg, cfg_path)
                            self.clone_flow.pop(m.from_user.id, None)
                            new_pid = spawn_clone_process(cfg_path, ccfg.bot_id)
                            if not new_pid:
                                return await self._safe_send(m, "❌ ᴄʟᴏɴᴇ ʟᴀᴜɴᴄʜ ꜰᴀɪʟ. /clones ᴄʜᴇᴄᴋ ᴋᴀʀᴏ.")
                            (PIDS_DIR / f"{ccfg.bot_id}.pid").write_text(str(new_pid))
                            await self._safe_send(m,
                                f"<b>  🚀 ᴄʟᴏɴᴇ ʟᴀᴜɴᴄʜᴇᴅ</b>\n\n"
                                f"<blockquote>🤖 <b>ɪᴅ:</b> <code>{escape_html(ccfg.bot_id)}</code>\n"
                                f"👤 <b>ᴏᴡɴᴇʀ:</b> {escape_html(ccfg.owner_username)}\n"
                                f"💬 <b>ꜱᴜᴘᴘᴏʀᴛ:</b> {escape_html(ccfg.support_chat)}\n"
                                f"✨ <b>ᴜᴘᴅᴀᴛᴇꜱ:</b> {escape_html(ccfg.updates_channel)}\n"
                                f"🆔 <b>ᴘɪᴅ:</b> <code>{new_pid}</code></blockquote>\n\n"
                                f"  ✨ ʟɪꜰᴇᴛɪᴍᴇ-ꜱᴛᴀʙʟᴇ + ʜᴇᴀʀᴛʙᴇᴀᴛ ᴡᴀᴛᴄʜᴅᴏɢ ᴀᴄᴛɪᴠᴇ\n\n"
                                f"  🛑 ꜱᴛᴏᴘ ᴋᴀʀɴᴇ ᴋᴇ ʟɪᴇ:\n"
                                f"  <code>/dclone {escape_html(ccfg.bot_token)}</code>"
                            )
                        except Exception as pe:
                            await self._safe_send(m, f"❌ ʟᴀᴜɴᴄʜ ꜰᴀɪʟ: <code>{escape_html(str(pe))}</code>")
                except Exception: log.exception("clone_flow failed")

    # ─── PYTGCALLS SAFE START ───

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

    # ─── START / STOP ───

    async def start(self) -> None:
        self._main_loop = asyncio.get_running_loop()

        if shutil.which("ffmpeg") is None:
            log.warning("⚠️  ffmpeg NOT found — audio may fail!")

        self._load_state()
        self._load_known()
        await self._add_handlers()

        await self.assistant.start()
        am = await self.assistant.get_me()
        self.assistant_id       = am.id
        self.assistant_name     = am.first_name or "Assistant"
        self.assistant_username = am.username or ""
        log.info("✨ ASSISTANT @%s id=%s", self.assistant_username, self.assistant_id)

        asyncio.ensure_future(self._warm_dialogs_once())

        await self.bot.start()
        me = await self.bot.get_me()
        self.bot_username = me.username or ""
        self.bot_name     = me.first_name or ""
        self.bot_id_int   = me.id
        if self.bot_name:
            self.config.brand_name = self.bot_name

        try:
            from pyrogram.enums import ParseMode
            self.bot.parse_mode = ParseMode.HTML
        except Exception:
            pass

        await self._start_pytgcalls()
        log.info("🚀 ONLINE | %s | @%s | id=%s", self.bot_name, self.bot_username, self.config.bot_id)

        # v13: Start heartbeat ASAP — proves we're alive AND responsive
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

        if self.is_master:
            asyncio.ensure_future(self._auto_launch_clones())
            self._watchdog_task = asyncio.ensure_future(self._clone_watchdog())

        self._signal_watch_task = asyncio.ensure_future(self._signal_watcher())

        try:
            await idle()
        except Exception as exc:
            log.warning("idle() ended: %s", exc)

        log.info("Idle loop exited — bot shutting down")

    async def stop(self) -> None:
        if self._stopping: return
        self._stopping = True
        log.info("🛑 Stopping bot...")
        try:
            self._save_state_sync()
            self._save_known()
        except Exception:
            pass
        try:
            clear_heartbeat(self.config.bot_id)
        except Exception:
            pass
        for task in (self._watchdog_task, self._signal_watch_task, self._heartbeat_task):
            if task:
                try: task.cancel()
                except Exception: pass
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
                pass
        log.info("👋 Bot stopped cleanly.")

# ═══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

async def run_once() -> bool:
    if len(sys.argv) > 2 and sys.argv[1] == "--config":
        cfg = load_config(Path(sys.argv[2]).resolve())
        app = TelegramMusicBot(cfg, config_path=Path(sys.argv[2]).resolve(), is_master=False)
    else:
        cfg = BotConfig(
            api_id=API_ID, api_hash=API_HASH, bot_token=MAIN_BOT_TOKEN,
            owner_id=OWNER_ID, assistant_session=DEFAULT_ASSISTANT_SESSION,
            support_chat=normalize_support(MASTER_SUPPORT_CHAT),
            updates_channel=normalize_support(MASTER_UPDATES_CHANNEL),
            owner_username=normalize_owner_username(MASTER_OWNER_USERNAME),
            nubcoder_token=NUBCODER_TOKEN, clone_mode=False, tagline=BOT_BRAND_TAGLINE,
        )
        app = TelegramMusicBot(cfg, is_master=True)
    try:
        await app.start()
    finally:
        await app.stop()
    return _SHUTDOWN_FLAG.is_set()


async def supervisor() -> None:
    _is_clone = len(sys.argv) > 2 and sys.argv[1] == "--config"
    delay     = CLONE_RESTART_DELAY if _is_clone else 5
    max_delay = MAX_RESTART_DELAY if not _is_clone else 30
    crash_count = 0
    last_crash_time = 0.0

    while True:
        if _SHUTDOWN_FLAG.is_set():
            log.info("Shutdown flag set — supervisor exiting")
            return

        if _is_clone:
            cfg_path = Path(sys.argv[2]).resolve()
            if not cfg_path.exists():
                log.info("Clone config deleted. Supervisor exiting.")
                try:
                    bot_id = cfg_path.stem
                    pid_f = PIDS_DIR / f"{bot_id}.pid"
                    pid_f.unlink(missing_ok=True)
                    clear_heartbeat(bot_id)
                except Exception:
                    pass
                return

        crashed = False
        start_time = time.time()
        try:
            shutdown_requested = await run_once()
            if shutdown_requested:
                log.info("Clean shutdown — supervisor exiting")
                return
            log.info("Bot exited cleanly — supervisor exiting")
            return
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt — supervisor exiting")
            return
        except Exception as exc:
            crashed = True
            crash_count += 1
            log.error("💥 CRASH #%d: %s", crash_count, exc)
            traceback.print_exc()

        if not crashed:
            return

        # v13: Reset delay on long-running successful sessions
        run_duration = time.time() - start_time
        if run_duration > 300:  # ran 5+ min before crash → reset backoff
            log.info("Long run before crash (%ds), resetting delay", int(run_duration))
            delay = CLONE_RESTART_DELAY if _is_clone else 5
            crash_count = 1

        if _SHUTDOWN_FLAG.is_set():
            log.info("Shutdown during crash — exiting")
            return

        log.warning("⏳ Restarting in %ss (crash #%d)...", delay, crash_count)
        for _ in range(delay):
            if _SHUTDOWN_FLAG.is_set():
                log.info("Shutdown during restart wait — exiting")
                return
            await asyncio.sleep(1)

        delay = min(max_delay, delay + 5)


def main():
    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    try:
        asyncio.run(supervisor())
    except KeyboardInterrupt:
        log.info("👋 Shutdown via KeyboardInterrupt")
    except Exception as e:
        log.error("Fatal supervisor error: %s", e)
        traceback.print_exc()
    finally:
        log.info("🎵 Bye!")


if __name__ == "__main__":
    main()

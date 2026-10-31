"""配置与数据文件读写（config / messages / secrets，全部位于 data/）。"""

from __future__ import annotations

import copy
import json
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
REPORTS_DIR = ROOT / "reports"
CONFIG_PATH = DATA_DIR / "config.json"
MESSAGES_PATH = DATA_DIR / "messages.json"
MESSAGES_PENDING_PATH = DATA_DIR / "messages_pending.json"
SECRETS_PATH = DATA_DIR / "secrets.json"
STATE_PATH = DATA_DIR / "state.sqlite3"
DEBUG_ARTIFACTS_DIR = DATA_DIR / "debug_artifacts"
PROFILE_DIR = DATA_DIR / "profiles"

DEFAULT_CONFIG = {
    "version": 2,
    "schedule": {
        "windowStart": "08:00",
        "windowEnd": "21:30",
        "randomStart": True,
        "catchUpStart": "20:30",
        "maxSlotAttempts": 3,
        "retryDelayMinutes": 10,
        "slots": [
            {"id": "morning", "label": "晨间", "start": "08:00", "end": "09:30"},
            {"id": "forenoon", "label": "上午", "start": "09:30", "end": "12:00"},
            {"id": "noon", "label": "午间", "start": "12:00", "end": "14:00"},
            {"id": "afternoon", "label": "下午", "start": "14:00", "end": "18:00"},
            {"id": "evening", "label": "晚间", "start": "18:00", "end": "21:30"},
        ],
    },
    "send": {
        "minIntervalSec": 30,
        "maxIntervalSec": 120,
        "retryPerFriendPerDay": 2,
        "shuffleOrder": True,
    },
    "friendListScan": {
        "maxScanSeconds": 600,
        "idleScanSeconds": 45,
        "scrollStepPx": 400,
        "scrollDelaySeconds": 1.5,
        "maxRecoveries": 2,
    },
    "risk": {
        "autoPauseOnConsecutiveBadDays": 2,
        "badDayRatioThreshold": 0.5,
        "paused": False,
    },
    "messages": {
        "denyWords": ["微信", "QQ", "vx", "VX", "加微信", "http"],
        "maxLength": 120,
        "lightVariation": False,
    },
    "network": {"mode": "direct", "proxyUrl": "http://127.0.0.1:7890"},
    "debug": {"saveArtifacts": True},
    "ui": {"port": 8899, "theme": "system"},
}


def _migrate_config(raw, merged):
    """一次性迁移旧默认值；返回需要落盘的新配置，否则 None。"""
    changed = False
    # v1 → v2：时段模型、间隔语义、微信从用户禁用词移除（用户明确要求照发）
    raw = raw if isinstance(raw, dict) else {}
    version = int(raw.get("version") or 1)
    if version < 2:
        if merged["send"]["minIntervalSec"] >= 300 and merged["send"]["maxIntervalSec"] >= 1000:
            merged["send"]["minIntervalSec"] = 30
            merged["send"]["maxIntervalSec"] = 120
        deny = [word for word in merged["messages"].get("denyWords", []) if word != "微信"]
        merged["messages"]["denyWords"] = deny
        merged["version"] = 2
        changed = True
    if merged["schedule"]["windowStart"] == "18:00":
        merged["schedule"]["windowStart"] = "08:00"
        changed = True
    if merged["schedule"]["windowEnd"] == "22:00":
        merged["schedule"]["windowEnd"] = "21:30"
        changed = True
    return merged if changed else None


def _deep_merge(base, override):
    merged = copy.deepcopy(base)
    if not isinstance(override, dict):
        return merged
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _read_json(path, default):
    if not path.exists():
        return copy.deepcopy(default)
    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return copy.deepcopy(default)
    if isinstance(default, dict) and isinstance(data, dict):
        return _deep_merge(default, data)
    return data if data is not None else copy.deepcopy(default)


def _write_json(path, data, private=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            try:
                os.unlink(temp_name)
            except OSError:
                pass
    if private:
        os.chmod(path, 0o600)


def load_config():
    if CONFIG_PATH.exists():
        try:
            raw_data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            raw_data = {}
        merged = _deep_merge(DEFAULT_CONFIG, raw_data) if isinstance(raw_data, dict) else copy.deepcopy(DEFAULT_CONFIG)
        migrated = _migrate_config(raw_data, merged)
        if migrated is not None:
            _write_json(CONFIG_PATH, migrated)
            return migrated
        return merged
    return copy.deepcopy(DEFAULT_CONFIG)


def save_config(data):
    merged = _deep_merge(DEFAULT_CONFIG, data or {})
    _write_json(CONFIG_PATH, merged)
    return merged


def load_messages():
    data = _read_json(MESSAGES_PATH, {"items": []})
    return {"items": _normalize_message_items(data.get("items") if isinstance(data, dict) else [])}


VALID_WHEN_VALUES = {"any", "morning", "forenoon", "noon", "afternoon", "evening"}


def _normalize_message_items(items):
    """把字符串或对象条目统一成 ``{"text", "when"}`` 列表。"""
    normalized = []
    for item in items:
        if isinstance(item, dict):
            text = str(item.get("text") or "").strip()
            when = str(item.get("when") or "any")
        else:
            text = str(item or "").strip()
            when = "any"
        if not text:
            continue
        if when not in VALID_WHEN_VALUES:
            when = "any"
        normalized.append({"text": text, "when": when})
    return normalized


def save_messages(data):
    payload = {"items": _normalize_message_items((data or {}).get("items", []))}
    _write_json(MESSAGES_PATH, payload)
    return payload


def load_pending_messages():
    data = _read_json(MESSAGES_PENDING_PATH, {"items": []})
    return {
        "items": _normalize_message_items(
            data.get("items") if isinstance(data, dict) else []
        )
    }


def save_pending_messages(data):
    payload = {"items": _normalize_message_items((data or {}).get("items", []))}
    _write_json(MESSAGES_PENDING_PATH, payload)
    return payload


def load_secrets():
    return _read_json(
        SECRETS_PATH,
        {"username": "", "unique_id": "", "cookies": []},
    )


def save_secrets(data):
    payload = {
        "username": str((data or {}).get("username") or ""),
        "unique_id": str((data or {}).get("unique_id") or ""),
        "cookies": list((data or {}).get("cookies") or []),
    }
    _write_json(SECRETS_PATH, payload, private=True)
    return payload

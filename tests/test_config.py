import os
import stat
import json

from spark import config


def test_load_config_returns_defaults_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / "nope.json")
    loaded = config.load_config()
    assert loaded["send"]["minIntervalSec"] == 30
    assert loaded["schedule"]["windowStart"] == "08:00"
    assert loaded["schedule"]["windowEnd"] == "21:30"
    assert [slot["id"] for slot in loaded["schedule"]["slots"]] == [
        "morning",
        "forenoon",
        "noon",
        "afternoon",
        "evening",
    ]
    assert loaded["version"] == 2
    assert loaded["ui"]["theme"] == "system"


def test_save_and_load_config_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / "config.json")
    config.save_config({"send": {"minIntervalSec": 45}})
    loaded = config.load_config()
    assert loaded["send"]["minIntervalSec"] == 45
    assert loaded["send"]["maxIntervalSec"] == 120


def test_save_secrets_is_private_and_roundtrips(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SECRETS_PATH", tmp_path / "secrets.json")
    config.save_secrets({"username": "test_user", "cookies": [{"name": "sessionid", "value": "x"}]})
    mode = stat.S_IMODE(os.stat(tmp_path / "secrets.json").st_mode)
    assert mode == 0o600
    loaded = config.load_secrets()
    assert loaded["username"] == "test_user"
    assert len(loaded["cookies"]) == 1


def test_messages_roundtrip_drops_blank_items(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "MESSAGES_PATH", tmp_path / "messages.json")
    config.save_messages({"items": ["  早上好  ", "", "晚安"]})
    assert config.load_messages()["items"] == [
        {"text": "早上好", "when": "any"},
        {"text": "晚安", "when": "any"},
    ]


def test_messages_load_legacy_strings_as_any(tmp_path, monkeypatch):
    path = tmp_path / "messages.json"
    path.write_text('{"items": ["旧文案", "另一条"]}', encoding="utf-8")
    monkeypatch.setattr(config, "MESSAGES_PATH", path)
    items = config.load_messages()["items"]
    assert items == [
        {"text": "旧文案", "when": "any"},
        {"text": "另一条", "when": "any"},
    ]


def test_messages_roundtrip_keeps_tags(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "MESSAGES_PATH", tmp_path / "messages.json")
    config.save_messages(
        {"items": [{"text": "晚安啦", "when": "evening"}, {"text": "随便", "when": "any"}]}
    )
    assert config.load_messages()["items"][0]["when"] == "evening"


def test_load_config_migrates_old_window_values(tmp_path, monkeypatch):
    path = tmp_path / "config.json"
    path.write_text(
        '{"schedule": {"windowStart": "18:00", "windowEnd": "22:00"}}',
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "CONFIG_PATH", path)
    loaded = config.load_config()
    assert loaded["schedule"]["windowStart"] == "08:00"
    assert loaded["schedule"]["windowEnd"] == "21:30"
    assert loaded["version"] == 2
    assert len(loaded["schedule"]["slots"]) == 5
    # 迁移要落盘：再次加载仍然保持新值
    assert config.load_config()["schedule"]["windowStart"] == "08:00"


def test_config_v2_migration_resets_intervals_and_wechat(tmp_path, monkeypatch):
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "schedule": {"windowStart": "08:00", "windowEnd": "21:30"},
                "send": {"minIntervalSec": 300, "maxIntervalSec": 3000},
                "messages": {"denyWords": ["微信", "QQ"]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "CONFIG_PATH", path)
    loaded = config.load_config()
    assert loaded["send"]["minIntervalSec"] == 30
    assert loaded["send"]["maxIntervalSec"] == 120
    assert "微信" not in loaded["messages"]["denyWords"]
    assert "QQ" in loaded["messages"]["denyWords"]


def test_pending_pool_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "MESSAGES_PENDING_PATH", tmp_path / "pending.json")
    config.save_pending_messages({"items": [{"text": "候选一", "when": "evening"}]})
    loaded = config.load_pending_messages()
    assert loaded["items"] == [{"text": "候选一", "when": "evening"}]

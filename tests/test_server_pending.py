import asyncio

from spark import config


def test_pending_accept_moves_item_into_pool(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "MESSAGES_PENDING_PATH", tmp_path / "pending.json")
    monkeypatch.setattr(config, "MESSAGES_PATH", tmp_path / "messages.json")
    config.save_pending_messages({"items": [{"text": "候选文案", "when": "evening"}]})
    config.save_messages({"items": [{"text": "现有文案", "when": "any"}]})

    from spark.server import MessagePayload, accept_pending, delete_pending

    result = asyncio.run(
        accept_pending(MessagePayload(text="候选文案", when="evening"))
    )
    assert result["pending"] == 0
    assert {"text": "候选文案", "when": "evening"} in config.load_messages()["items"]

    # 删除候选不影响主池
    config.save_pending_messages({"items": [{"text": "另一条", "when": "any"}]})
    asyncio.run(delete_pending(MessagePayload(text="另一条")))
    assert config.load_pending_messages()["items"] == []
    assert len(config.load_messages()["items"]) == 2

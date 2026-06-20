from __future__ import annotations

import pytest

from sementic.intent_classifier import TaskIntentClassifier
from sementic.llm import MockIntentLLMClient
from sementic.models import ChatMessage


@pytest.mark.asyncio
async def test_intent_classifier_rejects_chitchat() -> None:
    classifier = TaskIntentClassifier(llm=MockIntentLLMClient())
    decision = await classifier.classify(
        channel_id="room_1",
        sender_display_name="刘颂",
        recent_messages=[],
        current_message="哈哈哈，我是刘颂啊",
    )
    assert decision.needs_task is False


@pytest.mark.asyncio
async def test_intent_classifier_accepts_actionable_message() -> None:
    classifier = TaskIntentClassifier(llm=MockIntentLLMClient())
    decision = await classifier.classify(
        channel_id="room_1",
        sender_display_name="Hassan",
        recent_messages=[
            ChatMessage(sender="Hassan", text="刚才脚本报错了"),
        ],
        current_message="@项目助手 重启刚才报错的脚本",
    )
    assert decision.needs_task is True

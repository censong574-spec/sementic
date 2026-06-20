# sementic

LLM collaboration planner core for multi-agent orchestration.

## Install

```bash
pip install -e ".[dev]"
```

## Quick start

Mock mode (no API key):

```bash
python -m sementic.cli --mock
```

Real LLM with Alibaba Cloud DashScope:

```bash
copy .env.example .env
# Edit .env and paste your DashScope API key into SEMENTIC_LLM_API_KEY
python -m sementic.cli --provider aliyun -v
```

`.env` example:

```env
SEMENTIC_LLM_PROVIDER=aliyun
SEMENTIC_LLM_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1
SEMENTIC_LLM_API_KEY=sk-your-dashscope-token
SEMENTIC_LLM_MODEL=qwen-plus
```

You can also use `DASHSCOPE_API_KEY` instead of `SEMENTIC_LLM_API_KEY`.

## Python API

```python
import asyncio
from sementic import Planner, PlannerRequest, BotProfile, ChatMessage

async def main():
    planner = Planner()  # MockLLMClient by default
    request = PlannerRequest(
        channel_id="room_99",
        sender_user_id="user_hassan",
        sender_display_name="Hassan",
        recent_messages=[ChatMessage(sender="Hassan", text="刚才脚本报错了")],
        mentioned_bot_ids=["bot_project_assistant"],
        available_bots=[
            BotProfile(
                bot_user_id="bot_project_assistant",
                display_name="项目助手",
                role="DevOps assistant",
                expertise=["restart_cli"],
                owner_user_id="user_hassan",
                share_scope="channel_shared",
            )
        ],
        current_message="@项目助手，重启刚才报错的脚本。",
    )
    plan = await planner.plan(request)
    print(plan.model_dump_json(indent=2))

asyncio.run(main())
```

## Gateway Service

The outer semantic gateway is a separate stateless microservice:

```bash
python -m sementic.gateway.server
```

Default listen address: `http://0.0.0.0:8080`

IM should call the gateway first:

```http
POST /api/v1/im/messages
Content-Type: application/json
```

Gateway flow:

1. Layer-1 static noise filter (`哈哈`, `1111`, pure punctuation, etc.)
2. Redis circuit breaker for offline mentioned bots
3. Append accepted messages to Redis history window
4. Publish to Kafka with partition key = `group_session_id`

Noise or offline-bot requests return HTTP 200 but are not queued to Kafka.

## Intent Worker API

The planner service remains available separately for direct testing:

```bash
python -m sementic.server
```

Default listen address: `http://0.0.0.0:8000`

In production, the worker should consume from Kafka instead of exposing the same ingress directly.

IM webhook endpoint:

```http
POST /api/v1/im/messages
Content-Type: application/json
```

Example payload:

```json
{
  "event_id": "evt_10923841029384",
  "group_session_id": "room_dev_ecommerce_001",
  "user_context": {
    "user_id": "usr_hassan_95",
    "username": "Hassan",
    "is_bot": false,
    "ownership": "OTHERS"
  },
  "message_context": {
    "msg_id": "post_7a8b9c1d",
    "parent_msg_id": "post_7a8b9c1d",
    "content": "@Jira-Helper 挂个单，@Bug-Hunter 顺便准备查下日志。",
    "mentions_registry": [
      {"entity_id": "bot_jira_123", "ownership": "OTHERS"},
      {"entity_id": "bot_bug_hunter_01", "ownership": "MY_SYSTEM"}
    ]
  }
}
```

Flow:

1. Append message to Redis list for the group session
2. Keep only the latest 20 messages
3. Read the latest 10 messages as planner context
4. Call Aliyun LLM and return the execution plan JSON

## Remote deploy

Deploy scripts live under `scripts/deploy/`. See [scripts/deploy/DEPLOY.md](scripts/deploy/DEPLOY.md).

```powershell
.\scripts\deploy\deploy.ps1 full
.\scripts\deploy\deploy.ps1 diagnose
```

## Test

```bash
python -m pytest -q
```

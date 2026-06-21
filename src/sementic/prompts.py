from __future__ import annotations

from sementic.models import BotProfile, ChatMessage
from sementic.task_graph import TaskGraphPlan

# 输出外壳不变：TaskGraphPlan = confidence + reply_to_user + graph(control_flow)
_TASK_GRAPH_OUTPUT_TEMPLATE = """{
  "confidence": 0.85,
  "reply_to_user": "一句话告诉用户将要做什么",
  "graph": {
    "id": "im-session-id",
    "name": "任务名",
    "graph_type": "control_flow",
    "input": {"channel_id": "频道ID", "user_message": "用户最新消息", "workspace_id": "可选", "multica_token": "可选"},
    "start": "intake",
    "max_total_visits": 10,
    "nodes": [
      {"id": "intake", "operation": "emit", "params": {"user_message": "$input.user_message", "channel_id": "$input.channel_id"}},
      {
        "id": "run_task",
        "operation": "agent_task",
        "deps": ["intake"],
        "params": {},
        "agent": {
          "backend": "multica",
          "agent_key": "bot_user_id",
          "context_policy": "provided_context_only",
          "runtime_profile": "maos_compact_agent",
          "execution_mode": "multica",
          "status": "in_progress",
          "poll_seconds": 30,
          "prompt": "只基于A2A payload完成本节点任务；说明读哪些deps、产出什么；完成后提交评论并更新状态为 done 或 in_review"
        },
        "timeout_seconds": 7200
      }
    ],
    "edges": [{"from": "intake", "to": "run_task"}]
  }
}"""

_GRAPH_RULES = """1. graph_type 固定 control_flow，显式写 edges；node.id 用 snake_case
2. agent_task：agent.agent_key 必须是 Available bots 的 bot_user_id；节点指令只写在 agent.prompt，禁止写在 params
3. agent_task 的 params 固定为 {}，不要把 prompt/自然语言塞进 params
4. agent 默认：backend=multica, context_policy=provided_context_only, runtime_profile=maos_compact_agent, execution_mode=multica, poll_seconds=30, timeout_seconds=7200
5. emit/join/status 为 Simulator；params 必须是 JSON 对象，禁止字符串
6. 读上游输出须在 deps 声明；join 须在 params.fields 列出字段
7. 条件分支写 edges.when（如 last.decision == 'approved'）
8. 有环：设 max_total_visits、循环节点 max_visits、回边 join:any
9. 输出完整合法 JSON，nodes/edges 不要被截断"""

_CLARIFY_VS_EXECUTE = """澄清图仅当：目标不明、缺关键参数且无法合理默认、无法匹配任何 bot。
以下情况直接 agent_task，不要澄清：用户已给出动作+对象（如给出代码/查询/部署）；已点名技术栈（nginx/redis/python）；Recent messages 已补足指代。"""

_BOT_MATCHING = """按 role/expertise 匹配；@提到的 bot 若在 Available bots 中可优先，但仍需能力匹配。
- 写代码/脚本/示例/给出代码 → 代码类 bot
- 部署/nginx/redis/运维/配置 → 运维类 bot
- 同时涉及「实现代码」与「部署/配置」→ 并行：代码 bot + 运维 bot → join 汇总
- 简单单点查询/单技能 → 单 bot 线性即可"""

_PLANNING_CHECKLIST = """自检：①目标与交付物明确吗 ②是否误用澄清图 ③agent.prompt 是否在 agent 内、params 是否为 {} ④bot/并行/线性选对 ⑤JSON 完整可解析"""


SYSTEM_PROMPT = f"""你是 IM 多机器人 Planner（只规划、不执行）。根据对话和 Available bots 输出可运行的 control_flow 任务图（LangGraph 风格）。下游 runtime 执行 graph；不要编造 bot 结果。

CAN：读对话与 bot 列表；拆成 nodes+edges；为 agent_task 指定 agent_key 与 agent.prompt；写 reply_to_user。
CANNOT：用列表外 bot；代替 bot 执行；编造结果；输出非 JSON；把指令写入 params。

规划顺序：复述目标与交付物 → 判断澄清或执行 → 按能力选 bot（可并行）→ 选图形态 → 填 nodes/edges/deps → 写 agent.prompt → 给 confidence 与 reply_to_user。

{_CLARIFY_VS_EXECUTE}

{_BOT_MATCHING}

图形态：
- 线性：intake(emit) → agent_task → 可选 join 汇总
- 并行：intake → 多个 agent_task → join 汇总（代码+运维、多技能分工时用）
- 评审环：plan → review_gate；when needs_revision 回 plan，approved 进 join；设 max_visits
- 澄清：仅极少量信息缺失时用 intake → join(clarify)

agent.prompt 须含：只基于 A2A payload、读哪些 deps、本节点动作与产出、完成后提交评论并更新状态。评审门末尾输出 JSON：{{"decision":"needs_revision|approved","reason":"...","required_changes":[],"confidence":0.0}}

图规则：
{_GRAPH_RULES}

固定输出结构（照此填空，nodes/edges 按任务裁剪）：
{_TASK_GRAPH_OUTPUT_TEMPLATE}
"""


def build_user_prompt(
    *,
    channel_id: str,
    sender_user_id: str,
    sender_display_name: str,
    recent_messages: list[ChatMessage],
    available_bots: list[BotProfile],
    mentioned_bot_ids: list[str],
    current_message: str,
    workspace_id: str | None = None,
    multica_token: str | None = None,
) -> str:
    history = "\n".join(msg.format_line() for msg in recent_messages) or "(no prior messages)"
    bots = "\n".join(bot.format_profile() for bot in available_bots)
    mentioned = ", ".join(mentioned_bot_ids) if mentioned_bot_ids else "(none)"
    workspace_line = workspace_id or "(none)"
    multica_line = "present" if multica_token else "missing"

    return (
        f"Channel ID: {channel_id}\n"
        f"Sender: {sender_display_name} ({sender_user_id})\n"
        f"Workspace ID: {workspace_line}\n"
        f"Multica token: {multica_line}\n"
        f"Mentioned bot IDs: {mentioned}\n\n"
        f"Recent messages:\n{history}\n\n"
        f"Available bots:\n{bots}\n\n"
        f"Latest user message:\n{current_message}\n\n"
        f"{_PLANNING_CHECKLIST}"
    )


def build_repair_prompt(
    *,
    user_prompt: str,
    previous_output: str,
    error: str,
) -> str:
    return (
        f"{user_prompt}\n\n"
        f"校验失败：{error}\n"
        f"上次输出：\n{previous_output}\n\n"
        "修正 graph 结构，不改变用户目标。agent_task 指令放在 agent.prompt，params 保持 {}。只输出完整 TaskGraphPlan JSON。"
    )


def task_graph_plan_json_schema() -> dict:
    return TaskGraphPlan.model_json_schema()


INTENT_SYSTEM_PROMPT = """判断最新消息是否需要 bot 执行任务。只输出 JSON：

{
  "needs_task": true,
  "confidence": 0.9,
  "reason": "一句中文"
}
"""


def build_intent_user_prompt(
    *,
    channel_id: str,
    sender_display_name: str,
    recent_messages: list[ChatMessage],
    current_message: str,
) -> str:
    history = "\n".join(msg.format_line() for msg in recent_messages) or "(no prior messages)"
    return (
        f"Channel ID: {channel_id}\n"
        f"Speaker: {sender_display_name}\n\n"
        f"Recent messages:\n{history}\n\n"
        f"Latest message:\n{current_message}"
    )

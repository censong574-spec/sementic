# 任务图 JSON 格式说明

本文档描述当前持久化多 Agent 编排内核支持的任务图 JSON 格式。其他 Agent 可以根据本文档生成可被 Web 界面和 sandbox API 加载执行的任务图文件。

目标读者：负责生成任务图 JSON 的 Agent。

适用范围：`graph_type: "control_flow"` 控制流图，以及未显式声明 `edges` 的传统 DAG 图。

## 总体结构

一个任务图 JSON 是一个对象，至少需要包含：

```json
{
  "id": "unique-graph-id",
  "name": "Human readable graph name",
  "graph_type": "control_flow",
  "input": {},
  "start": "start_node_id",
  "max_total_visits": 20,
  "nodes": [],
  "edges": []
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `id` | string | 是 | 图的稳定 ID。建议只使用小写字母、数字、短横线、下划线。 |
| `name` | string | 否 | 展示名称。可使用中文。 |
| `graph_type` | string | 否 | 推荐填写 `control_flow`。如果不填且有 `edges`，系统会按控制流图处理。 |
| `input` | object | 否 | 图级输入。节点可以用 `$input.xxx` 引用。 |
| `start` | string 或 string[] | 否 | 起始节点。若不填，系统会从无入边节点推断；如果图有环，建议显式填写。 |
| `start_nodes` | string[] | 否 | 多起点写法，和 `start` 二选一即可。 |
| `default_join` | string | 否 | 默认入边汇合策略，常用 `all` 或 `any`。显式 `edges` 图默认是 `any`。 |
| `max_total_visits` | number | 否 | 整张图最多节点访问次数，用于防止无限循环。建议有循环时填写。 |
| `nodes` | array | 是 | 节点列表。每个节点必须有唯一 `id`。 |
| `edges` | array | 控制流图建议必填 | 控制流边。支持普通边、条件边、回环边。 |

## 节点格式

通用节点结构：

```json
{
  "id": "node_id",
  "label": "展示名称",
  "type": "agent",
  "operation": "agent_task",
  "deps": ["upstream_node_id"],
  "join": "any",
  "max_visits": 3,
  "params": {},
  "simulate": {
    "min_seconds": 2,
    "max_seconds": 5
  },
  "agent": {},
  "timeout_seconds": 7200
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `id` | string | 是 | 节点 ID，必须唯一。建议使用英文 snake_case。 |
| `label` | string | 否 | Web 图上的展示名称，可使用中文。 |
| `type` | string | 否 | 普通执行节点可省略或填 `agent`；条件节点填 `condition`、`decision`、`router` 或 `branch`。 |
| `operation` | string | 否 | 执行动作。真实 Agent 节点用 `agent_task`；Simulator 节点见下文。 |
| `deps` | string[] | 否 | 需要注入到该节点 A2A 上下文中的历史结果。即使使用显式 `edges`，也建议为需要读取上游结果的节点填写。 |
| `join` | string | 否 | 多入边何时触发。`all` 表示所有前置入边到达才执行；`any` 表示任一入边到达就执行。循环节点通常用 `any`。 |
| `max_visits` | number | 否 | 单个节点最多运行次数。循环中的节点必须设置合理上限，例如 `3`。 |
| `params` | object | 否 | Simulator 或 condition 节点使用的参数。 |
| `simulate` | object | 否 | Simulator 执行耗时范围。真实 Agent 节点一般不需要。 |
| `agent` | object | 否 | 真实 Multica Agent 配置。存在该字段且 `backend` 为 `multica` 时，会创建真实 Agent 任务。 |
| `timeout_seconds` | number | 否 | 节点最长等待时间。真实 Agent 节点建议设置 `7200` 或更高。 |

## 边格式

控制流图使用 `edges` 显式描述节点流转：

```json
{
  "from": "source_node_id",
  "to": "target_node_id",
  "when": "last.decision == 'approved'",
  "label": "批准后继续",
  "kind": "control"
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `from` | string | 是 | 起点节点 ID。 |
| `to` | string | 是 | 目标节点 ID。 |
| `when` | string | 否 | 条件表达式。为空表示无条件流转。 |
| `label` | string | 否 | Web 图上显示的边标签。 |
| `kind` | string | 否 | 目前通常填 `control` 或省略。 |

### 普通边

```json
{
  "from": "requirements",
  "to": "plan",
  "label": "生成方案"
}
```

### 条件分支边

同一个节点可以有多条带 `when` 的条件边：

```json
{
  "from": "review_gate",
  "to": "revise_plan",
  "when": "last.decision == 'needs_revision'",
  "label": "需要修改"
}
```

```json
{
  "from": "review_gate",
  "to": "final_package",
  "when": "last.decision == 'approved'",
  "label": "评审通过"
}
```

如果一个节点完成后所有条件边都不匹配，workflow 会失败。因此真实评审门 Agent 的输出必须稳定包含分支字段，例如 `decision`。

### 循环边

循环就是一条指向上游节点的边：

```json
{
  "from": "review_gate",
  "to": "plan",
  "when": "last.decision == 'needs_revision'",
  "label": "带评审意见回到方案节点"
}
```

循环图必须注意：

- 给循环中的节点设置 `max_visits`。
- 给整张图设置 `max_total_visits`。
- 回到上游的节点一般设置 `join: "any"`。
- 回到上游的节点应把评审门节点写入 `deps`，这样下次运行时能读取上轮评审意见。

示例：

```json
{
  "id": "plan",
  "label": "真实Agent 方案",
  "operation": "agent_task",
  "join": "any",
  "max_visits": 3,
  "deps": ["requirements", "review_gate"],
  "agent": {
    "backend": "multica",
    "agent_key": "architect",
    "context_policy": "provided_context_only",
    "runtime_profile": "maos_compact_agent",
    "execution_mode": "multica",
    "status": "in_progress",
    "priority": "high",
    "poll_seconds": 30,
    "prompt": "请只基于A2A payload生成方案。若看到review_gate中的needs_revision评审意见，请合并其reason和required_changes重新生成。完成后把结果作为Multica评论提交，并将任务状态更新为in_review或done。"
  },
  "timeout_seconds": 7200
}
```

## 条件表达式

`edge.when` 使用安全表达式求值，常用上下文包括：

| 名称 | 含义 |
| --- | --- |
| `last` | 当前 `from` 节点本次运行输出 payload。最常用于分支。 |
| `result` | 同 `last`。 |
| `results` | 所有已完成节点和实例结果。包含 `node_id` 和 `node_id#visit` 两类 key。 |
| `deps` | 同 `results`。 |
| `input` | 图级 `input`。 |
| `visits` | 每个节点当前访问次数。 |
| `attempts` | 同 `visits`。 |
| `node.id` | 当前出边源节点 ID。 |
| `node.visit` | 当前出边源节点访问次数。 |

支持的表达式能力：

- 比较：`==`, `!=`, `<`, `<=`, `>`, `>=`
- 成员判断：`in`, `not in`
- 布尔逻辑：`and`, `or`, `not`
- 常量：字符串、数字、`true/false/null`
- 简单函数：`len`, `int`, `float`, `str`, `bool`, `min`, `max`
- 属性访问：`last.decision`, `visits.review_gate`
- 下标访问：`last.required_changes[0]`

常用条件示例：

```text
last.decision == 'approved'
last.decision == 'needs_revision'
visits.confidence_gate < 2
len(last.required_changes) > 0
last.confidence >= 0.8 and last.decision == 'approved'
```

不要在条件表达式里使用任意 Python 代码、导入、复杂函数或副作用。

## 数据引用和 A2A 上下文

节点执行时会收到上游结果作为 A2A Context Payload。可通过两种方式引用数据：

1. 在 Simulator 节点 `params` 中使用路径引用。
2. 在真实 Agent 节点 prompt 中说明它应该读取 A2A payload 中的依赖节点输出。

路径引用格式：

| 引用 | 含义 |
| --- | --- |
| `$input.project` | 图级输入里的 `project` 字段。 |
| `$deps.node_id.status` | 某个节点最新输出 payload 的 `status` 字段。 |
| `$deps.node_id.latest_comment` | 真实 Agent 节点最终评论摘要。 |
| `$deps.review_gate.decision` | 评审门 Agent 输出的结构化决策。 |
| `$deps.review_gate.required_changes` | 评审门 Agent 输出的修改项列表。 |

注意：

- `deps` 是该节点声明的依赖结果，以及触发该节点的入边来源。
- 对循环节点，`deps.review_gate` 表示该评审门最近一次完成的输出。
- 系统也保存实例级结果，例如 `review_gate#1`、`review_gate#2`，用于 UI 查看历史轮次；任务图生成时通常使用节点级最新结果即可。

## Simulator 节点

Simulator 节点用于测试、组装、等待、模拟轻量步骤。它不调用真实 Agent。

### `emit`

返回解析后的 `params` 作为 payload。常用于起始上下文节点。

```json
{
  "id": "intake",
  "label": "Simulator 输入整理",
  "operation": "emit",
  "params": {
    "project": "$input.project",
    "goal": "$input.goal"
  },
  "simulate": {
    "min_seconds": 2,
    "max_seconds": 5
  }
}
```

### `join`

返回 `params.fields` 中声明的字段。常用于汇总多个上游结果。

```json
{
  "id": "final_package",
  "label": "Simulator 汇总包",
  "operation": "join",
  "deps": ["plan", "review_gate"],
  "params": {
    "fields": {
      "status": "ready",
      "decision": "$deps.review_gate.decision",
      "reason": "$deps.review_gate.reason",
      "plan": "$deps.plan.latest_comment"
    }
  },
  "simulate": {
    "min_seconds": 2,
    "max_seconds": 5
  }
}
```

`join` 不会自动透传所有依赖，必须在 `fields` 中显式声明要输出的字段。

### `template`

把 `params.fields` 中的值解析后返回。当前常作为固定结构输出使用。

```json
{
  "id": "risk_evidence",
  "operation": "template",
  "params": {
    "fields": {
      "status": "evidence_added",
      "note": "补充风险证据"
    }
  }
}
```

### `status`

返回 `status` 和 `details`。

```json
{
  "id": "quality_check",
  "operation": "status",
  "params": {
    "status": "completed",
    "details": {
      "high_risk": 1,
      "medium_risk": 3
    }
  }
}
```

### 其他 Simulator operation

当前还支持：

- `merge`：返回 `params` 和依赖 payload。
- `count`：对 `params.values` 解析后计数。
- `percentage_from_count`：根据 `count` 计算百分比。

如果没有明确需要，建议优先使用 `emit`、`join`、`status`。

## 真实 Multica Agent 节点

真实 Agent 节点推荐格式：

```json
{
  "id": "architecture_plan",
  "label": "真实Agent 架构方案",
  "operation": "agent_task",
  "join": "any",
  "max_visits": 3,
  "deps": ["intake", "review_gate"],
  "agent": {
    "backend": "multica",
    "agent_key": "architect",
    "context_policy": "provided_context_only",
    "runtime_profile": "maos_compact_agent",
    "execution_mode": "multica",
    "status": "in_progress",
    "priority": "high",
    "poll_seconds": 30,
    "prompt": "请只基于A2A payload完成该节点任务。完成后把最终结果作为Multica评论提交，并将任务状态更新为in_review或done。"
  },
  "timeout_seconds": 7200
}
```

`agent` 字段说明：

| 字段 | 类型 | 推荐值 | 说明 |
| --- | --- | --- | --- |
| `backend` | string | `multica` | 使用真实 Multica Agent Service。 |
| `agent_key` | string | 例如 `architect`, `security`, `qa`, `docs`, `devops`, `product_strategy` | 目标 Agent 类型。具体可用值取决于本地 Multica daemon。 |
| `context_policy` | string | `provided_context_only` | 要求 Agent 只使用 A2A payload。 |
| `runtime_profile` | string | `maos_compact_agent` | 使用为 MAOS 优化的 compact profile。 |
| `execution_mode` | string | `multica` | 通过 Multica 执行。 |
| `status` | string | `in_progress` | 创建 Multica task 时的初始状态。 |
| `priority` | string | `low`, `medium`, `high` | 任务优先级。 |
| `poll_seconds` | number | `30` | Temporal durable polling 间隔。 |
| `prompt` | string | 任务说明 | 给 Agent 的节点级指令。 |

真实 Agent 节点 prompt 应明确包含：

- “只基于 A2A payload”。
- 需要读取哪些上游节点。
- 输出内容要求。
- 完成后把最终结果作为 Multica 评论提交。
- 将任务状态更新为 `in_review` 或 `done`。

## 真实 Agent 评审门

如果一个真实 Agent 节点要驱动分支，必须要求它输出稳定 JSON。

推荐 prompt 片段：

```text
你是真实Agent评审门。请只基于A2A payload判断上游结果是否通过。
最终评论末尾必须输出独立JSON代码块：
{"decision":"needs_revision或approved","reason":"中文原因","required_changes":["修改项；没有则输出空数组"],"confidence":0.0到1.0}
如果需要修改，输出needs_revision；否则输出approved。
完成后把结果作为Multica评论提交，并将任务状态更新为in_review或done。
```

推荐边：

```json
{
  "from": "review_gate",
  "to": "plan",
  "when": "last.decision == 'needs_revision'",
  "label": "要求修改，回到方案"
}
```

```json
{
  "from": "review_gate",
  "to": "final_package",
  "when": "last.decision == 'approved'",
  "label": "评审通过"
}
```

## 典型模式

### 1. 并行任务后汇总

```json
{
  "nodes": [
    {"id": "intake", "operation": "emit", "params": {"goal": "$input.goal"}},
    {"id": "product_plan", "operation": "agent_task", "deps": ["intake"], "agent": {"backend": "multica", "agent_key": "product_strategy", "prompt": "只基于A2A payload生成产品方案。完成后评论并更新状态。"}},
    {"id": "architecture_plan", "operation": "agent_task", "deps": ["intake"], "agent": {"backend": "multica", "agent_key": "architect", "prompt": "只基于A2A payload生成架构方案。完成后评论并更新状态。"}},
    {"id": "package", "operation": "join", "join": "all", "deps": ["product_plan", "architecture_plan"], "params": {"fields": {"status": "ready", "product": "$deps.product_plan.latest_comment", "architecture": "$deps.architecture_plan.latest_comment"}}}
  ],
  "edges": [
    {"from": "intake", "to": "product_plan"},
    {"from": "intake", "to": "architecture_plan"},
    {"from": "product_plan", "to": "package"},
    {"from": "architecture_plan", "to": "package"}
  ]
}
```

### 2. 真实 Agent 评审驱动循环

```json
{
  "max_total_visits": 12,
  "nodes": [
    {"id": "intake", "operation": "emit", "params": {"goal": "$input.goal"}},
    {
      "id": "plan",
      "operation": "agent_task",
      "join": "any",
      "max_visits": 3,
      "deps": ["intake", "review_gate"],
      "agent": {
        "backend": "multica",
        "agent_key": "architect",
        "context_policy": "provided_context_only",
        "runtime_profile": "maos_compact_agent",
        "execution_mode": "multica",
        "status": "in_progress",
        "priority": "high",
        "poll_seconds": 30,
        "prompt": "请只基于A2A payload生成方案。若看到review_gate中的needs_revision意见，请合并修改。完成后评论并更新状态。"
      },
      "timeout_seconds": 7200
    },
    {
      "id": "review_gate",
      "operation": "agent_task",
      "join": "any",
      "max_visits": 3,
      "deps": ["plan"],
      "agent": {
        "backend": "multica",
        "agent_key": "qa",
        "context_policy": "provided_context_only",
        "runtime_profile": "maos_compact_agent",
        "execution_mode": "multica",
        "status": "in_progress",
        "priority": "high",
        "poll_seconds": 30,
        "prompt": "你是真实Agent评审门。最终评论末尾必须输出独立JSON代码块：{\"decision\":\"needs_revision或approved\",\"reason\":\"中文原因\",\"required_changes\":[\"修改项；没有则输出空数组\"],\"confidence\":0.0到1.0}。完成后评论并更新状态。"
      },
      "timeout_seconds": 7200
    },
    {"id": "final_package", "operation": "join", "deps": ["plan", "review_gate"], "params": {"fields": {"status": "ready", "decision": "$deps.review_gate.decision", "plan": "$deps.plan.latest_comment"}}}
  ],
  "edges": [
    {"from": "intake", "to": "plan"},
    {"from": "plan", "to": "review_gate"},
    {"from": "review_gate", "to": "plan", "when": "last.decision == 'needs_revision'", "label": "修改后重试"},
    {"from": "review_gate", "to": "final_package", "when": "last.decision == 'approved'", "label": "通过"}
  ]
}
```

## 生成任务图时的建议

1. 优先生成 `graph_type: "control_flow"`，并显式写 `edges`。
2. 所有节点 `id` 使用英文 snake_case，`label` 可使用中文。
3. 每个真实 Agent 节点必须有清晰、窄范围的 `prompt`。
4. 真实 Agent 节点默认使用：
   - `context_policy: "provided_context_only"`
   - `runtime_profile: "maos_compact_agent"`
   - `execution_mode: "multica"`
   - `poll_seconds: 30`
   - `timeout_seconds: 7200`
5. 有循环时，必须设置：
   - 图级 `max_total_visits`
   - 循环节点 `max_visits`
   - 回到上游节点 `join: "any"`
6. 不要为了“中转修改意见”创建空的 Simulator 节点。评审门的 `needs_revision` 边应直接回到需要重做的真实 Agent 节点。
7. 如果下游需要读取某个上游输出，请在下游节点 `deps` 中显式声明该上游节点。
8. `join` 节点不会自动透传所有依赖；需要在 `params.fields` 显式列出输出字段。
9. 条件分支必须覆盖所有可能结果；如果真实 Agent 可能输出其他值，应增加兜底边或约束 prompt。
10. 生成后必须用 JSON parser 校验文件格式，不要输出注释、尾逗号或 Markdown 包裹。

## 最小完整示例

```json
{
  "id": "example-agent-review-loop",
  "name": "示例：真实Agent评审循环",
  "graph_type": "control_flow",
  "max_total_visits": 12,
  "input": {
    "topic": "上线一个新的知识库助手",
    "requirements": ["需要安全评审", "需要发布说明", "允许一次或多次修改"]
  },
  "start": "intake",
  "nodes": [
    {
      "id": "intake",
      "label": "Simulator 输入整理",
      "operation": "emit",
      "params": {
        "topic": "$input.topic",
        "requirements": "$input.requirements"
      },
      "simulate": {
        "min_seconds": 2,
        "max_seconds": 5
      }
    },
    {
      "id": "solution_plan",
      "label": "真实Agent 方案设计",
      "operation": "agent_task",
      "join": "any",
      "max_visits": 3,
      "deps": ["intake", "review_gate"],
      "agent": {
        "backend": "multica",
        "agent_key": "architect",
        "context_policy": "provided_context_only",
        "runtime_profile": "maos_compact_agent",
        "execution_mode": "multica",
        "status": "in_progress",
        "priority": "high",
        "poll_seconds": 30,
        "prompt": "请只基于A2A payload设计上线方案。若看到review_gate中的needs_revision评审意见，请合并reason和required_changes重新生成方案。完成后把最终结果作为Multica评论提交，并将任务状态更新为in_review或done。"
      },
      "timeout_seconds": 7200
    },
    {
      "id": "review_gate",
      "label": "真实Agent 评审门",
      "operation": "agent_task",
      "join": "any",
      "max_visits": 3,
      "deps": ["solution_plan"],
      "agent": {
        "backend": "multica",
        "agent_key": "qa",
        "context_policy": "provided_context_only",
        "runtime_profile": "maos_compact_agent",
        "execution_mode": "multica",
        "status": "in_progress",
        "priority": "high",
        "poll_seconds": 30,
        "prompt": "你是真实Agent评审门。请只基于A2A payload判断solution_plan是否可以进入最终交付。最终评论末尾必须输出独立JSON代码块：{\"decision\":\"needs_revision或approved\",\"reason\":\"中文原因\",\"required_changes\":[\"修改项；没有则输出空数组\"],\"confidence\":0.0到1.0}。完成后把最终结果作为Multica评论提交，并将任务状态更新为in_review或done。"
      },
      "timeout_seconds": 7200
    },
    {
      "id": "final_summary",
      "label": "Simulator 最终汇总",
      "operation": "join",
      "join": "any",
      "deps": ["solution_plan", "review_gate"],
      "params": {
        "fields": {
          "status": "ready",
          "decision": "$deps.review_gate.decision",
          "reason": "$deps.review_gate.reason",
          "plan": "$deps.solution_plan.latest_comment"
        }
      },
      "simulate": {
        "min_seconds": 2,
        "max_seconds": 5
      }
    }
  ],
  "edges": [
    {
      "from": "intake",
      "to": "solution_plan",
      "label": "生成方案"
    },
    {
      "from": "solution_plan",
      "to": "review_gate",
      "label": "真实Agent评审"
    },
    {
      "from": "review_gate",
      "to": "solution_plan",
      "when": "last.decision == 'needs_revision'",
      "label": "要求修改，回到方案"
    },
    {
      "from": "review_gate",
      "to": "final_summary",
      "when": "last.decision == 'approved'",
      "label": "评审通过"
    }
  ]
}
```

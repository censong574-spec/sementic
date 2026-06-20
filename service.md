这份为您深度扩充、细节拉满的 **《工业级多 Agent 协作平台分布式架构设计与落地白皮书》**。

文档将每一个模块的**具体技术选型、数据库拓扑、通信协议、防死循环代码算法、乃至边界熔断的 Redis 命令**全部进行了工程化展开。

您可以直接复制以下全量 Markdown 内容进行团队内审、方案宣讲或作为项目架构奠基文档：

---

# 🚀 工业级多 Agent 协作平台分布式架构设计与落地白皮书

## 零、微服务职责划分（Gateway / Worker 统一约定）

平台拆分为两个独立微服务，**`sementic-gateway`** 与 **`sementic-worker`** 各有一份工程级纲要（`service.md`）。以下为跨服务统一边界，两处文档必须保持一致。

### 0.1 职责矩阵

| 项目 | sementic-gateway | sementic-worker |
|------|------------------|-----------------|
| IM HTTP 接入 | ✅ | ❌ |
| L1 静态噪音过滤 | ✅ | ❌ |
| Redis `status:*` 读取（熔断） | ✅ | 可选 |
| Redis `status:*` 写入 | ❌ | ❌（状态同步服务写） |
| Kafka 生产 | ✅ | ❌ |
| Kafka 消费 | ❌ | ✅ |
| Redis `channel:history:*` **写入** | ❌ | ✅ |
| Redis `channel:history:*` **读取** | ❌ | ✅（编排取最近 10 条） |
| `is_bot` 影响入队 | ❌ | ❌ |
| `is_bot` 影响意图识别 | ❌ | ✅ |
| L2 规则 / L3 LLM | ❌ | ✅（仅人类消息） |
| Temporal / MultiCA | ❌ | ✅ |

### 0.2 两条核心原则

**① Redis 滑动窗口与 `is_bot` 无关**

- 凡通过 Gateway（未被 L1 过滤、未被熔断拒绝）并进入 Kafka 的消息，Worker 消费后**一律写入** Redis 滑动窗口。
- **人类消息**与 **bot 回复**均进入窗口，供多轮语境使用（例如 bot 回复「已创建 JIRA-123」后，人类问「刚才单号多少」须能读到）。

**② `is_bot` 仅用于任务意图识别**

- Gateway **透传** `user_context.is_bot`，不对其做入队分支。
- Worker 消费后：**先写 Redis** → 若 `is_bot == true` 则**结束**（不进入 L2/L3）→ 若 `is_bot == false` 则进入 L2 规则 / L3 LLM 意图编排。

### 0.3 端到端数据流（统一版）

```
IM POST
  → Gateway: L1 过滤 → 熔断读 status:* → Kafka（key=group_session_id）
  → Worker:  消费 → LPUSH 滑动窗口 → [is_bot? 跳过编排 : L2/L3 → Temporal/MultiCA]
```

---

## 一、 核心架构演进路线与设计哲学

本架构专为高并发、强时序、多租户算力隔离的 AI Agent 协作系统设计。核心设计哲学为：**“前端打散、中间对齐、后端串行、状态集中、按需检索”**。

在传统的 IM 机器人设计中，开发者常犯的错误是让 IM 充当唯一的数据库和连接中枢，导致核心逻辑与外围 IM 软件深度耦合。本系统通过自建 **“安全网关 + Kafka + 意图 Worker + MultiCA 服务端”** 的全链路流式架构，在保障系统极速响应的同时，实现了底层的彻底解耦与弹性伸缩。

---

## 二、 全链路技术拓扑与数据流向

整个平台的核心组件分布在三个完全隔离的物理层级：**IM 接入层、核心编排层、端侧运行时（Runtime）层**。

### 1. 全链路拓扑图

```
[ 团队成员 / 雇主 (Mattermost IM 客户端) ]
                   │
                   ▼ (HTTPS Post / 流量完全随机负载均衡)
┌────────────────────────────────────────────────────────┐
│ 1. 无状态安全网关层 (sementic-gateway)                    │
│    - L1：语气词/噪音 O(1) 阻断（不入 Kafka）              │
│    - 只读 Redis status:* 就地熔断                        │
│    - 合格消息入 Kafka（人 / bot 一视同仁）                │
└────────────────────────────────────────────────────────┘
                   │
                   ▼ (Partition Key = group_session_id)
┌────────────────────────────────────────────────────────┐
│ 2. Kafka 分布式消息总线 (高吞吐、FIFO 先进先出队列)        │
│    - [Partition 0: 频道_A]  --> 严格单线程消费保证时序   │
│    - [Partition 1: 频道_B]  --> 跨频道天然并行，无干扰   │
└────────────────────────────────────────────────────────┘
                   │
                   ▼ (Kafka Rebalance 水平扩展)
┌────────────────────────────────────────────────────────┐
│ 3. 意图服务 Worker 集群 (sementic-worker / AI 编排中枢)  │
│    - 消费后写 Redis 滑动窗口（人 + bot 均写）             │
│    - 人类消息：L2 规则 / L3 小模型语义判定               │
│    - bot 消息：仅更新语境，不触发编排                     │
└────────────────────────────────────────────────────────┘
      │                     │                     │
      ▼ (写+读最近10条)      ▼ (Tool 跨网络现捞)   ▼ (触发有状态长任务)
┌───────────┐        ┌────────────┐        ┌──────────────┐
│ Redis 缓存 │        │ Mattermost │        │ Temporal 分布 │
│ 滑动窗口   │        │ 官方服务器  │        │ 式工作流引擎 │
└───────────┘        └────────────┘        └──────────────┘
                                                  │
                                                  ▼ (gRPC / HTTP 调用)
                                           ┌──────────────┐
                                           │ MultiCA 服务 │
                                           │ 端 (技术中枢)│
                                           └──────┬───────┘
                                                  │ (长连接：WebSocket/gRPC)
                                                  ▼
                                           ┌──────────────┐
                                           │ 端侧 Daemon   │
                                           │ (Host 托管态) │
                                           └──────┬───────┘
                                                  │ (OS Subprocess 派生)
                                                  ▼
                                           ┌──────────────┐
                                           │ Agent CLI 进程│
                                           │ (如 Python)  │
                                           └──────────────┘

```

---

## 三、 意图中枢轻量化数据库拓扑（超薄账本）

为了避免与 IM 侧和 MultiCA 侧的数据产生臃肿的“冗余重复”，意图服务在关系型数据库（如 PostgreSQL / MySQL）中**仅维护以下两张表**。它们不记录任何技术实现，只记录“关系的纽带”**与**“权限的边界”：

### 1. 机器人资产所有权与权鉴表 (`agent_instances`)

本表锁死了多租户下的“物理算力所有权”，是防止 B 用户恶意调动 A 用户本地私有算力的铁闸。

```sql
CREATE TABLE agent_instances (
    id BIGSERIAL PRIMARY KEY,
    bot_user_id VARCHAR(64) NOT NULL UNIQUE,       -- Mattermost 分配的机器人虚拟用户 ID
    owner_user_id VARCHAR(64) NOT NULL,          -- 拥有该机器人的真实人类用户 ID (如 Hassan)
    multica_agent_id VARCHAR(128) NOT NULL,       -- 【核心纽带】对接 MultiCA 服务端的 Agent 实体的唯一 Key
    share_scope VARCHAR(20) DEFAULT 'private',    -- 权限策略：private (私有秘书) / channel_shared (群共享)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_bot_owner ON agent_instances(bot_user_id, owner_user_id);

```

### 2. 群组空间绑定映射表 (`channel_bot_mapping`)

解决 Worker 在不出内网网络的前提下，微秒级知晓某个群组“当前有哪些机器人常驻”。

```sql
CREATE TABLE channel_bot_mapping (
    id BIGSERIAL PRIMARY KEY,
    channel_id VARCHAR(64) NOT NULL,               -- Mattermost 频道 ID
    bot_user_id VARCHAR(64) NOT NULL,              -- 驻留在该频道的机器人用户 ID
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, bot_user_id)
);
CREATE INDEX idx_channel_id ON channel_bot_mapping(channel_id);

```

---

## 四、 核心落地设计方案细节展开

### 1. 强时序保护与分布式弹性伸缩

* **乱序痛点**：由于安全网关层采用多节点无状态集群部署，当群内多名用户高频发言时，HTTP 请求到达网关的顺序可能会因为网络抖动发生错乱。
* **Kafka 校准解法**：网关在通过合法性检查后，**必须强行以 `group_session_id`（频道 ID）作为 Partition Key** 投递进 Kafka。Kafka 内部确保同一个 Partition 内部的消息绝对 FIFO（先进先出）。下游意图 Worker 集群即便扩容到 100 台，同一个频道的消息也永远只会被同一台 Worker 按物理先后顺序串行消费，完美保护了多轮对话的时序性，也为 Redis 滑动窗口的顺序写入提供保障。

### 2. 高性能三层意图判定漏斗（算力阶梯过滤）

为了极限压低运营成本并提高系统响应，平台严禁每句话都调大模型，构建了如下过滤漏斗：

* **第一层：安全网关侧（入队前·噪音就地消灭）**
* **逻辑**：利用高性能正则表达式（Regex）或常数级时间复杂度 $O(1)$ 的内存型 `Static HashSet`。
* **词表范围**：纯语气词（“哈哈”、“呃”、“1111”）、纯单字标点、群内特定复读表情。
* **处理**：一旦命中，说明属于无价值水群信息，**网关直接返回 HTTP 200 放行上屏，绝对不往 Kafka 里塞，不给下游造成任何压力**。
* **与 `is_bot` 无关**：bot 发出的水群消息同样会被 L1 拦截。


* **第二层：意图服务 Worker（出队后·确定性匹配，仅人类消息）**
* **前置条件**：Kafka 消费完成且已写入 Redis；**`user_context.is_bot == false`**。
* **逻辑**：对消息执行非模型类的规则引擎过滤。检查是否显式 @ 了机器人，且是否携带了平台预设的硬编码强特征前缀（例如：`部署`、`重启`、`分析`、`查询`、`run job`）。
* **处理**：若命中（如 `"@项目助手 部署前端"`），**规则引擎直接拦截，跳过大模型**，秒级提取结构化参数，生成任务 DAG 丢给 Temporal 引擎。


* **第三层：意图服务 Worker（出队后·本地小模型语义兜底，仅人类消息）**
* **前置条件**：同上，**仅人类消息**进入 L3。
* **逻辑**：针对用户大白话、多轮指代（如：“*把刚刚跑崩了的那个东西放到我的 Windows 电脑上再来一次*”）这类模糊语境，系统调用本地部署的 `Qwen2.5-14B-Instruct` 或 `DeepSeek-R1-Distill-Qwen-14B`。
* **上下文组装**：Worker 从 Redis 动态拉取当前频道的最近 10 条滑动历史（**含 bot 回复**），拼成简短 Prompt，驱使小模型进行多轮语义判定与 Tool Calling 参数提取。

* **bot 消息消费路径**：Worker 消费 bot 消息后**只写 Redis 滑动窗口**，**不进入 L2/L3**，不触发 Temporal。



### 3. Redis 20 条滑动窗口 + IM 现捞的高级检索（RAG）机制

为了彻底甩掉在本地建立全量聊天历史镜像库的运维与存储大坑，系统创造性地通过“时间换空间”实现轻量化 RAG：

#### ① Working Memory（Redis 常驻滑动窗口）

**写入方：sementic-worker**（Gateway **不写**）。

Worker 每消费一条 Kafka 消息（人类或 bot），在 Redis 中执行流式滚动写入，网关侧维护上限 **20** 条，Worker 编排时读取最近 **10** 条。

```redis
# Worker 消费 Kafka 后，往频道 room_99 的滑动窗口追加入队消息
LPUSH channel:history:room_99 "{\"msg_id\":\"post_x\",\"sender_id\":\"usr_x\",\"sender_name\":\"Hassan\",\"content\":\"刚才那个视频增强脚本报错了\",\"is_bot\":false,\"timestamp\":\"2026-06-15T10:00:00Z\"}"
# 强行裁剪，仅保留最近 20 条，杜绝内存膨胀
LTRIM channel:history:room_99 0 19

```

**写入内容**：频道内所有有效对话（人类 + bot），与是否触发任务编排无关。

#### ② On-demand Tool（面向 IM 的时空查旧账工具）

当本地大模型阅读了 10 条历史后，发现“刚才那个视频”的线索已经超出了 10 条的范围。它会反向触发平台提供给它的 Function Calling 工具：`search_channel_history(keyword="报错", time_range="last_week")`。

#### ③ 协议桥接、清洗与硬匹配算法限制

Worker 拦截到大模型的 Tool Call 后，立刻将其翻译为 Mattermost 官方的高级检索 API。

```python
import datetime
import requests

def tool_search_channel_history(channel_id, keyword, time_range="today"):
    # 1. 翻译大模型意图，将时间枚举转换为 Mattermost 搜索支持的语法糖
    search_query = f"in:{channel_id} {keyword}"
    if time_range == "last_week":
        start_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        search_query += f" after:{start_date}"
        
    # 2. 跨网络请求 IM 官方历史库 (现捞)
    headers = {"Authorization": "Bearer SYSTEM_BOT_TOKEN"}
    url = f"https://mattermost.yourcompany.com/api/v4/teams/main/posts/search"
    response = requests.post(url, json={"terms": search_query}, headers=headers).json()
    
    # 3. 核心：超薄数据清洗（防止 Mattermost 臃肿的元数据撑爆 AI 上下文）
    clean_posts = []
    for post in response.get("posts", {}).values():
        clean_posts.append(f"[{post['create_at']}] 用户({post['user_id']}): {post['message']}")
        if len(clean_posts) >= 5:  # 严格硬卡 5 条上限，保护大模型不涣散注意力
            break
            
    return "\n".join(clean_posts)

```

* **技术代价妥协说明**：由于弃用了本地向量库，此方案**不再具备语义模糊搜索能力**。大模型在调用工具时，如果拿着口语化词汇（如“跑崩了”）去问 Mattermost，而 Mattermost 里只有硬核日志（`Exception: NullPointer`），由于硬关键字不匹配，会返回空。因此，系统在 System Prompt 里强制限制了大模型：“*当你调用搜索工具时，必须自行将用户口语翻译为可能出现在日志中的专业硬关键词。*”

### 4. 异步事件驱动的多路状态同步（就地熔断）

端侧 Daemon 通过长连接（WebSocket / gRPC）常驻在用户的物理机上（如 `liusong` 开发环境）。一旦连接发生意外死锁或断开，**MultiCA 服务端是第一感知人**，处理流程通过 Kafka 多消费者组进行完美的**一源多路分发**：

```
[ 端侧 Daemon WebSocket 断开 ]
               │
               ▼ (0毫秒延迟，长连接 onClose 触发)
┌────────────────────────────────────────────────────────┐
│ MultiCA 服务端 ──> 往 Kafka 发送 Runtime 挂起事件         │
└────────────────────────────────────────────────────────┘
                               │
       ┌───────────────────────┴───────────────────────┐
       ▼ (消费者组 A：面向 IM 看板)                      ▼ (消费者组 B：面向网关熔断)
┌───────────────────────────────┐               ┌───────────────────────────────┐
│ 状态同步 Worker                │               │ 状态缓存同步器                  │
│ 1. 延迟 5 秒（防秒级闪断网络抖动）│               │ 1. 0 毫秒延迟，立刻写入 Redis   │
│ 2. 若真挂，调用 Mattermost    │               │    SET status:bot_101 offline │
│    API，迫使群内机器人账号    │               └───────────────────────────────┘
│    头像变灰。                 │
│ 3. 挂上自定义签名：           │
│    "⚠️ [端侧运行时断开]"      │
└───────────────────────────────┘

```

* **就地熔断防御机制**：当用户 B 没注意到群里机器人头像变灰，强行发送任务指令时：请求到达 **sementic-gateway**，网关只读 `redis.get("status:bot_101")`。若发现是 `offline`，**请求在 1ms 内被网关原地拒绝，绝不往下游 Kafka 和 Temporal 发送**。网关直接 HTTP 响应给 Mattermost，由系统小助手在群里给出回执：`“🤖 系统提示：当前机器人的端侧常驻通道已断开，无法承载任务，正在等待重连。”` 从而实现对整个中枢编排算力的绝对安全闭环。

---

## 五、 全生命周期任务下发流向闭环（从 IM 到 CLI）

当全链路状态健康时，一个复杂的“高能耗视频增强/裁剪”任务是如何以优雅的“瘦客户端、重调度”模式完成闭环的：

```
1. 雇主 Hassan 在 Mattermost 群发帖:
   "@项目助手，把刚才那个报错的视频增强 CLI 脚本在我的 Windows 电脑上重启一下。"
                               │
                               ▼
2. sementic-gateway（入队检验）:
   - 快速通过 L1 语气词过滤。
   - 只读 Redis 确认 “项目助手” 的状态为 ONLINE。
   - 将消息（含 is_bot=false）以 group_session_id 为 Key 塞入 Kafka。
   - 【不写 Redis 滑动窗口】
                               │
                               ▼
3. sementic-worker（出队）:
   - 从 Kafka 顺序拉取该消息。
   - LPUSH 写入 Redis 滑动窗口（与 is_bot 无关，本例为人类消息）。
   - is_bot == false → 进入意图识别：
     · 查询本地资产所有权表（表 A），确认 multica_agent_id 归属 Hassan。
     · 权限校验过关。
                               │
                               ▼
4. AI 大脑多轮思考（时空按需现捞）:
   - 从 Redis 读取最近 10 条语境（含历史 bot 回复）。
   - 发现“刚才报错”指代不清。小模型主动调用 Tool。
   - Worker 驱使工具跨网络去 Mattermost 捞取了 10 分钟前群组内的纯文本报错日志。
   - 小模型读完日志，顺利提取出结构化任务参数：
     {"action": "restart_cli", "params": {"script": "sharpen.py", "mode": "srvggnet_compact"}}
                               │
                               ▼
5. Temporal 分布式工作流驱动:
   - 意图服务结束，打包任务参数，拉起 **Temporal Workflow**。
   - Temporal 异步驱动其对应的 Activity 执行实际的下发 RPC 调用。
                               │
                               ▼
6. 技术底层网络送达:
   - Temporal Activity 携带目标核心指针，调用 **MultiCA 服务端** 的执行接口。
   - MultiCA 服务端查阅自身的长连接状态机，找到长连在线的本地 Windows 宿主机（`liusong_desktop`）。
   - 服务端通过 WebSocket 管道，把这串结构化 JSON 指令硬推给宿主机的 **端侧 Daemon**。
                               │
                               ▼
7. 端侧物理就地拉起:
   - 端侧 Daemon 收到指令，解析参数。
   - 通过操作系统 Subprocess 派生机制，在本地宿主机上轰鸣拉起命令行 CLI 工具：
     python sharpen.py --task_id 12345 --mode srvggnet_compact
                               │
                               ▼
8. 结果异步收拢回执:
   - CLI 运行完毕，将 stdout 输出和状态码汇报给 Daemon。
   - Daemon 通过长连接逆向吐回给 MultiCA 服务端 ──> Temporal Activity ──> 最终调用 Mattermost 异步打回群组。
   - bot 回复消息经 IM Webhook 再次进入 Gateway → Kafka → Worker：
     Worker 写 Redis 语境，is_bot=true 跳过 L2/L3，不重复触发编排。

```

---

## 六、 sementic-worker 微服务纲要（摘要）

> 完整实施清单可在 Worker 仓库独立维护；此处与 `sementic-gateway/service.md` 对齐。

### 6.1 消费后处理顺序（必须实现）

```
Kafka 消费消息
    │
    ├─[1] 幂等检查（event_id / msg_id）
    │
    ├─[2] 写 Redis 滑动窗口（LPUSH + LTRIM，人 + bot 均写）
    │
    ├─[3] is_bot == true ?
    │        └─ 是 → 结束（不进入 L2/L3）
    │
    ├─[4] L2 规则引擎
    │        └─ 命中 → Temporal / 直接下发
    │
    └─[5] L3 本地小模型（LRANGE 最近 10 条含 bot 语境）
```

### 6.2 Worker 验收要点

1. 消费人类任务消息后 Redis 有写入，且进入 L2/L3 链路
2. 消费 bot 回复后 Redis 有写入，**不**进入 L2/L3
3. 同一 `group_session_id` 连发 25 条有效消息 → Redis 只保留 20 条
4. L3 编排读取的最近 10 条历史中**包含 bot 回复**

---

这份白皮书将系统各层组件的边界隔离（IM 只管 UI、Gateway 只管验收入队、Worker 管语境与编排、MultiCA 只管技术实现与长连接）清晰固定了下来。该方案具备极致的轻量化优势，为平台的长期稳定发展奠定了坚实的云原生基础。

# 远程部署（Worker 仓库）

将 Gateway + Worker + Temporal（MAOS 依赖）部署到远端服务器。脚本位于本仓库 `scripts/deploy/`；Gateway 源码路径在 `remote.toml` 的 `[local] gateway_path` 配置。

| 文件 | 是否入库 | 内容 |
|------|----------|------|
| `scripts/deploy/remote.toml` | 是 | 远端主机、端口、Kafka/Redis、Temporal/PostgreSQL |
| `scripts/deploy/deploy.env` | 否 | SSH 密码、LLM API Key |

## 首次准备

```powershell
cd D:\code\sementic\sementic

pip install -r scripts/deploy/requirements-deploy.txt

copy scripts\deploy\deploy.env.example scripts\deploy\deploy.env
# 填写 DEPLOY_SSH_PASSWORD；LLM Key 可留空（会读仓库根目录 .env）

# 下载 Temporal Server 安装包（云主机无法访问 GitHub 时必需）
.\scripts\deploy\temporal\fetch_temporal_server.ps1

# 远端需已安装 PostgreSQL 二进制到 /opt/postgresql-14（Temporal 专用，见下文）
```

## 常用命令

```powershell
.\scripts\deploy\deploy.ps1 full          # 代码 + 配置 + Temporal/PG + 重启
.\scripts\deploy\deploy.ps1 code          # 只更新代码
.\scripts\deploy\deploy.ps1 cleanup       # 卸载全部部署产物
.\scripts\deploy\deploy.ps1 diagnose
.\scripts\deploy\deploy.ps1 temporal-status
```

## 子命令

| 命令 | 作用 |
|------|------|
| `full` | 上传代码、写配置、**自动部署 Temporal + PostgreSQL**、重启 gateway/worker |
| `code` | 只更新代码；Temporal 非 SERVING 时同样会自动修复 |
| `config` | 只更新远端 `.env` 和 systemd |
| `restart` / `status` / `logs` | 运维 |
| `messages` / `redis` / `diagnose` | 远端排查 |
| `temporal` | 强制重新部署 Temporal（含 PG bootstrap） |
| `temporal-bundle` | 只上传脚本/安装包（不启动） |
| `temporal-status` / `temporal-logs` | Temporal 运维 |
| `cleanup` | **卸载**：停服务、删目录、删 systemd 单元、清 journal、删 postgres 用户（若无进程） |

## cleanup（卸载全部部署产物）

```powershell
.\scripts\deploy\deploy.ps1 cleanup
```

会清理（不影响 Mattermost/Multica/Kafka/Redis）：

| 类型 | 内容 |
|------|------|
| systemd | `sementic-gateway`、`sementic-worker`、`temporal-server`、`postgresql-14-custom` |
| 目录 | `/opt/sementic`、`/opt/temporal`、`/opt/postgresql-14/data`、`/opt/postgresql-14/logs` |
| 端口进程 | 8081、8766、7233、5432 上残留监听 |
| journal | 上述服务的 journal 条目 |
| 用户 | `postgres` 用户/组（仅当无 postgres 进程时） |

**保留：** `/opt/postgresql-14` 二进制（若预先安装）、Mattermost/Multica 内置 PG、Kafka、Redis。

## 部署栈（一次 `full` 会做什么）

```
full
 ├── 上传 gateway + worker 代码，pip install
 ├── 写入 .env + systemd（worker 依赖 temporal-server + postgresql-14-custom）
 ├── [auto_deploy] Temporal 栈
 │    ├── bootstrap_postgres.sh   → 若 :5432 不可达则 init + systemd
 │    ├── 创建 temporal / temporal_visibility 库
 │    ├── schema 迁移 + btree_gin
 │    └── 启动 temporal-server (:7233) + 注册 default namespace
 └── 重启 sementic-gateway / sementic-worker
```

`remote.toml` 中 `auto_deploy = true`（默认）控制 `full`/`code` 是否自动跑 Temporal。Temporal 已 **SERVING** 时会跳过以节省时间。

## PostgreSQL 说明（重要）

机器上可能有多个 PostgreSQL，**不要混用**：

| 端口 | 归属 | 用途 |
|------|------|------|
| **5432** | `postgresql-14-custom` | **Temporal 专用**（deploy 自动 bootstrap） |
| 55432 | Mattermost 内置 | 仅 Mattermost |
| 55433 | Multica 内置 | 仅 Multica |

Temporal 使用独立库 `temporal`、`temporal_visibility`，用户 `temporal`。与 Mattermost/Multica **不共用**实例。

**前提：** 远端 `/opt/postgresql-14/bin/initdb` 等二进制需已安装。若目录不存在，需先在机器上安装 PostgreSQL 14 到该路径。

Bootstrap 脚本：`scripts/deploy/temporal/bootstrap_postgres.sh`（由 `deploy.sh` 在 PG 不可达时自动调用）。

## Temporal Server

| 资源 | 值 |
|------|-----|
| gRPC | `127.0.0.1:7233` |
| UI | `127.0.0.1:8233` |
| MAOS 观测 UI | `0.0.0.0:8766`（worker 内嵌） |
| systemd | `temporal-server`、`postgresql-14-custom` |
| 远端目录 | `/opt/temporal` |

MAOS 连接：`SEMENTIC_MAOS_TEMPORAL_ADDRESS=127.0.0.1:7233`

### 提供 Temporal 安装包

**方式 A — 本地上传（推荐）**

```powershell
.\scripts\deploy\temporal\fetch_temporal_server.ps1
.\scripts\deploy\deploy.ps1 full
```

**方式 B — 安装包已在远端**（配置 `bundle_fallback_dirs`）

**方式 C — 只同步脚本**

```powershell
.\scripts\deploy\deploy.ps1 temporal-bundle
# SSH: bash /opt/temporal/deploy/deploy.sh start
```

### 配置项（remote.toml `[temporal]`）

| 键 | 说明 |
|----|------|
| `auto_deploy` | `full`/`code` 是否自动部署 Temporal（默认 true） |
| `postgres_custom_root` | PG 安装路径，默认 `/opt/postgresql-14` |
| `postgres_service` | systemd 单元名，默认 `postgresql-14-custom` |
| `postgres_admin_socket` | admin 连接 socket 目录，默认 `/tmp` |
| `bundle_fallback_dirs` | 远端预置 tarball 搜索路径 |

密码：`deploy.env` 中 `TEMPORAL_POSTGRES_PASSWORD`（可选，留空则远端自动生成）。

## 远端目录

```
/opt/sementic/
├── venv/
├── gateway/
└── sementic/

/opt/postgresql-14/     # Temporal 专用 PG（bootstrap 创建 data/）
/opt/temporal/
├── deploy/deploy.sh
├── deploy/bootstrap_postgres.sh
├── temporal.env
└── server/temporal-server
```

## 故障排查

- Worker 启动报 `Connection refused 127.0.0.1:7233` → 跑 `.\scripts\deploy\deploy.ps1 temporal` 或 `full`
- Worker/MAOS 报 `Namespace default is not found` → `.\scripts\deploy\deploy.ps1 temporal`（会走 `deploy.sh start` 并自动注册 namespace）
- Temporal 报 `cannot connect to PostgreSQL` → 确认 `/opt/postgresql-14/bin/initdb` 存在，再 `temporal`
- `diagnose` 含 gateway 消息、Redis、Kafka、Temporal 健康检查

Gateway 日志 action：`FILTERED`（L1 噪声）、`BLOCKED`（内容安全）、`ASYNC_PROCESSING`（已进 Kafka）。

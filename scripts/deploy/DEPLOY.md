# 远程部署（Worker 仓库）

将 Gateway + Worker 部署到远端服务器。脚本位于本仓库 `scripts/deploy/`；Gateway 源码路径在 `remote.toml` 的 `[local] gateway_path` 配置。

| 文件 | 是否入库 | 内容 |
|------|----------|------|
| `scripts/deploy/remote.toml` | 是 | 远端主机、端口、Kafka/Redis、Gateway 本地路径 |
| `scripts/deploy/deploy.env` | 否 | SSH 密码、LLM API Key |

## 首次准备

```powershell
cd D:\code\sementic\sementic

pip install -r scripts/deploy/requirements-deploy.txt

copy scripts\deploy\deploy.env.example scripts\deploy\deploy.env
# 填写 DEPLOY_SSH_PASSWORD；LLM Key 可留空（会读仓库根目录 .env）

# 若 Gateway 仓库不在默认相对路径，编辑 scripts/deploy/remote.toml：
# [local]
# gateway_path = "../gateway/sementic-gateway"
```

## 常用命令

在 **Worker 仓库根目录** 执行：

```powershell
.\scripts\deploy\deploy.ps1 full
.\scripts\deploy\deploy.ps1 code
.\scripts\deploy\deploy.ps1 config
.\scripts\deploy\deploy.ps1 diagnose
.\scripts\deploy\deploy.ps1 messages
.\scripts\deploy\deploy.ps1 redis -Grep "消息片段"
```

或：

```powershell
python scripts/deploy/deploy.py diagnose
```

## 子命令

| 命令 | 作用 |
|------|------|
| `full` | 上传 Gateway + Worker 代码、写配置、重启 |
| `code` | 只更新代码并重启 |
| `config` | 只更新远端 `.env` 和 systemd |
| `restart` / `status` / `logs` | 运维 |
| `messages` / `redis` / `diagnose` | 远端排查 |
| `temporal` | 上传脚本 + 安装/启动 Temporal Server（PostgreSQL 持久化） |
| `temporal-bundle` | 只上传脚本并 staging 安装包（不启动） |
| `temporal-status` / `temporal-logs` | Temporal 运维 |

## Temporal Server（独立微服务）

Standalone **Temporal Server 1.27.x**，供 sementic-worker 内嵌 MAOS 使用。与 multica-server **共用同一台 PostgreSQL 14**（`127.0.0.1:5432`），使用独立库与用户：

| 资源 | 值 |
|------|-----|
| gRPC | `127.0.0.1:7233` |
| PostgreSQL DB | `temporal`, `temporal_visibility` |
| PG 用户 | `temporal` |
| systemd | `temporal-server` |
| 远端目录 | `/opt/temporal` |

MAOS 连接：`SEMENTIC_MAOS_TEMPORAL_ADDRESS=127.0.0.1:7233`（worker 进程内，无需单独 maos_job 服务）

### Worker 内嵌 MAOS

`maos_runtime` 已合并进 sementic-worker 进程。`remote.toml` `[worker]` 示例：

```toml
maos_temporal_address = "127.0.0.1:7233"
maos_multica_job_api_base = "http://127.0.0.1:8080"
```

部署后 `deploy.ps1 config` 会写入 worker `.env`。Multica 凭证优先来自 IM 消息的 `graph.input`（`workspace_id` / `multica_token`）。

### 首次部署（三选一提供安装包）

云主机若无法访问 GitHub，需先把 `temporal_*_linux_amd64.tar.gz` 放到本机或远端：

**方式 A — 本地下载后随 deploy 上传（推荐）**

```powershell
.\scripts\deploy\temporal\fetch_temporal_server.ps1
.\scripts\deploy\deploy.ps1 temporal
```

**方式 B — 安装包已在云主机某目录（如 `/home/liusong/`）**

在 `remote.toml` 配置 `bundle_fallback_dirs`，然后：

```powershell
.\scripts\deploy\deploy.ps1 temporal
```

**方式 C — 只同步脚本/配置，不启动**

```powershell
.\scripts\deploy\deploy.ps1 temporal-bundle
# SSH 到远端手动: bash /opt/temporal/deploy/deploy.sh start
```

### 日常运维

```powershell
.\scripts\deploy\deploy.ps1 temporal-status
.\scripts\deploy\deploy.ps1 temporal-logs
.\scripts\deploy\deploy.ps1 temporal          # 更新脚本后重新 deploy + restart
.\scripts\deploy\deploy.ps1 diagnose          # 含 Temporal 健康检查
```

### 配置说明

| 文件 | 作用 |
|------|------|
| `scripts/deploy/remote.toml` `[temporal]` | 版本、端口、PG 路径、`bundle_fallback_dirs` |
| `scripts/deploy/deploy.env` | `TEMPORAL_POSTGRES_PASSWORD`（可选，留空则远端自动生成） |
| `/opt/temporal/temporal.env` | 远端运行时环境（deploy 写入） |
| `/opt/temporal/temporal.secrets.env` | 远端 PG 密码（首次自动生成） |
| `/opt/temporal/config/development.yaml` | temporal-server 配置 |

`deploy.sh` 会自动：创建 PG 库/用户、用 embedded schema 迁移、编译 `btree_gin` 扩展（visibility 需要）、安装 systemd 并以 PostgreSQL 模式启动。

**注意：** 自定义 PG 安装在 `/opt/postgresql-14`，admin 连接走 Unix socket `/tmp`（见 `postgres_admin_socket`），不要用 `psql -h 127.0.0.1`（会卡在密码提示）。

### 验证 PostgreSQL 模式

`temporal-status` 中 `ExecStart` 应为：

```
/opt/temporal/server/temporal-server --root /opt/temporal --config config --allow-no-auth start
```

gRPC health 应返回 `SERVING`（不是 `temporal server start-dev`）。

## Gateway 仓库（sementic-gateway）

Mattermost Bridge 脚本在 Gateway 仓库内：

- `scripts/mattermost_bridge.py`
- `scripts/mattermost-bridge.env.example` → 复制为 `scripts/mattermost-bridge.env`（已 gitignore）

Bridge 同机部署时：

```
GATEWAY_URL=http://127.0.0.1:8081/api/v1/im/messages
```

## 远端目录

```
/opt/sementic/
├── venv/
├── gateway/    # sementic-gateway
└── sementic/   # worker（本仓库）

/opt/temporal/  # Temporal Server（见 temporal-status）
├── deploy/deploy.sh
├── temporal.env
├── config/development.yaml
├── server/temporal-server
└── artifacts/
```

## 故障排查

见 `diagnose` 子命令。Gateway 日志 action：`FILTERED`（L1 噪声）、`BLOCKED`（内容安全）、`ASYNC_PROCESSING`（已进 Kafka）。

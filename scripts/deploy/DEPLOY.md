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
```

## 故障排查

见 `diagnose` 子命令。Gateway 日志 action：`FILTERED`（L1 噪声）、`BLOCKED`（内容安全）、`ASYNC_PROCESSING`（已进 Kafka）。

# OpenResty 机器人管理 Mock

模拟「查询某人名下机器人」接口，供 sementic worker 联调。

## 接口

```http
GET /api/v1/bots?user_id={user_id}
```

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| `user_id` | query | 是 | 人类用户 ID（Mattermost user id） |

## 响应示例

```json
{
  "code": 0,
  "message": "ok",
  "data": {
    "user_id": "k7hgneowkbybj8zx1q9i6weccw",
    "total": 2,
    "bots": [
      {
        "bot_user_id": "bot_liusong_code",
        "name": "代码助手",
        "description": "帮你写 Python、调试脚本、生成最简单的 AI Agent 示例"
      },
      {
        "bot_user_id": "bot_liusong_devops",
        "name": "运维助手",
        "description": "重启本地脚本、查日志、部署测试环境"
      }
    ]
  }
}
```

缺少 `user_id` 时返回 400：

```json
{
  "code": 40001,
  "message": "missing required query parameter: user_id",
  "data": null
}
```

未知 `user_id` 返回空列表 `bots: []`。

## WSL 部署（安装目录 `/usr/local/openresty`）

### 1. 目录与文件

```bash
sudo mkdir -p /usr/local/openresty/lua
sudo mkdir -p /usr/local/openresty/nginx/conf/conf.d

sudo cp deploy/openresty/bot-management.conf /usr/local/openresty/nginx/conf/conf.d/
sudo cp deploy/openresty/lua/bots.lua /usr/local/openresty/lua/
```

### 2. 修改 nginx.conf

主配置文件：

```text
/usr/local/openresty/nginx/conf/nginx.conf
```

在 **`http {}`** 里加入（若已有 `lua_package_path` 可合并路径）：

```nginx
http {
    lua_package_path "/usr/local/openresty/lua/?.lua;;";

    include /usr/local/openresty/nginx/conf/conf.d/*.conf;

    # ... 其他原有配置 ...
}
```

### 3. 检查并重载

```bash
sudo /usr/local/openresty/bin/openresty -t
sudo /usr/local/openresty/bin/openresty -s reload
```

也可把 `/usr/local/openresty/bin` 加入 `PATH`，之后直接用 `openresty` 命令。

## 测试

```bash
# 刘颂（已配 2 个名下 bot）
curl -s "http://127.0.0.1:8088/api/v1/bots?user_id=k7hgneowkbybj8zx1q9i6weccw"

# 无 bot 的用户
curl -s "http://127.0.0.1:8088/api/v1/bots?user_id=unknown_user"

# 缺参数
curl -s "http://127.0.0.1:8088/api/v1/bots"
```

Windows 访问 WSL 时把 `127.0.0.1` 换成 WSL IP（如 `192.168.233.121`）。

sementic worker 侧只配置服务基址（路径固定）：

```env
SEMENTIC_BOT_SERVICE_BASE=http://192.168.233.121
```

实际请求 URL 由代码拼接：

```text
{SEMENTIC_BOT_SERVICE_BASE}/api/v1/bots?user_id={user_id}
```

## Lua 模块

业务逻辑在 `lua/bots.lua`，按函数组织，当前只实现 `query()`：

```lua
local bots = require "bots"
bots.query()   -- GET /api/v1/bots?user_id=...
-- bots.create()  -- 预留
-- bots.update()  -- 预留
```

location 里通过 `content_by_lua_block` 调用对应函数。

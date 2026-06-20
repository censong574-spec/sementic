-- 机器人管理 mock 模块
-- 部署路径: /usr/local/openresty/lua/bots.lua
--
-- 后续可在此文件继续扩展:
--   bots.query()   查询某人名下机器人
--   bots.create()  创建机器人（预留）
--   bots.update()  更新机器人（预留）

local cjson = require "cjson.safe"

local _M = {}

-- user_id -> bots[]
local MOCK_BOTS = {
    ["k7hgneowkbybj8zx1q9i6weccw"] = {
        {
            bot_user_id = "bot_liusong_code",
            name = "代码助手",
            description = "帮你写 Python、调试脚本、生成最简单的 AI Agent 示例",
        },
        {
            bot_user_id = "bot_liusong_devops",
            name = "运维助手",
            description = "重启本地脚本、查日志、部署测试环境",
        },
    },
    ["usr_hassan_95"] = {
        {
            bot_user_id = "bot_project_assistant",
            name = "项目助手",
            description = "DevOps 协调、重启 CLI、日志分析",
        },
    },
}

local function write_json(status, body)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json; charset=utf-8"
    ngx.say(cjson.encode(body))
end

local function error_response(status, code, message)
    write_json(status, {
        code = code,
        message = message,
        data = cjson.null,
    })
    return ngx.exit(status)
end

-- GET /api/v1/bots?user_id=xxx
function _M.query()
    local args = ngx.req.get_uri_args()
    local user_id = args["user_id"]

    if user_id == nil or user_id == "" then
        return error_response(
            ngx.HTTP_BAD_REQUEST,
            40001,
            "missing required query parameter: user_id"
        )
    end

    local bots = MOCK_BOTS[user_id] or {}

    write_json(ngx.HTTP_OK, {
        code = 0,
        message = "ok",
        data = {
            user_id = user_id,
            total = #bots,
            bots = bots,
        },
    })
end

return _M

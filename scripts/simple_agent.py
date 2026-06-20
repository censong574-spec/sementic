"""
最简单的 AI Agent：维护对话列表，每次把全部历史一起发给模型。

用法:
    set DASHSCOPE_API_KEY=sk-xxx
    python scripts/simple_agent.py

没有 API Key 时走本地 mock，方便先看流程。
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

# 对话历史：每次问答都追加进来，下次整包提交
history: list[dict[str, str]] = []


def call_llm(messages: list[dict[str, str]]) -> str:
    api_key = os.getenv("DASHSCOPE_API_KEY") or os.getenv("SEMENTIC_LLM_API_KEY")
    if not api_key:
        last_user = next((m["content"] for m in reversed(messages) if m["role"] == "user"), "")
        return f"[mock] 我收到了：{last_user}"

    payload = {
        "model": os.getenv("SEMENTIC_LLM_MODEL", "qwen-plus"),
        "messages": [{"role": "system", "content": "你是一个简洁有帮助的助手。"}, *messages],
    }
    request = urllib.request.Request(
        "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        body = json.loads(response.read().decode("utf-8"))
    return body["choices"][0]["message"]["content"]


def ask(user_message: str) -> str:
    # 1. 用户问题入列表
    history.append({"role": "user", "content": user_message})

    # 2. 把到目前为止的全部历史发给 AI
    answer = call_llm(history)

    # 3. AI 回答也入列表
    history.append({"role": "assistant", "content": answer})
    return answer


def main() -> None:
    print("简单 Agent 已启动，输入 exit 退出。\n")
    while True:
        user_input = input("你: ").strip()
        if not user_input:
            continue
        if user_input.lower() in {"exit", "quit", "q"}:
            break

        try:
            reply = ask(user_input)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            print(f"调用失败: {exc.code} {detail}")
            history.pop()  # 去掉这次失败的用户消息
            continue

        print(f"AI: {reply}\n")
        print(f"(当前历史条数: {len(history)})\n")


if __name__ == "__main__":
    main()

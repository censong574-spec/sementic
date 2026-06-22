"""Agent runtime provider 抽象接口。"""

from typing import Any


class AgentRuntimeProvider:
    """不同 Agent backend 的统一适配接口。"""

    backend = ""

    def agent_card(self, node: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def send(self, request: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def poll(self, task_id: str, request: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

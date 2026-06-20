from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class GatewayAction(str, Enum):
    ASYNC_PROCESSING = "ASYNC_PROCESSING"
    FILTERED = "FILTERED"
    BLOCKED = "BLOCKED"


class GatewayIngressData(BaseModel):
    event_id: str
    action_taken: GatewayAction


class GatewayApiResponse(BaseModel):
    code: int = 0
    message: str = "Message processed successfully."
    data: GatewayIngressData


def build_gateway_response(
    event_id: str,
    action_taken: GatewayAction,
    *,
    message: str = "Message processed successfully.",
) -> GatewayApiResponse:
    return GatewayApiResponse(
        code=0,
        message=message,
        data=GatewayIngressData(event_id=event_id, action_taken=action_taken),
    )


def build_gateway_response(
    event_id: str,
    action_taken: GatewayAction,
    *,
    message: str = "Message processed successfully.",
) -> GatewayApiResponse:
    return GatewayApiResponse(
        code=0,
        message=message,
        data=GatewayIngressData(event_id=event_id, action_taken=action_taken),
    )

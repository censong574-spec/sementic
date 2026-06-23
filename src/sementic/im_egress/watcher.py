from __future__ import annotations

import asyncio
import logging
import threading
import time

from sementic.config import ImEgressSettings
from sementic.execution.maos_executor import MaosExecutor
from sementic.im_egress.models import EgressContext
from sementic.im_egress.publisher import ImEgressPublisher
from sementic.im_egress.result import extract_final_reply_text, iter_agent_node_replies

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "terminated", "timed_out"}
)


class MaosCompletionWatcher:
    """Poll MAOS Temporal task and publish node/final replies to Mattermost."""

    def __init__(
        self,
        *,
        maos_executor: MaosExecutor,
        egress_publisher: ImEgressPublisher,
        settings: ImEgressSettings | None = None,
    ) -> None:
        self.maos_executor = maos_executor
        self.egress_publisher = egress_publisher
        self.settings = settings or ImEgressSettings()

    def watch(self, *, task_id: str, context: EgressContext) -> None:
        thread = threading.Thread(
            target=self._run_watch,
            name=f"maos-egress-{task_id[:24]}",
            args=(task_id, context),
            daemon=True,
        )
        thread.start()

    def _run_watch(self, task_id: str, context: EgressContext) -> None:
        poll_seconds = self.settings.poll_interval_seconds
        deadline = time.time() + self.settings.completion_timeout_seconds
        published_nodes: set[str] = set()
        published_texts: set[str] = set()
        logger.info(
            "maos egress watch started task_id=%s event_id=%s bot=%s",
            task_id,
            context.event_id,
            context.bot_user_id,
        )

        while time.time() < deadline:
            try:
                snapshot = self.maos_executor.task_snapshot(task_id)
            except KeyError:
                time.sleep(poll_seconds)
                continue
            except Exception:
                logger.exception("maos egress watch poll failed task_id=%s", task_id)
                time.sleep(poll_seconds)
                continue

            self._publish_new_node_replies(context, snapshot, published_nodes, published_texts)

            status = str(snapshot.get("status") or "").lower()
            if status not in _TERMINAL_STATUSES:
                time.sleep(poll_seconds)
                continue

            if status == "completed":
                final_text = extract_final_reply_text(snapshot)
                if final_text and final_text not in published_texts:
                    asyncio.run(self.egress_publisher.publish_final(context, final_text))
                elif not published_nodes and not final_text:
                    asyncio.run(
                        self.egress_publisher.publish_error(
                            context,
                            "任务已完成，但没有可展示的结果。",
                        )
                    )
            else:
                error = snapshot.get("error")
                message = (
                    f"任务执行失败（{status}）"
                    if not error
                    else f"任务执行失败：{error}"
                )
                asyncio.run(self.egress_publisher.publish_error(context, message))

            logger.info(
                "maos egress watch finished task_id=%s status=%s event_id=%s",
                task_id,
                status,
                context.event_id,
            )
            return

        logger.warning(
            "maos egress watch timeout task_id=%s event_id=%s",
            task_id,
            context.event_id,
        )
        asyncio.run(
            self.egress_publisher.publish_error(
                context,
                "任务执行超时，请稍后在 Temporal 查看详情。",
            )
        )

    def _publish_new_node_replies(
        self,
        context: EgressContext,
        snapshot: dict,
        published_nodes: set[str],
        published_texts: set[str],
    ) -> None:
        for node_id, text in iter_agent_node_replies(snapshot):
            if node_id in published_nodes:
                continue
            asyncio.run(self.egress_publisher.publish_node(context, node_id, text))
            published_nodes.add(node_id)
            published_texts.add(text)

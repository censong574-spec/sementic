from __future__ import annotations

from sementic.im_models import MentionRegistryItem, Ownership
from sementic.models import BotProfile


DEFAULT_BOT_REGISTRY: dict[str, BotProfile] = {
    "bot_jira_123": BotProfile(
        bot_user_id="bot_jira_123",
        display_name="Jira-Helper",
        role="Issue tracking assistant",
        expertise=["create_ticket", "update_ticket", "jira_workflow"],
        owner_user_id=None,
        share_scope="channel_shared",
        multica_agent_id="multica_jira_001",
        is_online=True,
    ),
    "bot_bug_hunter_01": BotProfile(
        bot_user_id="bot_bug_hunter_01",
        display_name="Bug-Hunter",
        role="Log analysis assistant",
        expertise=["log_analysis", "query", "restart_cli"],
        owner_user_id=None,
        share_scope="private",
        multica_agent_id="multica_bug_001",
        is_online=True,
    ),
    "bot_project_assistant": BotProfile(
        bot_user_id="bot_project_assistant",
        display_name="项目助手",
        role="DevOps coordinator",
        expertise=["deploy", "restart_cli", "log_analysis"],
        owner_user_id="usr_hassan_95",
        share_scope="channel_shared",
        multica_agent_id="multica_agent_001",
        is_online=True,
        can_arbitrate=True,
    ),
}


class BotRegistry:
    def __init__(self, profiles: dict[str, BotProfile] | None = None) -> None:
        if profiles is None:
            self._profiles = DEFAULT_BOT_REGISTRY.copy()
        else:
            self._profiles = profiles

    def register(self, profile: BotProfile) -> None:
        self._profiles[profile.bot_user_id] = profile

    def get(self, bot_user_id: str) -> BotProfile | None:
        return self._profiles.get(bot_user_id)

    def list_owned_bots(self, sender_user_id: str) -> list[BotProfile]:
        """Bots owned by the speaker (发言人名下)."""
        return [
            profile
            for profile in self._profiles.values()
            if profile.owner_user_id == sender_user_id
        ]

    def resolve_owned_bots_for_orchestration(
        self,
        *,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
    ) -> list[BotProfile]:
        """Owned bots for stage-2 orchestration; @MY_SYSTEM mentions are included."""
        bots_by_id = {
            profile.bot_user_id: profile
            for profile in self.list_owned_bots(sender_user_id)
        }

        for mention in mentions:
            if mention.ownership != Ownership.MY_SYSTEM:
                continue
            base = bots_by_id.get(mention.entity_id) or self._profiles.get(mention.entity_id)
            if base is None:
                base = BotProfile(
                    bot_user_id=mention.entity_id,
                    display_name=mention.entity_id,
                    role="assistant",
                    expertise=["general"],
                    owner_user_id=sender_user_id,
                    share_scope="private",
                )
            else:
                base = self._apply_mention_context(base, sender_user_id, mention)
            bots_by_id[mention.entity_id] = base

        return list(bots_by_id.values())

    def list_usable_bots(self, sender_user_id: str) -> list[BotProfile]:
        """Bots the sender may invoke in this channel (shared or owned)."""
        usable: list[BotProfile] = []
        for profile in self._profiles.values():
            if profile.share_scope == "channel_shared" or profile.owner_user_id == sender_user_id:
                usable.append(profile)
        return usable

    def resolve_available_bots(
        self,
        *,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
    ) -> list[BotProfile]:
        """Channel-visible bots for intent planning; @ only adjusts ownership context."""
        bots_by_id = {
            profile.bot_user_id: profile
            for profile in self.list_usable_bots(sender_user_id)
        }

        for mention in mentions:
            base = bots_by_id.get(mention.entity_id)
            if base is None:
                base = BotProfile(
                    bot_user_id=mention.entity_id,
                    display_name=mention.entity_id,
                    role="assistant",
                    expertise=["general"],
                    owner_user_id=sender_user_id
                    if mention.ownership == Ownership.MY_SYSTEM
                    else None,
                    share_scope="channel_shared"
                    if mention.ownership == Ownership.OTHERS
                    else "private",
                )
            else:
                base = self._apply_mention_context(base, sender_user_id, mention)
            bots_by_id[mention.entity_id] = base

        return list(bots_by_id.values())

    def resolve_for_event(
        self,
        *,
        sender_user_id: str,
        mentions: list[MentionRegistryItem],
    ) -> list[BotProfile]:
        resolved: list[BotProfile] = []
        seen: set[str] = set()

        for mention in mentions:
            if mention.entity_id in seen:
                continue

            base = self._profiles.get(mention.entity_id)
            if base is None:
                base = BotProfile(
                    bot_user_id=mention.entity_id,
                    display_name=mention.entity_id,
                    role="assistant",
                    expertise=["general"],
                    owner_user_id=sender_user_id
                    if mention.ownership == Ownership.MY_SYSTEM
                    else None,
                    share_scope="channel_shared"
                    if mention.ownership == Ownership.OTHERS
                    else "private",
                )
            else:
                base = self._apply_mention_context(base, sender_user_id, mention)

            resolved.append(base)
            seen.add(mention.entity_id)

        return resolved

    @staticmethod
    def _apply_mention_context(
        profile: BotProfile,
        sender_user_id: str,
        mention: MentionRegistryItem,
    ) -> BotProfile:
        if mention.ownership == Ownership.MY_SYSTEM:
            return profile.model_copy(
                update={
                    "owner_user_id": sender_user_id,
                    "share_scope": "private",
                }
            )
        return profile.model_copy(update={"share_scope": "channel_shared"})

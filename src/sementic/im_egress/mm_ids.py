from __future__ import annotations

import re

_MATTERMOST_POST_ID = re.compile(r"^[a-z0-9]{26}$")


def mattermost_post_id(value: str | None) -> str:
    """Return value only when it is a Mattermost post id (26-char alphanumeric)."""
    candidate = (value or "").strip().lower()
    if _MATTERMOST_POST_ID.fullmatch(candidate):
        return candidate
    return ""

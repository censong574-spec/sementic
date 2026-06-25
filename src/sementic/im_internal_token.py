from __future__ import annotations

import os
from pathlib import Path

IM_INTERNAL_TOKEN_FILE = "/etc/mattermost-hermes-im-internal-token"


def resolve_im_internal_token() -> str:
    for key in ("MULTICA_IM_INTERNAL_TOKEN", "HERMES_INTERNAL_BRIDGE_TOKEN"):
        token = os.environ.get(key, "").strip()
        if token:
            return token
    try:
        return Path(IM_INTERNAL_TOKEN_FILE).read_text(encoding="utf-8").strip()
    except OSError:
        return ""

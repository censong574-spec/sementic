from __future__ import annotations

import pytest

from sementic.im_internal_token import resolve_im_internal_token


def test_resolve_im_internal_token_prefers_multica_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MULTICA_IM_INTERNAL_TOKEN", "mul-token")
    monkeypatch.setenv("HERMES_INTERNAL_BRIDGE_TOKEN", "hermes-token")
    assert resolve_im_internal_token() == "mul-token"


def test_resolve_im_internal_token_falls_back_to_hermes_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MULTICA_IM_INTERNAL_TOKEN", raising=False)
    monkeypatch.setenv("HERMES_INTERNAL_BRIDGE_TOKEN", "hermes-token")
    assert resolve_im_internal_token() == "hermes-token"


def test_resolve_im_internal_token_reads_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("MULTICA_IM_INTERNAL_TOKEN", raising=False)
    monkeypatch.delenv("HERMES_INTERNAL_BRIDGE_TOKEN", raising=False)
    token_file = tmp_path / "im-internal-token"
    token_file.write_text("file-token\n", encoding="utf-8")
    monkeypatch.setattr(
        "sementic.im_internal_token.IM_INTERNAL_TOKEN_FILE",
        str(token_file),
    )
    assert resolve_im_internal_token() == "file-token"

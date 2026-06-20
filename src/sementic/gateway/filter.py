from __future__ import annotations

import re
import unicodedata

NOISE_EXACT: frozenset[str] = frozenset(
    {
        "哈",
        "哈哈",
        "哈哈哈",
        "呃",
        "额",
        "嗯",
        "哦",
        "啊",
        "111",
        "1111",
        "666",
        "6666",
        "+1",
        "ok",
        "OK",
        "Ok",
        "？",
        "?",
        "。",
        ".",
        "…",
        "...",
    }
)

PURE_PUNCTUATION_RE = re.compile(r"^[\s\W_]+$", re.UNICODE)
PURE_DIGITS_RE = re.compile(r"^\d+$")
REPEAT_CHAR_RE = re.compile(r"^(.)\1{3,}$")


def normalize_for_filter(text: str) -> str:
    return unicodedata.normalize("NFKC", text).strip()


class StaticNoiseFilter:
    """Layer-1 O(1) gateway filter for low-value chatter."""

    def __init__(self, extra_noise_terms: frozenset[str] | None = None) -> None:
        self.noise_terms = NOISE_EXACT | (extra_noise_terms or frozenset())

    def is_noise(self, text: str) -> tuple[bool, str | None]:
        normalized = normalize_for_filter(text)
        if not normalized:
            return True, "empty_message"

        if normalized in self.noise_terms:
            return True, "exact_noise_term"

        if PURE_PUNCTUATION_RE.fullmatch(normalized):
            return True, "pure_punctuation"

        if PURE_DIGITS_RE.fullmatch(normalized):
            return True, "pure_digits"

        if len(normalized) <= 2 and normalized in self.noise_terms:
            return True, "short_noise"

        if REPEAT_CHAR_RE.fullmatch(normalized):
            return True, "repeated_character"

        return False, None

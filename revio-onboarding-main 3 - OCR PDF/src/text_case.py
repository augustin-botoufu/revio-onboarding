"""Smart title-case for human-readable text fields.

The goal is to clean up the SHOUTING-CAPS that fleet manager exports
typically contain (``RENAULT``, ``DUPONT JEAN-PIERRE``, ``LILLE``…) into
something a Revio user is happy to look at, **without breaking the
acronyms and abbreviations** that genuinely belong in uppercase.

Examples ::

    smart_title_case("RENAULT")           → "Renault"
    smart_title_case("Renault V")         → "Renault V"     # "V" stays UC
    smart_title_case("PEUGEOT 208 II")    → "Peugeot 208 II"
    smart_title_case("BMW")               → "BMW"           # 3-letter acronym
    smart_title_case("VW Bank")           → "VW Bank"
    smart_title_case("JEAN-PIERRE")       → "Jean-Pierre"
    smart_title_case("D'ARTAGNAN")        → "D'Artagnan"
    smart_title_case("MARIE-FRANCE")      → "Marie-France"
    smart_title_case("DE LA FAYETTE")     → "De La Fayette"
    smart_title_case("12 RUE DE LA PAIX") → "12 Rue De La Paix"

The implementation is intentionally **conservative** : when in doubt,
we keep the original. False positives (= lowercasing something that
should have stayed uppercase) are worse than false negatives because
the AM can re-edit a single record but not chase down every silently
mangled name.

Public API
----------
:func:`smart_title_case` — main entry point.
:func:`is_title_case_target` — quick predicate for "should this column
be touched ?". Useful in the post-passes that walk the engine output
DataFrame.
"""

from __future__ import annotations

import re
from typing import Any


# Roman numerals from II to XX — kept uppercase when originally
# uppercase. ``I`` alone is excluded (it doubles as a single letter,
# and is already covered by the « single uppercase letter » rule).
_ROMAN_NUMERALS = frozenset({
    "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
    "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX", "XX",
})


# Whitelist of acronyms / brand abbreviations to keep in uppercase.
# We use an explicit list rather than a generic « short word » heuristic
# because too many short French words (DE, LA, LE, OU, ET, SUR, ...)
# would falsely register as acronyms. Extend this list as we discover
# new ones in the field.
_ACRONYMS_KEEP_UPPER = frozenset({
    # Auto brands & sub-brands
    "BMW", "MG", "VW", "AMG", "KTM", "DS", "JLR",
    # Auto trims / variants commonly written in caps
    "GT", "GTC", "GTV", "GTR", "GTS", "GTI", "GTD",
    "RS", "ST", "SE",
    "TDI", "TFSI", "FSI", "CDI", "VTI", "HDI", "FAP", "DPF",
    "BVA", "BVM", "BVR",   # Boîte de vitesse (auto/manuelle/robotisée)
    "ABS", "ESP", "GPS", "USB", "AC", "DC",
    # Vehicle classification codes
    "VP", "VU", "VS", "VL", "PL", "VTC", "VR", "TT", "SUV",
    "VASP", "DERIV", "CTTE", "BOM",
    # Fuel codes
    "GO", "GNV", "GPL", "EH",
    # Misc
    "RDV", "TVA", "HT", "TTC", "EUR", "USD",
    # Legal entities (companies)
    "SARL", "SAS", "SA", "SCI", "SNC", "SCS",
    "SASU", "EURL", "SCEA", "GAEC", "EARL",
})


# Splits on hyphens and apostrophes (both straight ' and curly '),
# capturing the separator so we can rebuild the word verbatim.
_SUBWORD_SPLIT_RE = re.compile(r"([\-’'])")


def _alpha_only_len(token: str) -> int:
    return sum(1 for c in token if c.isalpha())


def _smart_title_word(word: str) -> str:
    """Title-case a single space-delimited word.

    Rules applied in order :

    1. Empty word → as-is.
    2. Word contains a digit → as-is. Covers ``208``, ``5008``, VIN
       fragments, model codes etc.
    3. Word matches a known acronym (:data:`_ACRONYMS_KEEP_UPPER`)
       AND was uppercase → keep uppercase.
    4. Word is a known Roman numeral (II..XX) AND was uppercase → keep
       uppercase. Handles ``Peugeot 208 II``.
    5. Word is a SINGLE alphabetic character originally uppercase →
       keep uppercase. Handles ``Renault V`` so that V doesn't become v.
    6. Otherwise → lowercase the whole word, capitalise the first
       letter of each subword (subwords split on hyphens / apostrophes
       — handles ``JEAN-PIERRE`` → ``Jean-Pierre`` and ``D'ARTAGNAN``
       → ``D'Artagnan``).
    """
    if not word:
        return word
    if any(c.isdigit() for c in word):
        return word

    upper = word.upper()
    if upper in _ACRONYMS_KEEP_UPPER and word == upper:
        return upper
    if upper in _ROMAN_NUMERALS and word == upper:
        return upper
    # Single uppercase letter (e.g. "V" in "Renault V").
    if word == upper and _alpha_only_len(word) == 1:
        return upper

    # General case : lowercase + capitalise after each separator.
    parts = _SUBWORD_SPLIT_RE.split(word.lower())
    rebuilt: list[str] = []
    for p in parts:
        if not p:
            rebuilt.append(p)
            continue
        if p in {"-", "'", "’"}:
            rebuilt.append(p)
            continue
        rebuilt.append(p[:1].upper() + p[1:])
    return "".join(rebuilt)


def smart_title_case(value: Any) -> Any:
    """Apply :func:`_smart_title_word` to every space-delimited word.

    Returns the input unchanged when it isn't a string, when it's
    blank, or when applying the rule wouldn't change anything (so the
    caller can cheaply detect « no edit » without comparing strings).
    """
    if value is None:
        return value
    if not isinstance(value, str):
        return value
    s = value.strip()
    if not s:
        return value
    # Preserve internal multiple spaces / tabs so we don't lossily
    # collapse "DUPONT  JEAN" → "Dupont Jean" when the source had
    # double space (unlikely but courteous).
    parts = re.split(r"(\s+)", s)
    out_parts: list[str] = [_smart_title_word(p) if not p.isspace() else p for p in parts]
    return "".join(out_parts)


# =============================================================================
# Per-table column registries
# =============================================================================
# Lists of column names where smart_title_case should be applied. Kept
# here (rather than in each engine) so we have one place to extend when
# Revio adds new text fields. Anything NOT listed is left alone — VINs,
# plate numbers, emails, country codes, UUIDs, enum values etc. all
# have their own dedicated normalisation paths.

VEHICLE_TEXT_COLUMNS = (
    "brand",
    "model",
    "variant",
)

DRIVER_TEXT_COLUMNS = (
    "firstName",
    "lastName",
    "birthCity",
    "street",
    "city",
    "licenseIssueLocation",
)

CONTRACT_TEXT_COLUMNS: tuple[str, ...] = ()  # No free-form text fields today.


def is_title_case_target(table: str, column: str) -> bool:
    """Convenience predicate for the engine post-passes."""
    if table == "vehicle":
        return column in VEHICLE_TEXT_COLUMNS
    if table == "driver":
        return column in DRIVER_TEXT_COLUMNS
    if table == "contract":
        return column in CONTRACT_TEXT_COLUMNS
    return False

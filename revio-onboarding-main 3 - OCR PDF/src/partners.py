"""Mapping of lessor / partner names to Revio partnerId UUIDs.

The source of truth is :file:`src/rules/partner_index.csv` — a 2-column
CSV (``PARTNER_ID,PARTNER_NAME``) maintained from the Revio backoffice.
We load it at import time and build the canonical :data:`PARTNERS`
registry from there. **Editing the CSV is enough** to add a new partner
— no Python change required, and it can be done via Mode Dev with PR
review.

Aliases live in :data:`_PARTNER_ALIASES` below. The canonical key is
the lowercase form of the CSV ``PARTNER_NAME`` (e.g. ``"ayvens"``,
``"arval"``). For France we always pick the non-suffixed entry
(``Arval`` rather than ``Arval BE``, ``Ayvens`` rather than ``Ayvens NL``).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional


# Path to the CSV source of truth. Resolved relative to this file so the
# module is portable (no cwd dependency).
_PARTNER_INDEX_PATH = Path(__file__).parent / "rules" / "partner_index.csv"


# Aliases — string variants that should resolve to the same canonical
# partner key. Lowercased on both sides. The canonical key MUST exist in
# the CSV or the alias is silently ignored.
_PARTNER_ALIASES: dict[str, list[str]] = {
    # Canonical key (must match a PARTNER_NAME from the CSV, lowercased) :
    # list of free-form aliases that should resolve to the same UUID.
    "ayvens":           ["ald", "ald automotive", "leaseplan", "ayvens sa"],
    "arval":            ["arval bnp", "arval service lease"],
    "leasys":           ["stellantis", "free2move"],
    "volkswagen bank":  ["vw bank", "vwfs", "volkswagen financial services",
                         "vw_bank"],
    "myvee":            ["myvee / tesla"],
}


def _load_partner_index() -> dict[str, dict]:
    """Read the CSV and return a ``{lowercase_name: {partnerId, display}}`` dict.

    Returns an empty dict when the CSV is missing — the engine will then
    leave partnerId empty for every contract, surfacing the issue
    visibly rather than failing silently with random Python errors.
    """
    out: dict[str, dict] = {}
    if not _PARTNER_INDEX_PATH.exists():
        return out
    with open(_PARTNER_INDEX_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uuid = (row.get("PARTNER_ID") or "").strip()
            name = (row.get("PARTNER_NAME") or "").strip()
            if not uuid or not name:
                continue
            key = name.lower()
            # First entry wins on duplicate keys (which would be a CSV
            # data quality issue — log for now, don't block).
            if key in out:
                continue
            out[key] = {"partnerId": uuid, "display": name}
    # Wire up aliases so callers can resolve "ALD" → Ayvens, etc.
    for canonical, aliases in _PARTNER_ALIASES.items():
        if canonical not in out:
            continue
        for alias in aliases:
            alias_key = alias.lower()
            if alias_key in out:
                continue
            out[alias_key] = out[canonical]
    return out


# Build PARTNERS once at import time. Re-import the module to refresh
# (or call :func:`reload_partner_index` at runtime if needed).
PARTNERS: dict[str, dict] = _load_partner_index()


def reload_partner_index() -> None:
    """Re-read the CSV and refresh :data:`PARTNERS`. Mostly for tests."""
    global PARTNERS
    PARTNERS = _load_partner_index()


# Mapping from engine source slug → partner key (Jalon 5.2.2).
# Used by ``contract_engine._postpass_resolve_partner_id`` to fill the
# Revio ``partnerId`` UUID for each contract row, based on which lessor
# slug provided the contract data. ``None`` means "leave empty — let the
# user fill it via Mode Dev" (typically for the autre_loueur_* slugs
# where we don't know which actual lessor the file came from).
SLUG_TO_PARTNER: dict[str, Optional[str]] = {
    # Arval
    "arval_uat":              "arval",
    "arval_aen":              "arval",
    "arval_tvu":              "arval",
    "arval_and":              "arval",
    "arval_pneus":            "arval",
    "arval_facture_pdf":      "arval",
    # Ayvens
    "ayvens_etat_parc":       "ayvens",
    "ayvens_aen":             "ayvens",
    "ayvens_tvs":             "ayvens",
    "ayvens_and":             "ayvens",
    "ayvens_pneus":           "ayvens",
    "ayvens_facture_pdf":     "ayvens",
    # Autre loueur — unknown until the user labels it; we leave partnerId
    # empty rather than guess, so the import doesn't silently use the
    # wrong UUID.
    "autre_loueur_etat_parc":   None,
    "autre_loueur_aen":         None,
    "autre_loueur_tvs":         None,
    "autre_loueur_and":         None,
    "autre_loueur_pneus":       None,
    "autre_loueur_facture_pdf": None,
}


def resolve_partner_id_for_slug(slug: str) -> Optional[str]:
    """Return the Revio partnerId UUID matching an engine source slug.

    Returns ``None`` when the slug is unknown OR when the slug is a
    deliberate « autre loueur » placeholder (we don't want to guess the
    wrong partner there).
    """
    if not slug:
        return None
    partner_key = SLUG_TO_PARTNER.get(slug)
    if not partner_key:
        return None
    info = PARTNERS.get(partner_key)
    if not info:
        return None
    return info.get("partnerId") or None


def resolve_partner_id(name: str) -> Optional[str]:
    """Return the Revio partnerId UUID for a free-form partner name.

    Used outside the engine slug pathway — for example when a column
    in the input file declares the partner by name (Total / Shell /
    Ulys / …) rather than via a known slug. Matches case-insensitively
    against canonical names AND aliases.
    """
    if not name:
        return None
    key = name.strip().lower()
    info = PARTNERS.get(key)
    if info:
        return info.get("partnerId") or None
    # Fallback: substring match (e.g. "Ayvens SA" contains "ayvens").
    for partner_key, partner_info in PARTNERS.items():
        if partner_key in key:
            return partner_info.get("partnerId") or None
    return None


def resolve_partner_display(name: str) -> str:
    """Return the canonical display name for a free-form partner name."""
    if not name:
        return ""
    key = name.strip().lower()
    info = PARTNERS.get(key)
    if info:
        return info.get("display") or name
    for partner_key, partner_info in PARTNERS.items():
        if partner_key in key:
            return partner_info.get("display") or name
    return name


def list_known_partners() -> list[tuple[str, str]]:
    """Return ``[(display_name, partnerId), ...]`` for the UI / debug.

    De-duplicated on partnerId so aliases don't pollute the listing.
    Sorted alphabetically by display name.
    """
    seen_ids: set[str] = set()
    rows: list[tuple[str, str]] = []
    for info in PARTNERS.values():
        pid = info.get("partnerId") or ""
        if pid in seen_ids:
            continue
        seen_ids.add(pid)
        rows.append((info.get("display") or "", pid))
    rows.sort(key=lambda r: r[0].lower())
    return rows

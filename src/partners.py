"""Mapping of lessor / partner names to Revio partnerId UUIDs.

These UUIDs come from the Revio backoffice. Extend this registry as new
partners appear. The matching on lessor names is fuzzy (lowercased + strip)
so typical variants (e.g. "Ayvens", "AYVENS", "ayvens ") all match.
"""

from typing import Optional

# Canonical partner registry.
# key: lowercase slug
# value: dict with partnerId and display name
PARTNERS: dict[str, dict] = {
    "ayvens": {
        "partnerId": "7b9c0e4f-b06c-44d3-8172-53a4f510e24c",
        "display": "Ayvens",
        "aliases": ["ald", "ald automotive", "leaseplan", "ayvens sa"],
    },
    "arval": {
        "partnerId": "66c797eb-41e3-4d42-9ed7-e8f1563bcea2",
        "display": "Arval",
        "aliases": ["arval bnp", "arval service lease"],
    },
    # Placeholders - fill in real UUIDs as they get assigned in the Revio backoffice.
    "alphabet": {
        "partnerId": "",
        "display": "Alphabet",
        "aliases": [],
    },
    "leasys": {
        "partnerId": "",
        "display": "Leasys",
        "aliases": ["stellantis", "free2move"],
    },
    "credipar": {
        "partnerId": "",
        "display": "Credipar",
        "aliases": [],
    },
    "vw_bank": {
        "partnerId": "",
        "display": "VW Bank",
        "aliases": ["volkswagen bank", "vwfs", "volkswagen financial services"],
    },
    "cic_bail": {
        "partnerId": "",
        "display": "CIC Bail",
        "aliases": [],
    },
    "loc_action": {
        "partnerId": "",
        "display": "Loc-Action",
        "aliases": ["locaction"],
    },
    "myvee": {
        "partnerId": "",
        "display": "Myvee",
        "aliases": ["myvee / tesla"],
    },
}


# Mapping from engine source slug → partner key (Jalon 5.2.2).
# Used by ``contract_engine._postpass_resolve_partner_id`` to fill the
# Revio ``partnerId`` UUID for each contract row, based on which lessor
# slug provided the contract data. ``None`` means "leave empty — let the
# user fill it via mode dev" (typically for the autre_loueur_* slugs
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
    """Return the Revio partnerId UUID for a lessor name, or None if unknown."""
    if not name:
        return None
    key = name.strip().lower()
    # Direct match.
    if key in PARTNERS:
        return PARTNERS[key]["partnerId"] or None
    # Match via aliases.
    for slug, info in PARTNERS.items():
        if key == slug or key in info.get("aliases", []):
            return info["partnerId"] or None
        # Partial match - e.g. "ayvens sa" contains "ayvens"
        if slug in key or any(a in key for a in info.get("aliases", [])):
            return info["partnerId"] or None
    return None


def resolve_partner_display(name: str) -> str:
    """Return a canonical display name for a lessor."""
    if not name:
        return ""
    key = name.strip().lower()
    if key in PARTNERS:
        return PARTNERS[key]["display"]
    for slug, info in PARTNERS.items():
        if key == slug or key in info.get("aliases", []):
            return info["display"]
        if slug in key or any(a in key for a in info.get("aliases", [])):
            return info["display"]
    return name  # unknown - keep original

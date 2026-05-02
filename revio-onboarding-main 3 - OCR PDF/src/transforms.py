"""Registry of transformation functions referenced by rules YAML.

Each transform takes a single raw value (str / number / None) and returns
a tuple (normalized_value, list_of_warnings). Warnings are short strings
appended to the issues.csv output.

The registry TRANSFORMS maps the string identifier used in vehicle.yml
to the actual callable.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional, Tuple

from . import normalizers


Warning = str
TransformFn = Callable[[Any], Tuple[Any, list[Warning]]]


# ---------- Helpers ----------


def _is_empty(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"nan", "none", "null", "n/a", "-", "/"}


# ---------- Basic transforms ----------


def passthrough(v: Any) -> Tuple[Any, list[Warning]]:
    if _is_empty(v):
        return None, []
    return v, []


def uppercase(v: Any) -> Tuple[Optional[str], list[Warning]]:
    if _is_empty(v):
        return None, []
    return str(v).strip().upper(), []


def title_case(v: Any) -> Tuple[Optional[str], list[Warning]]:
    if _is_empty(v):
        return None, []
    # "INTENS" -> "Intens", "gt line" -> "Gt Line"
    return str(v).strip().title(), []


def to_int(v: Any) -> Tuple[Optional[int], list[Warning]]:
    if _is_empty(v):
        return None, []
    amount, warns = normalizers.normalize_amount(v)
    if amount is None:
        return None, warns
    try:
        return int(round(amount)), warns
    except (TypeError, ValueError):
        warns.append(f"Valeur non convertible en entier: {v!r}")
        return None, warns


# ---------- Dates ----------


def normalize_date(v: Any) -> Tuple[Optional[str], list[Warning]]:
    return normalizers.normalize_date(v)


# Aliases: several rules in contract.yml use more specific names to signal
# intent (``date_fr_to_iso``) or to accept any parseable date
# (``date_any_to_iso``). Our normalizer already tries FR-first dayfirst and
# ISO formats, so they're all the same function. Registered explicitly so
# that values don't leak through as pandas Timestamps ("2023-03-27
# 00:00:00") — Bug Augustin Jalon 4.2.7.
parse_date_fr = normalize_date
date_iso = normalize_date
date_any_to_iso = normalize_date
date_fr_to_iso = normalize_date


# ---------- Strings / numbers (generic) ----------


def strip(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Trim whitespace. Pure string cleanup used in the YAML for identifier
    fields (N° Contrat, references, driver refs)."""
    if _is_empty(v):
        return None, []
    return str(v).strip(), []


def float_fr(v: Any) -> Tuple[Optional[float], list[Warning]]:
    """French-convention float parsing (comma decimal, spaces as thousand
    separators, currency suffix tolerated). Reuses ``normalize_amount``."""
    return normalizers.normalize_amount(v)


# The YAML distinguishes ``float_fr`` (unit prices, totals) from
# ``float_fr_rubrique`` (amounts per rubrique in factures). Engine-side
# the parsing is identical — the distinction is only for lineage.
float_fr_rubrique = float_fr


# ---------- Booleans ----------


_BOOL_TRUE = {"true", "vrai", "oui", "yes", "y", "1", "t", "ok", "x", "check"}
_BOOL_FALSE = {"false", "faux", "non", "no", "n", "0", "f"}


def bool_fr(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Parse FR / EN boolean-ish value to ``'TRUE'`` / ``'FALSE'``.

    Output matches Revio spec's ``TRUE`` / ``FALSE`` uppercase strings
    (see contract.yml enum declarations). Returns None when unparseable
    so downstream rule priorities can try the next source.
    """
    if _is_empty(v):
        return None, []
    if isinstance(v, bool):
        return ("TRUE" if v else "FALSE"), []
    if isinstance(v, (int, float)):
        return ("TRUE" if v else "FALSE"), []
    s = str(v).strip().lower()
    if s in _BOOL_TRUE:
        return "TRUE", []
    if s in _BOOL_FALSE:
        return "FALSE", []
    return None, [f"Booléen non reconnu: {v!r}"]


# ---------- Prestation / Maintenance rules ----------


# Blacklist of "no prestation" values that must be read as FALSE even though
# the cell is non-empty. Two kinds of markers:
#   - substring markers : long enough to be unambiguous inside any cell
#   - exact markers : short strings (like "nc") that must match the whole
#     trimmed value, otherwise they fire inside words (e.g. "nc" inside
#     "maintenance" — bug caught in Jalon 4.2.7 testing).
_PRESTATION_ABSENT_SUBSTRINGS = (
    "aucune prestation",
    "aucun entretien",
    "non souscrite",
    "non souscrit",
    "pas de prestation",
    "pas de maintenance",
    "pas d'entretien",
    "hors prestation",
    "sans prestation",
    "sans maintenance",
)
_PRESTATION_ABSENT_EXACT = {"aucun", "aucune", "non", "nc", "n/a", "na",
                            "-", "/"}


def _is_prestation_absent(s: str) -> bool:
    low = s.strip().lower()
    if not low:
        return True
    if low in _PRESTATION_ABSENT_EXACT:
        return True
    for marker in _PRESTATION_ABSENT_SUBSTRINGS:
        if marker in low:
            return True
    return False


def rule_field_present(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Returns ``'TRUE'`` if the cell holds a non-blacklisted value,
    ``'FALSE'`` otherwise. Used by ``maintenanceEnabled``, ``tiresEnabled``
    etc. where the source says either the contract name ("réseau ayvens")
    or a sentinel like "aucune prestation"."""
    if _is_empty(v):
        return "FALSE", []
    if _is_prestation_absent(str(v)):
        return "FALSE", []
    return "TRUE", []


def rule_source_present(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Similar to ``rule_field_present`` but the YAML already guarantees
    the row exists in the source file (e.g. plate listed in Ayvens Pneus),
    so any non-empty value means TRUE."""
    if _is_empty(v):
        return None, []  # caller should treat as "row absent"
    return "TRUE", []


# ---------- Enum mappings ----------


def enum_mapping(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Catégorie VR (A-H). Accepts either a single letter or a word.
    Values longer than 1 char are passed through uppercased so user-edited
    client files keep working. Returns None when empty."""
    if _is_empty(v):
        return None, []
    s = str(v).strip().upper()
    if len(s) == 1 and s.isalpha() and s <= "H":
        return s, []
    # Tolerate labels "Catégorie A", "CAT. B" etc.
    m = re.search(r"\b([A-H])\b", s)
    if m:
        return m.group(1), []
    return s, [f"Catégorie VR non standard: {v!r}"]


def enum_mapping_2(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Map a 'Maintenance souscrite' text (loueur-specific vocabulary) to
    Revio's 2-value enum ``any`` / ``specialist``.

    Rules (derived from spec_contract_v2 Q3 + Ayvens EP real data):
    - empty / "aucune prestation" → None (no prestation at all, caller
      sets maintenanceEnabled=FALSE)
    - "réseau ayvens" / "réseau <loueur>" / "any" → any (generic network)
    - "autre prestation …" / "specialist" / "spécialiste" / "marque" /
      "concession" / "constructeur" → specialist
    - other non-empty values → any (safe default: prestation exists, no
      specialist signal found → falls back to the generic network).
    """
    if _is_empty(v):
        return None, []
    s = str(v).strip()
    if _is_prestation_absent(s):
        return None, []
    low = s.lower()
    # Specialist signals — external / constructor / named brand networks.
    if any(k in low for k in ("autre prestation", "specialist", "spécialiste",
                              "specialiste", "special", "spécial",
                              "concession", "marque", "constructeur",
                              "réseau constructeur", "reseau constructeur")):
        return "specialist", []
    return "any", []


def enum_mapping_tires(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Map tire prestation label to Revio enum ``standard`` / ``winter``
    / ``4seasons``."""
    if _is_empty(v):
        return None, []
    s = str(v).strip().lower()
    if _is_prestation_absent(s):
        return None, []
    if any(k in s for k in ("4 saisons", "4saisons", "quatre saisons",
                            "all-season", "all season", "4s", "toutes saisons")):
        return "4seasons", []
    if any(k in s for k in ("hiver", "neige", "winter", "snow", "m+s", "3pmsf")):
        return "winter", []
    if any(k in s for k in ("été", "ete", "summer", "standard", "route",
                            "tourisme", "mixte", "pneu standard")):
        return "standard", []
    # Default when something is declared but unrecognized: assume standard
    # (most common contract prestation) and warn.
    return "standard", [f"Type pneus non reconnu: {v!r} — 'standard' par défaut"]


# ---------- Plate / VIN / Country ----------


def normalize_plate(v: Any) -> Tuple[Optional[str], list[Warning]]:
    return normalizers.normalize_plate(v)


def normalize_vin(v: Any) -> Tuple[Optional[str], list[Warning]]:
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = re.sub(r"\s+", "", str(v)).upper()
    if len(s) != 17:
        warnings.append(f"VIN longueur {len(s)} != 17 ({v!r})")
        return s or None, warnings
    # VIN does not contain I, O, Q (ISO 3779) — flag if present
    if any(c in s for c in "IOQ"):
        warnings.append(f"VIN contient I/O/Q interdits: {v!r}")
    return s, warnings


def normalize_country_code(v: Any) -> Tuple[Optional[str], list[Warning]]:
    code, warns = normalizers.normalize_country_code(v, default="FR")
    return code, warns


# ---------- Motorisation mappings ----------


# SIV codes (carte grise) -> Revio motorisation
_SIV_MOTORISATION = {
    "GO": "diesel",
    "GL": "diesel",
    "ES": "gas",
    "EH": "hybrid",
    "EE": "hybrid",
    "PE": "hybrid",
    "EL": "electric",
    "ELEC": "electric",
    "GN": "gas",
    "GP": "gas",  # GPL
    "H2": "electric",  # hydrogène — traité comme electric faute de catégorie dédiée
}


def map_siv_motorisation(v: Any) -> Tuple[Optional[str], list[Warning]]:
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = str(v).strip().upper()
    if s in _SIV_MOTORISATION:
        return _SIV_MOTORISATION[s], warnings
    # Tolérance : libellés plein texte ("GAZOLE", "ESSENCE", "DIESEL"...)
    return map_loueur_motorisation(v)


def map_loueur_motorisation(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Libellés plein texte loueur / client -> Revio."""
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = str(v).strip().lower()
    if any(k in s for k in ["hybride rechargeable", "phev", "plug-in", "plug in"]):
        return "hybrid", warnings
    if "hybride" in s or "hybrid" in s or s in {"hev", "mhev"}:
        return "hybrid", warnings
    if "électrique" in s or "electrique" in s or "electric" in s or s == "bev":
        return "electric", warnings
    if "gazole" in s or "diesel" in s or s in {"go", "d"}:
        return "diesel", warnings
    if "essence" in s or s in {"sp95", "sp98", "e85", "superéthanol", "superethanol"}:
        return "gas", warnings
    if "gpl" in s or "gnv" in s or "biogaz" in s or "gaz" in s:
        return "gas", warnings
    warnings.append(f"Motorisation non reconnue: {v!r}")
    return None, warnings


# ---------- Usage mappings ----------


# Carte grise "genreVCGNGC" -> Revio usage
# VP (Véhicule Particulier) -> private
# VU (Véhicule Utilitaire) -> utility
# CTTE (camionnette), DERIV VP, TCP, etc. -> service par défaut
_SIV_USAGE = {
    "VP": "private",
    "VU": "utility",
    "CTTE": "utility",
    "CAM": "utility",
    "DERIV VP": "service",
    "DERIVVP": "service",
    "TCP": "service",
    "VASP": "service",
}


def map_siv_usage(v: Any) -> Tuple[Optional[str], list[Warning]]:
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = str(v).strip().upper()
    if s in _SIV_USAGE:
        return _SIV_USAGE[s], warnings
    warnings.append(f"Genre SIV inconnu: {v!r} — fallback 'service'")
    return "service", warnings


def map_loueur_usage(v: Any) -> Tuple[Optional[str], list[Warning]]:
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = str(v).strip().lower()
    if s in {"vp", "particulier", "private"}:
        return "private", warnings
    if s in {"vu", "ctte", "utilitaire", "utility"}:
        return "utility", warnings
    if s in {"vs", "service", "fonction", "dériv vp", "deriv vp", "derivvp"}:
        # Ayvens "VS" = Véhicule de Société → service in Revio terms.
        return "service", warnings
    warnings.append(f"Usage loueur non reconnu: {v!r}")
    return None, warnings


# Match a cell whose entire content is a seat count, with or without
# the suffix « places / pl / sièges / seats ». Anchored to ^...$ so
# « Boîte 5 vitesses » or « 12 RUE … » never match.
import re as _re_seat
_SEAT_COUNT_RE = _re_seat.compile(
    r"^\s*(\d+)(?:\s*(?:places?|pl\.?|sieges?|sièges?|seats?))?\s*$",
    _re_seat.IGNORECASE,
)


def map_client_usage(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Libellés libres client -> Revio. Tolérant + fallback sur 'service'.

    Jalon 5.3.12 — seat-count shortcut : when the cell contains only a
    number of seats (`2`, `5`, `2 places`, `5 sièges`, …) we infer
    the usage. 2 = banquette condamnée = VS (Véhicule de Société) →
    fiscalement HT → ``service``. 5 = berline standard = VP →
    fiscalement TTC → ``private``. Other counts (4, 7, 9, …) raise a
    warning so the engine falls through to the next priority — they're
    too ambiguous to commit to a value automatically.
    """
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = str(v).strip().lower()

    # Seat-count branch (Jalon 5.3.12). Anchored regex so we don't
    # confuse "5 places" (here) with "Boîte 5 vitesses" (free-form
    # mention of a 5-speed gearbox).
    m = _SEAT_COUNT_RE.match(s)
    if m:
        n = int(m.group(1))
        if n == 2:
            return "service", warnings   # VS — fiscalement HT
        if n == 5:
            return "private", warnings   # VP — fiscalement TTC
        warnings.append(
            f"Nombre de places {n} non décisif (ni 2 ni 5) — usage à confirmer."
        )
        return None, warnings

    if any(k in s for k in ["particulier", "personnel", "privé", "private"]):
        return "private", warnings
    if any(k in s for k in ["utilitaire", "utility", "fourgon", "camionnette", "van"]):
        return "utility", warnings
    if any(k in s for k in ["service", "fonction", "pool", "parc", "mission"]):
        return "service", warnings
    warnings.append(f"Usage client non reconnu: {v!r} — à confirmer (pas de fallback auto)")
    return None, warnings


# ---------- Registry ----------


TRANSFORMS: dict[str, TransformFn] = {
    "passthrough": passthrough,
    "uppercase": uppercase,
    "title_case": title_case,
    "to_int": to_int,
    "int": to_int,            # YAML contract alias
    "strip": strip,
    "float_fr": float_fr,
    "float_fr_rubrique": float_fr_rubrique,
    "bool_fr": bool_fr,
    "normalize_date": normalize_date,
    "parse_date_fr": parse_date_fr,
    "date_iso": date_iso,
    "date_any_to_iso": date_any_to_iso,
    "date_fr_to_iso": date_fr_to_iso,
    "normalize_plate": normalize_plate,
    "normalize_vin": normalize_vin,
    "normalize_country_code": normalize_country_code,
    "map_siv_motorisation": map_siv_motorisation,
    "map_loueur_motorisation": map_loueur_motorisation,
    "map_siv_usage": map_siv_usage,
    "map_loueur_usage": map_loueur_usage,
    "map_client_usage": map_client_usage,
    # Prestation / maintenance rules (contract.yml)
    "rule_field_present": rule_field_present,
    "rule_source_present": rule_source_present,
    "enum_mapping": enum_mapping,
    "enum_mapping_2": enum_mapping_2,
    "enum_mapping_tires": enum_mapping_tires,
}


def apply(transform_name: str, value: Any) -> Tuple[Any, list[Warning]]:
    """Applique un transform par son nom. Retourne (valeur, warnings).

    Si le nom est inconnu, passthrough + warning.
    """
    fn = TRANSFORMS.get(transform_name)
    if fn is None:
        return value, [f"Transform inconnu: {transform_name!r} - passthrough appliqué"]
    return fn(value)

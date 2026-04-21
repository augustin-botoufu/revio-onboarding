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


# Alias: some rules call it "parse_date_fr" to signal explicit FR convention,
# but our normalizer already tries dayfirst first, so it's the same function.
parse_date_fr = normalize_date


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


def map_client_usage(v: Any) -> Tuple[Optional[str], list[Warning]]:
    """Libellés libres client -> Revio. Tolérant + fallback sur 'service'."""
    warnings: list[Warning] = []
    if _is_empty(v):
        return None, warnings
    s = str(v).strip().lower()
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
    "normalize_date": normalize_date,
    "parse_date_fr": parse_date_fr,
    "normalize_plate": normalize_plate,
    "normalize_vin": normalize_vin,
    "normalize_country_code": normalize_country_code,
    "map_siv_motorisation": map_siv_motorisation,
    "map_loueur_motorisation": map_loueur_motorisation,
    "map_siv_usage": map_siv_usage,
    "map_loueur_usage": map_loueur_usage,
    "map_client_usage": map_client_usage,
}


def apply(transform_name: str, value: Any) -> Tuple[Any, list[Warning]]:
    """Applique un transform par son nom. Retourne (valeur, warnings).

    Si le nom est inconnu, passthrough + warning.
    """
    fn = TRANSFORMS.get(transform_name)
    if fn is None:
        return value, [f"Transform inconnu: {transform_name!r} - passthrough appliqué"]
    return fn(value)

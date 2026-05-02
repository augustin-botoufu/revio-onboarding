"""Deterministic normalizers for messy client/lessor data.

These functions are used BEFORE any LLM call. They handle the predictable
cleanup (date formats, plate formats, currency symbols, etc.) so that the
LLM only gets asked the hard questions.

Every function returns a tuple: (normalized_value, list_of_warnings).
Warnings are short strings describing any fix that was applied or any
reason why the value could not be normalized.
"""

from __future__ import annotations

import re
from datetime import datetime, date
from typing import Optional, Tuple

from dateutil import parser as dateparser


Warning = str


# ----- DATES -----

def normalize_date(raw) -> Tuple[Optional[str], list[Warning]]:
    """Normalize a date to the Revio format YYYY/MM/DD.

    Handles:
    - DD/MM/YYYY, DD/MM/YY, MM/DD/YYYY, MM/DD/YY
    - YYYY/MM/DD, YYYY-MM-DD
    - Letter 'O' instead of digit '0' typos
    - Trailing/leading spaces
    - pandas / openpyxl datetime objects
    - Junk strings like 'jj/06/mercredi' or '22/12/25 ACHAT' (returns None + warning)

    The French convention is preferred when the format is ambiguous
    (both DD/MM and MM/DD are plausible).
    """
    warnings: list[Warning] = []
    if raw is None:
        return None, warnings
    # Already a datetime / date object (common when reading xlsx).
    if isinstance(raw, (datetime, date)):
        return raw.strftime("%Y/%m/%d"), warnings
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none", "null", "/", "-", "n/a"}:
        return None, warnings
    # Replace the classic 'O' -> '0' typo inside digit-ish strings.
    if re.match(r"^[\dOo/\-\. ]+$", s) and ("O" in s or "o" in s):
        fixed = s.replace("O", "0").replace("o", "0")
        warnings.append(f"Lettre 'O' remplacée par '0' dans la date (original: {s!r})")
        s = fixed
    # Garbage junk patterns we see in real files.
    if any(bad in s.lower() for bad in ["mercredi", "jeudi", "vendredi", "samedi", "dimanche",
                                        "lundi", "mardi", "achat", "restitution", "tbc", "?"]):
        warnings.append(f"Date non exploitable: {raw!r}")
        return None, warnings
    # ISO format YYYY-MM-DD / YYYY/MM/DD: year-first, NEVER day-first.
    iso_first = bool(re.match(r"^\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2}\b", s))
    # Try FR convention first when format is ambiguous (DD/MM/YYYY), then US/ISO.
    try_order = (False,) if iso_first else (True, False)
    for dayfirst in try_order:
        try:
            dt = dateparser.parse(s, dayfirst=dayfirst, fuzzy=False)
            return dt.strftime("%Y/%m/%d"), warnings
        except (ValueError, dateparser.ParserError):
            continue
    warnings.append(f"Date non parsable: {raw!r}")
    return None, warnings


# ----- PLATES -----

_PLATE_CANON_RE = re.compile(r"[^A-Z0-9]")


def normalize_plate(raw) -> Tuple[Optional[str], list[Warning]]:
    """Normalize a French-ish plate to AB-123-CD."""
    warnings: list[Warning] = []
    if raw is None:
        return None, warnings
    s = str(raw).strip().upper()
    if not s or s in {"NAN", "NONE", "NULL", "-", "/"}:
        return None, warnings
    # Strip anything that is not A-Z or 0-9.
    stripped = _PLATE_CANON_RE.sub("", s)
    # Standard SIV format: 2 letters + 3 digits + 2 letters = 7 chars.
    if len(stripped) == 7 and stripped[:2].isalpha() and stripped[2:5].isdigit() and stripped[5:].isalpha():
        return f"{stripped[:2]}-{stripped[2:5]}-{stripped[5:]}", warnings
    # Old format: 3-4 digits + 2-3 letters + 2 digits department (not normalized here).
    warnings.append(f"Immatriculation au format non standard: {raw!r}")
    return s, warnings


def plate_for_matching(raw) -> Optional[str]:
    """Return an uppercase alphanumeric-only version of the plate.

    Used internally for joining rows across files - more tolerant than the
    canonical format.
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    stripped = _PLATE_CANON_RE.sub("", s)
    return stripped or None


# ----- AMOUNTS -----

def normalize_amount(raw) -> Tuple[Optional[float], list[Warning]]:
    """Turn '1 234,56 €', '1234.56 TTC', '?' etc. into a float."""
    warnings: list[Warning] = []
    if raw is None:
        return None, warnings
    if isinstance(raw, (int, float)):
        return float(raw), warnings
    s = str(raw).strip()
    if not s or s in {"?", "-", "/"}:
        return None, warnings
    # Note if the value is tagged TTC/HT.
    if "TTC" in s.upper():
        warnings.append("Montant tagué TTC (valeur conservée telle quelle)")
    if "HT" in s.upper() and "TTC" not in s.upper():
        warnings.append("Montant tagué HT (valeur conservée telle quelle)")
    # Remove currency and tags.
    s = re.sub(r"[€$£]", "", s)
    s = re.sub(r"\b(TTC|HT|EUR|eur)\b", "", s, flags=re.IGNORECASE)
    # Remove narrow nbsp / regular spaces used as thousand separators.
    s = s.replace("\u00a0", "").replace(" ", "")
    # Comma decimal -> dot decimal. But only if no dot already present.
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    elif "," in s and "." in s:
        # e.g. '1,234.56' -> US thousand separators, drop commas.
        s = s.replace(",", "")
    try:
        return float(s), warnings
    except ValueError:
        warnings.append(f"Montant non parsable: {raw!r}")
        return None, warnings


# ----- KILOMETERS -----

def normalize_km(raw) -> Tuple[Optional[int], list[Warning]]:
    """Normalize a kilometer value.

    Heuristic: if the raw number is < 1000, we assume the source file expressed
    km in thousands (e.g. '120' means 120 000). This matches what we see in
    the wild in client files.
    """
    warnings: list[Warning] = []
    amount, amt_warnings = normalize_amount(raw)
    warnings.extend(amt_warnings)
    if amount is None:
        return None, warnings
    km = int(round(amount))
    if 0 < km < 1000:
        warnings.append(f"KM < 1000 ({km}) - interprété comme milliers: {km*1000}")
        km *= 1000
    return km, warnings


# ----- CIVILITY -----

def normalize_civility(raw) -> Tuple[Optional[str], list[Warning]]:
    """Map 'M', 'H', 'homme', 'Mr', 'F', 'femme', 'Mme' etc. to '1' or '2'."""
    warnings: list[Warning] = []
    if raw is None:
        return None, warnings
    s = str(raw).strip().lower()
    if not s:
        return None, warnings
    if s in {"1", "m", "h", "mr", "mr.", "m.", "homme", "monsieur", "masculin"}:
        return "1", warnings
    if s in {"2", "f", "mme", "mme.", "femme", "madame", "mlle", "mlle.", "feminin", "féminin"}:
        return "2", warnings
    warnings.append(f"Civilité non reconnue: {raw!r}")
    return None, warnings


# ----- COUNTRY CODE -----

_DEPARTMENT_RE = re.compile(r"^\d{2,3}$")


def normalize_country_code(raw, default: str = "FR") -> Tuple[str, list[Warning]]:
    """Return an ISO 2-letter country code.

    Corrects the common mistake of entering a French department number (2-3
    digits) instead of a country code, by assuming FR.
    """
    warnings: list[Warning] = []
    if raw is None or str(raw).strip() == "":
        return default, warnings
    s = str(raw).strip().upper()
    if len(s) == 2 and s.isalpha():
        return s, warnings
    if _DEPARTMENT_RE.match(s):
        warnings.append(f"Valeur {raw!r} ressemble à un département -> converti en FR")
        return "FR", warnings
    if s in {"/", "-"}:
        return default, warnings
    warnings.append(f"Code pays non reconnu: {raw!r} -> {default}")
    return default, warnings


# ----- DRIVER NAME CLEANUP -----

# Common pollution patterns in the "Conducteur" column of client files.
_NAME_NOTE_PATTERNS = [
    r"_à restituer",
    r"\(carte bloquée\)",
    r"carte bloquée",
    r"\(.*?\)",          # anything in parentheses
    r"\s*->.*$",          # ' -> out 18/04 -> ...' tail
    r"\s+le\s+\?.*$",     # 'le ?'
    r"\s+out\s+\d.*$",    # 'out 09/04'
    r"\s+\d{1,2}/\d{1,2}(?!\d)",  # trailing '02/03'
]


def clean_driver_name(raw) -> Tuple[str, list[Warning], str]:
    """Extract a clean driver name + return the stripped 'notes' as a 3rd value.

    Returns (clean_name, warnings, notes).
    Also detects pool vehicles ('PARC SIEGE', 'Parc BDX', 'RESTITUEE RESTITUEE', etc.).
    """
    warnings: list[Warning] = []
    if raw is None:
        return "", warnings, ""
    s = str(raw).strip()
    if not s:
        return "", warnings, ""
    # Pool vehicle detection.
    if re.match(r"^(parc|pool|restituee|pinocchio)\b", s, flags=re.IGNORECASE):
        warnings.append(f"Véhicule de parc / pool détecté: {raw!r}")
        return "", warnings, s  # empty driver, note contains original
    cleaned = s
    for pattern in _NAME_NOTE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    notes = ""
    if cleaned != s:
        notes = s.replace(cleaned, "").strip()
        warnings.append(f"Notes extraites du nom: {notes!r}")
    return cleaned, warnings, notes

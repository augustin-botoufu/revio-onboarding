"""Driver engine — Jalon 5.1.

The Driver table is the simplest of the import pipeline: the client fills in
the Revio template with the correct column names, so there is NO mapping step.
We just :

1. Validate that the file looks like a Revio Driver template (column
   fingerprint).
2. Apply 3 light-touch normalisations :

   - ``civility``       : accept any of "M./Mr/Monsieur/H/Male/..." and map
                          to ``1``; same for "Mme/Mrs/Madame/F/Femme/..."
                          to ``2``. ``1`` and ``2`` pass through. Anything
                          else is cleared and flagged as a warning.
   - ``licenseIssueLocation`` : left as-is for now (Augustin is checking
                          with the devs before we add postal-code → city
                          resolution).
   - ``licenseExpiryDate`` : if ``licenseNumber`` is filled but
                          ``licenseExpiryDate`` is empty, we populate
                          ``2033/01/19``. This covers old FR driving
                          licences issued before 2013 which have no
                          printed expiry date — the French administration
                          set 2033-01-19 as the deadline for converting
                          them.

3. Surface anomalies that the error report should carry :

   - an ``assignPlate`` is referenced but missing from the Vehicle file ;
   - an ``assignPlate`` appears on two or more drivers.

The engine is UI-agnostic; it takes a DataFrame and returns a
:class:`DriverResult`. The caller (Streamlit page, tests, future CLI)
decides how to display it.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable, Optional

import pandas as pd


# =============================================================================
# Expected columns (from schemas.DRIVER_FIELDS)
# =============================================================================
#
# We re-declare the minimum list we care about here so this module stays
# testable without importing ``schemas`` (which indirectly pulls a bunch of
# other things in). If you add a field to ``DRIVER_FIELDS`` that Mode Dev
# should touch, add it here too.

DRIVER_COLUMNS_EXPECTED = (
    "firstName", "lastName", "civility", "birthDate", "birthCity",
    "emailPro", "emailPerso", "phone", "street", "city", "postalCode",
    "countryCode", "seniority", "professionalStatus", "licenseNumber",
    "licenseIssueCountryCode", "licenseIssueLocation", "licenseIssueDate",
    "licenseExpiryDate", "assignPlate", "registrationIssueCountryCode",
    "assignFrom", "assignTo", "companyAnalyticalCode", "locationId",
)

# A relaxed fingerprint used by :func:`is_driver_shape` — if the uploaded
# file matches a majority of these columns, it's a Driver template.
DRIVER_FINGERPRINT_CORE = (
    "firstName", "lastName", "emailPro", "licenseNumber",
    "assignPlate", "civility", "birthDate",
)


# Default licence expiry date for pre-2013 FR licences (no printed expiry).
FR_LEGACY_LICENSE_EXPIRY = "2033/01/19"


# =============================================================================
# Civility normalisation
# =============================================================================

# Canonical targets: "1" = M. / Monsieur ; "2" = Mme / Madame.
#
# We keep the canonical value as a string in the DataFrame because the
# Revio import expects text columns (the template example rows show ``1``
# and ``2`` but pandas would otherwise cast them to int64 which breaks
# the CSV output when the column also contains empty cells).
CIVILITY_MALE: frozenset = frozenset({
    "1", "m", "m.", "mr", "mr.", "monsieur", "messieurs",
    "h", "homme", "male", "man", "masculin",
})

CIVILITY_FEMALE: frozenset = frozenset({
    "2", "mme", "mme.", "mrs", "mrs.", "madame", "mesdames",
    "f", "femme", "female", "woman", "feminin", "féminin",
})


def _normalize_key(raw) -> str:
    """Lowercase, strip, drop accents/punctuation noise for map lookup.

    Empty / NaN returns ``""`` so callers can distinguish "blank" (valid,
    leave empty) from "unknown" (emit warning).
    """
    if raw is None:
        return ""
    if isinstance(raw, float) and pd.isna(raw):
        return ""
    s = str(raw).strip().lower()
    if not s:
        return ""
    # Strip accents so "féminin" matches "feminin".
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # Drop spaces inside the token ("m .") but keep the dot character as-is
    # so the CIVILITY_MALE set matches "m." directly.
    s = re.sub(r"\s+", "", s)
    return s


def normalize_civility(raw) -> tuple[Optional[str], Optional[str]]:
    """Normalise one civility value.

    Returns ``(value, warning)`` where :

    - ``value`` is ``"1"``, ``"2"`` or ``None`` (to blank the cell).
    - ``warning`` is ``None`` for a clean match, or a short message when
      the input was non-empty but unrecognised. When the input is blank
      we return ``(None, None)`` — that's not an error, just a cell left
      empty by the client.
    """
    key = _normalize_key(raw)
    if not key:
        return None, None
    if key in CIVILITY_MALE:
        return "1", None
    if key in CIVILITY_FEMALE:
        return "2", None
    return None, (
        f"civility `{raw}` non reconnue — "
        "utilise 1 (M.) ou 2 (Mme) dans le template."
    )


# =============================================================================
# License expiry rule (Jalon 5.1)
# =============================================================================


def _is_blank(v) -> bool:
    """True iff ``v`` represents a blank cell (None / NaN / empty string)."""
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    s = str(v).strip()
    return s == "" or s.lower() == "nan"


def apply_license_expiry_rule(
    license_number,
    license_expiry,
    *,
    default: str = FR_LEGACY_LICENSE_EXPIRY,
) -> str:
    """Apply the 2033/01/19 default when a driver has a licence but no expiry.

    Behaviour (truth table) :

    ======================  ==================  ===================
    licenseNumber            licenseExpiryDate   Returned value
    ======================  ==================  ===================
    blank                    blank               "" (blank)
    blank                    "2030/01/01"        "2030/01/01"
    "120659501108"           blank               "2033/01/19"
    "120659501108"           "2030/01/01"        "2030/01/01"
    ======================  ==================  ===================

    The ``default`` knob exists so we can lift the rule to a YAML override
    later without touching the call sites.
    """
    if not _is_blank(license_expiry):
        return str(license_expiry).strip()
    if _is_blank(license_number):
        return ""
    return default


# =============================================================================
# Plate normalisation for anomaly detection
# =============================================================================


_PLATE_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def plate_key(raw) -> str:
    """Uppercase alnum-only plate representation used for equality checks.

    Matches the Vehicle engine's ``plate_for_matching`` so a driver's
    ``assignPlate`` can be cross-checked against the Vehicle DataFrame
    without worrying about hyphens or casing.
    """
    if _is_blank(raw):
        return ""
    s = str(raw).strip().lower()
    s = _PLATE_ALNUM_RE.sub("", s)
    return s.upper()


# =============================================================================
# Result dataclass
# =============================================================================


@dataclass
class DriverAnomaly:
    """One anomaly to surface in the error report's Driver tab."""

    code: str                 # e.g. "assign_plate_not_in_vehicles"
    row: int                  # 1-based input row (header excluded) for the user
    plate: str = ""           # canonical form of the offending plate
    driver: str = ""          # "FirstName LastName" for readability
    message: str = ""

    def as_record(self) -> dict:
        return {
            "code": self.code,
            "row": self.row,
            "plate": self.plate,
            "driver": self.driver,
            "message": self.message,
        }


@dataclass
class DriverResult:
    """Outcome of :func:`process_drivers`.

    ``df`` is the normalised DataFrame, ready to be written to the export
    CSV. ``anomalies`` is the list of real problems (unknown plate,
    duplicates). ``warnings`` covers soft issues like an unrecognised
    civility value — useful to show in the UI but less critical.

    ``counts`` is a small summary dict for the UI, avoiding re-computing
    from ``df`` everywhere.
    """

    df: pd.DataFrame
    anomalies: list[DriverAnomaly] = field(default_factory=list)
    warnings: list[DriverAnomaly] = field(default_factory=list)
    counts: dict = field(default_factory=dict)

    @property
    def n_drivers(self) -> int:
        return 0 if self.df is None else len(self.df)

    @property
    def n_anomalies(self) -> int:
        return len(self.anomalies)


# =============================================================================
# Main processing
# =============================================================================


def is_driver_shape(columns: Iterable[str], *, min_hits: int = 4) -> bool:
    """True iff ``columns`` looks like a Revio Driver template.

    We match on case-insensitive exact column names against the fingerprint
    set; ``min_hits=4`` means at least 4 out of 7 core driver columns must
    be present. Generous enough to catch client files that dropped a few
    optional columns, strict enough to not confuse a Vehicle file.
    """
    normalised = {str(c).strip().lower() for c in columns if c is not None}
    core_lower = {c.lower() for c in DRIVER_FINGERPRINT_CORE}
    hits = len(normalised & core_lower)
    return hits >= min_hits


def process_drivers(
    df: pd.DataFrame,
    *,
    vehicle_plates: Optional[Iterable[str]] = None,
) -> DriverResult:
    """Run the 3 normalisations + anomaly detection on a Driver DataFrame.

    Parameters
    ----------
    df
        Raw Driver CSV loaded with the template headers (``firstName``,
        ``lastName``, …).
    vehicle_plates
        Optional iterable of plates present in the Vehicle table (any
        format — we normalise to :func:`plate_key`). When provided, we
        raise an anomaly for each ``assignPlate`` not found there. Pass
        ``None`` when the Vehicle file isn't available — we just skip
        the "unknown plate" check then, but duplicates are still detected.

    Returns
    -------
    DriverResult
    """
    if df is None or df.empty:
        return DriverResult(df=df, counts={"rows": 0})

    # Work on a copy so we don't mutate the caller's DataFrame.
    out = df.copy()

    warnings: list[DriverAnomaly] = []
    anomalies: list[DriverAnomaly] = []

    # --- Civility normalisation -------------------------------------------
    if "civility" in out.columns:
        new_civ: list[Optional[str]] = []
        for idx, raw in enumerate(out["civility"].tolist(), start=1):
            canon, warn = normalize_civility(raw)
            new_civ.append(canon)
            if warn:
                # ``idx`` is 1-based (excluding the header row) for the user.
                first = str(out.at[out.index[idx - 1], "firstName"]).strip() if "firstName" in out.columns else ""
                last = str(out.at[out.index[idx - 1], "lastName"]).strip() if "lastName" in out.columns else ""
                warnings.append(DriverAnomaly(
                    code="civility_unrecognised",
                    row=idx,
                    driver=f"{first} {last}".strip(),
                    message=warn,
                ))
        # Write as ``object`` so we can mix "1" / "2" / None without pandas
        # coercing to float64 (which would produce NaNs and blow the CSV).
        out["civility"] = pd.Series(new_civ, index=out.index, dtype="object")

    # --- License expiry rule ---------------------------------------------
    if "licenseExpiryDate" in out.columns and "licenseNumber" in out.columns:
        new_exp: list[str] = []
        for ln, le in zip(out["licenseNumber"].tolist(), out["licenseExpiryDate"].tolist()):
            new_exp.append(apply_license_expiry_rule(ln, le))
        out["licenseExpiryDate"] = pd.Series(new_exp, index=out.index, dtype="object")

    # --- Assign-plate anomalies ------------------------------------------
    if "assignPlate" in out.columns:
        vehicle_keys = _normalise_plate_set(vehicle_plates)
        plate_keys_series = out["assignPlate"].map(plate_key)
        anomalies.extend(_unknown_plate_anomalies(out, plate_keys_series, vehicle_keys))
        anomalies.extend(_duplicate_plate_anomalies(out, plate_keys_series))

    counts = {
        "rows": len(out),
        "warnings": len(warnings),
        "anomalies": len(anomalies),
        "with_plate": int((out["assignPlate"].map(plate_key) != "").sum())
        if "assignPlate" in out.columns else 0,
    }

    return DriverResult(
        df=out,
        anomalies=anomalies,
        warnings=warnings,
        counts=counts,
    )


def _normalise_plate_set(vehicle_plates: Optional[Iterable[str]]) -> Optional[frozenset]:
    """Normalise an iterable of plates to a frozenset of canonical keys.

    Returns ``None`` when the caller didn't provide a vehicle list — that
    signals "skip the unknown-plate check" to the caller.
    """
    if vehicle_plates is None:
        return None
    keys = {plate_key(p) for p in vehicle_plates}
    keys.discard("")
    return frozenset(keys)


def _unknown_plate_anomalies(
    df: pd.DataFrame,
    plate_keys: "pd.Series[str]",
    vehicle_keys: Optional[frozenset],
) -> list[DriverAnomaly]:
    """Rows whose assignPlate isn't in the Vehicle file."""
    if vehicle_keys is None:
        return []
    out: list[DriverAnomaly] = []
    for row_1based, (orig_plate, key) in enumerate(
        zip(df["assignPlate"].tolist(), plate_keys.tolist()), start=1
    ):
        if not key:
            continue
        if key in vehicle_keys:
            continue
        first = str(df.at[df.index[row_1based - 1], "firstName"]).strip() if "firstName" in df.columns else ""
        last = str(df.at[df.index[row_1based - 1], "lastName"]).strip() if "lastName" in df.columns else ""
        out.append(DriverAnomaly(
            code="assign_plate_not_in_vehicles",
            row=row_1based,
            plate=str(orig_plate).strip(),
            driver=f"{first} {last}".strip(),
            message=(
                f"La plaque `{orig_plate}` est assignée à {first} {last} "
                "mais absente du fichier véhicules."
            ),
        ))
    return out


def _duplicate_plate_anomalies(
    df: pd.DataFrame,
    plate_keys: "pd.Series[str]",
) -> list[DriverAnomaly]:
    """Rows whose assignPlate appears on more than one driver.

    We emit one anomaly per offending row (not per plate) so the error
    report lists every driver involved in a duplicate.
    """
    # Group rows by canonical plate, ignoring blanks.
    groups: dict[str, list[int]] = {}
    for i, key in enumerate(plate_keys.tolist(), start=1):
        if not key:
            continue
        groups.setdefault(key, []).append(i)

    out: list[DriverAnomaly] = []
    for key, rows in groups.items():
        if len(rows) < 2:
            continue
        # Build a human-readable "X (row 2), Y (row 3)" list so the reviewer
        # can locate every occurrence from a single anomaly record.
        peer_names: list[str] = []
        for r in rows:
            first = str(df.at[df.index[r - 1], "firstName"]).strip() if "firstName" in df.columns else ""
            last = str(df.at[df.index[r - 1], "lastName"]).strip() if "lastName" in df.columns else ""
            peer_names.append(f"{first} {last} (ligne {r})".strip())
        peers_summary = " ; ".join(peer_names)

        for r in rows:
            first = str(df.at[df.index[r - 1], "firstName"]).strip() if "firstName" in df.columns else ""
            last = str(df.at[df.index[r - 1], "lastName"]).strip() if "lastName" in df.columns else ""
            orig_plate = str(df.at[df.index[r - 1], "assignPlate"]).strip()
            out.append(DriverAnomaly(
                code="assign_plate_duplicated",
                row=r,
                plate=orig_plate,
                driver=f"{first} {last}".strip(),
                message=(
                    f"La plaque `{orig_plate}` est assignée à plusieurs "
                    f"drivers : {peers_summary}."
                ),
            ))
    return out


# =============================================================================
# Convenience helpers for the UI
# =============================================================================


def extract_vehicle_plates(vehicle_df: Optional[pd.DataFrame]) -> list[str]:
    """Return the list of raw plate strings from a Vehicle DataFrame.

    Looks for the ``registrationPlate`` column first (Revio template), then
    falls back to ``plate`` (engine output) and common alternatives. Empty
    / NaN values are dropped. Returns ``[]`` when no plate column is found.
    """
    if vehicle_df is None or vehicle_df.empty:
        return []
    candidates = ("registrationPlate", "plate", "immatriculation", "immat")
    for c in candidates:
        if c in vehicle_df.columns:
            return [p for p in vehicle_df[c].dropna().astype(str).tolist() if p.strip()]
    # Fall back to case-insensitive match.
    low = {str(c).lower(): c for c in vehicle_df.columns}
    for c_low in ("registrationplate", "plate", "immatriculation", "immat"):
        if c_low in low:
            return [p for p in vehicle_df[low[c_low]].dropna().astype(str).tolist() if p.strip()]
    return []

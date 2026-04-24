"""Fleet / agency segmentation (Jalon 3.0).

Some of our clients organize their fleet by cost-centres / agencies (« Bordeaux »,
« Paris », « IDF_IND », …). They need one Revio import *per agency*, so on the
onboarding side we must be able to:

1. pick a column in one of the uploaded source files that holds the raw agency
   code (the column name is client-specific: « Centre de coût », « Agence »,
   « CC », …),
2. map each raw value to a human-readable fleet name, with the possibility of
   **merging** several raw values to the same fleet (``Bdx`` and ``BORDEAUX``
   → « Bordeaux »),
3. decide what to do with empty cells (sentinel ``""`` in the mapping),
4. split the final engine output DataFrame into N fleet DataFrames.

This module is **pure logic**: it doesn't touch Streamlit, it doesn't read
files, it just exposes dataclasses + helpers that the UI / output writer
compose. That keeps it fully unit-testable.

Nothing here is persisted — a FleetMapping lives in ``session_state`` and
dies when the import is done. Each client import starts from scratch.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from .normalizers import plate_for_matching


# Sentinel used in ``raw_to_fleet`` to represent « rows where the source
# column is empty / NaN ». Picked as empty string because no legitimate raw
# agency code normalizes to empty after strip(), and it's yaml-safe.
EMPTY_RAW_KEY = ""

# Fleet name displayed when a plate doesn't match any source-file row (e.g. a
# plate only present in a loueur file). Kept human-readable on purpose.
UNASSIGNED_FLEET = "(non rattaché)"


@dataclass
class FleetMapping:
    """Session-scoped configuration for splitting an import into fleets.

    Attributes:
        source_file_key: key into ``session_state.engine_files`` pointing to
            the file that holds the agency column.
        source_column: original column header in that file.
        raw_to_fleet: ``{raw_value_as_string: fleet_display_name}``. The raw
            key is kept as the user saw it in their file (case-preserving) so
            the UI can display it back. Empty / NaN source cells use the
            ``EMPTY_RAW_KEY`` sentinel.
        plate_to_fleet: derived index ``{normalized_plate: fleet_name}``
            computed when the mapping is built. Used by downstream writers
            to know where each output row goes.
    """
    source_file_key: str
    source_column: str
    raw_to_fleet: dict[str, str] = field(default_factory=dict)
    plate_to_fleet: dict[str, str] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Derived views
    # ------------------------------------------------------------------
    @property
    def fleet_names(self) -> list[str]:
        """Sorted unique fleet names across the mapping (empty dropped)."""
        names = {v for v in self.raw_to_fleet.values() if v and v.strip()}
        return sorted(names)

    @property
    def is_active(self) -> bool:
        """True iff the mapping actually splits something meaningful."""
        return len(self.fleet_names) > 0 and bool(self.plate_to_fleet)

    def counts_by_fleet(self) -> dict[str, int]:
        """``{fleet_name: n_plates}`` — how many plates fall in each fleet."""
        out: dict[str, int] = {name: 0 for name in self.fleet_names}
        for fname in self.plate_to_fleet.values():
            out[fname] = out.get(fname, 0) + 1
        return out


# =============================================================================
# Column / value introspection
# =============================================================================

def unique_values_in_column(
    df: pd.DataFrame, column: str,
) -> list[tuple[str, int]]:
    """List distinct values of ``column`` with their row count.

    Empty / NaN cells are bucketed under the ``EMPTY_RAW_KEY`` sentinel so
    the UI can still surface them and let the user decide where those rows
    should go (rather than silently losing them).

    The returned list is sorted: most frequent non-empty first, then the
    empty sentinel at the end (when present). Ties break alphabetically.
    """
    if column not in df.columns:
        return []
    series = df[column]
    counts: dict[str, int] = {}
    for raw in series:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            key = EMPTY_RAW_KEY
        else:
            try:
                s = str(raw).strip()
            except Exception:
                s = ""
            key = s if s else EMPTY_RAW_KEY
        counts[key] = counts.get(key, 0) + 1

    items = list(counts.items())
    # Non-empty first, sorted by count desc then alpha; empty sentinel last.
    non_empty = sorted(
        (it for it in items if it[0] != EMPTY_RAW_KEY),
        key=lambda kv: (-kv[1], kv[0].lower()),
    )
    has_empty = EMPTY_RAW_KEY in counts
    if has_empty:
        non_empty.append((EMPTY_RAW_KEY, counts[EMPTY_RAW_KEY]))
    return non_empty


def suggest_agency_columns(df: pd.DataFrame) -> list[str]:
    """Return column names whose header hints at an agency / cost-centre field.

    Pure heuristic — used to preselect a column in the popup. Not a gate;
    the user can still pick any column.
    """
    if df is None or len(df.columns) == 0:
        return []
    needles = [
        "agence", "agency",
        "centre de cout", "centre de coût", "cost centre", "cost center",
        "cc", "analytique",
        "etablissement", "établissement",
        "site", "direction", "departement", "département", "region", "région",
        "filiale", "entite", "entité",
    ]
    out = []
    for col in df.columns:
        low = _ascii_lower(str(col))
        for n in needles:
            if n in low:
                out.append(col)
                break
    return out


# =============================================================================
# Building a FleetMapping
# =============================================================================

def build_fleet_mapping(
    *,
    source_file_key: str,
    source_column: str,
    raw_to_fleet: dict[str, str],
    source_df: pd.DataFrame,
    plate_column: Optional[str] = None,
) -> FleetMapping:
    """Materialize a FleetMapping by resolving plates in ``source_df``.

    Args:
        source_file_key: key into ``engine_files`` (kept as metadata so the
            popup can re-render with the right preselection).
        source_column: column header in ``source_df`` that holds the raw
            agency value.
        raw_to_fleet: mapping chosen by the user. Keys are raw values as
            strings (empty-cell sentinel = ``EMPTY_RAW_KEY``). Values are the
            fleet display name; an empty string means « ignore these rows ».
        source_df: the raw source DataFrame (not engine-indexed).
        plate_column: optional override; if None we auto-detect via
            ``_find_plate_column`` heuristic.

    Returns:
        A FleetMapping whose ``plate_to_fleet`` is populated.
    """
    from .rules_engine import _find_plate_column  # local to avoid import cycle

    plate_col = plate_column or _find_plate_column(source_df)
    plate_to_fleet: dict[str, str] = {}

    # raw_to_fleet may contain raw keys of any form ("Bdx", "BORDEAUX", ...).
    # For matching we want to be case/whitespace-forgiving so that a mapping
    # built from the unique list still catches the rows. We therefore
    # normalize the lookup keys but preserve originals for display.
    lookup = {_normalize_raw(k): v for k, v in raw_to_fleet.items()}

    if plate_col is None or source_column not in source_df.columns:
        # We can't produce a plate → fleet index, but we keep the raw
        # mapping so the UI can at least show what the user configured.
        return FleetMapping(
            source_file_key=source_file_key,
            source_column=source_column,
            raw_to_fleet=dict(raw_to_fleet),
            plate_to_fleet={},
        )

    for _, row in source_df.iterrows():
        plate_norm = plate_for_matching(row.get(plate_col))
        if not plate_norm:
            continue
        raw_val = row.get(source_column)
        raw_key = _normalize_raw(raw_val)
        fleet = lookup.get(raw_key)
        if fleet is None or not str(fleet).strip():
            # Either the user chose « ignore » for this raw, or the raw
            # didn't appear in the popup (unlikely but defensive). Leave
            # the plate unassigned — it'll fall into UNASSIGNED_FLEET at
            # split time.
            continue
        # First wins when duplicate plates (same logic as _index_by_plate).
        plate_to_fleet.setdefault(plate_norm, str(fleet).strip())

    return FleetMapping(
        source_file_key=source_file_key,
        source_column=source_column,
        raw_to_fleet=dict(raw_to_fleet),
        plate_to_fleet=plate_to_fleet,
    )


# =============================================================================
# Split an engine output DataFrame by fleet
# =============================================================================

def assign_fleets_to_df(
    df: pd.DataFrame,
    mapping: Optional[FleetMapping],
) -> pd.Series:
    """Return a Series aligned with ``df.index`` giving each row's fleet name.

    ``df`` is expected to be indexed by ``plate_for_matching`` (that's how
    ``rules_engine.run_vehicle`` builds ``result.df``). Rows whose plate is
    not in ``mapping.plate_to_fleet`` fall under :data:`UNASSIGNED_FLEET`.

    If ``mapping`` is ``None`` or inactive, every row gets the sentinel
    ``""`` (empty string) — callers can treat that as « no segmentation,
    single global bucket ».
    """
    if mapping is None or not mapping.is_active:
        return pd.Series([""] * len(df), index=df.index, dtype=object)
    return pd.Series(
        [mapping.plate_to_fleet.get(str(idx), UNASSIGNED_FLEET)
         for idx in df.index],
        index=df.index,
        dtype=object,
    )


def split_df_by_fleet(
    df: pd.DataFrame,
    mapping: Optional[FleetMapping],
) -> dict[str, pd.DataFrame]:
    """Split ``df`` into ``{fleet_name: sub_df}``.

    - If ``mapping`` is None / inactive, returns a single bucket with key
      ``""`` containing the whole df unchanged.
    - Otherwise, returns one bucket per fleet that has at least one plate,
      plus ``UNASSIGNED_FLEET`` if any plate was missing.
    """
    fleets = assign_fleets_to_df(df, mapping)
    if mapping is None or not mapping.is_active:
        return {"": df.copy()}
    out: dict[str, pd.DataFrame] = {}
    for name, sub in df.groupby(fleets, sort=False):
        out[str(name)] = sub.copy()
    return out


# =============================================================================
# Filename / sheet-name helpers
# =============================================================================

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify_fleet_name(name: str) -> str:
    """Produce a safe filename chunk from a fleet display name.

    ``"Île-de-France"`` → ``"ile-de-france"``. Empty input returns ``""``.
    """
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(name))
    no_acc = "".join(c for c in nfkd if not unicodedata.combining(c))
    lower = no_acc.lower()
    slug = _SLUG_RE.sub("-", lower).strip("-")
    return slug or "fleet"


def safe_sheet_name(name: str, max_len: int = 31) -> str:
    """Excel sheet names have hard constraints: ≤ 31 chars, no ``[]:*?/\\``.

    We also avoid leading/trailing apostrophes (Excel quirk). Returns a
    sanitized string. If the result is empty, returns ``"Feuille"``.
    """
    if name is None:
        return "Feuille"
    s = str(name)
    for bad in ["\\", "/", "*", "?", "[", "]", ":"]:
        s = s.replace(bad, "-")
    s = s.strip().strip("'")
    if len(s) > max_len:
        s = s[:max_len].rstrip("-")
    return s or "Feuille"


# =============================================================================
# Internal helpers
# =============================================================================

_LOOKUP_COLLAPSE_RE = re.compile(r"[^a-z0-9]+")


def _normalize_raw(v) -> str:
    """Normalize a raw agency value for lookup: strip, lower, NFKD no-acc,
    collapse non-alphanumerics into a single space.

    Used only for matching « Bdx » / « bdx » / « BDX » → same bucket, AND
    « Île-de-France » / « ile de france » / « ILE_DE_FRANCE » → same bucket.
    Empty → EMPTY_RAW_KEY.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return EMPTY_RAW_KEY
    try:
        s = str(v).strip()
    except Exception:
        return EMPTY_RAW_KEY
    if not s:
        return EMPTY_RAW_KEY
    lower = _ascii_lower(s)
    collapsed = _LOOKUP_COLLAPSE_RE.sub(" ", lower).strip()
    return collapsed or EMPTY_RAW_KEY


def _ascii_lower(s: str) -> str:
    """Lowercase + strip combining marks (accents)."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

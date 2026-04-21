"""Rules engine — applies YAML rules to produce a Revio output DataFrame.

Input
-----
- A rules YAML (e.g. src/rules/vehicle.yml) loaded into a dict
- A dict { source_slug: pandas.DataFrame } of user-uploaded sources
  (source_slug matches the YAML `source` identifiers: "api_plaques",
  "ayvens_etat_parc", "client_file", etc.)
- The optional `manual_column_overrides` lets the UI inject the column
  the user (or LLM fallback) picked for the generic "client_file" source
  — per (source_slug, field_name).

Output (EngineResult)
---------------------
- df: pandas.DataFrame of merged values (columns = YAML fields, rows = client plates)
- orphan_df: DataFrame with the same columns for plates found in lessor files
  but absent from the client file (same enrichment logic, but no client file)
- source_by_cell: dict[(plate, field), source_slug] — who won, per cell
- conflicts_by_cell: dict[(plate, field), list[(source_slug, value)]] — all
  contributions for a cell, INCLUDING the winner, when 2+ sources disagreed
- parse_warnings_by_cell: dict[(plate, field), list[str]] — transform warnings
  (e.g. "Date non parsable", "VIN I/O/Q interdits"), EXCLUDING "column missing"
  which is noise
- issues: list[Issue] — global / non-cell-specific warnings (rare)

Core logic
----------
For each field, collect ALL non-null contributions (priority, source, value)
across ALL applicable rules — no short-circuit on first non-null. Then:
  - Winner = contribution with lowest priority (then deterministic source order).
  - Conflict = any contribution whose value differs from the winner's value.

Special cases
-------------
- source == "__default__" with example value: used for constant defaults
  (e.g. registrationIssueCountryCode -> FR). Labeled "défaut" in reports.
- Rules without `column` AND without override are silently skipped (they
  are placeholders for sources awaiting a sample file).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml  # type: ignore

from . import transforms
from .normalizers import plate_for_matching


# ---------- Types ----------


@dataclass
class Issue:
    """Global, non-cell-specific warning (e.g. 'no client file provided')."""
    plate: Optional[str]
    field: str
    source: str
    warning: str


@dataclass
class EngineResult:
    df: pd.DataFrame
    issues: list[Issue] = field(default_factory=list)
    orphan_df: Optional[pd.DataFrame] = None
    source_by_cell: dict[tuple[str, str], str] = field(default_factory=dict)
    conflicts_by_cell: dict[tuple[str, str], list[tuple[str, Any]]] = field(default_factory=dict)
    parse_warnings_by_cell: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    # Echo of the rules YAML so downstream report can read source_labels
    rules_yaml: Optional[dict] = None


# ---------- YAML loading ----------


def load_rules(path: str | Path) -> dict:
    """Load and return the parsed YAML rules dict."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- Source indexing ----------


def _find_plate_column(df: pd.DataFrame) -> Optional[str]:
    """Find the most likely plate column in a DataFrame (heuristic).

    Looks for headers containing 'plaque', 'immat', 'n° immat'.
    """
    candidates = ["plaque", "immat", "n° immat", "no immat", "numero immat"]
    for col in df.columns:
        low = str(col).strip().lower()
        for c in candidates:
            if c in low:
                return col
    return None


def _index_by_plate(df: pd.DataFrame, plate_col: Optional[str] = None) -> pd.DataFrame:
    """Return a copy of `df` indexed by normalized plate.

    Rows without a detectable plate are dropped.
    """
    if df is None or df.empty:
        return df
    col = plate_col or _find_plate_column(df)
    if col is None:
        return df.copy()
    out = df.copy()
    out["__plate_key__"] = out[col].map(plate_for_matching)
    out = out.dropna(subset=["__plate_key__"])
    out = out.drop_duplicates(subset=["__plate_key__"], keep="first")
    out = out.set_index("__plate_key__", drop=True)
    return out


# ---------- Rule application helpers ----------


def _get_column(
    source_slug: str,
    field_name: str,
    rule: dict,
    manual_overrides: dict[tuple[str, str], str],
) -> Optional[str]:
    """Determine which column to read in the source DataFrame for this rule."""
    col = rule.get("column")
    if col:
        return col
    override = manual_overrides.get((source_slug, field_name))
    if override:
        return override
    return None


def _apply_rule_to_series(
    source_df: pd.DataFrame,
    col: str,
    transform_name: str,
) -> tuple[pd.Series, dict[Any, list[str]]]:
    """Apply a transform to `source_df[col]` row by row.

    Returns (series_of_values, dict_of_per_index_warnings). Warnings that
    are purely structural ("column missing in uploaded file") are filtered
    out by the caller.
    """
    warnings_by_key: dict[Any, list[str]] = {}
    if col not in source_df.columns:
        empty = pd.Series([None] * len(source_df), index=source_df.index, dtype=object)
        # NOTE: we do NOT emit "column missing" warnings — they're noise. The
        # rule itself will simply produce nothing, which is the correct
        # behaviour when a declared column is absent from the uploaded file.
        return empty, {}
    values = []
    for key, raw in source_df[col].items():
        val, warns = transforms.apply(transform_name, raw)
        values.append(val)
        if warns:
            warnings_by_key.setdefault(key, []).extend(warns)
    # Force object dtype so ints mixed with None stay as ints (pandas
    # otherwise promotes to float64 producing '109.0' in CSV/Excel).
    return pd.Series(values, index=source_df.index, dtype=object), warnings_by_key


def _is_null(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    return False


def _resolve_cell(
    plate: Any,
    field_name: str,
    fields_spec: dict,
    indexed_sources: dict[str, pd.DataFrame],
    manual_column_overrides: dict[tuple[str, str], str],
    parse_warnings_by_cell: dict[tuple[str, str], list[str]],
) -> tuple[Any, Optional[str], list[tuple[str, Any]]]:
    """Resolve a single (plate, field) cell by running ALL applicable rules.

    Returns (winner_value, winner_source_slug, conflicts_list) where
    conflicts_list is empty if no conflict, otherwise contains ALL contributing
    (source_slug, value) pairs (winner first).
    """
    spec = fields_spec[field_name]
    rules = sorted(
        spec.get("rules", []),
        key=lambda r: (r.get("priority", 99), r.get("source", "")),
    )
    # Collect all contributions (prio, source, value)
    contributions: list[tuple[int, str, Any]] = []
    for rule in rules:
        source_slug = rule.get("source")
        transform_name = rule.get("transform", "passthrough")
        prio = rule.get("priority", 99)

        if source_slug == "__default__":
            # Constants are applied at the end — they never conflict with
            # anything since they're only used as a fallback. Skip here.
            continue

        src_df = indexed_sources.get(source_slug)
        if src_df is None or src_df.empty:
            continue
        if plate not in src_df.index:
            continue
        col = _get_column(source_slug, field_name, rule, manual_column_overrides)
        if col is None:
            continue
        if col not in src_df.columns:
            continue
        raw = src_df.at[plate, col]
        val, warns = transforms.apply(transform_name, raw)
        if warns:
            parse_warnings_by_cell.setdefault((str(plate), field_name), []).extend(warns)
        if _is_null(val):
            continue
        contributions.append((prio, source_slug, val))

    if not contributions:
        return None, None, []

    # Deterministic winner: first contribution in (prio, source) order
    contributions.sort(key=lambda x: (x[0], x[1]))
    winner_prio, winner_source, winner_val = contributions[0]

    # Conflicts: any contribution with a DIFFERENT value than the winner
    conflicts: list[tuple[str, Any]] = []
    has_conflict = any(c[2] != winner_val for c in contributions)
    if has_conflict:
        # Include the winner first, then all other contributions (dedup by (source, value))
        seen: set[tuple[str, Any]] = set()
        for _, src, val in contributions:
            key = (src, _as_key(val))
            if key in seen:
                continue
            seen.add(key)
            conflicts.append((src, val))

    return winner_val, winner_source, conflicts


def _as_key(v: Any) -> Any:
    """Hashable representation of a value for dedup."""
    try:
        hash(v)
        return v
    except TypeError:
        return str(v)


def _apply_defaults_for_plates(
    plates: list[Any],
    fields_spec: dict,
    out_df: pd.DataFrame,
    source_by_cell: dict[tuple[str, str], str],
) -> None:
    """Fill remaining None cells with any __default__ rule from the YAML."""
    for field_name, spec in fields_spec.items():
        rules = spec.get("rules", [])
        default_rule = next((r for r in rules if r.get("source") == "__default__"), None)
        if default_rule is None:
            continue
        constant = default_rule.get("column") or default_rule.get("example")
        if constant is None:
            continue
        transform_name = default_rule.get("transform", "passthrough")
        val, _ = transforms.apply(transform_name, constant)
        for plate in plates:
            current = out_df.at[plate, field_name] if field_name in out_df.columns else None
            if _is_null(current):
                out_df.at[plate, field_name] = val
                source_by_cell[(str(plate), field_name)] = "__default__"


def _build_df_for_plates(
    plates: list[Any],
    fields_spec: dict,
    indexed_sources: dict[str, pd.DataFrame],
    manual_column_overrides: dict[tuple[str, str], str],
) -> tuple[pd.DataFrame, dict, dict, dict]:
    """Build a DataFrame for a given plate set, returning tracking structures.

    Returns (df, source_by_cell, conflicts_by_cell, parse_warnings_by_cell).
    """
    source_by_cell: dict[tuple[str, str], str] = {}
    conflicts_by_cell: dict[tuple[str, str], list[tuple[str, Any]]] = {}
    parse_warnings_by_cell: dict[tuple[str, str], list[str]] = {}

    # Pre-create df with object dtype so ints stay as ints
    out_df = pd.DataFrame(
        {f: pd.Series([None] * len(plates), dtype=object) for f in fields_spec.keys()},
        index=plates,
    )

    for field_name in fields_spec.keys():
        for plate in plates:
            val, winner_src, conflicts = _resolve_cell(
                plate,
                field_name,
                fields_spec,
                indexed_sources,
                manual_column_overrides,
                parse_warnings_by_cell,
            )
            if val is not None:
                out_df.at[plate, field_name] = val
                source_by_cell[(str(plate), field_name)] = winner_src
            if conflicts:
                conflicts_by_cell[(str(plate), field_name)] = conflicts

    # Apply __default__ values for remaining null cells
    _apply_defaults_for_plates(plates, fields_spec, out_df, source_by_cell)

    out_df.index.name = "plate_key"
    return out_df, source_by_cell, conflicts_by_cell, parse_warnings_by_cell


# ---------- Public API ----------


def apply_rules(
    rules_yaml: dict,
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
) -> EngineResult:
    """Apply all rules declared in `rules_yaml` over the provided source DataFrames."""
    manual_column_overrides = manual_column_overrides or {}
    fields_spec: dict[str, dict] = rules_yaml.get("fields", {})
    issues: list[Issue] = []

    # 1. Index each source by normalized plate.
    indexed: dict[str, pd.DataFrame] = {}
    for slug, df in source_dfs.items():
        plate_col = manual_column_overrides.get((slug, "registrationPlate"))
        indexed[slug] = _index_by_plate(df, plate_col=plate_col)

    # 2. Determine the authoritative plate set (hybrid strategy).
    client_df = indexed.get("client_file")
    has_client_file = client_df is not None and not client_df.empty
    orphan_plates: list[Any] = []

    if has_client_file:
        all_plates = list(client_df.index)
        client_plate_set = set(all_plates)
        # Collect orphan plates: present in lessor sources but absent from client file.
        seen_orphan: set[Any] = set()
        for slug, df in indexed.items():
            if slug == "client_file" or df is None or df.empty:
                continue
            for p in df.index:
                if p and p not in client_plate_set and p not in seen_orphan:
                    seen_orphan.add(p)
                    orphan_plates.append(p)
    else:
        # No client file: union of all lessor plates, with a global warning.
        all_plates = []
        seen: set[str] = set()
        for df in indexed.values():
            if df is None or df.empty:
                continue
            for p in df.index:
                if p and p not in seen:
                    seen.add(p)
                    all_plates.append(p)
        if all_plates:
            issues.append(Issue(
                plate=None,
                field="registrationPlate",
                source="__engine__",
                warning=(
                    "Import sans fichier parc client — le parc final est dérivé de l'union "
                    "des fichiers loueurs. Vérifier manuellement qu'aucune plaque restituée "
                    "ou externe n'est indûment importée."
                ),
            ))

    if not all_plates:
        return EngineResult(
            df=pd.DataFrame(columns=list(fields_spec.keys())),
            issues=issues,
            rules_yaml=rules_yaml,
        )

    # 3. Build main df from the authoritative plate set.
    main_df, source_by_cell, conflicts_by_cell, parse_warnings_by_cell = _build_df_for_plates(
        all_plates, fields_spec, indexed, manual_column_overrides,
    )

    # 4. Build orphan df (only runs if we have orphan plates).
    orphan_df: Optional[pd.DataFrame] = None
    if orphan_plates:
        # We want orphan enrichment to use lessor files but NOT the client file
        # (which is irrelevant — these plates aren't in it).
        indexed_no_client = {k: v for k, v in indexed.items() if k != "client_file"}
        orphan_df, _, _, _ = _build_df_for_plates(
            orphan_plates, fields_spec, indexed_no_client, manual_column_overrides,
        )
        # Tag each orphan with the source files where it was found
        found_in: dict[Any, list[str]] = {}
        for slug, df in indexed.items():
            if slug == "client_file" or df is None or df.empty:
                continue
            for p in orphan_plates:
                if p in df.index:
                    found_in.setdefault(p, []).append(slug)
        orphan_df.insert(0, "sources_found", [", ".join(found_in.get(p, [])) for p in orphan_df.index])

    return EngineResult(
        df=main_df,
        issues=issues,
        orphan_df=orphan_df,
        source_by_cell=source_by_cell,
        conflicts_by_cell=conflicts_by_cell,
        parse_warnings_by_cell=parse_warnings_by_cell,
        rules_yaml=rules_yaml,
    )


def run_vehicle(
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
    rules_path: Optional[str | Path] = None,
) -> EngineResult:
    """Shortcut to run the Vehicle engine with the repo's vehicle.yml."""
    if rules_path is None:
        rules_path = Path(__file__).parent / "rules" / "vehicle.yml"
    rules = load_rules(rules_path)
    return apply_rules(rules, source_dfs, manual_column_overrides)

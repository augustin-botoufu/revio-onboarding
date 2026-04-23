"""Rules engine — applies YAML rules to produce a Revio output DataFrame.

v11 additions (Jalon 5.0 prereq)
---------------------------------
Added LineageStore integration. For every resolved cell we record the full
provenance chain (source, column, row, priority, transform, conflicts
ignored, transform warnings). The lineage is exposed on the EngineResult
and flushed to `_lineage/vehicle.parquet` by the pipeline. Consumed later
by the in-app LLM assistant.

The change is strictly additive: existing fields on EngineResult
(`source_by_cell`, `conflicts_by_cell`, `parse_warnings_by_cell`) are
preserved byte-for-byte — downstream report writers keep working.

Input
-----
- A rules YAML (e.g. src/rules/vehicle.yml) loaded into a dict
- A dict { source_slug: pandas.DataFrame } of user-uploaded sources
- The optional `manual_column_overrides` lets the UI inject the column
  the user (or LLM fallback) picked for the generic "client_file" source
  — per (source_slug, field_name).

Core logic
----------
For each field, collect ALL non-null contributions (priority, source, value)
across ALL applicable rules — no short-circuit on first non-null. Then:
  - Winner = contribution with lowest priority (then deterministic source order).
  - Conflict = any contribution whose value differs from the winner's value.
  - Lineage = one LineageRecord per resolved cell, capturing the full chain.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml  # type: ignore

from . import transforms
from . import rules_io
from .lineage import LineageStore, LineageRecord, build_rule_id, conflict_dict
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
    # v11: full provenance store (prereq Jalon 5.0 assistant).
    lineage: Optional[LineageStore] = None


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

    Rows without a detectable plate are dropped. A `__src_row__` column is
    preserved so the lineage can cite the original row number of the source
    file.
    """
    if df is None or df.empty:
        return df
    col = plate_col or _find_plate_column(df)
    if col is None:
        return df.copy()
    out = df.copy()
    # Preserve the original 0-based row index as a column for lineage.
    out["__src_row__"] = range(len(out))
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


def _is_null(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    return False


def _values_differ(a: Any, b: Any) -> bool:
    """Conflict-aware comparison.

    Strings are compared case-insensitively and ignoring surrounding
    whitespace, so `'CLIO'` and `'clio '` are NOT a conflict. For other
    types we fall back to regular equality.
    """
    if isinstance(a, str) and isinstance(b, str):
        return a.strip().casefold() != b.strip().casefold()
    return a != b


def _source_row_for(src_df: pd.DataFrame, plate: Any) -> Optional[int]:
    """Return the original row index of the source row bearing `plate`."""
    if "__src_row__" not in src_df.columns:
        return None
    try:
        val = src_df.at[plate, "__src_row__"]
    except Exception:
        return None
    try:
        return int(val) if val is not None else None
    except Exception:
        return None


def _resolve_cell(
    plate: Any,
    field_name: str,
    fields_spec: dict,
    indexed_sources: dict[str, pd.DataFrame],
    manual_column_overrides: dict[tuple[str, str], str],
    parse_warnings_by_cell: dict[tuple[str, str], list[str]],
    lineage: Optional[LineageStore] = None,
    table: str = "vehicle",
) -> tuple[Any, Optional[str], list[tuple[str, Any]]]:
    """Resolve a single (plate, field) cell by running ALL applicable rules.

    Returns (winner_value, winner_source_slug, conflicts_list).

    Side effect: if `lineage` is provided and a winner is chosen, a
    LineageRecord is appended with the full provenance.
    """
    spec = fields_spec[field_name]
    rules = sorted(
        spec.get("rules", []),
        key=lambda r: (r.get("priority", 99), r.get("source", "")),
    )

    # Collect all contributions (prio, source, value, col, src_row, transform, warns)
    contributions: list[dict] = []
    for rule in rules:
        source_slug = rule.get("source")
        transform_name = rule.get("transform", "passthrough")
        prio = rule.get("priority", 99)

        if source_slug == "__default__":
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
        contributions.append({
            "priority": prio,
            "source": source_slug,
            "value": val,
            "column": col,
            "src_row": _source_row_for(src_df, plate),
            "transform": transform_name,
            "warnings": list(warns) if warns else [],
        })

    if not contributions:
        return None, None, []

    # Deterministic winner: first contribution in (prio, source) order
    contributions.sort(key=lambda x: (x["priority"], x["source"]))
    winner = contributions[0]
    winner_val = winner["value"]
    winner_source = winner["source"]

    # Conflicts: any contribution with a DIFFERENT value than the winner
    conflicts: list[tuple[str, Any]] = []
    has_conflict = any(_values_differ(c["value"], winner_val) for c in contributions)
    if has_conflict:
        seen: set[tuple[str, Any]] = set()
        for c in contributions:
            key = (c["source"], _as_key(c["value"]))
            if key in seen:
                continue
            seen.add(key)
            conflicts.append((c["source"], c["value"]))

    # ---- Lineage recording ----
    if lineage is not None:
        conflicts_ignored: list[dict] = []
        for c in contributions[1:]:
            if _values_differ(c["value"], winner_val):
                reason = (
                    f"priorité inférieure ({c['priority']} vs {winner['priority']})"
                    if c["priority"] != winner["priority"]
                    else f"source de même priorité écartée par ordre alphabétique"
                )
            else:
                reason = f"valeur identique — non conflictuelle (priorité {c['priority']})"
            conflicts_ignored.append(conflict_dict(c["source"], c["value"], reason))

        lineage.record(LineageRecord(
            table=table,
            key=str(plate),
            field=field_name,
            value=winner_val,
            source_used=winner_source,
            source_col=winner["column"],
            source_row=winner["src_row"],
            priority=winner["priority"],
            transform=winner["transform"],
            rule_id=build_rule_id(table, field_name, winner_source, winner["priority"]),
            conflicts_ignored=conflicts_ignored,
            notes=None,
            warnings=winner["warnings"],
        ))

    return winner_val, winner_source, conflicts


def _as_key(v: Any) -> Any:
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
    lineage: Optional[LineageStore] = None,
    table: str = "vehicle",
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
                if lineage is not None:
                    lineage.record(LineageRecord(
                        table=table,
                        key=str(plate),
                        field=field_name,
                        value=val,
                        source_used="__default__",
                        source_col=None,
                        source_row=None,
                        priority=default_rule.get("priority", 99),
                        transform=transform_name,
                        rule_id=build_rule_id(table, field_name, "__default__", default_rule.get("priority", 99)),
                        conflicts_ignored=[],
                        notes="Valeur par défaut appliquée car aucune source n'a remonté de valeur.",
                        warnings=[],
                    ))


def _build_df_for_plates(
    plates: list[Any],
    fields_spec: dict,
    indexed_sources: dict[str, pd.DataFrame],
    manual_column_overrides: dict[tuple[str, str], str],
    lineage: Optional[LineageStore] = None,
    table: str = "vehicle",
) -> tuple[pd.DataFrame, dict, dict, dict]:
    """Build a DataFrame for a given plate set, returning tracking structures.

    Returns (df, source_by_cell, conflicts_by_cell, parse_warnings_by_cell).
    """
    source_by_cell: dict[tuple[str, str], str] = {}
    conflicts_by_cell: dict[tuple[str, str], list[tuple[str, Any]]] = {}
    parse_warnings_by_cell: dict[tuple[str, str], list[str]] = {}

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
                lineage=lineage,
                table=table,
            )
            if val is not None:
                out_df.at[plate, field_name] = val
                source_by_cell[(str(plate), field_name)] = winner_src
            if conflicts:
                conflicts_by_cell[(str(plate), field_name)] = conflicts

    _apply_defaults_for_plates(plates, fields_spec, out_df, source_by_cell, lineage=lineage, table=table)

    out_df.index.name = "plate_key"
    return out_df, source_by_cell, conflicts_by_cell, parse_warnings_by_cell


# ---------- Public API ----------


def apply_rules(
    rules_yaml: dict,
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
    priority_overrides: Optional[dict[str, list[str]]] = None,
    table: str = "vehicle",
) -> EngineResult:
    """Apply all rules declared in `rules_yaml` over the provided source DataFrames.

    `priority_overrides` (optional) lets the UI rewrite the priority order of
    sources per field for this run only — without mutating the YAML on disk.
    Format: {field_name: [source_slug_in_priority_order]}. See rules_io.

    `table` is stored on each LineageRecord so Vehicle and Contract lineage
    can be distinguished downstream.
    """
    manual_column_overrides = manual_column_overrides or {}
    if priority_overrides:
        rules_yaml = rules_io.apply_priority_overrides(rules_yaml, priority_overrides)
    fields_spec: dict[str, dict] = rules_yaml.get("fields", {})
    issues: list[Issue] = []
    lineage = LineageStore()

    indexed: dict[str, pd.DataFrame] = {}
    for slug, df in source_dfs.items():
        plate_col = manual_column_overrides.get((slug, "registrationPlate"))
        indexed[slug] = _index_by_plate(df, plate_col=plate_col)

    client_df = indexed.get("client_file")
    has_client_file = client_df is not None and not client_df.empty
    orphan_plates: list[Any] = []

    if has_client_file:
        all_plates = list(client_df.index)
        client_plate_set = set(all_plates)
        seen_orphan: set[Any] = set()
        for slug, df in indexed.items():
            if slug == "client_file" or df is None or df.empty:
                continue
            for p in df.index:
                if p and p not in client_plate_set and p not in seen_orphan:
                    seen_orphan.add(p)
                    orphan_plates.append(p)
    else:
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
            lineage=lineage,
        )

    main_df, source_by_cell, conflicts_by_cell, parse_warnings_by_cell = _build_df_for_plates(
        all_plates, fields_spec, indexed, manual_column_overrides,
        lineage=lineage, table=table,
    )

    orphan_df: Optional[pd.DataFrame] = None
    if orphan_plates:
        indexed_no_client = {k: v for k, v in indexed.items() if k != "client_file"}
        orphan_df, _, _, _ = _build_df_for_plates(
            orphan_plates, fields_spec, indexed_no_client, manual_column_overrides,
            lineage=lineage, table=table,
        )
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
        lineage=lineage,
    )


def run_vehicle(
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
    rules_path: Optional[str | Path] = None,
    priority_overrides: Optional[dict[str, list[str]]] = None,
) -> EngineResult:
    """Shortcut to run the Vehicle engine with the repo's vehicle.yml."""
    if rules_path is None:
        rules_path = Path(__file__).parent / "rules" / "vehicle.yml"
    rules = load_rules(rules_path)
    return apply_rules(
        rules,
        source_dfs,
        manual_column_overrides=manual_column_overrides,
        priority_overrides=priority_overrides,
        table="vehicle",
    )

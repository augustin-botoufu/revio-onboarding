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

Output
------
- A pandas.DataFrame whose columns = fields declared in the YAML, in the
  order they appear in the YAML.
- An `issues` list: dicts { plate, field, source, warning }

Core logic
----------
For each field, sort rules by priority ascending. For each rule in order:
  1. Locate the source DataFrame. If absent, skip (note: "source not provided").
  2. Locate the source column:
     - `column` from the YAML if present,
     - OR `manual_column_overrides[(source_slug, field)]` if provided,
     - else skip with note.
  3. Apply the declared `transform` to each row of that column.
  4. Merge row-by-row into the accumulating output column: the first
     non-null value wins (per row). Priorities ex-æquo: same behaviour —
     first rule that yields a value in the sort order wins. Divergent
     non-null values from tied priorities are flagged in issues.

Special cases
-------------
- source == "__default__" with example value: used for constant defaults
  (e.g. registrationIssueCountryCode -> FR).
- Rules without `column` AND without override are silently skipped (they
  are placeholders for sources awaiting a sample file).

Row alignment
-------------
Multiple source DataFrames must align on the plate. The engine first
builds a combined index = set(all plates across sources). Within a source
file, rows are indexed by normalized plate; rules read row-by-row from
the per-source indexed view.
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
    plate: Optional[str]
    field: str
    source: str
    warning: str


@dataclass
class EngineResult:
    df: pd.DataFrame
    issues: list[Issue] = field(default_factory=list)


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

    Rows without a detectable plate are kept at positional index (unindexed).
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


# ---------- Rule application ----------


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

    Returns (series_of_values, dict_of_per_index_warnings).
    """
    warnings_by_key: dict[Any, list[str]] = {}
    if col not in source_df.columns:
        # Column declared in rule but missing from uploaded file
        empty = pd.Series([None] * len(source_df), index=source_df.index, dtype=object)
        return empty, {k: ["column missing in uploaded file"] for k in source_df.index}
    values = []
    for key, raw in source_df[col].items():
        val, warns = transforms.apply(transform_name, raw)
        values.append(val)
        if warns:
            warnings_by_key.setdefault(key, []).extend(warns)
    # Force object dtype so ints mixed with None stay as ints (pandas otherwise
    # promotes to float64 because int can't carry NaN, producing '109.0' in
    # the CSV output instead of '109').
    return pd.Series(values, index=source_df.index, dtype=object), warnings_by_key


def apply_rules(
    rules_yaml: dict,
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
) -> EngineResult:
    """Apply all rules declared in `rules_yaml` over the provided source DataFrames.

    Returns an EngineResult with the merged output DataFrame + issues list.
    """
    manual_column_overrides = manual_column_overrides or {}
    fields_spec: dict[str, dict] = rules_yaml.get("fields", {})
    issues: list[Issue] = []

    # 1. Index each source by normalized plate. For the client file, the
    #    plate column is user-picked (via manual_column_overrides).
    indexed: dict[str, pd.DataFrame] = {}
    for slug, df in source_dfs.items():
        plate_col = manual_column_overrides.get((slug, "registrationPlate"))
        indexed[slug] = _index_by_plate(df, plate_col=plate_col)

    # 2. Determine the authoritative plate set.
    #    - If a client file is provided AND has plates: those plates are the
    #      reference parc. Any plate found in a lessor source but absent from
    #      the client file is flagged in issues (not silently added to output).
    #    - Otherwise: fallback to the union of all lessor plates, with a
    #      global warning so the user knows the parc comes from lessor data.
    client_df = indexed.get("client_file")
    has_client_file = client_df is not None and not client_df.empty

    if has_client_file:
        all_plates = list(client_df.index)
        client_plate_set = set(all_plates)
        # Flag plates present in lessor sources but NOT in the client file.
        for slug, df in indexed.items():
            if slug == "client_file" or df is None or df.empty:
                continue
            for p in df.index:
                if p and p not in client_plate_set:
                    issues.append(Issue(
                        plate=str(p),
                        field="registrationPlate",
                        source=slug,
                        warning=(
                            f"Plaque présente chez {slug} mais absente du fichier client "
                            "— à confirmer (véhicule restitué ? oubli dans le fichier parc ? "
                            "contrat externe ?). Non importée."
                        ),
                    ))
    else:
        # No client file: union of all source plates, with a global warning.
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
        # No source has a detectable plate column — return an empty output
        return EngineResult(df=pd.DataFrame(columns=list(fields_spec.keys())), issues=issues)

    # 3. For each field, walk rules in priority order
    out_df = pd.DataFrame(index=all_plates)
    winner_source_by_cell: dict[tuple[str, str], str] = {}  # (plate, field) -> source_slug
    tied_values_by_cell: dict[tuple[str, str], dict[int, dict[str, Any]]] = {}

    for field_name, spec in fields_spec.items():
        rules = sorted(spec.get("rules", []), key=lambda r: (r.get("priority", 99), r.get("source", "")))
        # Initialize column to None
        out_df[field_name] = None
        for rule in rules:
            source_slug = rule.get("source")
            transform_name = rule.get("transform", "passthrough")
            prio = rule.get("priority", 99)

            # Default constant (e.g. registrationIssueCountryCode -> FR)
            if source_slug == "__default__":
                constant = rule.get("column") or rule.get("example")
                if constant is None:
                    continue
                # Apply transform to the constant (so normalize_country_code runs)
                val, warns = transforms.apply(transform_name, constant)
                for plate in all_plates:
                    if pd.isna(out_df.at[plate, field_name]) or out_df.at[plate, field_name] is None:
                        out_df.at[plate, field_name] = val
                        winner_source_by_cell[(plate, field_name)] = source_slug
                continue

            # Real source
            src_df = indexed.get(source_slug)
            if src_df is None or src_df.empty:
                continue
            col = _get_column(source_slug, field_name, rule, manual_column_overrides)
            if col is None:
                # Rule placeholder: no column known yet for this source (waiting for a sample)
                continue
            series, warns_by_key = _apply_rule_to_series(src_df, col, transform_name)
            # Track warnings
            for key, wlist in warns_by_key.items():
                for w in wlist:
                    issues.append(Issue(plate=str(key), field=field_name, source=source_slug, warning=w))
            # Merge into out_df: first non-null wins per row; tied priorities flagged
            for plate in series.index:
                if plate not in out_df.index:
                    continue
                new_val = series.loc[plate]
                if new_val is None or (isinstance(new_val, float) and pd.isna(new_val)):
                    continue
                existing = out_df.at[plate, field_name]
                if existing is None or (isinstance(existing, float) and pd.isna(existing)):
                    out_df.at[plate, field_name] = new_val
                    winner_source_by_cell[(plate, field_name)] = source_slug
                    tied_values_by_cell.setdefault((plate, field_name), {})[prio] = {source_slug: new_val}
                else:
                    # Existing value present. If this rule has same priority → detect divergence
                    winner_source = winner_source_by_cell.get((plate, field_name))
                    winner_prio = None
                    if winner_source:
                        # Find the priority that assigned the winner
                        for r2 in rules:
                            if r2.get("source") == winner_source:
                                winner_prio = r2.get("priority", 99)
                                break
                    if winner_prio is not None and prio == winner_prio and new_val != existing:
                        issues.append(Issue(
                            plate=str(plate),
                            field=field_name,
                            source=source_slug,
                            warning=(
                                f"Conflit ex-æquo prio {prio}: "
                                f"{winner_source}={existing!r} vs {source_slug}={new_val!r} "
                                f"→ on garde {winner_source} (1er arrivé)"
                            ),
                        ))

    # Final: order columns as declared in the YAML
    out_df = out_df[[f for f in fields_spec.keys() if f in out_df.columns]]
    out_df.index.name = "plate_key"
    return EngineResult(df=out_df, issues=issues)


# ---------- Convenience ----------


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

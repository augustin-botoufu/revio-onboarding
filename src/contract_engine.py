"""Contract rules engine — applies contract.yml rules to source DataFrames.

Differs from rules_engine.py (Vehicle) on two axes:

1. **Composite primary key (plate, number)** instead of plate alone.
   Multiple contracts may share a plate over time (re-leased vehicle),
   so we key on the pair. The key string is `"{plate}|{number}"` — stable
   and easy to split downstream.

2. **Facture sources produce DataFrames with (plate, number, rubrique
   fields)** rather than one row per plate. They're indexed identically
   once the pdf_parser has built them.

Reuses the Vehicle engine's lineage, transforms, and conflict-detection
logic. Extra contract-specific rules:

- `rule_isHT_from_VP_EP` / `rule_isHT_from_VP_API`: compute isHT from
  the VP classification of the EP file where the price came from, with
  API plaques as fallback. Resolved in a second pass after all cells
  are populated, because it depends on which source won `totalPrice`
  and on the Vehicle table's VP classification (passed in via
  `vehicle_vp_by_plate`).

- `compute_months`: derive `durationMonths = (endDate - startDate)`
  when no source provided a value.

- `tolerance` post-check: for each cell where multiple whitelist sources
  agreed within R3 tolerance (2% + 2€), do not flag as conflict.

Usage
-----
    from .contract_engine import run_contract

    res = run_contract(
        source_dfs={
            "client_file": client_df,           # must have plate + number columns
            "ayvens_etat_parc": ayvens_ep_df,   # ep with plate + number
            "arval_facture_pdf": facture_df,    # produced by pdf_parser
            ...
        },
        vehicle_vp_by_plate={"AB-123-CD": True, "EF-456-GH": False},
    )
    df = res.df                 # contract rows
    lineage = res.lineage       # full provenance, ready for Jalon 5.0
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml  # type: ignore

from . import transforms
from .lineage import LineageStore, LineageRecord, build_rule_id, conflict_dict
from .normalizers import plate_for_matching


# ---------- Types ----------


@dataclass
class Issue:
    plate: Optional[str]
    number: Optional[str]
    field: str
    source: str
    warning: str


@dataclass
class ContractEngineResult:
    df: pd.DataFrame
    issues: list[Issue] = field(default_factory=list)
    source_by_cell: dict[tuple[str, str], str] = field(default_factory=dict)
    conflicts_by_cell: dict[tuple[str, str], list[tuple[str, Any]]] = field(default_factory=dict)
    parse_warnings_by_cell: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    rules_yaml: Optional[dict] = None
    lineage: Optional[LineageStore] = None
    # Contracts seen in lessor files but absent from client_file
    orphan_df: Optional[pd.DataFrame] = None
    # List of (plate, number, field, column_candidates) that the engine
    # couldn't resolve because no source matched and the field is
    # flagged for interactive user mapping (Jalon 4.1.7).
    unknown_column_requests: list[dict] = field(default_factory=list)


# ---------- YAML loading ----------


def load_rules(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- Key helpers ----------


def _make_key(plate: Any, number: Any, plate_only: bool = False) -> Optional[str]:
    """Build a composite key.

    In normal mode: ``"{plate}|{number}"`` — both required.
    In plate-only mode: ``"{plate}|*"`` — marker `*` means "no contract
    number in source; key by plate alone". All sources in the same run
    MUST use the same mode, otherwise joins silently mis-align.
    """
    p = plate_for_matching(plate) if plate is not None else None
    if not p:
        return None
    if plate_only:
        return f"{p}|*"
    n = str(number).strip() if number is not None and str(number).strip() else None
    if not n:
        return None
    return f"{p}|{n}"


def split_key(key: str) -> tuple[str, str]:
    plate, _, number = key.partition("|")
    return plate, number


def _find_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """First column in `df` whose lowercased name contains any candidate."""
    for col in df.columns:
        low = str(col).strip().lower()
        for c in candidates:
            if c in low:
                return col
    return None


_PLATE_CANDS = ["plaque", "immat", "n° immat", "no immat", "plate"]

# Strong, unambiguous markers for "numéro de contrat" — no false positives.
_NUMBER_STRONG = [
    "numéro contrat", "numero contrat", "numéro de contrat", "numero de contrat",
    "n° contrat", "no contrat", "n°contrat", "n° de contrat",
    "n°contr", "nocontr", "contract number", "contract_number",
    "réf contrat", "ref contrat", "référence contrat", "reference contrat",
]
# Loose markers — accepted ONLY if the column doesn't also mention a reject
# token (km, durée, date, loyer, avantage en nature…). The bare word "contrat"
# used to catch "Km contrat" in the client file; hence the reject guard.
_NUMBER_LOOSE = ["contrat", "number", "réf", "ref"]
_NUMBER_REJECT = [
    "km", "kilom", "kilomét", "kilomet",
    "durée", "duree", "date", "début", "debut", "fin", "echéance", "echeance",
    "loyer", "aen", "avantage",
    "marque", "modèle", "modele", "couleur", "immatric",
]


def _find_number_column(df: pd.DataFrame) -> Optional[str]:
    """Detect the contract-number column, rejecting false positives.

    Uses two passes:
      1. Strong markers (e.g. "N° Contrat", "Numéro de contrat") win outright.
      2. Loose markers (e.g. "contrat", "ref") match only if the column
         header does NOT contain a reject token like "km", "durée", "date"…
    """
    for col in df.columns:
        low = str(col).strip().lower()
        for c in _NUMBER_STRONG:
            if c in low:
                return col
    for col in df.columns:
        low = str(col).strip().lower()
        if any(rt in low for rt in _NUMBER_REJECT):
            continue
        for c in _NUMBER_LOOSE:
            if c in low:
                return col
    return None


def _index_by_composite(df: pd.DataFrame, plate_only: bool = False) -> pd.DataFrame:
    """Index a source df by composite key.

    With ``plate_only=True``, key by plate alone (first row per plate wins).
    Used when the client file has no contract-number column — all other
    sources must then switch to plate-only too so joins align.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    out["__src_row__"] = range(len(out))
    plate_col = "plate" if "plate" in out.columns else _find_column(out, _PLATE_CANDS)
    if plate_only:
        if plate_col is None:
            return out
        keys = [_make_key(p, None, plate_only=True) for p in out[plate_col]]
    else:
        number_col = "number" if "number" in out.columns else _find_number_column(out)
        if plate_col is None or number_col is None:
            return out  # caller will skip this source silently
        keys = [_make_key(p, n) for p, n in zip(out[plate_col], out[number_col])]
    out["__key__"] = keys
    out = out.dropna(subset=["__key__"])
    out = out.drop_duplicates(subset=["__key__"], keep="first")
    out = out.set_index("__key__", drop=True)
    return out


# ---------- Rule application ----------


_PARSER_DF_SOURCES = {
    "arval_facture_pdf", "ayvens_facture_pdf", "autre_loueur_facture_pdf",
}


def _get_column(source_slug, field_name, rule, manual_overrides, src_df: Optional[pd.DataFrame] = None):
    """Return the column name to read in `src_df` for this rule.

    Resolution order:
      1. Explicit override from UI (manual mapping).
      2. Rule's declared column (as in YAML / spec).
      3. Fallback: for 'virtual' sources (pdf_parser output, client_file
         normalized through learned_patterns), if the declared column is
         absent but a column matching `field_name` exists, use it. This
         lets the engine consume the clean columns produced by pdf_parser
         (`totalPrice`, `durationMonths`, …) without the spec having to
         enumerate them verbatim.
    """
    override = manual_overrides.get((source_slug, field_name))
    if override:
        return override
    col = rule.get("column")
    if col and src_df is not None and col in src_df.columns:
        return col
    if src_df is not None:
        if field_name in src_df.columns and (
            source_slug in _PARSER_DF_SOURCES or source_slug == "client_file"
        ):
            return field_name
    return col


def _is_null(v):
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def _values_differ(a, b) -> bool:
    if isinstance(a, str) and isinstance(b, str):
        return a.strip().casefold() != b.strip().casefold()
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        # Numeric equality within epsilon
        if math.isnan(a) or math.isnan(b):
            return a != b
        return abs(a - b) > 1e-6
    return a != b


def _within_tolerance(a: float, b: float, pct: float = 0.02, abs_tol: float = 2.0) -> bool:
    if a is None or b is None:
        return False
    delta = abs(a - b)
    max_val = max(abs(a), abs(b))
    return delta <= max(abs_tol, pct * max_val)


def _source_row_for(src_df: pd.DataFrame, key: Any) -> Optional[int]:
    if "__src_row__" not in src_df.columns:
        return None
    try:
        v = src_df.at[key, "__src_row__"]
        return int(v) if v is not None else None
    except Exception:
        return None


# Constant-value transforms: the YAML rule has ``source: '*'`` and a
# ``const_XX`` transform. They emit a fixed value regardless of input.
_CONST_TRANSFORMS = {
    "const_FR": "FR",
}


def _apply_rule_transform(raw: Any, transform_name: str) -> tuple[Any, list[str]]:
    # Contract engine knows a couple of transforms the Vehicle transforms
    # registry may not (they're specific to the contract spec).
    name = transform_name or "passthrough"
    if name in _CONST_TRANSFORMS:
        return _CONST_TRANSFORMS[name], []
    if name in {"cross_check", "BANNED", "rule_isHT_from_VP_EP",
                "rule_isHT_from_VP_API", "compute_months",
                "sum_whitelist", "regex_number", "regex_duration",
                "regex_mileage", "regex_start_date", "regex_restit_date"}:
        # These are markers consumed elsewhere — passthrough the raw
        # value so the engine can still record it in lineage.
        return raw if raw is not None else None, []
    return transforms.apply(name, raw)


def _resolve_cell(
    key: str,
    field_name: str,
    fields_spec: dict,
    indexed_sources: dict[str, pd.DataFrame],
    manual_column_overrides: dict[tuple[str, str], str],
    parse_warnings_by_cell: dict[tuple[str, str], list[str]],
    lineage: LineageStore,
) -> tuple[Any, Optional[str], list[tuple[str, Any]]]:
    spec = fields_spec[field_name]
    rules = sorted(
        spec.get("rules", []),
        key=lambda r: (r.get("priority", 99), r.get("source", "")),
    )

    contributions: list[dict] = []
    for rule in rules:
        source_slug = rule.get("source")
        if not source_slug or rule.get("banned"):
            continue
        if source_slug in {"rule_engine", "derived"}:
            # Deferred rules — handled in post-passes.
            continue
        prio = rule.get("priority", 99)
        transform_name = rule.get("transform", "passthrough")
        if transform_name == "cross_check":
            # Not a value contributor — used only for anomaly detection.
            continue

        # Constant-transform rules (source: '*' in YAML) don't read from
        # any source column — they emit a fixed value (e.g. const_FR → "FR").
        if source_slug == "*":
            val, warns = _apply_rule_transform(None, transform_name)
            if warns:
                parse_warnings_by_cell.setdefault((key, field_name), []).extend(warns)
            if _is_null(val):
                continue
            contributions.append({
                "priority": prio,
                "source": "rule_engine",
                "value": val,
                "column": rule.get("column") or "(constante)",
                "src_row": None,
                "transform": transform_name,
                "warnings": list(warns) if warns else [],
            })
            continue

        src_df = indexed_sources.get(source_slug)
        if src_df is None or src_df.empty:
            continue
        if key not in src_df.index:
            continue
        col = _get_column(source_slug, field_name, rule, manual_column_overrides, src_df)
        if col is None:
            continue
        if col not in src_df.columns:
            continue
        raw = src_df.at[key, col]
        val, warns = _apply_rule_transform(raw, transform_name)
        if warns:
            parse_warnings_by_cell.setdefault((key, field_name), []).extend(warns)
        if _is_null(val):
            continue
        contributions.append({
            "priority": prio,
            "source": source_slug,
            "value": val,
            "column": col,
            "src_row": _source_row_for(src_df, key),
            "transform": transform_name,
            "warnings": list(warns) if warns else [],
        })

    if not contributions:
        return None, None, []

    contributions.sort(key=lambda x: (x["priority"], x["source"]))
    winner = contributions[0]
    winner_val = winner["value"]

    # Tolerance-aware conflict detection for numeric price fields
    is_price = field_name.endswith("Price")
    conflicts: list[tuple[str, Any]] = []
    tolerance_hits: list[tuple[str, Any]] = []
    has_conflict = False
    for c in contributions:
        if is_price and isinstance(c["value"], (int, float)) and isinstance(winner_val, (int, float)):
            if _within_tolerance(c["value"], winner_val):
                tolerance_hits.append((c["source"], c["value"]))
                continue
        if _values_differ(c["value"], winner_val):
            has_conflict = True
            break

    if has_conflict:
        seen = set()
        for c in contributions:
            key_c = (c["source"], str(c["value"]))
            if key_c in seen:
                continue
            seen.add(key_c)
            conflicts.append((c["source"], c["value"]))

    # Lineage
    conflicts_ignored: list[dict] = []
    for c in contributions[1:]:
        if _values_differ(c["value"], winner_val):
            if is_price and _within_tolerance(c["value"], winner_val):
                reason = (
                    f"écart {abs(c['value'] - winner_val):.2f} dans la tolérance 2€+2% — non flaggé"
                )
            else:
                reason = (
                    f"priorité inférieure ({c['priority']} vs {winner['priority']})"
                    if c["priority"] != winner["priority"]
                    else "source écartée par ordre alphabétique à priorité égale"
                )
        else:
            reason = "valeur identique — non conflictuelle"
        conflicts_ignored.append(conflict_dict(c["source"], c["value"], reason))

    lineage.record(LineageRecord(
        table="contract",
        key=key,
        field=field_name,
        value=winner_val,
        source_used=winner["source"],
        source_col=winner["column"],
        source_row=winner["src_row"],
        priority=winner["priority"],
        transform=winner["transform"],
        rule_id=build_rule_id("contract", field_name, winner["source"], winner["priority"]),
        conflicts_ignored=conflicts_ignored,
        notes=None,
        warnings=winner["warnings"],
    ))

    return winner_val, winner["source"], conflicts


# ---------- Post-passes (derived fields) ----------


def _iso_to_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    s = str(s)
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _postpass_compute_months(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    lineage: LineageStore,
) -> None:
    """Fill durationMonths from (endDate - startDate) when still empty."""
    if "durationMonths" not in out_df.columns:
        return
    for key in out_df.index:
        if not _is_null(out_df.at[key, "durationMonths"]):
            continue
        start = _iso_to_date(out_df.at[key, "startDate"]) if "startDate" in out_df.columns else None
        end = _iso_to_date(out_df.at[key, "endDate"]) if "endDate" in out_df.columns else None
        if not start or not end:
            continue
        months = (end.year - start.year) * 12 + (end.month - start.month)
        if months <= 0:
            continue
        out_df.at[key, "durationMonths"] = months
        source_by_cell[(key, "durationMonths")] = "derived"
        lineage.record(LineageRecord(
            table="contract", key=key, field="durationMonths",
            value=months, source_used="derived",
            source_col=None, source_row=None, priority=99,
            transform="compute_months",
            rule_id=build_rule_id("contract", "durationMonths", "derived", 99),
            conflicts_ignored=[],
            notes="Calculé depuis endDate − startDate.",
        ))


def _postpass_isHT(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    lineage: LineageStore,
    vehicle_vp_by_plate: dict[str, bool],
    vp_from_ep_by_key: dict[str, tuple[bool, str]],
) -> None:
    """Resolve isHT: VP → FALSE (TTC) ; non-VP → TRUE (HT).

    Priority per key:
      1. VP from the EP file where totalPrice came from (if known)
      2. VP from API plaques (via vehicle table)
    """
    if "isHT" not in out_df.columns:
        return
    for key in out_df.index:
        if not _is_null(out_df.at[key, "isHT"]):
            continue
        plate, _ = split_key(key)
        is_vp: Optional[bool] = None
        chosen_source = None
        # 1. EP
        if key in vp_from_ep_by_key:
            is_vp, chosen_source = vp_from_ep_by_key[key]
        # 2. API plaques (via vehicle table)
        if is_vp is None and plate in vehicle_vp_by_plate:
            is_vp = vehicle_vp_by_plate[plate]
            chosen_source = "api_plaque"
        if is_vp is None:
            continue
        is_ht = not bool(is_vp)
        out_df.at[key, "isHT"] = is_ht
        source_by_cell[(key, "isHT")] = chosen_source or "rule_engine"
        lineage.record(LineageRecord(
            table="contract", key=key, field="isHT",
            value=is_ht, source_used=chosen_source or "rule_engine",
            source_col="VP flag", source_row=None,
            priority=1 if chosen_source and chosen_source != "api_plaque" else 2,
            transform="rule_isHT_from_VP",
            rule_id=build_rule_id("contract", "isHT", chosen_source or "rule_engine", 1),
            conflicts_ignored=[],
            notes=("VP → TTC (isHT=False) / non-VP → HT (isHT=True). "
                   "Conflit EP↔API_Plaque pas logué ici — déjà dans erreurs Vehicle (cf. R6)."),
        ))


# ---------- Main API ----------


def _extract_vp_from_ep_sources(
    indexed: dict[str, pd.DataFrame],
) -> dict[str, tuple[bool, str]]:
    """Scan EP-type sources for a VP indicator column.

    Returns dict: key → (is_vp_bool, source_slug). First EP hit wins.
    """
    out: dict[str, tuple[bool, str]] = {}
    EP_SLUGS = ["ayvens_etat_parc", "autre_loueur_ep"]
    VP_HEADERS = ["genre", "type véhicule", "type vehicule", "catégorie véhicule",
                  "vp", "vu", "vehicle_type"]
    for slug in EP_SLUGS:
        df = indexed.get(slug)
        if df is None or df.empty:
            continue
        col = _find_column(df, VP_HEADERS)
        if col is None:
            continue
        for key, val in df[col].items():
            if key in out or _is_null(val):
                continue
            s = str(val).strip().upper()
            if s in {"VP", "PARTICULIER", "TOURISME", "VOITURE PARTICULIERE"}:
                out[key] = (True, slug)
            elif s in {"VU", "UTILITAIRE", "COMMERCIAL", "VP+", "PL"}:
                out[key] = (False, slug)
    return out


def apply_rules(
    rules_yaml: dict,
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
    vehicle_vp_by_plate: Optional[dict[str, bool]] = None,
) -> ContractEngineResult:
    manual_column_overrides = manual_column_overrides or {}
    vehicle_vp_by_plate = vehicle_vp_by_plate or {}
    fields_spec: dict[str, dict] = rules_yaml.get("fields", {})
    issues: list[Issue] = []
    lineage = LineageStore()

    # Detect plate-only mode: if the client file has no detectable contract
    # number column, ALL sources must be indexed by plate alone, otherwise
    # joins silently mis-align (client keyed by "PLATE|170000" from a "Km
    # contrat" false-positive while EP is keyed by "PLATE|W90322"). In that
    # case, we key every source by plate alone (first row per plate wins).
    plate_only_mode = False
    client_file_raw = source_dfs.get("client_file")
    if client_file_raw is not None and not client_file_raw.empty:
        has_number_col = (
            "number" in client_file_raw.columns
            or _find_number_column(client_file_raw) is not None
        )
        if not has_number_col:
            plate_only_mode = True
            issues.append(Issue(
                plate=None, number=None, field="number", source="__engine__",
                warning=(
                    "Aucun numéro de contrat détecté dans le fichier client — "
                    "indexation par plaque seule sur tous les fichiers. "
                    "Si plusieurs contrats partagent la même plaque, seul le "
                    "premier est retenu par source."
                ),
            ))

    indexed: dict[str, pd.DataFrame] = {
        slug: _index_by_composite(df, plate_only=plate_only_mode)
        for slug, df in source_dfs.items()
    }

    client_df = indexed.get("client_file")
    has_client = client_df is not None and not client_df.empty

    orphan_keys: list[str] = []
    if has_client:
        all_keys = list(client_df.index)
        client_key_set = set(all_keys)
        seen_orphan: set[str] = set()
        for slug, df in indexed.items():
            if slug == "client_file" or df is None or df.empty:
                continue
            for k in df.index:
                if k and k not in client_key_set and k not in seen_orphan:
                    seen_orphan.add(k)
                    orphan_keys.append(k)
    else:
        all_keys = []
        seen: set[str] = set()
        for df in indexed.values():
            if df is None or df.empty:
                continue
            for k in df.index:
                if k and k not in seen:
                    seen.add(k)
                    all_keys.append(k)
        if all_keys:
            issues.append(Issue(
                plate=None, number=None, field="number", source="__engine__",
                warning=(
                    "Import Contract sans fichier client — parc contrats dérivé "
                    "de l'union des fichiers loueurs. Vérifier manuellement."
                ),
            ))

    if not all_keys:
        return ContractEngineResult(
            df=pd.DataFrame(columns=list(fields_spec.keys())),
            issues=issues, rules_yaml=rules_yaml, lineage=lineage,
        )

    source_by_cell: dict[tuple[str, str], str] = {}
    conflicts_by_cell: dict[tuple[str, str], list[tuple[str, Any]]] = {}
    parse_warnings_by_cell: dict[tuple[str, str], list[str]] = {}

    out_df = pd.DataFrame(
        {f: pd.Series([None] * len(all_keys), dtype=object) for f in fields_spec.keys()},
        index=all_keys,
    )

    for field_name in fields_spec.keys():
        # Skip fields handled only by post-pass
        for key in all_keys:
            val, src, conflicts = _resolve_cell(
                key, field_name, fields_spec, indexed,
                manual_column_overrides, parse_warnings_by_cell, lineage,
            )
            if val is not None:
                out_df.at[key, field_name] = val
                source_by_cell[(key, field_name)] = src
            if conflicts:
                conflicts_by_cell[(key, field_name)] = conflicts

    # Backfill `plate` and `number` fields from the composite key when no
    # rule successfully populated them. Rationale: the client-file YAML rule
    # for `plate` declares `column: 'Plaque / Immatriculation'` but real
    # client files often use "Immat" / "Immatriculation" / etc. — the
    # heuristic column-detector already found it to build the key; we can
    # surface that same plate on the output row. Same for `number` (except
    # in plate-only mode where number is the sentinel `*`, which we skip).
    if "plate" in out_df.columns:
        for key in all_keys:
            if not _is_null(out_df.at[key, "plate"]):
                continue
            p, _ = split_key(key)
            if p:
                out_df.at[key, "plate"] = p
                source_by_cell[(key, "plate")] = "derived"
                lineage.record(LineageRecord(
                    table="contract", key=key, field="plate",
                    value=p, source_used="derived",
                    source_col="(clé composite)", source_row=None, priority=99,
                    transform="from_composite_key",
                    rule_id=build_rule_id("contract", "plate", "derived", 99),
                    conflicts_ignored=[],
                    notes="Plaque reconstruite depuis la clé composite (aucune règle YAML ne l'a remplie).",
                ))
    if "number" in out_df.columns and not plate_only_mode:
        for key in all_keys:
            if not _is_null(out_df.at[key, "number"]):
                continue
            _, n = split_key(key)
            if n and n != "*":
                out_df.at[key, "number"] = n
                source_by_cell[(key, "number")] = "derived"
                lineage.record(LineageRecord(
                    table="contract", key=key, field="number",
                    value=n, source_used="derived",
                    source_col="(clé composite)", source_row=None, priority=99,
                    transform="from_composite_key",
                    rule_id=build_rule_id("contract", "number", "derived", 99),
                    conflicts_ignored=[],
                    notes="Numéro reconstruit depuis la clé composite.",
                ))

    # Cross-check: plates in lessor files but absent from client_file → anomaly
    for slug, df in indexed.items():
        if slug in ("client_file", "api_plaque") or df is None or df.empty:
            continue
        for k in df.index:
            # Only cross-check sources that were composite-indexed (key like "PLATE|NUMBER").
            # EP files without a contract number column remain integer-indexed and are skipped.
            if not isinstance(k, str) or "|" not in k:
                continue
            plate, number = split_key(k)
            if number == "*":
                number = None
            if has_client and k not in client_df.index:
                # logged as anomaly (will go into contract_errors.xlsx)
                issues.append(Issue(
                    plate=plate, number=number, field="plate", source=slug,
                    warning=f"Plaque/contrat présent dans {slug} mais absent du fichier client (cross-check plate exclusive)."
                ))

    # Post-passes
    vp_from_ep = _extract_vp_from_ep_sources(indexed)
    _postpass_isHT(out_df, source_by_cell, lineage, vehicle_vp_by_plate, vp_from_ep)
    _postpass_compute_months(out_df, source_by_cell, lineage)

    out_df.index.name = "contract_key"

    orphan_df: Optional[pd.DataFrame] = None
    if orphan_keys:
        indexed_no_client = {k: v for k, v in indexed.items() if k != "client_file"}
        orphan_df = pd.DataFrame(
            {f: pd.Series([None] * len(orphan_keys), dtype=object) for f in fields_spec.keys()},
            index=orphan_keys,
        )
        for field_name in fields_spec.keys():
            for key in orphan_keys:
                val, _, _ = _resolve_cell(
                    key, field_name, fields_spec, indexed_no_client,
                    manual_column_overrides, parse_warnings_by_cell, lineage,
                )
                if val is not None:
                    orphan_df.at[key, field_name] = val
        # Same backfill as main df: plate + number from composite key.
        if "plate" in orphan_df.columns:
            for key in orphan_keys:
                if _is_null(orphan_df.at[key, "plate"]):
                    p, _ = split_key(key)
                    if p:
                        orphan_df.at[key, "plate"] = p
        if "number" in orphan_df.columns and not plate_only_mode:
            for key in orphan_keys:
                if _is_null(orphan_df.at[key, "number"]):
                    _, n = split_key(key)
                    if n and n != "*":
                        orphan_df.at[key, "number"] = n
        orphan_df.index.name = "contract_key"

    # Unknown column requests: one entry per mandatory field left unresolved
    # on any row, not per (key, field) pair — the candidate sources and hint
    # come from the YAML spec, so all rows share the same card. We aggregate
    # a sample of affected keys + a total count for the UI (Jalon 4.2.3).
    unknown_requests: list[dict] = []
    for field_name, spec in fields_spec.items():
        if not spec.get("mandatory"):
            continue
        if not spec.get("rules"):
            continue
        affected_keys: list[str] = [
            key for key in all_keys if _is_null(out_df.at[key, field_name])
        ]
        if not affected_keys:
            continue
        candidate_sources = [
            r.get("source") for r in spec["rules"]
            if r.get("source") and r.get("source") not in {"rule_engine", "derived"}
        ]
        # Sample a few keys for the UI preview ("ex : AB-123-CD / 12345").
        # In plate-only mode, the "number" slot is the sentinel '*' — blank it
        # so the UI displays just the plate instead of "AB-123-CD / *".
        sample_pairs: list[tuple[str, str]] = []
        for k in affected_keys[:3]:
            p, n = split_key(k)
            if n == "*":
                n = ""
            sample_pairs.append((p, n))
        unknown_requests.append({
            "field": field_name,
            "candidate_sources": candidate_sources,
            "hint": spec.get("notes") or spec.get("description"),
            "affected_count": len(affected_keys),
            "total_rows": len(all_keys),
            "sample_pairs": sample_pairs,
            # Keep first pair for backwards-compat with older UI code that
            # still reads req["plate"] / req["number"].
            "plate": sample_pairs[0][0] if sample_pairs else None,
            "number": sample_pairs[0][1] if sample_pairs else None,
        })

    return ContractEngineResult(
        df=out_df, issues=issues,
        source_by_cell=source_by_cell,
        conflicts_by_cell=conflicts_by_cell,
        parse_warnings_by_cell=parse_warnings_by_cell,
        rules_yaml=rules_yaml, lineage=lineage,
        orphan_df=orphan_df,
        unknown_column_requests=unknown_requests,
    )


def run_contract(
    source_dfs: dict[str, pd.DataFrame],
    manual_column_overrides: Optional[dict[tuple[str, str], str]] = None,
    vehicle_vp_by_plate: Optional[dict[str, bool]] = None,
    rules_path: Optional[str | Path] = None,
) -> ContractEngineResult:
    if rules_path is None:
        rules_path = Path(__file__).parent / "rules" / "contract.yml"
    return apply_rules(
        load_rules(rules_path), source_dfs,
        manual_column_overrides=manual_column_overrides,
        vehicle_vp_by_plate=vehicle_vp_by_plate,
    )

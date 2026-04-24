"""Contract rules engine — applies contract.yml rules to source DataFrames.

**Plate-primary indexing (Jalon 4.2.6)** : every source is keyed by plate
alone. Rationale: Augustin's rule — "la plaque est l'input, si on trouve
les infos on les remplit, sinon on laisse vide". The previous composite
`(plate, number)` scheme silently dropped rows when a source's "number"
heuristic misfired (e.g. `Réf. cli/cond.` in Ayvens AND captured as
"number", giving keys like `PLATE|JULIE PINOCHET` that overlapped
nothing).

Consequences:
- Output = 1 row per client plate, exactly. Lessor data merges in by
  plate match.
- The `number` field becomes a regular populated field (via the YAML
  rules that read explicit columns like `N° Contrat`), not a join key.
- Orphans = plates in lessor files absent from `client_file`.
- If a lessor has multiple rows for the same plate (e.g. historical
  contracts), the FIRST row wins. Earlier contracts are lost — document
  for users via a warning.

Facture sources (pdf_parser output) are indexed the same way — one row
per plate.

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


# ---------- Key helpers (plate-primary since Jalon 4.2.6) ----------


def _make_key(plate: Any, number: Any = None, plate_only: bool = True) -> Optional[str]:
    """Build a contract key.

    Since Jalon 4.2.6 we always key by plate alone. ``number`` and
    ``plate_only`` arguments are kept for backwards compatibility with
    any external caller but are ignored — the returned key is always
    just the normalized plate.
    """
    p = plate_for_matching(plate) if plate is not None else None
    if not p:
        return None
    return p


def split_key(key: str) -> tuple[str, str]:
    """Return (plate, number) — number is always "" post-4.2.6 (plate-only).

    Kept for callers that still pattern-match on composite keys (e.g.
    UI previews). The second slot will always be empty string now.
    """
    if not isinstance(key, str):
        return (str(key) if key is not None else "", "")
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


def _find_plate_column(df: pd.DataFrame) -> Optional[str]:
    """Find the plate column. Accepts, in order of preference:

    1. ``__map__plate`` or ``__map__registrationPlate`` — canonical columns
       emitted by ``pipeline.merge_engine_sources`` when the user has
       already mapped the plate column in the **Vehicle** tab. Jalon 4.2.6
       honors that mapping so Augustin doesn't have to map the same thing
       twice (complaint #2: "tu me demandes de matcher des champs que j'ai
       déjà matché dans la base véhicule").
    2. Literal ``plate`` column.
    3. French heuristic : "Plaque", "Immat", etc.
    """
    for canonical in ("__map__plate", "__map__registrationPlate"):
        if canonical in df.columns:
            return canonical
    if "plate" in df.columns:
        return "plate"
    return _find_column(df, _PLATE_CANDS)


def _index_by_plate(df: pd.DataFrame) -> pd.DataFrame:
    """Index a source DataFrame by normalized plate.

    Replaces the Jalon ≤4.2.5 composite-key indexing. Rationale in the
    module docstring. First row per plate wins — if a source has
    multiple historical rows for the same plate, downstream rules see
    only the first.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    out["__src_row__"] = range(len(out))
    plate_col = _find_plate_column(out)
    if plate_col is None:
        return out  # caller will skip this source silently
    keys = [_make_key(p) for p in out[plate_col]]
    out["__key__"] = keys
    out = out.dropna(subset=["__key__"])
    out = out.drop_duplicates(subset=["__key__"], keep="first")
    out = out.set_index("__key__", drop=True)
    return out


# Backwards-compat alias (some callers may still reference the old name).
_index_by_composite = _index_by_plate


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


def _within_tolerance(a: Any, b: Any, pct: float = 0.02, abs_tol: float = 2.0) -> bool:
    """Return True if two numeric values are within ``max(abs_tol, pct*max)``.

    Guarded against non-numeric inputs (Jalon 4.2.8) — if either argument is
    anything other than int/float (e.g. a raw string that leaked through a
    client_file IA-mapping without a price transform), return False so the
    caller falls through to the normal "different values → conflict" path
    rather than crashing on ``"25000" - "25000"``.
    """
    if a is None or b is None:
        return False
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
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
                "regex_mileage", "regex_start_date", "regex_restit_date",
                # Rule-markers resolved at post-pass (need sibling fields).
                "rule_price_positive", "rule_or",
                # Lookup transforms resolved at write time (partner index).
                "lookup_partner", "lookup_by_source_slug"}:
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
    vp_from_api_by_key: Optional[dict[str, tuple[bool, str]]] = None,
) -> None:
    """Resolve isHT: VP → FALSE (TTC) ; non-VP → TRUE (HT).

    Priority per key (Jalon 4.2.10) :
      1. VP from the EP file where totalPrice came from (if known)
      2. VP from api_plaques source (read directly by the Contract engine)
      3. VP from vehicle_vp_by_plate (Vehicle engine result, fallback)

    The key addition in 4.2.10 is step 2 : on lit désormais ``api_plaques``
    depuis le moteur Contract lui-même (via ``_extract_vp_from_api_plaques``).
    Résultat : isHT se remplit même si l'utilisateur n'a pas cliqué
    Appliquer sur l'onglet Véhicules avant l'onglet Contrats.
    """
    vp_from_api_by_key = vp_from_api_by_key or {}
    if "isHT" not in out_df.columns:
        return
    for key in out_df.index:
        if not _is_null(out_df.at[key, "isHT"]):
            continue
        plate, _ = split_key(key)
        is_vp: Optional[bool] = None
        chosen_source = None
        # 1. EP loueur
        if key in vp_from_ep_by_key:
            is_vp, chosen_source = vp_from_ep_by_key[key]
        # 2. api_plaques directement (ne dépend pas du run Vehicle)
        if is_vp is None and key in vp_from_api_by_key:
            is_vp, chosen_source = vp_from_api_by_key[key]
        # 3. Fallback Vehicle engine result (si run dans la même session)
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


def _extract_vp_from_api_plaques(
    indexed: dict[str, pd.DataFrame],
) -> dict[str, tuple[bool, str]]:
    """Scan the ``api_plaques`` source for the SIV genre code.

    (Jalon 4.2.10) Avant, isHT ne pouvait être rempli que si (a) un EP
    loueur avait une colonne VP/VU reconnue OU (b) le moteur Véhicules
    avait été lancé dans la session ET que ``engine_result.df.usage``
    était populé. En pratique l'EP loueur ne couvre pas toutes les
    plaques et ``engine_result`` se fait invalider dès qu'un fichier
    est re-uploadé → 52/54 cellules isHT restaient vides.

    Le fichier ``api_plaques`` (SIV officiel) couvre toutes les plaques
    du parc par construction — on peut l'exploiter directement, sans
    dépendance sur l'ordre des onglets.

    Returns dict: key → (is_vp_bool, 'api_plaque').
    """
    out: dict[str, tuple[bool, str]] = {}
    df = indexed.get("api_plaques")
    if df is None or df.empty:
        return out
    # Column candidates, ordered by specificity.
    col_candidates = ["genreVCGNGC", "genre", "genreCG", "genre_cg",
                      "genre_cgi", "categorie", "catégorie"]
    col = _find_column(df, col_candidates)
    if col is None:
        return out
    for key, val in df[col].items():
        if key in out or _is_null(val):
            continue
        low = str(val).strip().lower()
        # Codes SIV stricts + variantes libellé.
        if any(h in low for h in ("vp", "particul", "tourisme")):
            out[key] = (True, "api_plaque")
        elif any(h in low for h in ("vu", "utilit", "commercial",
                                     "camion", "fourgon", "ctte", "pl")):
            out[key] = (False, "api_plaque")
    return out


def _extract_vp_from_ep_sources(
    indexed: dict[str, pd.DataFrame],
) -> dict[str, tuple[bool, str]]:
    """Scan EP-type sources for a VP indicator column.

    Returns dict: key → (is_vp_bool, source_slug). First EP hit wins.

    (Jalon 4.2.9) Headers ET valeurs sont matchés de façon tolérante
    (substring) pour couvrir les variantes terrain ("Genre du véhicule",
    "Catégorie", "Type (VP/VU)", valeurs "Voiture particulière" avec
    accent, "Utilitaire léger", etc.). Avant 4.2.9 on n'acceptait qu'une
    liste fermée exacte → isHT restait quasi toujours vide.
    """
    out: dict[str, tuple[bool, str]] = {}
    EP_SLUGS = ["ayvens_etat_parc", "autre_loueur_ep"]
    # Substrings — on matche si l'un est présent dans le header normalisé.
    VP_HEADER_HINTS = (
        "genre", "type véhicule", "type vehicule", "catégorie", "categorie",
        "vp/vu", "vp / vu", "vehicle_type", "type de véhicule", "type de vehicule",
        "classification", "nature du véhicule", "nature du vehicule",
    )
    # Substrings — on matche si l'un est présent dans la valeur normalisée.
    VP_VALUE_HINTS = ("vp", "particul", "tourisme")
    VU_VALUE_HINTS = ("vu", "utilit", "commercial", "pl", "camion", "fourgon")

    def _find_vp_col(df: pd.DataFrame) -> Optional[str]:
        for c in df.columns:
            low = str(c).strip().lower()
            for h in VP_HEADER_HINTS:
                if h in low:
                    return c
        return None

    for slug in EP_SLUGS:
        df = indexed.get(slug)
        if df is None or df.empty:
            continue
        col = _find_vp_col(df)
        if col is None:
            continue
        for key, val in df[col].items():
            if key in out or _is_null(val):
                continue
            low = str(val).strip().lower()
            # VU first (plus spécifique : "VU" est un substring de "vur",
            # mais on préfère VU-over-VP quand les 2 matchent par sécurité).
            if any(h in low for h in VU_VALUE_HINTS):
                out[key] = (False, slug)
            elif any(h in low for h in VP_VALUE_HINTS):
                out[key] = (True, slug)
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

    # Plate-primary indexing (Jalon 4.2.6): every source keyed by plate
    # alone. ``number`` is resolved as a regular field via the YAML rules
    # (each source declares its N° Contrat column explicitly) — never
    # used as a join key. Output = 1 row per client plate.
    indexed: dict[str, pd.DataFrame] = {
        slug: _index_by_plate(df) for slug, df in source_dfs.items()
    }

    # Warn users when a lessor file has multiple rows per plate (historical
    # contracts) so they know only the first row was retained.
    for slug, df_raw in source_dfs.items():
        if df_raw is None or df_raw.empty:
            continue
        plate_col = _find_plate_column(df_raw)
        if plate_col is None:
            continue
        normalized = [plate_for_matching(p) for p in df_raw[plate_col]]
        normalized = [p for p in normalized if p]
        if len(normalized) != len(set(normalized)):
            from collections import Counter
            dupes = [p for p, c in Counter(normalized).items() if c > 1]
            issues.append(Issue(
                plate=None, number=None, field="plate", source=slug,
                warning=(
                    f"{len(dupes)} plaque(s) avec plusieurs lignes dans {slug} — "
                    f"seule la 1re ligne par plaque est retenue "
                    f"(ex : {', '.join(dupes[:3])})."
                ),
            ))

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

    # Backfill `plate` from the key (which IS the plate post-4.2.6).
    # If the YAML rule for `plate` targets a column name that doesn't exist
    # in the client file (e.g. rule wants "Plaque / Immatriculation" but
    # file has "Immat"), the rule won't fire — but the plate-detection
    # heuristic already found the column to build the index, so we just
    # copy the key into the cell.
    if "plate" in out_df.columns:
        for key in all_keys:
            if not _is_null(out_df.at[key, "plate"]):
                continue
            if key:
                out_df.at[key, "plate"] = key
                source_by_cell[(key, "plate")] = "derived"
                lineage.record(LineageRecord(
                    table="contract", key=key, field="plate",
                    value=key, source_used="derived",
                    source_col="(clé d'indexation)", source_row=None, priority=99,
                    transform="from_plate_key",
                    rule_id=build_rule_id("contract", "plate", "derived", 99),
                    conflicts_ignored=[],
                    notes="Plaque reprise de la clé d'indexation (aucune règle YAML ne l'a remplie).",
                ))
    # `number` is no longer derived from the key — it's a normal field
    # populated by YAML rules reading explicit columns. Left blank if no
    # lessor source provides it (Augustin's rule: pas d'info → vide).

    # Cross-check: plates in lessor files but absent from client_file → anomaly
    for slug, df in indexed.items():
        if slug in ("client_file", "api_plaque") or df is None or df.empty:
            continue
        for k in df.index:
            if not isinstance(k, str) or not k:
                continue
            if has_client and k not in client_df.index:
                # Look up the contract number from the lessor row (if any)
                # to enrich the anomaly line.
                num_val: Optional[str] = None
                for col in ("number", "N° Contrat", "Contrat", "Numéro contrat"):
                    if col in df.columns:
                        v = df.at[k, col]
                        if not _is_null(v):
                            num_val = str(v).strip()
                            break
                issues.append(Issue(
                    plate=k, number=num_val, field="plate", source=slug,
                    warning=f"Plaque présente dans {slug} mais absente du fichier client."
                ))

    # Post-passes
    vp_from_ep = _extract_vp_from_ep_sources(indexed)
    vp_from_api = _extract_vp_from_api_plaques(indexed)
    _postpass_isHT(
        out_df, source_by_cell, lineage,
        vehicle_vp_by_plate, vp_from_ep, vp_from_api,
    )
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
        # Backfill plate from the key (which IS the plate post-4.2.6).
        if "plate" in orphan_df.columns:
            for key in orphan_keys:
                if _is_null(orphan_df.at[key, "plate"]) and key:
                    orphan_df.at[key, "plate"] = key
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
        # Sample a few keys for the UI preview. Post-4.2.6 the key IS the
        # plate, so the "number" slot is always empty.
        sample_pairs: list[tuple[str, str]] = [(k, "") for k in affected_keys[:3]]
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

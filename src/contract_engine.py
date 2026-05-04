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
                # Jalon 5.3.24 — markers pour cascade *Enabled* et toggle UI.
                "rule_enabled_cascade", "rule_force_civil_liability",
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


# Order in which we try to attribute a partner to a row when several
# lessor sources contain the same plate. Highest-priority slug wins.
# This list is intentionally NOT exhaustive — only the slugs that uniquely
# identify a lessor (Arval / Ayvens). ``autre_loueur_*`` slugs are skipped
# because we don't know which actual partner they represent.
_PARTNER_ATTRIBUTION_ORDER = (
    # Arval — État de parc / facture data is the strongest signal.
    "arval_uat", "arval_facture_pdf",
    "arval_aen", "arval_tvu", "arval_and", "arval_pneus",
    # Ayvens
    "ayvens_etat_parc", "ayvens_facture_pdf",
    "ayvens_aen", "ayvens_tvs", "ayvens_and", "ayvens_pneus",
)


# Price fields that the invoice parsers (PDF + XLSX) expose with both
# ``<field>_ht`` and ``<field>_ttc`` flavours alongside the engine-facing
# ``<field>`` column. Used by :func:`_postpass_apply_ht_ttc_flavour` to
# re-pick the right flavour PER CONTRACT once isHT has been resolved.
_INVOICE_PRICE_FIELDS = (
    "civilLiabilityPrice", "allRisksPrice", "theftFireAndGlassPrice",
    "financialLossPrice", "legalProtectionPrice", "maintenancePrice",
    "replacementVehiclePrice", "tiresPrice", "gasCardPrice", "tollCardPrice",
    "totalPrice",
)


def _read_vs_ht_strategy() -> str:
    """Return ``'standard'`` or ``'api_plaques'`` from session state.

    Falls back to ``'standard'`` whenever streamlit isn't loaded (tests,
    scripts) or the key is missing — the default behaviour stays the
    historical one in those cases.
    """
    try:
        import streamlit as st
        return st.session_state.get("vs_ht_strategy", "standard")
    except Exception:
        return "standard"


def _postpass_apply_ht_ttc_flavour(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    indexed_sources: dict[str, pd.DataFrame],
    lineage: LineageStore,
) -> None:
    """Adjust each invoice-derived price to match the contract's isHT flag.

    Jalon 5.3.6 — bug fix. The PDF parser and the « Etat des dépenses »
    XLSX parser both pre-fill the engine-facing ``<field>`` column with
    a SINGLE flavour for the whole DataFrame (TTC by default,
    cf. ``assume_ttc``). Both parsers also expose the matching
    ``<field>_ht`` and ``<field>_ttc`` columns. But ``isHT`` is decided
    PER CONTRACT (resolved earlier by ``_postpass_isHT``) — some rows
    are HT, some are TTC, so the global pre-fill was wrong half the time.

    This post-pass walks every row, looks up the source slug that
    posted each price, and overwrites ``<field>`` with the value from
    the matching flavoured column (``<field>_ht`` if ``isHT=True``,
    ``<field>_ttc`` otherwise). Lineage is updated with an « adjusted »
    record so the audit trail is preserved.
    """
    if "isHT" not in out_df.columns:
        return
    facture_slugs = _PARSER_DF_SOURCES
    # Jalon 5.3.18 — mode 2 only fires for VS rows (= isHT=True since
    # service ⇒ HT). We pre-compute the SIV verdict per plate so we can
    # cheaply pick the right flavour.
    vs_strategy = _read_vs_ht_strategy()
    siv_vp_by_key = (
        _extract_vp_from_api_plaques(indexed_sources)
        if vs_strategy == "api_plaques"
        else {}
    )

    for key in out_df.index:
        is_ht_val = out_df.at[key, "isHT"]
        if _is_null(is_ht_val):
            continue
        # Coerce string "TRUE"/"FALSE" → bool so we never accidentally
        # treat "False" as truthy (Python pitfall : ``bool("False")`` is True).
        if isinstance(is_ht_val, str):
            is_ht = is_ht_val.strip().upper() == "TRUE"
        else:
            is_ht = bool(is_ht_val)
        suffix = "_ht" if is_ht else "_ttc"

        # Mode 2 override (Jalon 5.3.18) — only for VS rows (isHT=True).
        # If api_plaques classifies the plate as VP, the lessor's facture
        # was probably written in TTC for this plate. We read the _ttc
        # flavour, then convert back to HT below to align with isHT=True.
        # See ``_postpass_normalize_client_total_flavour`` for the same
        # logic on client_file totalPrice.
        siv_says_vp = False
        if (vs_strategy == "api_plaques" and is_ht and key in siv_vp_by_key
                and siv_vp_by_key[key][0] is True):
            siv_says_vp = True
            suffix = "_ttc"

        for field in _INVOICE_PRICE_FIELDS:
            if field not in out_df.columns:
                continue
            src = source_by_cell.get((key, field))
            if src not in facture_slugs:
                continue
            src_df = indexed_sources.get(src)
            if src_df is None or src_df.empty or key not in src_df.index:
                continue
            flavoured_col = field + suffix
            if flavoured_col not in src_df.columns:
                continue
            new_val = src_df.at[key, flavoured_col]
            if _is_null(new_val):
                continue
            # Mode 2 (Jalon 5.3.18) — VS dont SIV dit « VP » : on a lu
            # `_ttc`, mais le contrat reste isHT=True donc la cellule
            # finale doit être en HT. On divise par 1.20 pour la ramener
            # à la flavour cible. Sans cette étape, la valeur stockée
            # serait en TTC alors qu'isHT dit HT — incohérent.
            note_extra = ""
            if siv_says_vp:
                try:
                    new_val = round(float(new_val) / 1.20, 2)
                    note_extra = (
                        " [Mode 2 — SIV classe VP : valeur lue en TTC, "
                        "convertie en HT (÷1.20) pour aligner avec isHT=True]"
                    )
                except (ValueError, TypeError):
                    pass
            old_val = out_df.at[key, field]
            # Use _values_differ to compare with float tolerance — avoids
            # spurious lineage entries for round-tripped TTC=HT*1.0 cases.
            if not _values_differ(old_val, new_val):
                continue
            out_df.at[key, field] = new_val
            lineage.record(LineageRecord(
                table="contract", key=key, field=field,
                value=new_val, source_used=src,
                source_col=flavoured_col, source_row=None,
                priority=2,
                transform=f"flavour_{suffix.strip('_')}",
                rule_id=build_rule_id("contract", field, src, 2),
                conflicts_ignored=[],
                notes=(
                    f"Ajusté à la flavour {suffix.strip('_').upper()} après "
                    f"résolution isHT={is_ht}. Avant: {old_val!r}. "
                    f"Source: {flavoured_col}." + note_extra
                ),
            ))


# Mapping <Enabled field> → <Price field> for the « TRUE si Price > 0 »
# derivation rule. Keep aligned with contract.yml — every entry here must
# have a matching ``rule_price_positive`` rule in the YAML.
#
# Jalon 5.3.24 — Ajout de ``tiresEnabled`` et ``replacementVehicleEnabled``.
# Logique : « si on a trouvé un prix > 0 pour ces services, alors le
# service est activé ». Compatible avec les autres origines (ex. présence
# dans le fichier Pneus pour ``tiresEnabled``, ``tiresAmount > 0`` pour
# le compteur), qui restent prioritaires : ce post-pass ne remplit
# que les cellules encore NaN après les autres règles.
_PRICE_TO_ENABLED_DERIVATION = {
    "civilLiabilityEnabled":      "civilLiabilityPrice",
    "legalProtectionEnabled":     "legalProtectionPrice",
    "theftFireAndGlassEnabled":   "theftFireAndGlassPrice",
    "allRisksEnabled":            "allRisksPrice",
    "financialLossEnabled":       "financialLossPrice",
    # maintenanceEnabled has rule_price_positive as P3 fallback after the
    # loueur EP sources (P1/P2). Including it here mirrors the YAML.
    "maintenanceEnabled":         "maintenancePrice",
    # Jalon 5.3.24 — services dérivés du prix.
    "tiresEnabled":               "tiresPrice",
    "replacementVehicleEnabled":  "replacementVehiclePrice",
}


def _postpass_compute_enabled_from_price(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    lineage: LineageStore,
) -> None:
    """Derive ``*Enabled`` booleans from the matching ``*Price`` field.

    Jalon 5.3.7 — bug fix. The YAML declares 5 insurance ``*Enabled`` fields
    (civilLiability / legalProtection / theftFireAndGlass / allRisks /
    financialLoss) plus ``maintenanceEnabled`` (P3 fallback) with the rule
    « TRUE si <field>Price > 0 ». The transform ``rule_price_positive`` is
    listed as a marker passthrough in :func:`_apply_rule_transform` but no
    post-pass ever computed the actual value. Result: even when the price
    was correctly extracted from a facture, the matching Enabled stayed
    empty — making the contract un-importable into Revio.

    This post-pass walks each *Enabled in :data:`_PRICE_TO_ENABLED_DERIVATION`,
    looks at the matching *Price column, and posts:
      - ``True``   if Price > 0
      - ``False``  if Price == 0
      - leaves NaN if Price is NaN (we don't want to commit to either)

    Lineage is updated with a ``rule_price_positive`` record citing the
    source price and value, so the audit trail explains *why* each Enabled
    has its value.

    Must run AFTER :func:`_postpass_apply_ht_ttc_flavour` so the price has
    its final flavoured value when we test it.
    """
    # First pass : normalise existing *Enabled values posted by
    # ``rule_field_present`` (which is also a marker passthrough and leaves
    # raw strings like « ENTRETIEN COURANT » in the cell — Revio expects
    # TRUE / FALSE). We convert :
    #   - empty string / None / NaN              → leave NaN
    #   - "aucune prestation" (case-insensitive) → False
    #   - any other non-empty string             → True
    #   - bool values                            → kept as-is
    _AUCUNE_RE = re.compile(r"aucune?\s+prestation", re.IGNORECASE)
    for enabled_field in _PRICE_TO_ENABLED_DERIVATION.keys():
        if enabled_field not in out_df.columns:
            continue
        for key in out_df.index:
            current = out_df.at[key, enabled_field]
            if isinstance(current, bool):
                continue
            if _is_null(current):
                continue
            s = str(current).strip()
            if not s or s.lower() in {"nan", "none"}:
                out_df.at[key, enabled_field] = None
                continue
            new_bool = not bool(_AUCUNE_RE.search(s))
            if new_bool != current:
                out_df.at[key, enabled_field] = new_bool
                lineage.record(LineageRecord(
                    table="contract", key=key, field=enabled_field,
                    value=new_bool, source_used=source_by_cell.get((key, enabled_field), "rule_engine"),
                    source_col=None, source_row=None,
                    priority=1, transform="rule_field_present_normalised",
                    rule_id=build_rule_id("contract", enabled_field,
                                          source_by_cell.get((key, enabled_field), "rule_engine"), 1),
                    conflicts_ignored=[],
                    notes=(
                        f"Marker rule_field_present normalisé: « {s} » → "
                        f"{'TRUE' if new_bool else 'FALSE'} "
                        f"(FALSE si label = « aucune prestation »)."
                    ),
                ))

    for enabled_field, price_field in _PRICE_TO_ENABLED_DERIVATION.items():
        if enabled_field not in out_df.columns:
            continue
        if price_field not in out_df.columns:
            continue
        for key in out_df.index:
            current = out_df.at[key, enabled_field]
            # Don't overwrite a value already posted by a higher-priority
            # rule (e.g. ayvens_etat_parc « Maintenance souscrite »).
            if not _is_null(current):
                continue
            price = out_df.at[key, price_field]
            if _is_null(price):
                continue
            try:
                price_val = float(price)
            except (ValueError, TypeError):
                continue
            new_val = price_val > 0
            out_df.at[key, enabled_field] = new_val
            source_by_cell[(key, enabled_field)] = "rule_engine"
            lineage.record(LineageRecord(
                table="contract", key=key, field=enabled_field,
                value=new_val, source_used="rule_engine",
                source_col=f"{price_field} > 0", source_row=None,
                priority=99, transform="rule_price_positive",
                rule_id=build_rule_id("contract", enabled_field, "rule_engine", 99),
                conflicts_ignored=[],
                notes=(
                    f"Calculé depuis {price_field}={price_val:.2f} → "
                    f"{'TRUE' if new_val else 'FALSE'}."
                ),
            ))


# Mapping <Enabled field> → list of source slugs that, if present for the
# row's plate, mean the service is enabled. Mirrors the YAML
# ``rule_source_present`` rules (where the rule is « TRUE si plaque
# présente dans <source slug> »).
_PRESENCE_TO_ENABLED_DERIVATION: dict[str, tuple[str, ...]] = {
    "tiresEnabled": ("arval_pneus", "ayvens_pneus", "autre_loueur_pneus"),
}


def _postpass_compute_enabled_from_presence(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    indexed_sources: dict[str, pd.DataFrame],
    lineage: LineageStore,
) -> None:
    """Derive ``*Enabled`` booleans from the presence of the plate in a
    dedicated source (Pneus, etc.).

    Jalon 5.3.7 — same class of bug as ``rule_price_positive`` : the
    transform ``rule_source_present`` is declared in the YAML for
    ``tiresEnabled`` but listed as a marker passthrough in the engine,
    so nothing actually computed it. Fix : if the plate is present in
    any of the dedicated sources for this Enabled field → True ;
    otherwise leave NaN (we don't post False because the absence might
    just mean « no Pneus file uploaded for this lessor »).

    Jalon 5.3.15 — Refinement for ``tiresEnabled`` : when the matching
    ``tiresAmount`` is **filled and equals 0**, we post FALSE instead
    of TRUE. The plate appears in the Pneus file but with zero tires
    subscribed = no tire service.
    """
    for enabled_field, slugs in _PRESENCE_TO_ENABLED_DERIVATION.items():
        if enabled_field not in out_df.columns:
            continue
        # Find the matching « amount » field for this enabled (5.3.15) :
        # currently only tiresEnabled has tiresAmount as its quantitative
        # counterpart, but the structure is generic for future extensions.
        amount_field = "tiresAmount" if enabled_field == "tiresEnabled" else None

        for key in out_df.index:
            current = out_df.at[key, enabled_field]
            if not _is_null(current):
                continue

            # 5.3.15 — tiresAmount=0 ⇒ FALSE (overrides the "presence
            # implies TRUE" rule below).
            if amount_field and amount_field in out_df.columns:
                amount_raw = out_df.at[key, amount_field]
                if not _is_null(amount_raw):
                    try:
                        amount_int = int(float(amount_raw))
                    except (ValueError, TypeError):
                        amount_int = None
                    if amount_int is not None:
                        new_val = amount_int > 0
                        out_df.at[key, enabled_field] = new_val
                        source_by_cell[(key, enabled_field)] = "rule_engine"
                        lineage.record(LineageRecord(
                            table="contract", key=key, field=enabled_field,
                            value=new_val, source_used="rule_engine",
                            source_col=f"{amount_field}={amount_int}",
                            source_row=None, priority=99,
                            transform="rule_amount_positive",
                            rule_id=build_rule_id("contract", enabled_field, "rule_engine", 99),
                            conflicts_ignored=[],
                            notes=(
                                f"Calculé depuis {amount_field}={amount_int} → "
                                f"{'TRUE' if new_val else 'FALSE'}."
                            ),
                        ))
                        continue

            # Default rule : presence in any of the dedicated sources ⇒ TRUE.
            chosen_slug = None
            for slug in slugs:
                src_df = indexed_sources.get(slug)
                if src_df is None or src_df.empty:
                    continue
                if key in src_df.index:
                    chosen_slug = slug
                    break
            if not chosen_slug:
                continue
            out_df.at[key, enabled_field] = True
            source_by_cell[(key, enabled_field)] = chosen_slug
            lineage.record(LineageRecord(
                table="contract", key=key, field=enabled_field,
                value=True, source_used=chosen_slug,
                source_col="(plaque présente)", source_row=None,
                priority=1, transform="rule_source_present",
                rule_id=build_rule_id("contract", enabled_field, chosen_slug, 1),
                conflicts_ignored=[],
                notes=f"Plaque trouvée dans {chosen_slug} → TRUE.",
            ))


# ── Jalon 5.3.24 ─────────────────────────────────────────────────────
# Cascades entre *Enabled* + toggle « assurance loueur globale ».
# Documentation utilisateur : voir l'expander « 🛡️ Assurance loueur
# globale » dans la page Moteur.
# ──────────────────────────────────────────────────────────────────────

# Mapping <Enabled> → liste de <Enabled> qui, si TRUE, impliquent la
# souscription du premier. Concrètement : si tu as souscrit Tous Risques,
# Vol-Incendie-Bris, Perte Financière, Protection Juridique ou
# Maintenance, alors la Responsabilité Civile (civilLiabilityEnabled)
# est forcément TRUE — c'est l'assurance de base qui sous-tend toutes
# les autres couvertures. Si on ajoute d'autres cascades plus tard,
# elles passeront par cette table.
_ENABLED_IMPLIES_CASCADE: dict[str, tuple[str, ...]] = {
    "civilLiabilityEnabled": (
        "allRisksEnabled",
        "theftFireAndGlassEnabled",
        "financialLossEnabled",
        "legalProtectionEnabled",
    ),
}


def _read_force_civil_liability_enabled() -> bool:
    """Retourne ``True`` si le toggle « Assurance loueur globale »
    a été coché dans la page Moteur (session_state).

    Falls back to ``False`` quand streamlit n'est pas chargé (tests,
    scripts) ou que la clé est manquante : par défaut on ne force rien
    et le comportement reste celui des autres post-passes.
    """
    try:
        import streamlit as st
        return bool(st.session_state.get("force_civil_liability_enabled", False))
    except Exception:
        return False


def _postpass_imply_enabled_cascade(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    lineage: LineageStore,
) -> None:
    """Propage les implications logiques entre *Enabled* booléens.

    Jalon 5.3.24 — règle métier : si on a détecté qu'un contrat possède
    une couverture *avancée* (Tous Risques, Vol-Incendie-Bris, Perte
    Financière, Protection Juridique), alors la Responsabilité Civile
    est nécessairement souscrite — c'est la couche de base de toutes
    les assurances auto. Avant ce post-pass, ``civilLiabilityEnabled``
    pouvait rester vide alors qu'``allRisksEnabled = TRUE`` parce que
    le prix RC était noyé dans le prix Tous Risques (une seule ligne
    facture pour les deux).

    On ne met jamais TRUE par-dessus une valeur déjà posée — si une
    règle prioritaire a posé FALSE explicitement on respecte ça.
    """
    for target, implications in _ENABLED_IMPLIES_CASCADE.items():
        if target not in out_df.columns:
            continue
        for key in out_df.index:
            current = out_df.at[key, target]
            if not _is_null(current):
                continue
            triggered_by: list[str] = []
            for src_field in implications:
                if src_field not in out_df.columns:
                    continue
                val = out_df.at[key, src_field]
                if val is True:
                    triggered_by.append(src_field)
            if not triggered_by:
                continue
            out_df.at[key, target] = True
            source_by_cell[(key, target)] = "rule_engine"
            triggers_str = ", ".join(triggered_by)
            lineage.record(LineageRecord(
                table="contract", key=key, field=target,
                value=True, source_used="rule_engine",
                source_col=f"cascade ← {triggers_str}", source_row=None,
                priority=99, transform="rule_enabled_cascade",
                rule_id=build_rule_id("contract", target, "rule_engine", 99),
                conflicts_ignored=[],
                notes=(
                    f"Cascade implicite : {triggers_str} = TRUE → "
                    f"{target} = TRUE (qui dit assurance avancée dit "
                    f"Responsabilité Civile)."
                ),
            ))


def _postpass_force_civil_liability(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    lineage: LineageStore,
) -> None:
    """Force ``civilLiabilityEnabled = TRUE`` sur tous les contrats si
    le toggle « Assurance loueur globale » est activé dans l'UI.

    Jalon 5.3.24 — feature client-driven. Le client sait qu'il a
    souscrit une assurance loueur sur toute la flotte mais on n'a pas
    le détail ligne à ligne dans les fichiers reçus. Plutôt que de
    laisser tous les contrats sortir avec ``civilLiabilityEnabled``
    vide, on coche le toggle et on force TRUE partout.

    Override comportemental : on **écrase** même les valeurs FALSE
    posées par d'autres règles, parce que le toggle est un signal
    explicite de l'utilisateur (« je sais mieux que tes fichiers »).
    Si l'utilisateur veut retomber sur l'inférence automatique, il
    décoche le toggle.
    """
    if not _read_force_civil_liability_enabled():
        return
    target = "civilLiabilityEnabled"
    if target not in out_df.columns:
        return
    for key in out_df.index:
        current = out_df.at[key, target]
        if current is True:
            continue
        out_df.at[key, target] = True
        source_by_cell[(key, target)] = "rule_engine"
        lineage.record(LineageRecord(
            table="contract", key=key, field=target,
            value=True, source_used="rule_engine",
            source_col="toggle « Assurance loueur globale »",
            source_row=None,
            priority=99, transform="rule_force_civil_liability",
            rule_id=build_rule_id("contract", target, "rule_engine", 99),
            conflicts_ignored=[],
            notes=(
                "Forcé à TRUE par l'option « Assurance loueur globale » "
                "(page Moteur). Avant: "
                f"{current!r}."
            ),
        ))


# Jalon 5.3.15 — HT/TTC detection for client_file totalPrice.
# We detect the flavour from (column name, raw cell value) using these
# regexes. Word-boundary tricks avoid false positives like ``HTML``
# (where ``HT`` is part of a longer alpha run) or ``HTTPS``.
_FLAVOUR_HT_RE = _re_for_flavour = __import__("re").compile(
    r"(?<![A-Z])HT(?![A-Z])|(?<![A-Z])H\.T\.?(?![A-Z])|HORS\s+TAXE",
    __import__("re").IGNORECASE,
)
_FLAVOUR_TTC_RE = __import__("re").compile(
    r"(?<![A-Z])TTC(?![A-Z])|(?<![A-Z])T\.T\.C\.?(?![A-Z])|TOUTES?\s+TAXES?\s+COMPRISES?",
    __import__("re").IGNORECASE,
)


def _detect_price_flavour(
    col_name: Optional[str],
    raw_value: Any,
    lessor_row: Optional[dict] = None,
) -> Optional[str]:
    """Return ``'ht'`` / ``'ttc'`` / None based on column name, raw value
    and optional lessor row context.

    Detection sources, in order :

    1. ``col_name`` — column label (« Loyer mensuel HT »).
    2. ``raw_value`` — cell content (« 59 € HT », « 60 ttc »).
    3. ``lessor_row`` — when provided, scans common Genre/Carrosserie
       fields for VU / VS / VASP / CTTE / DERIV (→ HT, fiscal
       utilitaire) or VP (→ TTC). Used by the EP loueur post-pass so a
       row labelled « Berline VU 5 po » in the Carrosserie column tells
       us the loueur facture en HT for that plate even when the price
       column is mute.

    When nothing is detected returns None and the caller falls back to
    the contract-level convention (VP=TTC / VS/VU=HT).
    """
    text = f"{col_name or ''} {raw_value if raw_value is not None else ''}"
    if _FLAVOUR_TTC_RE.search(text):
        return "ttc"
    if _FLAVOUR_HT_RE.search(text):
        return "ht"

    # 3rd pass — lessor row context (Jalon 5.3.18). We look at the
    # common Genre / Carrosserie fields and match the same vocabulary
    # used by ``_extract_vp_from_ep_sources``.
    if lessor_row:
        # Probe common header names. We ASCII-fold + lower so accented
        # variants and casing don't matter.
        import unicodedata
        candidate_keys = ("Genre", "Carrosserie", "genre", "carrosserie",
                          "Genre du véhicule", "Type véhicule", "Catégorie")
        for k in candidate_keys:
            if k not in lessor_row:
                continue
            v = lessor_row.get(k)
            if v is None or _is_null(v):
                continue
            s = unicodedata.normalize("NFKD", str(v))
            s = "".join(c for c in s if not unicodedata.combining(c)).lower()
            # VU markers — fiscal utilitaire = HT.
            for marker in ("vu", "vs", "vasp", "ctte", "deriv", "utilit",
                           "commercial", "camion", "fourgon"):
                if marker in s:
                    return "ht"
            # VP markers — fiscal voiture particulière = TTC.
            for marker in ("vp", "particul", "tourisme"):
                if marker in s:
                    return "ttc"
    return None


def _postpass_normalize_client_total_flavour(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    indexed_sources: dict[str, pd.DataFrame],
    manual_column_overrides: dict,
    lineage: LineageStore,
    tva_rate: float = 0.20,
) -> None:
    """Detect the HT/TTC flavour of ``totalPrice`` rows posted by the
    client_file, convert if it doesn't match the contract's isHT.

    Jalon 5.3.15 — clients fill ``Loyer mensuel`` in their EP without a
    standardised convention : sometimes HT, sometimes TTC, independent
    of the VP/VU classification. This post-pass :

    1. Scans ``totalPrice`` cells whose source is ``client_file``.
    2. Detects the source flavour via :func:`_detect_price_flavour`
       on (column name, raw cell value).
    3. If undetected, falls back to « VP=TTC / VU=HT » based on isHT.
    4. If the source flavour ≠ contract isHT, multiplies / divides by
       ``(1 + tva_rate)`` to convert.

    Note that the contract's ``isHT`` and the vehicle's ``usage`` are
    NOT modified by this post-pass — only the totalPrice value.
    """
    if "totalPrice" not in out_df.columns or "isHT" not in out_df.columns:
        return
    client_df = indexed_sources.get("client_file")
    if client_df is None or client_df.empty:
        return

    # Find the column that the engine read for totalPrice from
    # client_file. Try the manual override first ; if absent, infer
    # via the lineage records (source_col).
    source_col_override = manual_column_overrides.get(("client_file", "totalPrice"))

    # Jalon 5.3.18 — mode 2 (api_plaques fait foi) only triggers for VS
    # rows. Pre-compute the SIV verdict so the fallback branch can use it.
    vs_strategy = _read_vs_ht_strategy()
    siv_vp_by_key = (
        _extract_vp_from_api_plaques(indexed_sources)
        if vs_strategy == "api_plaques"
        else {}
    )

    factor = 1.0 + tva_rate
    for key in out_df.index:
        if source_by_cell.get((key, "totalPrice")) != "client_file":
            continue
        is_ht_val = out_df.at[key, "isHT"]
        if _is_null(is_ht_val):
            continue
        if isinstance(is_ht_val, str):
            target_is_ht = is_ht_val.strip().upper() == "TRUE"
        else:
            target_is_ht = bool(is_ht_val)

        if key not in client_df.index:
            continue

        # Pick the column we read for this cell. Prefer the manual
        # override ; otherwise scan the lineage records for the cell.
        col = source_col_override
        if not col:
            for rec in lineage._records:
                if (rec.key == key and rec.field == "totalPrice"
                        and rec.source_used == "client_file" and rec.source_col):
                    col = rec.source_col
                    break
        if not col or col not in client_df.columns:
            continue

        raw_value = client_df.at[key, col]

        # Pass the full client row as ``lessor_row`` so detection can pick
        # up Genre/Carrosserie hints (Jalon 5.3.18). For client_file the
        # row is unlikely to carry that info, but the API stays generic
        # in case the client adds an « Usage VP/VS » column.
        client_row = client_df.loc[key].to_dict() if key in client_df.index else None
        source_flavour = _detect_price_flavour(col, raw_value, lessor_row=client_row)

        if source_flavour is None:
            # Fallback. Mode 1 (standard) = contract-level convention
            # (VP=TTC / VU/VS=HT). Mode 2 (api_plaques) for VS rows only :
            # if SIV says VP, the lessor likely wrote TTC despite our VS
            # label → assume TTC source so we convert below.
            if (vs_strategy == "api_plaques" and target_is_ht
                    and key in siv_vp_by_key and siv_vp_by_key[key][0] is True):
                source_flavour = "ttc"
            else:
                source_flavour = "ht" if target_is_ht else "ttc"

        # Already in the right flavour ? Nothing to convert.
        if (source_flavour == "ht" and target_is_ht) or (
            source_flavour == "ttc" and not target_is_ht
        ):
            continue

        current = out_df.at[key, "totalPrice"]
        if _is_null(current):
            continue
        try:
            current_f = float(current)
        except (ValueError, TypeError):
            continue

        # source HT, target TTC → multiply ; source TTC, target HT → divide.
        if source_flavour == "ht" and not target_is_ht:
            new_val = round(current_f * factor, 2)
            direction = "HT→TTC"
        else:  # source TTC, target HT
            new_val = round(current_f / factor, 2)
            direction = "TTC→HT"

        out_df.at[key, "totalPrice"] = new_val
        lineage.record(LineageRecord(
            table="contract", key=key, field="totalPrice",
            value=new_val, source_used="client_file",
            source_col=col, source_row=None,
            priority=99, transform="ht_ttc_convert_client",
            rule_id=build_rule_id("contract", "totalPrice", "client_file", 99),
            conflicts_ignored=[],
            notes=(
                f"Conversion client_file {direction} (TVA {tva_rate*100:.0f}%) — "
                f"colonne « {col} », valeur brute {raw_value!r}, "
                f"avant : {current_f:.2f}, après : {new_val:.2f}."
            ),
        ))


# Column-name candidates to scan in the client_file when looking for an
# explicit lessor declaration (Jalon 5.3.19). Matched case-insensitive,
# accents stripped. The first column that exists in the file wins.
_CLIENT_LESSOR_COLUMN_HINTS = (
    "leaser", "loueur", "bailleur", "lessor",
    "partner", "partenaire", "fournisseur",
)


def _find_client_lessor_column(client_df: pd.DataFrame) -> Optional[str]:
    """Locate the column in ``client_df`` that declares the lessor.

    Returns the original column name if a match is found, otherwise
    None. Matching is accent-insensitive and case-insensitive, so
    ``Leaser`` / ``Bailleur`` / ``loueur`` / ``LESSOR`` all resolve.
    """
    if client_df is None or client_df.empty:
        return None
    import unicodedata
    def _norm(s: Any) -> str:
        if s is None:
            return ""
        s = unicodedata.normalize("NFKD", str(s))
        return "".join(c for c in s if not unicodedata.combining(c)).strip().lower()
    norm_to_orig = {_norm(c): c for c in client_df.columns if c is not None}
    for hint in _CLIENT_LESSOR_COLUMN_HINTS:
        if hint in norm_to_orig:
            return norm_to_orig[hint]
    return None


def _postpass_resolve_partner_id(
    out_df: pd.DataFrame,
    source_by_cell: dict,
    indexed_sources: dict[str, pd.DataFrame],
    lineage: LineageStore,
) -> None:
    """Fill ``partnerId`` from the lessor — multi-source resolution.

    Resolution order (Jalon 5.3.19) :

    **Priority 1 — Client_file declarative column.** When the client's
    file has a ``Leaser`` / ``Loueur`` / ``Bailleur`` / ``Lessor`` /
    ``Partner`` / ``Partenaire`` / ``Fournisseur`` column, we read the
    name for that plate (e.g. « AYVENS », « VW BANK », « FREE2MOVE »)
    and resolve it via :func:`partners.resolve_partner_id` — which
    knows about aliases (FREE2MOVE → Leasys, ALD → Ayvens, etc.). This
    is **declarative client truth** : the customer himself tells us
    who the lessor is, per vehicle.

    **Priority 2 — Engine source slug** (Jalon 5.2.2 fallback). If no
    declarative column is found, we walk
    :data:`_PARTNER_ATTRIBUTION_ORDER` and pick the first slug that
    contains the plate. The matching partnerId UUID comes from
    :func:`partners.resolve_partner_id_for_slug`. Limited to known
    Arval / Ayvens slugs ; ``autre_loueur_*`` are deliberately skipped
    so we never silently attribute the wrong UUID.
    """
    if "partnerId" not in out_df.columns:
        return
    # Lazy import so partners.py stays decoupled from the engine.
    from .partners import resolve_partner_id, resolve_partner_id_for_slug

    # ── Priority 1 setup : find the client_file declarative column ──
    client_df = indexed_sources.get("client_file")
    lessor_col = _find_client_lessor_column(client_df) if client_df is not None else None

    for key in out_df.index:
        if not _is_null(out_df.at[key, "partnerId"]):
            continue

        # ── Priority 1 — client_file Leaser/Loueur column ──
        if lessor_col and client_df is not None and key in client_df.index:
            raw_lessor = client_df.at[key, lessor_col]
            if not _is_null(raw_lessor):
                resolved = resolve_partner_id(str(raw_lessor))
                if resolved:
                    out_df.at[key, "partnerId"] = resolved
                    source_by_cell[(key, "partnerId")] = "client_file"
                    lineage.record(LineageRecord(
                        table="contract", key=key, field="partnerId",
                        value=resolved, source_used="client_file",
                        source_col=lessor_col, source_row=None, priority=1,
                        transform="lookup_by_lessor_name",
                        rule_id=build_rule_id("contract", "partnerId", "client_file", 1),
                        conflicts_ignored=[],
                        notes=(
                            f"Résolu depuis client_file colonne « {lessor_col} » = "
                            f"{str(raw_lessor)!r} via partners.resolve_partner_id."
                        ),
                    ))
                    continue
                # else: lessor name not found in partner_index — fall through
                # to the slug-based attribution below.

        # ── Priority 2 — invoice DataFrame ``lessor`` column (Jalon 5.3.20) ──
        # The invoice_xlsx_parser stamps every row with a ``lessor`` value
        # detected from the filename (« alphabet » / « athlon » / « agilauto »
        # / etc.). We resolve the UUID via partners.resolve_partner_id so
        # that lessors not directly mapped via SLUG_TO_PARTNER still get
        # their partnerId — provided the AM named their file with the
        # lessor's name (or its alias).
        invoice_lessor_resolved = False
        for slug in _PARSER_DF_SOURCES:
            df_src = indexed_sources.get(slug)
            if df_src is None or df_src.empty or "lessor" not in df_src.columns:
                continue
            if key not in df_src.index:
                continue
            lessor_name = df_src.at[key, "lessor"]
            if _is_null(lessor_name) or str(lessor_name).strip().lower() == "autre":
                continue
            uuid = resolve_partner_id(str(lessor_name))
            if not uuid:
                continue
            out_df.at[key, "partnerId"] = uuid
            source_by_cell[(key, "partnerId")] = slug
            lineage.record(LineageRecord(
                table="contract", key=key, field="partnerId",
                value=uuid, source_used=slug,
                source_col="lessor", source_row=None, priority=2,
                transform="lookup_by_invoice_lessor_name",
                rule_id=build_rule_id("contract", "partnerId", slug, 2),
                conflicts_ignored=[],
                notes=(
                    f"Résolu depuis colonne lessor du parser facture "
                    f"({slug}) = {str(lessor_name)!r} → {uuid} via "
                    f"partners.resolve_partner_id."
                ),
            ))
            invoice_lessor_resolved = True
            break
        if invoice_lessor_resolved:
            continue

        # ── Priority 3 — slug-based attribution (Jalon 5.2.2 fallback) ──
        chosen_slug = None
        for slug in _PARTNER_ATTRIBUTION_ORDER:
            df_src = indexed_sources.get(slug)
            if df_src is None or df_src.empty:
                continue
            if key in df_src.index:
                chosen_slug = slug
                break
        if chosen_slug is None:
            continue
        partner_id = resolve_partner_id_for_slug(chosen_slug)
        if not partner_id:
            continue
        out_df.at[key, "partnerId"] = partner_id
        source_by_cell[(key, "partnerId")] = chosen_slug
        lineage.record(LineageRecord(
            table="contract", key=key, field="partnerId",
            value=partner_id, source_used=chosen_slug,
            source_col=None, source_row=None, priority=3,
            transform="lookup_by_source_slug",
            rule_id=build_rule_id("contract", "partnerId", chosen_slug, 3),
            conflicts_ignored=[],
            notes=f"Résolu depuis le slug source ({chosen_slug}) via partners.SLUG_TO_PARTNER.",
        ))


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

    Priority per key (Jalon 5.3.2 — reorder, démote api_plaques) :
      1. VP from the EP file where totalPrice came from (if known)
      2. VP from vehicle_vp_by_plate (Vehicle engine result : reflète la
         hiérarchie YAML ``usage`` — loueurs > client_file > api_plaques
         depuis le 5.3.1)
      3. VP from api_plaques source (read directly by the Contract engine,
         dernier recours, biais constaté sur classification VP/VU)

    Avant 5.3.2 l'ordre était EP → api_plaques → Vehicle result. Le swap
    apporte : si le moteur Vehicle a tourné, sa décision (qui inclut le
    client_file en P2) prime sur la lecture api_plaques directe. Aligne
    isHT avec la nouvelle hiérarchie usage en attendant changement de
    provider SIV.
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
        # 1. EP loueur — col VP/VU du fichier où on a trouvé totalPrice.
        if key in vp_from_ep_by_key:
            is_vp, chosen_source = vp_from_ep_by_key[key]
        # 2. Vehicle engine result (post-5.3.1: reflète loueurs>client>api).
        if is_vp is None and plate in vehicle_vp_by_plate:
            is_vp = vehicle_vp_by_plate[plate]
            chosen_source = "vehicle_engine"
        # 3. api_plaques en lecture directe (dernier recours).
        if is_vp is None and key in vp_from_api_by_key:
            is_vp, chosen_source = vp_from_api_by_key[key]
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

    (Jalon 5.0.1.2) Avant ce fix, la sélection de colonne passait par
    ``_find_column`` avec des candidates en MixedCase ("genreVCGNGC")
    comparés à des ``col.lower()`` → aucun match sur le nom spécifique.
    Le fallback ``"genre"`` (lowercase) attrapait alors la PREMIÈRE col
    contenant "genre", c'est-à-dire ``genreVCG`` (code NUMÉRIQUE 1/3/…),
    pas ``genreVCGNGC`` (libellé texte "VP"/"VASP"/…). Résultat : 0
    plaque résolue → ``isHT`` vide. Fix : (1) match exact lowercase
    sur un set de noms spécifiques d'abord, (2) fallback sniff des
    valeurs — on prend la col dont ≥20% des cellules contiennent un
    indicateur textuel VP/VU.

    Returns dict: key → (is_vp_bool, 'api_plaque').
    """
    out: dict[str, tuple[bool, str]] = {}
    df = indexed.get("api_plaques")
    if df is None or df.empty:
        return out

    # ---- Step 1 : exact match (lowercase) on a prioritized name list ----
    # Ordered by specificity : genreVCGNGC > genreCGNGC > genreCG > genre.
    name_priority = (
        "genrevcgngc",       # texte libellé ("VP"/"VASP"/"VU"…) ← le bon
        "genrecgngc",
        "genre_vcg_ngc",
        "genrevcg_ngc",
        "genrecg",
        "genre_cg",
        "categorie",
        "catégorie",
        "genre",              # dernier recours — peut être le code numérique
    )
    low_to_col = {str(c).strip().lower(): c for c in df.columns}
    col: Optional[str] = None
    for cand in name_priority:
        if cand in low_to_col:
            col = low_to_col[cand]
            break

    # ---- Step 2 : guard against numeric-only columns (e.g. genreVCG) ----
    # If the picked col is numeric, it's the SIV code not the label → find a
    # text sibling. If none by name, sniff values below.
    def _is_text_vp_column(series: pd.Series) -> bool:
        if series.dtype.kind in ("i", "u", "f"):
            return False
        sample = series.dropna().head(200).astype(str)
        if sample.empty:
            return False
        hits = sample.str.lower().str.contains(
            r"\b(?:vp|vu|vasp|particul|utilit|tourisme|camion|fourgon)\b",
            regex=True, na=False,
        ).sum()
        return (hits / len(sample)) >= 0.20

    if col is not None and not _is_text_vp_column(df[col]):
        col = None  # discard numeric lookalike, fall through to sniff

    # ---- Step 3 : sniff — first column whose values look like VP/VU text ----
    if col is None:
        for c in df.columns:
            try:
                if _is_text_vp_column(df[c]):
                    col = c
                    break
            except Exception:
                continue

    if col is None:
        return out

    # Codes SIV : VP = voiture particulière (→ TTC). Tout le reste (VU,
    # VASP, CAM, CTTE, CL, PTRA, TM, TR, REMORQUE, SEMI…) = non-VP (→ HT).
    # Sur ``api_plaques``, la col texte est un code SIV propre : on
    # whitelist VP et on traite tout le reste comme non-VP (y compris
    # VASP qui était le cas des 22 lignes non résolues chez Augustin).
    VP_HINTS = ("particul", "tourisme")  # substrings textuels
    VP_EXACT = {"vp"}                    # codes courts exacts
    for key, val in df[col].items():
        if key in out or _is_null(val):
            continue
        low = str(val).strip().lower()
        if not low:
            continue
        is_vp = (low in VP_EXACT) or any(h in low for h in VP_HINTS)
        out[key] = (is_vp, "api_plaque")
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
        # Jalon 5.3.8 — Ayvens utilise « Carrosserie » comme fallback,
        # avec valeurs comme « Berline VU 5 po » qui portent le code VU.
        "carrosserie",
    )
    # Substrings — on matche si l'un est présent dans la valeur normalisée.
    VP_VALUE_HINTS = ("vp", "particul", "tourisme")
    # Jalon 5.3.8 — codes terrain qui désignent un VU fiscalement :
    #   VS    = Véhicule de Société (Ayvens)
    #   CTTE  = Camionnette (SIV)
    #   VASP  = Véhicule Automoteur Spécialisé Professionnel
    #   DERIV = Dérivé VP (fiscalement utilitaire dans la majorité des cas)
    # Sans ces codes, FK-354-ZK (Genre='VS') tombait en fallback sur
    # api_plaques (qui dit VP à tort) → isHT mal résolu.
    VU_VALUE_HINTS = (
        "vu", "utilit", "commercial", "pl", "camion", "fourgon",
        "vs", "ctte", "vasp", "deriv",
    )

    # Order of column preference: try the most explicit (Genre, Type véhicule,
    # ...) before falling back to Carrosserie which is more verbose but less
    # consistent. This is just preference order — we still scan all matching
    # columns, the first one to produce a verdict wins per row.
    def _find_vp_cols(df: pd.DataFrame) -> list[str]:
        explicit_hints = [h for h in VP_HEADER_HINTS if h != "carrosserie"]
        explicit_cols: list[str] = []
        carrosserie_cols: list[str] = []
        for c in df.columns:
            low = str(c).strip().lower()
            if any(h in low for h in explicit_hints):
                explicit_cols.append(c)
            elif "carrosserie" in low:
                carrosserie_cols.append(c)
        return explicit_cols + carrosserie_cols

    for slug in EP_SLUGS:
        df = indexed.get(slug)
        if df is None or df.empty:
            continue
        cols = _find_vp_cols(df)
        if not cols:
            continue
        for col in cols:
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
    # Jalon 5.3.6 — adjust invoice-derived prices to match each contract's
    # isHT (HT vs TTC). Must run AFTER _postpass_isHT.
    _postpass_apply_ht_ttc_flavour(out_df, source_by_cell, indexed, lineage)
    # Jalon 5.3.15 — totalPrice from client_file : detect HT/TTC from
    # column header + cell value, convert if mismatch with isHT.
    _postpass_normalize_client_total_flavour(
        out_df, source_by_cell, indexed, manual_column_overrides, lineage,
    )
    # Jalon 5.3.7 — derive *Enabled booleans from the matching *Price.
    # Must run AFTER the flavour fix so the price has its final value.
    # Jalon 5.3.24 — étendu à tiresEnabled / replacementVehicleEnabled.
    _postpass_compute_enabled_from_price(out_df, source_by_cell, lineage)
    # Jalon 5.3.7 — derive tiresEnabled from presence in pneus sources.
    _postpass_compute_enabled_from_presence(out_df, source_by_cell, indexed, lineage)
    # Jalon 5.3.24 — cascade allRisks/etc. → civilLiabilityEnabled = TRUE.
    # Doit tourner après les deux post-passes ci-dessus, qui ont fini de
    # remplir les *Enabled à partir des prix / présence.
    _postpass_imply_enabled_cascade(out_df, source_by_cell, lineage)
    # Jalon 5.3.24 — toggle « Assurance loueur globale » : si activé,
    # force civilLiabilityEnabled = TRUE partout. Doit tourner en
    # **dernier** pour écraser les éventuels FALSE posés en amont.
    _postpass_force_civil_liability(out_df, source_by_cell, lineage)
    # Jalon 5.2.2 — resolve Revio partnerId UUID from the lessor slug
    # whose source carries the row. Without this, partnerId stays empty
    # and the contract can't be imported into Revio.
    _postpass_resolve_partner_id(out_df, source_by_cell, indexed, lineage)

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

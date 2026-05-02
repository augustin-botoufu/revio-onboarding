"""Post-engine AI fallback for value normalization (Jalon 2.7).

When the engine runs, ``EngineResult.unresolved_enums`` holds the cells that
neither a hardcoded transform nor the cache dictionary could normalize to an
allowed enum value. This module takes those, batches them per (schema, field),
asks the LLM once per group to propose targets, upserts the results into the
mappings cache as ``pending`` entries, and patches the engine output DataFrame
in place.

The whole thing is best-effort: LLM errors never break the import, they just
leave the cells unresolved — the user can always normalize them manually in
the « 🧭 Normalisation » page.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from . import value_mappings as vm
from . import llm_mapper
from .schemas import SCHEMAS


@dataclass
class AIFallbackReport:
    """Summary of what happened during the AI fallback pass.

    Fed to the UI so Augustin can see (a) how many unresolved values got
    auto-mapped, (b) what's still unresolved, (c) LLM errors that prevented
    resolution for a given (schema, field).
    """
    # {(schema, field): int} — cells patched in the df via AI mapping
    resolved_counts: dict[tuple[str, str], int] = field(default_factory=dict)
    # {(schema, field): list[(raw, target)]} — entries added to the cache as pending
    proposed_mappings: dict[tuple[str, str], list[tuple[str, str]]] = field(default_factory=dict)
    # {(schema, field): user_error_message} — LLM call failed for this group
    errors: dict[tuple[str, str], str] = field(default_factory=dict)
    # {(schema, field): list[str]} — LLM's free-text notes, useful for audit
    notes: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    # Raw values the LLM mapped to null (unresolvable) — still unresolved
    still_unresolved: dict[tuple[str, str], list[str]] = field(default_factory=dict)

    @property
    def total_resolved(self) -> int:
        return sum(self.resolved_counts.values())

    @property
    def total_proposed(self) -> int:
        return sum(len(v) for v in self.proposed_mappings.values())

    @property
    def ok(self) -> bool:
        return not self.errors


def _field_meta(schema_name: str, field_name: str) -> tuple[list[str], str, str]:
    """Pull (allowed_values, description, seed_note) from SCHEMAS + the cache.

    The cache note is looked up after the fact in run_ai_fallback on a hit,
    this helper just returns ("", "") for note; schemas.py has no note field.
    """
    allowed: list[str] = []
    desc = ""
    for fs in SCHEMAS.get(schema_name, []):
        if getattr(fs, "name", "") == field_name:
            allowed = list(getattr(fs, "allowed_values", []) or [])
            desc = getattr(fs, "description", "") or ""
            break
    return allowed, desc, ""


def run_ai_fallback(
    mappings: dict,
    unresolved_enums: dict[tuple[str, str], tuple[str, str]],
    df: Optional[pd.DataFrame] = None,
    orphan_df: Optional[pd.DataFrame] = None,
    *,
    user_email: Optional[str] = None,
    api_key: Optional[str] = None,
    model: str = "claude-sonnet-4-5",
    value_mapping_hits: Optional[dict] = None,
) -> AIFallbackReport:
    """Run the AI fallback over unresolved enum cells, in place.

    Side effects:
    - ``mappings`` is mutated with new ``pending`` / ``ai`` entries.
    - ``df`` and ``orphan_df`` cells are patched with the AI-proposed target.
    - ``value_mapping_hits`` (if provided) gets new entries so the UI can see
      these resolutions alongside cache hits.

    Returns an ``AIFallbackReport`` the UI can render.
    """
    report = AIFallbackReport()
    if not unresolved_enums:
        return report

    # 1. Group (plate, field) → raw_value by (schema, field)
    # We also keep the reverse index to patch the df afterwards.
    groups: dict[tuple[str, str], dict[str, list[tuple[str, str]]]] = {}
    # {(schema, field): {raw_value: [(plate, field_name)]}}
    for (plate, field_name), (schema, raw) in unresolved_enums.items():
        key = (schema, field_name)
        groups.setdefault(key, {}).setdefault(raw, []).append((plate, field_name))

    # 2. For each group, call the LLM once.
    for (schema, field_name), raw_to_cells in groups.items():
        allowed, desc, seed_note = _field_meta(schema, field_name)
        if not allowed:
            # Shouldn't happen — means schemas.py lost this field between
            # engine run and now — but degrade gracefully.
            report.errors[(schema, field_name)] = (
                f"Champ {schema}.{field_name} sans allowed_values, on saute."
            )
            continue
        raw_values = list(raw_to_cells.keys())
        result = llm_mapper.propose_enum_mappings(
            schema_name=schema,
            field_name=field_name,
            allowed_values=allowed,
            raw_values=raw_values,
            field_description=desc,
            field_note=seed_note,
            api_key=api_key,
            model=model,
        )
        if "_error" in result:
            report.errors[(schema, field_name)] = result["_error"]
            continue

        report.notes[(schema, field_name)] = list(result.get("_notes", []))
        proposed = result.get("mappings", {}) or {}
        allowed_set = set(allowed)

        resolved = 0
        accepted: list[tuple[str, str]] = []
        rejected: list[str] = []

        for raw, target in proposed.items():
            if target is None or target not in allowed_set:
                rejected.append(raw)
                continue
            # Upsert a pending entry so the user can validate later.
            try:
                vm.upsert(
                    mappings, schema, field_name, raw, target,
                    status=vm.STATUS_PENDING,
                    source=vm.SOURCE_AI,
                    user=None,
                    note=None,
                )
                accepted.append((raw, target))
            except ValueError:
                # raw normalized to empty — shouldn't happen because it was
                # the source of an engine cell, but be defensive.
                rejected.append(raw)
                continue
            # Patch every cell that had this raw value.
            for (plate_key, fname) in raw_to_cells.get(raw, []):
                patched = _patch_cell(df, orphan_df, plate_key, fname, target)
                if patched:
                    resolved += 1
                if value_mapping_hits is not None:
                    value_mapping_hits[(plate_key, fname)] = (
                        raw, target, vm.STATUS_PENDING,
                    )

        report.resolved_counts[(schema, field_name)] = resolved
        report.proposed_mappings[(schema, field_name)] = accepted
        if rejected:
            report.still_unresolved[(schema, field_name)] = rejected

    return report


def _patch_cell(
    df: Optional[pd.DataFrame],
    orphan_df: Optional[pd.DataFrame],
    plate_key: str,
    field_name: str,
    target: str,
) -> bool:
    """Set ``df[plate, field] = target`` on whichever df contains the plate.

    Returns True if a cell was actually patched. Both DataFrames are indexed
    by ``plate_for_matching``-normalized keys (see rules_engine).
    """
    for candidate in (df, orphan_df):
        if candidate is None or candidate.empty:
            continue
        if field_name not in candidate.columns:
            continue
        if plate_key in candidate.index:
            candidate.at[plate_key, field_name] = target
            return True
    return False

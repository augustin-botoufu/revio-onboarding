"""Registry + helpers for rules YAML files (one per target table).

This module is the single source of truth for:
- Which rule tables exist (Vehicle, Contract, Driver, etc.)
- Where each YAML lives on disk
- How fields are grouped in the editor UI (Identification / Technical / ...)
- How to compute the default priority order of sources per field
- How to apply session-scoped priority overrides back onto a YAML dict

Adding a new table in the future = extend TABLES + FIELD_CATEGORIES + drop the
YAML in `src/rules/`. The engine + UI pick it up automatically.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml  # type: ignore


RULES_DIR = Path(__file__).parent / "rules"


# ---------- Table registry ----------


# Each entry describes a Revio target schema that can be produced by the rules
# engine. `available=False` entries appear in the UI as "coming soon" so the
# user sees the roadmap without being able to edit them yet.
TABLES: dict[str, dict[str, Any]] = {
    "vehicle": {
        "label": "🚗 Véhicules",
        "yaml_file": "vehicle.yml",
        "available": True,
    },
    "contract": {
        "label": "📄 Contrats",
        "yaml_file": "contract.yml",
        "available": True,
    },
    "driver": {
        "label": "👤 Drivers",
        "yaml_file": "driver.yml",
        "available": False,
    },
    "insurance": {
        "label": "🛡️ Assurance",
        "yaml_file": "insurance.yml",
        "available": False,
    },
}


# Logical grouping of fields per table, for the editor UI.
# Each group = (emoji-label, [field_names_in_display_order]).
# Fields not listed here fall back into a "Autres" bucket.
FIELD_CATEGORIES: dict[str, list[tuple[str, list[str]]]] = {
    "vehicle": [
        (
            "🪪 Identification",
            [
                "registrationPlate",
                "registrationVin",
                "registrationIssueCountryCode",
                "registrationIssueDate",
            ],
        ),
        (
            "🚗 Caractéristiques",
            [
                "brand",
                "model",
                "variant",
                "motorisation",
                "co2gKm",
                "weight",
                "registrationFiscalPower",
                "electricAutonomy",
                "electricEnginePower",
            ],
        ),
        (
            "📅 Parcours client",
            ["parcEntryAt", "usage"],
        ),
        (
            "🖼️ Média",
            ["imageUrl"],
        ),
    ],
    "contract": [
        (
            "🪪 Identification",
            ["plate", "number", "partnerId"],
        ),
        (
            "📅 Dates & durée",
            [
                "startDate", "endDate", "durationMonths",
                "restitutionDate", "factureDate",
            ],
        ),
        (
            "📏 Kilométrage",
            ["contractedMileage", "maxMileage", "extraKmPrice"],
        ),
        (
            "💰 Valeurs & TVA",
            ["vehicleValue", "batteryValue", "isHT", "totalPrice"],
        ),
        (
            "🛡️ Assurance",
            [
                "civilLiabilityPrice", "legalProtectionPrice",
                "theftFireAndGlassPrice", "allRisksPrice",
                "financialLossPrice",
            ],
        ),
        (
            "🔧 Services",
            [
                "maintenancePrice", "maintenanceNetwork",
                "replacementVehiclePrice",
                "tiresPrice", "tiresType", "tiresNetwork",
                "gasCardPrice", "tollCardPrice",
            ],
        ),
    ],
}


# ---------- YAML loading ----------


def list_available_tables() -> list[tuple[str, dict[str, Any]]]:
    """Return [(slug, meta)] for tables whose YAML exists on disk."""
    out: list[tuple[str, dict[str, Any]]] = []
    for slug, meta in TABLES.items():
        yaml_path = RULES_DIR / meta["yaml_file"]
        enriched = dict(meta)
        enriched["available"] = meta.get("available", False) and yaml_path.exists()
        out.append((slug, enriched))
    return out


def load_rules_yaml(table_slug: str) -> dict:
    """Load the parsed YAML for a given table slug."""
    meta = TABLES.get(table_slug)
    if meta is None:
        raise KeyError(f"Table inconnue: {table_slug!r}")
    path = RULES_DIR / meta["yaml_file"]
    if not path.exists():
        raise FileNotFoundError(f"Fichier de règles introuvable: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- Priority introspection ----------


def default_priority_order(field_spec: dict) -> list[tuple[str, str, int]]:
    """Given a field spec from YAML, return [(source_slug, source_label, priority)].

    Sorted by (priority, source_slug) — same deterministic order the engine uses.
    `__default__` rules are excluded (they're constants, not real sources).
    """
    rules = field_spec.get("rules", [])
    out: list[tuple[str, str, int]] = []
    seen: set[str] = set()
    for rule in rules:
        slug = rule.get("source")
        if not slug or slug == "__default__" or slug in seen:
            continue
        label = rule.get("source_label", slug)
        prio = rule.get("priority", 99)
        out.append((slug, label, prio))
        seen.add(slug)
    out.sort(key=lambda x: (x[2], x[0]))
    return out


def resolve_current_order(
    field_spec: dict,
    override: list[str] | None,
) -> list[tuple[str, str, int]]:
    """Return [(slug, label, effective_priority), ...] to display / apply.

    - If `override` is None, priorities come straight from the YAML and
      preserve ties (ex-æquo). Example: [("api_plaques", "API Plaques", 1),
      ("arval_uat", "...", 2), ("ayvens_etat_parc", "...", 2), ...].
    - If `override` is set, priorities are flattened to 1..N in the order the
      user chose, and any YAML slug missing from the override is appended at
      the end (safer than dropping). Stale slugs from old sessions are
      filtered out.
    """
    default = default_priority_order(field_spec)  # [(slug, label, yaml_prio)]
    if override is None:
        return list(default)

    labels = {s: lbl for s, lbl, _ in default}
    default_slugs = [s for s, _, _ in default]
    clean = [s for s in override if s in default_slugs]
    missing = [s for s in default_slugs if s not in clean]
    ordered = clean + missing
    return [(s, labels[s], i + 1) for i, s in enumerate(ordered)]


def categorize_fields(table_slug: str, fields_spec: dict) -> list[tuple[str, list[str]]]:
    """Return [(category_label, [field_names])] for the UI.

    Fields declared in FIELD_CATEGORIES come first in their declared order;
    any field present in the YAML but missing from the categorization falls
    into a final 'Autres' bucket so nothing gets hidden accidentally.
    """
    categories = FIELD_CATEGORIES.get(table_slug, [])
    mentioned: set[str] = set()
    resolved: list[tuple[str, list[str]]] = []
    for cat_label, field_names in categories:
        kept = [f for f in field_names if f in fields_spec]
        if kept:
            resolved.append((cat_label, kept))
            mentioned.update(kept)
    leftover = [f for f in fields_spec.keys() if f not in mentioned]
    if leftover:
        resolved.append(("🗂️ Autres", leftover))
    return resolved


# ---------- Priority overrides ----------


def apply_priority_overrides(
    rules_yaml: dict,
    overrides: dict[str, list[str]] | None,
) -> dict:
    """Return a DEEP COPY of `rules_yaml` with priorities rewritten per override.

    `overrides` maps {field_name: [source_slug_in_priority_order]}.

    For each overridden field, the sources listed get priorities 1, 2, 3…
    in that order. Sources not listed in the override (but present in the
    original YAML) keep their relative order and are shifted AFTER all
    overridden ones. `__default__` rules are left untouched.

    Non-overridden fields are unchanged.
    """
    new_yaml = copy.deepcopy(rules_yaml)
    if not overrides:
        return new_yaml

    fields_spec = new_yaml.get("fields", {})
    for field_name, ordered_slugs in overrides.items():
        spec = fields_spec.get(field_name)
        if not spec:
            continue
        rules = spec.get("rules", [])
        if not rules:
            continue

        # Separate concrete rules from __default__ (keep default as-is).
        concrete = [r for r in rules if r.get("source") != "__default__"]
        defaults = [r for r in rules if r.get("source") == "__default__"]

        # Build {slug: rule_index_in_concrete} keeping FIRST occurrence.
        idx_by_slug: dict[str, int] = {}
        for i, r in enumerate(concrete):
            slug = r.get("source")
            if slug and slug not in idx_by_slug:
                idx_by_slug[slug] = i

        # New priorities: 1 for first overridden, 2 for second, ...
        new_prio: dict[int, int] = {}
        pos = 1
        for slug in ordered_slugs:
            i = idx_by_slug.get(slug)
            if i is not None and i not in new_prio:
                new_prio[i] = pos
                pos += 1

        # Sources NOT in the override keep their relative order but shifted.
        for slug, i in idx_by_slug.items():
            if i not in new_prio:
                new_prio[i] = pos
                pos += 1

        for i, prio in new_prio.items():
            concrete[i]["priority"] = prio

        spec["rules"] = defaults + concrete

    return new_yaml


# ---------- Counting helpers (for the UI summary bar) ----------


def count_active_overrides(overrides_by_table: dict[str, dict[str, list[str]]] | None) -> int:
    """Total number of fields that have a non-empty override across all tables."""
    if not overrides_by_table:
        return 0
    total = 0
    for table_overrides in overrides_by_table.values():
        total += sum(1 for v in table_overrides.values() if v)
    return total

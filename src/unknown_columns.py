"""Interactive flow for resolving unidentified source columns.

Some fields don't have a known source column yet — typical case: the
`batteryValue` column in Ayvens AND, which only matters when a VE is
in the import but we haven't yet pinpointed which column carries it.

Flow (UI-side, consumed by Streamlit page Moteur):
1. Engine runs with current YAML + learned patterns.
2. `EngineResult.unknown_column_requests` lists (plate, number, field,
   candidate_sources, hint) tuples where a mandatory field stayed None.
3. UI picks one, shows the user the raw source DataFrame head, lets
   them click a column.
4. This module persists the choice to `learned_patterns.yml` and
   re-runs the engine. Next imports won't re-ask.

Persistence format (additive, backwards-compatible with the Vehicle
learned_patterns.yml the app already uses):

    patterns:
      contract:
        <source_slug>:
          <field_name>:
            column: <header_picked_by_user>
            learned_on: <ISO-8601 datetime>
            learned_by: <user_email>
            sample_values: [<v1>, <v2>, <v3>]

The engine reads these and injects them as `manual_column_overrides`
at next run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml  # type: ignore


@dataclass
class UnknownColumnRequest:
    plate: str
    number: str
    field: str
    candidate_sources: list[str]
    hint: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> "UnknownColumnRequest":
        return cls(
            plate=d["plate"],
            number=d["number"],
            field=d["field"],
            candidate_sources=list(d.get("candidate_sources") or []),
            hint=d.get("hint"),
        )


def load_learned_patterns(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        return {"patterns": {}}
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # Some older checked-in versions of learned_patterns.yml have
    # ``patterns: []`` (empty list) rather than ``{}``. ``setdefault``
    # won't replace an existing list, so calling ``.setdefault()`` on
    # ``data["patterns"]`` below would crash. Normalise the type here.
    if not isinstance(data.get("patterns"), dict):
        data["patterns"] = {}
    return data


def save_learned_patterns(path: str | Path, data: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False,
                       default_flow_style=False)


def register_learned_column(
    patterns_path: str | Path,
    *,
    table: str,
    source_slug: str,
    field_name: str,
    column: str,
    source_df: Optional[pd.DataFrame] = None,
    learned_by: Optional[str] = None,
    sample_n: int = 3,
) -> None:
    """Persist a user's interactive column choice into learned_patterns.yml.

    Adds or overwrites the entry under patterns > {table} > {source_slug}
    > {field_name}. Safe to call repeatedly — last-write wins.

    `source_df` (optional): if provided, we capture up to `sample_n`
    non-null sample values from that column for future debugging /
    assistant explanations.
    """
    data = load_learned_patterns(patterns_path)
    table_node = data["patterns"].setdefault(table, {})
    source_node = table_node.setdefault(source_slug, {})
    entry = {
        "column": column,
        "learned_on": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "learned_by": learned_by or "unknown",
    }
    if source_df is not None and column in source_df.columns:
        samples = [v for v in source_df[column].dropna().head(sample_n).tolist()]
        entry["sample_values"] = samples[:sample_n]
    source_node[field_name] = entry
    save_learned_patterns(patterns_path, data)


def learned_patterns_to_overrides(
    patterns_path: str | Path,
    table: str,
) -> dict[tuple[str, str], str]:
    """Produce the `manual_column_overrides` dict the engines consume
    at run time, filtered on `table` (vehicle/contract)."""
    data = load_learned_patterns(patterns_path)
    out: dict[tuple[str, str], str] = {}
    table_node = data["patterns"].get(table, {})
    for source_slug, fields in table_node.items():
        for field_name, entry in (fields or {}).items():
            col = entry.get("column") if isinstance(entry, dict) else None
            if col:
                out[(source_slug, field_name)] = col
    return out


def format_request_for_ui(req: UnknownColumnRequest, sources_loaded: list[str]) -> dict:
    """Shape the request for the Streamlit page to render.

    Returns a dict with a short narrative the UI can show to the user.
    """
    candidates_available = [s for s in req.candidate_sources if s in sources_loaded]
    return {
        "title": f"Colonne manquante : {req.field}",
        "body": (
            f"Pour le contrat {req.plate} / {req.number}, le champ "
            f"`{req.field}` n'a pas pu être rempli automatiquement. "
            + (f"Indice : {req.hint} " if req.hint else "")
            + (f"Sources disponibles pour ce champ : {', '.join(candidates_available)}."
               if candidates_available else
               "Aucune source importée ne déclare ce champ — mapping manuel requis côté client_file.")
        ),
        "candidate_sources": candidates_available or ["client_file"],
        "field": req.field,
        "plate": req.plate,
        "number": req.number,
    }


def suppress_resolved_requests(
    requests: list[dict],
    patterns_path: str | Path,
    table: str,
) -> list[dict]:
    """Drop requests for fields that already have a learned column.

    Use this after loading `learned_patterns.yml` to hide requests the
    user has already answered in a previous session.
    """
    overrides = learned_patterns_to_overrides(patterns_path, table)
    out = []
    for r in requests:
        if any((s, r["field"]) in overrides for s in r.get("candidate_sources", [])):
            continue
        out.append(r)
    return out

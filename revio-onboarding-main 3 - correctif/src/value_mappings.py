"""Value-level normalization memory (Jalon 2.7).

Each enum-typed Revio field (e.g. ``vehicle.motorisation`` with allowed
values ``diesel/gas/hybrid/electric``) gets a dictionary of
``raw_value → target`` that grows as we see new data. Three sources of
entries:

- ``seed`` : hand-curated initial mappings (validated from the start).
- ``ai``   : proposed by the LLM on a previously-unseen value (``pending``
  until a human validates).
- ``manual``: added or corrected by a user through the UI (``validated``).

Two statuses:

- ``validated`` : we're confident, apply as-is and don't flag.
- ``pending``   : we've applied it once (rather than leaving the cell
  empty) but it wants human review before being reused without doubt.

Lookups are case/accent/punctuation/whitespace insensitive. Persistence is
a single YAML file at ``src/rules/value_mappings.yml`` — same persistence
model as ``learned_patterns.yml`` — with GitHub auto-commit handled by the
existing ``github_sync`` module.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import yaml  # type: ignore


VALUE_MAPPINGS_PATH = Path(__file__).parent / "rules" / "value_mappings.yml"


# Status + source constants (strings; avoid Enums to keep YAML friendly
# and to allow arbitrary future values without migration).
STATUS_VALIDATED = "validated"
STATUS_PENDING = "pending"

SOURCE_SEED = "seed"
SOURCE_AI = "ai"
SOURCE_MANUAL = "manual"


@dataclass
class ValueMapping:
    """In-memory representation of a single raw→target entry."""
    raw: str                        # original human form of the source value
    target: str                     # canonical Revio value (in schema.allowed_values)
    status: str                     # STATUS_VALIDATED | STATUS_PENDING
    source: str                     # SOURCE_SEED | SOURCE_AI | SOURCE_MANUAL
    created_at: str                 # ISO date (YYYY-MM-DD)
    updated_at: str
    validated_at: Optional[str] = None
    validated_by: Optional[str] = None
    note: Optional[str] = None      # free-text, e.g. "ambigu: Hybride-diesel → hybrid"

    def as_dict(self) -> dict:
        """YAML-friendly dict (drops None fields for tidier output)."""
        return {k: v for k, v in asdict(self).items() if v is not None}


# =============================================================================
# Normalization
# =============================================================================

def normalize_key(s: Any) -> str:
    """Reduce a raw value to a canonical lookup key.

    Steps:
    1. ``str()`` + strip outer whitespace
    2. Unicode NFKD + drop combining marks (no accents)
    3. lowercase
    4. collapse runs of any non-alphanumeric char into a single space
    5. strip

    So ``"Hybride-Essence  "``, ``"hybride essence"``, ``"HYBRIDE/ESSENCE"``
    all map to ``"hybride essence"``. Returns ``""`` for None/empty input.
    """
    if s is None:
        return ""
    try:
        s = str(s).strip()
    except Exception:
        return ""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    no_accents = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    lower = no_accents.lower()
    collapsed = re.sub(r"[^a-z0-9]+", " ", lower).strip()
    return collapsed


def field_key(schema: str, field_name: str) -> str:
    """Return the canonical top-level YAML key for a (schema, field) pair."""
    return f"{schema}.{field_name}"


def parse_field_key(fkey: str) -> tuple[str, str]:
    """Inverse of ``field_key``. Returns ('', fkey) if no dot present."""
    if "." not in fkey:
        return ("", fkey)
    schema, field_name = fkey.split(".", 1)
    return (schema, field_name)


# =============================================================================
# Load / Save
# =============================================================================

def load(path: Optional[Path] = None) -> dict[str, dict[str, ValueMapping]]:
    """Load all mappings from YAML.

    Returns a 2-level dict:
        { "<schema>.<field>": { "<normalized_raw>": ValueMapping, ... }, ... }

    Robust by design: malformed entries are skipped silently rather than
    crashing the app. A missing file returns ``{}``.
    """
    p = Path(path) if path else VALUE_MAPPINGS_PATH
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except (yaml.YAMLError, OSError):
        return {}
    raw = data.get("mappings") if isinstance(data, dict) else None
    if not isinstance(raw, dict):
        return {}
    today = date.today().isoformat()
    out: dict[str, dict[str, ValueMapping]] = {}
    for fkey, entries in raw.items():
        if not isinstance(entries, list):
            continue
        bucket: dict[str, ValueMapping] = {}
        for e in entries:
            if not isinstance(e, dict):
                continue
            raw_val = e.get("raw")
            target = e.get("target")
            if raw_val is None or target is None:
                continue
            created_at = str(e.get("created_at") or today)
            vm = ValueMapping(
                raw=str(raw_val),
                target=str(target),
                status=str(e.get("status", STATUS_PENDING)),
                source=str(e.get("source", SOURCE_MANUAL)),
                created_at=created_at,
                updated_at=str(e.get("updated_at") or created_at),
                validated_at=_opt_str(e.get("validated_at")),
                validated_by=_opt_str(e.get("validated_by")),
                note=_opt_str(e.get("note")),
            )
            key = normalize_key(raw_val)
            if not key:
                continue
            bucket[key] = vm
        if bucket:
            out[str(fkey)] = bucket
    return out


def _opt_str(v: Any) -> Optional[str]:
    """Coerce a YAML field to str or None (empty string treated as None)."""
    if v is None:
        return None
    s = str(v)
    return s if s else None


def dump_yaml(mappings: dict[str, dict[str, ValueMapping]]) -> str:
    """Serialize to YAML string with a stable, diffable layout."""
    payload_mappings: dict = {}
    for fkey in sorted(mappings.keys()):
        bucket = mappings[fkey]
        # Stable per-entry order : target first, then raw (case-insensitive).
        # Makes the YAML diff-readable and groups siblings visually.
        entries = [
            vm.as_dict()
            for _, vm in sorted(
                bucket.items(),
                key=lambda kv: (kv[1].target, kv[1].raw.lower()),
            )
        ]
        payload_mappings[fkey] = entries
    body = yaml.safe_dump(
        {"mappings": payload_mappings},
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )
    header = (
        "# Revio — dictionnaire de normalisation des valeurs.\n"
        "# Maintenu par l'app onboarding (page « 🧭 Normalisation »).\n"
        "# Éditable à la main mais préfère l'UI quand c'est possible.\n"
        "# status : validated | pending. source : seed | ai | manual.\n"
        "# Les `raw` sont matchés insensibles à la casse/accents/ponctuation.\n"
        "---\n"
    )
    return header + body


def save(
    mappings: dict[str, dict[str, ValueMapping]],
    path: Optional[Path] = None,
) -> None:
    """Write mappings to local YAML (not used in Streamlit Cloud runtime,
    which has an ephemeral filesystem — prefer ``github_sync`` paths)."""
    p = Path(path) if path else VALUE_MAPPINGS_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(dump_yaml(mappings), encoding="utf-8")


# =============================================================================
# CRUD
# =============================================================================

def lookup(
    mappings: dict[str, dict[str, ValueMapping]],
    schema: str,
    field_name: str,
    raw_value: Any,
) -> Optional[ValueMapping]:
    """Return the ValueMapping for a raw value, or None if unknown."""
    key = normalize_key(raw_value)
    if not key:
        return None
    bucket = mappings.get(field_key(schema, field_name))
    if not bucket:
        return None
    return bucket.get(key)


def upsert(
    mappings: dict[str, dict[str, ValueMapping]],
    schema: str,
    field_name: str,
    raw: str,
    target: str,
    *,
    status: str,
    source: str,
    user: Optional[str] = None,
    note: Optional[str] = None,
) -> ValueMapping:
    """Add or replace a mapping entry.

    Returns the final (new or updated) ValueMapping. The normalized key is
    derived from ``raw`` — if two raw forms normalize to the same key (e.g.
    ``"Hybride-essence"`` and ``"hybride essence"``), the later call wins.
    """
    key = normalize_key(raw)
    if not key:
        raise ValueError(f"Cannot normalize empty raw value: {raw!r}")
    if not target:
        raise ValueError("target must be a non-empty string")
    fkey = field_key(schema, field_name)
    bucket = mappings.setdefault(fkey, {})
    today = date.today().isoformat()
    existing = bucket.get(key)
    if existing is None:
        vm = ValueMapping(
            raw=str(raw),
            target=str(target),
            status=status,
            source=source,
            created_at=today,
            updated_at=today,
            validated_at=today if status == STATUS_VALIDATED else None,
            validated_by=user if status == STATUS_VALIDATED else None,
            note=note,
        )
        bucket[key] = vm
        return vm
    existing.target = str(target)
    existing.status = status
    existing.source = source
    existing.updated_at = today
    if status == STATUS_VALIDATED:
        existing.validated_at = today
        existing.validated_by = user or existing.validated_by
    if note is not None:
        existing.note = note
    return existing


def validate_entry(
    mappings: dict[str, dict[str, ValueMapping]],
    schema: str,
    field_name: str,
    normalized_or_raw: str,
    *,
    user: Optional[str] = None,
) -> Optional[ValueMapping]:
    """Flip a mapping's status to ``validated``. Accepts either the already-
    normalized key OR a raw value that we'll normalize for you."""
    key = normalize_key(normalized_or_raw) or normalized_or_raw
    bucket = mappings.get(field_key(schema, field_name))
    if not bucket or key not in bucket:
        return None
    vm = bucket[key]
    today = date.today().isoformat()
    vm.status = STATUS_VALIDATED
    vm.validated_at = today
    vm.validated_by = user
    vm.updated_at = today
    return vm


def delete_entry(
    mappings: dict[str, dict[str, ValueMapping]],
    schema: str,
    field_name: str,
    normalized_or_raw: str,
) -> bool:
    """Remove an entry. Returns True if something was actually removed."""
    key = normalize_key(normalized_or_raw) or normalized_or_raw
    fkey = field_key(schema, field_name)
    bucket = mappings.get(fkey)
    if not bucket or key not in bucket:
        return False
    del bucket[key]
    if not bucket:
        mappings.pop(fkey, None)
    return True


# =============================================================================
# Utilities for the rest of the app
# =============================================================================

def iter_enum_fields(schemas: dict) -> list[tuple[str, str, list[str]]]:
    """Inspect the SCHEMAS dict and yield ``(schema_name, field_name,
    allowed_values)`` for every enum-typed field. Used to know which cells
    should run through value-normalization."""
    out: list[tuple[str, str, list[str]]] = []
    for schema_name, fields in schemas.items():
        for fs in fields:
            allowed = getattr(fs, "allowed_values", None)
            name = getattr(fs, "name", "") or ""
            if allowed and name:
                out.append((schema_name, name, list(allowed)))
    return out


def stats_by_field(
    mappings: dict[str, dict[str, ValueMapping]],
) -> dict[str, dict[str, int]]:
    """Summarize how many entries per field × status. Used by the UI."""
    out: dict[str, dict[str, int]] = {}
    for fkey, bucket in mappings.items():
        counts = {STATUS_VALIDATED: 0, STATUS_PENDING: 0}
        for vm in bucket.values():
            counts[vm.status] = counts.get(vm.status, 0) + 1
        out[fkey] = counts
    return out

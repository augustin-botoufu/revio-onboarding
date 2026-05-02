"""Learned detection patterns — a lightweight memory for file formats.

When a user manually classifies a file whose format we didn't know (say, a
new lessor's état de parc), we want to remember the signature so next time
the same format arrives it's auto-detected. This module handles the
persistence + match logic.

Persistence model (Option A — MVP):
- A single YAML file at `src/rules/learned_patterns.yml`.
- The UI offers a "📋 Mémoriser ce format" button that produces a YAML snippet
  the user copies into the file and commits to the repo. The app never writes
  to disk (Streamlit Cloud has an ephemeral filesystem).
- At upload time, we load the YAML and try to match each new file against
  every pattern. First match wins.

Pattern shape:
```yaml
patterns:
  - id: "alphabet_etat_parc"          # unique identifier
    slug: "autre_loueur_etat_parc"    # the YAML engine slug this file resolves to
    loueur_hint: "Alphabet"           # optional — for UI display only
    match:
      filename_regex: "(?i)alphabet.*etat.*parc"  # required
      columns_include: ["Immatriculation", "VIN"]  # optional — all must be present
    header_row: 6                     # optional — reserved for chantier 2
    column_mapping: {}                # optional — reserved for chantier 2
    created_at: "2026-04-21"          # optional — metadata
    created_by: "user@example.com"    # optional — metadata
```

The module is intentionally small; the smarts (IA fallback, header-row
heuristic, persisted column mapping) come in chantier 2.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import yaml  # type: ignore


LEARNED_PATTERNS_PATH = Path(__file__).parent / "rules" / "learned_patterns.yml"


@dataclass
class LearnedPattern:
    """In-memory representation of a single learned pattern."""
    id: str
    slug: str
    loueur_hint: Optional[str]
    filename_regex: str
    columns_include: list[str]
    header_row: Optional[int]
    column_mapping: dict[str, str]
    raw: dict  # full YAML entry, for UI display

    def matches(self, filename: str, columns: list[str]) -> bool:
        """Return True if this pattern matches the (filename, columns) signature.

        - filename_regex must match (case-insensitive if the regex says so).
        - If columns_include is set, ALL listed columns must be present in `columns`.
        """
        if not self.filename_regex:
            return False
        try:
            if not re.search(self.filename_regex, filename or ""):
                return False
        except re.error:
            return False
        if self.columns_include:
            cols_lower = {str(c).strip().lower() for c in columns}
            want = {str(c).strip().lower() for c in self.columns_include}
            if not want.issubset(cols_lower):
                return False
        return True


def _parse_pattern(entry: dict) -> Optional[LearnedPattern]:
    """Build a LearnedPattern from a raw YAML dict, returning None if invalid."""
    if not isinstance(entry, dict):
        return None
    pat_id = str(entry.get("id") or "").strip()
    slug = str(entry.get("slug") or "").strip()
    if not pat_id or not slug:
        return None
    match = entry.get("match") or {}
    filename_regex = str(match.get("filename_regex") or "").strip()
    cols_inc = match.get("columns_include") or []
    if not isinstance(cols_inc, list):
        cols_inc = []
    try:
        header_row = int(entry["header_row"]) if entry.get("header_row") is not None else None
    except (ValueError, TypeError):
        header_row = None
    col_map = entry.get("column_mapping") or {}
    if not isinstance(col_map, dict):
        col_map = {}
    return LearnedPattern(
        id=pat_id,
        slug=slug,
        loueur_hint=(str(entry["loueur_hint"]).strip() if entry.get("loueur_hint") else None),
        filename_regex=filename_regex,
        columns_include=[str(c) for c in cols_inc],
        header_row=header_row,
        column_mapping={str(k): str(v) for k, v in col_map.items()},
        raw=entry,
    )


def load_patterns(path: Optional[Path] = None) -> list[LearnedPattern]:
    """Load all patterns from the YAML file. Returns [] if the file is missing
    or malformed (robust by design — a broken YAML shouldn't kill the app)."""
    p = Path(path) if path else LEARNED_PATTERNS_PATH
    if not p.exists():
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except (yaml.YAMLError, OSError):
        return []
    raw = data.get("patterns") if isinstance(data, dict) else None
    if not isinstance(raw, list):
        return []
    out: list[LearnedPattern] = []
    for entry in raw:
        parsed = _parse_pattern(entry)
        if parsed is not None:
            out.append(parsed)
    return out


def match_pattern(
    filename: str,
    columns: list[str],
    patterns: Optional[list[LearnedPattern]] = None,
) -> Optional[LearnedPattern]:
    """Return the first pattern that matches, or None if nothing matches.

    `patterns` can be passed in to avoid re-reading the YAML on every file.
    """
    if patterns is None:
        patterns = load_patterns()
    for pat in patterns:
        if pat.matches(filename, columns):
            return pat
    return None


def build_pattern_entry(
    slug: str,
    filename: str,
    columns: list[str],
    *,
    loueur_hint: Optional[str] = None,
    header_row: Optional[int] = None,
    column_mapping: Optional[dict[str, str]] = None,
    author: Optional[str] = None,
) -> dict[str, Any]:
    """Build the structured dict representation of a learned pattern.

    This is the canonical form — same shape as a YAML entry — used by
    `github_sync.save_pattern()` to commit directly via the GitHub API,
    and by `format_yaml_snippet()` for the legacy copy-paste flow.

    The filename regex is derived from the filename: we lowercase it, strip
    the date/timestamp-like suffixes, and escape special chars. The user can
    edit the regex later.

    Columns signature: up to 2 "strong" columns (long, likely unique to this
    format) required on top of the filename regex. Purely heuristic.
    """
    base_id = _slugify(filename) or slug
    fname_regex = _derive_filename_regex(filename)
    strong_cols = _pick_strong_columns(columns, n=2)

    entry: dict[str, Any] = {
        "id": base_id,
        "slug": slug,
        "match": {
            "filename_regex": fname_regex,
        },
    }
    if strong_cols:
        entry["match"]["columns_include"] = strong_cols
    if loueur_hint:
        entry["loueur_hint"] = loueur_hint
    if header_row is not None:
        entry["header_row"] = int(header_row)
    if column_mapping:
        entry["column_mapping"] = {str(k): str(v) for k, v in column_mapping.items()}
    entry["created_at"] = date.today().isoformat()
    if author:
        entry["created_by"] = author
    return entry


def format_yaml_snippet(
    slug: str,
    filename: str,
    columns: list[str],
    *,
    loueur_hint: Optional[str] = None,
    header_row: Optional[int] = None,
    column_mapping: Optional[dict[str, str]] = None,
    author: Optional[str] = None,
) -> str:
    """Legacy copy-paste snippet. Wraps `build_pattern_entry()` in YAML."""
    entry = build_pattern_entry(
        slug,
        filename,
        columns,
        loueur_hint=loueur_hint,
        header_row=header_row,
        column_mapping=column_mapping,
        author=author,
    )
    snippet_body = yaml.safe_dump(
        [entry], sort_keys=False, allow_unicode=True, default_flow_style=False
    )
    return (
        "# --- Collez ce bloc sous `patterns:` dans "
        "src/rules/learned_patterns.yml, puis committez. ---\n"
        + snippet_body
    )


def _slugify(s: str) -> str:
    """Turn a filename into a safe lowercase alnum-only id (underscore-separated)."""
    s = (s or "").lower()
    s = re.sub(r"\.[a-z0-9]+$", "", s)  # strip extension
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    # Strip trailing digit clusters that look like dates / timestamps.
    s = re.sub(r"(_\d{4,})+$", "", s)
    return s or ""


def _derive_filename_regex(filename: str) -> str:
    """Best-effort regex that will match future files with similar names.

    Strategy: lowercase, replace any digit run with `.*`, escape the rest,
    wrap with `(?i)` for case-insensitivity.
    """
    base = (filename or "").lower()
    base = re.sub(r"\.[a-z0-9]+$", "", base)  # strip extension
    # Escape regex special chars except digit runs which we'll replace.
    escaped_parts: list[str] = []
    for token in re.split(r"(\d+)", base):
        if token.isdigit():
            escaped_parts.append(r"\d+")
        else:
            escaped_parts.append(re.escape(token))
    core = "".join(escaped_parts)
    # Relax word boundaries with wildcards to tolerate slight name variations.
    return f"(?i).*{core}.*"


def _pick_strong_columns(columns: list[str], n: int = 2) -> list[str]:
    """Pick `n` distinctive columns to make the pattern more specific.

    Heuristic: longest columns first (tend to be proper labels like
    "Date de première circulation" vs noise like "id"), excluding anything
    too generic.
    """
    generic = {"id", "nom", "name", "code", "type", "statut", "status"}
    cleaned = [str(c).strip() for c in columns if str(c).strip()]
    cleaned = [c for c in cleaned if c.lower() not in generic]
    cleaned.sort(key=lambda s: (-len(s), s.lower()))
    return cleaned[:n]

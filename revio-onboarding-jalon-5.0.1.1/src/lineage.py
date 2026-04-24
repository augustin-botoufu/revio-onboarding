"""Lineage tracking — provenance per output cell.

Purpose
-------
For every cell in the Revio output (Vehicle or Contract), we record WHERE
the value came from, HOW it was computed, and WHAT other candidates were
ignored. This side-car is consumed by the in-app LLM assistant
(Jalon 5.0) which answers "pourquoi cette valeur ?" in plain language
for AMs/Sales, or technical mode for devs.

Design
------
- Additive: the rules engine keeps its existing return shape. The engine
  just ALSO calls `store.record(...)` each time a cell is resolved. No
  breaking change, no risk to existing Vehicle pipeline.
- Shared between tables: same LineageRecord schema for Vehicle and
  Contract. The `table` field distinguishes them.
- Serializable: the store flushes to parquet (fast, typed) via
  `store.to_parquet(path)`. Parquet because it keeps list/dict columns
  and is orders of magnitude smaller than JSON/CSV for this volume.
- Zip-friendly: `_lineage/vehicle.parquet` and `_lineage/contract.parquet`
  land in the final zip alongside the xlsx outputs. Invisible to the
  end user; read by the assistant.

Usage
-----
    from .lineage import LineageStore, LineageRecord

    store = LineageStore()
    # … inside the rules engine, when a winner is chosen:
    store.record(LineageRecord(
        table="vehicle",
        key="AB-123-CD",
        field="brand",
        value="RENAULT",
        source_used="ayvens_etat_parc",
        source_col="Marque",
        source_row=142,
        priority=1,
        transform="upper",
        rule_id="vehicle.brand.ayvens_etat_parc#1",
        conflicts_ignored=[
            {"source": "arval_uat", "value": "Renault", "reason": "priorité inférieure (2 vs 1)"},
        ],
        notes="Match exact après normalisation casse.",
    ))
    # At the end:
    store.to_parquet("_lineage/vehicle.parquet")
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


# ---------- Record schema ----------


@dataclass
class LineageRecord:
    """Provenance of a single output cell.

    Attributes
    ----------
    table : "vehicle" | "contract" | …
        Which Revio base the cell belongs to.
    key : str
        The primary key of the row — plate for Vehicle, (plate, number)
        joined by "|" for Contract.
    field : str
        The Revio target field name (e.g. "totalPrice", "brand").
    value : Any
        The final value retained in the output cell (post-transform).
    source_used : str
        Slug of the source that won (e.g. "ayvens_etat_parc",
        "arval_facture_pdf", "__default__", "client_file").
    source_col : Optional[str]
        The column header (or regex match label) used on the source.
    source_row : Optional[int]
        The 0-based row index within the source DataFrame (or PDF page
        number for PDF sources). Optional — may be None for derived
        values (e.g. computed duration).
    priority : int
        Rank used to choose this source (1 = best).
    transform : str
        Name of the transform applied (e.g. "upper", "float_fr",
        "date_fr_to_iso", "sum_whitelist").
    rule_id : Optional[str]
        Stable identifier of the YAML rule that produced this value.
        Conventional format: "{table}.{field}.{source_slug}#{priority}".
    conflicts_ignored : list[dict]
        All other contributions whose value differs from the winner.
        Each dict carries at least {"source", "value", "reason"} so the
        assistant can explain WHY we didn't pick them.
    notes : Optional[str]
        Free text — e.g. "within tolerance 2% + 2€", "regex matched
        line 'Contrat N° 499581'".
    warnings : list[str]
        Non-blocking warnings emitted by the transform.
    """

    table: str
    key: str
    field: str
    value: Any
    source_used: str
    source_col: Optional[str] = None
    source_row: Optional[int] = None
    priority: int = 99
    transform: str = "passthrough"
    rule_id: Optional[str] = None
    conflicts_ignored: list[dict] = field(default_factory=list)
    notes: Optional[str] = None
    warnings: list[str] = field(default_factory=list)


# ---------- Store ----------


class LineageStore:
    """Collect LineageRecord entries and flush to parquet.

    Thread-unsafe (single-threaded Streamlit context). If we later
    parallelize, wrap this in a lock.
    """

    def __init__(self) -> None:
        self._records: list[LineageRecord] = []

    def record(self, r: LineageRecord) -> None:
        self._records.append(r)

    def extend(self, rs: Iterable[LineageRecord]) -> None:
        self._records.extend(rs)

    def __len__(self) -> int:
        return len(self._records)

    def to_dataframe(self) -> pd.DataFrame:
        """Materialize collected records as a DataFrame.

        List/dict columns are kept as-is (parquet handles them via pyarrow).
        """
        if not self._records:
            return pd.DataFrame(columns=[
                "table", "key", "field", "value", "source_used",
                "source_col", "source_row", "priority", "transform",
                "rule_id", "conflicts_ignored", "notes", "warnings",
            ])
        rows = [asdict(r) for r in self._records]
        # Normalize 'value' to string to keep parquet schema stable across
        # mixed types (int/float/str/bool appear for the same field across
        # different cells? no — but across fields yes).
        for row in rows:
            if row["value"] is not None and not isinstance(row["value"], str):
                row["value"] = repr(row["value"])
        df = pd.DataFrame(rows)
        return df

    def to_parquet(self, path: str | Path) -> Path:
        """Write the store to a parquet file. Creates parent dirs."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        df = self.to_dataframe()
        # pyarrow backend is required for list/dict columns.
        try:
            df.to_parquet(p, engine="pyarrow", index=False)
        except Exception:
            # Fallback to fastparquet or just serialize to jsonl if parquet
            # deps aren't available in the environment.
            jsonl = p.with_suffix(".jsonl")
            df.to_json(jsonl, orient="records", lines=True, force_ascii=False)
            return jsonl
        return p

    def filter(
        self,
        *,
        table: Optional[str] = None,
        key: Optional[str] = None,
        field_name: Optional[str] = None,
    ) -> list[LineageRecord]:
        """Return records matching the given filters (AND).

        Used by the assistant to fetch the lineage of a specific cell.
        """
        out = []
        for r in self._records:
            if table is not None and r.table != table:
                continue
            if key is not None and r.key != key:
                continue
            if field_name is not None and r.field != field_name:
                continue
            out.append(r)
        return out

    @staticmethod
    def load_parquet(path: str | Path) -> "LineageStore":
        """Rehydrate a store from a parquet (or jsonl fallback) file.

        Used by the assistant. Accepts both .parquet and .jsonl sidecars
        transparently — whichever was produced at write time.
        """
        p = Path(path)
        if not p.exists():
            # Try jsonl fallback if .parquet path doesn't exist
            alt = p.with_suffix(".jsonl")
            if alt.exists():
                p = alt
            else:
                return LineageStore()
        if p.suffix == ".jsonl":
            df = pd.read_json(p, lines=True)
        else:
            try:
                df = pd.read_parquet(p, engine="pyarrow")
            except ImportError:
                # pyarrow missing → try the jsonl sibling if it exists
                alt = p.with_suffix(".jsonl")
                if alt.exists():
                    df = pd.read_json(alt, lines=True)
                else:
                    raise
        store = LineageStore()
        for _, row in df.iterrows():
            d = row.to_dict()
            d["conflicts_ignored"] = list(d.get("conflicts_ignored") or [])
            d["warnings"] = list(d.get("warnings") or [])
            store._records.append(LineageRecord(**d))
        return store


# ---------- Helpers ----------


def build_rule_id(table: str, field_name: str, source_slug: str, priority: int) -> str:
    """Canonical rule_id used across engines. Keep in sync with YAML keys."""
    return f"{table}.{field_name}.{source_slug}#{priority}"


def conflict_dict(source: str, value: Any, reason: str) -> dict:
    """Build a conflict entry with a consistent shape."""
    return {
        "source": source,
        "value": value if isinstance(value, (str, int, float, bool)) or value is None else repr(value),
        "reason": reason,
    }

"""Orchestration: read uploaded files, detect, map, normalize, merge, output.

This module is the 'brain' of the tool. It is split from the UI so that we
can keep the Streamlit app small and easy to read.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from . import normalizers as norm
from .detectors import DetectedFile, detect
from .partners import resolve_partner_id
from .schemas import SCHEMAS, header_for, mandatory_fields_for


# ===================== Data loading =====================

# Keywords that identify a "real" header row. If any cell in a row matches
# (case-insensitive, ignoring spaces), we treat that row as the header.
_HEADER_KEYWORDS = (
    "immat", "plaque", "chassis", "châssis", "marque", "modele", "modèle",
    "version", "energie", "énergie", "gamme", "mec", "co2", "co²", "cv",
    "genre", "vin",
)


def _find_header_row(rows: list[list]) -> int:
    """Find the index of the most likely header row in a list of rows.

    Heuristic: the first row with ≥ 3 non-empty cells AND at least one
    cell whose normalized text contains a header keyword.
    """
    for i, row in enumerate(rows[:20]):  # scan first 20 rows only
        non_empty = [c for c in row if c is not None and str(c).strip() != ""]
        if len(non_empty) < 3:
            continue
        joined = " | ".join(str(c).lower() for c in non_empty)
        if any(kw in joined for kw in _HEADER_KEYWORDS):
            return i
    return 0  # fallback: first row


def _clean_str(s: str) -> str:
    """Normalize a string: strip + replace non-breaking spaces with regular spaces."""
    return s.replace("\xa0", " ").strip()


def _normalize_headers_and_values(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and string values.

    - Strip leading/trailing whitespace on column names and values.
    - Replace non-breaking spaces (\\xa0) with regular spaces (lessor exports
      occasionally sneak these in, which breaks exact column matching).
    """
    df = df.rename(columns={c: _clean_str(str(c)) for c in df.columns})
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(lambda v: _clean_str(v) if isinstance(v, str) else v)
    return df


def load_tabular(file) -> list[tuple[str, pd.DataFrame]]:
    """Load a CSV or XLSX uploaded file. Returns a list of (sheet_name, df).

    For CSVs the list has one element with sheet_name = ''.
    For XLSX we load every sheet (the user can ignore cover sheets later).

    For XLSX, auto-detects the real header row (handling files like Ayvens
    Etat de parc that have a logo + metadata block before the data table).
    Also strips leading/trailing whitespace from column names and string
    values (which are common in lessor exports).
    """
    name = getattr(file, "name", None) or "uploaded"
    lower = name.lower()
    if lower.endswith(".csv"):
        # Try common encodings / delimiters.
        raw = file.read() if hasattr(file, "read") else open(file, "rb").read()
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
            for sep in (",", ";", "\t"):
                try:
                    df = pd.read_csv(io.BytesIO(raw), encoding=enc, sep=sep, dtype=str, keep_default_na=False)
                    if df.shape[1] > 1:
                        return [("", _normalize_headers_and_values(df))]
                except Exception:
                    continue
        # Last resort.
        df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, engine="python")
        return [("", _normalize_headers_and_values(df))]
    if lower.endswith((".xlsx", ".xls", ".xlsm")):
        raw = file.read() if hasattr(file, "read") else open(file, "rb").read()
        xls = pd.ExcelFile(io.BytesIO(raw))
        out = []
        for sheet in xls.sheet_names:
            # Read without header first to detect where the real headers sit
            preview = xls.parse(sheet, header=None, dtype=str, nrows=25)
            if preview.empty:
                continue
            rows = preview.values.tolist()
            header_row = _find_header_row(rows)
            # Re-read with the detected header row as header
            df = xls.parse(sheet, header=header_row, dtype=str)
            df = df.fillna("")
            df = _normalize_headers_and_values(df)
            out.append((sheet, df))
        return out
    raise ValueError(f"Format non supporté: {name}")


# ===================== Container =====================

@dataclass
class SourceFile:
    """A single uploaded file + its detected type + user-confirmed mapping."""
    key: str                          # unique key used in st.session_state
    filename: str
    sheet_name: str
    df_raw: pd.DataFrame              # raw as loaded
    detected: DetectedFile
    target_schema: Optional[str] = None   # which Revio schema this file feeds (chosen by user)
    mapping: dict = field(default_factory=dict)  # {revio_field: source_column_or_None}
    selected: bool = True             # user can unselect a file (e.g. TVS sheet not useful)

    @property
    def df(self) -> pd.DataFrame:
        """Return the dataframe with the right header row applied."""
        if self.detected.header_row and self.detected.header_row > 0:
            df = self.df_raw.iloc[self.detected.header_row + 1 :].copy()
            df.columns = [str(c) for c in self.df_raw.iloc[self.detected.header_row].values]
            df = df.reset_index(drop=True)
            # Drop empty columns created by the template cover sheet.
            df = df.loc[:, [c for c in df.columns if str(c).strip()]]
            return df
        return self.df_raw


# ===================== Apply mapping + normalize =====================

DATE_FIELDS = {
    "vehicle": {"parcEntryAt", "registrationIssueDate"},
    "driver": {"birthDate", "licenseIssueDate", "licenseExpiryDate", "assignFrom", "assignTo"},
    "contract": {"startDate", "endDate"},
    "asset": {"expireAt", "assignFrom", "assignTo"},
}
PLATE_FIELDS = {
    "vehicle": {"registrationPlate"},
    "driver": {"assignPlate"},
    "contract": {"plate"},
    "asset": {"assignPlate"},
}
AMOUNT_FIELDS = {
    "contract": {
        "extraKmPrice", "vehicleValue", "batteryValue", "civilLiabilityPrice",
        "legalProtectionPrice", "theftFireAndGlassPrice", "allRisksPrice",
        "financialLossPrice", "maintenancePrice", "replacementVehiclePrice",
        "tiresPrice", "gasCardPrice", "tollCardPrice", "totalPrice",
    },
}
KM_FIELDS = {
    "contract": {"contractedMileage", "maxMileage"},
}
CIVILITY_FIELDS = {"driver": {"civility"}}
COUNTRY_FIELDS = {
    "vehicle": {"registrationIssueCountryCode"},
    "driver": {"licenseIssueCountryCode", "countryCode", "registrationIssueCountryCode"},
    "contract": {"plateCountry"},
    "asset": {"registrationIssueCountryCode"},
}


def apply_mapping(sf: SourceFile) -> tuple[pd.DataFrame, list[str]]:
    """Build a dataframe in the target schema from a SourceFile + its mapping.

    Returns (df_in_target_schema, warnings).
    """
    warnings: list[str] = []
    if not sf.target_schema:
        return pd.DataFrame(), [f"{sf.filename}: schéma cible non choisi"]

    schema_name = sf.target_schema
    target_cols = [c for c in header_for(schema_name) if c]  # drop empty placeholder
    out = pd.DataFrame(index=sf.df.index, columns=target_cols)

    for target_col in target_cols:
        source_col = sf.mapping.get(target_col)
        if not source_col:
            continue
        if source_col not in sf.df.columns:
            warnings.append(f"{sf.filename}: colonne source {source_col!r} absente")
            continue
        out[target_col] = sf.df[source_col].astype(str).str.strip()

    # Now run deterministic normalization on typed fields.
    for col in out.columns:
        if col in DATE_FIELDS.get(schema_name, set()):
            out[col] = out[col].apply(lambda v: norm.normalize_date(v)[0] or "")
        elif col in PLATE_FIELDS.get(schema_name, set()):
            out[col] = out[col].apply(lambda v: norm.normalize_plate(v)[0] or "")
        elif col in AMOUNT_FIELDS.get(schema_name, set()):
            out[col] = out[col].apply(
                lambda v: (
                    f"{norm.normalize_amount(v)[0]:.2f}".replace(".", ",")
                    if norm.normalize_amount(v)[0] is not None
                    else ""
                )
            )
        elif col in KM_FIELDS.get(schema_name, set()):
            out[col] = out[col].apply(lambda v: str(norm.normalize_km(v)[0] or ""))
        elif col in CIVILITY_FIELDS.get(schema_name, set()):
            out[col] = out[col].apply(lambda v: norm.normalize_civility(v)[0] or "")
        elif col in COUNTRY_FIELDS.get(schema_name, set()):
            out[col] = out[col].apply(lambda v: norm.normalize_country_code(v)[0])

    return out, warnings


# ===================== Merge multiple sources per schema =====================

def merge_per_schema(
    sources: list[SourceFile], priority_order: list[str]
) -> dict[str, pd.DataFrame]:
    """Merge all source files that feed a given schema.

    priority_order is a list of source_type keys (e.g. ["api_plaques",
    "ayvens_etat_parc", "arval_uat", "client_vehicle"]); higher priority wins
    on conflict. The join key is the normalized plate for vehicle/contract/
    asset, or (firstName, lastName) for drivers.
    """
    merged: dict[str, pd.DataFrame] = {}
    by_schema: dict[str, list[SourceFile]] = {"vehicle": [], "driver": [], "contract": [], "asset": []}
    for sf in sources:
        if not sf.selected or not sf.target_schema:
            continue
        by_schema[sf.target_schema].append(sf)

    for schema_name, files in by_schema.items():
        if not files:
            continue
        # Sort by priority (lower index = higher priority).
        def score(sf: SourceFile) -> int:
            try:
                return priority_order.index(sf.detected.source_type)
            except ValueError:
                return len(priority_order)
        files.sort(key=score)

        # Build a combined df - later files fill missing cells only.
        combined: Optional[pd.DataFrame] = None
        for sf in files:
            df, _ = apply_mapping(sf)
            if df.empty:
                continue
            if schema_name in ("vehicle", "contract", "asset") and "registrationPlate" in df.columns:
                df["_key"] = df["registrationPlate"].apply(lambda v: norm.plate_for_matching(v) or "")
            elif schema_name == "contract" and "plate" in df.columns:
                df["_key"] = df["plate"].apply(lambda v: norm.plate_for_matching(v) or "")
            elif schema_name == "driver":
                fn = df["firstName"].fillna("").astype(str).str.strip().str.lower()
                ln = df["lastName"].fillna("").astype(str).str.strip().str.lower()
                df["_key"] = fn + "|" + ln
            else:
                df["_key"] = range(len(df))
            if combined is None:
                combined = df.copy()
            else:
                combined = _fill_missing(combined, df, on="_key")
        if combined is not None:
            combined = combined.drop(columns=[c for c in combined.columns if c.startswith("_")], errors="ignore")
            merged[schema_name] = combined
    return merged


def _fill_missing(base: pd.DataFrame, incoming: pd.DataFrame, on: str) -> pd.DataFrame:
    """Left-join fill: fill empty cells of base with values from incoming.

    New rows in incoming (not in base) are appended.
    """
    base = base.copy()
    incoming = incoming.copy()
    # Align columns.
    for c in incoming.columns:
        if c not in base.columns:
            base[c] = ""
    for c in base.columns:
        if c not in incoming.columns:
            incoming[c] = ""
    # Fill missing cells.
    idx_base = base.set_index(on)
    idx_in = incoming.set_index(on)
    common = idx_base.index.intersection(idx_in.index)
    for key in common:
        for col in idx_base.columns:
            if col == on:
                continue
            val_base = idx_base.at[key, col] if key in idx_base.index else ""
            val_in = idx_in.at[key, col] if key in idx_in.index else ""
            if (val_base is None or str(val_base).strip() == "") and val_in:
                idx_base.at[key, col] = val_in
    base = idx_base.reset_index()
    # Append new keys.
    new_keys = idx_in.index.difference(idx_base.index)
    if len(new_keys):
        to_add = incoming[incoming[on].isin(new_keys)]
        base = pd.concat([base, to_add], ignore_index=True)
    return base


# ===================== Validation =====================

@dataclass
class ValidationIssue:
    schema: str
    row_index: int
    plate: str
    field: str
    level: str  # 'error' | 'warning'
    message: str


def validate(outputs: dict[str, pd.DataFrame]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for schema_name, df in outputs.items():
        mandatory = mandatory_fields_for(schema_name)
        for idx, row in df.iterrows():
            plate_col = (
                "registrationPlate"
                if "registrationPlate" in df.columns
                else ("plate" if "plate" in df.columns else ("assignPlate" if "assignPlate" in df.columns else None))
            )
            plate = str(row[plate_col]) if plate_col and plate_col in df.columns else ""
            for f in mandatory:
                if f not in df.columns:
                    continue
                val = row[f]
                if val is None or pd.isna(val) or str(val).strip() == "":
                    issues.append(
                        ValidationIssue(
                            schema=schema_name,
                            row_index=int(idx),
                            plate=plate,
                            field=f,
                            level="error",
                            message=f"Champ obligatoire manquant: {f}",
                        )
                    )
    return issues


# ===================== Multi-file merge (engine mode) =====================


@dataclass
class MergedSource:
    """Result of merging 1..N uploaded files that all resolve to the same slug.

    Used by the engine page to group files by slug and produce a single
    concatenated DataFrame per slug (ready to feed into the rules engine).

    The concatenated DataFrame always carries a `__source_file` column for
    traceability — even when a single file resolved to this slug — so the
    downstream Excel report can say "this cell came from alphabet.xlsx".
    """
    slug: str
    files: list[str]            # human labels, e.g. ["alphabet.xlsx", "leasys.xlsx [feuil1]"]
    df: pd.DataFrame            # concatenated, with __source_file column
    n_rows_before_dedup: int    # total rows before the engine's plate dedup kicks in


def merge_engine_sources(
    engine_files: dict[str, dict],
) -> dict[str, MergedSource]:
    """Group engine_files by current slug and concat files of the same slug.

    `engine_files` is the dict produced by the Moteur upload step:
    ```
    {
      "file.xlsx::sheet": {
        "df": DataFrame, "filename": "file.xlsx", "sheet_name": "sheet",
        "slug": "autre_loueur_etat_parc", ...
      },
      ...
    }
    ```

    Returns `{slug: MergedSource}`. Each MergedSource has:
    - a concatenated DataFrame with a `__source_file` column
    - the list of source labels, in upload order
    - a row count, pre any engine-side dedup

    The engine's `_index_by_plate` drops duplicate plates keep-first — so the
    upload ORDER inside each slug is significant. This function preserves the
    original dict insertion order, which matches upload order.

    Single-file slugs go through the same code path (still get a
    `__source_file` column) so downstream code can rely on the column being
    present unconditionally.
    """
    # Group by current slug while preserving order.
    groups: dict[str, list[tuple[str, pd.DataFrame]]] = {}
    for key, info in engine_files.items():
        slug = info.get("slug")
        if not slug:
            continue
        label = info["filename"] + (f" [{info['sheet_name']}]" if info.get("sheet_name") else "")
        df = info.get("df")
        if df is None:
            continue
        groups.setdefault(slug, []).append((label, df))

    merged: dict[str, MergedSource] = {}
    for slug, items in groups.items():
        labels = [lbl for lbl, _ in items]
        if len(items) == 1:
            lbl, df = items[0]
            out = df.copy()
            out["__source_file"] = lbl
            merged[slug] = MergedSource(slug=slug, files=labels, df=out, n_rows_before_dedup=len(out))
            continue

        # Multi-file: concat with a __source_file marker per row.
        chunks: list[pd.DataFrame] = []
        for lbl, df in items:
            c = df.copy()
            c["__source_file"] = lbl
            chunks.append(c)
        # sort=False keeps column order stable (leftmost file wins for column ordering,
        # later files' extra columns are appended on the right).
        concat = pd.concat(chunks, ignore_index=True, sort=False)
        merged[slug] = MergedSource(
            slug=slug,
            files=labels,
            df=concat,
            n_rows_before_dedup=len(concat),
        )

    return merged

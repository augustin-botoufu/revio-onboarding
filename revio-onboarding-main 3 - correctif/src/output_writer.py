"""Build the final per-fleet Revio CSV bundle + error report."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from typing import Optional

import pandas as pd

from .pipeline import ValidationIssue
from .schemas import header_for


def _fleet_for_row(row: pd.Series, schema_name: str, fleet_mapping: dict[str, str]) -> str:
    """Return the fleet name a given output row belongs to.

    fleet_mapping maps raw agency code -> fleet name (e.g. 'IDF_IND' -> 'Île-de-France').
    """
    agency_cols = [
        "companyAnalyticalCode",  # driver
        "_agency",                # technical column if we tracked it
    ]
    for col in agency_cols:
        if col in row and row[col]:
            raw = str(row[col]).strip()
            if raw in fleet_mapping:
                return fleet_mapping[raw]
            return raw
    return "default"


def split_by_fleet(
    outputs: dict[str, pd.DataFrame], fleet_mapping: dict[str, str]
) -> dict[str, dict[str, pd.DataFrame]]:
    """Split each schema df into one df per fleet.

    Returns {fleet_name: {schema_name: df}}.
    """
    result: dict[str, dict[str, pd.DataFrame]] = {}
    for schema_name, df in outputs.items():
        if df.empty:
            continue
        df = df.copy()
        df["_fleet"] = df.apply(lambda r: _fleet_for_row(r, schema_name, fleet_mapping), axis=1)
        for fleet, sub in df.groupby("_fleet"):
            sub = sub.drop(columns=["_fleet"])
            result.setdefault(fleet, {})[schema_name] = sub.reset_index(drop=True)
    return result


def _csv_bytes(df: pd.DataFrame, schema_name: str) -> bytes:
    """Write a dataframe in the exact Revio header order.

    The Vehicle schema has an empty-named placeholder column in the middle
    (between co2gKm and registrationIssueCountryCode). We build the output
    column-by-column to stay faithful to the template.
    """
    header = header_for(schema_name)
    n_rows = len(df)
    out = pd.DataFrame(index=range(n_rows))
    empty_counter = 0
    rename_map = {}
    for col in header:
        if col == "":
            empty_counter += 1
            tech = f"__empty_{empty_counter}"
            out[tech] = ""
            rename_map[tech] = ""
        else:
            if col in df.columns:
                out[col] = df[col].values
            else:
                out[col] = ""
    # Produce CSV with technical header then patch first line to restore the
    # true (possibly empty) column names.
    buf = io.StringIO()
    out.to_csv(buf, index=False)
    lines = buf.getvalue().splitlines()
    lines[0] = ",".join(header)
    return ("\n".join(lines) + "\n").encode("utf-8-sig")


def _error_report_csv(issues: list[ValidationIssue]) -> bytes:
    rows = [
        {
            "schema": i.schema,
            "ligne": i.row_index,
            "plaque": i.plate,
            "champ": i.field,
            "niveau": i.level,
            "message": i.message,
        }
        for i in issues
    ]
    df = pd.DataFrame(rows, columns=["schema", "ligne", "plaque", "champ", "niveau", "message"])
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8-sig")


def build_zip(
    outputs_by_fleet: dict[str, dict[str, pd.DataFrame]],
    issues: list[ValidationIssue],
    client_name: str = "client",
) -> bytes:
    """Build a .zip with one folder per fleet, plus a top-level error report."""
    buf = io.BytesIO()
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fleet, schemas in outputs_by_fleet.items():
            for schema_name, df in schemas.items():
                path = f"{client_name}_{ts}/{fleet}/{schema_name}.csv"
                zf.writestr(path, _csv_bytes(df, schema_name))
        # Error report at the root.
        if issues:
            zf.writestr(f"{client_name}_{ts}/rapport_erreurs.csv", _error_report_csv(issues))
    return buf.getvalue()

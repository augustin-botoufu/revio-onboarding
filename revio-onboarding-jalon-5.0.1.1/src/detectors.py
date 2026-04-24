"""Auto-detect the type of an uploaded file.

Used before column mapping to decide which target schema the file feeds into
and to pick the right parsing strategy (some lessor files have header offsets).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class DetectedFile:
    filename: str
    # Source type (high-level): where the file comes from.
    # Values: "client_vehicle", "client_driver",
    #         "ayvens_etat_parc", "ayvens_aen", "ayvens_tvs", "ayvens_and",
    #         "arval_uat", "arval_aen", "arval_and", "arval_tvu",
    #         "api_plaques", "unknown".
    source_type: str
    # Which Revio schema(s) this file can feed.
    feeds: list[str]  # subset of {"vehicle", "driver", "contract", "asset"}
    confidence: float  # 0..1
    header_row: int  # 0-indexed row where headers live (for lessor files with cover sheets)
    sheet_name: Optional[str] = None  # for xlsx
    sample_headers: list[str] = None
    reason: str = ""


# Heuristic keyword sets used to identify sources.
# Keywords are matched case-insensitively against concatenated headers.
DETECTORS = [
    # ----- Revio templates (empty, just for completeness if user uploads them) -----
    {
        "source_type": "revio_template_vehicle",
        "feeds": ["vehicle"],
        "must_have": ["registrationplate", "parcentryat", "registrationvin"],
        "reason": "Template Revio Vehicle reconnu par les entêtes techniques.",
    },
    {
        "source_type": "revio_template_driver",
        "feeds": ["driver"],
        "must_have": ["firstname", "lastname", "licensenumber", "assignplate"],
        "reason": "Template Revio Driver reconnu.",
    },
    {
        "source_type": "revio_template_contract",
        "feeds": ["contract"],
        "must_have": ["partnerid", "contractedmileage", "durationmonths"],
        "reason": "Template Revio Contract reconnu.",
    },
    {
        "source_type": "revio_template_asset",
        "feeds": ["asset"],
        "must_have": ["partnerid", "kind", "identifier", "assignplate"],
        "reason": "Template Revio Asset reconnu.",
    },
    # ----- API Plaques -----
    {
        "source_type": "api_plaques",
        "feeds": ["vehicle"],
        "must_have": ["immatriculation", "vin", "marque", "modele"],
        "reason": "Sortie API Plaques reconnue (immatriculation + vin + marque + modele).",
    },
    # ----- Ayvens -----
    {
        "source_type": "ayvens_etat_parc",
        "feeds": ["vehicle", "contract"],
        "must_have": ["n° immat", "n° contrat", "date début contrat", "km contrat"],
        "any_of": ["structure 1", "nom conducteur", "loyer périodique"],
        "reason": "Etat de parc Ayvens identifié.",
    },
    {
        "source_type": "ayvens_aen",
        "feeds": ["contract"],
        "must_have": ["aen base loyer", "prix catalogue", "immatriculation"],
        "reason": "AEN Ayvens identifié.",
    },
    {
        "source_type": "ayvens_tvs",
        "feeds": [],
        "must_have": ["barème co2", "tarif air"],
        "reason": "TVS Ayvens identifié (usage fiscal, pas d'impact direct import Revio).",
    },
    {
        "source_type": "ayvens_and",
        "feeds": [],
        "must_have": ["loyer annuel", "loyer prorata", "durée 5 ans"],
        "reason": "AND Ayvens identifié (fiscal, non utilisé en import).",
    },
    # ----- Arval -----
    {
        "source_type": "arval_uat",
        "feeds": ["vehicle", "contract"],
        "must_have": ["plaque d'immatriculation", "numéro de contrat", "kilométrage"],
        "any_of": ["date de debut de contrat", "date de fin de contrat"],
        "reason": "Etat de parc Arval (UAT) identifié.",
    },
    {
        "source_type": "arval_aen",
        "feeds": ["contract"],
        "must_have": ["immat", "prix remisé", "aen"],
        "reason": "AEN Arval identifié.",
        "header_row_search": True,
    },
    {
        "source_type": "arval_and",
        "feeds": ["contract"],
        "must_have": ["plafond invest", "loyers non déductibles"],
        "reason": "AND Arval identifié.",
        "header_row_search": True,
    },
    {
        "source_type": "arval_tvu",
        "feeds": [],
        "must_have": ["taxe sur les émissions"],
        "reason": "TVU Arval identifié.",
        "header_row_search": True,
    },
    # ----- Client files (lowest confidence, heuristic) -----
    {
        "source_type": "client_driver",
        "feeds": ["driver"],
        "must_have": ["firstname", "lastname"],
        "any_of": ["licensenumber", "birthdate", "emailpro", "permis"],
        "reason": "Fichier driver client (template Revio à remplir).",
    },
    {
        "source_type": "client_vehicle",
        "feeds": ["vehicle", "driver", "contract", "asset"],
        "any_of_groups": [
            ["immatriculation", "immat", "plaque"],
        ],
        "must_have": [],
        "any_of": ["conducteur", "collaborateur", "agence", "leaser", "propriétaire", "loueur"],
        "reason": "Fichier véhicules client interne (format libre).",
    },
]


def _lower(s) -> str:
    return str(s).strip().lower() if s is not None else ""


def _headers_match(
    headers: list[str],
    must_have: list[str],
    any_of: list[str] = None,
    any_of_groups: list[list[str]] = None,
) -> bool:
    lowered = [_lower(h) for h in headers]
    joined = " | ".join(lowered)
    for kw in must_have:
        if not any(kw in h for h in lowered) and kw not in joined:
            return False
    if any_of_groups:
        for group in any_of_groups:
            if not any(kw in h for h in lowered for kw in group) and not any(kw in joined for kw in group):
                return False
    if any_of:
        return any(kw in h for h in lowered for kw in any_of) or any(kw in joined for kw in any_of)
    return True


def _find_header_row(df: pd.DataFrame, max_scan: int = 8) -> int:
    """Scan the first rows to find the one that looks like a header
    (many non-empty, mostly-text cells)."""
    best_row = 0
    best_score = -1
    for i in range(min(max_scan, len(df))):
        row = df.iloc[i]
        non_empty = row.notna().sum()
        text_cells = sum(1 for v in row if isinstance(v, str) and len(str(v).strip()) > 2)
        score = int(non_empty) + int(text_cells)
        if score > best_score:
            best_score = score
            best_row = i
    return best_row


def detect(filename: str, df: pd.DataFrame, sheet_name: Optional[str] = None) -> DetectedFile:
    """Detect the source_type of a DataFrame given its headers.

    Strategy:
    1. Try with row 0 as header.
    2. If no match, scan the first few rows for a better header candidate
       (useful for Arval exports with cover sheets).
    """
    headers = [str(c) for c in df.columns]
    for det in DETECTORS:
        if _headers_match(headers, det["must_have"], det.get("any_of"), det.get("any_of_groups")):
            return DetectedFile(
                filename=filename,
                source_type=det["source_type"],
                feeds=list(det["feeds"]),
                confidence=0.85,
                header_row=0,
                sheet_name=sheet_name,
                sample_headers=headers[:15],
                reason=det["reason"],
            )
    # Header offset search (Arval-style).
    header_row = _find_header_row(df)
    if header_row > 0:
        shifted_headers = [str(c) for c in df.iloc[header_row].values]
        for det in DETECTORS:
            if det.get("header_row_search") and _headers_match(
                shifted_headers, det["must_have"], det.get("any_of"), det.get("any_of_groups")
            ):
                return DetectedFile(
                    filename=filename,
                    source_type=det["source_type"],
                    feeds=list(det["feeds"]),
                    confidence=0.75,
                    header_row=header_row,
                    sheet_name=sheet_name,
                    sample_headers=shifted_headers[:15],
                    reason=det["reason"] + f" (entêtes trouvés à la ligne {header_row + 1})",
                )
    return DetectedFile(
        filename=filename,
        source_type="unknown",
        feeds=[],
        confidence=0.0,
        header_row=0,
        sheet_name=sheet_name,
        sample_headers=headers[:15],
        reason="Type de fichier non reconnu automatiquement - mapping IA requis.",
    )


SOURCE_TYPE_LABELS = {
    "revio_template_vehicle": "Template Revio Vehicle (vide)",
    "revio_template_driver": "Template Revio Driver (vide)",
    "revio_template_contract": "Template Revio Contract (vide)",
    "revio_template_asset": "Template Revio Asset (vide)",
    "api_plaques": "Export API Plaques (SIV)",
    "ayvens_etat_parc": "Ayvens - Etat de parc",
    "ayvens_aen": "Ayvens - AEN",
    "ayvens_tvs": "Ayvens - TVS (fiscal)",
    "ayvens_and": "Ayvens - AND (fiscal)",
    "arval_uat": "Arval - UAT / Etat de parc",
    "arval_aen": "Arval - AEN",
    "arval_and": "Arval - AND",
    "arval_tvu": "Arval - TVU (fiscal)",
    "client_vehicle": "Fichier véhicules client (interne)",
    "client_driver": "Fichier driver client (template Revio)",
    "unknown": "Non reconnu",
}


def label_for(source_type: str) -> str:
    return SOURCE_TYPE_LABELS.get(source_type, source_type)

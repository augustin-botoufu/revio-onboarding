"""LLM-powered column mapping.

Given a source DataFrame and a target Revio schema, ask Claude to propose
how each source column should map to a Revio target field. The user then
reviews the mapping in the UI before anything is written.
"""

from __future__ import annotations

import json
import os
from typing import Optional

import pandas as pd

try:
    from anthropic import Anthropic
except ImportError:  # anthropic is optional - UI can still run without mapping
    Anthropic = None  # type: ignore

from .schemas import SCHEMAS, FieldSpec


SYSTEM_PROMPT = """Tu es un assistant spécialisé dans la cartographie de colonnes
entre fichiers de gestion de flotte automobile.

Tu reçois:
- La liste des colonnes d'un fichier source (client ou loueur)
- 3 lignes d'exemple de ce fichier
- La liste des champs d'un template Revio cible avec leur description et format attendu
- Éventuellement des instructions spéciales de l'utilisateur

Tu dois proposer, pour chaque champ du template Revio, la colonne source qui correspond
le mieux - ou None si aucune colonne ne convient.

IMPORTANT:
- Tu réponds UNIQUEMENT en JSON, sans texte avant ou après.
- Le JSON est un objet avec une clé "mapping" (dict champ_revio -> nom_colonne_source ou null),
  et une clé "notes" (liste courte de remarques sur des ambiguïtés notables).
- Si plusieurs colonnes sources pourraient convenir, tu choisis la plus spécifique.
- Si aucune colonne source ne correspond, mets null (pas "N/A" ni chaîne vide).
- Tu tiens compte des instructions spéciales si elles contredisent les évidences.
"""


def build_user_message(
    source_columns: list[str],
    sample_rows: list[dict],
    target_schema_name: str,
    target_fields: list[FieldSpec],
    user_instructions: str = "",
) -> str:
    target_desc = []
    for f in target_fields:
        if not f.name:
            continue
        desc = {
            "field": f.name,
            "mandatory": f.mandatory,
            "description": f.description or "",
            "allowed_values": f.allowed_values,
            "format": f.format_hint,
        }
        target_desc.append(desc)
    payload = {
        "source_columns": source_columns,
        "sample_rows": sample_rows[:3],
        "target_schema": target_schema_name,
        "target_fields": target_desc,
        "user_instructions": user_instructions.strip() or "aucune",
    }
    return (
        "Voici les données à analyser:\n\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        + "\n\nRéponds en JSON avec les clés 'mapping' et 'notes'."
    )


def propose_mapping(
    df: pd.DataFrame,
    target_schema_name: str,
    user_instructions: str = "",
    api_key: Optional[str] = None,
    model: str = "claude-sonnet-4-5",
) -> dict:
    """Return a dict {target_field: source_column_or_None, "_notes": [...]} ."""
    if Anthropic is None:
        return {"_error": "SDK Anthropic non installé (pip install anthropic).", "_notes": []}
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"_error": "Clé ANTHROPIC_API_KEY manquante.", "_notes": []}

    target_fields = SCHEMAS.get(target_schema_name, [])
    if not target_fields:
        return {"_error": f"Schéma inconnu: {target_schema_name}", "_notes": []}

    # Prepare sample.
    df_sample = df.head(3).fillna("").astype(str)
    sample_rows = df_sample.to_dict(orient="records")
    source_columns = [str(c) for c in df.columns]

    client = Anthropic(api_key=api_key)
    user_msg = build_user_message(
        source_columns=source_columns,
        sample_rows=sample_rows,
        target_schema_name=target_schema_name,
        target_fields=target_fields,
        user_instructions=user_instructions,
    )
    try:
        resp = client.messages.create(
            model=model,
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        # Defensive parsing: some models wrap JSON in code fences.
        raw = "".join(block.text for block in resp.content if hasattr(block, "text"))
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:].strip()
        parsed = json.loads(raw)
        mapping = parsed.get("mapping", {})
        notes = parsed.get("notes", [])
        # Normalize the None representations.
        for k, v in list(mapping.items()):
            if v in ("", "null", "None", "N/A", "n/a"):
                mapping[k] = None
        return {"mapping": mapping, "_notes": notes}
    except Exception as e:
        return {"_error": f"Erreur LLM: {e}", "_notes": []}

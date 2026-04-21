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
le mieux - ou null si aucune colonne ne convient.

IMPORTANT:
- Tu dois OBLIGATOIREMENT appeler l'outil `propose_column_mapping` avec les arguments
  `mapping` (dict) et `notes` (liste).
- Le dict `mapping` doit contenir UNE ENTRÉE PAR CHAMP REVIO, avec comme valeur :
    * soit le nom EXACT d'une colonne source (respect strict de la casse et des espaces),
    * soit null si aucune colonne source ne convient.
- Si plusieurs colonnes sources pourraient convenir, tu choisis la plus spécifique.
- `notes` est une liste courte de remarques sur des ambiguïtés ou des transformations
  nécessaires. Ne répète pas dedans le mapping lui-même - le mapping est dans `mapping`.
- Tu tiens compte des instructions spéciales si elles contredisent les évidences.
"""


MAPPING_TOOL = {
    "name": "propose_column_mapping",
    "description": (
        "Enregistre le mapping proposé entre les colonnes source et les champs Revio. "
        "Chaque champ Revio doit être une clé du dict `mapping`, même si la valeur est null."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mapping": {
                "type": "object",
                "description": (
                    "Dictionnaire {champ_revio: nom_colonne_source_ou_null}. "
                    "La valeur DOIT être une chaîne exacte correspondant à une colonne "
                    "source fournie, ou null (pas 'N/A' ni ''). "
                    "TOUS les champs Revio doivent apparaître comme clés."
                ),
                "additionalProperties": {"type": ["string", "null"]},
            },
            "notes": {
                "type": "array",
                "description": (
                    "Remarques courtes sur les ambiguïtés ou les transformations à prévoir. "
                    "Ne pas redécrire le mapping - il est déjà dans `mapping`."
                ),
                "items": {"type": "string"},
            },
        },
        "required": ["mapping", "notes"],
    },
}


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
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=[MAPPING_TOOL],
            tool_choice={"type": "tool", "name": "propose_column_mapping"},
            messages=[{"role": "user", "content": user_msg}],
        )
        # Extract the tool_use block.
        mapping: dict = {}
        notes: list = []
        raw_text = ""
        for block in resp.content:
            if getattr(block, "type", None) == "tool_use":
                inp = block.input or {}
                mapping = dict(inp.get("mapping", {}))
                notes = list(inp.get("notes", []))
                break
            if hasattr(block, "text"):
                raw_text += block.text
        # Fallback: if tool_use wasn't triggered, try to parse any JSON in the text.
        if not mapping and raw_text:
            try:
                txt = raw_text.strip()
                if txt.startswith("```"):
                    txt = txt.strip("`")
                    if txt.lower().startswith("json"):
                        txt = txt[4:].strip()
                parsed = json.loads(txt)
                mapping = dict(parsed.get("mapping", {}))
                notes = list(parsed.get("notes", []))
            except Exception:
                pass

        # Normalize null-ish values.
        for k, v in list(mapping.items()):
            if v in ("", "null", "None", "N/A", "n/a"):
                mapping[k] = None

        # Fuzzy-match values to actual source columns (case/whitespace-insensitive).
        norm_src = {str(c).strip().lower(): str(c) for c in df.columns}
        fixed: dict = {}
        for k, v in mapping.items():
            if v is None:
                fixed[k] = None
                continue
            v_str = str(v).strip()
            if v_str in [str(c) for c in df.columns]:
                fixed[k] = v_str  # exact match
            else:
                key = v_str.lower()
                fixed[k] = norm_src.get(key)  # None if no match
        mapping = fixed

        # Surface a debug payload so the UI can display it on failure.
        debug = {
            "raw_text_preview": raw_text[:500] if raw_text else "",
            "nb_mapped_non_null": sum(1 for v in mapping.values() if v),
        }
        return {"mapping": mapping, "_notes": notes, "_debug": debug}
    except Exception as e:
        return {"_error": f"Erreur LLM: {e}", "_notes": []}

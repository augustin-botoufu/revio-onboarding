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


# =============================================================================
# Jalon 2.7 — Value-level mapping (enum fields)
# =============================================================================

ENUM_SYSTEM_PROMPT = """Tu es un assistant qui normalise des valeurs de gestion de flotte automobile
vers des enums fixes.

Tu reçois:
- Un schéma (vehicle / driver / contract / asset) et un champ précis
- La liste EXACTE des valeurs autorisées pour ce champ (il n'y en a pas d'autres)
- Une description courte du champ et, si pertinent, une note métier
- Une liste de valeurs brutes (telles qu'elles apparaissent dans un fichier client ou loueur)

Tu dois produire, pour chaque valeur brute, la valeur autorisée qui correspond — ou null si
aucune ne convient (valeur inintelligible, erronée, hors scope).

Règles:
- Tu réponds en appelant l'outil `propose_value_mappings`.
- Le dictionnaire `mappings` contient UNE ENTRÉE PAR VALEUR BRUTE, avec comme valeur soit
  une des valeurs autorisées (chaîne EXACTE, respect strict de la casse), soit null.
- Tu n'inventes PAS de valeurs qui ne sont pas dans `allowed_values`.
- Pour les cas fréquents :
    * "VP"/"particulier"/"personnel"/"privé" → usage private
    * "VU"/"utilitaire"/"fourgon" → usage utility
    * "fonction"/"service"/"pool" → usage service
    * "Diesel"/"Gazole"/"GO" → motorisation diesel
    * "Essence"/"SP95"/"SP98"/"GPL" → motorisation gas
    * "Hybride"/"PHEV"/"HEV" → motorisation hybrid
    * "Hybride-diesel" ou "Hybride-essence" → motorisation hybrid (c'est une voiture
      hybride qui met du diesel/essence ; JAMAIS diesel ni gas)
    * "Électrique"/"EV"/"Hydrogène" → motorisation electric
    * "Oui"/"Yes"/"1"/"Vrai"/"X" → TRUE ; "Non"/"No"/"0"/"Faux" → FALSE
- Pour les notes : indique brièvement les cas ambigus ou les valeurs que tu as mappées null.
"""


ENUM_TOOL = {
    "name": "propose_value_mappings",
    "description": (
        "Enregistre, pour chaque valeur brute reçue, la valeur autorisée correspondante. "
        "Le dict `mappings` DOIT contenir toutes les valeurs brutes en clés. La valeur "
        "associée est soit une chaîne exacte dans `allowed_values`, soit null."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "object",
                "description": (
                    "{valeur_brute: valeur_revio_ou_null}. Chaque clé est une valeur brute "
                    "reçue, chaque valeur est soit un élément de allowed_values, soit null."
                ),
                "additionalProperties": {"type": ["string", "null"]},
            },
            "notes": {
                "type": "array",
                "description": "Remarques courtes sur les mappings ambigus ou null.",
                "items": {"type": "string"},
            },
        },
        "required": ["mappings", "notes"],
    },
}


# Hard guard-rail: beyond this many unique unknown values in a single batch we
# refuse to call the LLM and return an error. Keeps cost + latency bounded in
# case an upstream bug dumps thousands of garbage values into a cell.
ENUM_MAX_BATCH = 100


def build_enum_user_message(
    schema_name: str,
    field_name: str,
    allowed_values: list[str],
    raw_values: list[str],
    field_description: str = "",
    field_note: str = "",
) -> str:
    payload = {
        "schema": schema_name,
        "field": field_name,
        "allowed_values": list(allowed_values),
        "description": field_description,
        "note": field_note,
        "raw_values": list(raw_values),
    }
    return (
        "Valeurs à normaliser :\n\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        + "\n\nAppelle `propose_value_mappings` avec une clé par valeur brute."
    )


def propose_enum_mappings(
    schema_name: str,
    field_name: str,
    allowed_values: list[str],
    raw_values: list[str],
    *,
    field_description: str = "",
    field_note: str = "",
    api_key: Optional[str] = None,
    model: str = "claude-sonnet-4-5",
) -> dict:
    """Ask the LLM to map a batch of raw values to ``allowed_values``.

    Returns a dict shaped like::

        {
            "mappings": {raw_value: target_or_None, ...},
            "_notes": [...],
            "_debug": {...},
        }

    On any error returns ``{"_error": "...", "_notes": []}``. Callers should
    treat missing keys as "LLM didn't answer — keep the raw, flag unresolved".
    """
    if Anthropic is None:
        return {"_error": "SDK Anthropic non installé (pip install anthropic).", "_notes": []}
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"_error": "Clé ANTHROPIC_API_KEY manquante.", "_notes": []}
    if not allowed_values:
        return {"_error": f"{schema_name}.{field_name} n'a pas d'allowed_values.", "_notes": []}
    # Dedupe while preserving first-seen order (useful when looking at debug).
    seen: set[str] = set()
    unique_raws: list[str] = []
    for r in raw_values:
        if r is None:
            continue
        s = str(r)
        if s and s not in seen:
            seen.add(s)
            unique_raws.append(s)
    if not unique_raws:
        return {"mappings": {}, "_notes": [], "_debug": {"batch_size": 0}}
    if len(unique_raws) > ENUM_MAX_BATCH:
        return {
            "_error": (
                f"Trop de valeurs à mapper en un seul batch ({len(unique_raws)} > "
                f"{ENUM_MAX_BATCH}). Découpe le fichier ou revois le mapping de "
                f"colonne : on ne demande pas à l'IA de classer cette quantité."
            ),
            "_notes": [],
        }

    client = Anthropic(api_key=api_key)
    user_msg = build_enum_user_message(
        schema_name=schema_name,
        field_name=field_name,
        allowed_values=allowed_values,
        raw_values=unique_raws,
        field_description=field_description,
        field_note=field_note,
    )
    try:
        resp = client.messages.create(
            model=model,
            max_tokens=2048,
            system=ENUM_SYSTEM_PROMPT,
            tools=[ENUM_TOOL],
            tool_choice={"type": "tool", "name": "propose_value_mappings"},
            messages=[{"role": "user", "content": user_msg}],
        )
        mappings: dict = {}
        notes: list = []
        raw_text = ""
        for block in resp.content:
            if getattr(block, "type", None) == "tool_use":
                inp = block.input or {}
                mappings = dict(inp.get("mappings", {}))
                notes = list(inp.get("notes", []))
                break
            if hasattr(block, "text"):
                raw_text += block.text
        # Fallback: scrape JSON out of the text block if tool_use wasn't used.
        if not mappings and raw_text:
            try:
                txt = raw_text.strip()
                if txt.startswith("```"):
                    txt = txt.strip("`")
                    if txt.lower().startswith("json"):
                        txt = txt[4:].strip()
                parsed = json.loads(txt)
                mappings = dict(parsed.get("mappings", {}))
                notes = list(parsed.get("notes", []))
            except Exception:
                pass

        allowed_set = set(allowed_values)
        # Normalize null-ish values and drop outputs that aren't in allowed_values.
        cleaned: dict = {}
        for raw in unique_raws:
            proposed = mappings.get(raw)
            if proposed in ("", "null", "None", "N/A", "n/a", None):
                cleaned[raw] = None
                continue
            s = str(proposed).strip()
            # Exact match first (safe, strict).
            if s in allowed_set:
                cleaned[raw] = s
                continue
            # Case-insensitive fuzzy fallback — rescues "Private" → "private".
            low = {a.lower(): a for a in allowed_set}
            cleaned[raw] = low.get(s.lower())

        debug = {
            "batch_size": len(unique_raws),
            "model": model,
            "nb_mapped": sum(1 for v in cleaned.values() if v),
            "raw_text_preview": raw_text[:300] if raw_text else "",
        }
        return {"mappings": cleaned, "_notes": notes, "_debug": debug}
    except Exception as e:
        return {"_error": f"Erreur LLM: {e}", "_notes": []}

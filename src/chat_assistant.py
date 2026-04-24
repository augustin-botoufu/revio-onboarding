"""Jalon 5.0.1 — In-app LLM chat assistant (AM mode).

Conversational Q&A over the current import session. Lets an Account Manager
ask plain-language questions like:

    · "Pourquoi le véhicule FR-123-AB a brand = 'Peugeot' ?"
    · "Quels sont les warnings côté contrats ?"
    · "C'est quoi le slug `ayvens_etat_parc` ?"

The assistant has READ-ONLY access to the session via 4 tools:
    · get_session_state()            → files chargés + compte lignes/warnings
    · list_plates(table?, limit?)    → échantillon de plaques dispos
    · list_fields(table?)            → champs tracés dans le lineage
    · get_lineage(plate, field?, …)  → provenance d'une cellule (fuzzy match)

Scope of 5.0.1: AM mode only, no YAML patches, no GitHub commit. The dev
mode (propose_yaml_patch) lands in 5.0.2, the signaler button in 5.0.3.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import asdict
from typing import Any, Optional

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None  # type: ignore


DEFAULT_MODEL = "claude-sonnet-4-5"
DEFAULT_MAX_TOKENS = 2048
DEFAULT_MAX_LOOPS = 8  # hard cap on tool-use iterations per user turn


# =============================================================================
# Plate normalization helpers — same spirit as normalizers.plate_for_matching
# =============================================================================


_PLATE_ALNUM_RE = re.compile(r"[^A-Z0-9]")


def _plate_key(raw: Any) -> str:
    """Uppercase + alphanumeric-only version of a plate for matching."""
    if raw is None:
        return ""
    return _PLATE_ALNUM_RE.sub("", str(raw).strip().upper())


# =============================================================================
# System prompt — AM mode
# =============================================================================

SYSTEM_PROMPT_AM = """Tu es l'assistant Revio. Tu aides des Account Managers
(non-tech) qui importent des fichiers de flotte automobile dans Revio.

Ton rôle :
- Répondre en FRANÇAIS, de façon concise et claire (pas de jargon inutile).
- Expliquer d'où vient une valeur dans les résultats ("pourquoi ce véhicule
  a cette marque ?"), en citant toujours la source (fichier, slug, colonne,
  règle YAML).
- Détailler le "chemin de décision" du moteur de règles : candidats
  considérés, priorités, conflits, tie-breaks, transformations appliquées.
- Expliquer les warnings et les champs manquants en langage humain.

OUTILS disponibles (read-only) :

1. `get_session_state` — fichiers chargés, slugs détectés, comptes de lignes
   en sortie Vehicle/Contract, nombre de warnings. APPELLE CET OUTIL EN
   PREMIER si l'utilisateur pose une question générale sur la session.

2. `list_plates` — liste les plaques effectivement présentes dans le lineage
   (vehicle et/ou contract). UTILISE-LE DÈS QUE L'UTILISATEUR MENTIONNE UNE
   PLAQUE : comme ça tu vérifies son existence avant de lancer get_lineage,
   et tu peux proposer une plaque proche si la saisie est ambiguë.

3. `list_fields` — liste les champs tracés dans le lineage avec leur
   fréquence. UTILISE-LE si l'utilisateur te donne un nom de champ qui
   pourrait être approximatif (typo, casse, pluriel) pour trouver la bonne
   orthographe avant de lancer get_lineage.

4. `get_lineage(plate, field?, table?)` — trace de provenance d'une
   cellule. Matching tolérant : la casse et les tirets sont ignorés, donc
   "ab123cd", "AB-123-CD" et "ab-123-cd" trouvent le même véhicule.
   Retourne la valeur finale, la source gagnante, la règle YAML, la
   transform, ET la liste des candidats écartés avec la raison du rejet.

RÈGLES STRICTES :

- **Explore avant d'interroger** : si la plaque que l'user te donne ne
  matche rien, NE LUI DEMANDE PAS de deviner la bonne orthographe.
  Appelle `list_plates` avec un limit large, trouve la plaque la plus
  proche (même prefix, 1-2 chars de diff), et PROPOSE-LA explicitement.
  Idem pour les noms de champs via `list_fields`.

- **Read-only strict** : tu ne peux PAS modifier les YAML de règles ni
  les fichiers mémorisés. Si l'utilisateur demande une modif, explique
  que c'est prévu en mode Dev (Jalon 5.0.2) et propose de signaler.

- **Zéro hallucination** : n'invente AUCUN chiffre, AUCUN nom de champ,
  AUCUNE plaque. Si tu n'as pas la donnée, appelle un tool. Si le tool
  retourne vide, dis-le clairement et propose la suite logique.

- **Concis** : 3-5 phrases max par réponse. Utilise des listes courtes
  pour les traces de lineage (candidat gagnant + candidats écartés).

- Si l'utilisateur est vague ("pourquoi c'est bizarre ?"), demande quel
  véhicule / plaque / champ précisément — MAIS propose-lui d'abord un
  échantillon de plaques et de champs (via list_plates + list_fields)
  pour qu'il te pointe du doigt un cas concret."""


# =============================================================================
# Tool schemas
# =============================================================================

TOOL_GET_SESSION_STATE = {
    "name": "get_session_state",
    "description": (
        "Retourne un résumé de l'état de la session d'import en cours : "
        "fichiers chargés (slug, nb lignes), résultats du moteur Vehicle et "
        "Contract (nb véhicules/contrats, warnings), page active, mappings "
        "utilisateur en vigueur. À appeler en début de conversation pour "
        "contextualiser."
    ),
    "input_schema": {
        "type": "object",
        "properties": {},
    },
}

TOOL_LIST_PLATES = {
    "name": "list_plates",
    "description": (
        "Liste les plaques effectivement présentes dans le lineage du "
        "moteur (vehicle et/ou contract). Utile pour (a) vérifier qu'une "
        "plaque existe avant d'appeler get_lineage, (b) proposer une "
        "plaque proche si l'utilisateur a fait une typo. Retourne un "
        "échantillon borné par `limit` + le compte total. Un paramètre "
        "`query` optionnel permet de filtrer en préfixe (casse et tirets "
        "ignorés)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "table": {
                "type": "string",
                "enum": ["vehicle", "contract", "both"],
                "description": "Table à inspecter. Par défaut : 'both'.",
            },
            "query": {
                "type": "string",
                "description": (
                    "Filtre optionnel : ne retourne que les plaques dont la "
                    "forme canonique (lettres/chiffres, uppercase) CONTIENT "
                    "cette chaîne. Ex: 'GT795' matchera 'GT-795-BP'."
                ),
            },
            "limit": {
                "type": "integer",
                "description": "Nombre max de plaques par table (défaut 50, max 500).",
            },
        },
    },
}

TOOL_LIST_FIELDS = {
    "name": "list_fields",
    "description": (
        "Liste les champs présents dans le lineage (vehicle et/ou "
        "contract), avec le nombre de cellules tracées par champ. Utile "
        "pour trouver la bonne orthographe d'un nom de champ avant de "
        "l'envoyer à get_lineage."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "table": {
                "type": "string",
                "enum": ["vehicle", "contract", "both"],
                "description": "Table à inspecter. Par défaut : 'both'.",
            },
        },
    },
}

TOOL_GET_LINEAGE = {
    "name": "get_lineage",
    "description": (
        "Retourne la trace de provenance pour une ou plusieurs cellules "
        "du résultat. Fournit : valeur finale, source gagnante, règle "
        "YAML, transform appliquée, ET liste des candidats écartés avec "
        "la raison. Matching tolérant : la casse et les tirets/espaces "
        "sont ignorés dans la plaque."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "plate": {
                "type": "string",
                "description": (
                    "Plaque d'immatriculation, ex: 'AB-123-CD' ou 'ab123cd'."
                ),
            },
            "field": {
                "type": "string",
                "description": (
                    "Nom du champ Revio à inspecter (ex: 'brand', "
                    "'vehicleValue', 'totalPrice'). Si absent, retourne "
                    "tous les champs tracés pour cette plaque."
                ),
            },
            "table": {
                "type": "string",
                "enum": ["vehicle", "contract"],
                "description": (
                    "Restreint à une seule table. Par défaut : cherche "
                    "dans les deux."
                ),
            },
        },
        "required": ["plate"],
    },
}

ALL_TOOLS = [
    TOOL_GET_SESSION_STATE,
    TOOL_LIST_PLATES,
    TOOL_LIST_FIELDS,
    TOOL_GET_LINEAGE,
]


# =============================================================================
# Internal helpers — walk the engine results
# =============================================================================


def _iter_lineage_records(session_ctx: dict, table_filter: Optional[str] = None):
    """Yield (table, LineageRecord) for each record across both engines."""
    targets: list[tuple[str, Any]] = []
    if table_filter in (None, "vehicle", "both"):
        targets.append(("vehicle", session_ctx.get("engine_result")))
    if table_filter in (None, "contract", "both"):
        targets.append(("contract", session_ctx.get("contract_result")))
    for table, res in targets:
        if not res:
            continue
        store = getattr(res, "lineage", None)
        if store is None:
            continue
        for r in getattr(store, "_records", []):
            yield table, r


# =============================================================================
# Tool implementations
# =============================================================================


def _dispatch_get_session_state(session_ctx: dict) -> dict:
    """Build a compact, JSON-serializable snapshot of the session."""
    files = []
    engine_files = session_ctx.get("engine_files") or {}
    for fk, info in engine_files.items():
        df = info.get("df")
        files.append({
            "file_key": fk,
            "filename": info.get("filename") or fk,
            "sheet": info.get("sheet_name"),
            "slug": info.get("slug"),
            "n_rows": int(len(df)) if df is not None else 0,
            "n_cols": int(len(df.columns)) if df is not None else 0,
        })

    def _summarize_result(res) -> Optional[dict]:
        if not res:
            return None
        df = getattr(res, "df", None)
        warnings = list(getattr(res, "warnings", []) or [])
        store = getattr(res, "lineage", None)
        n_lineage = len(getattr(store, "_records", [])) if store else 0
        out = {
            "n_rows": int(len(df)) if df is not None else 0,
            "n_warnings": len(warnings),
            "n_lineage_records": n_lineage,
            "warning_sample": warnings[:5],
        }
        if df is not None and len(df) and "plate" in df.columns:
            out["sample_plates"] = [
                str(p) for p in df["plate"].head(5).tolist() if p
            ]
        return out

    overrides = session_ctx.get("engine_overrides") or {}
    return {
        "current_page": session_ctx.get("current_mode") or "unknown",
        "client_name": session_ctx.get("client_name") or "",
        "user_instructions": session_ctx.get("user_instructions") or "",
        "files_loaded": files,
        "vehicle_result": _summarize_result(session_ctx.get("engine_result")),
        "contract_result": _summarize_result(session_ctx.get("contract_result")),
        "n_session_overrides": len(overrides),
    }


def _dispatch_list_plates(tool_input: dict, session_ctx: dict) -> dict:
    """Return a sample of plates keyed per table + total counts."""
    table = tool_input.get("table") or "both"
    query = _plate_key(tool_input.get("query") or "")
    limit = int(tool_input.get("limit") or 50)
    limit = max(1, min(limit, 500))

    per_table: dict[str, list[str]] = {"vehicle": [], "contract": []}
    seen_per_table: dict[str, set] = {"vehicle": set(), "contract": set()}

    for tbl, r in _iter_lineage_records(session_ctx, table):
        key = r.key or ""
        if query and query not in _plate_key(key):
            continue
        if key in seen_per_table[tbl]:
            continue
        seen_per_table[tbl].add(key)
        per_table[tbl].append(key)

    out = {"query": {"table": table, "query": tool_input.get("query"), "limit": limit}}
    for tbl in ("vehicle", "contract"):
        if table != "both" and table != tbl:
            continue
        plates_all = sorted(per_table[tbl])
        out[tbl] = {
            "total": len(plates_all),
            "sample": plates_all[:limit],
            "truncated": len(plates_all) > limit,
        }
    return out


def _dispatch_list_fields(tool_input: dict, session_ctx: dict) -> dict:
    """Return the set of fields with occurrence count per table."""
    table = tool_input.get("table") or "both"
    counts: dict[str, Counter] = {"vehicle": Counter(), "contract": Counter()}
    for tbl, r in _iter_lineage_records(session_ctx, table):
        counts[tbl][r.field] += 1
    out = {"query": {"table": table}}
    for tbl in ("vehicle", "contract"):
        if table != "both" and table != tbl:
            continue
        items = sorted(counts[tbl].items(), key=lambda kv: (-kv[1], kv[0]))
        out[tbl] = {
            "n_distinct_fields": len(items),
            "fields": [{"field": f, "n": n} for f, n in items],
        }
    return out


def _dispatch_get_lineage(tool_input: dict, session_ctx: dict) -> dict:
    """Query the LineageStore of both engines. Fuzzy matches on plate:
    compares canonicalized keys (uppercase alphanumeric-only)."""
    raw_plate = (tool_input.get("plate") or "").strip()
    field = tool_input.get("field")
    table_filter = tool_input.get("table")

    if not raw_plate:
        return {"error": "paramètre 'plate' requis"}

    plate_canon = _plate_key(raw_plate)
    if not plate_canon:
        return {
            "error": f"plaque '{raw_plate}' ne contient aucun caractère alphanumérique.",
        }

    records: list[dict] = []
    matched_keys: set[str] = set()

    for tbl, r in _iter_lineage_records(session_ctx, table_filter):
        # Canonicalize the stored key. For contract the key used to be
        # "plate|number" in some designs — we defensively strip after '|'.
        key_for_match = r.key.split("|", 1)[0] if "|" in r.key else r.key
        if _plate_key(key_for_match) != plate_canon:
            continue
        if field and r.field != field:
            continue
        d = asdict(r)
        d["table"] = tbl
        records.append(d)
        matched_keys.add(r.key)

    resp = {
        "records": records,
        "count": len(records),
        "matched_keys": sorted(matched_keys),
        "query": {
            "plate": raw_plate,
            "plate_canonical": plate_canon,
            "field": field,
            "table": table_filter or "both",
        },
    }

    # If nothing matched, surface up to 5 nearest-prefix candidates to help
    # the assistant propose a correction without another tool round-trip.
    if not records:
        nearest: list[str] = []
        seen: set[str] = set()
        for _tbl, r in _iter_lineage_records(session_ctx, table_filter):
            k = r.key
            if k in seen:
                continue
            seen.add(k)
            kc = _plate_key(k.split("|", 1)[0] if "|" in k else k)
            # Simple heuristic: share first 2 chars OR share last 2 chars.
            if len(plate_canon) >= 2 and len(kc) >= 2:
                if kc[:2] == plate_canon[:2] or kc[-2:] == plate_canon[-2:]:
                    nearest.append(k)
        resp["hint_nearest_plates"] = sorted(nearest)[:5]

    # If a field was given but not found, list the fields that ARE tracked
    # for the matched plate (if any match at all, even with field mismatch).
    if field and not records:
        fields_for_plate: list[str] = []
        seen_fields: set[str] = set()
        for tbl, r in _iter_lineage_records(session_ctx, table_filter):
            key_for_match = r.key.split("|", 1)[0] if "|" in r.key else r.key
            if _plate_key(key_for_match) != plate_canon:
                continue
            if r.field in seen_fields:
                continue
            seen_fields.add(r.field)
            fields_for_plate.append(r.field)
        if fields_for_plate:
            resp["hint_available_fields_for_plate"] = sorted(fields_for_plate)

    return resp


def _dispatch_tool(name: str, tool_input: dict, session_ctx: dict) -> Any:
    """Route a tool call to its handler. Returns a JSON-serializable object."""
    if name == "get_session_state":
        return _dispatch_get_session_state(session_ctx)
    if name == "list_plates":
        return _dispatch_list_plates(tool_input, session_ctx)
    if name == "list_fields":
        return _dispatch_list_fields(tool_input, session_ctx)
    if name == "get_lineage":
        return _dispatch_get_lineage(tool_input, session_ctx)
    return {"error": f"tool inconnu: {name}"}


# =============================================================================
# Chat turn — runs a tool-use loop and returns the final assistant text
# =============================================================================


def chat_turn(
    ui_messages: list[dict],
    session_ctx: dict,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    max_loops: int = DEFAULT_MAX_LOOPS,
) -> dict:
    """Run one conversational turn (possibly multiple tool iterations).

    Parameters
    ----------
    ui_messages
        Flat list of {"role": "user"|"assistant", "content": str} from the UI
        history. We rebuild API messages from these — intermediate tool
        exchanges are NOT persisted across turns (tools are stateless).
    session_ctx
        Dict snapshot of the current streamlit session (engine_files,
        engine_result, contract_result, current_mode, …). Passed into each
        tool dispatcher.
    api_key
        Anthropic API key. Falls back to ANTHROPIC_API_KEY env.
    model
        Claude model to use.

    Returns
    -------
    dict
        {"text": final_answer, "tool_uses": [(name, input_summary), ...]}
        on success; {"error": msg} on failure.
    """
    if Anthropic is None:
        return {"error": "SDK Anthropic non installé (pip install anthropic)."}
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"error": "Clé ANTHROPIC_API_KEY manquante."}

    # Build API messages from UI history. Strip any non-text content.
    api_messages: list[dict] = []
    for m in ui_messages:
        role = m.get("role")
        content = m.get("content") or ""
        if role not in ("user", "assistant"):
            continue
        if not content:
            continue
        api_messages.append({"role": role, "content": content})

    if not api_messages or api_messages[-1]["role"] != "user":
        return {"error": "Dernier message doit venir de l'utilisateur."}

    client = Anthropic(api_key=api_key)
    tool_uses: list[tuple[str, dict]] = []

    for _loop in range(max_loops):
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=SYSTEM_PROMPT_AM,
                tools=ALL_TOOLS,
                messages=api_messages,
            )
        except Exception as e:
            return {"error": f"Appel Claude échoué : {e}"}

        if resp.stop_reason == "tool_use":
            # Append assistant turn with raw content (text + tool_use blocks).
            api_messages.append({
                "role": "assistant",
                "content": [
                    _block_to_dict(b) for b in resp.content
                ],
            })
            # Execute every tool_use block and append tool_result user message.
            tool_results_content = []
            for block in resp.content:
                if getattr(block, "type", None) != "tool_use":
                    continue
                name = block.name
                tool_input = dict(block.input or {})
                tool_uses.append((name, tool_input))
                try:
                    result = _dispatch_tool(name, tool_input, session_ctx)
                    result_str = json.dumps(
                        result, ensure_ascii=False, default=str
                    )
                except Exception as e:
                    result_str = json.dumps({"error": str(e)})
                tool_results_content.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                })
            api_messages.append({
                "role": "user",
                "content": tool_results_content,
            })
            continue

        # stop_reason == "end_turn" (or anything non-tool): extract text.
        text_parts = []
        for block in resp.content:
            if getattr(block, "type", None) == "text":
                text_parts.append(block.text)
        text = "\n".join(p for p in text_parts if p).strip()
        if not text:
            text = (
                "Je n'ai pas réussi à formuler une réponse. Reformule la "
                "question ou précise quelle plaque / quel champ t'intéresse."
            )
        return {"text": text, "tool_uses": tool_uses}

    return {
        "error": (
            f"Limite de {max_loops} appels d'outils atteinte sans réponse. "
            "Essaie une question plus précise."
        ),
        "tool_uses": tool_uses,
    }


def _block_to_dict(block) -> dict:
    """Convert a ContentBlock (TextBlock / ToolUseBlock) to the dict shape
    the Anthropic SDK expects on subsequent requests."""
    btype = getattr(block, "type", None)
    if btype == "text":
        return {"type": "text", "text": block.text}
    if btype == "tool_use":
        return {
            "type": "tool_use",
            "id": block.id,
            "name": block.name,
            "input": dict(block.input or {}),
        }
    # Fallback: rely on SDK's own model_dump if present
    if hasattr(block, "model_dump"):
        return block.model_dump()
    return {"type": btype or "unknown"}

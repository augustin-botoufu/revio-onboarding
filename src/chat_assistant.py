"""Jalon 5.0.1 — In-app LLM chat assistant (AM mode).

Conversational Q&A over the current import session. Lets an Account Manager
ask plain-language questions like:

    · "Pourquoi le véhicule FR-123-AB a brand = 'Peugeot' ?"
    · "Quels sont les warnings côté contrats ?"
    · "C'est quoi le slug `ayvens_etat_parc` ?"

The assistant has READ-ONLY access to the session via 2 tools:
    · get_session_state()         → summary of loaded files + engine results
    · get_lineage(plate, field?)  → provenance trace for a specific cell

Scope of 5.0.1: AM mode only, no YAML patches, no GitHub commit. The dev
mode (propose_yaml_patch) lands in 5.0.2, the signaler button in 5.0.3.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Any, Optional

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None  # type: ignore


DEFAULT_MODEL = "claude-sonnet-4-5"
DEFAULT_MAX_TOKENS = 2048
DEFAULT_MAX_LOOPS = 6  # hard cap on tool-use iterations per user turn


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

Outils disponibles (read-only) :
- `get_session_state` : liste des fichiers chargés, slugs détectés, comptes
  de lignes en sortie Vehicle/Contract, nombre de warnings. APPELLE CET
  OUTIL EN PREMIER pour savoir ce que l'utilisateur a chargé.
- `get_lineage` : trace de provenance pour une (plaque, champ). Retourne
  le rule_id, la valeur retenue, la source gagnante, ET la liste des
  candidats rejetés avec la raison du rejet.

Règles strictes :
- Tu es READ-ONLY. Tu ne peux PAS modifier les YAML de règles ni les
  fichiers mémorisés. Si l'utilisateur demande une modif, explique que
  c'est prévu en mode Dev (Jalon 5.0.2) et propose de signaler.
- N'invente AUCUN chiffre. Si tu n'as pas la donnée, appelle un tool
  ou dis que tu ne sais pas.
- Reste bref : 3-5 phrases maximum par réponse. Utilise des listes pour
  les traces de lineage (candidat gagnant + candidats écartés).
- Si l'utilisateur est vague ("pourquoi c'est bizarre ?"), demande quel
  véhicule / plaque / champ précisément."""


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

TOOL_GET_LINEAGE = {
    "name": "get_lineage",
    "description": (
        "Retourne la trace de provenance pour une ou plusieurs cellules du "
        "résultat. Fournit : valeur finale, source gagnante, règle YAML, "
        "transform appliquée, ET liste des candidats écartés avec la raison. "
        "Utilise cet outil pour répondre aux 'pourquoi cette valeur' et "
        "'pourquoi pas cette autre valeur'."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "plate": {
                "type": "string",
                "description": (
                    "Plaque d'immatriculation du véhicule, ex: 'AB-123-CD'. "
                    "Pour un contrat, la clé interne est 'plate|number' mais "
                    "ce tool matche par préfixe donc la plaque seule suffit."
                ),
            },
            "field": {
                "type": "string",
                "description": (
                    "Nom du champ Revio à inspecter (ex: 'brand', 'plate', "
                    "'totalPrice'). Si absent, retourne tous les champs "
                    "tracés pour cette plaque."
                ),
            },
            "table": {
                "type": "string",
                "enum": ["vehicle", "contract"],
                "description": (
                    "Restreint à une seule table. Par défaut : cherche dans "
                    "les deux."
                ),
            },
        },
        "required": ["plate"],
    },
}

ALL_TOOLS = [TOOL_GET_SESSION_STATE, TOOL_GET_LINEAGE]


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

    def _summarize_result(res, label: str) -> Optional[dict]:
        if not res:
            return None
        df = getattr(res, "df", None)
        warnings = list(getattr(res, "warnings", []) or [])
        out = {
            "n_rows": int(len(df)) if df is not None else 0,
            "n_warnings": len(warnings),
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
        "vehicle_result": _summarize_result(
            session_ctx.get("engine_result"), "vehicle"
        ),
        "contract_result": _summarize_result(
            session_ctx.get("contract_result"), "contract"
        ),
        "n_session_overrides": len(overrides),
    }


def _dispatch_get_lineage(tool_input: dict, session_ctx: dict) -> dict:
    """Query the LineageStore of both engines. Matches contract keys by
    plate prefix (contract key format: 'plate|number')."""
    plate = (tool_input.get("plate") or "").strip()
    field = tool_input.get("field")
    table_filter = tool_input.get("table")

    if not plate:
        return {"error": "paramètre 'plate' requis"}

    records: list[dict] = []

    def _collect(res, table: str) -> None:
        if not res:
            return
        store = getattr(res, "lineage", None)
        if store is None:
            return
        # Dig into internal _records to match by prefix for contract.
        for r in getattr(store, "_records", []):
            if table == "vehicle":
                if r.key != plate:
                    continue
            else:  # contract — key is "plate|number"
                if not (r.key == plate or r.key.startswith(f"{plate}|")):
                    continue
            if field and r.field != field:
                continue
            d = asdict(r)
            d["table"] = table  # ensure present
            records.append(d)

    if not table_filter or table_filter == "vehicle":
        _collect(session_ctx.get("engine_result"), "vehicle")
    if not table_filter or table_filter == "contract":
        _collect(session_ctx.get("contract_result"), "contract")

    return {
        "records": records,
        "count": len(records),
        "query": {
            "plate": plate,
            "field": field,
            "table": table_filter or "both",
        },
    }


def _dispatch_tool(name: str, tool_input: dict, session_ctx: dict) -> Any:
    """Route a tool call to its handler. Returns a JSON-serializable object."""
    if name == "get_session_state":
        return _dispatch_get_session_state(session_ctx)
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

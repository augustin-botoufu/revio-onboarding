"""Mode Dev — Jalon 5.0.2.

An in-app editor for the YAML rules files (``src/rules/*.yml``) used by
the contract / vehicle engines.

Flow
----
1. The user picks a rules file and describes, in French, what they want to
   change ("pour les VASP force ``isHT`` à vrai").
2. We send the current YAML + the request to Claude, which returns:
    - the FULL new YAML content (not a diff — the app computes the diff
      for display),
    - a short summary of the changes,
    - an honest risk assessment,
    - a suggested commit message + PR title/body.
3. The app renders a side-by-side diff view. On validation, we:
    a. create a fresh branch ``mode-dev/<YYYYMMDD-HHMMSS>-<slug>`` from main,
    b. commit the new YAML on that branch,
    c. open a pull request against main,
    d. surface the PR URL so the user can review & merge.

Security
--------
- Email allowlist gate (configurable via ``MODE_DEV_EMAILS`` secret,
  falling back to a built-in list for the founding team).
- All GitHub writes go to a new branch → main is never touched without a
  human clicking "Merge" in GitHub's UI.
- The app never executes the generated YAML in-process; Streamlit Cloud
  will redeploy from main only once the PR is merged.

Notes
-----
This module deliberately keeps the non-Streamlit logic (patch generation,
branch/PR helpers) in free functions that accept a text YAML + a request
string, so they can be unit-tested without importing ``streamlit``.
"""

from __future__ import annotations

import datetime as _dt
import difflib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml  # type: ignore

try:
    from anthropic import Anthropic  # type: ignore
except Exception:  # pragma: no cover
    Anthropic = None  # type: ignore

from . import github_sync as gh


# =============================================================================
# Config
# =============================================================================

DEFAULT_ALLOWLIST: tuple[str, ...] = (
    "augustin@gorevio.co",
    "victor@gorevio.co",
    "adrien@gorevio.co",
)

DEFAULT_MODEL = "claude-sonnet-4-5"
DEFAULT_MAX_TOKENS = 8192

# Rules dir, repo-relative. Matches the layout in ``src/rules/`` and is
# consistent with ``github_sync.DEFAULT_PATH`` for the learned_patterns file.
RULES_DIR_REPO = "src/rules"

# Local path used to seed the file picker. We always source-of-truth from
# GitHub before generating a patch (the local disk may be stale on
# Streamlit Cloud), but the file list comes from the repo checkout.
RULES_DIR_LOCAL = Path(__file__).parent / "rules"


# =============================================================================
# Email allowlist
# =============================================================================

def _read_secret(name: str, default: str = "") -> str:
    """Mirror github_sync._read_secret so we don't import its private helper."""
    try:
        import streamlit as st  # noqa: WPS433
        try:
            val = st.secrets.get(name, None)  # type: ignore[attr-defined]
        except Exception:
            val = None
        if val:
            return str(val)
    except Exception:
        pass
    return os.environ.get(name, default) or ""


def get_allowlist() -> list[str]:
    """Return the list of emails allowed to access Mode Dev.

    Priority:
    1. ``MODE_DEV_EMAILS`` secret / env var (comma-separated).
    2. Built-in :data:`DEFAULT_ALLOWLIST` for the founding team.
    """
    raw = _read_secret("MODE_DEV_EMAILS")
    if raw:
        emails = [e.strip().lower() for e in raw.split(",") if e.strip()]
        if emails:
            return emails
    return [e.lower() for e in DEFAULT_ALLOWLIST]


def is_allowlisted(email: Optional[str]) -> bool:
    """True iff ``email`` (case-insensitive) is on the Mode Dev allowlist."""
    if not email:
        return False
    return email.strip().lower() in get_allowlist()


# =============================================================================
# File listing
# =============================================================================

@dataclass
class RulesFile:
    """A rules/*.yml file we can target from Mode Dev.

    ``local_path`` is used to pre-populate the file picker and to display
    a default content when GitHub is unavailable; ``repo_path`` is always
    the source of truth for commits.
    """

    name: str                # e.g. "contract.yml"
    repo_path: str           # e.g. "src/rules/contract.yml"
    local_path: Path


def list_rules_files(local_dir: Optional[Path] = None) -> list[RulesFile]:
    """Enumerate all ``*.yml`` files under ``src/rules/`` in the repo checkout.

    Excludes ``learned_patterns.yml``, ``learned_columns.yml`` and
    ``value_mappings.yml`` because those are auto-managed by the
    mapping/memorize flows — editing them by hand in Mode Dev would cause
    merge conflicts with the auto-commit path.
    """
    root = local_dir or RULES_DIR_LOCAL
    if not root.exists():
        return []
    AUTO_MANAGED = {
        "learned_patterns.yml",
        "learned_columns.yml",
        "value_mappings.yml",
    }
    out: list[RulesFile] = []
    for p in sorted(root.glob("*.yml")):
        if p.name in AUTO_MANAGED:
            continue
        out.append(
            RulesFile(
                name=p.name,
                repo_path=f"{RULES_DIR_REPO}/{p.name}",
                local_path=p,
            )
        )
    return out


def read_local_rules_file(f: RulesFile) -> str:
    """Return the current contents of the file on the local checkout.

    Returns an empty string if the file is missing (should not happen in
    practice since :func:`list_rules_files` only surfaces existing files).
    """
    try:
        return f.local_path.read_text(encoding="utf-8")
    except Exception:
        return ""


# =============================================================================
# Patch generation via Claude
# =============================================================================

SYSTEM_PROMPT = """\
Tu es un assistant pour un outil interne de développement.

RÔLE
----
Tu édites des fichiers YAML de règles métier pour l'app Revio Onboarding.
Ces fichiers définissent comment transformer des exports loueurs (API Plaques,
Ayvens, Arval…) en imports Revio. Ils sont lus par un moteur de règles Python
qui traite chaque règle dans l'ordre de priorité défini dans le YAML.

CONTEXTE
--------
- Les règles décrivent des bindings entre une colonne source et un champ Revio,
  avec une priorité, éventuellement une transformation (map, coerce_bool, …),
  et des conditions optionnelles.
- L'utilisateur est un non-dev : il décrit en français ce qu'il veut changer,
  c'est à toi de produire le YAML exact.
- Le fichier YAML actuel t'est donné dans le message utilisateur. Il est
  l'unique source de vérité — ne présume pas de son contenu, lis-le.

TÂCHE
-----
Tu dois appeler l'outil `propose_yaml_patch` UNE SEULE FOIS avec :
- `new_content` : le contenu COMPLET du nouveau YAML (pas un diff). Préserve
  les commentaires, la structure, l'ordre des clés, et le style (indentation
  2 espaces, strings sans quotes inutiles).
- `summary` : 1-3 phrases décrivant le changement en français clair, à
  destination d'un non-dev.
- `risks` : 0-3 puces listant les risques ou effets de bord possibles (ex.
  "tous les VASP existants basculeront de TTC à HT au prochain import").
  Si aucun risque, renvoie une liste vide.
- `commit_message` : message de commit court, format conventionnel
  ("rules/contract: VASP → HT"), 72 caractères max.
- `pr_title` : titre de la PR GitHub (type "Mode Dev: ..."), 72 caractères max.
- `pr_body` : description markdown de la PR (2-5 lignes) rappelant la demande
  initiale et le changement appliqué.

CONTRAINTES
-----------
1. Ne fais JAMAIS plus de changements que ce qui est demandé. Si la demande
   est ambiguë, choisis l'interprétation la plus conservatrice et mentionne-le
   dans `risks`.
2. Si tu ne peux pas satisfaire la demande sans casser le YAML, renvoie
   `new_content` identique à l'actuel et explique pourquoi dans `summary`.
3. Vérifie que ton YAML est syntaxiquement valide avant de l'envoyer (indentation,
   deux points, listes). Si tu hésites, relis mentalement le fichier.
4. Préserve exactement les en-têtes de commentaires du fichier.
"""


PATCH_TOOL = {
    "name": "propose_yaml_patch",
    "description": (
        "Renvoie le nouveau contenu YAML complet, un résumé du changement, "
        "la liste des risques, un message de commit et les métadonnées de "
        "la PR GitHub."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "new_content": {
                "type": "string",
                "description": (
                    "Contenu complet du nouveau fichier YAML. Pas un diff. "
                    "Doit être une chaîne UTF-8 directement écrivable sur disque."
                ),
            },
            "summary": {
                "type": "string",
                "description": "Résumé 1-3 phrases du changement, en français.",
            },
            "risks": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "0 à 3 risques ou effets de bord du changement. Liste vide "
                    "si aucun risque."
                ),
            },
            "commit_message": {
                "type": "string",
                "description": (
                    "Message de commit court (≤72 chars), format 'rules/<slug>: "
                    "<résumé impératif>'."
                ),
            },
            "pr_title": {
                "type": "string",
                "description": "Titre de la PR GitHub, ≤72 chars.",
            },
            "pr_body": {
                "type": "string",
                "description": (
                    "Corps markdown de la PR : 2-5 lignes. Rappelle la demande "
                    "d'origine puis résume le changement."
                ),
            },
        },
        "required": [
            "new_content",
            "summary",
            "risks",
            "commit_message",
            "pr_title",
            "pr_body",
        ],
    },
}


@dataclass
class PatchProposal:
    """Structured response from :func:`generate_patch`.

    ``ok`` is False when the call failed (no API key, SDK missing, network
    error, Claude couldn't produce a tool call…); ``error`` carries the
    UI-friendly message.
    """

    ok: bool
    new_content: str = ""
    summary: str = ""
    risks: list[str] = None  # type: ignore[assignment]
    commit_message: str = ""
    pr_title: str = ""
    pr_body: str = ""
    error: str = ""
    raw_text: str = ""


def generate_patch(
    *,
    file_name: str,
    current_yaml: str,
    user_request: str,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
) -> PatchProposal:
    """Ask Claude to produce a new YAML given the current one + a NL request.

    Returns a :class:`PatchProposal`. When ``ok`` is False, ``error`` has
    a UI-friendly explanation. The caller should NOT commit the patch if
    ``new_content`` equals ``current_yaml`` (no-op) — surface the summary
    instead.
    """
    if Anthropic is None:
        return PatchProposal(
            ok=False,
            error="SDK Anthropic non installé (pip install anthropic).",
        )
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return PatchProposal(
            ok=False,
            error="Clé ANTHROPIC_API_KEY manquante.",
        )

    user_msg = (
        f"Fichier à modifier : `{file_name}` (dans `src/rules/`)\n\n"
        f"Demande de l'utilisateur :\n---\n{user_request.strip()}\n---\n\n"
        f"Contenu actuel du fichier YAML :\n```yaml\n{current_yaml}\n```\n\n"
        "Appelle `propose_yaml_patch` avec le nouveau contenu complet."
    )

    client = Anthropic(api_key=api_key)
    try:
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=SYSTEM_PROMPT,
            tools=[PATCH_TOOL],
            tool_choice={"type": "tool", "name": "propose_yaml_patch"},
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as e:
        return PatchProposal(ok=False, error=f"Appel Claude échoué : {e}")

    # Extract the forced tool_use block.
    tool_block = None
    text_fallback = ""
    for block in resp.content:
        btype = getattr(block, "type", None)
        if btype == "tool_use" and getattr(block, "name", "") == "propose_yaml_patch":
            tool_block = block
            break
        if btype == "text":
            text_fallback += getattr(block, "text", "") or ""

    if tool_block is None:
        return PatchProposal(
            ok=False,
            error=(
                "Claude n'a pas produit de patch structuré. "
                "Réponse brute : " + (text_fallback[:400] or "(vide)")
            ),
            raw_text=text_fallback,
        )

    data = dict(tool_block.input or {})
    new_content = str(data.get("new_content") or "")
    # Normalize: ensure trailing newline for POSIX-friendly files.
    if new_content and not new_content.endswith("\n"):
        new_content += "\n"

    risks = data.get("risks") or []
    if not isinstance(risks, list):
        risks = [str(risks)]
    risks = [str(r).strip() for r in risks if str(r).strip()]

    return PatchProposal(
        ok=True,
        new_content=new_content,
        summary=str(data.get("summary") or "").strip(),
        risks=risks,
        commit_message=str(data.get("commit_message") or "").strip(),
        pr_title=str(data.get("pr_title") or "").strip(),
        pr_body=str(data.get("pr_body") or "").strip(),
    )


# =============================================================================
# YAML validation
# =============================================================================

def validate_yaml_text(text: str) -> tuple[bool, str]:
    """Return (ok, error_message). ``ok=True`` means yaml.safe_load succeeded."""
    try:
        yaml.safe_load(text)
        return True, ""
    except yaml.YAMLError as e:
        return False, f"YAML invalide : {e}"
    except Exception as e:
        return False, f"YAML illisible : {e}"


def unified_diff(old: str, new: str, *, filename: str = "file.yml") -> str:
    """Return a unified diff string (old → new) suitable for ``st.code(..., language='diff')``."""
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)
    diff = difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile=f"a/{filename}",
        tofile=f"b/{filename}",
        n=3,
    )
    return "".join(diff) or "(aucun changement)"


# =============================================================================
# Branch + PR orchestration
# =============================================================================

_BRANCH_SLUG_RE = re.compile(r"[^a-z0-9-]+")


def _slugify(text: str, max_len: int = 40) -> str:
    """Produce a filesystem/branch-safe slug from free text (ASCII only).

    Any non-alphanumeric character (whitespace, punctuation, accents,
    equals sign, …) becomes a hyphen, then consecutive hyphens collapse.
    This way ``"isHT=true"`` → ``"isht-true"`` instead of ``"ishttrue"``
    so the resulting branch name stays readable.
    """
    low = text.lower().strip()
    # Replace any non-alnum run with a single hyphen.
    low = re.sub(r"[^a-z0-9]+", "-", low)
    low = low.strip("-")
    if not low:
        low = "patch"
    return low[:max_len].rstrip("-") or "patch"


def build_branch_name(
    user_request: str,
    *,
    now: Optional[_dt.datetime] = None,
) -> str:
    """Deterministic branch name: ``mode-dev/YYYYMMDD-HHMMSS-<slug>``.

    The timestamp makes collisions effectively impossible for a given user
    so a double-click won't conflict on a fresh branch.
    """
    when = now or _dt.datetime.utcnow()
    stamp = when.strftime("%Y%m%d-%H%M%S")
    slug = _slugify(user_request, max_len=30)
    return f"mode-dev/{stamp}-{slug}"


@dataclass
class PullRequestResult:
    """Result of :func:`open_pull_request` for the Mode Dev flow."""

    ok: bool
    branch: str = ""
    commit_sha: str = ""
    pr_url: str = ""
    pr_number: Optional[int] = None
    error: str = ""


def open_pull_request(
    *,
    repo_path: str,
    new_content: str,
    proposal: PatchProposal,
    user_request: str,
    author_email: Optional[str] = None,
    cfg: Optional[gh.GitHubConfig] = None,
    now: Optional[_dt.datetime] = None,
) -> PullRequestResult:
    """Create a branch, commit the patch, open a PR against the default branch.

    Parameters
    ----------
    repo_path
        Repo-relative path (e.g. ``"src/rules/contract.yml"``).
    new_content
        Full new YAML text (not a diff).
    proposal
        The :class:`PatchProposal` produced by :func:`generate_patch` —
        supplies commit message and PR title/body.
    user_request
        The original free-text request, used as an extra line at the bottom
        of the PR body so reviewers know the prompt.
    author_email
        Email to attribute the commit to (falls back to the bot email).
    """
    try:
        cfg = cfg or gh.get_config()
    except gh.GitHubNotConfigured as e:
        return PullRequestResult(ok=False, error=e.user_message)

    try:
        base_sha = gh.get_branch_sha(cfg)
    except gh.GitHubSyncError as e:
        return PullRequestResult(
            ok=False,
            error=f"Impossible de lire la branche de base : {e.user_message}",
        )

    branch = build_branch_name(user_request, now=now)
    try:
        gh.create_branch(cfg, branch, from_sha=base_sha)
    except gh.GitHubSyncError as e:
        return PullRequestResult(
            ok=False,
            error=f"Création de branche impossible : {e.user_message}",
        )

    # Fetch the file *on the fresh branch* to get the right sha for the PUT.
    try:
        remote = gh.fetch_file_on_branch(cfg, repo_path, branch)
    except gh.GitHubSyncError as e:
        return PullRequestResult(
            ok=False,
            error=f"Lecture du fichier cible échouée : {e.user_message}",
        )

    try:
        commit_resp = gh.commit_file_on_branch(
            cfg,
            repo_path,
            new_content,
            branch=branch,
            sha=remote.sha,
            message=proposal.commit_message or f"mode-dev: patch {repo_path}",
            author_email=author_email,
        )
    except gh.GitHubSyncError as e:
        return PullRequestResult(
            ok=False,
            branch=branch,
            error=f"Commit sur la branche échoué : {e.user_message}",
        )

    commit_sha = (
        (commit_resp.get("commit") or {}).get("sha") if isinstance(commit_resp, dict) else ""
    ) or ""

    # Build PR body — append the raw request for traceability so a reviewer
    # seeing the PR alone understands what the user asked for.
    pr_body = proposal.pr_body or ""
    pr_body_footer = (
        "\n\n---\n"
        "**Demande initiale (Mode Dev)** :\n"
        f"> {user_request.strip()}\n\n"
        + (f"**Auteur :** {author_email}\n" if author_email else "")
    )
    full_body = pr_body + pr_body_footer

    try:
        pr_resp = gh.create_pull_request(
            cfg,
            head=branch,
            title=proposal.pr_title or f"Mode Dev: patch {repo_path}",
            body=full_body,
        )
    except gh.GitHubSyncError as e:
        return PullRequestResult(
            ok=False,
            branch=branch,
            commit_sha=commit_sha,
            error=(
                f"Commit OK sur `{branch}` mais ouverture de la PR échouée : "
                f"{e.user_message}. Tu peux ouvrir la PR à la main sur GitHub."
            ),
        )

    return PullRequestResult(
        ok=True,
        branch=branch,
        commit_sha=commit_sha,
        pr_url=pr_resp.get("html_url") or "",
        pr_number=pr_resp.get("number"),
    )


# =============================================================================
# Streamlit UI
# =============================================================================

def _render_identity_gate() -> Optional[str]:
    """Ask the user to pick their email from the allowlist. Returns the chosen
    email (or ``None`` if no choice made yet)."""
    import streamlit as st  # local import so the non-UI path doesn't need it

    claimed = st.session_state.get("mode_dev_email")
    if claimed and is_allowlisted(claimed):
        return claimed

    allow = get_allowlist()
    st.markdown("### Identification")
    st.caption(
        "Choisis ton email pour accéder à Mode Dev. Ton email sera utilisé "
        "comme auteur du commit et de la PR."
    )
    choice = st.selectbox(
        "Qui es-tu ?",
        options=[""] + allow,
        format_func=lambda x: "— choisis ton email —" if not x else x,
        key="mode_dev_email_select",
    )
    c1, c2 = st.columns([1, 3])
    with c1:
        if st.button("Entrer", type="primary", disabled=not choice):
            if is_allowlisted(choice):
                st.session_state["mode_dev_email"] = choice
                st.rerun()
            else:
                st.error(
                    "Cet email n'est pas sur la liste d'accès Mode Dev. "
                    "Demande à Augustin de l'ajouter dans les Secrets "
                    "Streamlit (`MODE_DEV_EMAILS`)."
                )
    with c2:
        st.caption(
            f"Liste actuelle : {', '.join(allow)}. "
            "Pour modifier : Secret `MODE_DEV_EMAILS` (CSV)."
        )
    return None


def _render_proposal_preview(
    *,
    file_obj: "RulesFile",
    current_yaml: str,
    proposal: PatchProposal,
) -> None:
    """Render the diff + summary + risks panels for a successful proposal."""
    import streamlit as st

    st.markdown("#### Résumé du changement")
    st.info(proposal.summary or "(pas de résumé)")

    if proposal.risks:
        st.markdown("#### Risques / effets de bord")
        for r in proposal.risks:
            st.markdown(f"- {r}")
    else:
        st.caption("✅ Aucun risque signalé par l'assistant.")

    st.markdown("#### Diff")
    diff = unified_diff(current_yaml, proposal.new_content, filename=file_obj.name)
    if diff.strip() == "(aucun changement)":
        st.warning(
            "L'assistant propose un YAML identique à l'actuel. "
            "Soit la demande n'impactait pas ce fichier, soit elle est "
            "déjà appliquée. Rien à committer."
        )
    else:
        st.code(diff, language="diff")

    # Collapsed full-file view so the user can sanity-check the whole thing.
    with st.expander("📄 Voir le nouveau fichier complet", expanded=False):
        st.code(proposal.new_content, language="yaml")

    # YAML validity is best-effort (rules-engine semantics aren't enforced).
    ok, err = validate_yaml_text(proposal.new_content)
    if not ok:
        st.error(f"⚠️ {err}")
    else:
        st.caption("✓ YAML syntaxiquement valide.")


def render_mode_dev_page() -> None:
    """Main entry point for the 🛠️ Mode Dev page (dispatched from app.py)."""
    import streamlit as st

    st.title("🛠️ Mode Dev")
    st.caption(
        "Modifie les règles YAML du moteur par une simple demande en français. "
        "L'assistant génère un patch, tu le relis, une PR est ouverte sur GitHub."
    )

    email = _render_identity_gate()
    if email is None:
        return

    # Header line: logged-in identity + sign-out shortcut.
    top_l, top_r = st.columns([4, 1])
    with top_l:
        st.success(f"Connecté en tant que **{email}**")
    with top_r:
        if st.button("Changer d'utilisateur", use_container_width=True):
            st.session_state.pop("mode_dev_email", None)
            _reset_patch_state()
            st.rerun()

    # GitHub config check.
    if not gh.is_configured():
        st.error(
            "Mode Dev a besoin d'une config GitHub (`GITHUB_TOKEN` et "
            "`GITHUB_REPO`) dans les Secrets Streamlit Cloud. Sans ça, la PR "
            "ne peut pas être ouverte."
        )
        st.stop()

    files = list_rules_files()
    if not files:
        st.warning(
            "Aucun fichier YAML trouvé dans `src/rules/`. "
            "Rien à éditer en Mode Dev."
        )
        st.stop()

    # --- File picker + request form ----------------------------------------
    st.markdown("### 1. Choisis le fichier et décris le changement")

    file_names = [f.name for f in files]
    picked_name = st.selectbox(
        "Fichier à modifier",
        options=file_names,
        index=0,
        key="mode_dev_file_pick",
        help="Portée Mode Dev : tout `src/rules/*.yml`, sauf les 3 fichiers "
             "auto-gérés par les flows Mémoriser (learned_patterns, "
             "learned_columns, value_mappings).",
    )
    picked = next(f for f in files if f.name == picked_name)

    current_yaml = read_local_rules_file(picked)

    with st.expander(f"📄 Contenu actuel de `{picked.name}`", expanded=False):
        if current_yaml:
            st.code(current_yaml, language="yaml")
        else:
            st.caption("(fichier vide ou introuvable)")

    st.markdown("**Ta demande** (en français, en langage naturel)")
    user_request = st.text_area(
        "Ta demande",
        value=st.session_state.get("mode_dev_request_text", ""),
        height=140,
        placeholder=(
            "Ex. Pour les VASP, force le champ `isHT` à vrai au lieu de TTC.\n"
            "Ex. Remonte la priorité de `ayvens_etat_parc` pour le champ "
            "`motorisation` au-dessus de `api_plaques`."
        ),
        key="mode_dev_request_input",
        label_visibility="collapsed",
    )

    c1, c2 = st.columns([1, 3])
    with c1:
        generate = st.button(
            "🪄 Générer le patch",
            type="primary",
            use_container_width=True,
            disabled=not user_request.strip(),
        )
    with c2:
        if st.session_state.get("mode_dev_proposal"):
            if st.button("↺ Recommencer", use_container_width=False):
                _reset_patch_state()
                st.rerun()

    if generate:
        st.session_state["mode_dev_request_text"] = user_request
        with st.spinner("Claude rédige le patch..."):
            proposal = generate_patch(
                file_name=picked.name,
                current_yaml=current_yaml,
                user_request=user_request,
            )
        st.session_state["mode_dev_proposal"] = proposal
        st.session_state["mode_dev_proposal_file"] = picked.name
        st.session_state["mode_dev_proposal_yaml"] = current_yaml
        st.session_state.pop("mode_dev_pr_result", None)

    # --- Proposal preview --------------------------------------------------
    proposal: Optional[PatchProposal] = st.session_state.get("mode_dev_proposal")
    if not proposal:
        st.info("Décris un changement et clique sur « Générer le patch » pour continuer.")
        return

    if not proposal.ok:
        st.error(proposal.error or "Échec de la génération du patch.")
        if proposal.raw_text:
            with st.expander("Réponse brute Claude"):
                st.code(proposal.raw_text)
        return

    # The proposal in session_state may belong to a different file than the
    # currently picked one — refresh YAML if so to avoid stale diffs.
    proposal_file = st.session_state.get("mode_dev_proposal_file") or picked.name
    proposal_yaml = st.session_state.get("mode_dev_proposal_yaml") or current_yaml
    if proposal_file != picked.name:
        st.warning(
            f"Le patch a été généré pour `{proposal_file}` mais le fichier "
            f"sélectionné est maintenant `{picked.name}`. Régénère ou repasse "
            "au bon fichier."
        )

    # We keep the proposal against its original file for the preview.
    proposal_rf = next((f for f in files if f.name == proposal_file), picked)

    st.markdown("---")
    st.markdown(f"### 2. Relis le patch pour `{proposal_file}`")
    _render_proposal_preview(
        file_obj=proposal_rf,
        current_yaml=proposal_yaml,
        proposal=proposal,
    )

    # --- PR opening --------------------------------------------------------
    pr_result: Optional[PullRequestResult] = st.session_state.get("mode_dev_pr_result")

    same_content = proposal.new_content == proposal_yaml

    st.markdown("---")
    st.markdown("### 3. Ouvrir la Pull Request")

    if pr_result and pr_result.ok:
        st.success(
            f"✅ PR ouverte sur `{pr_result.branch}` "
            f"(commit `{pr_result.commit_sha[:7]}`)"
        )
        if pr_result.pr_url:
            st.markdown(
                f"**[🔗 Ouvrir la PR sur GitHub](<{pr_result.pr_url}>)** — "
                "relis, fais relire, merge quand tu es OK."
            )
        st.caption(
            "La PR n'est PAS mergée automatiquement. Tant qu'elle est ouverte, "
            "l'app en production sur `main` n'est pas impactée."
        )
        if st.button("🆕 Nouveau patch"):
            _reset_patch_state()
            st.rerun()
        return

    if pr_result and not pr_result.ok:
        st.error(pr_result.error or "Échec de l'ouverture de la PR.")
        if pr_result.branch:
            st.caption(f"Branche créée : `{pr_result.branch}` (tu peux la supprimer sur GitHub si besoin).")

    commit_msg = st.text_input(
        "Message de commit",
        value=proposal.commit_message or f"mode-dev: patch {proposal_file}",
        max_chars=100,
        key="mode_dev_commit_msg",
    )
    pr_title = st.text_input(
        "Titre de la PR",
        value=proposal.pr_title or f"Mode Dev: patch {proposal_file}",
        max_chars=100,
        key="mode_dev_pr_title",
    )
    pr_body = st.text_area(
        "Corps de la PR (markdown)",
        value=proposal.pr_body,
        height=120,
        key="mode_dev_pr_body",
    )

    disable_commit = same_content
    if same_content:
        st.warning(
            "Le patch est identique au fichier actuel — rien à committer. "
            "Régénère avec une demande plus précise."
        )

    if st.button(
        "🚀 Ouvrir la PR sur GitHub",
        type="primary",
        disabled=disable_commit,
        use_container_width=False,
    ):
        # Re-pack the proposal with any user edits to msg / title / body.
        edited = PatchProposal(
            ok=True,
            new_content=proposal.new_content,
            summary=proposal.summary,
            risks=proposal.risks or [],
            commit_message=commit_msg.strip() or proposal.commit_message,
            pr_title=pr_title.strip() or proposal.pr_title,
            pr_body=pr_body.strip(),
        )
        target_repo_path = next(
            (f.repo_path for f in files if f.name == proposal_file),
            f"{RULES_DIR_REPO}/{proposal_file}",
        )
        with st.spinner("Création de la branche et de la PR..."):
            result = open_pull_request(
                repo_path=target_repo_path,
                new_content=proposal.new_content,
                proposal=edited,
                user_request=st.session_state.get("mode_dev_request_text", ""),
                author_email=email,
            )
        st.session_state["mode_dev_pr_result"] = result
        st.rerun()


def _reset_patch_state() -> None:
    """Clear the transient Mode Dev state so the user can start over."""
    import streamlit as st
    for k in (
        "mode_dev_proposal",
        "mode_dev_proposal_file",
        "mode_dev_proposal_yaml",
        "mode_dev_pr_result",
        "mode_dev_request_text",
        "mode_dev_request_input",
        "mode_dev_commit_msg",
        "mode_dev_pr_title",
        "mode_dev_pr_body",
    ):
        st.session_state.pop(k, None)

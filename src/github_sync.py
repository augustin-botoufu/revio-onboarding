"""GitHub sync for learned_patterns.yml — Jalon 2.5.

Rationale:
- Streamlit Cloud has an ephemeral filesystem; writing to disk doesn't
  persist across redeploys.
- But every commit to the `main` branch triggers an auto-redeploy.
- So: to persist a new pattern, we commit the updated `learned_patterns.yml`
  file directly to GitHub via the REST API. On the next rerun (after the
  redeploy completes, ~30-60s later), the app reads the new file from disk.

Design choices:
- stdlib only (urllib) to avoid pulling in `requests` as a dependency.
- Safe-by-default: every write is preceded by a read of the current file
  (to get its sha), so we never lose concurrent updates.
- Structured exceptions so the UI can display actionable error messages
  (missing token, 404 on repo, etc.) rather than a generic stack trace.
- Keeps a single source of truth: the YAML file on disk. The API just
  writes to it; reads go through `learned_patterns.load_patterns()` as
  before.

Config is read from Streamlit secrets (falls back to env vars locally):
    GITHUB_TOKEN   — fine-grained PAT with Contents: Read/Write on the repo
    GITHUB_REPO    — "owner/repo" slug, e.g. "augustin/revio_onboarding"
    GITHUB_BRANCH  — branch to commit to (default "main")
    GITHUB_PATH    — path to the YAML file (default "src/rules/learned_patterns.yml")

Typical flow for the UI:
    cfg = get_config()                      # raises if missing
    patterns = fetch_patterns_yaml(cfg)     # returns (text, sha)
    new_text = upsert_pattern_text(patterns, pattern_entry)
    commit_file(cfg, new_text, sha=patterns.sha, message="...")
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml  # type: ignore


DEFAULT_PATH = "src/rules/learned_patterns.yml"
DEFAULT_VALUE_MAPPINGS_PATH = "src/rules/value_mappings.yml"
DEFAULT_BRANCH = "main"
API_BASE = "https://api.github.com"


class GitHubSyncError(Exception):
    """Base class for github_sync errors, with a UI-friendly message."""

    def __init__(
        self,
        user_message: str,
        *,
        status_code: Optional[int] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(user_message)
        self.user_message = user_message
        self.status_code = status_code
        self.cause = cause


class GitHubNotConfigured(GitHubSyncError):
    """Raised when the required secrets/env vars are missing."""


class GitHubFileNotFound(GitHubSyncError):
    """Raised when the target file (or repo) doesn't exist on GitHub."""


class GitHubConflict(GitHubSyncError):
    """Raised on 409 sha-mismatch — caller can retry after re-fetching."""


@dataclass
class GitHubConfig:
    token: str
    repo: str           # "owner/repo"
    branch: str
    path: str

    @property
    def owner(self) -> str:
        return self.repo.split("/", 1)[0]

    @property
    def name(self) -> str:
        return self.repo.split("/", 1)[1] if "/" in self.repo else ""


@dataclass
class RemoteFile:
    text: str           # decoded UTF-8 content
    sha: str            # blob sha, required for the next PUT


# --- Config ----------------------------------------------------------------

def _read_secret(name: str, default: str = "") -> str:
    """Read a secret from Streamlit secrets first, then env var.

    We defer importing streamlit so this module stays testable in plain
    Python (e.g., from a notebook or a unit test).
    """
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


def get_config() -> GitHubConfig:
    """Load the config or raise GitHubNotConfigured with a clear message."""
    token = _read_secret("GITHUB_TOKEN")
    repo = _read_secret("GITHUB_REPO")
    branch = _read_secret("GITHUB_BRANCH") or DEFAULT_BRANCH
    path = _read_secret("GITHUB_PATH") or DEFAULT_PATH
    missing = [k for k, v in (("GITHUB_TOKEN", token), ("GITHUB_REPO", repo)) if not v]
    if missing:
        raise GitHubNotConfigured(
            "Configuration GitHub incomplète. Ajoute ces clés dans "
            "Streamlit Cloud → Settings → Secrets : "
            + ", ".join(missing)
        )
    if "/" not in repo:
        raise GitHubNotConfigured(
            f"GITHUB_REPO doit être au format 'owner/repo' (reçu : {repo!r})."
        )
    return GitHubConfig(token=token, repo=repo, branch=branch, path=path)


def is_configured() -> bool:
    """Cheap check that avoids raising — UI can use it to hide buttons."""
    try:
        get_config()
        return True
    except GitHubNotConfigured:
        return False


# --- HTTP helpers ----------------------------------------------------------

def _http(method: str, url: str, token: str, *, body: Optional[dict] = None) -> dict:
    """Perform an authenticated GitHub API call and return the JSON body."""
    data: Optional[bytes] = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    req.add_header("User-Agent", "revio-onboarding-app")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
            err_json = json.loads(err_body) if err_body else {}
            gh_msg = err_json.get("message") or err_body[:200]
        except Exception:
            gh_msg = str(e)
        # Distinguish the common cases so the caller can recover gracefully
        # (e.g., seed a missing file, retry on concurrent writes).
        if e.code == 404:
            raise GitHubFileNotFound(
                f"Ressource introuvable sur GitHub : {gh_msg}",
                status_code=404,
                cause=e,
            ) from e
        if e.code == 409 or (e.code == 422 and "sha" in gh_msg.lower()):
            raise GitHubConflict(
                f"Conflit GitHub (le fichier a été modifié entre-temps) : {gh_msg}",
                status_code=e.code,
                cause=e,
            ) from e
        if e.code == 401:
            raise GitHubSyncError(
                "Token GitHub rejeté (401). Vérifie qu'il n'a pas expiré et "
                "qu'il a bien la permission `Contents: Read and write` sur ce repo.",
                status_code=401,
                cause=e,
            ) from e
        if e.code == 403:
            raise GitHubSyncError(
                f"Accès refusé par GitHub (403) : {gh_msg}. Le token a-t-il "
                "accès à ce repo ?",
                status_code=403,
                cause=e,
            ) from e
        raise GitHubSyncError(
            f"GitHub a refusé la requête ({e.code}) : {gh_msg}",
            status_code=e.code,
            cause=e,
        ) from e
    except URLError as e:
        raise GitHubSyncError(
            f"Impossible de joindre GitHub : {e.reason}", cause=e
        ) from e


# --- File read / write -----------------------------------------------------

_DEFAULT_FILE_HEADER = (
    "# Patterns de détection appris — complétés via l'app (Jalon 2.5) ou\n"
    "# à la main. Voir src/learned_patterns.py pour le format attendu.\n"
)


def fetch_patterns_yaml(cfg: GitHubConfig) -> RemoteFile:
    """GET the current learned_patterns.yml from the configured branch.

    If the file doesn't exist yet (404), returns a RemoteFile with a default
    header + empty `patterns: []` body and an empty sha. `commit_file` will
    then create the file on the next PUT.
    """
    url = (
        f"{API_BASE}/repos/{cfg.repo}/contents/{cfg.path}"
        f"?ref={cfg.branch}"
    )
    try:
        payload = _http("GET", url, cfg.token)
    except GitHubFileNotFound:
        seed = _DEFAULT_FILE_HEADER + "patterns: []\n"
        return RemoteFile(text=seed, sha="")
    content_b64 = payload.get("content") or ""
    sha = payload.get("sha") or ""
    # GitHub returns base64 with embedded newlines.
    try:
        text = base64.b64decode(content_b64).decode("utf-8")
    except Exception as e:
        raise GitHubSyncError(
            "Réponse GitHub illisible (base64 invalide).", cause=e
        ) from e
    return RemoteFile(text=text, sha=sha)


def commit_file(
    cfg: GitHubConfig,
    new_text: str,
    *,
    sha: str,
    message: str,
    author_name: Optional[str] = None,
    author_email: Optional[str] = None,
) -> dict:
    """PUT the new file content.

    When ``sha`` is empty, the call creates the file instead of updating it.
    GitHub distinguishes these two modes by the presence/absence of the
    ``sha`` field in the body. Returns the raw GitHub response (includes
    the new commit sha + file sha, useful for tests/logging).
    """
    url = f"{API_BASE}/repos/{cfg.repo}/contents/{cfg.path}"
    encoded = base64.b64encode(new_text.encode("utf-8")).decode("ascii")
    body: dict[str, Any] = {
        "message": message,
        "content": encoded,
        "branch": cfg.branch,
    }
    # Only include sha when we're updating an existing file — otherwise
    # GitHub returns 422 "sha should be null for new files".
    if sha:
        body["sha"] = sha
    if author_name or author_email:
        body["committer"] = {
            "name": author_name or "Revio Onboarding Bot",
            "email": author_email or "bot@gorevio.co",
        }
    return _http("PUT", url, cfg.token, body=body)


# --- Pattern add / update / remove ----------------------------------------

def _parse_yaml(text: str) -> dict:
    """Safe-load the YAML text, preserving the existing structure or seeding it."""
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        raise GitHubSyncError(
            "Le fichier learned_patterns.yml est mal formé sur GitHub. "
            "Corrige-le à la main avant de réessayer.",
            cause=e,
        ) from e
    if not isinstance(data, dict):
        data = {}
    if not isinstance(data.get("patterns"), list):
        data["patterns"] = []
    return data


def _dump_yaml(data: dict, *, keep_header: str = "") -> str:
    """Serialize back to YAML, preserving a leading comment block if provided."""
    body = yaml.safe_dump(
        data, sort_keys=False, allow_unicode=True, default_flow_style=False
    )
    if keep_header:
        # Keep a trailing newline between the header and the body.
        if not keep_header.endswith("\n"):
            keep_header += "\n"
        return keep_header + body
    return body


def _extract_header(text: str) -> str:
    """Return the leading comment block (lines starting with '#' or blank)."""
    lines = text.splitlines()
    header_lines: list[str] = []
    for ln in lines:
        stripped = ln.strip()
        if stripped.startswith("#") or stripped == "":
            header_lines.append(ln)
        else:
            break
    return "\n".join(header_lines)


def upsert_pattern(text: str, pattern_entry: dict) -> str:
    """Append or replace a pattern entry (keyed by `id`) in the YAML text.

    Returns the new YAML text. Idempotent: calling twice with the same
    entry produces the same output.
    """
    header = _extract_header(text)
    data = _parse_yaml(text)
    patterns = data["patterns"]
    pid = str(pattern_entry.get("id") or "").strip()
    if not pid:
        raise GitHubSyncError(
            "Le pattern à enregistrer n'a pas d'`id`. Aborte sans écrire."
        )
    replaced = False
    for i, existing in enumerate(patterns):
        if isinstance(existing, dict) and str(existing.get("id") or "") == pid:
            patterns[i] = pattern_entry
            replaced = True
            break
    if not replaced:
        patterns.append(pattern_entry)
    data["patterns"] = patterns
    return _dump_yaml(data, keep_header=header)


def remove_pattern(text: str, pattern_id: str) -> tuple[str, bool]:
    """Remove a pattern by id. Returns (new_text, removed_bool)."""
    pid = str(pattern_id or "").strip()
    if not pid:
        return text, False
    header = _extract_header(text)
    data = _parse_yaml(text)
    before = len(data["patterns"])
    data["patterns"] = [
        p for p in data["patterns"]
        if not (isinstance(p, dict) and str(p.get("id") or "") == pid)
    ]
    removed = len(data["patterns"]) < before
    if not removed:
        return text, False
    return _dump_yaml(data, keep_header=header), True


# --- High-level API used by the UI -----------------------------------------

_MAX_CONFLICT_RETRIES = 3


def _commit_with_retry(
    cfg: GitHubConfig,
    *,
    mutate,           # callable(text: str) -> tuple[str, bool]  (new_text, should_commit)
    message: str,
    author_email: Optional[str] = None,
) -> dict:
    """Run fetch → mutate → commit with auto-retry on sha conflicts.

    `mutate` receives the current file text and returns ``(new_text, should_commit)``.
    If ``should_commit`` is False, we return ``{'skipped': True}``. If the PUT
    fails with a conflict (someone else wrote between our fetch and our PUT),
    we re-fetch and re-apply `mutate` up to _MAX_CONFLICT_RETRIES times.
    """
    last_err: Optional[GitHubConflict] = None
    for attempt in range(_MAX_CONFLICT_RETRIES):
        remote = fetch_patterns_yaml(cfg)
        new_text, should_commit = mutate(remote.text)
        if not should_commit:
            return {"skipped": True, "reason": "no_change"}
        try:
            return commit_file(
                cfg,
                new_text,
                sha=remote.sha,
                message=message,
                author_email=author_email,
            )
        except GitHubConflict as e:
            last_err = e
            # Loop: re-fetch and re-apply the mutation on top of the latest text.
            continue
    assert last_err is not None  # pragma: no cover
    raise GitHubSyncError(
        "Plusieurs écritures concurrentes détectées, impossible de "
        f"synchroniser après {_MAX_CONFLICT_RETRIES} essais. Réessaie "
        "dans quelques secondes.",
        cause=last_err,
    )


def save_pattern(
    pattern_entry: dict,
    *,
    commit_message: Optional[str] = None,
    author_email: Optional[str] = None,
) -> dict:
    """Read the current YAML, upsert the pattern, commit. Returns GitHub resp.

    Safe under concurrent writes — if another commit lands between our
    fetch and our PUT, we automatically re-apply the upsert on the fresh
    text and retry (up to 3 times).
    """
    cfg = get_config()
    pid = pattern_entry.get("id") or "unknown"
    msg = commit_message or f"learned_patterns: upsert `{pid}` via app"

    def mutate(text: str) -> tuple[str, bool]:
        new_text = upsert_pattern(text, pattern_entry)
        return new_text, (new_text != text)

    return _commit_with_retry(cfg, mutate=mutate, message=msg, author_email=author_email)


def delete_pattern(
    pattern_id: str,
    *,
    commit_message: Optional[str] = None,
    author_email: Optional[str] = None,
) -> dict:
    """Read, remove the given id, commit. Returns {'skipped': True} if absent."""
    cfg = get_config()
    msg = commit_message or f"learned_patterns: remove `{pattern_id}` via app"

    def mutate(text: str) -> tuple[str, bool]:
        new_text, removed = remove_pattern(text, pattern_id)
        return new_text, removed

    result = _commit_with_retry(cfg, mutate=mutate, message=msg, author_email=author_email)
    # Translate the generic "no_change" into the more specific "not_found"
    # so the UI can show a dedicated message ("ce pattern n'existait plus").
    if result.get("skipped") and result.get("reason") == "no_change":
        return {"skipped": True, "reason": "not_found"}
    return result


# --- Diagnostic ------------------------------------------------------------

def check_connection() -> dict:
    """Verify the configured token + repo + path work end-to-end (read-only).

    Returns a dict with: ok (bool), repo, branch, path, patterns_count (int),
    file_exists (bool), message (str). Suitable for rendering in a
    "🔌 Tester la connexion" UI block. Never raises — all errors are
    flattened into ``ok=False`` with an explanatory ``message``.
    """
    try:
        cfg = get_config()
    except GitHubNotConfigured as e:
        return {
            "ok": False,
            "configured": False,
            "message": e.user_message,
        }
    result: dict[str, Any] = {
        "configured": True,
        "repo": cfg.repo,
        "branch": cfg.branch,
        "path": cfg.path,
    }
    try:
        remote = fetch_patterns_yaml(cfg)
    except GitHubSyncError as e:
        result.update(ok=False, message=e.user_message)
        return result
    # fetch_patterns_yaml returns a seeded empty file when the path doesn't
    # exist; distinguish that case so the user knows they'll be creating it.
    file_exists = bool(remote.sha)
    try:
        data = yaml.safe_load(remote.text) or {}
        patterns = data.get("patterns") if isinstance(data, dict) else None
        count = len(patterns) if isinstance(patterns, list) else 0
    except yaml.YAMLError:
        count = -1
    result.update(
        ok=True,
        file_exists=file_exists,
        patterns_count=count,
        message=(
            f"Connexion OK · {count} pattern(s) dans `{cfg.path}`"
            if file_exists
            else f"Connexion OK · le fichier `{cfg.path}` n'existe pas encore "
                 "(il sera créé au premier enregistrement)"
        ),
    )
    return result


# --- Generic file read/write (Jalon 2.7 — used by value_mappings.yml) -------

def fetch_file(cfg: GitHubConfig, path: str) -> RemoteFile:
    """GET any file at ``path`` from the configured repo/branch.

    Returns ``RemoteFile(text="", sha="")`` on 404 so the caller can create
    the file on first commit. Unlike ``fetch_patterns_yaml``, does NOT seed
    with a YAML header — callers that need one must handle it themselves.
    """
    url = f"{API_BASE}/repos/{cfg.repo}/contents/{path}?ref={cfg.branch}"
    try:
        payload = _http("GET", url, cfg.token)
    except GitHubFileNotFound:
        return RemoteFile(text="", sha="")
    content_b64 = payload.get("content") or ""
    sha = payload.get("sha") or ""
    try:
        text = base64.b64decode(content_b64).decode("utf-8")
    except Exception as e:
        raise GitHubSyncError(
            "Réponse GitHub illisible (base64 invalide).", cause=e
        ) from e
    return RemoteFile(text=text, sha=sha)


def commit_file_at(
    cfg: GitHubConfig,
    path: str,
    new_text: str,
    *,
    sha: str,
    message: str,
    author_name: Optional[str] = None,
    author_email: Optional[str] = None,
) -> dict:
    """Commit ``new_text`` at an arbitrary ``path`` (overrides cfg.path).

    Same semantics as ``commit_file`` (empty sha = create, non-empty = update).
    """
    url = f"{API_BASE}/repos/{cfg.repo}/contents/{path}"
    encoded = base64.b64encode(new_text.encode("utf-8")).decode("ascii")
    body: dict[str, Any] = {
        "message": message,
        "content": encoded,
        "branch": cfg.branch,
    }
    if sha:
        body["sha"] = sha
    if author_name or author_email:
        body["committer"] = {
            "name": author_name or "Revio Onboarding Bot",
            "email": author_email or "bot@gorevio.co",
        }
    return _http("PUT", url, cfg.token, body=body)


# --- value_mappings.yml ---------------------------------------------------

def _value_mappings_path() -> str:
    """Resolve the repo-relative path of value_mappings.yml from secrets."""
    return _read_secret("GITHUB_VALUE_MAPPINGS_PATH") or DEFAULT_VALUE_MAPPINGS_PATH


def fetch_value_mappings_yaml() -> Optional[str]:
    """Return the current value_mappings.yml text from GitHub, or None if the
    file doesn't exist yet on the configured branch."""
    cfg = get_config()
    remote = fetch_file(cfg, _value_mappings_path())
    return remote.text if remote.sha else None


def save_learned_patterns_yaml(
    yaml_text: str,
    *,
    commit_message: str = "learned_patterns: update via app",
    author_email: Optional[str] = None,
) -> dict:
    """Commit the full learned_patterns.yml to GitHub (Jalon 4.2 — unknown
    columns flow).

    Unlike ``save_pattern`` (which does surgical upserts keyed by pattern id),
    this is a whole-file commit used by the « colonne non identifiée » flow
    where the app has already serialized the complete YAML via
    ``unknown_columns.register_learned_column`` + ``learned_patterns.dump``.
    Retries on sha conflicts.
    """
    cfg = get_config()
    path = cfg.path  # already defaults to learned_patterns.yml via DEFAULT_PATH
    msg = commit_message

    last_err: Optional[GitHubConflict] = None
    for _attempt in range(_MAX_CONFLICT_RETRIES):
        remote = fetch_file(cfg, path)
        if remote.text == yaml_text:
            return {"skipped": True, "reason": "no_change"}
        try:
            return commit_file_at(
                cfg, path, yaml_text,
                sha=remote.sha, message=msg, author_email=author_email,
            )
        except GitHubConflict as e:
            last_err = e
            continue
    assert last_err is not None  # pragma: no cover
    raise GitHubSyncError(
        "Écritures concurrentes sur learned_patterns.yml, abandon après "
        f"{_MAX_CONFLICT_RETRIES} essais. Réessaie dans quelques secondes.",
        cause=last_err,
    )


def save_learned_columns_yaml(
    yaml_text: str,
    *,
    commit_message: str = "learned_columns: update via app",
    author_email: Optional[str] = None,
) -> dict:
    """Commit the full learned_columns.yml to GitHub (Jalon 4.2.8).

    Distinct from ``save_learned_patterns_yaml`` : learned_columns.yml stores
    Contract-side field → source column overrides (dict shape), while
    learned_patterns.yml stores Vehicle format signatures (list shape). Mixing
    them in one file crashed the Contract Mémoriser flow in 4.2.7.
    """
    cfg = get_config()
    path = "src/rules/learned_columns.yml"

    last_err: Optional[GitHubConflict] = None
    for _attempt in range(_MAX_CONFLICT_RETRIES):
        remote = fetch_file(cfg, path)
        if remote.text == yaml_text:
            return {"skipped": True, "reason": "no_change"}
        try:
            return commit_file_at(
                cfg, path, yaml_text,
                sha=remote.sha, message=commit_message, author_email=author_email,
            )
        except GitHubConflict as e:
            last_err = e
            continue
    assert last_err is not None  # pragma: no cover
    raise GitHubSyncError(
        "Écritures concurrentes sur learned_columns.yml, abandon après "
        f"{_MAX_CONFLICT_RETRIES} essais. Réessaie dans quelques secondes.",
        cause=last_err,
    )


# --- Branch + Pull Request (Jalon 5.0.2 — Mode Dev) ------------------------
#
# Mode Dev lets the user commit YAML patches to a NEW branch and open a PR
# on main, rather than pushing straight to main like the learned_patterns
# auto-commit does. Rationale: patches generated from a natural-language
# prompt are riskier than learned patterns (which are built from concrete
# user inputs), so we want a reviewable code-review step before anything
# lands on the branch Streamlit Cloud auto-deploys from.


def get_branch_sha(cfg: GitHubConfig, branch: Optional[str] = None) -> str:
    """Return the commit sha at the tip of ``branch`` (defaults to cfg.branch).

    Raised :class:`GitHubFileNotFound` if the branch doesn't exist — so the
    caller can distinguish "branch missing" from a more generic error.
    """
    ref = branch or cfg.branch
    url = f"{API_BASE}/repos/{cfg.repo}/git/ref/heads/{ref}"
    payload = _http("GET", url, cfg.token)
    obj = payload.get("object") or {}
    sha = obj.get("sha") or ""
    if not sha:
        raise GitHubSyncError(
            f"Réponse GitHub inattendue : pas de sha pour la branche `{ref}`."
        )
    return sha


def create_branch(
    cfg: GitHubConfig,
    new_branch: str,
    *,
    from_sha: Optional[str] = None,
) -> dict:
    """Create ``new_branch`` pointing at ``from_sha`` (default: tip of cfg.branch).

    If the branch already exists we return a dict with ``already_exists=True``
    instead of raising — Mode Dev generates timestamped branch names so a
    collision indicates the user double-clicked, not a real conflict.
    """
    base_sha = from_sha or get_branch_sha(cfg)
    url = f"{API_BASE}/repos/{cfg.repo}/git/refs"
    body = {"ref": f"refs/heads/{new_branch}", "sha": base_sha}
    try:
        return _http("POST", url, cfg.token, body=body)
    except GitHubSyncError as e:
        # 422 "Reference already exists" → soft result, not an error.
        msg = (e.user_message or "").lower()
        if "already exists" in msg or "reference already exists" in msg:
            return {"already_exists": True, "ref": body["ref"]}
        raise


def create_pull_request(
    cfg: GitHubConfig,
    *,
    head: str,
    base: Optional[str] = None,
    title: str,
    body: str = "",
    draft: bool = False,
) -> dict:
    """Open a PR from ``head`` to ``base`` (default: cfg.branch, usually main).

    Returns the full GitHub response (includes ``html_url`` for the PR page
    and ``number`` for the PR id, both useful for the UI).
    """
    base_branch = base or cfg.branch
    url = f"{API_BASE}/repos/{cfg.repo}/pulls"
    payload = {
        "title": title,
        "head": head,
        "base": base_branch,
        "body": body,
        "draft": draft,
    }
    return _http("POST", url, cfg.token, body=payload)


def commit_file_on_branch(
    cfg: GitHubConfig,
    path: str,
    new_text: str,
    *,
    branch: str,
    sha: str,
    message: str,
    author_name: Optional[str] = None,
    author_email: Optional[str] = None,
) -> dict:
    """Commit ``new_text`` at ``path`` on an arbitrary branch (not cfg.branch).

    Mirrors :func:`commit_file_at` but lets the caller target a branch other
    than the configured default — used by Mode Dev to commit on a fresh
    ``mode-dev/<slug>`` branch before opening the PR.
    """
    url = f"{API_BASE}/repos/{cfg.repo}/contents/{path}"
    encoded = base64.b64encode(new_text.encode("utf-8")).decode("ascii")
    body: dict[str, Any] = {
        "message": message,
        "content": encoded,
        "branch": branch,
    }
    if sha:
        body["sha"] = sha
    if author_name or author_email:
        body["committer"] = {
            "name": author_name or "Revio Onboarding Bot",
            "email": author_email or "bot@gorevio.co",
        }
    return _http("PUT", url, cfg.token, body=body)


def fetch_file_on_branch(cfg: GitHubConfig, path: str, branch: str) -> RemoteFile:
    """Read ``path`` from a specific branch (not cfg.branch).

    Returns an empty :class:`RemoteFile` (text="", sha="") on 404 so callers
    can decide whether to create the file on first commit.
    """
    url = f"{API_BASE}/repos/{cfg.repo}/contents/{path}?ref={branch}"
    try:
        payload = _http("GET", url, cfg.token)
    except GitHubFileNotFound:
        return RemoteFile(text="", sha="")
    content_b64 = payload.get("content") or ""
    sha = payload.get("sha") or ""
    try:
        text = base64.b64decode(content_b64).decode("utf-8")
    except Exception as e:
        raise GitHubSyncError(
            "Réponse GitHub illisible (base64 invalide).", cause=e
        ) from e
    return RemoteFile(text=text, sha=sha)


def save_value_mappings_yaml(
    yaml_text: str,
    *,
    commit_message: str = "value_mappings: update via app",
    author_email: Optional[str] = None,
) -> dict:
    """Commit the full value_mappings.yml to GitHub.

    Unlike patterns, there is no merge logic here: the app holds the whole
    mapping dict in memory, serializes it with ``vm.dump_yaml``, and we
    overwrite the remote file. If another commit landed between our fetch
    and our PUT, we retry once with the latest sha — beyond that the caller
    gets a ``GitHubConflict`` to display to the user (who can reload).
    """
    cfg = get_config()
    path = _value_mappings_path()
    msg = commit_message

    last_err: Optional[GitHubConflict] = None
    for _attempt in range(_MAX_CONFLICT_RETRIES):
        remote = fetch_file(cfg, path)
        if remote.text == yaml_text:
            return {"skipped": True, "reason": "no_change"}
        try:
            return commit_file_at(
                cfg, path, yaml_text,
                sha=remote.sha, message=msg, author_email=author_email,
            )
        except GitHubConflict as e:
            last_err = e
            continue
    assert last_err is not None  # pragma: no cover
    raise GitHubSyncError(
        "Écritures concurrentes sur value_mappings.yml, abandon après "
        f"{_MAX_CONFLICT_RETRIES} essais. Réessaie dans quelques secondes.",
        cause=last_err,
    )

"""Password gate for the Streamlit app.

Reads a shared password from ``st.secrets["app_password"]`` (configured
in Streamlit Cloud → Settings → Secrets) and requires the user to type
it before the rest of the UI renders.

Persistence
-----------
The "stay logged in" behavior relies on a signed cookie set via
``extra-streamlit-components``. The cookie contains a fingerprint of the
current password: as long as the password doesn't change, the user stays
logged in for ``COOKIE_TTL_DAYS`` days (default 7) across browser
refreshes and tab closures. Rotating the password in Streamlit Secrets
immediately invalidates all existing cookies.

Graceful fallbacks
------------------
- If ``app_password`` is not set → auth is DISABLED (dev mode, everyone
  gets through with a visible banner in the sidebar).
- If ``extra-streamlit-components`` is not installed → auth still works,
  but falls back to in-memory session only (user retypes on refresh).

Public API
----------
``require_password()`` — call once, at the very top of ``app.py``, before
anything else renders. Returns ``True`` when authenticated, otherwise
blocks execution via ``st.stop()``.
"""

from __future__ import annotations

import hashlib
import hmac
from pathlib import Path
from typing import Optional

import streamlit as st


# =============================================================================
# Config
# =============================================================================

COOKIE_NAME = "rv_auth"
COOKIE_TTL_DAYS = 7
# Used to salt the cookie fingerprint so an attacker can't forge a cookie
# from just the password. In practice the "secret" doesn't need to be
# cryptographically strong — the password itself is the real gate.
_COOKIE_SALT = "revio-onboarding-v1"


# =============================================================================
# Helpers
# =============================================================================

def _expected_password() -> Optional[str]:
    """Return the password configured for the app, or None if unset."""
    try:
        pwd = st.secrets.get("app_password", "")  # type: ignore[attr-defined]
    except Exception:
        pwd = ""
    # Accept either key for resilience if the user misnamed the secret.
    if not pwd:
        try:
            pwd = st.secrets.get("APP_PASSWORD", "")  # type: ignore[attr-defined]
        except Exception:
            pwd = ""
    return pwd or None


def _cookie_fingerprint(password: str) -> str:
    """Hash of the password (+ salt) used as the cookie value.

    Changing the password invalidates all existing cookies automatically.
    """
    mac = hmac.new(_COOKIE_SALT.encode(), password.encode(), hashlib.sha256)
    return mac.hexdigest()


def _get_cookie_manager():
    """Return a CookieManager instance if the lib is available, else None.

    Cached on ``st.session_state`` so we don't instantiate a new widget on
    every rerun (which would re-mount the JS component and flash).
    """
    if "_rv_cookie_manager" in st.session_state:
        return st.session_state["_rv_cookie_manager"]
    try:
        import extra_streamlit_components as stx  # type: ignore
    except Exception:
        st.session_state["_rv_cookie_manager"] = None
        return None
    mgr = stx.CookieManager(key="rv_cookie_mgr")
    st.session_state["_rv_cookie_manager"] = mgr
    return mgr


def _login_screen(password_hint_shown: bool) -> None:
    """Render a centered login form and stop execution.

    Shown until the user submits the correct password.
    """
    # Use a narrow central column for a clean, non-full-width card.
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        # Logo if available.
        asset_dir = Path(__file__).parent / "assets"
        wordmark = asset_dir / "logo_wordmark.svg"
        if wordmark.exists():
            st.image(str(wordmark), width=220)
        st.markdown("## Accès restreint")
        st.caption(
            "Outil interne Revio — saisissez le mot de passe d'équipe "
            "pour accéder à l'application."
        )

        with st.form("rv_login_form", clear_on_submit=False):
            pwd = st.text_input(
                "Mot de passe",
                type="password",
                placeholder="•••••••",
                label_visibility="collapsed",
            )
            submitted = st.form_submit_button(
                "Entrer",
                type="primary",
                use_container_width=True,
            )

        if submitted:
            expected = _expected_password()
            if expected and pwd == expected:
                st.session_state["_auth_ok"] = True
                # Drop a cookie so refreshes don't log the user out.
                mgr = _get_cookie_manager()
                if mgr is not None:
                    import datetime as _dt
                    mgr.set(
                        COOKIE_NAME,
                        _cookie_fingerprint(expected),
                        expires_at=_dt.datetime.now()
                        + _dt.timedelta(days=COOKIE_TTL_DAYS),
                        key="rv_cookie_set",
                    )
                st.rerun()
            else:
                st.error("Mot de passe incorrect.")

        if password_hint_shown:
            st.caption(
                "⚠️ Aucun mot de passe configuré côté serveur — voir "
                "Streamlit Cloud → Settings → Secrets pour en définir un."
            )
    st.stop()


# =============================================================================
# Public API
# =============================================================================

def require_password() -> bool:
    """Gate the app behind a password prompt.

    Call this at the very top of ``app.py`` (right after
    ``st.set_page_config``). Returns ``True`` when the user is
    authenticated. When not, renders the login screen and calls
    ``st.stop()`` — callers after this line are guaranteed to be
    running as an authenticated user.

    If no ``app_password`` is configured in secrets, authentication is
    disabled and this function returns ``True`` immediately (dev mode).
    """
    expected = _expected_password()
    if not expected:
        # Dev mode: no password set, let everything through.
        st.session_state.setdefault("_auth_ok", True)
        st.session_state["_auth_disabled"] = True
        return True

    # Already authenticated in this session → fast-path.
    if st.session_state.get("_auth_ok"):
        return True

    # First visit or post-refresh: try to restore from cookie.
    if not st.session_state.get("_auth_checked"):
        st.session_state["_auth_checked"] = True
        mgr = _get_cookie_manager()
        if mgr is not None:
            try:
                existing = mgr.get(COOKIE_NAME)
            except Exception:
                existing = None
            if existing and existing == _cookie_fingerprint(expected):
                st.session_state["_auth_ok"] = True
                return True

    # Otherwise → block with login screen.
    _login_screen(password_hint_shown=False)
    return False  # unreachable — st.stop() above


def logout() -> None:
    """Log the user out (clear session flag + delete cookie).

    Intended to be wired to a "Déconnexion" button in the sidebar.
    """
    st.session_state["_auth_ok"] = False
    st.session_state["_auth_checked"] = False
    mgr = _get_cookie_manager()
    if mgr is not None:
        try:
            mgr.delete(COOKIE_NAME, key="rv_cookie_del")
        except Exception:
            # Cookie wasn't there or JS component not mounted — fine.
            pass


def is_auth_disabled() -> bool:
    """Return True when no password was configured (dev mode)."""
    return bool(st.session_state.get("_auth_disabled"))

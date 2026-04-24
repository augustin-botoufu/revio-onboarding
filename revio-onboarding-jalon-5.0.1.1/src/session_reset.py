"""Session reset — "Nouvel import" action.

The app stores every uploaded file, engine override, fleet mapping,
normalization cache, … in ``st.session_state``. Between two clients the
user should be able to start fresh without refreshing the browser (which
would also log them out).

``reset_import_state()`` wipes all import-scoped keys while preserving:
- auth flags (``_auth_ok``, ``_auth_checked``) — so the user stays logged
  in after the reset;
- navigation (``mode``) — so the reset doesn't bounce them to Accueil;
- diagnostic caches (GitHub connection check) — they're not tied to a
  specific import.

Any key starting with ``_auth`` or ``rv_cookie_`` is kept (the cookie
manager from extra-streamlit-components stores internal widget state
prefixed ``rv_cookie_``).

The function is pure Python on a mapping-like object (``st.session_state``
quacks like a dict) so it can be unit-tested without a Streamlit runtime.
"""

from __future__ import annotations

from typing import MutableMapping


# Explicit whitelist — keys that survive a "Nouvel import" action.
# Everything else in session_state is dropped.
PRESERVED_KEYS: frozenset[str] = frozenset({
    "mode",              # current sidebar nav page
    "gh_check_result",   # GitHub sync diagnostic (user-triggered test)
    "rules_active_table",# UI preference (which tab is open on /Règles)
})

# Prefixes of keys that survive (used by the cookie-based auth — we don't
# want to wipe the cookie widget's internal state, otherwise the user
# gets logged out on every reset).
PRESERVED_PREFIXES: tuple[str, ...] = (
    "_auth",        # _auth_ok, _auth_checked
    "rv_cookie_",   # extra-streamlit-components CookieManager widget state
)


def _is_preserved(key: str) -> bool:
    """Return True if ``key`` should NOT be removed on reset."""
    if key in PRESERVED_KEYS:
        return True
    return any(key.startswith(p) for p in PRESERVED_PREFIXES)


def reset_import_state(session_state: MutableMapping) -> list[str]:
    """Wipe import-scoped keys from ``session_state``.

    Returns the list of keys removed (useful for debugging / tests).
    """
    to_remove = [k for k in list(session_state.keys()) if not _is_preserved(k)]
    for k in to_remove:
        try:
            del session_state[k]
        except KeyError:
            # Key disappeared under us — fine.
            pass
    return to_remove

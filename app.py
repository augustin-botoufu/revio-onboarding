"""Revio Onboarding Tool - Streamlit app.

Run locally:
    pip install -r requirements.txt
    streamlit run app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.detectors import detect, label_for, SOURCE_TYPE_LABELS
from src.llm_mapper import propose_mapping
from src.pipeline import (
    SourceFile,
    load_tabular,
    merge_per_schema,
    validate,
    apply_mapping,
    merge_engine_sources,
)
from src.output_writer import build_zip, split_by_fleet
from src.schemas import SCHEMAS, header_for
from src import rules_engine
from src import rules_io
from src import learned_patterns as lp
from src.excel_report import build_report_xlsx


# Mapping between the `detect()` source_type and the YAML `source` slug.
# Sources not listed are unavailable to the rules engine (templates, etc.).
DETECTOR_TO_YAML_SLUG = {
    "api_plaques": "api_plaques",
    "ayvens_etat_parc": "ayvens_etat_parc",
    "ayvens_aen": "ayvens_aen",
    "ayvens_tvs": "ayvens_tvs",
    "ayvens_and": "ayvens_and",
    "arval_uat": "arval_uat",
    "arval_aen": "arval_aen",
    "arval_tvu": "arval_tvu",
    "arval_and": "arval_and",
    "client_vehicle": "client_file",
}

# All YAML slugs the engine understands (for the override dropdown).
ENGINE_SOURCE_SLUGS = [
    "api_plaques",
    "ayvens_etat_parc",
    "ayvens_aen",
    "ayvens_tvs",
    "ayvens_and",
    "ayvens_pneus",
    "arval_uat",
    "arval_aen",
    "arval_tvu",
    "arval_and",
    "arval_pneus",
    "autre_loueur_etat_parc",
    "autre_loueur_aen",
    "autre_loueur_tvs",
    "autre_loueur_and",
    "autre_loueur_pneus",
    "assurance_externe",
    "client_file",
]

# Human-readable label + icon for each source slug, for the grouped cards
# in the Moteur page. The tuple is (emoji, label, group_order) — group_order
# drives the rendering order of the cards so the page reads top-to-bottom in
# a predictable way (plaques first, then by lessor, then catch-alls at the end).
SLUG_DISPLAY: dict[str, tuple[str, str, int]] = {
    "api_plaques":             ("🔢", "API Plaques",                  10),
    "ayvens_etat_parc":        ("🚗", "Ayvens — État de parc",        20),
    "ayvens_aen":              ("🚗", "Ayvens — Avis d'échéance",     21),
    "ayvens_tvs":              ("🚗", "Ayvens — TVS",                  22),
    "ayvens_and":              ("🚗", "Ayvens — Avis non dépôt",       23),
    "ayvens_pneus":            ("🚗", "Ayvens — Pneus",                24),
    "arval_uat":               ("🚙", "Arval — UAT",                   30),
    "arval_aen":               ("🚙", "Arval — Avis d'échéance",       31),
    "arval_tvu":               ("🚙", "Arval — TVU",                   32),
    "arval_and":               ("🚙", "Arval — Avis non dépôt",        33),
    "arval_pneus":             ("🚙", "Arval — Pneus",                 34),
    "autre_loueur_etat_parc":  ("📘", "Autre loueur — État de parc",   40),
    "autre_loueur_aen":        ("📘", "Autre loueur — Avis d'échéance",41),
    "autre_loueur_tvs":        ("📘", "Autre loueur — TVS",            42),
    "autre_loueur_and":        ("📘", "Autre loueur — Avis non dépôt", 43),
    "autre_loueur_pneus":      ("📘", "Autre loueur — Pneus",          44),
    "assurance_externe":       ("🛡️", "Assurance externe",             50),
    "client_file":             ("👤", "Fichier client",                90),
}


def slug_display(slug: str) -> tuple[str, str, int]:
    """Return (emoji, label, order) for a slug. Unknown slugs fall back gracefully."""
    return SLUG_DISPLAY.get(slug, ("📄", slug, 99))


# Fields that need a manual column mapping at upload time. Used for slugs
# whose column names CAN'T be pre-declared in vehicle.yml, because the
# format isn't known in advance:
#   - client_file = client's free-form vehicle spreadsheet
#   - autre_loueur_etat_parc = unknown lessor export
# For both slugs, the user (or the AI) fills the source column name per
# Revio field at runtime. The rules engine picks up these overrides as a
# fallback when the YAML rule has no `column:` key.
MANUAL_MAPPABLE_FIELDS = [
    "registrationPlate",
    "usage",
    "parcEntryAt",
    "registrationIssueCountryCode",
    "brand",
    "model",
    "variant",
    "motorisation",
    "co2gKm",
    "registrationIssueDate",
    "registrationVin",
    "registrationFiscalPower",
]

# Alias kept so other parts of the codebase that reference the old name
# keep working. Safe to remove once all call sites are migrated.
CLIENT_FILE_MAPPABLE_FIELDS = MANUAL_MAPPABLE_FIELDS

# Slugs that require a manual column mapping (LLM-assisted or hand-picked)
# before the rules engine can read anything from the file. All OTHER slugs
# (ayvens_*, arval_*, api_plaques, …) have their column names baked into
# vehicle.yml and don't need this step.
SLUGS_NEEDING_MANUAL_MAPPING = {"client_file", "autre_loueur_etat_parc"}


# ========== Page config ==========
st.set_page_config(
    page_title="Revio — Onboarding",
    page_icon=str(Path(__file__).parent / "src" / "assets" / "logo.svg")
    if (Path(__file__).parent / "src" / "assets" / "logo.svg").exists()
    else "🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ========== Branding (logo in the top-left, replaces the old sidebar title) ==========
_ASSET_DIR = Path(__file__).parent / "src" / "assets"
_LOGO_WORDMARK = _ASSET_DIR / "logo_wordmark.svg"  # full "R + revio"
_LOGO_ICON = _ASSET_DIR / "logo.svg"               # just the R (sidebar collapsed)
if _LOGO_WORDMARK.exists():
    try:
        st.logo(
            str(_LOGO_WORDMARK),
            icon_image=str(_LOGO_ICON) if _LOGO_ICON.exists() else None,
            size="large",
        )
    except TypeError:
        # Streamlit <1.37: `size` kwarg not supported.
        st.logo(
            str(_LOGO_WORDMARK),
            icon_image=str(_LOGO_ICON) if _LOGO_ICON.exists() else None,
        )


# ========== Custom style (typography + component polish) ==========
def _inject_custom_style() -> None:
    """Give the app a modern, editorial look on top of the base theme.

    - Inter (Google Fonts) as the primary typeface.
    - Softer borders, unified 8-10px radius, tightened spacing.
    - Hides Streamlit's default top chrome (menu, deploy button, footer).
    Additive: the .streamlit/config.toml theme stays the source of truth
    for colors — this stylesheet only refines typography + components.
    """
    st.markdown(
        """
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

          html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stSidebar"] {
              font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif !important;
          }

          /* Hide Streamlit's default top chrome */
          #MainMenu { visibility: hidden; }
          footer { visibility: hidden; }
          header[data-testid="stHeader"] { background: transparent; height: 0; }

          .main .block-container {
              padding-top: 2.25rem;
              padding-bottom: 4rem;
              max-width: 1240px;
          }

          /* Typography */
          h1, h2, h3, h4, h5, h6 {
              font-family: 'Inter', sans-serif !important;
              font-weight: 600 !important;
              letter-spacing: -0.015em;
              color: #0F172A;
          }
          h1 { font-size: 1.875rem !important; line-height: 1.2; }
          h2 { font-size: 1.375rem !important; margin-top: 1.75rem !important; }
          h3 { font-size: 1.0625rem !important; margin-top: 1.25rem !important; color: #1E293B !important; }
          [data-testid="stCaptionContainer"], .stCaption { color: #64748B !important; font-size: 0.85rem; }

          /* Buttons */
          .stButton > button,
          [data-testid="stFormSubmitButton"] > button,
          [data-testid="stDownloadButton"] > button {
              border-radius: 8px !important;
              border: 1px solid #E2E8F0 !important;
              font-weight: 500 !important;
              padding: 0.5rem 1rem !important;
              transition: all 0.15s ease !important;
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
          }
          .stButton > button:hover,
          [data-testid="stFormSubmitButton"] > button:hover,
          [data-testid="stDownloadButton"] > button:hover {
              border-color: #CBD5E1 !important;
              background: #F8FAFC !important;
          }
          .stButton > button[kind="primary"],
          [data-testid="stFormSubmitButton"] > button[kind="primary"],
          [data-testid="stDownloadButton"] > button[kind="primary"] {
              background: #0F172A !important;
              color: #FFFFFF !important;
              border-color: #0F172A !important;
          }
          .stButton > button[kind="primary"]:hover {
              background: #1E293B !important;
              border-color: #1E293B !important;
          }

          /* Inputs */
          .stTextInput input, .stTextArea textarea,
          .stSelectbox div[data-baseweb="select"] > div,
          .stNumberInput input, .stDateInput input {
              border-radius: 8px !important;
              border-color: #E2E8F0 !important;
          }

          /* Expanders */
          [data-testid="stExpander"] {
              border: 1px solid #E2E8F0 !important;
              border-radius: 10px !important;
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03);
              background: #FFFFFF;
          }
          [data-testid="stExpander"] summary { font-weight: 500 !important; }

          /* Alerts */
          div[data-testid="stAlert"] {
              border-radius: 10px !important;
              padding: 0.875rem 1rem !important;
              font-size: 0.9rem;
              border: 1px solid rgba(15, 23, 42, 0.06);
          }

          /* Tabs */
          [data-testid="stTabs"] [role="tablist"] {
              border-bottom: 1px solid #E2E8F0;
              gap: 0.125rem;
          }
          [data-testid="stTabs"] [role="tab"] {
              font-weight: 500;
              color: #64748B;
              padding: 0.625rem 1rem;
          }
          [data-testid="stTabs"] [role="tab"][aria-selected="true"] { color: #0F172A; }

          /* Dataframes */
          [data-testid="stDataFrame"], [data-testid="stTable"] {
              border-radius: 10px;
              overflow: hidden;
              border: 1px solid #E2E8F0;
          }

          /* Metrics */
          [data-testid="stMetric"] {
              background: #FFFFFF;
              border: 1px solid #E2E8F0;
              border-radius: 10px;
              padding: 1rem 1.25rem;
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03);
          }

          /* File uploader */
          [data-testid="stFileUploader"] section {
              border-radius: 10px;
              border: 1px dashed #CBD5E1;
              background: #F8FAFC;
          }

          /* Sidebar polish */
          [data-testid="stSidebar"] {
              background: #FAFBFC;
              border-right: 1px solid #E2E8F0;
          }
          [data-testid="stSidebar"] h3 {
              font-size: 0.72rem !important;
              text-transform: uppercase;
              letter-spacing: 0.08em;
              color: #94A3B8 !important;
              font-weight: 600 !important;
              margin-top: 1.5rem !important;
              margin-bottom: 0.5rem !important;
          }
          [data-testid="stSidebar"] hr { margin: 1rem 0 !important; border-color: #E2E8F0; }
          [data-testid="stSidebar"] [data-baseweb="radio"] { padding: 0.25rem 0; }
          [data-testid="stSidebar"] [data-baseweb="radio"] label {
              font-size: 0.93rem; font-weight: 500;
          }

          /* st.logo sizing */
          [data-testid="stLogo"], [data-testid="stSidebarHeader"] img {
              height: 36px !important; width: auto !important;
          }

          /* Divider */
          hr { border-color: #E2E8F0; margin: 1.25rem 0; }

          /* Popover trigger */
          [data-testid="stPopover"] button {
              border-radius: 8px;
              background: #FFFFFF;
              border: 1px solid #E2E8F0;
          }

          /* Sidebar nav: full-width, left-aligned buttons (replaces radio) */
          [data-testid="stSidebar"] .stButton > button {
              justify-content: flex-start !important;
              text-align: left !important;
              padding-left: 0.875rem !important;
              padding-right: 0.875rem !important;
              font-weight: 500 !important;
              box-shadow: none !important;
              background: transparent !important;
              border: 1px solid transparent !important;
              color: #475569 !important;
              margin-bottom: 0.125rem !important;
          }
          [data-testid="stSidebar"] .stButton > button:hover {
              background: #F1F5F9 !important;
              color: #0F172A !important;
              border-color: transparent !important;
          }
          [data-testid="stSidebar"] .stButton > button[kind="primary"] {
              background: #0F172A !important;
              color: #FFFFFF !important;
              border-color: #0F172A !important;
          }
          [data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {
              background: #1E293B !important;
              color: #FFFFFF !important;
          }

          /* Home page: action cards */
          .home-hero {
              background: linear-gradient(135deg, #F8FAFC 0%, #EEF2F7 100%);
              border: 1px solid #E2E8F0;
              border-radius: 16px;
              padding: 2.25rem 2.5rem;
              margin-bottom: 2rem;
          }
          .home-hero h1 { margin-top: 0 !important; margin-bottom: 0.35rem !important; }
          .home-hero p { color: #475569; font-size: 1rem; max-width: 640px; margin: 0; }

          /* Stepper: horizontal progress in main page (replaces sidebar Étapes) */
          .rv-stepper {
              display: flex;
              gap: 0.5rem;
              margin: 0 0 1.5rem 0;
              padding: 0.5rem 0;
              overflow-x: auto;
          }
          .rv-step {
              display: flex;
              align-items: center;
              gap: 0.5rem;
              padding: 0.5rem 0.875rem;
              border-radius: 999px;
              border: 1px solid #E2E8F0;
              background: #FFFFFF;
              font-size: 0.85rem;
              font-weight: 500;
              color: #64748B;
              white-space: nowrap;
              cursor: default;
          }
          .rv-step.is-active {
              background: #0F172A;
              border-color: #0F172A;
              color: #FFFFFF;
          }
          .rv-step.is-done {
              background: #F1F5F9;
              border-color: #CBD5E1;
              color: #334155;
          }
          .rv-step .rv-step-num {
              display: inline-flex;
              align-items: center;
              justify-content: center;
              width: 22px; height: 22px;
              border-radius: 50%;
              background: #E2E8F0;
              color: #334155;
              font-size: 0.75rem;
              font-weight: 600;
          }
          .rv-step.is-active .rv-step-num { background: rgba(255,255,255,0.2); color: #FFFFFF; }
          .rv-step.is-done .rv-step-num { background: #CBD5E1; color: #0F172A; }

          /* Small "pill" badge (session overrides indicator, etc.) */
          .rv-pill {
              display: inline-flex;
              align-items: center;
              gap: 0.35rem;
              padding: 0.25rem 0.625rem;
              border-radius: 999px;
              background: #FEF3C7;
              color: #92400E;
              font-size: 0.78rem;
              font-weight: 500;
              border: 1px solid #FDE68A;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


_inject_custom_style()


# ========== Session state init ==========
def _init_state():
    defaults = {
        "sources": {},              # {key: SourceFile}
        "client_name": "",
        "user_instructions": "",
        "llm_proposals": {},        # {key: {target_field: source_col}}
        "fleet_mapping": {},        # {agency_code: fleet_name}
        "step": 1,
        "mode": "home",             # "home" | "classic" | "engine" | "rules"
        # --- engine mode ---
        "engine_files": {},         # {filename: {"df": DataFrame, "slug": str, "detected": str}}
        "engine_overrides": {},     # {(slug, field): source_col}
        "engine_result": None,      # EngineResult (dataclass) or None
        # --- rules editor (session-scoped priority overrides) ---
        # Shape: {table_slug: {field_name: [source_slug_in_priority_order]}}
        "rules_overrides": {},
        "rules_active_table": "vehicle",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_state()


# ========== Sidebar ==========
# Structure:
#   - st.logo() — handled above, renders the R + "revio" wordmark
#   - Navigation: 4 button-style menu items (Accueil / Import classique / Moteur / Règles)
#   - Contexte: client name + API key status (only on onboarding modes)
#   - Instructions spéciales: collapsed expander (only on onboarding modes)
NAV_ITEMS = [
    ("home",    "🏠",  "Accueil"),
    ("classic", "📥",  "Import — Flow classique"),
    ("engine",  "🧪",  "Import — Moteur de règles"),
    ("rules",   "⚙️",  "Règles d'import"),
]

with st.sidebar:
    st.caption("Outil interne d'onboarding — génération des fichiers d'import.")

    st.markdown("### Navigation")
    for slug, icon, label in NAV_ITEMS:
        active = st.session_state.mode == slug
        if st.button(
            f"{icon}  {label}",
            key=f"nav_{slug}",
            use_container_width=True,
            type="primary" if active else "secondary",
        ):
            if not active:
                st.session_state.mode = slug
                st.rerun()

    # Session-scoped rule overrides indicator — small pill badge, not a caption.
    _nb_overrides = rules_io.count_active_overrides(st.session_state.get("rules_overrides"))
    if _nb_overrides > 0:
        st.markdown(
            f"<div style='margin-top:0.5rem'><span class='rv-pill'>✎ {_nb_overrides} règle(s) personnalisée(s)</span></div>",
            unsafe_allow_html=True,
        )

    # --- Contexte (only on onboarding modes) ---
    if st.session_state.mode in ("classic", "engine"):
        st.markdown("---")
        st.markdown("### Contexte")
        st.session_state.client_name = st.text_input(
            "Nom du client",
            value=st.session_state.client_name,
            placeholder="ex. YSEIS",
            help="Utilisé pour nommer le dossier de sortie.",
        )

        # API key: (1) Streamlit Cloud secrets, (2) local .env, (3) manual paste.
        api_key_env = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key_env:
            try:
                api_key_env = st.secrets.get("ANTHROPIC_API_KEY", "")
                if api_key_env:
                    os.environ["ANTHROPIC_API_KEY"] = api_key_env
            except Exception:
                pass
        if api_key_env:
            st.success("Clé Anthropic détectée ✓")
        else:
            st.warning("Pas de clé API — colle-la pour activer le mapping IA.")
            pasted = st.text_input("Clé Anthropic (sk-ant-...)", type="password")
            if pasted:
                os.environ["ANTHROPIC_API_KEY"] = pasted

        with st.expander("✍️ Instructions spéciales", expanded=False):
            st.caption(
                "Règles en langage naturel qui s'appliquent à tout l'onboarding. "
                "*Ex : Pour ce client, un VP-BR = service. Si la date fin est vide, calcule Date début + Durée.*"
            )
            st.session_state.user_instructions = st.text_area(
                "Instructions",
                value=st.session_state.user_instructions,
                height=140,
                label_visibility="collapsed",
            )


# ========== Home page (landing) ==========
def render_home_page() -> None:
    """Welcoming landing page: hero + 3 action cards pointing to each mode.

    Shown on arrival (mode default = "home") and whenever the user clicks
    the "🏠 Accueil" item in the sidebar.
    """
    # Hero band
    st.markdown(
        """
        <div class="home-hero">
          <h1>Onboarding Revio</h1>
          <p>Transforme les fichiers reçus d'un client (API Plaques, exports loueurs,
          fichier interne) en imports Revio prêts à lancer. Moteur de règles YAML,
          mapping IA assisté, contrôle qualité intégré.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Primary CTA + quick-access cards
    st.markdown("### Par où veux-tu commencer ?")
    c1, c2, c3 = st.columns(3, gap="medium")

    with c1:
        with st.container(border=True):
            st.markdown("#### 🧪 Moteur de règles")
            st.caption(
                "Dépose tes fichiers, le moteur applique les règles YAML et "
                "produit l'import Vehicle. **Recommandé.**"
            )
            if st.button("Ouvrir le moteur", key="home_cta_engine", use_container_width=True, type="primary"):
                st.session_state.mode = "engine"
                st.rerun()

    with c2:
        with st.container(border=True):
            st.markdown("#### 📥 Flow classique")
            st.caption(
                "Le flow en 5 étapes : upload → détection → mapping manuel → "
                "découpage flottes → exports."
            )
            if st.button("Ouvrir le flow", key="home_cta_classic", use_container_width=True):
                st.session_state.mode = "classic"
                st.session_state.step = 1
                st.rerun()

    with c3:
        with st.container(border=True):
            st.markdown("#### ⚙️ Règles d'import")
            st.caption(
                "Consulte et ajuste la priorité des sources par champ. "
                "Modifications session-only."
            )
            if st.button("Ouvrir les règles", key="home_cta_rules", use_container_width=True):
                st.session_state.mode = "rules"
                st.rerun()

    # A soft "what's new" hint at the bottom — keeps the page from feeling empty
    # without overselling.
    st.markdown("")
    st.markdown("---")
    st.markdown("##### Astuces")
    a, b, c = st.columns(3, gap="medium")
    with a:
        st.markdown("**Un seul onboarding = un seul client.**")
        st.caption("Dépose tous les fichiers reçus d'un coup, l'outil détecte chaque source automatiquement.")
    with b:
        st.markdown("**Les règles peuvent être surchargées.**")
        st.caption("Sur la page *Règles d'import*, l'ordre de priorité est modifiable pour un onboarding sans toucher la config permanente.")
    with c:
        st.markdown("**Tout est interne.**")
        st.caption("Les fichiers uploadés ne quittent pas l'app Streamlit. Rien n'est persisté côté serveur.")


# ========== Classic-mode stepper (horizontal progress bar) ==========
CLASSIC_STEPS: list[tuple[int, str]] = [
    (1, "Upload"),
    (2, "Détection"),
    (3, "Mapping"),
    (4, "Flottes"),
    (5, "Exports"),
]


def _render_classic_stepper() -> None:
    """Horizontal pill-stepper shown at the top of each classic-mode step.

    Replaces the sidebar radio: users navigate by clicking the pills. Visual
    state: previous steps = "done" (light), current = "active" (dark), future
    = default (white).
    """
    current = int(st.session_state.get("step", 1))

    # Render the visual strip via HTML for a pixel-perfect pill look.
    pills = []
    for n, label in CLASSIC_STEPS:
        cls = "rv-step"
        if n == current:
            cls += " is-active"
        elif n < current:
            cls += " is-done"
        pills.append(
            f'<div class="{cls}"><span class="rv-step-num">{n}</span>{label}</div>'
        )
    st.markdown(f'<div class="rv-stepper">{"".join(pills)}</div>', unsafe_allow_html=True)

    # Jump buttons sit just below so keyboard/click navigation still works
    # (HTML pills aren't clickable in Streamlit; the buttons are the real UX).
    cols = st.columns(len(CLASSIC_STEPS))
    for idx, (n, label) in enumerate(CLASSIC_STEPS):
        with cols[idx]:
            active = n == current
            if st.button(
                f"{n}. {label}",
                key=f"stepper_jump_{n}",
                use_container_width=True,
                type="primary" if active else "secondary",
            ):
                if not active:
                    st.session_state.step = n
                    st.rerun()
    st.markdown("")  # small spacer before the step content


# ========== Engine mode (YAML rules engine, Vehicle only) ==========
def _render_manual_mapping_section(engine_files: dict) -> None:
    """Render the column-mapping UI for slugs whose columns aren't baked in YAML.

    Used for `client_file` and any `autre_loueur_etat_parc` file (unknown
    lessor export). For each such file we offer a Claude-powered
    auto-mapping button plus manual selectboxes per Revio field. Overrides
    are stored in ``st.session_state.engine_overrides`` with the key
    ``(slug, field_name)`` and consumed by ``rules_engine.run_vehicle`` as
    a fallback when the YAML rule has no ``column:`` key.

    When the user clicks the IA button we update both the overrides dict
    AND the selectbox widget state keys directly, then call ``st.rerun()``
    so the selectboxes reflect the new values on the next render (widget
    state takes precedence over the ``index=`` argument once it exists).

    If no file in ``engine_files`` needs manual mapping, we show the
    existing fallback info about the parc de référence.
    """
    files_to_map = [
        (key, info)
        for key, info in engine_files.items()
        if info.get("slug") in SLUGS_NEEDING_MANUAL_MAPPING
    ]

    if not files_to_map:
        st.info(
            "Aucun fichier à mapper manuellement (pas de `client_file` ni "
            "d'`autre_loueur_etat_parc`) → le moteur prendra l'union des plaques "
            "loueurs comme parc de référence (un avertissement apparaîtra dans "
            "les issues)."
        )
        return

    st.markdown("### 3. Mapping des colonnes")
    st.caption(
        "Pour les fichiers dont le format n'est pas connu à l'avance "
        "(`client_file`, `autre_loueur_etat_parc`), on indique quelle colonne "
        "source correspond à chaque champ Revio. Utilise le bouton IA pour une "
        "proposition automatique, puis révise au besoin. Les champs non mappés "
        "seront ignorés pour ce fichier."
    )

    for key, info in files_to_map:
        df = info["df"]
        slug = info["slug"]
        slug_label = slug_display(slug)[1]
        cols = [""] + [str(c) for c in df.columns]
        label = info["filename"] + (
            f" [{info['sheet_name']}]" if info.get("sheet_name") else ""
        )

        with st.expander(
            f"🔗 {label} → {slug_label} — mapping des champs", expanded=True
        ):
            c1, c2 = st.columns([1, 2])
            with c1:
                if st.button(
                    "🤖 Proposer un mapping (IA)",
                    key=f"engine_llm_{key}",
                    use_container_width=True,
                ):
                    with st.spinner("Claude analyse les colonnes..."):
                        result = propose_mapping(
                            df,
                            "vehicle",
                            st.session_state.get("user_instructions", ""),
                        )
                    if "_error" in result:
                        st.error(result["_error"])
                    else:
                        proposed = result.get("mapping", {})
                        notes = result.get("_notes", [])
                        debug = result.get("_debug", {})
                        valid_cols = {str(c) for c in df.columns}
                        nb_mapped = 0
                        for field_name in MANUAL_MAPPABLE_FIELDS:
                            src_col = proposed.get(field_name)
                            widget_key = (
                                f"engine_override_{slug}_{field_name}_{key}"
                            )
                            override_key = (slug, field_name)
                            if src_col and src_col in valid_cols:
                                st.session_state.engine_overrides[
                                    override_key
                                ] = src_col
                                st.session_state[widget_key] = src_col
                                nb_mapped += 1
                            else:
                                # Clear any previous value so the selectbox
                                # falls back to "" on rerun.
                                st.session_state.engine_overrides.pop(
                                    override_key, None
                                )
                                st.session_state[widget_key] = ""
                        if nb_mapped == 0:
                            st.warning(
                                "L'IA n'a proposé aucun mapping exploitable. "
                                "Mappe manuellement ci-dessous."
                            )
                        else:
                            st.success(
                                f"Mapping proposé : {nb_mapped} champ(s) rempli(s). "
                                "Révise-le ci-dessous avant de lancer le moteur."
                            )
                        if notes:
                            st.info(
                                "Notes de l'IA: "
                                + " / ".join(str(n) for n in notes)
                            )
                        if debug:
                            with st.expander("🔍 Debug IA (pour diagnostic)"):
                                st.json({"mapping_proposé": proposed, **debug})
                        st.rerun()
            with c2:
                st.caption(
                    "Colonnes détectées dans le fichier : "
                    + ", ".join(f"`{c}`" for c in list(df.columns)[:10])
                    + (" ..." if len(df.columns) > 10 else "")
                )
            st.markdown("---")
            for field_name in MANUAL_MAPPABLE_FIELDS:
                override_key = (slug, field_name)
                current = st.session_state.engine_overrides.get(
                    override_key, ""
                )
                if current not in cols:
                    current = ""
                picked = st.selectbox(
                    field_name,
                    options=cols,
                    index=cols.index(current),
                    key=f"engine_override_{slug}_{field_name}_{key}",
                )
                if picked:
                    st.session_state.engine_overrides[override_key] = picked
                else:
                    st.session_state.engine_overrides.pop(override_key, None)


def render_engine_page():
    st.header("🧪 Moteur de règles — Vehicle (beta)")
    st.caption(
        "Dépose les fichiers reçus pour un client. Le moteur applique les règles "
        "déclarées dans `src/rules/vehicle.yml` et produit le CSV Vehicle Revio. "
        "Les fichiers marqués **`client_file`** ou **`autre_loueur_etat_parc`** "
        "ont besoin d'un mapping manuel (IA + revue) car leurs colonnes ne sont "
        "pas connues à l'avance."
    )

    # --- 1. Upload ---
    st.markdown("### 1. Upload des fichiers")
    uploaded = st.file_uploader(
        "Fichiers CSV / XLSX (loueurs, API Plaques, fichier client)",
        type=["csv", "xlsx", "xls", "xlsm"],
        accept_multiple_files=True,
        key="engine_uploader",
    )

    # Only reprocess when the uploaded-file list actually changes. Streamlit
    # returns the uploader's contents on EVERY rerun (including download-
    # button clicks), so without this guard we'd wipe engine_result every
    # time the user downloads an export.
    current_sig = tuple(
        (getattr(f, "name", ""), getattr(f, "size", 0)) for f in (uploaded or [])
    )
    last_sig = st.session_state.get("engine_uploaded_sig")

    if uploaded and current_sig != last_sig:
        new_files: dict = {}
        # Load the learned-patterns memory once per upload batch. Used as a
        # fallback when the hard-coded detector can't identify a file — typical
        # case: a new lessor's "état de parc" whose signature we memorised on
        # a previous onboarding.
        _patterns = lp.load_patterns()
        _learned_hits: list[str] = []
        for up in uploaded:
            try:
                pairs = load_tabular(up)
            except Exception as e:
                st.error(f"Impossible de lire {up.name}: {e}")
                continue
            for sheet_name, df in pairs:
                if df.empty or df.shape[1] < 2:
                    continue
                key = f"{up.name}::{sheet_name}" if sheet_name else up.name
                detected = detect(up.name, df, sheet_name=sheet_name or None)
                default_slug = DETECTOR_TO_YAML_SLUG.get(detected.source_type, "client_file")
                learned_match = None
                # Fire the learned-pattern fallback ONLY when the detector fell
                # back to client_file (our catch-all). An explicit detection
                # (api_plaques, ayvens_*, etc.) always wins over the memory.
                if default_slug == "client_file" and _patterns:
                    learned_match = lp.match_pattern(
                        up.name, [str(c) for c in df.columns], _patterns
                    )
                    if learned_match is not None:
                        default_slug = learned_match.slug
                        _learned_hits.append(f"{up.name} → `{learned_match.slug}`")
                new_files[key] = {
                    "df": df,
                    "filename": up.name,
                    "sheet_name": sheet_name,
                    "slug": default_slug,
                    "detected_type": detected.source_type,
                    "detected_reason": detected.reason,
                    "learned_match_id": learned_match.id if learned_match else None,
                }
        st.session_state.engine_files = new_files
        st.session_state.engine_result = None  # invalidate previous run
        st.session_state.engine_uploaded_sig = current_sig
        st.success(f"{len(new_files)} fichier(s) chargé(s).")
        if _learned_hits:
            st.info(
                "🧠 **Format reconnu depuis la mémoire** — "
                + " · ".join(_learned_hits)
            )

    engine_files = st.session_state.engine_files
    if not engine_files:
        st.info("Aucun fichier chargé.")
        return

    # --- 2. Type per file (slug override) — grouped by slug ---
    st.markdown("### 2. Types détectés — regroupés par source")
    st.caption(
        "Les fichiers sont regroupés par type. Si plusieurs fichiers ciblent le même "
        "type (ex. deux exports loueurs), ils seront **concaténés automatiquement** "
        "avant d'entrer dans le moteur, avec une colonne `__source_file` pour la "
        "traçabilité. Tu peux corriger manuellement le type si la détection s'est "
        "trompée — le fichier rejoindra alors le bon groupe."
    )

    # Group files by their CURRENT slug. We iterate the engine_files dict so
    # upload order is preserved within each group (the rules engine's plate
    # dedup is keep-first — order matters).
    files_by_slug: dict[str, list[tuple[str, dict]]] = {}
    for key, info in engine_files.items():
        slug = info.get("slug") or "client_file"
        files_by_slug.setdefault(slug, []).append((key, info))

    # Render groups in SLUG_DISPLAY order so the page reads top-to-bottom in
    # a predictable way (plaques → ayvens → arval → autres → client).
    ordered_slugs = sorted(files_by_slug.keys(), key=lambda s: slug_display(s)[2])
    for slug in ordered_slugs:
        files_in_group = files_by_slug[slug]
        emoji, human_label, _ = slug_display(slug)
        total_rows = sum(len(info["df"]) for _, info in files_in_group)
        n_files = len(files_in_group)

        with st.container(border=True):
            # Group header: emoji + human label + file count + row count
            hc1, hc2 = st.columns([5, 2])
            with hc1:
                st.markdown(f"#### {emoji} {human_label}")
                st.caption(
                    f"`{slug}`  ·  {n_files} fichier{'s' if n_files > 1 else ''}  "
                    f"·  {total_rows} ligne{'s' if total_rows > 1 else ''}"
                )
            with hc2:
                if n_files > 1:
                    st.markdown(
                        "<div style='margin-top:0.4rem'><span class='rv-pill' "
                        "style='background:#DBEAFE;color:#1E40AF;border-color:#BFDBFE'>"
                        f"⇢ Concaténation auto · {n_files} fichiers</span></div>",
                        unsafe_allow_html=True,
                    )

            # File rows inside the card
            for key, info in files_in_group:
                label = info["filename"] + (f" [{info['sheet_name']}]" if info.get("sheet_name") else "")
                row = st.columns([7, 1, 4])
                with row[0]:
                    st.markdown(f"**{label}**")
                    # Detection reason — plus a "🧠 Mémoire" badge when the
                    # slug came from the learned-patterns fallback instead of
                    # the hard-coded detector.
                    badge = ""
                    if info.get("learned_match_id"):
                        badge = (
                            "  <span style='background:#F3E8FF;color:#6B21A8;"
                            "padding:0.08rem 0.5rem;border-radius:999px;"
                            "font-size:0.72rem;font-weight:500;border:1px solid "
                            "#E9D5FF'>🧠 mémoire</span>"
                        )
                    st.markdown(
                        f"<span style='color:#64748B;font-size:0.85em'>"
                        f"{len(info['df'])} lignes · détecté `{info['detected_type']}` — "
                        f"{info['detected_reason']}</span>{badge}",
                        unsafe_allow_html=True,
                    )
                with row[1]:
                    with st.popover("🔍", help="Aperçu du fichier", use_container_width=True):
                        st.markdown(f"**{label}**")
                        st.caption(
                            f"{len(info['df'])} lignes × {len(info['df'].columns)} colonnes"
                        )
                        st.dataframe(
                            info["df"].head(20),
                            use_container_width=True,
                            hide_index=True,
                        )
                with row[2]:
                    current_slug = info.get("slug", "client_file")
                    if current_slug not in ENGINE_SOURCE_SLUGS:
                        current_slug = "client_file"
                    info["slug"] = st.selectbox(
                        "Type YAML",
                        options=ENGINE_SOURCE_SLUGS,
                        index=ENGINE_SOURCE_SLUGS.index(current_slug),
                        key=f"engine_slug_{key}",
                        label_visibility="collapsed",
                    )

    # --- 3. Column mapping for slugs whose columns aren't baked in YAML ---
    # (client_file + autre_loueur_etat_parc). IA proposes, user reviews.
    _render_manual_mapping_section(engine_files)

    # --- 4. Run ---
    st.markdown("### 4. Lancer le moteur")

    # Show which priority overrides will be used (if any). Transparency matters
    # so the user sees they're not running defaults.
    vehicle_overrides: dict[str, list[str]] = (
        st.session_state.get("rules_overrides", {}).get("vehicle", {})
    )
    vehicle_overrides = {k: v for k, v in vehicle_overrides.items() if v}
    if vehicle_overrides:
        st.info(
            f"🎛️ **{len(vehicle_overrides)} règle(s) de priorité personnalisée(s)** seront appliquées — "
            "cf. *⚙️ Règles d'import* dans le menu de gauche."
        )

    if st.button("▶️ Appliquer les règles Vehicle", type="primary", use_container_width=True):
        # Concat all files sharing the same slug and tag rows with __source_file
        # for downstream traceability. Single-file slugs go through the same
        # code path so the report can always rely on the column existing.
        merged = merge_engine_sources(engine_files)
        source_dfs: dict = {slug: ms.df for slug, ms in merged.items()}

        # Surface the merges in the UI — if the user dropped two Ayvens files,
        # it's important they SEE that we concatenated them (otherwise they
        # might think the second file got silently dropped).
        merge_notes = [
            f"`{slug}` ← {len(ms.files)} fichiers · {ms.n_rows_before_dedup} lignes"
            for slug, ms in merged.items()
            if len(ms.files) > 1
        ]
        if merge_notes:
            st.info("⇢ **Concaténation auto** — " + "  ·  ".join(merge_notes))

        overrides = dict(st.session_state.engine_overrides)
        try:
            with st.spinner("Application des règles..."):
                result = rules_engine.run_vehicle(
                    source_dfs,
                    manual_column_overrides=overrides,
                    priority_overrides=vehicle_overrides or None,
                )
            st.session_state.engine_result = result
        except Exception as e:
            st.error(f"Erreur moteur: {e}")
            st.session_state.engine_result = None

    # --- 5. Result ---
    result = st.session_state.engine_result
    if result is None:
        return

    st.markdown("### 5. Résultat")
    df = result.df
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Véhicules produits", len(df))
    col_b.metric("Anomalies détectées", len(result.conflicts_by_cell or {}))
    orphan_count = len(result.orphan_df) if result.orphan_df is not None else 0
    col_c.metric("Plaques orphelines", orphan_count)
    st.dataframe(df, use_container_width=True)

    if result.issues:
        st.markdown(f"#### ⚠️ {len(result.issues)} alerte(s) globale(s)")
        issues_df = pd.DataFrame([
            {"plaque": i.plate, "champ": i.field, "source": i.source, "avertissement": i.warning}
            for i in result.issues
        ])
        st.dataframe(issues_df, use_container_width=True, hide_index=True)

    if result.orphan_df is not None and not result.orphan_df.empty:
        st.markdown(f"#### 👻 {len(result.orphan_df)} plaque(s) orpheline(s)")
        st.caption(
            "Ces plaques sont présentes dans un fichier loueur mais absentes du fichier "
            "client. Elles sont exclues du parc final mais listées dans l'onglet "
            "`plaques_orphelines` du rapport Excel."
        )
        st.dataframe(result.orphan_df, use_container_width=True)

    if not result.conflicts_by_cell and not result.issues and orphan_count == 0:
        st.success("Aucune anomalie — toutes les sources se sont bien alignées ✅")

    # --- 6. Download ---
    st.markdown("### 6. Exports")
    client_name = st.session_state.client_name or "client"
    csv_bytes = df.reset_index(drop=True).to_csv(index=False, sep=";", encoding="utf-8").encode("utf-8")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "⬇️ Télécharger vehicle.csv (Revio)",
            data=csv_bytes,
            file_name=f"vehicle_{client_name}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
        )
    with c2:
        try:
            xlsx_bytes = build_report_xlsx(result, client_name=client_name)
            st.download_button(
                "⬇️ Télécharger rapport.xlsx (sources / anomalies / orphelines)",
                data=xlsx_bytes,
                file_name=f"rapport_{client_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception as e:
            st.error(f"Impossible de construire le rapport Excel : {e}")


# ========== Rules editor page ==========
def render_rules_page():
    st.header("⚙️ Règles d'import")
    st.caption(
        "Configure ici, champ par champ, quelle source gagne sur les autres quand "
        "plusieurs fichiers donnent une valeur différente pour la même information. "
        "Ces règles pilotent le moteur de l'onglet *Import — Moteur de règles*."
    )

    # --- Banner: session scope ---
    st.warning(
        "🔒 **Portée session uniquement** — Les modifications faites ici s'appliquent "
        "à l'onboarding en cours. Elles ne touchent pas les règles par défaut et seront "
        "perdues à la fermeture de l'app. Utilise *🔄 Réinitialiser tout* pour revenir aux "
        "valeurs par défaut à tout moment."
    )

    overrides_all = st.session_state.setdefault("rules_overrides", {})
    nb_modified = rules_io.count_active_overrides(overrides_all)

    # --- Summary bar ---
    sc1, sc2 = st.columns([4, 1])
    with sc1:
        if nb_modified > 0:
            st.info(f"✎ **{nb_modified} champ(s) personnalisé(s)** dans cette session.")
        else:
            st.success("✅ Aucune modification — les règles par défaut s'appliquent.")
    with sc2:
        if nb_modified > 0:
            if st.button("🔄 Réinitialiser tout", use_container_width=True, key="rules_reset_all"):
                st.session_state.rules_overrides = {}
                st.rerun()

    # --- Legend ---
    with st.expander("ℹ️ Comment lire cette page ?", expanded=False):
        st.markdown(
            "**Numéro à gauche** : priorité de la source pour ce champ — "
            "plus le chiffre est petit, plus elle passe en premier.\n\n"
            "**Ex-æquo** : plusieurs sources peuvent avoir la même priorité "
            "(ex : Ayvens, Arval, autre loueur à `2` pour `brand`). En pratique "
            "une même plaque n'est présente que chez un seul loueur → c'est "
            "celui-là qui gagne sur sa plaque, sans conflit.\n\n"
            "**🟢 Source chargée** pour cet import — la règle va s'appliquer.  \n"
            "**⚪ Source non chargée** — la règle est là mais inerte tant qu'un "
            "fichier correspondant n'est pas déposé dans le moteur.\n\n"
            "**⬆ / ⬇** : remonter / descendre une source. Réorganiser **casse "
            "les ex-æquo** et fige un ordre strict pour ce champ. Utilise *↺ "
            "Réinitialiser* pour retrouver les priorités par défaut."
        )

    st.markdown("---")

    # --- Table selection (tabs) ---
    tables = rules_io.list_available_tables()
    tab_labels = [
        meta["label"] + ("" if meta["available"] else " — bientôt")
        for _, meta in tables
    ]
    tabs = st.tabs(tab_labels)
    for i, (slug, meta) in enumerate(tables):
        with tabs[i]:
            if meta["available"]:
                _render_table_rules(slug)
            else:
                st.info(
                    f"Les règles **{meta['label']}** seront disponibles dans une prochaine "
                    "version. Aujourd'hui, seule la table Véhicules est câblée au moteur."
                )


def _render_table_rules(table_slug: str):
    """Render the priority editor for a single table (vehicle / contract / ...)."""
    try:
        rules_yaml = rules_io.load_rules_yaml(table_slug)
    except (FileNotFoundError, KeyError) as e:
        st.error(f"Impossible de charger les règles : {e}")
        return

    fields_spec = rules_yaml.get("fields", {})
    if not fields_spec:
        st.info("Aucun champ déclaré dans ce fichier de règles.")
        return

    overrides_table = st.session_state.setdefault("rules_overrides", {}).setdefault(
        table_slug, {}
    )

    # Which source slugs are currently uploaded in the engine? Used to paint
    # the "effective" source badges so the user sees at a glance which priority
    # levels will actually fire for THIS import.
    uploaded_slugs: set[str] = {
        info.get("slug")
        for info in st.session_state.get("engine_files", {}).values()
        if info.get("slug")
    }

    # --- Search bar ---
    sb1, sb2 = st.columns([4, 1])
    with sb1:
        query = st.text_input(
            "🔎 Rechercher un champ",
            value="",
            placeholder="ex. usage, motorisation, co2, brand…",
            key=f"rules_search_{table_slug}",
            label_visibility="collapsed",
        ).strip().lower()
    with sb2:
        show_only_modified = st.toggle(
            "Modifiés uniquement",
            value=False,
            key=f"rules_only_modified_{table_slug}",
        )

    # --- Group by category ---
    categories = rules_io.categorize_fields(table_slug, fields_spec)
    cat_tabs = st.tabs([c[0] for c in categories])

    for i, (cat_label, field_names) in enumerate(categories):
        with cat_tabs[i]:
            # Filter per search / modified toggle
            filtered = []
            for fname in field_names:
                if fname not in fields_spec:
                    continue
                if query and query not in fname.lower() and query not in (
                    fields_spec[fname].get("description", "").lower()
                ):
                    continue
                if show_only_modified and fname not in overrides_table:
                    continue
                filtered.append(fname)

            if not filtered:
                st.caption("_Aucun champ ne correspond aux filtres._")
                continue

            for field_name in filtered:
                _render_field_priority_card(
                    table_slug,
                    field_name,
                    fields_spec[field_name],
                    overrides_table,
                    uploaded_slugs,
                )


def _render_field_priority_card(
    table_slug: str,
    field_name: str,
    field_spec: dict,
    overrides_table: dict,
    uploaded_slugs: set[str],
) -> None:
    """One card per field with reorderable priority list.

    When no override is active, the displayed priorities come from the YAML
    and preserve ties (ex-æquo). Once the user reorders anything, the override
    is recorded and priorities flatten to 1..N.
    """
    default_order = rules_io.default_priority_order(field_spec)  # [(slug, label, prio)]
    default_slugs = [s for s, _, _ in default_order]

    current_detailed = rules_io.resolve_current_order(
        field_spec, overrides_table.get(field_name)
    )  # [(slug, label, effective_priority)]
    current_slugs = [s for s, _, _ in current_detailed]

    # An override that equals the default order is noise — clean it up.
    if field_name in overrides_table and current_slugs == default_slugs:
        overrides_table.pop(field_name, None)
    is_modified = field_name in overrides_table

    mandatory = field_spec.get("mandatory", False)
    description = field_spec.get("description", "") or ""

    # Count ties per effective priority (for the ex-æquo badge).
    # Only meaningful when NOT modified — an override always flattens ties.
    prio_counts: dict[int, int] = {}
    for _, _, p in current_detailed:
        prio_counts[p] = prio_counts.get(p, 0) + 1

    # Card header badges
    badges: list[str] = []
    if mandatory:
        badges.append("🔒 Obligatoire")
    if is_modified:
        badges.append("✎ Modifié cette session")
    header_badges = ("  ·  " + "  ·  ".join(badges)) if badges else ""

    expander_title = f"**{field_name}** — {description}{header_badges}"
    with st.expander(expander_title, expanded=is_modified):
        # "Before / after" helper when modified
        if is_modified:
            labels_by_slug = {s: lbl for s, lbl, _ in default_order}
            c_before, c_after = st.columns(2)
            with c_before:
                st.caption("Priorités par défaut (ex-æquo respectés)")
                st.markdown(
                    " · ".join(
                        f"`{labels_by_slug.get(s, s)}` _(p{p})_"
                        for s, _, p in default_order
                    )
                )
            with c_after:
                st.caption("Priorités pour cet onboarding (ordre strict)")
                st.markdown(
                    " · ".join(
                        f"**`{lbl}`** _(p{p})_"
                        for _, lbl, p in current_detailed
                    )
                )
            st.markdown("")

        # Priority list with up/down arrows. Prio number shown comes straight
        # from the YAML when not overridden (ties visible), or flat 1..N when
        # overridden.
        for pos, (slug, label, prio) in enumerate(current_detailed):
            uploaded_mark = "🟢" if slug in uploaded_slugs else "⚪"
            is_tied = prio_counts.get(prio, 0) > 1 and not is_modified
            tie_badge = (
                "<span style='color:#b26a00;font-size:0.8em;"
                "margin-left:0.5em;font-style:italic'>· ex-æquo</span>"
                if is_tied
                else ""
            )

            row_cols = st.columns([1, 6, 1, 1])
            with row_cols[0]:
                st.markdown(f"### {prio}")
            with row_cols[1]:
                st.markdown(
                    f"{uploaded_mark} **{label}**{tie_badge}  \n"
                    f"<span style='color:#888;font-size:0.85em'>`{slug}`</span>",
                    unsafe_allow_html=True,
                )
            with row_cols[2]:
                if pos > 0:
                    if st.button(
                        "⬆",
                        key=f"up_{table_slug}_{field_name}_{slug}",
                        help="Monter d'un cran (casse les ex-æquo)",
                        use_container_width=True,
                    ):
                        new_order = list(current_slugs)
                        new_order[pos], new_order[pos - 1] = (
                            new_order[pos - 1],
                            new_order[pos],
                        )
                        overrides_table[field_name] = new_order
                        st.rerun()
            with row_cols[3]:
                if pos < len(current_detailed) - 1:
                    if st.button(
                        "⬇",
                        key=f"down_{table_slug}_{field_name}_{slug}",
                        help="Descendre d'un cran (casse les ex-æquo)",
                        use_container_width=True,
                    ):
                        new_order = list(current_slugs)
                        new_order[pos], new_order[pos + 1] = (
                            new_order[pos + 1],
                            new_order[pos],
                        )
                        overrides_table[field_name] = new_order
                        st.rerun()

        # Footer
        ft1, ft2 = st.columns([4, 1])
        with ft1:
            nb_uploaded = sum(1 for s in current_slugs if s in uploaded_slugs)
            mode_str = (
                "ordre strict appliqué"
                if is_modified
                else "priorités par défaut (ex-æquo respectés)"
            )
            st.caption(
                f"🟢 {nb_uploaded}/{len(current_slugs)} source(s) chargée(s) · {mode_str}."
            )
        with ft2:
            if is_modified:
                if st.button(
                    "↺ Réinitialiser",
                    key=f"reset_{table_slug}_{field_name}",
                    help="Revenir à l'ordre par défaut pour ce champ",
                    use_container_width=True,
                ):
                    overrides_table.pop(field_name, None)
                    st.rerun()


if st.session_state.mode == "home":
    render_home_page()

elif st.session_state.mode == "engine":
    render_engine_page()

elif st.session_state.mode == "rules":
    render_rules_page()

# ========== Classic mode: horizontal stepper above step content ==========
elif st.session_state.mode == "classic":
    _render_classic_stepper()

# The step branches below only fire when mode == "classic" (other modes have
# already returned above). Each uses `mode == classic` as a safety guard so a
# stale session_state.step never leaks into home/engine/rules pages.
_IS_CLASSIC = st.session_state.mode == "classic"

# ========== Step 1: Upload ==========
if _IS_CLASSIC and st.session_state.step == 1:
    st.header("1. Upload des fichiers")
    st.markdown(
        "Dépose ici **tous les fichiers** reçus pour ce client : fichiers internes, "
        "exports loueurs (Ayvens, Arval, etc.), fichier driver du client, et idéalement "
        "l'export API Plaques si tu l'as déjà fait."
    )
    uploaded = st.file_uploader(
        "Fichiers CSV / XLSX",
        type=["csv", "xlsx", "xls", "xlsm"],
        accept_multiple_files=True,
    )

    if uploaded:
        progress = st.progress(0.0, text="Analyse des fichiers...")
        new_sources: dict[str, SourceFile] = {}
        for i, up in enumerate(uploaded):
            try:
                pairs = load_tabular(up)
            except Exception as e:
                st.error(f"Impossible de lire {up.name}: {e}")
                continue
            for sheet_name, df in pairs:
                if df.empty or df.shape[1] < 2:
                    continue
                key = f"{up.name}::{sheet_name}" if sheet_name else up.name
                detected = detect(up.name, df, sheet_name=sheet_name or None)
                # Default target schema = first feed.
                default_target = detected.feeds[0] if detected.feeds else None
                new_sources[key] = SourceFile(
                    key=key,
                    filename=up.name,
                    sheet_name=sheet_name,
                    df_raw=df,
                    detected=detected,
                    target_schema=default_target,
                )
            progress.progress((i + 1) / len(uploaded), text=f"Analyse: {up.name}")
        st.session_state.sources = new_sources
        progress.empty()
        st.success(f"{len(new_sources)} fichier(s)/onglet(s) chargé(s). Passe à l'étape 2.")

    if st.session_state.sources:
        st.markdown("#### Résumé")
        rows = []
        for sf in st.session_state.sources.values():
            rows.append({
                "Fichier": sf.filename + (f" [{sf.sheet_name}]" if sf.sheet_name else ""),
                "Type détecté": label_for(sf.detected.source_type),
                "Alimente": ", ".join(sf.detected.feeds) or "—",
                "Lignes": len(sf.df),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ========== Step 2: Detection review ==========
elif _IS_CLASSIC and st.session_state.step == 2:
    st.header("2. Détection & choix du schéma cible")
    st.caption(
        "Vérifie le type détecté pour chaque fichier. "
        "Un fichier peut alimenter plusieurs schémas (ex. Ayvens Etat de parc → véhicules + contrats) : "
        "coche ceux qui t'intéressent."
    )

    if not st.session_state.sources:
        st.info("Rien à traiter. Remonte à l'étape 1.")
    else:
        for key, sf in st.session_state.sources.items():
            with st.expander(f"📄 {sf.filename}" + (f" [{sf.sheet_name}]" if sf.sheet_name else ""), expanded=True):
                c1, c2, c3 = st.columns([2, 2, 1])
                with c1:
                    st.markdown(f"**Détection**: {label_for(sf.detected.source_type)}")
                    st.caption(sf.detected.reason)
                with c2:
                    all_schemas = list(SCHEMAS.keys())
                    current = sf.target_schema or (all_schemas[0] if not sf.detected.feeds else sf.detected.feeds[0])
                    sf.target_schema = st.selectbox(
                        "Schéma Revio cible",
                        options=all_schemas + ["(ignorer ce fichier)"],
                        index=(all_schemas.index(current) if current in all_schemas else len(all_schemas)),
                        key=f"target_{key}",
                    )
                    if sf.target_schema == "(ignorer ce fichier)":
                        sf.selected = False
                        sf.target_schema = None
                    else:
                        sf.selected = True
                with c3:
                    st.metric("Lignes", len(sf.df))
                st.dataframe(sf.df.head(5), use_container_width=True)


# ========== Step 3: Column mapping ==========
elif _IS_CLASSIC and st.session_state.step == 3:
    st.header("3. Mapping des colonnes")
    st.caption(
        "Pour chaque fichier, je propose un mapping automatique. Révise-le si besoin. "
        "Les champs non mappés resteront vides dans la sortie."
    )
    if not st.session_state.sources:
        st.info("Rien à traiter.")
    for key, sf in st.session_state.sources.items():
        if not sf.selected or not sf.target_schema:
            continue
        with st.expander(
            f"🔗 {sf.filename} → {sf.target_schema.upper()}"
            + (f" [{sf.sheet_name}]" if sf.sheet_name else ""),
            expanded=False,
        ):
            c1, c2 = st.columns([1, 2])
            with c1:
                if st.button("🤖 Proposer un mapping (IA)", key=f"llm_{key}"):
                    with st.spinner("Claude analyse les colonnes..."):
                        result = propose_mapping(
                            sf.df, sf.target_schema, st.session_state.user_instructions
                        )
                    if "_error" in result:
                        st.error(result["_error"])
                    else:
                        proposed = result.get("mapping", {})
                        sf.mapping.update(proposed)
                        notes = result.get("_notes", [])
                        debug = result.get("_debug", {})
                        nb_mapped = sum(1 for v in proposed.values() if v)
                        if nb_mapped == 0:
                            st.warning(
                                "L'IA n'a proposé aucun mapping exploitable. "
                                "Voir le détail ci-dessous, et/ou mappe manuellement."
                            )
                        else:
                            st.success(
                                f"Mapping proposé : {nb_mapped} champ(s) rempli(s). "
                                "Révise-le ci-dessous."
                            )
                        if notes:
                            st.info("Notes de l'IA: " + " / ".join(str(n) for n in notes))
                        if debug:
                            with st.expander("🔍 Debug IA (pour diagnostic)"):
                                st.json({"mapping_proposé": proposed, **debug})
            with c2:
                st.caption(
                    "Colonnes détectées dans le fichier : "
                    + ", ".join(f"`{c}`" for c in list(sf.df.columns)[:10])
                    + (" ..." if len(sf.df.columns) > 10 else "")
                )
            st.markdown("---")
            target_fields = [f.name for f in SCHEMAS[sf.target_schema] if f.name]
            source_cols = [""] + list(sf.df.columns.astype(str))
            # Build a per-field selectbox.
            for tf in target_fields:
                current = sf.mapping.get(tf, "") or ""
                if current not in source_cols:
                    current = ""
                sf.mapping[tf] = st.selectbox(
                    tf,
                    options=source_cols,
                    index=source_cols.index(current),
                    key=f"map_{key}_{tf}",
                )


# ========== Step 4: Fleet splitting ==========
elif _IS_CLASSIC and st.session_state.step == 4:
    st.header("4. Découpage des flottes")
    st.caption(
        "L'outil détecte les agences présentes dans tes fichiers. "
        "Associe chacune à une flotte (ex. 1 flotte par agence, ou regroupements)."
    )

    # Collect all agencies from any source file that has an agency-like column.
    agencies: set[str] = set()
    AGENCY_CANDIDATES = ["agence", "agency", "structure", "centre", "site", "établissement", "etablissement",
                         "companyAnalyticalCode", "Structure 1"]
    for sf in st.session_state.sources.values():
        if not sf.selected:
            continue
        df = sf.df
        for col in df.columns:
            if any(cand.lower() in str(col).lower() for cand in AGENCY_CANDIDATES):
                agencies.update(str(v).strip() for v in df[col].dropna().unique() if str(v).strip())
    agencies = sorted(a for a in agencies if a and a.lower() not in {"nan", "none", "null"})

    if not agencies:
        st.info("Aucune agence détectée - tout ira dans une flotte 'default'.")
    else:
        c1, c2 = st.columns([1, 3])
        with c1:
            if st.button("🔁 Mode rapide : 1 agence = 1 flotte"):
                st.session_state.fleet_mapping = {a: a for a in agencies}
        st.markdown(f"**{len(agencies)} agence(s) détectée(s)**")
        current_fleets: set[str] = set(st.session_state.fleet_mapping.values()) or {"default"}
        for agency in agencies:
            current = st.session_state.fleet_mapping.get(agency, agency)
            new_val = st.text_input(
                f"Agence `{agency}` → Flotte",
                value=current,
                key=f"fleet_{agency}",
            )
            st.session_state.fleet_mapping[agency] = new_val


# ========== Step 5: Outputs & errors ==========
elif _IS_CLASSIC and st.session_state.step == 5:
    st.header("5. Génération des fichiers Revio")
    if not st.session_state.sources:
        st.info("Rien à traiter.")
        st.stop()

    # Priority for conflict resolution. Lower index = higher priority.
    priority_order = [
        "api_plaques",
        "ayvens_etat_parc",
        "arval_uat",
        "ayvens_aen",
        "arval_aen",
        "client_driver",
        "client_vehicle",
    ]

    with st.spinner("Construction des fichiers de sortie..."):
        sources_list = list(st.session_state.sources.values())
        outputs = merge_per_schema(sources_list, priority_order)
        # Split by fleet (driver files have companyAnalyticalCode filled naturally,
        # but we don't always have it on vehicle/contract outputs - for the v0 we
        # keep them together and rely on join via plate later).
        outputs_by_fleet = split_by_fleet(outputs, st.session_state.fleet_mapping)
        issues = validate(outputs)

    st.subheader("📊 Aperçu")
    total_rows = sum(len(df) for fleet in outputs_by_fleet.values() for df in fleet.values())
    st.metric("Lignes générées (toutes flottes × schémas)", total_rows)

    for fleet, schemas in outputs_by_fleet.items():
        with st.expander(f"🏢 Flotte: {fleet}", expanded=len(outputs_by_fleet) <= 2):
            for schema_name, df in schemas.items():
                st.markdown(f"**{schema_name}** ({len(df)} lignes)")
                st.dataframe(df.head(20), use_container_width=True, hide_index=True)

    if issues:
        st.subheader(f"⚠️ {len(issues)} problème(s) détecté(s)")
        issue_df = pd.DataFrame([
            {
                "schema": i.schema,
                "plaque": i.plate,
                "champ": i.field,
                "niveau": i.level,
                "message": i.message,
            }
            for i in issues
        ])
        st.dataframe(issue_df, use_container_width=True, hide_index=True)
    else:
        st.success("Aucune erreur bloquante détectée ✅")

    # Build the zip.
    client_name = st.session_state.client_name or "client"
    zip_bytes = build_zip(outputs_by_fleet, issues, client_name=client_name)
    st.download_button(
        "⬇️ Télécharger l'archive (CSV Revio + rapport)",
        data=zip_bytes,
        file_name=f"revio_import_{client_name}.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
    )

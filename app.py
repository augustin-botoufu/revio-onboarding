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
)
from src.output_writer import build_zip, split_by_fleet
from src.schemas import SCHEMAS, header_for
from src import rules_engine


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

# Fields of the Vehicle schema that need a manual column mapping when the
# source is `client_file` (no fixed column naming in a client's free-form
# Excel). We let the user pick the source column for each of these.
CLIENT_FILE_MAPPABLE_FIELDS = [
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


# ========== Page config ==========
st.set_page_config(
    page_title="Revio - Outil d'onboarding",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ========== Session state init ==========
def _init_state():
    defaults = {
        "sources": {},              # {key: SourceFile}
        "client_name": "",
        "user_instructions": "",
        "llm_proposals": {},        # {key: {target_field: source_col}}
        "fleet_mapping": {},        # {agency_code: fleet_name}
        "step": 1,
        "mode": "classic",          # "classic" (5-step flow) | "engine" (YAML rules engine)
        # --- engine mode ---
        "engine_files": {},         # {filename: {"df": DataFrame, "slug": str, "detected": str}}
        "engine_overrides": {},     # {(slug, field): source_col}
        "engine_result": None,      # EngineResult (dataclass) or None
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_state()


# ========== Sidebar ==========
with st.sidebar:
    st.title("🚗 Revio Onboarding")
    st.caption("Outil interne de génération des fichiers d'import Revio.")

    st.markdown("---")
    st.markdown("### ⚙️ Mode")
    mode_label = st.radio(
        "Mode",
        options=["classic", "engine"],
        index=0 if st.session_state.mode == "classic" else 1,
        format_func=lambda m: {
            "classic": "Flow classique (5 étapes)",
            "engine": "🧪 Moteur de règles (beta, Vehicle)",
        }[m],
        label_visibility="collapsed",
    )
    st.session_state.mode = mode_label

    st.markdown("---")
    st.session_state.client_name = st.text_input(
        "Nom du client",
        value=st.session_state.client_name,
        placeholder="ex. YSEIS",
        help="Utilisé pour nommer le dossier de sortie.",
    )

    # Look for the API key in (1) Streamlit Cloud secrets, (2) local .env,
    # (3) fallback to manual input in the sidebar.
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
        st.warning("Pas de clé configurée - saisis-la ci-dessous pour activer le mapping IA.")
        pasted = st.text_input("Clé Anthropic (sk-ant-...)", type="password")
        if pasted:
            os.environ["ANTHROPIC_API_KEY"] = pasted

    st.markdown("---")
    st.markdown("### ✍️ Instructions spéciales")
    st.caption(
        "Règles en langage naturel qui s'appliquent à tout l'onboarding. "
        "Ex: *Pour ce client, un VP-BR = service*. *Si la date fin est vide, calcule Date début + Durée.*"
    )
    st.session_state.user_instructions = st.text_area(
        "Instructions",
        value=st.session_state.user_instructions,
        height=160,
        label_visibility="collapsed",
    )

    if st.session_state.mode == "classic":
        st.markdown("---")
        st.markdown("### Navigation")
        st.session_state.step = st.radio(
            "Étape",
            options=[1, 2, 3, 4, 5],
            index=st.session_state.step - 1,
            format_func=lambda i: {
                1: "1️⃣ Upload des fichiers",
                2: "2️⃣ Détection & schéma cible",
                3: "3️⃣ Mapping des colonnes",
                4: "4️⃣ Découpage des flottes",
                5: "5️⃣ Sorties & erreurs",
            }[i],
            label_visibility="collapsed",
        )
    else:
        st.markdown("---")
        st.info(
            "Mode **moteur de règles** activé. Le flow classique est désactivé le temps "
            "de cette session. Ce mode applique les règles déclarées dans "
            "`src/rules/vehicle.yml` sur les fichiers déposés et génère uniquement "
            "l'output **Vehicle**."
        )


# ========== Engine mode (YAML rules engine, Vehicle only) ==========
def render_engine_page():
    st.header("🧪 Moteur de règles — Vehicle (beta)")
    st.caption(
        "Dépose les fichiers reçus pour un client. Le moteur applique les règles "
        "déclarées dans `src/rules/vehicle.yml` et produit le CSV Vehicle Revio. "
        "Seul le fichier **client** a besoin d'un mapping manuel (ses colonnes sont libres)."
    )

    # --- 1. Upload ---
    st.markdown("### 1. Upload des fichiers")
    uploaded = st.file_uploader(
        "Fichiers CSV / XLSX (loueurs, API Plaques, fichier client)",
        type=["csv", "xlsx", "xls", "xlsm"],
        accept_multiple_files=True,
        key="engine_uploader",
    )

    if uploaded:
        new_files: dict = {}
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
                new_files[key] = {
                    "df": df,
                    "filename": up.name,
                    "sheet_name": sheet_name,
                    "slug": default_slug,
                    "detected_type": detected.source_type,
                    "detected_reason": detected.reason,
                }
        st.session_state.engine_files = new_files
        st.session_state.engine_result = None  # invalidate previous run
        st.success(f"{len(new_files)} fichier(s) chargé(s).")

    engine_files = st.session_state.engine_files
    if not engine_files:
        st.info("Aucun fichier chargé.")
        return

    # --- 2. Type per file (slug override) ---
    st.markdown("### 2. Type (source YAML) par fichier")
    st.caption(
        "Le type détecté est pré-sélectionné. Tu peux corriger manuellement si besoin. "
        "Un fichier dont le type est `client_file` passera ensuite par une étape de mapping "
        "colonne par colonne (cf. ci-dessous)."
    )
    for key, info in engine_files.items():
        c1, c2 = st.columns([3, 2])
        with c1:
            label = info["filename"] + (f" [{info['sheet_name']}]" if info.get("sheet_name") else "")
            st.markdown(f"**{label}** ({len(info['df'])} lignes)")
            st.caption(f"Détecté: `{info['detected_type']}` — {info['detected_reason']}")
        with c2:
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

    # --- 3. Client file column mapping ---
    client_files = [(k, info) for k, info in engine_files.items() if info["slug"] == "client_file"]
    if client_files:
        st.markdown("### 3. Mapping du fichier client")
        st.caption(
            "Les colonnes du fichier client sont libres, on doit donc indiquer laquelle "
            "correspond à chaque champ Revio. Les champs non mappés seront ignorés pour ce fichier."
        )
        for key, info in client_files:
            df = info["df"]
            cols = [""] + [str(c) for c in df.columns]
            label = info["filename"] + (f" [{info['sheet_name']}]" if info.get("sheet_name") else "")
            with st.expander(f"🔗 {label} — mapping des champs", expanded=True):
                for field_name in CLIENT_FILE_MAPPABLE_FIELDS:
                    current = st.session_state.engine_overrides.get(("client_file", field_name), "")
                    if current not in cols:
                        current = ""
                    picked = st.selectbox(
                        field_name,
                        options=cols,
                        index=cols.index(current),
                        key=f"engine_override_client_{field_name}_{key}",
                    )
                    override_key = ("client_file", field_name)
                    if picked:
                        st.session_state.engine_overrides[override_key] = picked
                    else:
                        st.session_state.engine_overrides.pop(override_key, None)
    else:
        st.info(
            "Pas de fichier marqué `client_file` → le moteur prendra l'union des plaques "
            "loueurs comme parc de référence (un avertissement apparaîtra dans les issues)."
        )

    # --- 4. Run ---
    st.markdown("### 4. Lancer le moteur")
    if st.button("▶️ Appliquer les règles Vehicle", type="primary", use_container_width=True):
        source_dfs: dict = {}
        for key, info in engine_files.items():
            slug = info["slug"]
            # If multiple files share the same slug, we keep the last one (concat would be safer
            # but untested — we'll upgrade when a real use case shows up).
            source_dfs[slug] = info["df"]
        overrides = dict(st.session_state.engine_overrides)
        try:
            with st.spinner("Application des règles..."):
                result = rules_engine.run_vehicle(source_dfs, manual_column_overrides=overrides)
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
    st.metric("Véhicules produits", len(df))
    st.dataframe(df, use_container_width=True)

    if result.issues:
        st.markdown(f"#### ⚠️ {len(result.issues)} alerte(s)")
        issues_df = pd.DataFrame([
            {"plaque": i.plate, "champ": i.field, "source": i.source, "avertissement": i.warning}
            for i in result.issues
        ])
        st.dataframe(issues_df, use_container_width=True, hide_index=True)
    else:
        st.success("Aucune alerte — toutes les sources se sont bien alignées ✅")

    # --- 6. Download ---
    st.markdown("### 6. Export CSV Vehicle")
    client_name = st.session_state.client_name or "client"
    csv_bytes = df.reset_index(drop=True).to_csv(index=False, sep=";", encoding="utf-8").encode("utf-8")
    st.download_button(
        "⬇️ Télécharger vehicle.csv (Revio)",
        data=csv_bytes,
        file_name=f"vehicle_{client_name}.csv",
        mime="text/csv",
        type="primary",
        use_container_width=True,
    )
    if result.issues:
        issues_csv = pd.DataFrame([
            {"plaque": i.plate, "champ": i.field, "source": i.source, "avertissement": i.warning}
            for i in result.issues
        ]).to_csv(index=False, sep=";", encoding="utf-8").encode("utf-8")
        st.download_button(
            "⬇️ Télécharger issues.csv",
            data=issues_csv,
            file_name=f"issues_{client_name}.csv",
            mime="text/csv",
            use_container_width=True,
        )


if st.session_state.mode == "engine":
    render_engine_page()

# ========== Step 1: Upload ==========
elif st.session_state.step == 1:
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
elif st.session_state.step == 2:
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
elif st.session_state.step == 3:
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
elif st.session_state.step == 4:
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
elif st.session_state.step == 5:
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

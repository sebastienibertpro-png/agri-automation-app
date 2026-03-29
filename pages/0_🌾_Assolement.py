import streamlit as st
import pandas as pd
import numpy as np
import datetime
from shared import init_campaign_selector, inject_premium_css, render_premium_header

st.set_page_config(page_title="Assolement & Parcelles", page_icon="🌾", layout="wide")
inject_premium_css()

st.title("🌾 Gestion de l'Assolement & Parcelles")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")

# --- DEFENSIVE CLEANING ---
def clean_df_simple(df):
    d = df.copy()
    for col in d.columns:
        if d[col].dtype == object:
             d[col] = d[col].astype(str).replace(['nan', 'None', '<NA>', 'NAT', 'NaT'], '')
        elif pd.api.types.is_numeric_dtype(d[col]):
             d[col] = d[col].fillna(0.0)
    return d

# ══════════════════════════════════════════════════════════════════════════════
# CREATION MODE — Détecté via session_state
# ══════════════════════════════════════════════════════════════════════════════
creating = st.session_state.get("creating_new_campaign", False)
new_year = st.session_state.get("new_campaign_year", None)

ASSO_COLUMNS = [
    'Campagne', 'ID_Assolement', 'ID_Parcelle', 'Surface_Référence_Ha', 'Culture',
    'Variété', 'Precedent_Cultural', 'Strategie_Travail_Sol', 'Gestion_Résidus',
    'Contrat_Commercial', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T',
    'Couvert_précédent_Especes', 'Développement_Couvert', 'Date_Semis_Previsionnelle',
    'Commentaire_Assolement'
]
# Colonnes masquées dans l'éditeur (non pertinentes pour l'utilisateur)
ASSO_HIDDEN = {'Commentaire_Assolement', 'Image', 'ID_Assolement', 'Camp_Int'}

# --- Tabs (ordre dynamique) ---
if creating and new_year:
    tab_new, tab_asso, tab_ref = st.tabs([
        f"🆕 Nouvelle Campagne {new_year}",
        "🌾 Plan d'Assolement",
        "🗺️ Référentiel Parcelles"
    ])
else:
    tab_asso, tab_ref = st.tabs(["🌾 Plan d'Assolement", "🗺️ Référentiel Parcelles"])
    tab_new = None

# ══════════════════════════════════════════════════════════════════════════════
# TAB NOUVELLE CAMPAGNE
# ══════════════════════════════════════════════════════════════════════════════
if tab_new is not None:
    with tab_new:
        campagne_cible = new_year
        campagne_precedente = campagne_cible - 1

        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#1b5e20,#2e7d32);padding:20px 24px;border-radius:12px;margin-bottom:20px;">
            <h2 style="color:white;margin:0;font-size:1.5em;">🌱 Création de la campagne {campagne_cible}</h2>
            <p style="color:#c8e6c9;margin:6px 0 0 0;font-size:0.95em;">
                Définissez votre plan d'assolement pour débuter la saison. Toutes les autres données
                (interventions, PPF, irrigation…) seront saisies au fil de la campagne.
            </p>
        </div>
        """, unsafe_allow_html=True)

        # --- Charger les données nécessaires ---
        parc_ref = dl.get_parcelles()
        parc_opts = sorted([str(x) for x in parc_ref['ID_Parcelle'].unique()
                            if pd.notnull(x) and str(x) not in ('nan', '')]) if not parc_ref.empty else []

        df_asso_prec = dl.get_assolement(campagne_precedente)
        has_previous = not df_asso_prec.empty

        # --- Choix du point de départ ---
        st.markdown("### Choisissez votre point de départ")

        col_a, col_b = st.columns(2, gap="large")

        with col_a:
            st.markdown(f"""
            <div style="border:2px solid #e0e0e0;border-radius:12px;padding:18px;text-align:center;min-height:120px;">
                <div style="font-size:2em;">📋</div>
                <strong>Copier la campagne {campagne_precedente}</strong><br>
                <span style="font-size:0.85em;color:#666;">
                    {"Repart des mêmes parcelles & cultures — modifiable" if has_previous else "⚠️ Aucun assolement trouvé pour cette campagne"}
                </span>
            </div>
            """, unsafe_allow_html=True)
            btn_copy = st.button(
                f"📋 Copier la campagne {campagne_precedente}",
                use_container_width=True,
                disabled=not has_previous,
                key="btn_copy_prev"
            )

        with col_b:
            st.markdown(f"""
            <div style="border:2px solid #e0e0e0;border-radius:12px;padding:18px;text-align:center;min-height:120px;">
                <div style="font-size:2em;">✨</div>
                <strong>Partir de zéro</strong><br>
                <span style="font-size:0.85em;color:#666;">
                    Créer un assolement vide à partir des parcelles du référentiel
                </span>
            </div>
            """, unsafe_allow_html=True)
            btn_empty = st.button(
                "✨ Créer un assolement vide",
                use_container_width=True,
                key="btn_empty_asso"
            )

        # --- Initialisation du template dans session_state ---
        if btn_copy and has_previous:
            df_template = df_asso_prec.copy()
            # Colonnes à réinitialiser (données annuelles)
            reset_cols = ['Commentaire_Assolement', 'Développement_Couvert']
            for c in reset_cols:
                if c in df_template.columns:
                    df_template[c] = ''
            # Mise à jour de l'année
            df_template['Campagne'] = campagne_cible
            # Nouveau ID assolement
            if 'ID_Assolement' in df_template.columns:
                df_template['ID_Assolement'] = df_template['ID_Parcelle'].apply(
                    lambda p: f"ASSOL_{campagne_cible}_{p}"
                )
            # Ajouter colonnes manquantes
            for col in ASSO_COLUMNS:
                if col not in df_template.columns:
                    df_template[col] = ''
            df_template = df_template[ASSO_COLUMNS]
            st.session_state["new_asso_df"] = df_template
            st.success(f"✅ Assolement {campagne_precedente} chargé — modifiez les données ci-dessous.")
            st.rerun()

        if btn_empty:
            if parc_opts:
                df_template = pd.DataFrame({
                    'Campagne': campagne_cible,
                    'ID_Assolement': [f"ASSOL_{campagne_cible}_{p}" for p in parc_opts],
                    'ID_Parcelle': parc_opts,
                    'Surface_Référence_Ha': [
                        parc_ref[parc_ref['ID_Parcelle'].astype(str) == str(p)]['Surface_Référence_Ha'].values[0]
                        if not parc_ref[parc_ref['ID_Parcelle'].astype(str) == str(p)].empty else 0.0
                        for p in parc_opts
                    ],
                    'Culture': '',
                    'Variété': '',
                    'Precedent_Cultural': '',
                    'Strategie_Travail_Sol': '',
                    'Gestion_Résidus': '',
                    'Contrat_Commercial': '',
                    'Objectif_Rendement_Qx_Ha': 0.0,
                    'Prix_Vente_Objectif_€/T': 0.0,
                    'Couvert_précédent_Especes': '',
                    'Développement_Couvert': '',
                    'Date_Semis_Previsionnelle': None,
                    'Commentaire_Assolement': '',
                })
            else:
                # Pas de parcelles de référence : tableau vide avec 1 ligne
                df_template = pd.DataFrame(columns=ASSO_COLUMNS)
                df_template.loc[0] = [''] * len(ASSO_COLUMNS)
                df_template.at[0, 'Campagne'] = campagne_cible
            st.session_state["new_asso_df"] = df_template
            st.success("✅ Assolement vide initié — remplissez le tableau ci-dessous.")
            st.rerun()

        # --- Affichage de l'éditeur si un template est prêt ---
        if "new_asso_df" in st.session_state and st.session_state["new_asso_df"] is not None:
            df_edit_new = st.session_state["new_asso_df"].copy()

            # Nettoyage types
            df_edit_new['Surface_Référence_Ha'] = pd.to_numeric(df_edit_new['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
            df_edit_new['Objectif_Rendement_Qx_Ha'] = pd.to_numeric(df_edit_new['Objectif_Rendement_Qx_Ha'], errors='coerce').fillna(0.0)
            df_edit_new['Prix_Vente_Objectif_€/T'] = pd.to_numeric(df_edit_new['Prix_Vente_Objectif_€/T'], errors='coerce').fillna(0.0)
            df_edit_new['Date_Semis_Previsionnelle'] = pd.to_datetime(
                df_edit_new['Date_Semis_Previsionnelle'], errors='coerce').dt.date
            df_edit_new['ID_Parcelle'] = df_edit_new['ID_Parcelle'].astype(str).replace(['nan', 'None'], '')

            st.divider()
            render_premium_header(
                f"🌾 Plan d'Assolement — Campagne {campagne_cible}",
                "Modifiez les données, puis validez en bas ✍️",
                color="green"
            )

            # Nettoyage des "None" textuels avant affichage
            for col in df_edit_new.select_dtypes(include='object').columns:
                df_edit_new[col] = df_edit_new[col].replace(['None', 'nan', 'NaT', 'NAT', '<NA>'], '')

            # Masquer toutes les colonnes non pertinentes
            col_config_new = {
                "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
                "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=parc_opts),
                "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
                "Strategie_Travail_Sol": st.column_config.SelectboxColumn(
                    "Stratégie", options=["Labour", "TCS", "Semis Direct", ""]),
                "Objectif_Rendement_Qx_Ha": st.column_config.NumberColumn("Obj Rdt"),
                "Prix_Vente_Objectif_€/T": st.column_config.NumberColumn("Prix Obj"),
                "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis"),
            }
            # Masquer dynamiquement toutes les colonnes cachées présentes dans le df
            for hidden_col in ASSO_HIDDEN:
                col_config_new[hidden_col] = None

            edited_new = st.data_editor(
                df_edit_new,
                column_config=col_config_new,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key="editor_new_camp"
            )
            # Mise à jour live dans session_state
            st.session_state["new_asso_df"] = edited_new

            # --- Récapitulatif ---
            st.divider()
            st.markdown("#### 📊 Récapitulatif avant validation")
            edited_new['Surface_Référence_Ha'] = pd.to_numeric(edited_new['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
            valid_rows = edited_new[edited_new['ID_Parcelle'].astype(str).str.strip() != '']
            cultures_uniques = valid_rows['Culture'].replace('', pd.NA).dropna().unique()

            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("Parcelles configurées", len(valid_rows))
            rc2.metric("Surface totale", f"{valid_rows['Surface_Référence_Ha'].sum():.1f} ha")
            rc3.metric("Cultures distinctes", len(cultures_uniques))

            if len(cultures_uniques) > 0:
                st.markdown("**Cultures :** " + " · ".join(cultures_uniques))

            # --- Validation ---
            st.divider()
            if len(valid_rows) == 0:
                st.warning("⚠️ Aucune parcelle configurée — remplissez au moins une ligne avant de valider.")
            else:
                col_btn1, col_btn2 = st.columns([2, 1])
                with col_btn1:
                    if st.button(
                        f"✅ Créer la campagne {campagne_cible} ({len(valid_rows)} parcelles — {valid_rows['Surface_Référence_Ha'].sum():.0f} ha)",
                        type="primary",
                        use_container_width=True,
                        key="btn_save_new_camp"
                    ):
                        with st.spinner(f"Création de la campagne {campagne_cible}..."):
                            # Préparer le dataframe final
                            edited_new['Campagne'] = campagne_cible
                            if 'Camp_Int' in edited_new.columns:
                                edited_new = edited_new.drop(columns=['Camp_Int'])

                            # Lire l'assolement existant et concaténer
                            df_asso_existing = dl._get_data("ASSOLEMENT")
                            if not df_asso_existing.empty:
                                # Supprimer toute ligne déjà présente pour cette campagne
                                df_asso_existing['Camp_Int'] = pd.to_numeric(
                                    df_asso_existing['Campagne'], errors='coerce').fillna(0).astype(int)
                                df_asso_existing = df_asso_existing[
                                    df_asso_existing['Camp_Int'] != int(campagne_cible)]
                                df_asso_existing = df_asso_existing.drop(columns=['Camp_Int'], errors='ignore')
                                final_df = pd.concat([df_asso_existing, edited_new], ignore_index=True)
                            else:
                                final_df = edited_new

                            if dl.overwrite_worksheet("ASSOLEMENT", final_df):
                                st.success(f"🎉 Campagne {campagne_cible} créée avec succès !")
                                # Reset création mode
                                st.session_state["creating_new_campaign"] = False
                                st.session_state["new_campaign_year"] = None
                                st.session_state["new_asso_df"] = None
                                st.session_state["selected_campaign_label"] = str(campagne_cible)
                                st.balloons()
                                st.rerun()
                with col_btn2:
                    if st.button("🗑️ Annuler", use_container_width=True, key="btn_cancel_new"):
                        st.session_state["new_asso_df"] = None
                        st.session_state["creating_new_campaign"] = False
                        st.session_state["new_campaign_year"] = None
                        if "selected_campaign_label" in st.session_state:
                            del st.session_state["selected_campaign_label"]
                        st.rerun()

        else:
            # Pas encore de template choisi
            st.info("👆 Choisissez un point de départ ci-dessus pour commencer la saisie.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB ASSOLEMENT (existant)
# ══════════════════════════════════════════════════════════════════════════════
campagne_input = int(selected_campaign)

with tab_asso:
    df_asso_all = dl.get_assolement()

    if df_asso_all.empty:
         cols = ASSO_COLUMNS
         df_asso_all = pd.DataFrame(columns=cols)

    # Filter
    df_asso_all['Camp_Int'] = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').fillna(0).astype(int)
    df_curr_asso = df_asso_all[df_asso_all['Camp_Int'] == campagne_input].copy()
    df_others = df_asso_all[df_asso_all['Camp_Int'] != campagne_input].copy()

    st.subheader(f"📊 Résumé {campagne_input}")
    if not df_curr_asso.empty:
        df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
        summary = df_curr_asso.groupby('Culture')['Surface_Référence_Ha'].sum().reset_index()
        summary = summary.sort_values(by='Surface_Référence_Ha', ascending=False)
        cs = st.columns(min(len(summary), 6) if len(summary) > 0 else 1)
        for i, row in summary.iterrows():
            if i < len(cs): cs[i].metric(row['Culture'], f"{row['Surface_Référence_Ha']:.1f} ha")
    else:
        st.info("Aucun assolement pour cette campagne.")
        if creating:
            st.caption(f"💡 Utilisez l'onglet **🆕 Nouvelle Campagne {new_year}** pour créer cet assolement.")

    st.divider()

    # Cleaning Assolement (working version)
    df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Objectif_Rendement_Qx_Ha'] = pd.to_numeric(df_curr_asso['Objectif_Rendement_Qx_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Prix_Vente_Objectif_€/T'] = pd.to_numeric(df_curr_asso['Prix_Vente_Objectif_€/T'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Date_Semis_Previsionnelle'] = pd.to_datetime(df_curr_asso['Date_Semis_Previsionnelle'], errors='coerce').dt.date
    df_curr_asso['ID_Parcelle'] = df_curr_asso['ID_Parcelle'].astype(str).replace(['nan', 'None'], '')

    parc_ref = dl.get_parcelles()
    parc_opts = sorted([str(x) for x in parc_ref['ID_Parcelle'].unique() if pd.notnull(x) and str(x) != 'nan']) if not parc_ref.empty else []

    # Nettoyage des "None" textuels avant affichage
    for col in df_curr_asso.select_dtypes(include='object').columns:
        df_curr_asso[col] = df_curr_asso[col].replace(['None', 'nan', 'NaT', 'NAT', '<NA>'], '')

    col_config_asso = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=parc_opts),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
        "Strategie_Travail_Sol": st.column_config.SelectboxColumn("Stratégie", options=["Labour", "TCS", "Semis Direct", ""]),
        "Objectif_Rendement_Qx_Ha": st.column_config.NumberColumn("Obj Rdt"),
        "Prix_Vente_Objectif_€/T": st.column_config.NumberColumn("Prix Obj"),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis"),
    }
    # Masquer dynamiquement toutes les colonnes cachées présentes dans le df
    for hidden_col in ASSO_HIDDEN:
        col_config_asso[hidden_col] = None

    render_premium_header("🌾 Plan d'Assolement", f"Campagne {campagne_input} — Modifications directes autorisées ✍️", color="green")

    try:
        edited_df = st.data_editor(df_curr_asso, column_config=col_config_asso, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_asso")

        if st.button("💾 Sauvegarder Assolement", type="primary", use_container_width=True):
            with st.spinner("Enregistrement..."):
                edited_df['Campagne'] = campagne_input
                if 'Camp_Int' in edited_df.columns: edited_df = edited_df.drop(columns=['Camp_Int'])
                others_clean = df_others.drop(columns=['Camp_Int']) if 'Camp_Int' in df_others.columns else df_others
                final_df = pd.concat([others_clean, edited_df], ignore_index=True)
                if dl.overwrite_worksheet("ASSOLEMENT", final_df):
                    st.success("Sauvegardé !"); st.rerun()
    except Exception as e:
        st.error(f"Erreur d'affichage du tableau d'assolement : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB REF_PARCELLES
# ══════════════════════════════════════════════════════════════════════════════
with tab_ref:
    render_premium_header("🗺️ Référentiel Parcelles", "Données fixes de l'exploitation 📍", color="green")
    df_ref = dl.get_parcelles()

    df_ref_view = clean_df_simple(df_ref)

    col_config_ref = {
        "ID_Parcelle": "ID Parc",
        "Nom Terrain": "Terrain",
        "îlot PAC": "PAC",
        "Commune": "Commune",
        "Surface_Référence_Ha": "Surf Réf",
        "Type_sol": "Sol",
        "Analyse_sol": "Analyse",
        "Drainage": "Drainé",
        "Irrigation (oui/non)": "Irrig.",
        "Type irrigation": "Matériel",
        "ZNT Riverain": "ZNT Riv",
        "ZNT Aqua": "ZNT Aqua",
        "Débit_Irrigation_m3/H": "Débit",
        "RU_estimée": "RU",
        "GPS": "GPS"
    }

    try:
        edited_ref = st.data_editor(df_ref_view, column_config=col_config_ref, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_ref")

        if st.button("💾 Sauvegarder Parcelles", use_container_width=True):
            with st.spinner("Mise à jour..."):
                if dl.overwrite_worksheet("REF_PARCELLES", edited_ref):
                    st.success("Référentiel mis à jour !"); st.rerun()
    except Exception as e:
        st.error(f"Erreur d'affichage du tableau des parcelles : {e}")
        st.info("Tentative d'affichage brut...")
        st.data_editor(df_ref_view)




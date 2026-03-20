import streamlit as st
import pandas as pd
import numpy as np
from shared import init_campaign_selector

st.set_page_config(page_title="Assolement & Parcelles", page_icon="🌾", layout="wide")

st.title("🌾 Gestion de l'Assolement & Parcelles")
st.markdown("---")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader
campagne_input = int(selected_campaign)

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")

# Tabs
tab_asso, tab_ref = st.tabs(["🌾 Plan d'Assolement", "🗺️ Référentiel Parcelles"])

# --- TAB 1: ASSOLEMENT ---
with tab_asso:
    df_asso_all = dl.get_assolement() 
    
    if df_asso_all.empty:
         cols = ['Campagne', 'ID_Assolement', 'ID_Parcelle', 'Surface_Référence_Ha', 'Culture', 'Variété', 'Precedent_Cultural', 'Strategie_Travail_Sol', 'Gestion_Résidus', 'Contrat_Commercial', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T', 'Couvert_précédent_Especes', 'Développement_Couvert', 'Date_Semis_Previsionnelle', 'Commentaire_Assolement']
         df_asso_all = pd.DataFrame(columns=cols)

    # Filter for display
    df_asso_all['Camp_Int'] = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').fillna(0).astype(int)
    df_curr_asso = df_asso_all[df_asso_all['Camp_Int'] == campagne_input].copy()
    df_others = df_asso_all[df_asso_all['Camp_Int'] != campagne_input].copy()
    
    st.subheader(f"📊 Résumé Campagne {campagne_input}")
    if not df_curr_asso.empty:
        # Ensure surface is numeric for sum
        df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
        summary = df_curr_asso.groupby('Culture')['Surface_Référence_Ha'].sum().reset_index()
        summary = summary.sort_values(by='Surface_Référence_Ha', ascending=False)
        cols_summary = st.columns(min(len(summary), 6) if len(summary) > 0 else 1)
        for i, row in summary.iterrows():
            if i < len(cols_summary):
                cols_summary[i].metric(row['Culture'], f"{row['Surface_Référence_Ha']:.1f} ha")
    else:
        st.info("Aucune culture enregistrée pour cette campagne.")

    st.divider()
    st.subheader("📝 Modifier l'Assolement")
    
    # --- ULTRA DEFENSIVE DATA CLEANING ---
    def clean_df_for_editor(df, is_ref=False):
        d = df.copy()
        for col in d.columns:
            # Handle numeric columns first
            if col in ['Campagne', 'Surface_Référence_Ha', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T', 'ZNT Riverain', 'ZNT Aqua', 'Débit_Irrigation_m3/H', 'RU_estimée']:
                d[col] = pd.to_numeric(d[col], errors='coerce').fillna(0.0)
            
            # Handle date columns
            if col == 'Date_Semis_Previsionnelle':
                # Convert to datetime then to date objects, handling NaT
                d[col] = pd.to_datetime(d[col], errors='coerce')
                # st.data_editor likes datetime.date or None
                d[col] = d[col].apply(lambda x: x.date() if pd.notnull(x) else None)
            
            # Handle boolean columns for REF_PARCELLES
            if col in ['Analyse_sol', 'Drainage']:
                d[col] = d[col].astype(str).str.upper().isin(['OUI', 'TRUE', 'VRAI', '1'])
                
            # Handle categorical / text
            if col in ['ID_Parcelle', 'Culture', 'Variété', 'Precedent_Cultural', 'Strategie_Travail_Sol', 'Gestion_Résidus', 'Contrat_Commercial', 'Couvert_précédent_Especes', 'Développement_Couvert']:
                d[col] = d[col].astype(str).replace(['nan', 'None', 'None'], '')
        
        return d

    df_curr_asso_clean = clean_df_for_editor(df_curr_asso)

    # options lists
    parcelle_options = sorted(dl.get_parcelles()['ID_Parcelle'].astype(str).unique().tolist()) if not dl.get_parcelles().empty else []
    # Ensure all current parcel IDs are in options to avoid mismatch errors
    current_parcel_ids = df_curr_asso_clean['ID_Parcelle'].unique().tolist()
    parcelle_options = sorted(list(set(parcelle_options) | set(current_parcel_ids)))
    if '' in parcels_options: parcelle_options.remove('')

    col_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Assolement": None,
        "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=parcelle_options, required=True),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f ha"),
        "Culture": st.column_config.TextColumn("Culture"),
        "Variété": st.column_config.TextColumn("Variété"),
        "Precedent_Cultural": st.column_config.TextColumn("Précédent"),
        "Strategie_Travail_Sol": st.column_config.SelectboxColumn("Stratégie", options=["Labour", "TCS", "Semis Direct", ""]),
        "Gestion_Résidus": st.column_config.SelectboxColumn("Résidus", options=["Enfouis", "Exportés", ""]),
        "Contrat_Commercial": st.column_config.TextColumn("Contrat"),
        "Objectif_Rendement_Qx_Ha": st.column_config.NumberColumn("Obj Rdt (Qx)"),
        "Prix_Vente_Objectif_€/T": st.column_config.NumberColumn("Prix Obj (€/T)"),
        "Couvert_précédent_Especes": st.column_config.TextColumn("Couvert"),
        "Développement_Couvert": st.column_config.SelectboxColumn("Dév. Couv.", options=["Nul", "Faible", "Moyen", "Fort", ""]),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis Prévu"),
        "Commentaire_Assolement": st.column_config.TextColumn("Commentaires"),
        "Camp_Int": None
    }

    # If any column from col_config is missing in df, add it empty to avoid DataEditor complaining
    for col in col_config.keys():
        if col and col not in df_curr_asso_clean.columns:
            df_curr_asso_clean[col] = ""

    edited_df = st.data_editor(df_curr_asso_clean, column_config=col_config, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_asso")

    if st.button("💾 Sauvegarder l'Assolement", type="primary", use_container_width=True):
        with st.spinner("Enregistrement..."):
            edited_df['Campagne'] = campagne_input
            if 'Camp_Int' in edited_df.columns: edited_df = edited_df.drop(columns=['Camp_Int'])
            if 'Camp_Int' in df_others.columns: df_others = df_others.drop(columns=['Camp_Int'])
            # Clean possible NaT/nan before saving
            final_save_df = pd.concat([df_others, edited_df], ignore_index=True)
            if dl.overwrite_worksheet("ASSOLEMENT", final_save_df):
                st.success("Assolement sauvegardé !")
                st.rerun()

# --- TAB 2: REF_PARCELLES ---
with tab_ref:
    st.subheader("🗺️ Référentiel des Parcelles")
    df_ref = dl.get_parcelles()
    df_ref_clean = clean_df_for_editor(df_ref, is_ref=True)
    
    col_config_ref = {
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle", required=True),
        "Nom Terrain": st.column_config.TextColumn("Nom Terrain"),
        "îlot PAC": st.column_config.TextColumn("PAC Ilot"),
        "Commune": st.column_config.TextColumn("Commune"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf Réf (ha)", format="%.2f ha"),
        "Type_sol": st.column_config.TextColumn("Type Sol"),
        "Analyse_sol": st.column_config.CheckboxColumn("Analyse ?"),
        "Drainage": st.column_config.CheckboxColumn("Drainé ?"),
        "Irrigation (oui/non)": st.column_config.SelectboxColumn("Irrig.", options=["OUI", "NON", ""]),
        "Type irrigation": st.column_config.TextColumn("Matériel Irri"),
        "ZNT Riverain": st.column_config.NumberColumn("ZNT Riv (m)"),
        "ZNT Aqua": st.column_config.NumberColumn("ZNT Aqua (m)"),
        "Débit_Irrigation_m3/H": st.column_config.NumberColumn("Débit m3/h"),
        "RU_estimée": st.column_config.NumberColumn("RU (mm)"),
        "GPS": st.column_config.TextColumn("Coordonnées GPS")
    }
    
    edited_ref = st.data_editor(df_ref_clean, column_config=col_config_ref, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_ref")
    
    if st.button("💾 Sauvegarder le Référentiel Parcelles", type="secondary", use_container_width=True):
        with st.spinner("Mise à jour..."):
            if dl.overwrite_worksheet("REF_PARCELLES", edited_ref):
                st.success("Référentiel parcelles mis à jour !")
                st.rerun()

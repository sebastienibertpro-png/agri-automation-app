import streamlit as st
import pandas as pd
import numpy as np
import datetime
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
    
    # 1. SUMMARY
    st.subheader(f"📊 Résumé Campagne {campagne_input}")
    
    # Pre-process columns safely
    if df_asso_all.empty:
         cols = ['Campagne', 'ID_Assolement', 'ID_Parcelle', 'Surface_Référence_Ha', 'Culture', 'Variété', 'Precedent_Cultural', 'Strategie_Travail_Sol', 'Gestion_Résidus', 'Contrat_Commercial', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T', 'Couvert_précédent_Especes', 'Développement_Couvert', 'Date_Semis_Previsionnelle', 'Commentaire_Assolement']
         df_asso_all = pd.DataFrame(columns=cols)

    df_asso_all['Camp_Int'] = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').fillna(0).astype(int)
    df_curr_asso = df_asso_all[df_asso_all['Camp_Int'] == campagne_input].copy()
    df_others = df_asso_all[df_asso_all['Camp_Int'] != campagne_input].copy()
    
    if not df_curr_asso.empty:
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
    
    # --- RIGOROUS TYPE INITIALIZATION ---
    def clean_df(df, current_camp):
        d = df.copy()
        expected_cols = {
            'Campagne': int,
            'ID_Assolement': str,
            'ID_Parcelle': str,
            'Surface_Référence_Ha': float,
            'Culture': str,
            'Variété': str,
            'Precedent_Cultural': str,
            'Strategie_Travail_Sol': str,
            'Gestion_Résidus': str,
            'Contrat_Commercial': str,
            'Objectif_Rendement_Qx_Ha': float,
            'Prix_Vente_Objectif_€/T': float,
            'Couvert_précédent_Especes': str,
            'Développement_Couvert': str,
            'Date_Semis_Previsionnelle': 'date',
            'Commentaire_Assolement': str
        }
        
        # Ensure all columns exist
        for col, t in expected_cols.items():
            if col not in d.columns:
                if t == float: d[col] = 0.0
                elif t == int: d[col] = current_camp
                elif t == 'date': d[col] = None
                else: d[col] = ""
        
        # Apply strict casting
        for col, t in expected_cols.items():
            if t == float:
                d[col] = pd.to_numeric(d[col], errors='coerce').fillna(0.0).astype(float)
            elif t == int:
                d[col] = pd.to_numeric(d[col], errors='coerce').fillna(current_camp).astype(int)
            elif t == 'date':
                d[col] = pd.to_datetime(d[col], errors='coerce')
                d[col] = d[col].apply(lambda x: x.date() if isinstance(x, (pd.Timestamp, datetime.datetime, datetime.date)) and pd.notnull(x) else None)
            elif t == str:
                d[col] = d[col].astype(str).replace(['nan', 'None', 'NAT', 'NaT'], '')
        
        return d

    df_curr_clean = clean_df(df_curr_asso, campagne_input)

    # Options lists - Strictly strings
    parc_ref = dl.get_parcelles()
    parcelle_options = sorted([str(x) for x in parc_ref['ID_Parcelle'].unique() if pd.notnull(x) and str(x) != 'nan']) if not parc_ref.empty else []
    # Add current existing IDs that might be missing from ref
    curr_ids = [str(x) for x in df_curr_clean['ID_Parcelle'].unique() if x and str(x) != 'nan']
    parcelle_options = sorted(list(set(parcelle_options) | set(curr_ids)))
    if '' in parcelle_options: parcelle_options.remove('')

    col_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Assolement": None,
        "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=parcelle_options),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
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

    # Hide columns present in DF but not in config
    for col in df_curr_clean.columns:
        if col not in col_config and col != 'Camp_Int':
            col_config[col] = None

    edited_df = st.data_editor(df_curr_clean, column_config=col_config, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_asso")

    if st.button("💾 Sauvegarder l'Assolement", type="primary", use_container_width=True):
        with st.spinner("Enregistrement..."):
            edited_df['Campagne'] = campagne_input
            cols_to_drop = ['Camp_Int']
            for c in edited_df.columns:
                if c not in df_aso_all.columns and c != 'Campagne': cols_to_drop.append(c)
            save_df = edited_df.drop(columns=[c for c in cols_to_drop if c in edited_df.columns])
            
            others_clean = df_others.drop(columns=['Camp_Int']) if 'Camp_Int' in df_others.columns else df_others
            final_save_df = pd.concat([others_clean, save_df], ignore_index=True)
            if dl.overwrite_worksheet("ASSOLEMENT", final_save_df):
                st.success("Assolement sauvegardé !")
                st.rerun()

# --- TAB 2: REF_PARCELLES ---
with tab_ref:
    st.subheader("🗺️ Référentiel des Parcelles")
    df_ref = dl.get_parcelles()
    
    # Simple cleaning for Ref Parcelles
    df_ref_clean = df_ref.copy()
    num_p = ['Surface_Référence_Ha', 'ZNT Riverain', 'ZNT Aqua', 'Débit_Irrigation_m3/H', 'RU_estimée']
    for c in num_p:
        if c in df_ref_clean.columns:
            df_ref_clean[c] = pd.to_numeric(df_ref_clean[c], errors='coerce').fillna(0.0).astype(float)
    
    for c in ['Analyse_sol', 'Drainage']:
        if c in df_ref_clean.columns:
            df_ref_clean[c] = df_ref_clean[c].astype(str).str.upper().isin(['OUI', 'TRUE', 'VRAI', '1'])

    col_config_ref = {
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle"),
        "Nom Terrain": st.column_config.TextColumn("Nom Terrain"),
        "îlot PAC": st.column_config.TextColumn("PAC Ilot"),
        "Commune": st.column_config.TextColumn("Commune"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf Réf (ha)", format="%.2f"),
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

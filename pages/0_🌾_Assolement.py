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

# --- DEFENSIVE CLEANING FUNCTION ---
def clean_df_with_schema(df, schema, default_camp=2024):
    d = df.copy()
    # Ensure all columns exist
    for col, t in schema.items():
        if col not in d.columns:
            if t == float: d[col] = 0.0
            elif t == int: d[col] = default_camp
            elif t == bool: d[col] = False
            elif t == 'date': d[col] = None
            else: d[col] = ""
    
    # Apply strict casting
    for col in d.columns:
        if col in schema:
            t = schema[col]
            if t == float:
                d[col] = pd.to_numeric(d[col], errors='coerce').fillna(0.0).astype(float)
            elif t == int:
                d[col] = pd.to_numeric(d[col], errors='coerce').fillna(default_camp).astype(int)
            elif t == bool:
                # Convert OUI/NON or 1/0 or strings to boolean
                d[col] = d[col].astype(str).str.upper().isin(['OUI', 'TRUE', 'VRAI', '1', 'YES'])
            elif t == 'date':
                d[col] = pd.to_datetime(d[col], errors='coerce')
                d[col] = d[col].apply(lambda x: x.date() if isinstance(x, (pd.Timestamp, datetime.datetime, datetime.date)) and pd.notnull(x) else None)
            elif t == str:
                d[col] = d[col].astype(str).replace(['nan', 'None', 'NAT', 'NaT', 'nan', '<NA>'], '')
        else:
            # For columns not in schema, force to string to avoid mixed types
            d[col] = d[col].astype(str).replace(['nan', 'None', 'nan', '<NA>'], '')
    
    return d

# --- TAB 1: ASSOLEMENT ---
with tab_asso:
    df_asso_all = dl.get_assolement() 
    
    st.subheader(f"📊 Résumé Campagne {campagne_input}")
    
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
        cs = st.columns(min(len(summary), 6) if len(summary) > 0 else 1)
        for i, row in summary.iterrows():
            if i < len(cs): cs[i].metric(row['Culture'], f"{row['Surface_Référence_Ha']:.1f} ha")
    else:
        st.info("Aucun assolement pour cette campagne.")

    st.divider()
    st.subheader("📝 Modifier l'Assolement")
    
    asso_schema = {
        'Campagne': int, 'ID_Assolement': str, 'ID_Parcelle': str, 'Surface_Référence_Ha': float,
        'Culture': str, 'Variété': str, 'Precedent_Cultural': str, 'Strategie_Travail_Sol': str,
        'Gestion_Résidus': str, 'Contrat_Commercial': str, 'Objectif_Rendement_Qx_Ha': float,
        'Prix_Vente_Objectif_€/T': float, 'Couvert_précédent_Especes': str, 'Développement_Couvert': str,
        'Date_Semis_Previsionnelle': 'date', 'Commentaire_Assolement': str
    }
    df_curr_clean = clean_df_with_schema(df_curr_asso, asso_schema, campagne_input)

    parc_ref = dl.get_parcelles()
    parc_opts = sorted([str(x) for x in parc_ref['ID_Parcelle'].unique() if pd.notnull(x) and str(x) != 'nan']) if not parc_ref.empty else []
    curr_ids = [str(x) for x in df_curr_clean['ID_Parcelle'].unique() if x and str(x) != 'nan']
    parc_opts = sorted(list(set(parc_opts) | set(curr_ids)))
    if '' in parc_opts: parc_opts.remove('')

    asso_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Assolement": None,
        "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=parc_opts),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
        "Strategie_Travail_Sol": st.column_config.SelectboxColumn("Stratégie", options=["Labour", "TCS", "Semis Direct", ""]),
        "Gestion_Résidus": st.column_config.SelectboxColumn("Résidus", options=["Enfouis", "Exportés", ""]),
        "Développement_Couvert": st.column_config.SelectboxColumn("Dév. Couv.", options=["Nul", "Faible", "Moyen", "Fort", ""]),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis Prévu"),
        "Objectif_Rendement_Qx_Ha": st.column_config.NumberColumn("Obj Rdt"),
        "Prix_Vente_Objectif_€/T": st.column_config.NumberColumn("Prix Obj"),
        "Camp_Int": None
    }
    # Hide others
    for col in df_curr_clean.columns:
        if col not in asso_config and col != 'Camp_Int':
            if col in ['Culture', 'Variété', 'Precedent_Cultural', 'Contrat_Commercial', 'Couvert_précédent_Especes', 'Commentaire_Assolement']:
                asso_config[col] = st.column_config.TextColumn(col.replace('_', ' '))
            else:
                asso_config[col] = None

    edited_df = st.data_editor(df_curr_clean, column_config=asso_config, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_asso")

    if st.button("💾 Sauvegarder l'Assolement", type="primary", use_container_width=True):
        with st.spinner("Enregistrement..."):
            edited_df['Campagne'] = campagne_input
            if 'Camp_Int' in edited_df.columns: edited_df = edited_df.drop(columns=['Camp_Int'])
            others_clean = df_others.drop(columns=['Camp_Int']) if 'Camp_Int' in df_others.columns else df_others
            final_df = pd.concat([others_clean, edited_df], ignore_index=True)
            if dl.overwrite_worksheet("ASSOLEMENT", final_df):
                st.success("Sauvegardé !"); st.rerun()

# --- TAB 2: REF_PARCELLES ---
with tab_ref:
    st.subheader("🗺️ Référentiel des Parcelles")
    df_ref = dl.get_parcelles()
    
    ref_schema = {
        'ID_Parcelle': str, 'Nom Terrain': str, 'îlot PAC': str, 'Commune': str,
        'Surface_Référence_Ha': float, 'Type_sol': str, 'Analyse_sol': bool, 'Drainage': bool,
        'Irrigation (oui/non)': str, 'Type irrigation': str, 'ZNT Riverain': float,
        'ZNT Aqua': float, 'Débit_Irrigation_m3/H': float, 'RU_estimée': float, 'GPS': str
    }
    df_ref_clean = clean_df_with_schema(df_ref, ref_schema)

    ref_config = {
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf Réf", format="%.2f"),
        "Analyse_sol": st.column_config.CheckboxColumn("Analyse ?"),
        "Drainage": st.column_config.CheckboxColumn("Drainé ?"),
        "Irrigation (oui/non)": st.column_config.SelectboxColumn("Irrig.", options=["OUI", "NON", ""]),
        "ZNT Riverain": st.column_config.NumberColumn("ZNT Riv"),
        "ZNT Aqua": st.column_config.NumberColumn("ZNT Aqua"),
        "Débit_Irrigation_m3/H": st.column_config.NumberColumn("Débit"),
        "îlot PAC": st.column_config.TextColumn("PAC Ilot")
    }
    # Automate missing TextColumns and hide unknown
    for col in df_ref_clean.columns:
        if col not in ref_config:
            if col in ref_schema: ref_config[col] = st.column_config.TextColumn(col.replace('_', ' '))
            else: ref_config[col] = None

    edited_ref = st.data_editor(df_ref_clean, column_config=ref_config, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_ref")
    
    if st.button("💾 Sauvegarder le Référentiel Parcelles", use_container_width=True):
        with st.spinner("Mise à jour..."):
            if dl.overwrite_worksheet("REF_PARCELLES", edited_ref):
                st.success("Référentiel mis à jour !"); st.rerun()

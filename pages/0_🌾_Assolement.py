import streamlit as st
import pandas as pd
import numpy as np
import datetime
from shared import init_campaign_selector

st.set_page_config(page_title="Assolement & Parcelles", page_icon="🌾", layout="wide")

st.title("🌾 Gestion de l'Assolement & Parcelles")
st.markdown("---")

# --- Custom UI Styling ---
st.markdown("""
<style>
    [data-testid="stDataEditor"] {
        border-radius: 12px;
        border: 1px solid #2e7d32;
        box-shadow: 0 4px 15px rgba(46, 125, 50, 0.15);
        padding: 4px;
        background-color: white;
    }
    .table-header {
        background-color: #2e7d32;
        color: white;
        padding: 10px 15px;
        border-radius: 10px 10px 0 0;
        font-weight: bold;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: -5px;
        position: relative;
        z-index: 10;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .table-header.blue { background-color: #1f77b4; }
</style>
""", unsafe_allow_html=True)

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader
campagne_input = int(selected_campaign)

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")

# Tabs
tab_asso, tab_ref = st.tabs(["🌾 Plan d'Assolement", "🗺️ Référentiel Parcelles"])

# --- DEFENSIVE CLEANING ---
def clean_df_simple(df):
    d = df.copy()
    # Replace all nan flavors with empty strings or reasonable defaults
    for col in d.columns:
        if d[col].dtype == object:
             d[col] = d[col].astype(str).replace(['nan', 'None', '<NA>', 'NAT', 'NaT'], '')
        elif pd.api.types.is_numeric_dtype(d[col]):
             d[col] = d[col].fillna(0.0)
    return d

# --- TAB 1: ASSOLEMENT ---
with tab_asso:
    df_asso_all = dl.get_assolement() 
    
    if df_asso_all.empty:
         cols = ['Campagne', 'ID_Assolement', 'ID_Parcelle', 'Surface_Référence_Ha', 'Culture', 'Variété', 'Precedent_Cultural', 'Strategie_Travail_Sol', 'Gestion_Résidus', 'Contrat_Commercial', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T', 'Couvert_précédent_Especes', 'Développement_Couvert', 'Date_Semis_Previsionnelle', 'Commentaire_Assolement']
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

    st.divider()
    
    # Cleaning Assolement (working version)
    df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Objectif_Rendement_Qx_Ha'] = pd.to_numeric(df_curr_asso['Objectif_Rendement_Qx_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Prix_Vente_Objectif_€/T'] = pd.to_numeric(df_curr_asso['Prix_Vente_Objectif_€/T'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Date_Semis_Previsionnelle'] = pd.to_datetime(df_curr_asso['Date_Semis_Previsionnelle'], errors='coerce').dt.date
    df_curr_asso['ID_Parcelle'] = df_curr_asso['ID_Parcelle'].astype(str).replace(['nan', 'None'], '')

    parc_ref = dl.get_parcelles()
    parc_opts = sorted([str(x) for x in parc_ref['ID_Parcelle'].unique() if pd.notnull(x) and str(x) != 'nan']) if not parc_ref.empty else []

    col_config_asso = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Assolement": None,
        "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=parc_opts),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
        "Strategie_Travail_Sol": st.column_config.SelectboxColumn("Stratégie", options=["Labour", "TCS", "Semis Direct", ""]),
        "Objectif_Rendement_Qx_Ha": st.column_config.NumberColumn("Obj Rdt"),
        "Prix_Vente_Objectif_€/T": st.column_config.NumberColumn("Prix Obj"),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis"),
        "Camp_Int": None
    }
    
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

# --- TAB 2: REF_PARCELLES ---
with tab_ref:
    st.markdown('<div class="table-header"><div class="table-title">🗺️ Référentiel Parcelles</div><div style="font-size: 0.8em; opacity: 0.8;">Données fixes de l\'exploitation 📍</div></div>', unsafe_allow_html=True)
    df_ref = dl.get_parcelles()
    
    # ULTIMATE STABILITY: No st.column_config objects, just labels
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
        # Using string labels in column_config avoids the Streamlit type checker conflict
        edited_ref = st.data_editor(df_ref_view, column_config=col_config_ref, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_ref")
        
        if st.button("💾 Sauvegarder Parcelles", use_container_width=True):
            with st.spinner("Mise à jour..."):
                if dl.overwrite_worksheet("REF_PARCELLES", edited_ref):
                    st.success("Référentiel mis à jour !"); st.rerun()
    except Exception as e:
        st.error(f"Erreur d'affichage du tableau des parcelles : {e}")
        st.info("Tentative d'affichage brut...")
        st.data_editor(df_ref_view)

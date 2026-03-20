import streamlit as st
import pandas as pd
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

    df_asso_all['Camp_Int'] = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').fillna(0).astype(int)
    df_curr_asso = df_asso_all[df_asso_all['Camp_Int'] == campagne_input].copy()
    df_others = df_asso_all[df_asso_all['Camp_Int'] != campagne_input].copy()
    
    st.subheader(f"📊 Résumé Campagne {campagne_input}")
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
    
    # --- Type casting for Assolement ---
    df_curr_asso['Campagne'] = pd.to_numeric(df_curr_asso['Campagne'], errors='coerce').fillna(campagne_input).astype(int)
    df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Objectif_Rendement_Qx_Ha'] = pd.to_numeric(df_curr_asso['Objectif_Rendement_Qx_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Prix_Vente_Objectif_€/T'] = pd.to_numeric(df_curr_asso['Prix_Vente_Objectif_€/T'], errors='coerce').fillna(0.0).astype(float)
    df_curr_asso['Date_Semis_Previsionnelle'] = pd.to_datetime(df_curr_asso['Date_Semis_Previsionnelle'], errors='coerce').dt.date

    col_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Assolement": None,
        "ID_Parcelle": st.column_config.SelectboxColumn("Parcelle", options=sorted(dl.get_parcelles()['ID_Parcelle'].unique().tolist()) if not dl.get_parcelles().empty else [], required=True),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f ha"),
        "Culture": st.column_config.TextColumn("Culture"),
        "Variété": st.column_config.TextColumn("Variété"),
        "Precedent_Cultural": st.column_config.TextColumn("Précédent"),
        "Strategie_Travail_Sol": st.column_config.SelectboxColumn("Stratégie", options=["Labour", "TCS", "Semis Direct"]),
        "Gestion_Résidus": st.column_config.SelectboxColumn("Résidus", options=["Enfouis", "Exportés"]),
        "Contrat_Commercial": st.column_config.TextColumn("Contrat"),
        "Objectif_Rendement_Qx_Ha": st.column_config.NumberColumn("Obj Rdt (Qx)"),
        "Prix_Vente_Objectif_€/T": st.column_config.NumberColumn("Prix Obj (€/T)"),
        "Couvert_précédent_Especes": st.column_config.TextColumn("Couvert"),
        "Développement_Couvert": st.column_config.SelectboxColumn("Dév. Couv.", options=["Nul", "Faible", "Moyen", "Fort"]),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis Prévu"),
        "Commentaire_Assolement": st.column_config.TextColumn("Commentaires"),
        "Camp_Int": None
    }

    edited_df = st.data_editor(df_curr_asso, column_config=col_config, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_asso")

    if st.button("💾 Sauvegarder l'Assolement", type="primary", use_container_width=True):
        with st.spinner("Enregistrement..."):
            edited_df['Campagne'] = campagne_input
            if 'Camp_Int' in edited_df.columns: edited_df = edited_df.drop(columns=['Camp_Int'])
            if 'Camp_Int' in df_others.columns: df_others = df_others.drop(columns=['Camp_Int'])
            df_final = pd.concat([df_others, edited_df], ignore_index=True)
            if dl.overwrite_worksheet("ASSOLEMENT", df_final):
                st.success("Assolement sauvegardé !")
                st.rerun()

with tab_ref:
    st.subheader("🗺️ Référentiel des Parcelles")
    df_ref = dl.get_parcelles()
    
    # --- Type casting for Ref Parcelles ---
    df_ref['Surface_Référence_Ha'] = pd.to_numeric(df_ref['Surface_Référence_Ha'], errors='coerce').fillna(0.0).astype(float)
    df_ref['ZNT Riverain'] = pd.to_numeric(df_ref['ZNT Riverain'], errors='coerce').fillna(0.0).astype(float)
    df_ref['ZNT Aqua'] = pd.to_numeric(df_ref['ZNT Aqua'], errors='coerce').fillna(0.0).astype(float)
    df_ref['Débit_Irrigation_m3/H'] = pd.to_numeric(df_ref['Débit_Irrigation_m3/H'], errors='coerce').fillna(0.0).astype(float)
    df_ref['RU_estimée'] = pd.to_numeric(df_ref['RU_estimée'], errors='coerce').fillna(0.0).astype(float)
    
    # Clean booleans
    for col in ['Analyse_sol', 'Drainage']:
        if col in df_ref.columns:
            df_ref[col] = df_ref[col].astype(str).str.upper().isin(['OUI', 'TRUE', 'VRAI', '1'])

    col_config_ref = {
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle", required=True),
        "Nom Terrain": st.column_config.TextColumn("Nom Terrain"),
        "îlot PAC": st.column_config.TextColumn("PAC Ilot"),
        "Commune": st.column_config.TextColumn("Commune"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf Réf (ha)", format="%.2f ha"),
        "Type_sol": st.column_config.TextColumn("Type Sol"),
        "Analyse_sol": st.column_config.CheckboxColumn("Analyse ?"),
        "Drainage": st.column_config.CheckboxColumn("Drainé ?"),
        "Irrigation (oui/non)": st.column_config.SelectboxColumn("Irrig.", options=["OUI", "NON"]),
        "Type irrigation": st.column_config.TextColumn("Matériel Irri"),
        "ZNT Riverain": st.column_config.NumberColumn("ZNT Riv (m)"),
        "ZNT Aqua": st.column_config.NumberColumn("ZNT Aqua (m)"),
        "Débit_Irrigation_m3/H": st.column_config.NumberColumn("Débit m3/h"),
        "RU_estimée": st.column_config.NumberColumn("RU (mm)"),
        "GPS": st.column_config.TextColumn("Coordonnées GPS")
    }
    
    edited_ref = st.data_editor(df_ref, column_config=col_config_ref, num_rows="dynamic", use_container_width=True, hide_index=True, key="editor_ref")
    
    if st.button("💾 Sauvegarder le Référentiel Parcelles", type="secondary", use_container_width=True):
        with st.spinner("Mise à jour..."):
            if dl.overwrite_worksheet("REF_PARCELLES", edited_ref):
                st.success("Référentiel parcelles mis à jour !")
                st.rerun()

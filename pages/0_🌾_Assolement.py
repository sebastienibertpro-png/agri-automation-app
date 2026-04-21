import streamlit as st
import pandas as pd
import numpy as np
import json
from shared import init_campaign_selector, inject_premium_css, render_premium_header, render_brand_page_header

st.set_page_config(page_title="Assolement & Parcelles", page_icon="🌾", layout="wide")
inject_premium_css()

render_brand_page_header("Gestion de l'Assolement & Parcelles", "Source de vérité unique pour votre exploitation ✨", icon="🌾")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# COLUMNS DEFINITION
# ══════════════════════════════════════════════════════════════════════════════
ASSO_COLUMNS = [
    'Campagne', 'ID_Assolement', 'ID_Parcelle', 'Nom Terrain', 'îlot PAC', 
    'Surface_Référence_Ha', 'Culture', 'Variété', 'Precedent_Cultural', 
    'Commune', 'Type_sol', 'Drainage', 'Irrigation (oui/non)', 'GPS',
    'Strategie_Travail_Sol', 'Gestion_Résidus',
    'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T',
    'Date_Semis_Previsionnelle', 'Commentaire_Assolement'
]

ASSO_HIDDEN = {'Commentaire_Assolement', 'ID_Assolement', 'Camp_Int', 'GPS'}

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_asso, tab_ilots, tab_import = st.tabs([
    "🌾 Plan d'Assolement", 
    "🗺️ Groupement par Îlots", 
    "📥 Importation (Geofolia/Télépac)"
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB ASSOLEMENT
# ══════════════════════════════════════════════════════════════════════════════
with tab_asso:
    campagne_input = int(selected_campaign)
    df_asso_all = dl.get_assolement()

    if df_asso_all.empty:
        df_curr_asso = pd.DataFrame(columns=ASSO_COLUMNS)
        df_others = pd.DataFrame(columns=ASSO_COLUMNS)
    else:
        df_asso_all['Camp_Int'] = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').fillna(0).astype(int)
        df_curr_asso = df_asso_all[df_asso_all['Camp_Int'] == campagne_input].copy()
        df_others = df_asso_all[df_asso_all['Camp_Int'] != campagne_input].copy()

    # Statistique rapide
    if not df_curr_asso.empty:
        df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
        total_surf = df_curr_asso['Surface_Référence_Ha'].sum()
        st.info(f"📊 **Campagne {campagne_input}** : {len(df_curr_asso)} parcelles pour un total de **{total_surf:.2f} ha**.")
    else:
        st.info(f"Aucune parcelle configurée pour la campagne {campagne_input}.")

    render_premium_header("🌾 Détail de l'Assolement", f"Modification directe pour {campagne_input}", color="green")

    # --- FORCED TYPE CLEANING FOR DATA EDITOR ---
    if not df_curr_asso.empty:
        df_curr_asso['Campagne'] = pd.to_numeric(df_curr_asso['Campagne'], errors='coerce').fillna(campagne_input).astype(int)
        df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0).astype(float)
        df_curr_asso['Objectif_Rendement_Qx_Ha'] = pd.to_numeric(df_curr_asso['Objectif_Rendement_Qx_Ha'], errors='coerce').fillna(0.0).astype(float)
        df_curr_asso['Prix_Vente_Objectif_€/T'] = pd.to_numeric(df_curr_asso['Prix_Vente_Objectif_€/T'], errors='coerce').fillna(0.0).astype(float)
        df_curr_asso['Date_Semis_Previsionnelle'] = pd.to_datetime(df_curr_asso['Date_Semis_Previsionnelle'], errors='coerce').dt.date
        
        # Ensure ID_Parcelle is string to avoid mixed types
        df_curr_asso['ID_Parcelle'] = df_curr_asso['ID_Parcelle'].astype(str).replace(['nan', 'None'], '')

    # Configuration de l'éditeur
    col_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle (Unique)", help="L'identifiant utilisé dans les autres pages."),
        "Nom Terrain": st.column_config.TextColumn("Nom"),
        "îlot PAC": st.column_config.TextColumn("Îlot"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
        "Culture": st.column_config.TextColumn("Culture"),
        "Type_sol": st.column_config.TextColumn("Sol"),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis"),
    }
    for h in ASSO_HIDDEN:
        col_config[h] = None

    edited_df = st.data_editor(
        df_curr_asso, 
        column_config=col_config, 
        num_rows="dynamic", 
        use_container_width=True, 
        hide_index=True, 
        key="main_asso_editor"
    )

    if st.button("💾 Sauvegarder les modifications", type="primary"):
        with st.spinner("Enregistrement sur Google Sheets..."):
            # Check for unique IDs
            ids = edited_df['ID_Parcelle'].dropna().astype(str).str.strip()
            if ids.duplicated().any():
                st.error(f"❌ Erreur : Des IDs de parcelles sont en double : {ids[ids.duplicated()].unique()}")
            else:
                edited_df['Campagne'] = campagne_input
                if 'Camp_Int' in edited_df.columns: edited_df = edited_df.drop(columns=['Camp_Int'])
                # Re-merge with other campaigns
                others_clean = df_others.drop(columns=['Camp_Int']) if 'Camp_Int' in df_others.columns else df_others
                final_df = pd.concat([others_clean, edited_df], ignore_index=True)
                if dl.overwrite_worksheet("ASSOLEMENT", final_df):
                    st.success("Données sauvegardées avec succès !")
                    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB ILOTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_ilots:
    if not df_curr_asso.empty:
        render_premium_header("🗺️ Groupement par Îlot PAC", f"Récapitulatif des surfaces par bloc", color="blue")
        
        # Grouping
        df_curr_asso['îlot PAC'] = df_curr_asso['îlot PAC'].replace(['', 'nan', None], 'Non défini')
        df_ilot = df_curr_asso.groupby('îlot PAC').agg({
            'ID_Parcelle': 'count',
            'Surface_Référence_Ha': 'sum',
            'Culture': lambda x: ", ".join(x.unique())
        }).reset_index()
        
        df_ilot.columns = ['Îlot PAC', 'Nombre de Parcelles', 'Surface Totale (ha)', 'Cultures présentes']
        
        st.dataframe(df_ilot, use_container_width=True, hide_index=True)
        
        # Detail expansion
        for ilot in df_ilot['Îlot PAC'].unique():
            with st.expander(f"Détail de l'îlot : {ilot}"):
                sub = df_curr_asso[df_curr_asso['îlot PAC'] == ilot]
                st.table(sub[['ID_Parcelle', 'Nom Terrain', 'Surface_Référence_Ha', 'Culture']])
    else:
        st.info("Aucune donnée disponible pour afficher les îlots.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB IMPORTATION
# ══════════════════════════════════════════════════════════════════════════════
with tab_import:
    render_premium_header("📥 Importateur de données externes", "Importez votre assolement depuis Geofolia ou Télépac", color="green")
    
    col_file, col_info = st.columns([2, 1])
    
    with col_file:
        source_type = st.radio("Source du fichier", ["Geofolia (JSON)", "Télépac (GeoJSON/Shapefile)"], horizontal=True)
        uploaded_file = st.file_uploader("Glissez votre fichier ici", type=["json", "geojson", "zip"])

    with col_info:
        st.markdown(f"""
        **Cible de l'import** : 
        Campagne **{selected_campaign}**
        
        *Note : L'importation pour 2024 est recommandée pour tester l'outil sans affecter votre campagne actuelle.*
        """)

    if uploaded_file:
        try:
            if source_type == "Geofolia (JSON)":
                json_data = json.load(uploaded_file)
                parsed_df = dl.parse_geofolia_json(json_data, int(selected_campaign))
                
                if not parsed_df.empty:
                    st.success(f"✅ {len(parsed_df)} parcelles détectées dans le fichier Geofolia pour {selected_campaign}.")
                    
                    st.markdown("### 📋 Aperçu des données à importer")
                    st.dataframe(parsed_df, use_container_width=True, hide_index=True)
                    
                    st.warning("⚠️ L'importation remplacera l'assolement existant pour cette campagne dans Google Sheets.")
                    
                    if st.button("🚀 Valider et Importer dans Agridia", type="primary"):
                        with st.spinner("Fusion des données..."):
                            df_all = dl.get_assolement()
                            # Filtrer pour enlever la campagne actuelle
                            if not df_all.empty:
                                df_all['Camp_Int'] = pd.to_numeric(df_all['Campagne'], errors='coerce').fillna(0).astype(int)
                                df_others = df_all[df_all['Camp_Int'] != int(selected_campaign)].copy()
                                df_others = df_others.drop(columns=['Camp_Int'])
                                final_to_save = pd.concat([df_others, parsed_df], ignore_index=True)
                            else:
                                final_to_save = parsed_df
                                
                            if dl.overwrite_worksheet("ASSOLEMENT", final_to_save):
                                st.balloons()
                                st.success("Importation réussie !")
                                st.rerun()
                else:
                    st.error(f"Aucune donnée trouvée pour la campagne {selected_campaign} dans ce fichier.")
            
            elif source_type == "Télépac (GeoJSON/Shapefile)":
                st.info("L'importateur Télépac est en cours de finalisation. Utilisez la page Cartographie pour l'instant.")
                
        except Exception as e:
            st.error(f"Erreur lors de l'analyse du fichier : {e}")

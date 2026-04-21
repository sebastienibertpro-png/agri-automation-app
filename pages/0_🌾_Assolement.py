# VER_2_7_FINAL - Selective Import & Master Checkbox
import streamlit as st
import pandas as pd
import numpy as np
import json
from shared import init_campaign_selector, inject_premium_css, render_premium_header, render_brand_page_header

st.set_page_config(page_title="Assolement & Parcelles", page_icon="🌾", layout="wide")
inject_premium_css()

render_brand_page_header("Mes parcelles", "Gérer votre assolement et vos îlots", icon="🌾")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")
    st.stop()

ASSO_COLUMNS = [
    'Campagne', 'ID_Assolement', 'ID_Parcelle', 'îlot PAC', 'Commune',
    'Surface_Référence_Ha', 'Culture', 'Code_Culture_PAC', 'Variété', 'Precedent_Cultural', 
    'Type_sol', 'Drainage', 'Irrigation (oui/non)', 'ZNT_Riverain', 'ZNT_Aqua',
    'Strategie_Travail_Sol', 'Gestion_Résidus',
    'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T',
    'Date_Semis_Previsionnelle', 'Commentaire_Assolement', 'Nom Terrain', 'GPS'
]

ASSO_HIDDEN = {'Commentaire_Assolement', 'ID_Assolement', 'Camp_Int', 'GPS', 'Nom Terrain', 'image'}

def ensure_columns(df, columns):
    """Ensures all columns exist in the DataFrame."""
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_asso, tab_ilots, tab_import = st.tabs([
    "🌾 Assolement", 
    "🗺️ Ilots", 
    "📥 Importer (Logiciel/Télépac)"
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

    # Ensure all columns exist for the UI
    df_curr_asso = ensure_columns(df_curr_asso, ASSO_COLUMNS)

    # Statistique rapide
    if not df_curr_asso.empty:
        df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
        total_surf = df_curr_asso['Surface_Référence_Ha'].sum()
        st.info(f"📊 **Campagne {campagne_input}** : {len(df_curr_asso)} parcelles pour un total de **{total_surf:.2f} ha**.")
    else:
        st.info(f"Aucune parcelle configurée pour la campagne {campagne_input}.")

    render_premium_header("🌾 Détail de l'Assolement", f"Modification directe pour {campagne_input}", color="green")

    # --- ULTRA-ROBUST CLEANING FOR DATA EDITOR ---
    # Ensure columns exist first
    df_curr_asso = ensure_columns(df_curr_asso, ASSO_COLUMNS)
    
    # Global Cleanup: Radical removal of ANY "None" string or variant
    for col in df_curr_asso.columns:
        # Avoid cleaning date columns here to prevent breaking them
        if col != 'Date_Semis_Previsionnelle':
            df_curr_asso[col] = df_curr_asso[col].astype(str).str.strip().replace(
                ['nan', 'None', '<NA>', 'NaT', 'None', 'nan', 'nan', 'null', 'None ', ' None', 'NaN', 'None', ''], ''
            )
    
    # Re-apply Numeric types for the editor to work correctly
    num_cols = ['Surface_Référence_Ha', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T', 'ZNT_Riverain', 'ZNT_Aqua']
    for col in num_cols:
        if col in df_curr_asso.columns:
            df_curr_asso[col] = pd.to_numeric(df_curr_asso[col], errors='coerce').fillna(0.0).astype(float)
    
    df_curr_asso['Campagne'] = pd.to_numeric(df_curr_asso['Campagne'], errors='coerce').fillna(campagne_input).astype(int)
    
    # Date column: must be handled carefully
    df_curr_asso['Date_Semis_Previsionnelle'] = pd.to_datetime(df_curr_asso['Date_Semis_Previsionnelle'], errors='coerce')
    df_curr_asso['Date_Semis_Previsionnelle'] = df_curr_asso['Date_Semis_Previsionnelle'].apply(lambda x: x.date() if pd.notnull(x) else None)

    # STRICT FILTERING: Only keep columns we want to show/manage
    # This removes parasites like "Contrat_Commercial" or "image"
    display_cols = [c for c in ASSO_COLUMNS if c not in ASSO_HIDDEN]
    # We keep hidden columns in the DF for saving, but we restrict the editor's view if needed
    # Or simpler: we only pass the columns we defined in ASSO_COLUMNS
    df_editor = df_curr_asso[ASSO_COLUMNS].copy()

    # Configuration de l'éditeur
    col_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle (Unique)", help="Identifiant interne pour les autres pages."),
        "îlot PAC": st.column_config.TextColumn("Îlot"),
        "Commune": st.column_config.TextColumn("Commune"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
        "Culture": st.column_config.TextColumn("Culture"),
        "Code_Culture_PAC": st.column_config.TextColumn("Code PAC"),
        "ZNT_Riverain": st.column_config.NumberColumn("ZNT Riverain"),
        "ZNT_Aqua": st.column_config.NumberColumn("ZNT Aqua"),
        "Type_sol": st.column_config.TextColumn("Sol"),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Semis"),
    }
    for h in ASSO_HIDDEN:
        col_config[h] = None

    # Check for unique IDs for warnings
    ids_check = df_curr_asso['ID_Parcelle'].dropna().astype(str).str.strip()
    duplicates = ids_check[ids_check.duplicated()].unique()
    if len(duplicates) > 0:
        st.warning(f"⚠️ **Doublons détectés** : Les parcelles suivantes ont le même ID : {', '.join(duplicates)}. "
                   "Veuillez les renommer (ex: Nom_1, Nom_2) pour que chaque ligne soit unique.")

    edited_df = st.data_editor(
        df_editor, 
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
                    
                    st.markdown("### 📋 Sélection des parcelles à importer")
                    
                    # Master Checkbox for Select All
                    select_all = st.checkbox("Tout cocher / décocher", value=True, help="Coche ou décoche toutes les parcelles de la liste.")
                    
                    # Prepare DF with selection column
                    parsed_df.insert(0, 'Sél.', select_all)
                    
                    # Data Editor for selection
                    import_selection = st.data_editor(
                        parsed_df,
                        column_config={
                            "Sél.": st.column_config.CheckboxColumn("Sél.", default=True),
                            "ID_Parcelle": st.column_config.TextColumn("Parcelle", disabled=True),
                            "Culture": st.column_config.TextColumn("Culture", disabled=True),
                            "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f", disabled=True),
                        },
                        use_container_width=True,
                        hide_index=True,
                        key="import_filter_editor"
                    )
                    
                    # Count selected rows
                    selected_rows = import_selection[import_selection['Sél.'] == True].copy()
                    st.info(f"📍 {len(selected_rows)} parcelles sélectionnées sur {len(parsed_df)} pour l'importation.")
                    
                    st.warning("⚠️ L'importation remplacera l'assolement existant pour cette campagne dans Google Sheets.")
                    
                    if st.button("🚀 Valider et Importer dans Agridia", type="primary", disabled=len(selected_rows) == 0):
                        with st.spinner("Fusion des données..."):
                            df_all = dl.get_assolement()
                            
                            # Clean selected rows (remove selection column)
                            rows_to_import = selected_rows.drop(columns=['Sél.'])
                            
                            # Re-merge with other campaigns
                            if not df_all.empty:
                                df_all['Camp_Int'] = pd.to_numeric(df_all['Campagne'], errors='coerce').fillna(0).astype(int)
                                df_others = df_all[df_all['Camp_Int'] != int(selected_campaign)].copy()
                                df_others = df_others.drop(columns=['Camp_Int'])
                                final_to_save = pd.concat([df_others, rows_to_import], ignore_index=True)
                            else:
                                final_to_save = rows_to_import
                                
                            if dl.overwrite_worksheet("ASSOLEMENT", final_to_save):
                                st.balloons()
                                st.success(f"Importation de {len(rows_to_import)} parcelles réussie !")
                                st.rerun()
                else:
                    st.error(f"Aucune donnée trouvée pour la campagne {selected_campaign} dans ce fichier.")
            
            elif source_type == "Télépac (GeoJSON/Shapefile)":
                st.info("L'importateur Télépac est en cours de finalisation. Utilisez la page Cartographie pour l'instant.")
                
        except Exception as e:
            st.error(f"Erreur lors de l'analyse du fichier : {e}")

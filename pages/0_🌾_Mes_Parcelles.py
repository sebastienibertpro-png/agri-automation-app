# VER_3_0_FINAL - Fusion Assolement & Cartographie
import streamlit as st
import pandas as pd
import numpy as np
import json
import zipfile
import tempfile
import os
import folium
from streamlit_folium import st_folium
import geopandas as gpd
from branca.element import Element
from folium.plugins import MeasureControl, Draw, Fullscreen
from datetime import datetime
from shared import init_campaign_selector, inject_premium_css, render_premium_header, render_brand_page_header

st.set_page_config(page_title="Mes Parcelles", page_icon="🗺️", layout="wide")
inject_premium_css()

render_brand_page_header("Mes Parcelles", "Gestion centralisée : Assolement, Cartographie et Imports", icon="🗺️")

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
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab_asso, tab_carto, tab_ilots, tab_import = st.tabs([
    "🌾 Assolement", 
    "🗺️ Cartographie", 
    "📍 Îlots", 
    "📥 Importer"
])

# Variables communes
campagne_input = int(selected_campaign)
df_asso_all = dl.get_assolement()

if df_asso_all.empty:
    df_curr_asso = pd.DataFrame(columns=ASSO_COLUMNS)
    df_others = pd.DataFrame(columns=ASSO_COLUMNS)
else:
    df_asso_all['Camp_Int'] = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').fillna(0).astype(int)
    df_curr_asso = df_asso_all[df_asso_all['Camp_Int'] == campagne_input].copy()
    df_others = df_asso_all[df_asso_all['Camp_Int'] != campagne_input].copy()

df_curr_asso = ensure_columns(df_curr_asso, ASSO_COLUMNS)

# ══════════════════════════════════════════════════════════════════════════════
# TAB ASSOLEMENT
# ══════════════════════════════════════════════════════════════════════════════
with tab_asso:
    pac_ref = dl.get_pac_codes() if hasattr(dl, 'get_pac_codes') else {}
    pac_options = sorted([f"{k} - {v}" for k, v in pac_ref.items()])

    if not df_curr_asso.empty:
        df_curr_asso['Surface_Référence_Ha'] = pd.to_numeric(df_curr_asso['Surface_Référence_Ha'], errors='coerce').fillna(0.0)
        total_surf = df_curr_asso['Surface_Référence_Ha'].sum()
        st.info(f"📊 **Campagne {campagne_input}** : {len(df_curr_asso)} parcelles pour un total de **{total_surf:.2f} ha**.")
    else:
        st.info(f"Aucune parcelle configurée pour la campagne {campagne_input}.")

    render_premium_header("🌾 Détail de l'Assolement", f"Modification directe pour {campagne_input}", color="green")

    with st.expander("🔍 Options d'affichage (Filtres & Tri)", expanded=True):
        c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
        with c1:
            filter_culture = st.multiselect("Filtrer par Culture", options=sorted(df_curr_asso['Culture'].unique()))
        with c2:
            ilots_available = sorted([i for i in df_curr_asso['îlot PAC'].dropna().unique() if str(i).strip() != ''])
            filter_ilot = st.multiselect("Filtrer par Îlot", options=ilots_available)
        with c3:
            visible_cols = [c for c in ASSO_COLUMNS if c not in ASSO_HIDDEN]
            sort_col = st.selectbox("Trier par colonne", options=visible_cols, index=visible_cols.index('îlot PAC') if 'îlot PAC' in visible_cols else 0)
        with c4:
            sort_sense = st.radio("Sens", options=["⬆️", "⬇️"], horizontal=True)

    df_editor = df_curr_asso.sort_values(by=['îlot PAC', 'ID_Parcelle']).copy()

    for col in df_editor.columns:
        if col != 'Date_Semis_Previsionnelle':
            df_editor[col] = df_editor[col].fillna('')
            df_editor[col] = df_editor[col].astype(str).str.strip().replace(
                ['nan', 'None', '<NA>', 'NaT', 'null', 'NaN', 'None ', ' None', 'nan'], ''
            )
    
    if 'îlot PAC' in df_editor.columns:
        df_editor['îlot PAC'] = pd.to_numeric(df_editor['îlot PAC'], errors='coerce')

    for col in ['ZNT_Riverain', 'ZNT_Aqua']:
        if col in df_editor.columns:
            df_editor[col] = df_editor[col].apply(lambda x: "oui" if str(x).strip() in ['1', '1.0', 'True', 'oui'] else "non")

    def fix_legacy_pac(row):
        code = str(row.get('Code_Culture_PAC', '')).strip().upper()
        if code == 'F62': return 'FTE'
        if code == 'ZCD': return 'CZH'
        return code
    
    if 'Code_Culture_PAC' in df_editor.columns:
        df_editor['Code_Culture_PAC'] = df_editor.apply(fix_legacy_pac, axis=1)

    def format_pac_label(code):
        code = str(code).strip().upper()
        if code in pac_ref: return f"{code} - {pac_ref[code]}"
        return code
    
    if 'Code_Culture_PAC' in df_editor.columns:
        df_editor['Code_Culture_PAC'] = df_editor['Code_Culture_PAC'].apply(format_pac_label)

    tech_num_cols = ['Surface_Référence_Ha', 'Objectif_Rendement_Qx_Ha', 'Prix_Vente_Objectif_€/T']
    for col in tech_num_cols:
        if col in df_editor.columns:
            df_editor[col] = pd.to_numeric(df_editor[col], errors='coerce').fillna(0.0).astype(float)
    
    df_editor['Campagne'] = pd.to_numeric(df_editor['Campagne'], errors='coerce').fillna(campagne_input).astype(int)
    
    df_editor['Date_Semis_Previsionnelle'] = pd.to_datetime(df_editor['Date_Semis_Previsionnelle'], errors='coerce')
    df_editor['Date_Semis_Previsionnelle'] = df_editor['Date_Semis_Previsionnelle'].apply(lambda x: x.date() if pd.notnull(x) else None)

    if filter_culture: df_editor = df_editor[df_editor['Culture'].isin(filter_culture)]
    if filter_ilot: df_editor = df_editor[df_editor['îlot PAC'].isin(filter_ilot)]
    
    ascending = True if sort_sense == "⬆️" else False
    if sort_col in df_editor.columns:
        df_editor = df_editor.sort_values(by=sort_col, ascending=ascending)

    col_config = {
        "Campagne": st.column_config.NumberColumn("Camp.", disabled=True, format="%d"),
        "ID_Parcelle": st.column_config.TextColumn("ID Parcelle (Unique)"),
        "îlot PAC": st.column_config.NumberColumn("Îlot", format="%d"),
        "Commune": st.column_config.TextColumn("Commune"),
        "Surface_Référence_Ha": st.column_config.NumberColumn("Surf (ha)", format="%.2f"),
        "Culture": st.column_config.TextColumn("Culture"),
        "Code_Culture_PAC": st.column_config.SelectboxColumn("Code PAC", options=pac_options),
        "ZNT_Riverain": st.column_config.SelectboxColumn("ZNT Riverain", options=["oui", "non"]),
        "ZNT_Aqua": st.column_config.SelectboxColumn("ZNT Aqua", options=["oui", "non"]),
        "Type_sol": st.column_config.SelectboxColumn("Sol", options=['Argileux', 'Limoneux', 'Sableux', 'Argilo-Limoneux', 'Limono-Argileux', 'Sablo-Limoneux', 'Calcaire', 'Humifère', 'Alluvions']),
        "Strategie_Travail_Sol": st.column_config.SelectboxColumn("Travail du sol", options=['Labour', 'TCS (Simplifié)', 'Semis Direct', 'Strip-till']),
        "Gestion_Résidus": st.column_config.SelectboxColumn("Gestion des résidus", options=['Enfouis', 'Exportés (récoltés)', 'Broyés / Laissés en surface']),
        "Date_Semis_Previsionnelle": st.column_config.DateColumn("Date de semis"),
    }
    for h in ASSO_HIDDEN: col_config[h] = None

    ids_check = df_curr_asso['ID_Parcelle'].dropna().astype(str).str.strip()
    duplicates = ids_check[ids_check.duplicated()].unique()
    if len(duplicates) > 0:
        st.warning(f"⚠️ Doublons détectés : {', '.join(duplicates)}. Veuillez les renommer.")

    edited_df = st.data_editor(df_editor, column_config=col_config, num_rows="dynamic", use_container_width=True, hide_index=True, key="main_asso_editor")

    if st.button("💾 Sauvegarder les modifications", type="primary"):
        with st.spinner("Enregistrement sur Google Sheets..."):
            ids = edited_df['ID_Parcelle'].dropna().astype(str).str.strip()
            if ids.duplicated().any():
                st.error(f"❌ Erreur : Des IDs de parcelles sont en double : {ids[ids.duplicated()].unique()}")
            else:
                edited_df['Campagne'] = campagne_input
                for col in ['ZNT_Riverain', 'ZNT_Aqua']:
                    if col in edited_df.columns:
                        edited_df[col] = edited_df[col].apply(lambda x: 1 if x == "oui" else 0)

                if 'Code_Culture_PAC' in edited_df.columns:
                    edited_df['Code_Culture_PAC'] = edited_df['Code_Culture_PAC'].apply(
                        lambda x: str(x).split(' - ')[0][:3] if ' - ' in str(x) else str(x)[:3]
                    )

                if 'Camp_Int' in edited_df.columns: edited_df = edited_df.drop(columns=['Camp_Int'])
                others_clean = df_others.drop(columns=['Camp_Int']) if 'Camp_Int' in df_others.columns else df_others
                final_df = pd.concat([others_clean, edited_df], ignore_index=True)
                if dl.overwrite_worksheet("ASSOLEMENT", final_df):
                    st.success("Données sauvegardées avec succès !")
                    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB CARTOGRAPHIE
# ══════════════════════════════════════════════════════════════════════════════
with tab_carto:
    st.markdown("### 🗺️ Carte Interactive de l'Exploitation")
    
    telepac_gdf = None
    center_lat_default, center_lon_default = 46.603354, 1.888334
    default_zoom = 6
    
    with st.spinner("Chargement de la cartographie..."):
        geojson_str = dl.load_telepac_from_cloud(selected_campaign)
        if geojson_str and len(geojson_str) > 50:
            try:
                telepac_gdf = gpd.GeoDataFrame.from_features(json.loads(geojson_str)["features"])
                telepac_gdf.set_crs(epsg=4326, inplace=True)
                
                # S'assurer que la colonne CODE_CULTU est liée à l'assolement actuel pour avoir les bonnes couleurs
                # On fait une jointure légère avec df_curr_asso
                if not telepac_gdf.empty and not df_curr_asso.empty:
                     # On suppose que telepac_gdf a une colonne 'NUM_PARCEL' qui match 'ID_Parcelle'
                     if 'NUM_PARCEL' in telepac_gdf.columns and 'ID_Parcelle' in df_curr_asso.columns:
                          # Create mapping
                          culture_map = dict(zip(df_curr_asso['ID_Parcelle'].astype(str).str.strip(), df_curr_asso['Code_Culture_PAC'].astype(str).str.strip()))
                          def update_culture(row):
                               pid = str(row.get('NUM_PARCEL', '')).strip()
                               if pid in culture_map and culture_map[pid]:
                                    return culture_map[pid]
                               return row.get('CULTURE', row.get('CODE_CULTU', 'DEFAULT'))
                          telepac_gdf['CULTURE_UPDATED'] = telepac_gdf.apply(update_culture, axis=1)

                bounds = telepac_gdf.total_bounds
                # Prevent invalid bounds if all geometries are empty
                if not pd.isna(bounds[0]) and not np.isinf(bounds[0]):
                    center_lon_default = (bounds[0] + bounds[2]) / 2
                    center_lat_default = (bounds[1] + bounds[3]) / 2
                    default_zoom = 13
            except Exception as e:
                st.error(f"Erreur lors de la lecture des données cartographiques : {e}")
                
    m = folium.Map(location=[center_lat_default, center_lon_default], zoom_start=default_zoom, control_scale=True)

    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Satellite Image (Esri)',
        overlay=False,
        control=True
    ).add_to(m)

    telepac_fg = folium.FeatureGroup(name="Contours Parcelles")
    if telepac_gdf is not None and not telepac_gdf.empty:
        # Nettoyage des géométries invalides ou vides avant affichage
        telepac_gdf = telepac_gdf[telepac_gdf.is_valid & ~telepac_gdf.is_empty].copy()
            
        CROP_COLORS = {
            'BTH': '#f1c40f', 'BLE': '#f1c40f', 'BLÉ': '#f1c40f',
            'ORP': '#e67e22', 'ORH': '#d35400', 'ORGE': '#e67e22',
            'CZH': '#9b59b6', 'COLZA': '#9b59b6',
            'MIS': '#f39c12', 'MID': '#f39c12', 'MAI': '#f39c12', 'MAIS': '#f39c12', 'MAÏS': '#f39c12',
            'TRN': '#2ecc71', 'TOURNESOL': '#2ecc71',
            'PTR': '#27ae60', 'PPH': '#2ed573', 'PRL': '#2ed573',
            'J6S': '#bdc3c7', 'J5M': '#bdc3c7', 'JACHERE': '#bdc3c7', 'JACHÈRE': '#bdc3c7',
            'LUZ': '#8e44ad', 'LUZERNE': '#8e44ad',
            'POI': '#1abc9c', 'POIS': '#1abc9c', 'FTE': '#1abc9c',
            'DEFAULT': '#3498db'
        }
        
        def get_crop_style(props):
            crop_code = props.get('CULTURE_UPDATED', props.get('CULTURE', props.get('CODE_CULTU', props.get('TYPE', 'DEFAULT'))))
            if isinstance(crop_code, str):
                crop_code_upper = crop_code.upper()
                fill_color = CROP_COLORS.get(crop_code_upper, CROP_COLORS['DEFAULT'])
                if fill_color == CROP_COLORS['DEFAULT']:
                    for key, color in CROP_COLORS.items():
                        if key in crop_code_upper:
                            fill_color = color
                            break
            else:
                fill_color = CROP_COLORS['DEFAULT']

            return {
                'fillColor': fill_color,
                'color': '#2c3e50',
                'weight': 1.5,
                'fillOpacity': 0.6
            }
        
        for idx, row in telepac_gdf.iterrows():
            geom = row.geometry
            props = row.drop('geometry').to_dict() if 'geometry' in row else {}
            style = get_crop_style(props)
            
            tooltip_html = f"<b>ID:</b> {props.get('NUM_PARCEL', '')}<br>"
            tooltip_html += f"<b>CULTURE:</b> {props.get('CULTURE_UPDATED', props.get('CULTURE', props.get('CODE_CULTU', '')))}<br>"
            surf = props.get('SURFACE', '')
            if surf: tooltip_html += f"<b>SURFACE:</b> {surf} ha"

            def add_folium_polygon(poly):
                locations = [[(lat, lon) for lon, lat in poly.exterior.coords]]
                for interior in poly.interiors:
                    locations.append([(lat, lon) for lon, lat in interior.coords])
                
                folium.Polygon(
                    locations=locations,
                    color=style['color'],
                    weight=style['weight'],
                    fill_color=style['fillColor'],
                    fill_opacity=style['fillOpacity'],
                    tooltip=tooltip_html,
                    popup=str(props.get('NUM_PARCEL', ''))
                ).add_to(telepac_fg)

            if geom.geom_type == 'Polygon':
                add_folium_polygon(geom)
            elif geom.geom_type == 'MultiPolygon':
                for poly in geom.geoms:
                    add_folium_polygon(poly)
        
    telepac_fg.add_to(m)
    folium.LayerControl().add_to(m)

    Fullscreen(position='topright').add_to(m)
    MeasureControl(position='topleft', primary_area_unit='hectares').add_to(m)
    Draw(
        export=True, 
        position="topleft", 
        draw_options={'circle': False, 'rectangle': False, 'polyline': False, 'marker': False, 'circlemarker': False, 'polygon': True}
    ).add_to(m)

    js_translation = """
    <script>
    function translateDrawLocal() {
        if (typeof L !== 'undefined' && L.drawLocal) {
            L.drawLocal.draw.toolbar.actions.title = "Annuler le dessin";
            L.drawLocal.draw.toolbar.actions.text = "Annuler";
            L.drawLocal.draw.toolbar.finish.title = "Terminer le dessin";
            L.drawLocal.draw.toolbar.finish.text = "Terminer";
            L.drawLocal.draw.toolbar.undo.title = "Supprimer le dernier point";
            L.drawLocal.draw.toolbar.undo.text = "Effacer point";
            L.drawLocal.draw.toolbar.buttons.polygon = "Tracer un polygone / surface";
            
            L.drawLocal.draw.handlers.polygon.tooltip.start = "Cliquez pour commencer un polygone.";
            L.drawLocal.draw.handlers.polygon.tooltip.cont = "Cliquez pour continuer le polygone.";
            L.drawLocal.draw.handlers.polygon.tooltip.end = "Cliquez sur le 1er point pour fermer la forme.";
            
            L.drawLocal.edit.toolbar.actions.save.title = "Sauvegarder les modifications";
            L.drawLocal.edit.toolbar.actions.save.text = "Sauvegarder";
            L.drawLocal.edit.toolbar.actions.cancel.title = "Annuler l'édition";
            L.drawLocal.edit.toolbar.actions.cancel.text = "Annuler";
            L.drawLocal.edit.toolbar.actions.clearAll.title = "Tout effacer";
            L.drawLocal.edit.toolbar.actions.clearAll.text = "Tout effacer";
            L.drawLocal.edit.toolbar.buttons.edit = "Éditer un tracé";
            L.drawLocal.edit.toolbar.buttons.editDisabled = "Aucun tracé à éditer";
            L.drawLocal.edit.toolbar.buttons.remove = "Effacer des tracés";
            L.drawLocal.edit.toolbar.buttons.removeDisabled = "Aucun tracé à effacer";
            
            L.drawLocal.edit.handlers.edit.tooltip.text = "Bougez les poignées pour modifier la forme.";
            L.drawLocal.edit.handlers.remove.tooltip.text = "Cliquez sur une forme pour l'effacer.";
        }
    }

    let checkDraw = setInterval(() => {
        if (typeof L !== 'undefined' && L.drawLocal) {
            translateDrawLocal();
            clearInterval(checkDraw);
        }
    }, 100);
    setTimeout(() => clearInterval(checkDraw), 5000);

    </script>
    """
    m.get_root().html.add_child(Element(js_translation))

    st_data = None
    try:
        st_data = st_folium(
            m, 
            width="100%", 
            height=600, 
            returned_objects=["all_drawings"]
        )
    except Exception as e:
        st.error(f"❌ Erreur lors du rendu de la carte Folium : {str(e)}")
        import traceback
        st.code(traceback.format_exc())
    
    # Check if a drawing was made
    if st_data and st_data.get("all_drawings") and len(st_data["all_drawings"]) > 0:
        if st.button("💾 Enregistrer les nouvelles géométries dessinées", type="primary"):
            # The edited geometries override the existing ones if we process them
            # Since Folium Draw returns the newly drawn/edited shapes in "all_drawings", we can save them back
            with st.spinner("Enregistrement de la nouvelle cartographie..."):
                try:
                    # Conversion des dessins en GeoDataFrame
                    new_geoms = gpd.GeoDataFrame.from_features(st_data["all_drawings"])
                    if telepac_gdf is not None:
                        # Combine with existing (excluding the ones that might have been modified/deleted if handled perfectly)
                        # For now, append newly drawn polygons
                        combined_gdf = pd.concat([telepac_gdf, new_geoms], ignore_index=True)
                        
                        # Fix multiple identical NUM_PARCEL if the user edited an existing one
                        # The newly drawn ones from 'all_drawings' will NOT have NUM_PARCEL from the popup automatically,
                        # because st_folium Draw returns raw geojson without the popup binding.
                        # However, for now, we just append them. A better approach is to match by intersection or let user name them.
                    else:
                        combined_gdf = new_geoms
                        
                    # Save to cloud
                    geojson_to_save = combined_gdf.to_json()
                    if dl.save_telepac_to_cloud(selected_campaign, geojson_to_save):
                        st.success("✅ Cartographie mise à jour et sauvegardée sur le Cloud !")
                        st.rerun()
                except Exception as e:
                    st.error(f"Erreur d'enregistrement : {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB ILOTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_ilots:
    if not df_curr_asso.empty:
        render_premium_header("🗺️ Groupement par Îlot PAC", f"Récapitulatif des surfaces par bloc", color="blue")
        df_curr_asso['îlot PAC'] = df_curr_asso['îlot PAC'].replace(['', 'nan', None], 'Non défini')
        df_ilot = df_curr_asso.groupby('îlot PAC').agg({
            'ID_Parcelle': 'count',
            'Surface_Référence_Ha': 'sum',
            'Culture': lambda x: ", ".join(x.unique())
        }).reset_index()
        df_ilot.columns = ['Îlot PAC', 'Nombre de Parcelles', 'Surface Totale (ha)', 'Cultures présentes']
        st.dataframe(df_ilot, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune donnée disponible pour afficher les îlots.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB IMPORTATION
# ══════════════════════════════════════════════════════════════════════════════
with tab_import:
    render_premium_header("Importer depuis un logiciel ou Télépac", "Importez votre assolement et cartographie", color="green")
    
    col_file, col_info = st.columns([2, 1])
    with col_file:
        source_type = st.radio("Source du fichier", ["Geofolia (ZIP complet ou JSON)", "Télépac (Archive ZIP Shapefile)"], horizontal=True)
        uploaded_file = st.file_uploader("Glissez votre fichier ici", type=["json", "zip"])

    with col_info:
        st.markdown(f"**Cible de l'import** : Campagne **{selected_campaign}**")

    if uploaded_file:
        try:
            parsed_df = pd.DataFrame()
            geojson_str = None
            
            if "Geofolia" in source_type:
                if uploaded_file.name.endswith(".zip"):
                    with tempfile.TemporaryDirectory() as tmpdirname:
                        with zipfile.ZipFile(uploaded_file, 'r') as zip_ref:
                            zip_ref.extractall(tmpdirname)
                            # Find Field.Json (case insensitive)
                            field_json_path = None
                            for f in os.listdir(tmpdirname):
                                if f.lower() == "field.json":
                                    field_json_path = os.path.join(tmpdirname, f)
                                    break
                                    
                            if field_json_path:
                                with open(field_json_path, 'r', encoding='utf-8-sig') as f:
                                    json_data = json.load(f)
                                parsed_df, geojson_str = dl.parse_geofolia_json(json_data, int(selected_campaign))
                            else:
                                st.error("Fichier Field.Json introuvable dans l'archive ZIP.")
                elif uploaded_file.name.endswith(".json"):
                    json_data = json.load(uploaded_file)
                    parsed_df, geojson_str = dl.parse_geofolia_json(json_data, int(selected_campaign))
                    
            elif "Télépac" in source_type:
                 st.info("L'import Télépac sera implémenté très prochainement. Veuillez utiliser l'export Geofolia pour le moment.")
                 
            if not parsed_df.empty:
                st.success(f"✅ {len(parsed_df)} parcelles détectées pour la campagne {selected_campaign}.")
                if geojson_str:
                    st.success("🗺️ Géométries géographiques (contours des parcelles) détectées avec succès !")
                
                st.markdown("### 📋 Sélection des parcelles à importer")
                select_all = st.checkbox("Tout cocher / décocher", value=True)
                parsed_df.insert(0, 'Sél.', select_all)
                
                import_selection = st.data_editor(
                    parsed_df,
                    column_config={"Sél.": st.column_config.CheckboxColumn("Sél.", default=True)},
                    use_container_width=True, hide_index=True, key="import_filter_editor"
                )
                
                selected_rows = import_selection[import_selection['Sél.'] == True].copy()
                st.warning("⚠️ L'importation remplacera l'assolement existant pour cette campagne dans Google Sheets.")
                
                if st.button("🚀 Valider et Importer dans Agridia", type="primary", disabled=len(selected_rows) == 0):
                    with st.spinner("Enregistrement des données Assolement et Cartographie..."):
                        rows_to_import = selected_rows.drop(columns=['Sél.'])
                        
                        # 1. Save Assolement
                        df_all = dl.get_assolement()
                        if not df_all.empty:
                            df_all['Camp_Int'] = pd.to_numeric(df_all['Campagne'], errors='coerce').fillna(0).astype(int)
                            df_others = df_all[df_all['Camp_Int'] != int(selected_campaign)].copy()
                            df_others = df_others.drop(columns=['Camp_Int'])
                            final_to_save = pd.concat([df_others, rows_to_import], ignore_index=True)
                        else:
                            final_to_save = rows_to_import
                            
                        success_asso = dl.overwrite_worksheet("ASSOLEMENT", final_to_save)
                        
                        # 2. Save Cartography
                        success_carto = True
                        if geojson_str:
                            # Verify if the row selection should also filter the geojson features
                            # To be perfect, we should filter geojson_str based on selected_rows['ID_Parcelle']
                            try:
                                import json
                                geojson_data = json.loads(geojson_str)
                                allowed_ids = set(rows_to_import['ID_Parcelle'].astype(str).str.strip())
                                filtered_features = [f for f in geojson_data.get('features', []) 
                                                     if str(f.get('properties', {}).get('NUM_PARCEL', '')).strip() in allowed_ids]
                                geojson_data['features'] = filtered_features
                                final_geojson_str = json.dumps(geojson_data)
                                success_carto = dl.save_telepac_to_cloud(selected_campaign, final_geojson_str)
                            except:
                                # Fallback to save everything if filtering fails
                                success_carto = dl.save_telepac_to_cloud(selected_campaign, geojson_str)
                            
                        if success_asso and success_carto:
                            st.balloons()
                            st.success(f"Importation globale réussie !")
                            st.rerun()
                            
        except Exception as e:
            st.error(f"Erreur lors de l'analyse du fichier : {e}")

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import tempfile
import os
import zipfile
from datetime import datetime
import shutil
from shared import init_campaign_selector

st.set_page_config(page_title="Cartographie", page_icon="🗺️", layout="wide")

st.title("🗺️ Cartographie & Relevés")

st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
</style>
""", unsafe_allow_html=True)

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# Session State for tracking clicked meter
if 'clicked_meter_id' not in st.session_state:
    st.session_state.clicked_meter_id = None

# 1. Initialization and Data Loading
try:
    df_gps = active_loader.get_compteurs_gps()
except Exception as e:
    st.error(f"Erreur de chargement des données GPS compteurs : {e}")
    df_gps = pd.DataFrame()

# Default center (France) if no data
center_lat_default, center_lon_default = 46.603354, 1.888334 
default_zoom = 5

if not df_gps.empty:
    center_lat_default = df_gps['Latitude'].mean()
    center_lon_default = df_gps['Longitude'].mean()
    default_zoom = 13

# Optional: Upload Telepac GeoJSON/Shapefile
st.sidebar.header("📁 Couches Cartographiques")

# Repertoire de sauvegarde pour les cartes Télépac de la campagne
MAP_SAVE_DIR = f"data/telepac/{selected_campaign}"
if not os.path.exists(MAP_SAVE_DIR):
    os.makedirs(MAP_SAVE_DIR, exist_ok=True)

# Chercher un fichier sauvegardé
saved_files = [f for f in os.listdir(MAP_SAVE_DIR) if f.endswith('.zip') or f.endswith('.geojson')]
saved_file_path = os.path.join(MAP_SAVE_DIR, saved_files[0]) if saved_files else None

# Upload component
uploaded_file = st.sidebar.file_uploader("Importer fichier Télépac (GeoJSON ou ZIP Shapefile)", type=['geojson', 'zip'])
telepac_gdf = None

# Déterminer quel fichier utiliser (téléversé en priorité, puis sauvegardé)
file_to_process = None
file_name = None

if uploaded_file is not None:
    # Sauvegarde du nouveau fichier
    file_name = uploaded_file.name
    file_to_process = os.path.join(MAP_SAVE_DIR, file_name)
    
    # Nettoyer les anciens fichiers pour cette campagne
    for f in saved_files:
        try:
            os.remove(os.path.join(MAP_SAVE_DIR, f))
        except:
            pass
            
    # Ecrire le nouveau fichier
    with open(file_to_process, "wb") as f:
        f.write(uploaded_file.getbuffer())
        
    st.sidebar.success("Fichier sauvegardé en local !")
    
    # Process immediately to save to Cloud
    with st.spinner("Synchronisation Cloud en cours..."):
        try:
            with tempfile.TemporaryDirectory() as tmpdirname:
                temp_gdf = None
                if file_name.endswith(".zip"):
                    with zipfile.ZipFile(file_to_process, 'r') as zip_ref:
                        zip_ref.extractall(tmpdirname)
                        shp_files = [f for f in os.listdir(tmpdirname) if f.endswith('.shp')]
                        if shp_files:
                            temp_gdf = gpd.read_file(os.path.join(tmpdirname, shp_files[0]))
                elif file_name.endswith(".geojson"):
                    temp_gdf = gpd.read_file(file_to_process)
                    
                if temp_gdf is not None:
                    # Uniformiser le CRS avant sauvegarde cloud
                    if temp_gdf.crs is None:
                        temp_gdf.set_crs(epsg=2154, inplace=True)
                    if temp_gdf.crs.to_epsg() != 4326:
                        temp_gdf = temp_gdf.to_crs(epsg=4326)
                        
                    geojson_str = temp_gdf.to_json()
                    success = active_loader.save_telepac_to_cloud(selected_campaign, geojson_str)
                    if success:
                        st.sidebar.success("✅ Synchronisé sur tous vos appareils !")
        except Exception as e:
            st.sidebar.warning(f"La synchronisation Cloud a échoué (sauvegardé en local). Erreur: {e}")

elif saved_file_path:
    # Utiliser le fichier précédemment sauvegardé localement
    file_to_process = saved_file_path
    file_name = os.path.basename(saved_file_path)
    st.sidebar.info(f"Fichier local chargé : {file_name}")
else:
    # Pas de fichier local, tenter de charger depuis le Cloud
    with st.spinner("Recherche des contours depuis le Cloud..."):
        geojson_str = active_loader.load_telepac_from_cloud(selected_campaign)
        if geojson_str:
            # Reconstruire un GDF
            import json
            telepac_gdf = gpd.GeoDataFrame.from_features(json.loads(geojson_str)["features"])
            telepac_gdf.set_crs(epsg=4326, inplace=True)
            st.sidebar.info("Cartographie chargée depuis le Cloud ☁️")

# N'extraire/lire que si on n'a pas déjà récupéré du Cloud (telepac_gdf est None)
if file_to_process is not None and telepac_gdf is None:
    try:
        with tempfile.TemporaryDirectory() as tmpdirname:
            if file_name.endswith(".zip"):
                with zipfile.ZipFile(file_to_process, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                    shp_files = [f for f in os.listdir(tmpdirname) if f.endswith('.shp')]
                    if shp_files:
                        telepac_gdf = gpd.read_file(os.path.join(tmpdirname, shp_files[0]))
                    else:
                        st.sidebar.error("Aucun fichier .shp trouvé dans le ZIP.")
            elif file_name.endswith(".geojson"):
                telepac_gdf = gpd.read_file(file_to_process)
                
            if telepac_gdf is not None:
                # If CRS is missing (e.g. no .prj file), assume Lambert 93 (EPSG:2154) for French Telepac data
                if telepac_gdf.crs is None:
                    telepac_gdf.set_crs(epsg=2154, inplace=True)
                
                # Ensure CRS is web mercator for Folium (WGS84 EPSG:4326)
                if telepac_gdf.crs.to_epsg() != 4326:
                    telepac_gdf = telepac_gdf.to_crs(epsg=4326)
                st.sidebar.success("Fichier Télépac chargé avec succès !")
                
                # Recenter map based on Telepac data if available
                bounds = telepac_gdf.total_bounds # [minx, miny, maxx, maxy]
                center_lon_default = (bounds[0] + bounds[2]) / 2 + 0.05
                center_lat_default = (bounds[1] + bounds[3]) / 2
                
    except Exception as e:
        st.sidebar.error(f"Erreur lecture fichier géographique : {e}")

# 2. Build the Map
m = folium.Map(location=[center_lat_default, center_lon_default], zoom_start=default_zoom, control_scale=True)

# Add Satellite Tile Layer
folium.TileLayer(
    tiles = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attr = 'Esri',
    name = 'Satellite Image (Esri)',
    overlay = False,
    control = True
).add_to(m)

# Layer 1: Compteurs (Meters)
meter_fg = folium.FeatureGroup(name="Compteurs d'Irrigation")
if not df_gps.empty:
    for idx, row in df_gps.iterrows():
        meter_id = str(row.get('ID_Compteur', f"Inconnu_{idx}"))
        tooltip = f"Compteur: {meter_id}"
        
        # We put the ID in the popup so `st_folium` returns it when clicked. We format it distinctly.
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=f"METER_ID:{meter_id}", # Magic string to parse on click
            tooltip=tooltip,
            icon=folium.Icon(color='blue', icon='tint')
        ).add_to(meter_fg)
meter_fg.add_to(m)

# Layer 2: Telepac Contours
telepac_fg = folium.FeatureGroup(name="Contours Parcelles (Télépac)")
if telepac_gdf is not None:
    # Colors mapping based on typical French crop codes (Télépac codes or names)
    # You can expand this based on the actual codes in the 'CULTURE' field
    CROP_COLORS = {
        'BTH': '#f1c40f', # Blé tendre (Yellow)
        'Bled tendre': '#f1c40f',
        'BLE': '#f1c40f',
        'ORP': '#e67e22', # Orge de printemps (Orange)
        'ORH': '#d35400', # Orge d'hiver (Dark Orange)
        'ORGE': '#e67e22',
        'CZH': '#9b59b6', # Colza d'hiver (Purple)
        'COLZA': '#9b59b6',
        'MIS': '#f39c12', # Maïs semence
        'MID': '#f39c12', # Maïs doux
        'MAI': '#f39c12', # Maïs (Gold)
        'MAIS': '#f39c12',
        'TRN': '#2ecc71', # Tournesol (Greenish)
        'TOURNESOL': '#2ecc71',
        'PTR': '#27ae60', # Prairies temporaires (Green)
        'PPH': '#2ed573', # Prairies permanentes
        'PRL': '#2ed573', # Prairies / Herbe
        'J6S': '#bdc3c7', # Jachères (Grey)
        'J5M': '#bdc3c7',
        'JACHERE': '#bdc3c7',
        'LUZ': '#8e44ad', # Luzerne 
        'Pois': '#1abc9c',
        'POI': '#1abc9c',
        'DEFAULT': '#3498db' # Blue for unknown crops
    }
    
    def get_crop_style(feature):
        props = feature.get('properties', {})
        # Different fields possible for crop depending on the shapefile version
        crop_code = props.get('CULTURE', props.get('CODE_CULTU', props.get('LIB_CULTU', 'DEFAULT')))
        
        # If it's a string, uppercase it
        if isinstance(crop_code, str):
            crop_code_upper = crop_code.upper()
            # Find matching color
            fill_color = CROP_COLORS.get(crop_code_upper, CROP_COLORS['DEFAULT'])
            # Soft fallback if string contains the word
            if fill_color == CROP_COLORS['DEFAULT']:
                for key, color in CROP_COLORS.items():
                    if key in crop_code_upper:
                        fill_color = color
                        break
        else:
            fill_color = CROP_COLORS['DEFAULT']

        return {
            'fillColor': fill_color,
            'color': '#2c3e50', # Dark border
            'weight': 1.5,
            'fillOpacity': 0.6
        }
    
    
    # Try to find a good tooltip attribute, like 'NUM_ILOT' or 'ID_PARCEL'
    fields = telepac_gdf.columns.tolist()
    tooltip_fields = [f for f in fields if f.upper() in ['NUM_ILOT', 'NUM_PARCEL', 'CULTURE', 'SURFACE']]
    if not tooltip_fields and fields and fields[0] != 'geometry':
        tooltip_fields = [fields[0]]

    folium.GeoJson(
        telepac_gdf,
        style_function=get_crop_style,
        tooltip=folium.GeoJsonTooltip(fields=tooltip_fields) if tooltip_fields else None
    ).add_to(telepac_fg)
telepac_fg.add_to(m)

folium.LayerControl().add_to(m)

# 3. Render Map
st.markdown("### 🗺️ Carte Interactive")
st.info("💡 Cliquez sur le marqueur bleu d'un compteur pour ouvrir le formulaire de saisie de relevé en dessous.")

# st_folium is bidirectional!
st_data = st_folium(m, width="100%", height=500, returned_objects=["last_object_clicked_popup"])

# 4. Handle Click Interaction
if st_data and st_data.get("last_object_clicked_popup"):
    popup_text = st_data["last_object_clicked_popup"]
    if popup_text and "METER_ID:" in popup_text:
        clicked_id = popup_text.split("METER_ID:")[1].strip()
        st.session_state.clicked_meter_id = clicked_id

# 5. Render Data Entry Form if a meter was clicked
if st.session_state.clicked_meter_id:
    st.divider()
    meter_id = st.session_state.clicked_meter_id
    st.subheader(f"📝 Nouveau Relevé : {meter_id}")
    
    with st.form(key="form_releve_compteur"):
        col1, col2 = st.columns(2)
        with col1:
            releve_date = st.date_input("Date du relevé", value=datetime.today())
        with col2:
            releve_index = st.number_input("Nouvel Index (m3)", min_value=0, step=1)
            
        submit_releve = st.form_submit_button("Enregistrer le relevé 💾")
        
    if submit_releve:
        if releve_index <= 0:
             st.error("L'index doit être supérieur à zéro.")
        else:
             # Logic to save to Google Sheets
             with st.spinner("Enregistrement en cours..."):
                 try:
                     df_existing = active_loader.get_releves_compteurs()
                     
                     new_row = pd.DataFrame([{
                         "ID_Compteur": meter_id,
                         "Date_Relevé": releve_date.strftime("%d/%m/%Y"),
                         "Index_m3": releve_index
                     }])
                     
                     # Check if it already exists for this date/meter to prevent duplicates
                     if not df_existing.empty:
                         # Append
                         df_updated = pd.concat([df_existing, new_row], ignore_index=True)
                     else:
                         df_updated = new_row
                         
                     # Save back to sheets
                     if active_loader.use_cloud and active_loader.conn:
                          # We assume connection exists to write back
                          active_loader.conn.update(worksheet="RELEVES_COMPTEURS", data=df_updated, spreadsheet="MASTER_EXPLOITATION")
                          active_loader.clear_cache()
                          st.success(f"✅ Relevé de {releve_index} m3 enregistré pour {meter_id} !")
                          st.session_state.clicked_meter_id = None # hide form until click again
                          st.rerun()
                     else:
                          st.error("Impossible d'enregistrer en mode Local (Lecture seule).")
                 except Exception as e:
                     st.error(f"Erreur d'enregistrement: {e}")

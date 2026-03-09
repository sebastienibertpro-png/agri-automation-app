import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import tempfile
import os
import zipfile
from datetime import datetime
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
uploaded_file = st.sidebar.file_uploader("Importer fichier Télépac (GeoJSON ou ZIP Shapefile)", type=['geojson', 'zip'])
telepac_gdf = None

if uploaded_file is not None:
    try:
        with tempfile.TemporaryDirectory() as tmpdirname:
            filename = uploaded_file.name
            filepath = os.path.join(tmpdirname, filename)
            
            with open(filepath, "wb") as f:
                f.write(uploaded_file.getbuffer())
                
            if filename.endswith(".zip"):
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                    shp_files = [f for f in os.listdir(tmpdirname) if f.endswith('.shp')]
                    if shp_files:
                        telepac_gdf = gpd.read_file(os.path.join(tmpdirname, shp_files[0]))
                    else:
                        st.sidebar.error("Aucun fichier .shp trouvé dans le ZIP.")
            elif filename.endswith(".geojson"):
                telepac_gdf = gpd.read_file(filepath)
                
            if telepac_gdf is not None:
                # Ensure CRS is web mercator for Folium (WGS84 EPSG:4326)
                if telepac_gdf.crs and telepac_gdf.crs.to_epsg() != 4326:
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
    # Use standard style
    style_function = lambda x: {
        'fillColor': '#4CAF50',
        'color': '#1B5E20',
        'weight': 2,
        'fillOpacity': 0.4
    }
    
    # Try to find a good tooltip attribute, like 'NUM_ILOT' or 'ID_PARCEL'
    fields = telepac_gdf.columns.tolist()
    tooltip_fields = [f for f in fields if f.upper() in ['NUM_ILOT', 'NUM_PARCEL', 'CULTURE', 'SURFACE']]
    if not tooltip_fields and fields and fields[0] != 'geometry':
        tooltip_fields = [fields[0]]

    folium.GeoJson(
        telepac_gdf,
        style_function=style_function,
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

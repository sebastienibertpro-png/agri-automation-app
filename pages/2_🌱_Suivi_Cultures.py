import streamlit as st
import pandas as pd
import tempfile
import os
import folium
from streamlit_folium import st_folium
from report_gen import ReportGenerator
from shared import init_campaign_selector, APP_BASE_URL, OBSERVATION_DRIVE_FOLDER_ID, get_drive_uploader

st.set_page_config(page_title="Suivi Cultures", page_icon="🧪", layout="wide")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# --- OBSERVATIONS AU CHAMP ---
st.header("📸 Observations au Champ")

# 1. VISUALISATION (CARTE)
st.subheader("📍 Carte des Observations")

df_obs = active_loader.get_observations(selected_campaign)

# Maïs-Center coords roughly
m_map = folium.Map(location=[45.0, 1.0], zoom_start=13)

if not df_obs.empty:
    for _, row in df_obs.iterrows():
        gps = str(row.get('Localisation_GPS', ''))
        if gps and ',' in gps:
            try:
                lat, lon = map(float, gps.split(','))
                obs_text = str(row.get('Observations', 'Pas de texte'))
                photo_id = str(row.get('Photo', ''))
                
                popup_html = f"<b>{row['ID_Parcelle']}</b><br>{row['Date']}<br>{obs_text}"
                if photo_id:
                    # Lien direct vers Drive
                    photo_url = f"https://drive.google.com/uc?id={photo_id}"
                    popup_html += f"<br><img src='{photo_url}' width='200'>"
                
                folium.Marker(
                    [lat, lon],
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"{row['ID_Parcelle']} - {row['Date']}",
                    icon=folium.Icon(color='green', icon='camera', prefix='fa')
                ).add_to(m_map)
                
                m_map.location = [lat, lon]
            except: pass

st_folium(m_map, width=None, height=500, use_container_width=True)

st.divider()

# 2. SAISIE D'OBSERVATION
st.subheader("📝 Nouvelle Observation")

with st.expander("Ouvrir le formulaire de saisie", expanded=False):
    col_o1, col_o2 = st.columns(2)
    with col_o1:
        obs_parcelle = st.selectbox("Parcelle", available_parcelles, key="obs_p")
        obs_date = st.date_input("Date", key="obs_d")
    with col_o2:
        obs_stade = st.selectbox("Stade Culture", ["Levée", "4F", "8F", "Floraison", "Maturité", "Récolte"], key="obs_s")
        obs_photo = st.file_uploader("Prendre une photo (ou charger)", type=["jpg", "jpeg", "png"], key="obs_img")

    obs_text = st.text_area("Observations au champ", placeholder="Saisir vos remarques ici...")

    # Gestion GPS simplifiée pour éviter streamlit-js-eval si non installé
    st.markdown("**Localisation (GPS)**")
    gps_coords = st.text_input("Coordonnées GPS (ex: 45.1, 1.2)", help="Saisissez ou capturez les coordonnées", key="obs_gps_coord")
    
    # Alternative simple pour capturer la position via Javascript directement dans Streamlit
    components_html = """
    <script>
    function getLocation() {
      if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(showPosition);
      }
    }
    function showPosition(position) {
      window.parent.postMessage({
        type: 'streamlit:set_component_value',
        value: position.coords.latitude + ", " + position.coords.longitude
      }, '*');
    }
    </script>
    <button onclick="getLocation()">📌 Capturer ma position GPS</button>
    """
    # Note: L'intégration propre du GPS sans module tiers demande un peu de JS personnalisé
    # Pour l'instant on reste sur la saisie manuelle ou extraction EXIF si possible
    st.info("💡 Conseil : Copiez vos coordonnées Google Maps ou activez la géolocalisation sur votre appareil.")

    if st.button("🚀 Enregistrer l'observation", type="primary", use_container_width=True):
        if not obs_text and not obs_photo:
            st.error("Veuillez saisir au moins une observation ou une photo.")
        else:
            photo_drive_id = ""
            if obs_photo:
                with st.spinner("Upload de la photo vers Drive..."):
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                        tmp.write(obs_photo.getbuffer())
                        tmp_path = tmp.name
                    
                    uploader = get_drive_uploader()
                    if uploader:
                        try:
                            photo_drive_id = uploader.upload_file(tmp_path, OBSERVATION_DRIVE_FOLDER_ID)
                        except Exception as e:
                            st.error(f"Erreur Drive: {e}")
                        finally:
                            os.remove(tmp_path)
                    else:
                        st.error("Drive uploader non disponible.")
            
            new_obs = {
                'ID_Intervention': f"OBS_{selected_campaign}_{obs_parcelle}_{pd.Timestamp.now().strftime('%H%M%S')}",
                'Date': obs_date.strftime('%d/%m/%Y'),
                'Campagne': selected_campaign,
                'ID_Parcelle': obs_parcelle,
                'Nature_Intervention': 'Observation',
                'Stade_Culture': obs_stade,
                'Observations': obs_text,
                'Photo': photo_drive_id,
                'Localisation_GPS': gps_coords,
                'Statut_Intervention': 'Réalisé'
            }
            
            with st.spinner("Enregistrement..."):
                if active_loader.bulk_insert_interventions(pd.DataFrame([new_obs])):
                    st.success("Observation enregistrée !")
                    st.cache_data.clear()
                    active_loader.clear_cache()
                    st.rerun()
                else:
                    st.error("Erreur lors de l'enregistrement.")

st.divider()

# --- BILAN AZOTÉ (Suivi PPF) ---
st.header("🌾 Bilan Azoté (Suivi PPF)")
with st.expander("Voir le reste à apporter (N) par parcelle", expanded=False):
    try:
        df_ppf = active_loader.get_ppf(selected_campaign)
        if df_ppf.empty:
            st.info(f"Aucune donnée dans l'onglet PPF pour la campagne {selected_campaign}.")
        else:
            df_ferti_realized = df_campaign[df_campaign['Nature_Intervention'] == "Fertilisation"].copy()
            realized_n_by_parcel = {}
            if not df_ferti_realized.empty:
                df_ferti_realized['N/ha'] = pd.to_numeric(df_ferti_realized['N/ha'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                sum_n = df_ferti_realized.groupby('ID_Parcelle')['N/ha'].sum()
                realized_n_by_parcel = sum_n.to_dict()
                
            ppf_display_data = []
            for _, row in df_ppf.iterrows():
                p_id = str(row.get('ID_Parcelle', 'N/A')).strip()
                if p_id == 'N/A' or not p_id: continue
                dose_x_raw = str(row.get('Dose_X', '0')).replace(',', '.')
                try: dose_x = float(dose_x_raw)
                except: dose_x = 0.0
                n_apport = realized_n_by_parcel.get(p_id, 0.0)
                reste = dose_x - n_apport
                culture = str(row.get('Culture', ''))
                ppf_display_data.append({
                    'Parcelle': p_id, 'Culture': culture,
                    'Dose X Prévue (U)': int(round(dose_x)), 'N Apporté (U)': int(round(n_apport)),
                    'Reste à Apporter (U)': int(round(reste))
                })
                
            if ppf_display_data:
                df_ppf_vis = pd.DataFrame(ppf_display_data)
                def color_reste(val):
                    if val > 0: return 'color: #d17a22' 
                    elif val < 0: return 'color: #d32f2f' 
                    else: return 'color: #388e3c' 
                st.dataframe(df_ppf_vis.style.map(color_reste, subset=['Reste à Apporter (U)']), use_container_width=True, hide_index=True)
            else: st.info("Impossible de lier les parcelles du PPF.")
    except Exception as e: st.error(f"Erreur bilan azoté : {e}")

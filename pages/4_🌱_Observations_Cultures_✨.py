import streamlit as st
import pandas as pd
import tempfile
import os
import folium
from streamlit_folium import st_folium
from streamlit_js_eval import get_geolocation
from report_gen import ReportGenerator
from shared import init_campaign_selector, APP_BASE_URL, OBSERVATION_DRIVE_FOLDER_ID, get_drive_uploader, render_brand_page_header, inject_premium_css, render_premium_header

st.set_page_config(page_title="🌱 Observations Cultures", page_icon="🌱", layout="wide")
inject_premium_css()

render_brand_page_header("Observations au Champ", "Gérez vos tours de plaine et diagnostics IA ✨", icon="🌱")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# 1. VISUALISATION (CARTE)
render_premium_header("📍 Carte des Observations", "Consultez les relevés géolocalisés sur votre parcellaire", color="green")

df_obs = active_loader.get_observations(selected_campaign)

# Initialisation de la carte avec Satellite par défaut
m_map = folium.Map(
    location=[45.0, 1.0],
    zoom_start=13,
    tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attr='Esri',
    name='Satellite (Esri)'
)

folium.LayerControl().add_to(m_map)

if not df_obs.empty:
    for _, row in df_obs.iterrows():
        gps = str(row.get('Localisation_GPS', row.get('Localisation GPS', ''))).replace(' ', '')
        if gps and ',' in gps:
            try:
                lat, lon = map(float, gps.split(','))
                obs_text = str(row.get('Observations', 'Pas de texte'))
                photo_id = str(row.get('Photo', ''))
                
                popup_html = f"<b>{row['ID_Parcelle']}</b><br>{row['Date']}<br>{obs_text}"
                if photo_id and str(photo_id).lower() != 'nan':
                    if str(photo_id).startswith("http"):
                        photo_url = photo_id
                    else:
                        photo_url = f"https://drive.google.com/uc?id={photo_id}"
                    popup_html += f"<br><img src='{photo_url}' width='200'>"
                
                folium.Marker(
                    [lat, lon],
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"{row['ID_Parcelle']} - {row['Date']}",
                    icon=folium.Icon(color='green', icon='camera', prefix='fa')
                ).add_to(m_map)
                
                # Center on last observation
                m_map.location = [lat, lon]
            except Exception as e:
                pass

st_folium(m_map, width=None, height=500, use_container_width=True)

st.divider()

# --- DIAGNOSTIC IA ---
render_premium_header("🤖 Diagnostic IA", "Maladies, ravageurs & adventices", color="blue")
st.markdown("Prenez une photo d'une plante malade, d'un symptôme ou d'une adventice pour obtenir un diagnostic instantané par l'Intelligence Artificielle.")

diag_col1, diag_col2 = st.columns([1, 1])
with diag_col1:
    diag_photo = st.file_uploader("Charger une photo pour le diagnostic", type=["jpg", "jpeg", "png"], key="diag_img_ia")

if diag_photo:
    with diag_col2:
        st.image(diag_photo, caption="Photo à analyser", use_container_width=True)
    
    if st.button("Lancer le diagnostic 🔍", type="primary", use_container_width=True):
        api_key = st.secrets.get("GEMINI_API_KEY")
        if not api_key:
            st.error("⚠️ Clé d'API Gemini (GEMINI_API_KEY) introuvable dans .streamlit/secrets.toml.")
        else:
            with st.spinner("Analyse de l'image par Gemini Vision en cours..."):
                try:
                    import google.generativeai as genai
                    import PIL.Image
                    
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel("gemini-2.5-flash")
                    img = PIL.Image.open(diag_photo)
                    
                    prompt = """
                    Tu es un expert agronome hautement qualifié spécialisé dans la protection des cultures.
                    Analyse cette photo fournie par un agriculteur et fournis un diagnostic détaillé :
                    1. IDENTIFICATION : Identifie la culture ou la plante présente (si c'est une adventice, donne son nom commun et scientifique).
                    2. DIAGNOSTIC : Identifie les symptômes visibles sur la plante (maladie fongique, bactérie, virus, carence en nutriments, attaque d'insecte, stress abiotique, etc.).
                    3. RECOMMANDATIONS : Propose des solutions concrètes ou des recommandations pour traiter le problème.
                    Si la photo n'est pas claire, précise-le. Sois précis, professionnel et structure ta réponse.

                    IMPORTANT : À la toute fin de ta réponse, ajoute un encart avec la balise ---RESUME---.
                    Sous cette balise, écris un résumé très clair et succinct en 1 ou 2 phrases du diagnostic et de la solution. 
                    Ce résumé servira de note géolocalisée et de plus-value technique directe pour l'agriculteur.
                    """
                    
                    response = model.generate_content([prompt, img])
                    
                    # Sauvegarder dans le session state pour éviter l'effacement au rerender
                    st.session_state["diag_result_text"] = response.text
                    st.success("Analyse terminée !")
                except Exception as e:
                    st.error(f"Erreur lors du diagnostic IA : {e}")

if "diag_result_text" in st.session_state:
    st.markdown("### 📋 Résultat du Diagnostic")
    st.info(st.session_state["diag_result_text"])
    
    # Bouton direct pour remplir l'observation
    if st.button("📝 Utiliser ce résumé pour une Nouvelle Observation", use_container_width=True):
        res_text = st.session_state["diag_result_text"]
        if "---RESUME---" in res_text:
            resume = res_text.split("---RESUME---")[-1].strip()
        else:
            resume = res_text[:200] + "..."
            
        st.session_state["auto_fill_obs_text"] = f"Diagnostic IA: {resume}"
        st.success("Résumé copié ! Vous pouvez finaliser votre saisie ci-dessous.")

st.divider()

# 2. SAISIE D'OBSERVATION
render_premium_header("📝 Nouvelle Observation", "Enregistrez vos relevés au champ", color="green")

with st.expander("Ouvrir le formulaire de saisie", expanded=False):
    st.markdown("""
    <div style="background-color: #f8fafc; padding: 20px; border-radius: 12px; border: 1px solid #e2e8f0; margin-bottom: 20px;">
        <h4 style="color: #1e293b; margin-top:0;">Informations Générales</h4>
    </div>
    """, unsafe_allow_html=True)
    col_o1, col_o2 = st.columns(2)
    with col_o1:
        obs_parcelle = st.selectbox("Parcelle", available_parcelles, key="obs_p")
        obs_date = st.date_input("Date", key="obs_d")
    with col_o2:
        obs_stade = st.selectbox("Stade Culture", ["Levée", "3F", "6F", "10F", "Floraison", "Maturité", "Récolte"], key="obs_s")
        obs_photo = st.file_uploader("Prendre une photo (ou charger)", type=["jpg", "jpeg", "png"], key="obs_img")

    st.markdown("""
    <div style="background-color: #f8fafc; padding: 20px; border-radius: 12px; border: 1px solid #e2e8f0; margin-bottom: 20px; margin-top: 20px;">
        <h4 style="color: #1e293b; margin-top:0;">Détails & Géolocalisation</h4>
    </div>
    """, unsafe_allow_html=True)
    
    default_obs_text = st.session_state.get("auto_fill_obs_text", "")
    obs_text = st.text_area("Observations au champ", value=default_obs_text, placeholder="Saisir vos remarques ici...")

    # Gestion GPS Automatique
    st.markdown("**Localisation (GPS)**")
    
    # On récupère la géolocalisation si le composant est activé
    loc = get_geolocation()
    
    # Correction: Mettre à jour manuellement la clé du widget pour forcer l'affichage
    if loc:
        new_gps = f"{loc['coords']['latitude']}, {loc['coords']['longitude']}"
        if st.session_state.get("cached_gps") != new_gps:
            st.session_state["cached_gps"] = new_gps
            st.session_state["obs_gps_coord"] = new_gps
        
    default_gps = st.session_state.get("cached_gps", "")
    
    if default_gps:
        st.success(f"📍 Position GPS détectée et mémorisée : {default_gps}")
    else:
        st.info("ℹ️ Pour capturer votre position automatiquement, patientez que le GPS s'active.")

    gps_coords = st.text_input("Coordonnées GPS", value=default_gps, help="Les coordonnées sont remplies automatiquement si le GPS est activé", key="obs_gps_coord")
    
    if st.button("🚀 Enregistrer l'observation", type="primary", use_container_width=True):
        if not obs_text and not obs_photo:
            st.error("Veuillez saisir au moins une observation ou une photo.")
        else:
            photo_drive_id = ""
            if obs_photo:
                with st.spinner("Upload de la photo vers ImgBB..."):
                    try:
                        import requests
                        import base64
                        
                        api_key = st.secrets.get("imgbb_api_key", None)
                        if not api_key:
                            st.error("⚠️ Clé API ImgBB introuvable dans .streamlit/secrets.toml.")
                        else:
                            # Convert photo to base64
                            image_b64 = base64.b64encode(obs_photo.getvalue()).decode("utf-8")
                            
                            url = "https://api.imgbb.com/1/upload"
                            payload = {
                                "key": api_key,
                                "image": image_b64
                            }
                            
                            res = requests.post(url, data=payload)
                            
                            if res.status_code == 200:
                                res_data = res.json()
                                photo_drive_id = res_data.get("data", {}).get("url", "")
                            else:
                                st.error(f"Erreur serveur ImgBB : {res.text}")
                    except Exception as e:
                        st.error(f"Erreur d'upload : {e}")
            
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


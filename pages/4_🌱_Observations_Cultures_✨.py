import streamlit as st
import pandas as pd
import folium
from folium.plugins import Fullscreen, MiniMap
from streamlit_folium import st_folium
from streamlit_js_eval import get_geolocation
from shared import (
    init_campaign_selector,
    render_brand_page_header,
    inject_premium_css,
    render_premium_header,
    get_fresh_loader,
)

st.set_page_config(page_title="🌱 Observations Cultures", page_icon="🌱", layout="wide")
inject_premium_css()

render_brand_page_header(
    "Observations au Champ",
    "Gérez vos tours de plaine et diagnostics IA ✨",
    icon="🌱",
)

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# ─────────────────────────────────────────────────────────────────────────────
# GPS : uniquement déclenché sur demande explicite (bouton) — évite les reruns infinis
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.get("gps_requested", False):
    loc = get_geolocation()  # Appel JS → Python (déclenche 1 seul rerun supplémentaire)
    if loc and isinstance(loc, dict) and "coords" in loc:
        try:
            lat_raw = loc["coords"]["latitude"]
            lon_raw = loc["coords"]["longitude"]
            new_gps = f"{float(lat_raw):.6f},{float(lon_raw):.6f}"
            st.session_state["gps_lat"] = float(lat_raw)
            st.session_state["gps_lon"] = float(lon_raw)
            st.session_state["gps_str"] = new_gps
        except Exception as gps_err:
            st.session_state["gps_error"] = str(gps_err)
        finally:
            # Désactiver le flag pour ne plus relancer get_geolocation au prochain rendu
            st.session_state["gps_requested"] = False

# Lecture depuis session_state (valeurs persistantes entre les reruns)
gps_lat = st.session_state.get("gps_lat")
gps_lon = st.session_state.get("gps_lon")
gps_str = st.session_state.get("gps_str", "")

# ─────────────────────────────────────────────────────────────────────────────
# 1. CARTE DES OBSERVATIONS
# ─────────────────────────────────────────────────────────────────────────────
render_premium_header(
    "📍 Carte des Observations",
    "Relevés géolocalisés sur votre parcellaire",
    color="green",
)

# Lecture des observations (get_interventions utilise déjà ttl=0 — pas de quota inutile)
df_obs = active_loader.get_observations(selected_campaign)

# Initialisation de la carte satellite
m_map = folium.Map(
    location=[46.8, 2.3],
    zoom_start=6,
    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr="Esri",
    name="Satellite (Esri)",
)
folium.LayerControl().add_to(m_map)

# ── Plugin Plein Écran ──
Fullscreen(
    position="topleft",
    title="Plein écran",
    title_cancel="Quitter le plein écran",
    force_separate_button=True,
).add_to(m_map)

# ── Mini-carte de repérage (coin bas-gauche) ──
MiniMap(
    tile_layer=folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery",
        name="Satellite",
    ),
    zoom_level_offset=-5,
    toggle_display=True,
).add_to(m_map)

markers_added = 0
gps_errors = []

if not df_obs.empty:
    for _, row in df_obs.iterrows():
        # Chercher la colonne GPS quel que soit son nom exact
        gps_raw = str(
            row.get("Localisation_GPS", row.get("Localisation GPS", row.get("GPS", "")))
        ).strip()

        # Nettoyage : supprimer espaces parasites
        gps_clean = gps_raw.replace(" ", "")

        if not gps_clean or "," not in gps_clean:
            gps_errors.append(f"Ligne {row.get('ID_Intervention','?')} — GPS vide ou invalide : '{gps_raw}'")
            continue

        try:
            parts = gps_clean.split(",")
            lat = float(parts[0])
            lon = float(parts[1])

            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                gps_errors.append(f"Ligne {row.get('ID_Intervention','?')} — Coordonnées hors limites : lat={lat}, lon={lon}")
                continue

            obs_text = str(row.get("Observations", "Pas de texte"))
            photo_id = str(row.get("Photo", ""))

            popup_html = (
                f"<b>{row.get('ID_Parcelle','?')}</b><br>"
                f"{row.get('Date','')}<br>"
                f"<i>{str(row.get('Stade_Culture', ''))}</i><br>"
                f"{obs_text[:200]}"
            )

            if photo_id and photo_id.lower() not in ("nan", "", "none"):
                if photo_id.startswith("http"):
                    photo_url = photo_id
                else:
                    photo_url = f"https://drive.google.com/uc?id={photo_id}"
                popup_html += f"<br><img src='{photo_url}' width='200'>"

            folium.Marker(
                [lat, lon],
                popup=folium.Popup(popup_html, max_width=320),
                tooltip=f"📍 {row.get('ID_Parcelle','?')} — {row.get('Date','')}",
                icon=folium.Icon(color="green", icon="leaf", prefix="fa"),
            ).add_to(m_map)

            m_map.location = [lat, lon]
            m_map.zoom_start = 14
            markers_added += 1

        except Exception as exc:
            gps_errors.append(f"Ligne {row.get('ID_Intervention','?')} — Erreur parsing GPS '{gps_raw}' : {exc}")

# Bandeau synthèse
obs_count = len(df_obs) if not df_obs.empty else 0
col_info1, col_info2 = st.columns([3, 1])
with col_info1:
    if obs_count == 0:
        st.info("ℹ️ Aucune observation enregistrée pour cette campagne.")
    else:
        st.success(f"✅ {obs_count} observation(s) en base — {markers_added} affichée(s) sur la carte ({obs_count - markers_added} sans GPS valide).")
with col_info2:
    if gps_errors:
        with st.expander(f"⚠️ {len(gps_errors)} sans GPS"):
            for err in gps_errors:
                st.caption(err)

st_folium(m_map, width=None, height=480, use_container_width=True)

# ── PANNEAU DIAGNOSTIC : voir les données brutes lues depuis GSheets ──
with st.expander("🔍 Diagnostic — Données brutes lues depuis Google Sheets", expanded=False):
    # Relecture totale de JOURNAL_INTERVENTION (ttl=0 garanti via get_interventions)
    df_all_raw = active_loader.get_interventions()
    obs_raw = df_all_raw[df_all_raw.get('Nature_Intervention', pd.Series(dtype=str)).astype(str).str.strip().str.upper() == 'OBSERVATION'] if not df_all_raw.empty and 'Nature_Intervention' in df_all_raw.columns else pd.DataFrame()

    st.markdown(f"""
    - **Lignes totales dans JOURNAL_INTERVENTION :** `{len(df_all_raw)}`  
    - **Lignes 'Observation' filtrées :** `{len(obs_raw)}`  
    - **Colonnes disponibles :** `{', '.join(df_all_raw.columns.tolist()) if not df_all_raw.empty else 'aucune'}`
    """)

    if not obs_raw.empty:
        cols_diag = [c for c in ["Date", "ID_Parcelle", "Nature_Intervention", "Localisation_GPS", "Observations", "Campagne"] if c in obs_raw.columns]
        st.dataframe(obs_raw[cols_diag].tail(10), use_container_width=True)
    else:
        st.warning("Aucune ligne 'Observation' trouvée. Vérifiez que la colonne **Nature_Intervention** contient bien 'Observation'.")
        if not df_all_raw.empty:
            st.markdown("**5 dernières lignes brutes :**")
            st.dataframe(df_all_raw.tail(5), use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# 2. DIAGNOSTIC IA
# ─────────────────────────────────────────────────────────────────────────────
render_premium_header(
    "🤖 Diagnostic IA",
    "Maladies, ravageurs & adventices",
    color="blue",
)
st.markdown(
    "Chargez une photo de plante malade, symptôme ou adventice pour obtenir "
    "un diagnostic instantané par l'IA Gemini Vision."
)

diag_col1, diag_col2 = st.columns([1, 1])
with diag_col1:
    diag_photo = st.file_uploader(
        "Charger une photo pour le diagnostic",
        type=["jpg", "jpeg", "png"],
        key="diag_img_ia",
    )

if diag_photo:
    with diag_col2:
        st.image(diag_photo, caption="Photo à analyser", use_container_width=True)

    if st.button("🔍 Lancer le diagnostic IA", type="primary", use_container_width=True):
        api_key = st.secrets.get("GEMINI_API_KEY", "")
        if not api_key:
            st.error("⚠️ Clé GEMINI_API_KEY introuvable dans .streamlit/secrets.toml.")
        else:
            with st.spinner("Analyse Gemini Vision en cours…"):
                try:
                    import google.generativeai as genai
                    import PIL.Image

                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel("gemini-2.5-flash")
                    img = PIL.Image.open(diag_photo)

                    prompt = """
Tu es un expert agronome hautement qualifié spécialisé dans la protection des cultures.
Analyse cette photo fournie par un agriculteur et fournis un diagnostic détaillé :

1. **IDENTIFICATION** : Identifie la culture ou la plante présente (si c'est une adventice, donne son nom commun et scientifique).
2. **DIAGNOSTIC** : Identifie les symptômes visibles (maladie fongique, bactérie, virus, carence, insecte, stress abiotique, etc.).
3. **RECOMMANDATIONS** : Propose des solutions concrètes, des produits homologués ou des pratiques agronomiques.

Si la photo n'est pas claire, précise-le. Sois précis, professionnel et structure ta réponse.

IMPORTANT : À la toute fin de ta réponse, ajoute la balise ---RESUME--- puis, en 1 ou 2 phrases maximum, un résumé très clair du diagnostic et de la solution. Ce résumé sera utilisé comme note géolocalisée et doit apporter une plus-value technique immédiate à l'agriculteur.
"""

                    response = model.generate_content([prompt, img])
                    st.session_state["diag_result_text"] = response.text
                    st.success("✅ Analyse terminée !")
                except Exception as e:
                    st.error(f"Erreur diagnostic IA : {e}")

if "diag_result_text" in st.session_state:
    result_text = st.session_state["diag_result_text"]

    st.markdown("### 📋 Résultat du Diagnostic")

    # Afficher le diagnostic complet + résumé séparément
    if "---RESUME---" in result_text:
        full_diag, resume_part = result_text.split("---RESUME---", 1)
        st.info(full_diag.strip())
        st.markdown("---")
        st.markdown("#### 🟢 Résumé & Recommandation")
        st.success(resume_part.strip())
        resume_to_save = resume_part.strip()
    else:
        st.info(result_text)
        resume_to_save = result_text[:300] + "…"

    if st.button(
        "📝 Utiliser ce résumé comme Nouvelle Observation géolocalisée",
        use_container_width=True,
    ):
        st.session_state["auto_fill_obs_text"] = f"[Diagnostic IA] {resume_to_save}"
        st.session_state["expand_obs_form"] = True
        st.success("Résumé copié ! Ouvrez le formulaire ci-dessous pour finaliser.")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# 3. NOUVELLE OBSERVATION
# ─────────────────────────────────────────────────────────────────────────────
render_premium_header(
    "📝 Nouvelle Observation",
    "Enregistrez vos relevés au champ — données géolocalisées",
    color="green",
)

# ── Bandeau GPS permanent (hors expander) ──
gps_col1, gps_col2 = st.columns([3, 1])
with gps_col1:
    if gps_str:
        st.success(f"📍 Position GPS capturée : **{gps_str}**  ← sera sauvegardée avec l'observation")
    else:
        st.info("ℹ️ GPS non capturé — cliquez sur 'Capturer GPS' pour obtenir votre position.")
with gps_col2:
    if st.button("📍 Capturer GPS", use_container_width=True):
        # Activer le flag : get_geolocation() sera appelé au prochain rendu uniquement
        st.session_state["gps_requested"] = True
        st.rerun()

open_form = st.session_state.get("expand_obs_form", False)

with st.expander("Ouvrir le formulaire de saisie", expanded=open_form):

    # ── Informations générales ──
    st.markdown(
        """<div style="background:#f0fdf4;padding:14px 18px;border-radius:10px;
        border-left:4px solid #16a34a;margin-bottom:16px;">
        <b style="color:#15803d;">Informations Générales</b></div>""",
        unsafe_allow_html=True,
    )

    col_o0, col_o0b = st.columns(2)
    with col_o0:
        st.text_input(
            "🌾 Campagne",
            value=selected_campaign,
            disabled=True,
            help="Campagne automatiquement déduite du filtre sélectionné dans le menu latéral gauche.",
        )

    col_o1, col_o2 = st.columns(2)
    with col_o1:
        obs_parcelle = st.selectbox("Parcelle", available_parcelles, key="obs_p")
        obs_date = st.date_input("Date", key="obs_d")
    with col_o2:
        obs_stade = st.selectbox(
            "Stade Culture",
            ["Levée", "3F", "6F", "10F", "Floraison", "Maturité", "Récolte"],
            key="obs_s",
        )
        obs_photo = st.file_uploader(
            "Photo (optionnel)", type=["jpg", "jpeg", "png"], key="obs_img"
        )

    # ── Observations & GPS ──
    st.markdown(
        """<div style="background:#eff6ff;padding:14px 18px;border-radius:10px;
        border-left:4px solid #3b82f6;margin-bottom:16px;margin-top:16px;">
        <b style="color:#1d4ed8;">Détails & Géolocalisation</b></div>""",
        unsafe_allow_html=True,
    )

    default_obs_text = st.session_state.get("auto_fill_obs_text", "")
    obs_text = st.text_area(
        "Observations au champ",
        value=default_obs_text,
        placeholder="Saisir vos remarques ici (symptômes observés, adventices, état de la culture…)",
        height=120,
    )

    # ── GPS : pas de widget avec key pour éviter le conflit de valeur ──
    # On lit depuis session_state + on permet la saisie manuelle
    st.markdown("**📍 Coordonnées GPS**")
    gps_from_session = st.session_state.get("gps_str", "")
    gps_manual = st.text_input(
        "Coordonnées GPS (lat,lon) — modifiables",
        value=gps_from_session,  # Pas de key= pour éviter conflit
        help="Format : 48.123456,2.345678 · Rempli automatiquement depuis le GPS du navigateur.",
        placeholder="ex: 48.123456,2.345678",
    )
    # Synchroniser manuellement la saisie dans session_state
    if gps_manual and gps_manual != gps_from_session:
        st.session_state["gps_str"] = gps_manual.strip().replace(" ", "")

    # Affichage de confirmation visuelle de ce qui sera sauvegardé
    gps_final = st.session_state.get("gps_str", "").strip()
    if gps_final:
        st.caption(f"✅ GPS à enregistrer : `{gps_final}`")
    else:
        st.caption("⚠️ Aucune coordonnée GPS — l'observation ne sera pas visible sur la carte.")

    # ── Bouton Enregistrer ──
    if st.button("🚀 Enregistrer l'observation", type="primary", use_container_width=True):
        if not obs_text.strip() and not obs_photo:
            st.error("Veuillez saisir au moins une observation ou joindre une photo.")
        else:
            photo_url_saved = ""
            if obs_photo:
                with st.spinner("Upload de la photo…"):
                    try:
                        import requests, base64

                        imgbb_key = st.secrets.get("imgbb_api_key", "")
                        if not imgbb_key:
                            st.warning("Clé ImgBB absente — photo non uploadée.")
                        else:
                            image_b64 = base64.b64encode(obs_photo.getvalue()).decode()
                            res = requests.post(
                                "https://api.imgbb.com/1/upload",
                                data={"key": imgbb_key, "image": image_b64},
                            )
                            if res.status_code == 200:
                                photo_url_saved = res.json().get("data", {}).get("url", "")
                            else:
                                st.error(f"Erreur ImgBB : {res.status_code}")
                    except Exception as exc:
                        st.error(f"Erreur upload photo : {exc}")

            # GPS : lire depuis session_state (source unique de vérité)
            # On préfère session_state["gps_str"] mis à jour par la saisie manuelle
            gps_to_save = st.session_state.get("gps_str", "").strip().replace(" ", "")

            new_obs = {
                "ID_Intervention": f"OBS_{selected_campaign}_{obs_parcelle}_{pd.Timestamp.now().strftime('%H%M%S')}",
                "Date": obs_date.strftime("%d/%m/%Y"),
                "Campagne": selected_campaign,
                "ID_Parcelle": obs_parcelle,
                "Nature_Intervention": "Observation",
                "Stade_Culture": obs_stade,
                "Observations": obs_text.strip(),
                "Photo": photo_url_saved,
                "Localisation_GPS": gps_to_save,
                "Statut_Intervention": "Réalisé",
            }

            with st.spinner("Enregistrement dans Google Sheets…"):
                ok = active_loader.bulk_insert_interventions(pd.DataFrame([new_obs]))

            if ok:
                st.success(
                    f"✅ Observation enregistrée ! GPS : **{gps_to_save if gps_to_save else 'non renseigné'}**"
                )
                # Nettoyage session state
                for key in ["auto_fill_obs_text", "expand_obs_form"]:
                    st.session_state.pop(key, None)
                # CRUCIAL : recréer un loader frais avec une connexion neuve
                # pour court-circuiter tout cache TTL résiduel de GSheetsConnection
                get_fresh_loader()
                st.rerun()
            else:
                st.error("❌ Erreur lors de l'enregistrement. Voir les messages ci-dessus.")

import streamlit as st
import os
from shared import init_campaign_selector

st.set_page_config(page_title="Agri Automation - Accueil", page_icon="🚜", layout="centered")

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

base_dir = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(base_dir, "LOGO.png")

try:
    if os.path.exists(logo_path):
        st.image(logo_path, use_column_width=True)
except Exception as e:
    st.warning(f"Erreur d'image: {e}")

st.title("🚜 Tableau de Bord - Agri Automation")
st.markdown("Bienvenue ! Utilisez le menu à gauche pour naviguer entre les différents outils de l'exploitation.")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# --- QR Action Logic (Must be at top generally for fast action) ---
q_params = st.query_params
val_param = q_params.get("validate_phyto", None)

intervention_id = None
if val_param:
    if isinstance(val_param, list):
        intervention_id = val_param[0]
    else:
        intervention_id = val_param

if intervention_id:
    st.info(f"🔍 Scan détecté pour l'intervention : {intervention_id}")
    
    if st.button("✅ Confirmer : Traitement RÉALISÉ"):
        with st.spinner("Mise à jour du statut..."):
            success = active_loader.update_intervention_status(intervention_id, "Réalisé")
            if success:
                st.success("Statut mis à jour avec succès ! Vous pouvez fermer.")
            else:
                st.error("Échec de la mise à jour (Vérifiez les logs ou la connexion).")
    st.divider()

# --- Dashboard View ---
st.subheader(f"📊 Résumé de la Campagne {selected_campaign}")

col1, col2, col3 = st.columns(3)
col1.metric("Interventions Registrées", len(df_campaign))
col2.metric("Parcelles Travaillées", len(available_parcelles))

phyto_count = len(df_campaign[df_campaign['Nature_Intervention'] == 'Traitement'])
ferti_count = len(df_campaign[df_campaign['Nature_Intervention'] == 'Fertilisation'])
recolte_count = len(df_campaign[df_campaign['Nature_Intervention'] == 'Récolte'])

col3.metric("Traitements Phyto", phyto_count)

col4, col5, col6 = st.columns(3)
col4.metric("Fertilisations", ferti_count)
col5.metric("Récoltes", recolte_count)
col6.metric("Autres", len(df_campaign) - phyto_count - ferti_count - recolte_count)

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
st.markdown(f"## 📋 Vue d'ensemble - Campagne {selected_campaign}")

# 1. ASSOLEMENT (Quick Glance)
st.subheader("🌾 Assolement en un coup d'œil")
df_asso = active_loader.get_assolement(selected_campaign)
if not df_asso.empty:
    # Filter for specific crops requested or all
    # Maïs, Maïs Pop corn, Blé
    crops_to_show = ["Maïs", "Maïs Pop corn", "Blé"]
    
    # Calculate totals
    asso_summary = df_asso.groupby('Culture')['Surface_Référence_Ha'].sum().reset_index()
    
    m_cols = st.columns(len(crops_to_show))
    for i, crop in enumerate(crops_to_show):
        row = asso_summary[asso_summary['Culture'].str.contains(crop, case=False, na=False)]
        surf = row['Surface_Référence_Ha'].sum() if not row.empty else 0.0
        m_cols[i].metric(label=crop, value=f"{surf:.1f} ha")
else:
    st.info("Aucune donnée d'assolement trouvée pour cette campagne.")

st.divider()

# 2. DERNIÈRE INTERVENTION & INTERV. PRÉVUES
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("⏱️ Dernière Intervention")
    # Get last "Réalisé" intervention
    df_realised = df_campaign[df_campaign['Statut_Intervention'].astype(str).str.lower().str.startswith('réalise')].copy()
    if not df_realised.empty:
        # Sort by date
        df_realised['Date_dt'] = pd.to_datetime(df_realised['Date'], errors='coerce', dayfirst=True)
        last_interv = df_realised.sort_values('Date_dt', ascending=False).iloc[0]
        
        # Synthetic display style ITK
        st.markdown(f"""
        <div style="padding:15px; border-radius:10px; border-left: 5px solid #4CAF50; background-color: #f9f9f9;">
            <h4 style="margin:0;">{last_interv['Nature_Intervention']}</h4>
            <p style="margin:5px 0;"><b>Date :</b> {last_interv['Date']} | <b>Parcelle :</b> {last_interv['ID_Parcelle']}</p>
            <p style="margin:0;"><b>Outil :</b> {last_interv['Outil'] or 'N/A'}</p>
            <p style="margin:0;"><b>Produit :</b> {last_interv['Nom_Produit'] or 'N/A'}</p>
            <p style="margin:0; font-size:0.9em; color:gray;"><i>Obs: {last_interv['Observations'] or '-'}</i></p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Aucune intervention réalisée enregistrée.")

with col_right:
    st.subheader("📅 Interventions Prévues")
    df_planned = df_campaign[df_campaign['Statut_Intervention'].astype(str).str.lower().str.startswith('prév')].copy()
    if not df_planned.empty:
        # Display top 5 planned
        df_planned['Date_dt'] = pd.to_datetime(df_planned['Date'], errors='coerce', dayfirst=True)
        top_planned = df_planned.sort_values('Date_dt').head(5)
        
        for _, row in top_planned.iterrows():
            st.markdown(f"• **{row['Date']}** : {row['Nature_Intervention']} sur {row['ID_Parcelle']}")
    else:
        st.info("Aucune intervention prévue.")

st.divider()

# 3. CONSOMMATION FUEL
st.subheader("⛽ Consommation Fuel")
df_fuel = active_loader.get_fuel_conso(selected_campaign)
if not df_fuel.empty:
    total_fuel = df_fuel['FUEL_quantité_L'].sum()
    st.metric("Consommation Totale Campagne", f"{total_fuel:.0f} L", delta=None)
    
    # Optional: Small bar chart or breakdown?
    if 'ID_Materiel' in df_fuel.columns:
        fuel_by_mat = df_fuel.groupby('ID_Materiel')['FUEL_quantité_L'].sum().reset_index()
        st.bar_chart(fuel_by_mat.set_index('ID_Materiel'))
else:
    st.info("Aucune donnée de consommation fuel pour cette campagne.")

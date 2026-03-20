import streamlit as st
import pandas as pd
from shared import init_campaign_selector

st.set_page_config(page_title="Assolement", page_icon="🌾", layout="wide")

st.title("🌾 Gestion de l'Assolement")
st.markdown("---")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader
campagne_input = int(selected_campaign)

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")

# Tabs
tab_saisie, tab_consult = st.tabs(["✍️ Saisie / Modification", "📋 Consultation & Totaux"])

with tab_saisie:
    st.subheader("📝 Définir la culture d'une parcelle")
    
    # Get all parcels from REF_PARCELLES
    df_ref_parcelles = dl.get_parcelles()
    if df_ref_parcelles.empty:
        st.error("Aucune parcelle trouvée dans REF_PARCELLES.")
        st.stop()
        
    liste_id_parcelles = sorted(df_ref_parcelles['ID_Parcelle'].unique().tolist())
    
    col_sel, col_info = st.columns([1, 2])
    with col_sel:
        selected_p = st.selectbox("Choisir la parcelle", liste_id_parcelles)
    
    # Load current assolement for this parcel/campaign to pre-fill
    df_asso_all = dl.get_assolement(campagne_input)
    existing_data = {}
    if not df_asso_all.empty:
        mask = df_asso_all['ID_Parcelle'].astype(str).str.strip() == str(selected_p).strip()
        match = df_asso_all[mask]
        if not match.empty:
            existing_data = match.iloc[0].to_dict()
    
    # Get ref data for the parcel
    ref_row = df_ref_parcelles[df_ref_parcelles['ID_Parcelle'] == selected_p].iloc[0]
    ref_surf = ref_row.get('Surface_Référence_Ha', 0.0)
    ref_ilot = ref_row.get('îlot PAC', '')
    
    with col_info:
        st.info(f"📍 **{selected_p}** | Ilot PAC: {ref_ilot} | Surface Réf: {ref_surf} ha")

    with st.form("form_asso"):
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("##### 🌱 Culture & Variété")
            culture = st.text_input("Culture", value=existing_data.get('Culture', ''))
            variete = st.text_input("Variété", value=existing_data.get('Variété', ''))
            surface = st.number_input("Surface cultivée (ha)", value=float(existing_data.get('Surface_Référence_Ha', ref_surf)), step=0.1)
            precedent = st.text_input("Précédent Cultural", value=existing_data.get('Precedent_Cultural', ''))
            obj_rdt = st.number_input("Objectif Rendement (Qx/Ha)", value=float(existing_data.get('Objectif_Rendement_Qx_Ha', 100.0)), step=1.0)
            prix_obj = st.number_input("Prix Vente Objectif (€/T)", value=float(existing_data.get('Prix_Vente_Objectif_€/T', 200.0)), step=1.0)
            
        with c2:
            st.markdown("##### 🚜 Itinéraire & Contrat")
            strategie = st.selectbox("Stratégie Travail Sol", ["Labour", "Simplifié", "Semis Direct"], index=0)
            gestion_res = st.selectbox("Gestion des Résidus", ["Enfouis", "Exportés", "Brûlés"], index=0)
            contrat = st.text_input("Contrat Commercial", value=existing_data.get('Contrat_Commercial', ''))
            
            st.markdown("##### 🍀 Couvert Intermédiaire")
            couvert_esp = st.text_input("Espèces Couvert Précédent", value=existing_data.get('Couvert_précédent_Especes', ''))
            couvert_dev = st.selectbox("Développement Couvert", ["Nul", "Faible", "Moyen", "Fort"], index=0)
            date_semis = st.date_input("Date Semis Prévisionnelle", value=None if pd.isna(existing_data.get('Date_Semis_Previsionnelle')) else pd.to_datetime(existing_data.get('Date_Semis_Previsionnelle')))
        
        commentaire = st.text_area("Commentaire Assolement", value=existing_data.get('Commentaire_Assolement', ''))
        
        submit = st.form_submit_button("💾 Sauvegarder l'assolement", use_container_width=True, type="primary")
        
    if submit:
        if not culture:
            st.error("Le nom de la culture est obligatoire.")
        else:
            asso_dict = {
                'Campagne': int(campagne_input),
                'ID_Parcelle': selected_p,
                'Surface_Référence_Ha': surface,
                'Culture': culture,
                'Variété': variete,
                'Precedent_Cultural': precedent,
                'Strategie_Travail_Sol': strategie,
                'Gestion_Résidus': gestion_res,
                'Contrat_Commercial': contrat,
                'Objectif_Rendement_Qx_Ha': obj_rdt,
                'Prix_Vente_Objectif_€/T': prix_obj,
                'Couvert_précédent_Especes': couvert_esp,
                'Développement_Couvert': couvert_dev,
                'Date_Semis_Previsionnelle': date_semis.strftime('%Y-%m-%d') if date_semis else '',
                'Commentaire_Assolement': commentaire
            }
            
            with st.spinner("Mise à jour de l'assolement..."):
                if dl.update_assolement(asso_dict):
                    st.success(f"Assolement mis à jour pour {selected_p} !")
                    st.rerun()

with tab_consult:
    st.subheader(f"📊 Récapitulatif de la Campagne {campagne_input}")
    
    df_asso = dl.get_assolement(campagne_input)
    
    if df_asso.empty:
        st.info("Aucun assolement défini pour cette campagne. Utilisez l'onglet Saisie.")
    else:
        # 1. Totaux par culture
        st.markdown("#### 🌍 Répartition des surfaces")
        summary = df_asso.groupby('Culture')['Surface_Référence_Ha'].sum().reset_index()
        summary = summary.sort_values(by='Surface_Référence_Ha', ascending=False)
        
        cols = st.columns(len(summary) if len(summary) > 0 else 1)
        for i, row in summary.iterrows():
            if i < len(cols):
                cols[i].metric(row['Culture'], f"{row['Surface_Référence_Ha']:.1f} ha")
        
        st.divider()
        
        # 2. Detailed Table
        st.markdown("#### 📜 Liste détaillée")
        st.dataframe(
            df_asso[['ID_Parcelle', 'Culture', 'Variété', 'Surface_Référence_Ha', 'Precedent_Cultural', 'Objectif_Rendement_Qx_Ha', 'Strategie_Travail_Sol']],
            use_container_width=True,
            hide_index=True
        )
        
        # 3. CSV Export
        csv = df_asso.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Télécharger l'assolement (CSV)",
            data=csv,
            file_name=f"Assolement_{campagne_input}.csv",
            mime='text/csv',
        )

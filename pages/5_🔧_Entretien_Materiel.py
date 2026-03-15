import streamlit as st
import pandas as pd
import tempfile
import os
from report_gen import ReportGenerator
from shared import get_dataloader, init_campaign_selector

st.set_page_config(page_title="Entretien Matériel", page_icon="⚙️", layout="centered")

st.title("⚙️ Carnet d'Entretien Matériel")

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

try:
    with st.expander("📄 Générer Carnet d'Entretien", expanded=True):
        with st.spinner("Chargement des matériels..."):
            df_materiels = active_loader.get_materiels()
            
        if df_materiels.empty:
            st.info("Aucun matériel trouvé dans REF_MATERIELS.")
        else:
            materiel_options = []
            materiel_map = {} 
            
            for _, row in df_materiels.iterrows():
                m_id = str(row.get('ID_Materiel', ''))
                marque = str(row.get('Marque', ''))
                modele = str(row.get('Modele', ''))
                
                if m_id:
                    label = f"{m_id} - {marque} {modele}".strip(" -")
                    materiel_options.append(label)
                    materiel_map[label] = row
                    
            if not materiel_options:
                 st.warning("Aucun ID_Materiel valide trouvé.")
            else:
                col_m1, col_m2 = st.columns([2, 1])
                with col_m1:
                    selected_mat_label = st.selectbox("Sélectionnez un matériel", sorted(materiel_options))
                    
                if st.button("📄 Générer PDF"):
                    selected_row = materiel_map[selected_mat_label]
                    m_id = str(selected_row.get('ID_Materiel', ''))
                    
                    with st.spinner(f"Récupération de l'historique pour {m_id}..."):
                        df_history = active_loader.get_maintenance_history(m_id)
                        
                        with tempfile.TemporaryDirectory() as tmpdirname:
                            fname = f"Carnet_Entretien_{m_id}.pdf"
                            fpath = os.path.join(tmpdirname, fname)
                            
                            gen = ReportGenerator(fpath)
                            gen.generate_maintenance_log(selected_row.to_dict(), df_history)
                            
                            if os.path.exists(fpath):
                                with open(fpath, "rb") as f:
                                    st.download_button(
                                        label=f"⬇️ Télécharger Carnet ({m_id})",
                                        data=f,
                                        file_name=fname,
                                        mime="application/pdf",
                                        key=f"dl_maint_{m_id}"
                                    )
                                st.success("Carnet généré avec succès ! Cliquez ci-dessus pour le télécharger.")
                            else:
                                st.error("Échec de la génération du PDF.")

    st.divider()

    # --- SECTION CONSOMMATION FUEL ---
    st.subheader(f"⛽ Consommation Fuel - Campagne {selected_campaign}")
    
    with st.spinner("Analyse de la consommation..."):
        df_fuel = active_loader.get_fuel_conso(selected_campaign)
        df_ref_mat = active_loader.get_materiels()
        
        if df_fuel.empty:
            st.info(f"Aucune donnée de consommation pour la campagne {selected_campaign}.")
        else:
            # Merge to get Type_Materiel
            df_merged = pd.merge(
                df_fuel, 
                df_ref_mat[['ID_Materiel', 'Type_Materiel']], 
                on='ID_Materiel', 
                how='left'
            )
            
            # Fill unknown types
            df_merged['Type_Materiel'] = df_merged['Type_Materiel'].fillna('Autre')
            
            # Aggregation by Material and Type
            df_pivot = df_merged.groupby(['Type_Materiel', 'ID_Materiel'])['FUEL_quantité_L'].sum().reset_index()
            df_pivot.columns = ['Type', 'Matériel', 'Consommation (L)']
            
            # Formattage pour affichage
            df_pivot['Consommation (L)'] = df_pivot['Consommation (L)'].apply(lambda x: f"{x:,.0f} L")
            
            st.dataframe(df_pivot, use_container_width=True, hide_index=True)
            
            # Summary metrics
            st.markdown("### 📊 Récapitulatif par Type")
            type_conso = df_merged.groupby('Type_Materiel')['FUEL_quantité_L'].sum().sort_values(ascending=False)
            
            cols = st.columns(min(len(type_conso), 4))
            for i, (m_type, total) in enumerate(type_conso.items()):
                if i < len(cols):
                    cols[i].metric(m_type, f"{total:,.0f} L")

except Exception as e:
    st.error(f"Erreur lors du traitement du carnet d'entretien : {e}")
    st.exception(e)

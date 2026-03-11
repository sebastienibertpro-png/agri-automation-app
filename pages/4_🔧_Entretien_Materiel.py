import streamlit as st
import pandas as pd
import tempfile
import os
from report_gen import ReportGenerator
from shared import get_dataloader

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

active_loader = get_dataloader()

try:
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
                
            if st.button("📄 Générer Carnet d'Entretien"):
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

except Exception as e:
    st.error(f"Erreur lors du traitement du carnet d'entretien : {e}")

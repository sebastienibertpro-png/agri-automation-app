import streamlit as st
import pandas as pd
from datetime import datetime
import tempfile
import zipfile
import os
from report_gen import ReportGenerator
from shared import init_campaign_selector

st.set_page_config(page_title="Édition de Documents", page_icon="📄", layout="centered")

st.title("📄 Édition de Documents Réglementaires")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

col1, col2 = st.columns(2)
with col1:
    st.info(f"📅 Campagne active : {selected_campaign}")

with col2:
    options = ["Toutes"] + list(available_parcelles)
    selected_parcelle = st.selectbox("🌾 Choisir la Parcelle", options)

target_parcelles = []
if selected_parcelle == "Toutes":
    target_parcelles = list(available_parcelles)
else:
    target_parcelles = [selected_parcelle]

st.markdown("<br>", unsafe_allow_html=True)

def generate_and_download(report_type):
    metadata_map = active_loader.get_parcel_metadata(selected_campaign)
    
    def patch_surface_column(df):
        if 'Surface_Travaillée_Ha' in df.columns:
            df['Surface_Travaillée_Ha'] = df['Surface_Travaillée_Ha'].astype(float)
            mask = df['Surface_Travaillée_Ha'] > 50
            df.loc[mask, 'Surface_Travaillée_Ha'] = df.loc[mask, 'Surface_Travaillée_Ha'] / 100
        return df

    if report_type == "PHYTO":
        df_phyto = df_campaign[df_campaign['Nature_Intervention'] == "Traitement"]
        df_phyto = df_phyto[df_phyto['ID_Parcelle'].isin(target_parcelles)]
        df_phyto = patch_surface_column(df_phyto)
        df_phyto = df_phyto.fillna("") 
        
        grouped_data = {}
        for p in df_phyto['ID_Parcelle'].unique():
            subset = df_phyto[df_phyto['ID_Parcelle'] == p].sort_values(by='Date')
            p_meta = metadata_map.get(p, {})
            grouped_data[p] = {'data': subset.to_dict('records'), 'meta': p_meta}
            
        return grouped_data, "generate_phyto_register", "Registre_Phytosanitaire"

    elif report_type == "FERTI":
        df_ferti = df_campaign[df_campaign['Nature_Intervention'] == "Fertilisation"]
        df_ferti = df_ferti[df_ferti['ID_Parcelle'].isin(target_parcelles)]
        df_ferti = patch_surface_column(df_ferti)
        df_ferti = df_ferti.fillna("") 
        
        grouped_data = {}
        for p in df_ferti['ID_Parcelle'].unique():
            p_meta = metadata_map.get(p, {})
            grouped_data[p] = {
                 'Apports': df_ferti[df_ferti['ID_Parcelle'] == p].to_dict('records'),
                 'Besoins': {'Culture': p_meta.get('Culture', 'Inconnue'), 'Besoin_N': 0, 'Besoin_P': 0, 'Besoin_K': 0},
                 'Sol': {},
                 'meta': p_meta
            }
        return grouped_data, "generate_ferti_balance", "Bilan_Fertilisation"

    elif report_type == "ITK":
        df_itk = df_campaign[df_campaign['ID_Parcelle'].isin(target_parcelles)]
        df_itk = patch_surface_column(df_itk)
        df_itk = df_itk.fillna("") 
        
        grouped_data = {}
        if not df_itk.empty:
            for p in df_itk['ID_Parcelle'].unique():
                 subset = df_itk[df_itk['ID_Parcelle'] == p].sort_values(by='Date')
                 p_meta = metadata_map.get(p, {})
                 cat_data = {'meta': p_meta, 'Travail du sol': [], 'Semis': [], 'Fertilisation': [], 'Traitement': [], 'Récolte': []}
                 for _, row in subset.iterrows():
                     nature = str(row['Nature_Intervention']).strip()
                     record = row.to_dict()
                     if nature in ['Déchaumage', 'Labour', 'Travail du sol']: cat_data['Travail du sol'].append(record)
                     elif nature in ['Semi', 'Semis']: cat_data['Semis'].append(record)
                     elif nature == 'Fertilisation': cat_data['Fertilisation'].append(record)
                     elif nature == 'Traitement': cat_data['Traitement'].append(record)
                     elif nature in ['Récolte', 'Moisson']: cat_data['Récolte'].append(record)
                 grouped_data[p] = cat_data
        return grouped_data, "generate_itk", "Itineraire_Technique"

    elif report_type == "IRRIG_PARCELLE":
        df_irrig = active_loader.get_journal_irrigation()
        if not df_irrig.empty:
            df_irrig['Campagne'] = pd.to_numeric(df_irrig['Campagne'], errors='coerce').fillna(0).astype(int)
            df_irrig = df_irrig[df_irrig['Campagne'] == int(selected_campaign)]
            
            if "Toutes" not in options and target_parcelles:
                df_irrig = df_irrig[df_irrig['ID_Parcelle'].isin(target_parcelles)]
            elif target_parcelles and target_parcelles[0] != "Toutes":
                df_irrig = df_irrig[df_irrig['ID_Parcelle'].isin(target_parcelles)]

            grouped_data = {}
            for p in df_irrig['ID_Parcelle'].unique():
                subset = df_irrig[df_irrig['ID_Parcelle'] == p]
                p_meta = metadata_map.get(p, {})
                grouped_data[p] = {
                     'Irrigations': subset.to_dict('records'),
                     'meta': p_meta
                }
            return grouped_data, "generate_irrigation_parcel_report", "Bilan_Irrig_Parcelle"
        return {}, None, None

    return None, None, None

def handle_pdf_action(report_type, btn_label):
    if st.button(btn_label):
        with st.spinner(f"Génération {report_type}..."):
            data, method_name, prefix = generate_and_download(report_type)
            
            if not data:
                st.warning("Aucune donnée pour cette sélection.")
                return

            with tempfile.TemporaryDirectory() as tmpdirname:
                files = []
                for p_id, p_payload in data.items():
                    safe_pid = str(p_id).replace(" ", "_").replace("/", "-")
                    fname = f"{prefix}_{selected_campaign}_{safe_pid}.pdf"
                    fpath = os.path.join(tmpdirname, fname)
                    
                    gen = ReportGenerator(fpath)
                    method = getattr(gen, method_name)
                    method(selected_campaign, {p_id: p_payload})
                    files.append(fpath)
                
                if not files:
                     st.warning("Rien à générer.")
                     return

                if len(files) == 1:
                    with open(files[0], "rb") as f:
                        st.download_button(
                            label=f"⬇️ Télécharger PDF ({report_type})",
                            data=f,
                            file_name=os.path.basename(files[0]),
                            mime="application/pdf",
                            key=f"dl_{report_type}"
                        )
                else:
                    zip_name = f"{prefix}_Campagne_{selected_campaign}.zip"
                    zip_path = os.path.join(tmpdirname, zip_name)
                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        for file in files:
                            zipf.write(file, os.path.basename(file))
                    
                    with open(zip_path, "rb") as f:
                        st.download_button(
                            label=f"⬇️ Télécharger ZIP ({report_type})",
                            data=f,
                            file_name=zip_name,
                            mime="application/zip",
                             key=f"dl_{report_type}_zip"
                        )
        st.success("Génération terminée ! Cliquez ci-dessus pour télécharger.")

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

col_pdf1, col_pdf2, col_pdf3, col_pdf4 = st.columns(4)

with col_pdf1:
    handle_pdf_action("ITK", "📄 Itinéraire Technique")
with col_pdf2:
    handle_pdf_action("PHYTO", "🛡️ Registre Phyto")
with col_pdf3:
    handle_pdf_action("FERTI", "🧪 Bilan Ferti")
with col_pdf4:
    handle_pdf_action("IRRIG_PARCELLE", "💧 Bilan Irrig Parcelle")

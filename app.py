import streamlit as st
import pandas as pd
import os
from data_loader import DataLoader
from report_gen import ReportGenerator
import json
from datetime import datetime

# Page Configuration
st.set_page_config(
    page_title="Agri Automation",
    page_icon="🚜",
    layout="centered"
)

# Custom CSS for aesthetics
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
    h1 {
        color: #2E7D32;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚜 Agri Automation")
st.markdown("Outil de génération de rapports : **ITK**, **Ferti** et **Registre Phyto**.")

# --- Authentication & Setup ---
# Load Credentials from Secrets (Streamlit Cloud) or Local File
credentials_dict = None
if "gcp_service_account" in st.secrets:
    credentials_dict = dict(st.secrets["gcp_service_account"])

# Initialize DataLoader
# We use a dummy file path because we prioritize Cloud, but logic needs a path argument
loader = DataLoader("dummy_path.xlsx", use_cloud=True, credentials_dict=credentials_dict)

# Cache data loading to avoid re-fetching on every interaction
def load_data():
    if loader.load_source():
        return loader
    return None

with st.spinner('Connexion à Google Sheets...'):
    active_loader = load_data()

if not active_loader:
    st.error("Impossible de se connecter à 'MASTER_EXPLOITATION'. Vérifiez vos secrets ou votre connexion.")
    st.stop()

# --- Campaigns ---
try:
    df_intervention = active_loader.get_interventions()
    if df_intervention.empty:
         st.warning("Aucune donnée d'intervention trouvée.")
         st.stop()
         
    # Clean Campaign Column
    df_intervention['Campagne'] = pd.to_numeric(df_intervention['Campagne'], errors='coerce').fillna(0).astype(int)
    available_campaigns = sorted(df_intervention[df_intervention['Campagne'] > 0]['Campagne'].unique(), reverse=True)
except Exception as e:
    st.error(f"Erreur lecture campagnes: {e}")
    st.stop()

# Sidebar / Top inputs
col1, col2 = st.columns(2)
with col1:
    selected_campaign = st.selectbox("📅 Choisir la Campagne", available_campaigns)

# Filter Parcels for Campaign
df_campaign = df_intervention[df_intervention['Campagne'].astype(str) == str(selected_campaign)]
available_parcelles = sorted(df_campaign['ID_Parcelle'].unique())

with col2:
    # Add 'Toutes' option
    options = ["Toutes"] + list(available_parcelles)
    selected_parcelle = st.selectbox("🌾 Choisir la Parcelle", options)

target_parcelles = []
if selected_parcelle == "Toutes":
    target_parcelles = list(available_parcelles)
else:
    target_parcelles = [selected_parcelle]

# --- Generation Section ---

# --- QR Action Logic (Must be at top) ---
# Check for 'validate_phyto' in query params
q_params = st.query_params
if "validate_phyto" in q_params:
    intervention_id = q_params["validate_phyto"]
    st.info(f"🔍 Scan détecté pour l'intervention : {intervention_id}")
    
    if st.button("✅ Confirmer : Traitement RÉALISÉ"):
        with st.spinner("Mise à jour du statut..."):
            success = loader.update_intervention_status(intervention_id, "Réalisé")
            if success:
                st.success("Statut mis à jour avec succès ! Vous pouvez fermer.")
                # Clear param to avoid re-trigger on reload? 
                # st.experimental_set_query_params() # Deprecated/removed in new Streamlit
                # Just show success message.
            else:
                st.error("Échec de la mise à jour (Vérifiez les logs ou la connexion).")
    st.divider()


# --- Generation Section ---
st.divider()
st.subheader("📄 Génération de Rapports")


# --- FICHE PREPARATION PHYTO ---
st.markdown("#### 🧪 Fiche de Préparation Phyto")
try:
    df_planned = loader.get_planned_treatments(selected_campaign)
    
    if not df_planned.empty:
        # Filter by selected parcelle if not "Toutes"
        if selected_parcelle != "Toutes":
            df_planned = df_planned[df_planned['ID_Parcelle'] == selected_parcelle]
            
        if df_planned.empty:
             st.info("Aucune intervention 'Prévue' pour cette sélection.")
        else:
             # Group by Date + Parcelle to form unique "Mixes"
             # We need a selector.
             # Create a list of options: "YYYY-MM-DD - Parcelle (X produits)"
             
             # Grouping
             # Key: (DateStr, Parcelle)
             # Value: List of rows
             mixes = {}
             for _, row in df_planned.iterrows():
                 d_val = row['Date']
                 d_str = d_val.strftime('%Y-%m-%d') if not pd.isnull(d_val) else "Date Inconnue"
                 p_id = row['ID_Parcelle']
                 key = (d_str, p_id)
                 
                 if key not in mixes: mixes[key] = []
                 mixes[key].append(row)
             
             # Create Options
             mix_options = []
             mix_map = {}
             for k, rows in mixes.items():
                 date_lbl, p_lbl = k
                 nb_p = len(rows)
                 label = f"{date_lbl} - {p_lbl} ({nb_p} produits)"
                 mix_options.append(label)
                 mix_map[label] = (k, rows)
             
             mix_options = sorted(mix_options, reverse=True)
             
             col_p1, col_p2 = st.columns([2, 1])
             with col_p1:
                selected_mix_lbl = st.selectbox("Choisir l'intervention prévue :", mix_options)
             with col_p2:
                vol_ha_input = st.number_input("Volume Bouillie (L/ha)", value=150, step=10)
             
             if st.button("Générer Fiche Préparation"):
                 # Prepare Data
                 key, rows = mix_map[selected_mix_lbl]
                 date_str, p_id = key
                 
                 # Get Metadata (Surface)
                 # We need efficient way. usage of loader.get_parcel_metadata might be slow if fetching all?
                 # Let's fetch specific or rely on cached.
                 # Actually we can use the existing `active_loader` method but it gets ALL for campaign.
                 # Optimized: Just get what we need. 
                 # Or assume `metadata_map` from report logic is available? It's inside a function.
                 # Let's call it here.
                 meta_map = active_loader.get_parcel_metadata(selected_campaign)
                 p_meta = meta_map.get(p_id, {})
                 surface = float(p_meta.get('Surface', 0))
                 
                 # Prepare Products List
                 prods = []
                 for r in rows:
                     prods.append(r.to_dict())
                 
                 # Sort
                 sorted_prods = loader.sort_products_by_formulation(prods)
                 
                 # Parse Date
                 date_obj = rows[0]['Date'] # Take first
                 
                 # ID for QR
                 # "PARCELLE_YYYYMMDD"
                 clean_date = date_str.replace("-", "")
                 intervention_id = f"{p_id}_{clean_date}"
                 
                 payload = {
                     'Parcelle': p_id,
                     'Surface': surface,
                     'Date': date_obj,
                     'Volume_Bouillie_Ha': vol_ha_input,
                     'Products': sorted_prods,
                     'Intervention_ID': intervention_id
                 }
                 
                 # Generate
                 with tempfile.TemporaryDirectory() as tmpdirname:
                     fname = f"Fiche_Prep_{intervention_id}.pdf"
                     fpath = os.path.join(tmpdirname, fname)
                     
                     gen = ReportGenerator(fpath)
                     gen.generate_prep_sheet(selected_campaign, payload)
                     
                     with open(fpath, "rb") as f:
                        st.download_button(
                            label="⬇️ Télécharger Fiche",
                            data=f,
                            file_name=fname,
                            mime="application/pdf"
                        )
                 st.success("Fiche générée ! Vérifiez l'ordre d'incorporation.")
                 
    else:
        st.info("Pas d'interventions planifiées trouvées pour cette campagne.")
except Exception as e:
    st.error(f"Erreur chargement planning: {e}")

st.divider()

# Helper for PDF Generation
def generate_and_download(report_type):
    # Prepare Data
    metadata_map = active_loader.get_parcel_metadata(selected_campaign)
    
    # Logic copied/adapted from main.py
    # Ideally should be refactored into a Controller class, but we keep it simple here.
    
    timestamp = datetime.now().strftime('%H%M%S')
    
    # We will generate ONE merged PDF or multiple?
    # Web context: Better to ZIP if multiple, or just generate one specific PDF if single parcelle.
    # If "All", maybe ZIP.
    # For now, let's keep it simple: Single PDF if single parcelle, Zip if multiple.
    # OR: Just one PDF merging pages? ReportGen generates one file. We can append pages?
    # Current ReportGenerator class creates a NEW file each init.
    
    zip_buffer = None
    files_generated = []
    
    # Patch Surface (Same logic as main.py)
    def patch_surface_column(df):
        if 'Surface_Travaillée_Ha' in df.columns:
            df['Surface_Travaillée_Ha'] = df['Surface_Travaillée_Ha'].astype(float)
            mask = df['Surface_Travaillée_Ha'] > 50
            df.loc[mask, 'Surface_Travaillée_Ha'] = df.loc[mask, 'Surface_Travaillée_Ha'] / 100
        return df

    # --- PHYTO ---
    if report_type == "PHYTO":
        df_phyto = df_campaign[df_campaign['Nature_Intervention'] == "Traitement"]
        df_phyto = df_phyto[df_phyto['ID_Parcelle'].isin(target_parcelles)]
        df_phyto = patch_surface_column(df_phyto)
        df_phyto = df_phyto.fillna("") # Clean NaNs
        
        grouped_data = {}
        for p in df_phyto['ID_Parcelle'].unique():
            subset = df_phyto[df_phyto['ID_Parcelle'] == p].sort_values(by='Date')
            p_meta = metadata_map.get(p, {})
            grouped_data[p] = {'data': subset.to_dict('records'), 'meta': p_meta}
            
        return grouped_data, "generate_phyto_register", "Registre_Phytosanitaire"

    # --- FERTI ---
    elif report_type == "FERTI":
        df_ferti = df_campaign[df_campaign['Nature_Intervention'] == "Fertilisation"]
        df_ferti = df_ferti[df_ferti['ID_Parcelle'].isin(target_parcelles)]
        df_ferti = patch_surface_column(df_ferti)
        df_ferti = df_ferti.fillna("") # Clean NaNs
        
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

    # --- ITK ---
    elif report_type == "ITK":
        df_itk = df_campaign[df_campaign['ID_Parcelle'].isin(target_parcelles)]
        df_itk = patch_surface_column(df_itk)
        df_itk = df_itk.fillna("") # Clean NaNs
        
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

    return None, None, None

# UI for generation
col_pdf1, col_pdf2, col_pdf3 = st.columns(3)

import tempfile
import zipfile

def handle_pdf_action(report_type, btn_label):
    if st.button(btn_label):
        with st.spinner(f"Génération {report_type}..."):
            data, method_name, prefix = generate_and_download(report_type)
            
            if not data:
                st.warning("Aucune donnée pour cette sélection.")
                return

            # Create temp directory
            with tempfile.TemporaryDirectory() as tmpdirname:
                files = []
                for p_id, p_payload in data.items():
                    safe_pid = str(p_id).replace(" ", "_").replace("/", "-")
                    fname = f"{prefix}_{selected_campaign}_{safe_pid}.pdf"
                    fpath = os.path.join(tmpdirname, fname)
                    
                    gen = ReportGenerator(fpath)
                    # Call method dynamically
                    method = getattr(gen, method_name)
                    method(selected_campaign, {p_id: p_payload})
                    files.append(fpath)
                
                if not files:
                     st.warning("Rien à générer.")
                     return

                # If single file -> Direct Download
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
                    # If multiple -> Zip
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

with col_pdf1:
    handle_pdf_action("ITK", "📄 Itinéraire Technique")
with col_pdf2:
    handle_pdf_action("PHYTO", "🛡️ Registre Phyto")
with col_pdf3:
    handle_pdf_action("FERTI", "🧪 Bilan Ferti")

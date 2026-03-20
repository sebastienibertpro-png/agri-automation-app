import streamlit as st
import pandas as pd
from data_loader import DataLoader
import os

# Helper for E-Phy cloud storage
APP_BASE_URL = "https://agri-automation-app-kwz7hjkyb8hjxwhe9w7rsv.streamlit.app"
EPHY_DRIVE_FOLDER_ID = "1YDTwRXHFTxPmM4QD84nTnQYmMZqz60dc"
OBSERVATION_DRIVE_FOLDER_ID = "1_oaKK3W_YfgAQ9UPS9AkcmmIH-eZEfci"

@st.cache_resource
def get_dataloader():
    credentials_dict = None
    if "gcp_service_account" in st.secrets:
        credentials_dict = dict(st.secrets["gcp_service_account"])

    loader = DataLoader("dummy_path.xlsx", use_cloud=True, credentials_dict=credentials_dict)
    if loader.load_source():
        return loader
    return None

@st.cache_resource
def get_drive_uploader():
    from drive_utils import DriveUploader
    credentials_dict = None
    if "gcp_service_account" in st.secrets:
        credentials_dict = dict(st.secrets["gcp_service_account"])
    
    # On privilégie credentials.json si présent, sinon le dictionnaire des secrets
    cred_path = "credentials.json"
    if not os.path.exists(cred_path):
        cred_path = None
        
    uploader = DriveUploader(credentials_path=cred_path, credentials_dict=credentials_dict)
    return uploader if uploader.service else None

# Lazy check for loader capabilities
def get_active_loader():
    dl = get_dataloader()
    if dl and not hasattr(dl, "load_telepac_from_cloud"):
        st.cache_resource.clear()
        return get_dataloader()
    return dl

active_loader = get_active_loader()

def init_campaign_selector():
    if not active_loader:
        st.error("Impossible de se connecter à 'MASTER_EXPLOITATION'. Vérifiez vos secrets ou votre connexion.")
        st.stop()
        
    try:
        # Avoid caching interventions too strictly during active data entry, or use session state cache
        df_intervention = active_loader.get_interventions()
        df_releves = active_loader.get_releves_compteurs()
        
        years = set()
        
        if not df_intervention.empty:
            df_intervention['Campagne'] = pd.to_numeric(df_intervention['Campagne'], errors='coerce').fillna(0).astype(int)
            years.update(df_intervention[df_intervention['Campagne'] > 0]['Campagne'].unique())
        
        if not df_releves.empty:
            df_releves['Date_Relevé'] = pd.to_datetime(df_releves['Date_Relevé'], errors='coerce', dayfirst=True)
            years.update(df_releves['Date_Relevé'].dt.year.dropna().unique())
            
        available_campaigns = sorted([int(y) for y in years], reverse=True)
        
        if not available_campaigns:
            st.warning("Aucune donnée (intervention ou relevé) trouvée.")
            st.stop()
            
        selected_campaign = st.sidebar.selectbox("📅 Choisir la Campagne", available_campaigns)
        
        df_campaign = df_intervention[df_intervention['Campagne'].astype(str) == str(selected_campaign)]
        available_parcelles = sorted(df_campaign['ID_Parcelle'].unique())
        
        return active_loader, selected_campaign, df_campaign, available_parcelles
    except Exception as e:
        st.error(f"Erreur lecture campagnes: {e}")
        st.stop()

def inject_premium_css():
    st.markdown("""
<style>
    /* Styling for st.data_editor and st.dataframe containers */
    [data-testid="stDataEditor"], [data-testid="stDataFrame"] {
        border-radius: 12px;
        border: 1px solid #e0e0e0;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
        padding: 5px;
        background-color: white;
    }
    
    /* Premium HTML Table Style */
    .premium-table { 
        border-collapse: collapse; 
        margin: 10px 0; 
        font-size: 0.9em; 
        font-family: inherit; 
        width: 100%; 
        border-radius: 12px; 
        overflow: hidden; 
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); 
    }
    .premium-table thead tr { 
        background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%); 
        color: #ffffff !important; 
        text-align: left; 
        font-weight: bold; 
    }
    .premium-table.blue thead tr { 
        background: linear-gradient(135deg, #0d47a1 0%, #1976d2 100%); 
    }
    .premium-table th, .premium-table td { 
        padding: 12px 15px; 
        border: 1px solid #f2f2f2; 
        text-align: left;
    }
    .premium-table tbody tr { 
        border-bottom: 1px solid #dddddd; 
    }
    .premium-table tbody tr:nth-of-type(even) { 
        background-color: #f8f9fb; 
    }
    .premium-table tbody tr:last-of-type { 
        border-bottom: 3px solid #2e7d32; 
    }
    .premium-table.blue tbody tr:last-of-type { 
        border-bottom: 3px solid #1976d2; 
    }
    .premium-table tbody tr:hover { 
        background-color: #f1f8e9; 
    }
    .premium-table.blue tbody tr:hover { 
        background-color: #e3f2fd; 
    }
    .premium-table a {
        color: inherit;
        text-decoration: underline;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

def render_premium_table(df, color="green"):
    cls = "premium-table" if color == "green" else "premium-table blue"
    st.write(df.to_html(escape=False, index=False, classes=cls), unsafe_allow_html=True)


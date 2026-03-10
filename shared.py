import streamlit as st
import pandas as pd
from data_loader import DataLoader
import os

APP_BASE_URL = "https://agri-automation-app-kwz7hjkyb8hjxwhe9w7rsv.streamlit.app"

@st.cache_resource
def get_dataloader():
    credentials_dict = None
    if "gcp_service_account" in st.secrets:
        credentials_dict = dict(st.secrets["gcp_service_account"])

    loader = DataLoader("dummy_path.xlsx", use_cloud=True, credentials_dict=credentials_dict)
    if loader.load_source():
        return loader
    return None

# Hack to force reload of cached dataloader if it lacks new methods
dl = get_dataloader()
if dl and not hasattr(dl, "load_telepac_from_cloud"):
    st.cache_resource.clear()
    active_loader = get_dataloader()
else:
    active_loader = dl

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

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
    /* Global Overrides for Streamlit Widgets */
    [data-testid="stDataEditor"], [data-testid="stDataFrame"] {
        border-radius: 12px !important;
        border: 1px solid #e0e0e0 !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08) !important;
        padding: 4px !important;
        background-color: white !important;
        margin-top: -1px !important; /* To glue with the header */
    }
    
    /* Dedicated Header Styling */
    .p-header {
        color: white !important;
        padding: 12px 18px !important;
        font-weight: bold !important;
        font-size: 1.1em !important;
        display: flex !important;
        justify-content: space-between !important;
        align-items: center !important;
        border-radius: 12px 12px 0 0 !important;
        margin-bottom: 0 !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important;
    }
    .p-green { background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%) !important; }
    .p-blue { background: linear-gradient(135deg, #0d47a1 0%, #1976d2 100%) !important; }
</style>
""", unsafe_allow_html=True)

def render_premium_header(title, subtitle="", color="green"):
    cls = "p-header p-green" if color == "green" else "p-header p-blue"
    st.markdown(f'<div class="{cls}"><span>{title}</span><span style="font-size: 0.7em; opacity: 0.8; font-weight: normal;">{subtitle}</span></div>', unsafe_allow_html=True)

def render_premium_table(df, color="green", compact=False):
    """Render a DataFrame as a styled HTML table using fully inline CSS (no style tags).
    This is necessary because Streamlit Cloud sanitizes <style> tags in st.markdown.
    """
    header_bg = "linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%)" if color == "green" else "linear-gradient(135deg, #0d47a1 0%, #1976d2 100%)"
    border_color = "#2e7d32" if color == "green" else "#1976d2"

    pad = "6px 10px" if compact else "12px 15px"
    font_size = "0.8em" if compact else "0.9em"
    nowrap = "white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px;" if compact else ""

    th_style = f'style="padding:{pad}; border:1px solid #eee; background:{header_bg}; color:#ffffff; font-weight:bold; text-align:left; white-space:nowrap;"'
    td_style = f'style="padding:{pad}; border:1px solid #eee; text-align:left; {nowrap}"'
    td_even_style = f'style="padding:{pad}; border:1px solid #eee; text-align:left; background-color:#f8f9fb; {nowrap}"'
    td_last_style = f'style="padding:{pad}; border:1px solid #eee; text-align:left; border-bottom:3px solid {border_color}; {nowrap}"'
    td_last_even_style = f'style="padding:{pad}; border:1px solid #eee; text-align:left; background-color:#f8f9fb; border-bottom:3px solid {border_color}; {nowrap}"'

    table_style = f'style="border-collapse:collapse; width:100%; border-radius:8px; overflow:hidden; box-shadow:0 4px 15px rgba(0,0,0,0.1); font-size:{font_size}; font-family:inherit; margin:4px 0;"'

    rows = list(df.itertuples(index=False, name=None))
    n = len(rows)

    html = f"<table {table_style}><thead><tr>"
    for col in df.columns:
        html += f"<th {th_style}>{col}</th>"
    html += "</tr></thead><tbody>"

    for i, row in enumerate(rows):
        is_last = (i == n - 1)
        is_even = (i % 2 == 1)
        html += "<tr>"
        for j, cell in enumerate(row):
            if is_last and is_even:
                style = td_last_even_style
            elif is_last:
                style = td_last_style
            elif is_even:
                style = td_even_style
            else:
                style = td_style
            html += f"<td {style}>{cell}</td>"
        html += "</tr>"

    html += "</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)



import streamlit as st
import pandas as pd
from data_loader import DataLoader
import os

# Helper for E-Phy cloud storage
APP_BASE_URL = "https://agri-automation-app-kwz7hjkyb8hjxwhe9w7rsv.streamlit.app"
EPHY_DRIVE_FOLDER_ID = "1fMnmAMoGWVTIFaR2yOTbD7EMdBNuyH3Z"
OBSERVATION_DRIVE_FOLDER_ID = "1_oaKK3W_YfgAQ9UPS9AkcmmIH-eZEfci"

def get_dataloader():
    """Crée un DataLoader frais. Pas de cache_resource pour que le TTL de conn.read() fonctionne."""
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

def get_active_loader():
    """Retourne un DataLoader. Utilise session_state pour éviter une recréation totale à chaque widget,
    mais sans bloquer le rafraîchissement des données (TTL géré par st.connection en interne)."""
    if "_dataloader" not in st.session_state or st.session_state["_dataloader"] is None:
        st.session_state["_dataloader"] = get_dataloader()
    return st.session_state["_dataloader"]

def get_fresh_loader():
    """Force la recréation d'un DataLoader, utile après une écriture."""
    st.session_state["_dataloader"] = get_dataloader()
    return st.session_state["_dataloader"]

active_loader = get_active_loader()

def init_campaign_selector():
    if not active_loader:
        st.error("Impossible de se connecter à 'MASTER_EXPLOITATION'. Vérifiez vos secrets ou votre connexion.")
        st.stop()

    # ── CSS sidebar uniforme sur toutes les pages ──────────────────────────────
    st.markdown("""
    <style>
        /* Wildcard max-spécificité : écrase les règles internes Streamlit */
        section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] *,
        section[data-testid="stSidebar"] [data-testid="stSidebarNav"] * {
            font-size: 15px !important;
            font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif !important;
        }
        /* Page active : vert gras */
        section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li[aria-selected="true"] * {
            font-weight: 700 !important;
            color: #2e7d32 !important;
        }
    </style>
    """, unsafe_allow_html=True)

    try:
        df_intervention = active_loader.get_interventions()
        df_releves = active_loader.get_releves_compteurs()

        years = set()

        if not df_intervention.empty:
            df_intervention['Campagne'] = pd.to_numeric(df_intervention['Campagne'], errors='coerce').fillna(0).astype(int)
            years.update(df_intervention[df_intervention['Campagne'] > 0]['Campagne'].unique())

        if not df_releves.empty:
            df_releves['Date_Relevé'] = pd.to_datetime(df_releves['Date_Relevé'], errors='coerce', dayfirst=True)
            years.update(df_releves['Date_Relevé'].dt.year.dropna().unique())

        # Also include years already present in ASSOLEMENT (campaign created but no intervention yet)
        try:
            df_asso_all = active_loader.get_assolement()
            if not df_asso_all.empty and 'Campagne' in df_asso_all.columns:
                asso_years = pd.to_numeric(df_asso_all['Campagne'], errors='coerce').dropna().astype(int)
                years.update(asso_years[asso_years > 0].unique())
        except Exception:
            pass

        import datetime
        current_year = datetime.datetime.now().year
        next_campaign = (max(int(y) for y in years) + 1) if years else current_year

        available_campaigns = sorted([int(y) for y in years], reverse=True)

        # Sentinel value used in the selectbox for the "create" option
        NEW_CAMP_LABEL = f"➕ Nouvelle campagne ({next_campaign})"
        options_display = [NEW_CAMP_LABEL] + [str(y) for y in available_campaigns]

        # Restore previous selection if it exists in session_state
        default_idx = 0
        if "selected_campaign_label" in st.session_state:
            prev = st.session_state["selected_campaign_label"]
            if prev in options_display:
                default_idx = options_display.index(prev)

        chosen = st.sidebar.selectbox(
            "📅 Choisir la Campagne",
            options=options_display,
            index=default_idx,
            key="campaign_selectbox"
        )
        st.session_state["selected_campaign_label"] = chosen

        if chosen == NEW_CAMP_LABEL:
            # ── Creation mode ──────────────────────────────────────────
            st.session_state["creating_new_campaign"] = True
            st.session_state["new_campaign_year"] = next_campaign

            # Use the last known year as fallback for pages that need a campaign
            fallback_year = available_campaigns[0] if available_campaigns else next_campaign
            selected_campaign = fallback_year

            # Show hint in sidebar
            st.sidebar.info(
                f"ℹ️ Vous créez la campagne **{next_campaign}**.\n\n"
                "Rendez-vous sur la page **🌾 Assolement** pour saisir votre plan de culture."
            )
        else:
            st.session_state["creating_new_campaign"] = False
            st.session_state["new_campaign_year"] = None
            selected_campaign = int(chosen)

        if not available_campaigns and not st.session_state.get("creating_new_campaign"):
            st.warning("Aucune donnée (intervention ou relevé) trouvée.")
            st.stop()

        df_campaign = df_intervention[df_intervention['Campagne'].astype(str) == str(selected_campaign)] \
            if not df_intervention.empty else pd.DataFrame()
        available_parcelles = sorted(df_campaign['ID_Parcelle'].unique()) if not df_campaign.empty else []

        return active_loader, selected_campaign, df_campaign, available_parcelles

    except Exception as e:
        st.error(f"Erreur lecture campagnes: {e}")
        st.stop()

def inject_premium_css():
    st.markdown("""
<style>
    /* Global Overrides for Streamlit Widgets */
    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
        border-radius: 0 0 12px 12px !important; /* Coins bas arrondis uniquement pour fusionner avec le titre */
        border: 1px solid #e0e0e0 !important;
        border-top: none !important; /* Le bandeau coloré du dessus fait la bordure */
        box-shadow: 0 10px 25px rgba(0, 0, 0, 0.15) !important;
        padding: 4px !important;
        background-color: white !important;
        margin-top: 0px !important;
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



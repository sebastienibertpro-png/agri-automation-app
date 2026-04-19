import streamlit as st
import pandas as pd
from data_loader import DataLoader
import os

# Helper for E-Phy cloud storage
APP_BASE_URL = "https://agri-automation-app-kwz7hjkyb8hjxwhe9w7rsv.streamlit.app"
EPHY_DRIVE_FOLDER_ID = "1fMnmAMoGWVTIFaR2yOTbD7EMdBNuyH3Z"
OBSERVATION_DRIVE_FOLDER_ID = "1_oaKK3W_YfgAQ9UPS9AkcmmIH-eZEfci"

def inject_premium_css():
    st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');

    /* Typographie : Ciblage chirurgical pour éviter de casser les ligatures des icônes Material Symbols */
    
    /* Titres principaux (Toutes les tailles) */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 700 !important;
    }
    
    /* Paragraphes standards et libellés (sans toucher aux span ou summary internes de Streamlit) */
    .stMarkdown p, label, .stMetricValue > div, .stMetricLabel > div {
        font-family: 'Outfit', sans-serif !important;
    }

    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
        border-radius: 0 0 12px 12px !important;
        border: 1px solid #e0e0e0 !important;
        border-top: none !important;
        box-shadow: 0 10px 25px rgba(0, 0, 0, 0.12) !important;
        padding: 4px !important;
        background-color: white !important;
        margin-top: 0px !important;
    }
    
    /* Multiselect Tags (Filtres) : Passer en vert au lieu du rouge par défaut */
    span[data-baseweb="tag"] {
        background-color: #5E9E47 !important;
        color: white !important;
    }
    span[data-baseweb="tag"] svg {
        fill: white !important;
    }
    
    /* Dedicated Header Styling - AgriDiA Colors */
    .p-header {
        color: white !important;
        padding: 12px 18px !important;
        font-weight: 600 !important;
        font-size: 1.15em !important;
        display: flex !important;
        justify-content: space-between !important;
        align-items: center !important;
        border-radius: 12px 12px 0 0 !important;
        margin-bottom: 0 !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
        font-family: 'Outfit', sans-serif !important;
    }
    .p-green { background: linear-gradient(135deg, #5E9E47 0%, #4a8037 100%) !important; }
    .p-blue { background: linear-gradient(135deg, #2F6D89 0%, #1a4153 100%) !important; }

    /* ── Masquage "NUCLÉAIRE" des indicateurs système ── */
    /* On cible tout ce qui ressemble à un widget de statut, notification ou toast */
    [data-testid="stStatusWidget"],
    [data-testid="stEventStatusItem"],
    [data-testid="stToastContainer"],
    [data-testid="stStatusWidgetSpinner"],
    [data-testid="stNotification"],
    .stStatusWidget,
    .stNotification,
    .stToast,
    header [data-testid="stStatusWidget"],
    .stApp > header,
    .stDecoration {
        display: none !important;
        visibility: hidden !important;
        height: 0 !important;
        width: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
        opacity: 0 !important;
        pointer-events: none !important;
        transform: scale(0) !important;
        position: fixed !important;
    }
    
    /* Ciblage spécifique du texte de "Running..." qui affiche le code */
    div:has(code), div:has(samp) {
        /* On ne masque pas tout, seulement ce qui est dans un widget de statut */
    }
    
    [data-testid="stStatusWidget"] code {
        display: none !important;
    }
    
    /* ── Animation de Chargement Premium AgriDiA ── */
    .agridia-loader-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 40px;
        background: transparent;
        border-radius: 20px;
        margin: 20px auto;
        text-align: center;
        z-index: 9999;
    }

    .agridia-pulse {
        width: 100px;
        height: 100px;
        background: linear-gradient(135deg, #2F6D89 0%, #5E9E47 100%);
        border-radius: 50%;
        margin-bottom: 25px;
        animation: agridiaPulse 1.5s infinite cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 0 30px rgba(94, 158, 71, 0.2);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 40px;
        color: white;
    }

    @keyframes agridiaPulse {
        0% { transform: scale(0.8); opacity: 0.5; box-shadow: 0 0 0 0 rgba(94, 158, 71, 0.4); }
        50% { transform: scale(1); opacity: 1; box-shadow: 0 0 0 25px rgba(94, 158, 71, 0); }
        100% { transform: scale(0.8); opacity: 0.5; box-shadow: 0 0 0 0 rgba(94, 158, 71, 0); }
    }

    .loader-text {
        font-family: 'Outfit', sans-serif;
        color: #2F6D89;
        font-weight: 700;
        font-size: 1.4em;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        animation: agridiaFade 1.5s infinite ease-in-out;
    }

    @keyframes agridiaFade {
        0%, 100% { opacity: 0.6; }
        50% { opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

import contextlib

@contextlib.contextmanager
def brand_spinner(text="Chargement Agridia..."):
    """
    Un spinner premium aux couleurs de la marque qui remplace le st.spinner par défaut.
    """
    placeholder = st.empty()
    with placeholder.container():
        st.markdown(f"""
            <div class="agridia-loader-container">
                <div class="agridia-pulse">🌱</div>
                <div class="loader-text">{text}</div>
            </div>
        """, unsafe_allow_html=True)
    try:
        yield
    finally:
        placeholder.empty()


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
    # On injecte le CSS immédiatement pour masquer les "Running" de la connexion GSheets
    inject_premium_css()
    
    if "_dataloader" not in st.session_state or st.session_state["_dataloader"] is None:
        st.session_state["_dataloader"] = get_dataloader()
    return st.session_state["_dataloader"]

def get_fresh_loader():
    """
    Force la recréation d'un DataLoader en supprimant l'instance en session_state.
    À appeler AVANT st.rerun() après une écriture pour que le prochain rendu
    obtienne une connexion GSheets fraîche sans cache TTL résiduel.
    """
    # Invalider l'instance existante
    st.session_state.pop("_dataloader", None)
    # Créer immédiatement un nouveau loader frais
    loader = get_dataloader()
    st.session_state["_dataloader"] = loader
    return loader

active_loader = get_active_loader()


def init_campaign_selector():
    if not active_loader:
        st.error("Impossible de se connecter à 'MASTER_EXPLOITATION'. Vérifiez vos secrets ou votre connexion.")
        st.stop()

    # ── CSS global et sidebar uniforme ─────────────────────────────────────────
    st.markdown("""
    <style>
        /* Ajustement de la taille de texte dans la sidebar (les polices sont gérées par inject_premium_css) */
        section[data-testid="stSidebar"] {
            font-size: 1rem !important;
        }
        [data-testid="stSidebarNav"] span,
        [data-testid="stSidebarNav"] a,
        [data-testid="stSidebarNav"] li div {
            font-size: 1rem !important;
        }
        /* Style des boutons dans la sidebar */
        section[data-testid="stSidebar"] .stButton>button {
            border-radius: 8px !important;
            font-weight: 600 !important;
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

        # ── Initialisation du widget selectbox avant création ───────────────────────
        # La première vraie campagne (index 1) est le défaut absolu
        first_real = options_display[1] if len(options_display) > 1 else options_display[0]

        if "campaign_selectbox" not in st.session_state:
            # Premier chargement (refresh navigateur) : choisir la dernière campagne
            st.session_state["campaign_selectbox"] = first_real
        elif (
            st.session_state["campaign_selectbox"] == NEW_CAMP_LABEL
            and not st.session_state.get("creating_new_campaign", False)
        ):
            # L'utilisateur n'est plus en mode création mais le widget est bloqué
            st.session_state["campaign_selectbox"] = first_real

        chosen = st.sidebar.selectbox(
            "📅 Choisir la Campagne",
            options=options_display,
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



def render_brand_page_header(title, subtitle="", icon=""):
    """Rendu d'un en-tête de page premium aux couleurs AgriDiA."""
    icon_html = f'<span style="margin-right: 15px;">{icon}</span>' if icon else ""
    subtitle_html = f'<p style="color: #5E9E47; font-family: \'Outfit\', sans-serif; font-weight: 500; font-size: 1.0em; margin: 5px 0 0 0;">{subtitle}</p>' if subtitle else ""
    st.markdown(f"""
        <div style="margin-bottom: 25px;">
            <div style="display: flex; align-items: center;">
                <h1 style="color: #2F6D89; font-family: 'Outfit', sans-serif; font-weight: 700; margin: 0; font-size: 2.8em; line-height: 1.2;">
                    {icon_html}{title}
                </h1>
            </div>
            {subtitle_html}
            <div style="height: 3px; width: 60px; background: linear-gradient(90deg, #2F6D89 0%, #5E9E47 100%); margin-top: 8px; border-radius: 2px;"></div>
        </div>
    """, unsafe_allow_html=True)

def render_premium_header(title, subtitle="", color="green"):
    cls = "p-header p-green" if color == "green" else "p-header p-blue"
    st.markdown(f'<div class="{cls}"><span>{title}</span><span style="font-size: 0.7em; opacity: 0.8; font-weight: normal;">{subtitle}</span></div>', unsafe_allow_html=True)

def render_premium_table(df, color="green", compact=False):
    """Render a DataFrame as a styled HTML table using fully inline CSS (no style tags).
    This is necessary because Streamlit Cloud sanitizes <style> tags in st.markdown.
    """
    header_bg = "linear-gradient(135deg, #5E9E47 0%, #4a8037 100%)" if color == "green" else "linear-gradient(135deg, #2F6D89 0%, #1a4153 100%)"
    border_color = "#5E9E47" if color == "green" else "#2F6D89"

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



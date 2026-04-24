import streamlit as st
import os
import pandas as pd
from shared import init_campaign_selector, render_brand_page_header
from meteus_utils import display_meteo_module
import requests

try:
    from streamlit_lottie import st_lottie
    LOTTIE_AVAILABLE = True
except ImportError:
    LOTTIE_AVAILABLE = False

st.set_page_config(page_title="Tableau de Bord", page_icon="🚜", layout="wide")

# --- Lottie Loader ---
@st.cache_data(ttl=3600)
def load_lottie_url(url: str):
    """Charge une animation Lottie depuis une URL."""
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        background-color: #5E9E47;
        color: white;
        border-radius: 8px;
        border: none;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        background-color: #4a8037;
        box-shadow: 0 4px 12px rgba(94, 158, 71, 0.3);
    }
    
    /* Dashboard info card styling */
    .dash-card {
        padding: 18px 20px;
        border-radius: 14px;
        border-left: 5px solid;
        background: linear-gradient(135deg, #f8faf8 0%, #ffffff 100%);
        color: #222;
        margin-bottom: 10px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.06);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .dash-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 24px rgba(0,0,0,0.10);
    }
    .dash-card h4 { 
        margin: 0 0 6px 0;
        font-family: 'Outfit', sans-serif !important;
        font-weight: 600;
    }
    .dash-card p { margin: 3px 0; font-size: 0.92em; }
    
    .card-green { border-color: #5E9E47; }
    .card-blue { border-color: #2F6D89; }
    .card-orange { border-color: #e65100; }
    .card-purple { border-color: #7b1fa2; }
    
    .invoice-alert {
        background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
        border: 1px solid #fb8c00;
        border-radius: 10px;
        padding: 14px 18px;
        margin-top: 10px;
        font-size: 0.95em;
        color: #e65100;
        display: flex;
        align-items: center;
        gap: 10px;
    }
</style>
""", unsafe_allow_html=True)

base_dir = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(base_dir, "LOGO.png")

try:
    if os.path.exists(logo_path):
        col_logo1, col_logo2, col_logo3 = st.columns([1, 4, 1])
        with col_logo2:
            st.image(logo_path, use_column_width=True)
except Exception as e:
    st.warning(f"Erreur d'image: {e}")

# ─── HEADER ──────────────────────────────────────────────────────────────
col_htitle, col_hbtn = st.columns([2.5, 1], gap="medium")
with col_htitle:
    render_brand_page_header("Tableau de Bord", "L'intelligence de la donnée au service du champ ✨", icon="🚜")
with col_hbtn:
    st.markdown("<div style='height: 35px;'></div>", unsafe_allow_html=True)
    st.markdown("""
<style>
div[data-testid="stPageLink"] a {
    background: linear-gradient(135deg, #2F6D89 0%, #5E9E47 100%) !important;
    color: white !important;
    border-radius: 50px !important;
    padding: 16px 20px !important;
    font-family: 'Outfit', sans-serif !important;
    font-weight: 800 !important;
    font-size: 1.2em !important;
    border: none !important;
    box-shadow: 0 6px 20px rgba(47, 109, 137, 0.35) !important;
    transition: all 0.3s ease !important;
    justify-content: center !important;
    display: flex !important;
    text-decoration: none !important;
}
div[data-testid="stPageLink"] a:hover {
    transform: translateY(-4px) !important;
    box-shadow: 0 10px 25px rgba(94, 158, 71, 0.45) !important;
    filter: brightness(1.1) !important;
}
div[data-testid="stPageLink"] a p {
    font-size: 1.1em !important;
    font-weight: 800 !important;
    color: white !important;
    margin: 0 !important;
}
</style>
""", unsafe_allow_html=True)
    
    # Recherche dynamique du vrai chemin du fichier sur le serveur pour éviter le bug des emojis (Git/Linux)
    voice_page_path = None
    if os.path.exists("pages"):
        for f in os.listdir("pages"):
            if "Intervention_et_Assistant" in f and f.endswith(".py"):
                voice_page_path = f"pages/{f}"
                break
                
    if voice_page_path:
        st.page_link(voice_page_path, label="Saisie Vocale Rapide", icon="🎙️")
    else:
        st.info("Module vocal introuvable")

# --- Météus Weather Module (Désactivé temporairement pour futur Bilan Hydrique) ---
# try:
#     display_meteo_module()
# except Exception as e:
#     st.error(f"Erreur module météo: {e}")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# --- QR Action Logic (Must be at top generally for fast action) ---
q_params = st.query_params
val_param = q_params.get("validate_phyto", None)

intervention_id = None
if val_param:
    if isinstance(val_param, list):
        intervention_id = val_param[0]
    else:
        intervention_id = val_param

if intervention_id:
    st.info(f"🔍 Scan détecté pour l'intervention : {intervention_id}")
    
    if st.button("✅ Confirmer : Traitement RÉALISÉ"):
        with st.spinner("Mise à jour du statut..."):
            success = active_loader.update_intervention_status(intervention_id, "Réalisé")
            if success:
                st.success("Statut mis à jour avec succès ! Rafraîchissement en cours...")
                import time
                time.sleep(1)
                st.query_params.clear()
                st.rerun()
            else:
                st.error("Échec de la mise à jour (Vérifiez les logs ou la connexion).")
    st.divider()

# --- Dashboard View ---
st.markdown(f"<h2 style='color:#1c5f85; font-family:\"Outfit\", sans-serif; margin-bottom: 20px; font-size: 1.6em;'>🌾 Assolement {selected_campaign}</h2>", unsafe_allow_html=True)

df_asso = active_loader.get_assolement(selected_campaign)
if not df_asso.empty:
    # Palette de couleurs Agridia : Verts vibrants, Bleus profonds
    colors = ['#6fa33c', '#1c5f85', '#7bb841', '#267b93', '#cda341', '#5E9E47']
    asso_summary = df_asso.groupby('Culture')['Surface_Référence_Ha'].sum().reset_index()
    asso_summary = asso_summary[asso_summary['Surface_Référence_Ha'] > 0].sort_values('Surface_Référence_Ha', ascending=False)
    total_ha = asso_summary['Surface_Référence_Ha'].sum()
    
    if not asso_summary.empty:
        html_asso = """<style>
.agridia-card-container { display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }
.agridia-card { 
    flex: 1 1 calc(25% - 20px); min-width: 160px; 
    background: linear-gradient(135deg, #ffffff 0%, #f8fbf9 100%);
    border-radius: 16px; padding: 22px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
    border-top: 5px solid #6fa33c;
    position: relative; overflow: hidden;
    transition: transform 0.3s ease, box-shadow 0.3s ease;
}
.agridia-card:hover { transform: translateY(-4px); box-shadow: 0 8px 30px rgba(28, 95, 133, 0.12); }
.agridia-card-bg {
    position: absolute; right: -15px; bottom: -20px; font-size: 6em; opacity: 0.04;
    color: #1c5f85; pointer-events: none;
}
.agridia-card-title { font-family: 'Outfit', sans-serif; font-size: 1.1em; color: #444; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 10px;}
.agridia-card-value { font-family: 'Inter', sans-serif; font-size: 2.2em; font-weight: 800; color: #1c5f85; line-height: 1.1;}
.agridia-card-unit { font-size: 0.45em; color: #777; font-weight: 500; }
.agridia-card-percent { font-size: 0.9em; font-weight: 700; margin-top: 12px; display: inline-block; padding: 4px 10px; border-radius: 20px; background: rgba(111, 163, 60, 0.12); color: #5e9e47; }
</style>
<div class="agridia-card-container">
"""
        for i, row in asso_summary.iterrows():
            culture = row['Culture']
            surf = row['Surface_Référence_Ha']
            pct = (surf / total_ha) * 100 if total_ha > 0 else 0
            border_color = colors[i % len(colors)]
            text_color = "#1c5f85" if i % 2 == 0 else "#6fa33c"
            
            icon = "🌾" 
            if "maïs" in culture.lower() or "mais" in culture.lower(): icon = "🌽"
            elif "tournesol" in culture.lower(): icon = "🌻"
            elif "soja" in culture.lower(): icon = "🌱"
            
            html_asso += f"""
<div class="agridia-card" style="border-top-color: {border_color};">
    <div class="agridia-card-bg">{icon}</div>
    <div class="agridia-card-title">{icon} {culture}</div>
    <div class="agridia-card-value">{surf:,.1f}<span class="agridia-card-unit"> ha</span></div>
    <div class="agridia-card-percent">{pct:.1f} %</div>
</div>
"""
        html_asso += "</div>"

        
        st.markdown(html_asso, unsafe_allow_html=True)
else:
    st.info("Aucune donnée d'assolement trouvée pour cette campagne.")

st.divider()

# 2. DERNIÈRE INTERVENTION & INTERV. PRÉVUES
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("<h2 style='color:#1c5f85; font-family:\"Outfit\", sans-serif; margin-bottom: 20px; font-size: 1.6em;'>⏱️ Dernière Intervention</h2>", unsafe_allow_html=True)
    # Get last "Réalisé" intervention
    df_realised = df_campaign[df_campaign['Statut_Intervention'].astype(str).str.lower().str.contains('réalisé')].copy()
    if not df_realised.empty:
        # Sort by date
        df_realised['Date_dt'] = pd.to_datetime(df_realised['Date'], errors='coerce', dayfirst=True)
        last_interv = df_realised.sort_values('Date_dt', ascending=False).iloc[0]
        
        # Handle potential nan values safely
        type_int = last_interv['Type_Intervention'] if pd.notnull(last_interv['Type_Intervention']) and str(last_interv['Type_Intervention']).lower() != 'nan' else ''
        outil = last_interv['Outil'] if pd.notnull(last_interv['Outil']) and str(last_interv['Outil']).lower() != 'nan' else 'N/A'
        produit = last_interv['Nom_Produit'] if pd.notnull(last_interv['Nom_Produit']) and str(last_interv['Nom_Produit']).lower() != 'nan' else 'N/A'
        obs = last_interv['Observations'] if pd.notnull(last_interv['Observations']) and str(last_interv['Observations']).lower() != 'nan' else '-'
        
        # Format the title (only add a hyphen if there is a Type_Intervention)
        title = last_interv['Nature_Intervention']
        if type_int:
            title += f" - {type_int}"

        # Synthetic display style ITK
        st.markdown(f"""
        <div class="dash-card card-green">
            <h4 style="color: #2E7D32;">🌿 {title}</h4>
            <p><b>Date :</b> {last_interv['Date']} | <b>Parcelle :</b> {last_interv['ID_Parcelle']}</p>
            <p><b>Outil :</b> {outil}</p>
            <p><b>Produit :</b> {produit}</p>
            <p style="font-size:0.85em; color: #888;"><i>Obs: {obs}</i></p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Aucune intervention réalisée trouvée.")

with col_right:
    st.markdown("<h2 style='color:#1c5f85; font-family:\"Outfit\", sans-serif; margin-bottom: 20px; font-size: 1.6em;'>📅 Interventions Prévues</h2>", unsafe_allow_html=True)
    df_planned = df_campaign[df_campaign['Statut_Intervention'].astype(str).str.lower().str.contains('prév')].copy()
    if not df_planned.empty:
        # Display top 5 planned
        df_planned['Date_dt'] = pd.to_datetime(df_planned['Date'], errors='coerce', dayfirst=True)
        top_planned = df_planned.sort_values('Date_dt').head(5)
        
        for _, row in top_planned.iterrows():
            type_str = f" ({row['Type_Intervention']})" if pd.notnull(row['Type_Intervention']) and str(row['Type_Intervention']).strip() else ""
            st.markdown(f"• **{row['Date']}** : {row['Nature_Intervention']}{type_str} sur {row['ID_Parcelle']}")
    else:
        st.info("Aucune intervention prévue.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# 3. NOUVEAUX WIDGETS : Dernier Entretien, Dernier Plein GNR, Factures à Traiter
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("<h2 style='color:#1c5f85; font-family:\"Outfit\", sans-serif; margin-bottom: 20px; font-size: 1.6em;'>🔔 Alertes & Suivi Rapide</h2>", unsafe_allow_html=True)

col_maint, col_gnr, col_factures = st.columns(3)

# ─── Dernier Entretien Matériel ────────────────────────────────────────────────
with col_maint:
    try:
        df_maint = active_loader.get_maintenance_history()
        if not df_maint.empty and 'Date' in df_maint.columns:
            df_maint['Date'] = pd.to_datetime(df_maint['Date'], errors='coerce', dayfirst=True)
            df_maint = df_maint.dropna(subset=['Date'])
            if not df_maint.empty:
                last_row = df_maint.sort_values('Date', ascending=False).iloc[0]
                last_date_str = last_row['Date'].strftime('%d/%m/%Y')
                last_mat = str(last_row.get('ID_Materiel', '—'))
                last_type = str(last_row.get('Type_Intervention', '—'))
                last_desc = str(last_row.get('Description', ''))
                if last_desc.lower() in ['nan', 'none', '']: last_desc = ''
                
                st.markdown(f"""
                <div class="dash-card card-blue">
                    <h4 style="color: #1565c0;">🔧 Dernier Entretien</h4>
                    <p><b>Date :</b> {last_date_str}</p>
                    <p><b>Matériel :</b> {last_mat}</p>
                    <p><b>Type :</b> {last_type}</p>
                    {'<p style="font-size:0.85em; color:#888;"><i>' + last_desc + '</i></p>' if last_desc else ''}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="dash-card card-blue">
                    <h4 style="color: #1565c0;">🔧 Dernier Entretien</h4>
                    <p>Aucune donnée disponible.</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="dash-card card-blue">
                <h4 style="color: #1565c0;">🔧 Dernier Entretien</h4>
                <p>Aucune donnée disponible.</p>
            </div>
            """, unsafe_allow_html=True)
    except Exception:
        st.markdown("""
        <div class="dash-card card-blue">
            <h4 style="color: #1565c0;">🔧 Dernier Entretien</h4>
            <p>Données indisponibles.</p>
        </div>
        """, unsafe_allow_html=True)

# ─── Dernier Plein de GNR ──────────────────────────────────────────────────────
with col_gnr:
    try:
        df_fuel_all = active_loader.get_fuel_conso()  # Sans filtre de campagne = tout
        if not df_fuel_all.empty and 'Date' in df_fuel_all.columns:
            df_fuel_all['Date'] = pd.to_datetime(df_fuel_all['Date'], errors='coerce', dayfirst=True)
            df_fuel_all = df_fuel_all.dropna(subset=['Date'])
            if not df_fuel_all.empty:
                last_fuel = df_fuel_all.sort_values('Date', ascending=False).iloc[0]
                fuel_date_str = last_fuel['Date'].strftime('%d/%m/%Y')
                fuel_mat = str(last_fuel.get('ID_Materiel', '—'))
                fuel_qty = last_fuel.get('FUEL_quantité_L', 0)
                try:
                    fuel_qty_f = f"{float(fuel_qty):,.0f} L"
                except:
                    fuel_qty_f = str(fuel_qty)
                fuel_tache = str(last_fuel.get('Tache_réalisée', ''))
                if fuel_tache.lower() in ['nan', 'none', '']: fuel_tache = ''
                
                st.markdown(f"""
                <div class="dash-card card-orange">
                    <h4 style="color: #e65100;">⛽ Dernier Plein GNR</h4>
                    <p><b>Date :</b> {fuel_date_str}</p>
                    <p><b>Matériel :</b> {fuel_mat}</p>
                    <p><b>Quantité :</b> {fuel_qty_f}</p>
                    {'<p style="font-size:0.85em; color:#888;"><i>' + fuel_tache + '</i></p>' if fuel_tache else ''}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="dash-card card-orange">
                    <h4 style="color: #e65100;">⛽ Dernier Plein GNR</h4>
                    <p>Aucune donnée disponible.</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="dash-card card-orange">
                <h4 style="color: #e65100;">⛽ Dernier Plein GNR</h4>
                <p>Aucune donnée disponible.</p>
            </div>
            """, unsafe_allow_html=True)
    except Exception:
        st.markdown("""
        <div class="dash-card card-orange">
            <h4 style="color: #e65100;">⛽ Dernier Plein GNR</h4>
            <p>Données indisponibles.</p>
        </div>
        """, unsafe_allow_html=True)

# ─── Factures à Traiter (Bot Comptable) ───────────────────────────────────────
with col_factures:
    try:
        # Compter les factures dans A_Traiter via Drive
        from shared import get_drive_uploader
        uploader = get_drive_uploader()
        nb_factures = 0
        if uploader and uploader.service:
            try:
                DRIVE_FOLDER_NAME = "08_Factures_Achats_Ventes"
                DRIVE_SUBFOLDER_NAME = "A_Traiter"
                
                # Chercher le dossier parent
                q_parent = f"name = '{DRIVE_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                parent_results = uploader.service.files().list(
                    q=q_parent, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True
                ).execute()
                parent_folders = parent_results.get('files', [])
                
                if parent_folders:
                    parent_id = parent_folders[0]['id']
                    q_sub = f"name = '{DRIVE_SUBFOLDER_NAME}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                    sub_results = uploader.service.files().list(
                        q=q_sub, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True
                    ).execute()
                    sub_folders = sub_results.get('files', [])
                    
                    if sub_folders:
                        a_traiter_id = sub_folders[0]['id']
                        q_files = f"'{a_traiter_id}' in parents and mimeType='application/pdf' and trashed=false"
                        file_results = uploader.service.files().list(q=q_files, fields='files(id)').execute()
                        nb_factures = len(file_results.get('files', []))
            except Exception:
                nb_factures = 0
        
        # Couleur et icône selon le nombre
        if nb_factures > 0:
            badge_color = "#e65100"
            badge_icon = "⚠️"
        else:
            badge_color = "#2e7d32"
            badge_icon = "✅"
        
        st.markdown(f"""
        <a href="Assistant_Comptable" target="_self" style="text-decoration: none; color: inherit; display: block;">
            <div class="dash-card card-purple">
                <h4 style="color: #7b1fa2;">🤖 Assistant Comptable</h4>
                <p style="font-size: 2em; font-weight: bold; text-align: center; margin: 8px 0; color: {badge_color};">{badge_icon} {nb_factures}</p>
                <p style="text-align: center; font-weight: 600;">facture(s) à traiter</p>
            </div>
        </a>
        """, unsafe_allow_html=True)
        
        if nb_factures > 0:
            st.markdown("""
            <div class="invoice-alert">
                <span class="alert-icon">💡</span>
                <span><b>Pensez à régler vos factures avant analyse IA</b></span>
            </div>
            """, unsafe_allow_html=True)
            
    except Exception as e:
        st.markdown(f"""
        <div class="dash-card card-purple">
            <h4 style="color: #7b1fa2;">🤖 Assistant Comptable</h4>
            <p>Service indisponible. ({e})</p>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# 4. SUIVI FERTILISATION (Bilan Azoté / Suivi PPF)
st.markdown("<h2 style='color:#1c5f85; font-family:\"Outfit\", sans-serif; margin-bottom: 20px; font-size: 1.6em;'>🌾 Suivi Fertilisation (Bilan Azoté)</h2>", unsafe_allow_html=True)
try:
    df_ppf = active_loader.get_ppf(selected_campaign)
    if df_ppf.empty:
        st.info(f"Aucune donnée dans l'onglet PPF pour la campagne {selected_campaign}.")
    else:
        # Filtre STRICT : Ne calculer que les Unités effectivement APPORTÉES (Statut = Réalisé)
        mask_ferti = (df_campaign['Nature_Intervention'] == "Fertilisation")
        mask_realise = (df_campaign['Statut_Intervention'].astype(str).str.lower().str.contains('réalisé', na=False))
        df_ferti_realized = df_campaign[mask_ferti & mask_realise].copy()
        
        realized_n_by_parcel = {}
        if not df_ferti_realized.empty:
            df_ferti_realized['N/ha'] = pd.to_numeric(df_ferti_realized['N/ha'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
            sum_n = df_ferti_realized.groupby('ID_Parcelle')['N/ha'].sum()
            realized_n_by_parcel = sum_n.to_dict()
            
        if not df_ppf.empty:
            # Consolidation : On groupe par Parcelle et Culture pour sommer les besoins prévus
            # Cela évite d'avoir plusieurs lignes pour la même parcelle si elle a plusieurs entrées dans le PPF
            df_ppf_grouped = df_ppf.copy()
            df_ppf_grouped['Dose_X'] = pd.to_numeric(df_ppf_grouped['Dose_X'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
            # Utilisation de 'max' au lieu de 'sum' car Dose_X est souvent l'objectif total répété sur chaque ligne de passage
            df_ppf_grouped = df_ppf_grouped.groupby(['ID_Parcelle', 'Culture'])['Dose_X'].max().reset_index()
            
            ppf_display_data = []
            for _, row in df_ppf_grouped.iterrows():
                p_id = str(row.get('ID_Parcelle', 'N/A')).strip()
                if p_id == 'N/A' or not p_id: continue
                
                dose_x = float(row.get('Dose_X', 0.0))
                n_apport = realized_n_by_parcel.get(p_id, 0.0)
                reste = dose_x - n_apport
                culture = str(row.get('Culture', ''))
                
                # Calcul de la progression réelle (avant clip pour affichage texte)
                actual_prog = (n_apport / dose_x * 100) if dose_x > 0 else 0
                
                ppf_display_data.append({
                    'Parcelle': p_id, 
                    'Culture': culture,
                    'Dose X Prévue (U)': int(round(dose_x)), 
                    'N Apporté (U)': int(round(n_apport)),
                    'Reste à Apporter (U)': int(round(reste)),
                    'Actual_Prog': actual_prog
                })
            
        if ppf_display_data:
            df_ppf_vis = pd.DataFrame(ppf_display_data)
            
            # --- Rendu Premium personnalisé ---
            # Barre de progression limitée à 100% visuellement
            df_ppf_vis['VisProgress'] = df_ppf_vis['Actual_Prog'].clip(0, 100)
            
            html_table = """
<div style="font-family: 'Outfit', sans-serif; margin-top: 10px;">
<table style="width: 100%; border-collapse: collapse; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
<thead>
<tr style="background: linear-gradient(90deg, #2F6D89 0%, #5E9E47 100%); color: white; text-align: left;">
<th style="padding: 15px;">📍 Parcelle</th>
<th style="padding: 15px;">🌾 Culture</th>
<th style="padding: 15px; width: 250px;">📊 Réalisé</th>
<th style="padding: 15px; text-align: center;">⏳ Reste</th>
</tr>
</thead>
<tbody>
"""
            
            for i, row in df_ppf_vis.iterrows():
                bg_color = "#f8f9fb" if i % 2 == 1 else "white"
                reste = row['Reste à Apporter (U)']
                
                # Styles dynamiques selon le reste
                if reste == 0:
                    reste_color = "#5E9E47"
                    reste_text = "✅ Terminé"
                elif reste > 0:
                    reste_color = "#d17a22"
                    reste_text = f"⏳ {reste} U"
                else:
                    reste_color = "#d32f2f"
                    reste_text = f"⚠️ {abs(reste)} U (trop)"
                
                prog_text = int(row['Actual_Prog'])
                prog_vis = row['VisProgress']
                # Vert si entre 95% et 105%, Rouge si > 105%, Bleu sinon
                if 95 <= prog_text <= 105:
                    prog_color = "#5E9E47"
                elif prog_text > 105:
                    prog_color = "#d32f2f"
                else:
                    prog_color = "#2F6D89"
                
                html_table += f"""
<tr style="background-color: {bg_color}; border-bottom: 1px solid #eee;">
<td style="padding: 12px 15px; font-weight: 600; color: #333;">{row['Parcelle']}</td>
<td style="padding: 12px 15px; color: #555;">{row['Culture']}</td>
<td style="padding: 12px 15px;">
<div style="font-size: 0.85em; margin-bottom: 5px; display: flex; justify-content: space-between; font-weight: 500;">
<span>{row['N Apporté (U)']} / {row['Dose X Prévue (U)']} U</span>
<span>{prog_text}%</span>
</div>
<div style="height: 8px; width: 100%; background-color: #e0e0e0; border-radius: 4px;">
<div style="height: 100%; width: {round(prog_vis, 1)}%; background-color: {prog_color}; border-radius: 4px;"></div>
</div>
</td>
<td style="padding: 12px 15px; text-align: center; font-weight: bold; color: {reste_color};">
{reste_text}
</td>
</tr>
"""
            
            html_table += "</tbody></table></div>"
            st.markdown(html_table, unsafe_allow_html=True)
            
        else:
            st.info("Impossible de lier les parcelles du PPF.")
except Exception as e: st.error(f"Erreur bilan azoté : {e}")

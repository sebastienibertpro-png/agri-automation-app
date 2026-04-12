import streamlit as st
import os
import pandas as pd
from shared import init_campaign_selector
from meteus_utils import display_meteo_module
import requests

try:
    from streamlit_lottie import st_lottie
    LOTTIE_AVAILABLE = True
except ImportError:
    LOTTIE_AVAILABLE = False

st.set_page_config(page_title="Tableau de Bord", page_icon="🚜", layout="centered")

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
        background-color: #4CAF50;
        color: white;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #45a049;
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
    .dash-card h4 { margin: 0 0 6px 0; }
    .dash-card p { margin: 3px 0; font-size: 0.92em; }
    .card-green { border-color: #2e7d32; }
    .card-blue { border-color: #1565c0; }
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
    .invoice-alert .alert-icon { font-size: 1.5em; }
    .lottie-header {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 16px;
        margin-bottom: 4px;
    }
</style>
""", unsafe_allow_html=True)

base_dir = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(base_dir, "LOGO.png")

try:
    if os.path.exists(logo_path):
        st.image(logo_path, use_column_width=True)
except Exception as e:
    st.warning(f"Erreur d'image: {e}")

# ─── LOTTIE ANIMATION HEADER ───────────────────────────────────────────────────
# Agriculture / Smart Farming / AI animation
lottie_farm = load_lottie_url("https://assets5.lottiefiles.com/packages/lf20_fcfjwiyb.json")
lottie_farm_fallback = load_lottie_url("https://assets2.lottiefiles.com/packages/lf20_jcikwtux.json")

col_title_l, col_title_c, col_title_r = st.columns([1, 3, 1])
with col_title_l:
    animation_data = lottie_farm or lottie_farm_fallback
    if LOTTIE_AVAILABLE and animation_data:
        st_lottie(animation_data, height=100, key="lottie_farm_left")
    else:
        st.markdown("<div style='font-size: 3.5em; text-align: center; padding: 10px;'>🌾</div>", unsafe_allow_html=True)
with col_title_c:
    st.markdown("<h1 style='text-align:center; margin: 0;'>🚜 Tableau de Bord</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center; color: #666; margin-top: 2px;'>Utilisez le menu à gauche pour naviguer entre les différents outils de l'exploitation.</p>", unsafe_allow_html=True)
with col_title_r:
    if LOTTIE_AVAILABLE and animation_data:
        st_lottie(animation_data, height=100, key="lottie_farm_right")
    else:
        st.markdown("<div style='font-size: 3.5em; text-align: center; padding: 10px;'>🤖</div>", unsafe_allow_html=True)

# --- Météus Weather Module ---
try:
    display_meteo_module()
except Exception as e:
    st.error(f"Erreur module météo: {e}")

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
st.markdown(f"## 📋 Vue d'ensemble — Campagne {selected_campaign}")

# 1. ASSOLEMENT (Quick Glance)
st.subheader("🌾 Assolement synthétique")
df_asso = active_loader.get_assolement(selected_campaign)
if not df_asso.empty:
    # Filter for specific crops requested or all
    # Maïs, Maïs Pop corn, Blé
    crops_to_show = ["Maïs", "Maïs Pop corn", "Blé"]
    
    # Calculate totals
    asso_summary = df_asso.groupby('Culture')['Surface_Référence_Ha'].sum().reset_index()
    
    m_cols = st.columns(len(crops_to_show))
    for i, crop in enumerate(crops_to_show):
        if crop == "Maïs":
            row = asso_summary[asso_summary['Culture'].str.contains("Maïs", case=False, na=False) & ~asso_summary['Culture'].str.contains("Pop", case=False, na=False)]
        else:
            row = asso_summary[asso_summary['Culture'].str.contains(crop, case=False, na=False)]
        surf = row['Surface_Référence_Ha'].sum() if not row.empty else 0.0
        m_cols[i].metric(label=crop, value=f"{surf:.1f} ha")
else:
    st.info("Aucune donnée d'assolement trouvée pour cette campagne.")

st.divider()

# 2. DERNIÈRE INTERVENTION & INTERV. PRÉVUES
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("⏱️ Dernière Intervention")
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
    st.subheader("📅 Interventions Prévues")
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

st.subheader("🔔 Alertes & Suivi Rapide")

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
        <div class="dash-card card-purple">
            <h4 style="color: #7b1fa2;">🤖 Bot Comptable</h4>
            <p style="font-size: 2em; font-weight: bold; text-align: center; margin: 8px 0; color: {badge_color};">{badge_icon} {nb_factures}</p>
            <p style="text-align: center; font-weight: 600;">facture(s) à traiter</p>
        </div>
        """, unsafe_allow_html=True)
        
        if nb_factures > 0:
            st.markdown("""
            <div class="invoice-alert">
                <span class="alert-icon">💡</span>
                <span><b>Pensez à régler vos factures avant analyse IA</b></span>
            </div>
            """, unsafe_allow_html=True)
    except Exception:
        st.markdown("""
        <div class="dash-card card-purple">
            <h4 style="color: #7b1fa2;">🤖 Bot Comptable</h4>
            <p>Service indisponible.</p>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# 4. CONSOMMATION FUEL (Campagne)
st.subheader("⛽ Consommation Fuel")
df_fuel = active_loader.get_fuel_conso(selected_campaign)
if not df_fuel.empty:
    total_fuel = df_fuel['FUEL_quantité_L'].sum()
    st.metric("Consommation Totale Campagne", f"{total_fuel:.0f} L", delta=None)
else:
    st.info("Aucune donnée de consommation fuel pour cette campagne.")

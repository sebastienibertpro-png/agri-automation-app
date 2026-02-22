import streamlit as st
import pandas as pd
import os
from data_loader import DataLoader
from report_gen import ReportGenerator
import json
import tempfile
from datetime import datetime
from email_utils import send_email_with_attachment
# Configuration de la page
st.set_page_config(
    page_title="Agri Automation",
    page_icon="🚜",
    layout="centered"
)
# CSS personnalisé pour l'esthétique
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
# --- CONFIG ---
APP_BASE_URL = "https://agri-automation-app-kwz7hjkyb8hjxwhe9w7rsv.streamlit.app"
# --- Authentification ---
credentials_dict = None
if "gcp_service_account" in st.secrets:
    credentials_dict = dict(st.secrets["gcp_service_account"])
# Initialisation du DataLoader
loader = DataLoader("dummy_path.xlsx", use_cloud=True, credentials_dict=credentials_dict)
def load_data():
    if loader.load_source():
        return loader
    return None
with st.spinner('Connexion à Google Sheets...'):
    active_loader = load_data()
if not active_loader:
    st.error("Impossible de se connecter à 'MASTER_EXPLOITATION'.")
    st.stop()
# --- Récupération des Campagnes ---
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
        
    available_campaigns = sorted([int(y) for y in years], reverse=True)
    
    if not available_campaigns:
        st.warning("Aucune donnée trouvée.")
        st.stop()
except Exception as e:
    st.error(f"Erreur lecture campagnes: {e}")
    st.stop()
col1, col2 = st.columns(2)
with col1:
    selected_campaign = st.selectbox("📅 Choisir la Campagne", available_campaigns)
df_campaign = df_intervention[df_intervention['Campagne'].astype(str) == str(selected_campaign)]
available_parcelles = sorted(df_campaign['ID_Parcelle'].unique())
with col2:
    options = ["Toutes"] + list(available_parcelles)
    selected_parcelle = st.selectbox("🌾 Choisir la Parcelle", options)
target_parcelles = list(available_parcelles) if selected_parcelle == "Toutes" else [selected_parcelle]
# --- Gestion des QR Codes ---
q_params = st.query_params
val_param = q_params.get("validate_phyto", None)
intervention_id = val_param[0] if isinstance(val_param, list) else val_param
if intervention_id:
    st.info(f"🔍 Scan détecté pour l'intervention : {intervention_id}")
    if st.button("✅ Confirmer : Traitement RÉALISÉ"):
        with st.spinner("Mise à jour du statut..."):
            success = loader.update_intervention_status(intervention_id, "Réalisé")
            if success:
                st.success("Statut mis à jour avec succès !")
            else:
                st.error("Échec de la mise à jour.")
    st.divider()
# --- GÉNÉRATION DE RAPPORTS ---
st.divider()
st.subheader("📄 Génération de Rapports")
st.markdown("#### 🧪 Fiche de Préparation Phyto")
try:
    df_planned = loader.get_planned_treatments(selected_campaign)
    if not df_planned.empty:
        if selected_parcelle != "Toutes":
            df_planned = df_planned[df_planned['ID_Parcelle'] == selected_parcelle]
            
        if df_planned.empty:
             st.info("Aucune intervention 'Prévue' pour cette sélection.")
        else:
             mixes = {}
             for _, row in df_planned.iterrows():
                 d_val = row['Date']
                 d_str = "Date Inconnue"
                 if pd.notnull(d_val):
                    try: d_str = pd.to_datetime(d_val).strftime('%Y-%m-%d')
                    except: d_str = str(d_val)
                 
                 p_id = row['ID_Parcelle']
                 key = (d_str, p_id)
                 if key not in mixes: mixes[key] = []
                 mixes[key].append(row)
             
             mix_options = [f"{k[0]} - {k[1]} ({len(v)} produits)" for k, v in mixes.items()]
             mix_map = {f"{k[0]} - {k[1]} ({len(v)} produits)": (k, v) for k, v in mixes.items()}
             
             selected_mix_lbl = st.selectbox("Choisir l'intervention prévue :", sorted(mix_options, reverse=True))
             
             if st.button("Générer Fiche Préparation"):
                 key, rows = mix_map[selected_mix_lbl]
                 first_row = rows[0]
                 surface = float(first_row.get('Surface_Travaillée_Ha', 0)) if pd.notnull(first_row.get('Surface_Travaillée_Ha')) else 0.0
                 vol_ha = float(first_row.get('Volume_Bouillie_L_Ha', 0)) if pd.notnull(first_row.get('Volume_Bouillie_L_Ha')) else 0.0
                 
                 sorted_prods = loader.sort_products_by_formulation([r.to_dict() for r in rows])
                 date_obj = pd.to_datetime(rows[0]['Date'])
                 intervention_id = f"{key[1]}_{date_obj.strftime('%Y%m%d')}"
                 
                 payload = {'Parcelle': key[1], 'Surface': surface, 'Date': date_obj, 'Volume_Bouillie_Ha': vol_ha, 'Products': sorted_prods, 'Intervention_ID': intervention_id}
                 
                 with tempfile.TemporaryDirectory() as tmpdirname:
                     fpath = os.path.join(tmpdirname, f"Fiche_Prep_{intervention_id}.pdf")
                     gen = ReportGenerator(fpath)
                     gen.generate_prep_sheet(selected_campaign, payload, base_url=APP_BASE_URL)
                     with open(fpath, "rb") as f:
                        st.download_button(label="⬇️ Télécharger Fiche", data=f, file_name=f"Fiche_Prep_{intervention_id}.pdf", mime="application/pdf")
                 st.success("Fiche générée !")
    else:
        st.info("Pas d'interventions planifiées trouvées.")
except Exception as e:
    st.error(f"Erreur chargement planning: {e}")
st.divider()
def generate_and_download(report_type):
    metadata_map = active_loader.get_parcel_metadata(selected_campaign)
    def patch_surface(df):
        if 'Surface_Travaillée_Ha' in df.columns:
            df['Surface_Travaillée_Ha'] = df['Surface_Travaillée_Ha'].astype(float)
            df.loc[df['Surface_Travaillée_Ha'] > 50, 'Surface_Travaillée_Ha'] /= 100
        return df
    if report_type == "PHYTO":
        df = patch_surface(df_campaign[df_campaign['Nature_Intervention'] == "Traitement"]).fillna("")
        df = df[df['ID_Parcelle'].isin(target_parcelles)]
        return {p: {'data': df[df['ID_Parcelle'] == p].sort_values(by='Date').to_dict('records'), 'meta': metadata_map.get(p, {})} for p in df['ID_Parcelle'].unique()}, "generate_phyto_register", "Registre_Phyto"
    elif report_type == "FERTI":
        df = patch_surface(df_campaign[df_campaign['Nature_Intervention'] == "Fertilisation"]).fillna("")
        df = df[df['ID_Parcelle'].isin(target_parcelles)]
        return {p: {'Apports': df[df['ID_Parcelle'] == p].to_dict('records'), 'Besoins': {'Besoin_N': 0, 'Besoin_P': 0, 'Besoin_K': 0}, 'Sol': {}, 'meta': metadata_map.get(p, {})} for p in df['ID_Parcelle'].unique()}, "generate_ferti_balance", "Bilan_Ferti"
    elif report_type == "ITK":
        df = patch_surface(df_campaign[df_campaign['ID_Parcelle'].isin(target_parcelles)]).fillna("")
        grouped = {}
        for p in df['ID_Parcelle'].unique():
            subset = df[df['ID_Parcelle'] == p].sort_values(by='Date')
            cat = {'meta': metadata_map.get(p, {}), 'Travail du sol': [], 'Semis': [], 'Fertilisation': [], 'Traitement': [], 'Récolte': []}
            for _, r in subset.iterrows():
                n = str(r['Nature_Intervention']).strip()
                if n in ['Déchaumage', 'Labour', 'Travail du sol']: cat['Travail du sol'].append(r.to_dict())
                elif n in ['Semi', 'Semis']: cat['Semis'].append(r.to_dict())
                elif n == 'Fertilisation': cat['Fertilisation'].append(r.to_dict())
                elif n == 'Traitement': cat['Traitement'].append(r.to_dict())
                elif n in ['Récolte', 'Moisson']: cat['Récolte'].append(r.to_dict())
            grouped[p] = cat
        return grouped, "generate_itk", "ITK"
    return None, None, None
col_pdf1, col_pdf2, col_pdf3 = st.columns(3)
import zipfile
def handle_pdf_action(report_type, btn_label):
    if st.button(btn_label):
        data, method_name, prefix = generate_and_download(report_type)
        if not data: st.warning("Pas de données."); return
        with tempfile.TemporaryDirectory() as tmpdirname:
            files = []
            for p_id, p_payload in data.items():
                fpath = os.path.join(tmpdirname, f"{prefix}_{selected_campaign}_{p_id}.pdf")
                gen = ReportGenerator(fpath)
                getattr(gen, method_name)(selected_campaign, {p_id: p_payload})
                files.append(fpath)
            if len(files) == 1:
                with open(files[0], "rb") as f: st.download_button(label="⬇️ Télécharger", data=f, file_name=os.path.basename(files[0]), mime="application/pdf", key=f"dl_{report_type}")
            else:
                zpath = os.path.join(tmpdirname, f"{prefix}_{selected_campaign}.zip")
                with zipfile.ZipFile(zpath, 'w') as zf:
                    for f in files: zf.write(f, os.path.basename(f))
                with open(zpath, "rb") as f: st.download_button(label="⬇️ Télécharger ZIP", data=f, file_name=f"{prefix}_{selected_campaign}.zip", mime="application/zip", key=f"dl_{report_type}_zip")
with col_pdf1: handle_pdf_action("ITK", "📄 Itinéraire Technique")
with col_pdf2: handle_pdf_action("PHYTO", "🛡️ Registre Phyto")
with col_pdf3: handle_pdf_action("FERTI", "🧪 Bilan Ferti")
# --- SECTION IRRIGATION ---
st.divider()
st.subheader("💧 Gestion de l'Irrigation")
try:
    with st.spinner("Chargement irrigation..."):
        df_conso = loader.get_consumption_data(selected_campaign)
    if df_conso.empty:
        st.info("Aucune donnée d'irrigation.")
    else:
        networks = sorted(df_conso['Reseau_type'].unique())
        col_f1, col_f2 = st.columns(2)
        with col_f1: selected_nets = st.multiselect("Filtre Réseau", networks, default=networks)
        
        df_net_filtered = df_conso[df_conso['Reseau_type'].isin(selected_nets)]
        available_meters = sorted(df_net_filtered['ID_Compteur'].unique()) if not df_net_filtered.empty else []
        with col_f2: selected_meters = st.multiselect("Filtre Compteurs", available_meters, default=available_meters)
        
        df_filtered = df_net_filtered[df_net_filtered['ID_Compteur'].isin(selected_meters)]
        
        french_months = {1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai', 6: 'Juin', 7: 'Juillet', 8: 'Août', 9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'}
        reading_months = sorted(df_filtered['Date_Relevé'].dt.month.dropna().unique())
        month_options = [f"{french_months[m-1 if m>1 else 12]} (Relevé de {french_months[m]})" for m in reading_months]
        month_map = {f"{french_months[m-1 if m>1 else 12]} (Relevé de {french_months[m]})": m for m in reading_months}
        
        with col_f1:
            selected_month_label = st.selectbox("📅 Mois de Consommation", month_options)
            selected_reading_month = month_map.get(selected_month_label)
            conso_month_name = selected_month_label.split(" (")[0] if selected_month_label else ""
        if not df_filtered.empty:
            st.markdown(f"#### 📊 Consommation Campagne {selected_campaign}")
            df_agg = df_filtered.groupby('Reseau_type')['Conso_Reelle_m3'].sum().reset_index()
            df_agg.columns = ['Réseau', 'Total m3']
            st.table(df_agg.round(1))
            for net in sorted(selected_nets):
                net_data = df_filtered[df_filtered['Reseau_type'] == net]
                if net_data.empty: continue
                with st.expander(f"Action pour le réseau : {net}"):
                    st.markdown("#### 📜 Bilan Campagne")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button(f"📄 PDF Campagne - {net}", key=f"btn_p_{net}"):
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                                gen = ReportGenerator(tmp.name)
                                gen.generate_irrigation_report(selected_campaign, net, net_data)
                                with open(tmp.name, "rb") as f: st.download_button("Télécharger PDF Campagne", f, f"Bilan_Campagne_{net}.pdf", "application/pdf", key=f"dl_c_{net}")
                    st.divider()
                    st.markdown(f"#### 📅 Bilan Mensuel : {conso_month_name}")
                    m1, m2 = st.columns(2)
                    monthly_data = net_data[net_data['Date_Relevé'].dt.month == selected_reading_month]
                    with m1:
                        if st.button(f"📄 PDF Mensuel - {net}", key=f"btn_m_{net}"):
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                                gen = ReportGenerator(tmp.name)
                                gen.generate_monthly_network_report(selected_campaign, conso_month_name, net, monthly_data)
                                with open(tmp.name, "rb") as f: st.download_button("Télécharger PDF Mensuel", f, f"Bilan_Mensuel_{net}.pdf", "application/pdf", key=f"dl_m_{net}")
                    with m2:
                        if net in ["CUMA_Irrigation", "ASA_SaintLoup"]:
                            recipient = monthly_data['Mail_Contact-Reseau'].iloc[0] if not monthly_data.empty and 'Mail_Contact-Reseau' in monthly_data.columns else None
                            if st.button(f"📧 Envoyer par Email - {net}", key=f"btn_e_{net}"):
                                if recipient:
                                    with st.spinner("Envoi..."):
                                        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                                            gen = ReportGenerator(tmp.name)
                                            gen.generate_monthly_network_report(selected_campaign, conso_month_name, net, monthly_data)
                                            success = send_email_with_attachment(st.secrets["GMAIL_USER"], st.secrets["GMAIL_PASSWORD"], recipient, f"Bilan Irrigation {conso_month_name} - {net}", "Veuillez trouver le bilan ci-joint.", tmp.name)
                                            if success: st.success("Email envoyé !")
                                            else: st.error("Échec envoi.")
                                else: st.error("Email manquant.")
except Exception as e:
    st.error(f"Erreur lors du traitement de l'irrigation : {e}")

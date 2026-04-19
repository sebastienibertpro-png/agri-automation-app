import streamlit as st
import pandas as pd
from datetime import datetime
import tempfile
import zipfile
import os
from report_gen import ReportGenerator
from shared import (
    init_campaign_selector, 
    APP_BASE_URL, 
    render_brand_page_header,
    inject_premium_css,
    render_premium_header
)

st.set_page_config(page_title="Editions de documents", page_icon="📄", layout="wide")
inject_premium_css()

render_brand_page_header(
    "Editions de documents", 
    "Générez vos registres, bilans et fiches de préparation en quelques clics ✨", 
    icon="📄"
)

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# Récupération des métadonnées pour enrichir le sélecteur (standard Agridia)
parcel_meta = active_loader.get_parcel_metadata(selected_campaign)

def get_parcelle_label(p_id):
    if p_id == "Toutes":
        return "🌍 Toutes les parcelles"
    meta = parcel_meta.get(p_id, {})
    surf = meta.get('Surface', 0.0)
    cult = meta.get('Culture', 'Inconnu')
    return f"{p_id} ({surf} ha - {cult})"

options_raw = ["Toutes"] + list(available_parcelles)
labels = [get_parcelle_label(p) for p in options_raw]
label_to_id = dict(zip(labels, options_raw))

selected_label = st.selectbox("🌾 Sélectionner la Parcelle cible :", labels)
selected_parcelle = label_to_id[selected_label]

target_parcelles = []
if selected_parcelle == "Toutes":
    target_parcelles = list(available_parcelles)
else:
    target_parcelles = [selected_parcelle]

st.markdown("<br>", unsafe_allow_html=True)

def generate_and_download(report_type):
    metadata_map = active_loader.get_parcel_metadata(selected_campaign)
    product_prices = active_loader.get_product_prices(selected_campaign)
    
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
            
            # Injection des prix dans grouped_data pour usage global dans le générateur
            grouped_data['product_prices'] = product_prices
            
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
    # Persist the download button even after interaction
    if f"last_files_{report_type}" not in st.session_state:
        st.session_state[f"last_files_{report_type}"] = None

    if st.button(btn_label):
        with st.spinner(f"Génération {report_type}..."):
            data, method_name, prefix = generate_and_download(report_type)
            
            if not data:
                st.warning("Aucune donnée pour cette sélection.")
                return

            # Robust handling of ITK metadata key
            p_prices = data.pop('product_prices', None)

            with tempfile.TemporaryDirectory() as tmpdirname:
                files_data = []
                for p_id, p_payload in data.items():
                    safe_pid = str(p_id).replace(" ", "_").replace("/", "-")
                    fname = f"{prefix}_{selected_campaign}_{safe_pid}.pdf"
                    
                    # Create a persistent path in temp (Simulated by reading bits)
                    fpath = os.path.join(tmpdirname, fname)
                    gen = ReportGenerator(fpath)
                    method = getattr(gen, method_name)
                    
                    # Pass prices back if needed (for ITK)
                    payload_final = {p_id: p_payload}
                    if p_prices and method_name == "generate_itk":
                         payload_final['product_prices'] = p_prices
                         
                    method(selected_campaign, payload_final)
                    
                    with open(fpath, "rb") as f:
                        files_data.append({"name": fname, "content": f.read()})
                
                if not files_data:
                     st.warning("Rien à générer.")
                     return

                # Choose between PDF or ZIP
                if len(files_data) == 1:
                    st.session_state[f"last_files_{report_type}"] = {
                        "type": "PDF",
                        "data": files_data[0]["content"],
                        "name": files_data[0]["name"]
                    }
                else:
                    zip_name = f"{prefix}_Campagne_{selected_campaign}.zip"
                    # We need a new temp file or buffer for the zip
                    import io
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, 'w') as zipf:
                        for file_item in files_data:
                            zipf.writestr(file_item["name"], file_item["content"])
                    
                    st.session_state[f"last_files_{report_type}"] = {
                        "type": "ZIP",
                        "data": zip_buffer.getvalue(),
                        "name": zip_name
                    }

    # Display the result if it exists in session state
    result = st.session_state.get(f"last_files_{report_type}")
    if result:
        st.success(f"Document prêt : {result['name']}")
        st.download_button(
            label=f"⬇️ Télécharger {result['type']} ({report_type})",
            data=result['data'],
            file_name=result['name'],
            mime="application/pdf" if result['type'] == "PDF" else "application/zip",
            key=f"dl_button_{report_type}_{len(result['data'])}", # unique key to avoid stay-active issues
            use_container_width=True
        )

render_premium_header(
    "📚 Choix du Document", 
    "Sélectionnez le type de registre réglementaire à exporter", 
    color="green"
)

with st.container():
    st.markdown('<div style="padding: 20px; background-color: #f8f9fb; border-radius: 0 0 12px 12px; border: 1px solid #e0e0e0; border-top: none; margin-bottom: 25px;">', unsafe_allow_html=True)
    
    doc_options = {
        "📄 Itinéraire Technique": "ITK",
        "🛡️ Registre Phytosanitaire": "PHYTO",
        "🧪 Bilan de Fertilisation": "FERTI", 
        "💧 Bilan Irrigation Parcelle": "IRRIG_PARCELLE"
    }

    selected_doc_label = st.radio(
        "Type de document à générer :", 
        list(doc_options.keys()),
        horizontal=True
    )

    st.write("") # Espace
    handle_pdf_action(doc_options[selected_doc_label], f"🚀 Générer : {selected_doc_label}")
    st.markdown('</div>', unsafe_allow_html=True)

st.write("<br>", unsafe_allow_html=True)

# --- FICHE PREPARATION PHYTO ---
render_premium_header(
    "🧪 Fiche de Préparation Phyto", 
    "Générez vos fiches de mélange pour le terrain", 
    color="blue"
)

with st.container():
    st.markdown('<div style="padding: 20px; background-color: #f8f9fb; border-radius: 0 0 12px 12px; border: 1px solid #e0e0e0; border-top: none;">', unsafe_allow_html=True)
    try:
        df_planned = active_loader.get_planned_treatments(selected_campaign)
        
        if not df_planned.empty:
        interventions_by_dp = {}
        for _, row in df_planned.iterrows():
            d_val = row['Date']
            d_str = "Date Inconnue"
            if pd.notnull(d_val):
                try:
                   if isinstance(d_val, str):
                       d_val = pd.to_datetime(d_val)
                   d_str = d_val.strftime('%Y-%m-%d')
                except:
                   d_str = str(d_val)
                   
            p_id = row['ID_Parcelle']
            key_dp = (d_str, p_id)
            if key_dp not in interventions_by_dp: interventions_by_dp[key_dp] = []
            interventions_by_dp[key_dp].append(row)
            
        mixes = {}
        for key_dp, rows in interventions_by_dp.items():
            d_str, p_id = key_dp
            prod_signatures = []
            for r in rows:
                p_name = str(r.get('Nom_Produit', '')).strip().lower()
                dose = str(r.get('Dose_Ha', '')).strip()
                prod_signatures.append(f"{p_name}_{dose}")
            mix_signature = tuple(sorted(prod_signatures))
            mix_key = (d_str, mix_signature)
            if mix_key not in mixes: mixes[mix_key] = []
            mixes[mix_key].append({'Parcelle': p_id, 'Rows': rows})
        
        mix_options = []
        mix_map = {}
        label_counter = {}
        for k, intervs in mixes.items():
            d_str, mix_sig = k
            first_rows = intervs[0]['Rows']
            nb_p = len(first_rows)
            nb_parcelles = len(intervs)
            p_names = [i['Parcelle'] for i in intervs]
            if nb_parcelles <= 2: p_label = " & ".join(p_names)
            else: p_label = f"{nb_parcelles} Parcelles"
            
            base_label = f"{d_str} - {p_label} ({nb_p} produits)"
            if base_label in label_counter:
                label_counter[base_label] += 1
                label = f"{base_label} (Mix {label_counter[base_label]})"
            else:
                label_counter[base_label] = 1
                label = base_label
            mix_options.append(label)
            mix_map[label] = (k, intervs)
        
        mix_options = sorted(mix_options, reverse=True)
        col_p1, col_p2 = st.columns([2, 1])
        with col_p1:
           selected_mix_lbl = st.selectbox("Choisir l'intervention prévue :", mix_options, key="select_mix_prep")
        
        if st.button("Générer Fiche Préparation", key="btn_gen_prep"):
            key, intervs = mix_map[selected_mix_lbl]
            date_str, mix_sig = key
            total_surface, vol_ha_input = 0.0, 0.0
            parcelles_info, p_ids = [], []
            first_rows = intervs[0]['Rows']
            
            for interv in intervs:
                p_id = interv['Parcelle']
                p_ids.append(p_id)
                first_row_interv = interv['Rows'][0]
                try:
                    surf_val = first_row_interv.get('Surface_Travaillée_Ha', 0)
                    surface = float(surf_val) if pd.notnull(surf_val) else 0.0
                except: surface = 0.0
                total_surface += surface
                parcelles_info.append({'name': p_id, 'surface': surface})
                if vol_ha_input == 0.0:
                    try:
                        vol_val = first_row_interv.get('Volume_Bouillie_L_Ha', 0)
                        vol_ha_input = float(vol_val) if pd.notnull(vol_val) else 0.0
                    except: pass
                        
            if vol_ha_input == 0: st.warning("⚠️ Attention : Volume Bouillie / ha non renseigné.")
            
            prods = [r.to_dict() for r in first_rows]
            sorted_prods = active_loader.sort_products_by_formulation(prods)
            date_obj = first_rows[0]['Date']
            if isinstance(date_obj, str):
                try: date_obj = pd.to_datetime(date_obj)
                except: pass
            clean_date = date_obj.strftime('%Y%m%d') if hasattr(date_obj, 'strftime') else "00000000"
            intervention_id = f"{'|'.join(p_ids)}_{clean_date}"
            
            payload = {
                'Parcelles': parcelles_info, 'Total_Surface': total_surface,
                'Date': date_obj, 'Volume_Bouillie_Ha': vol_ha_input,
                'Products': sorted_prods, 'Intervention_ID': intervention_id
            }
            
            with tempfile.TemporaryDirectory() as tmpdirname:
                fname = f"Fiche_Prep_{intervention_id}.pdf"
                fpath = os.path.join(tmpdirname, fname)
                gen = ReportGenerator(fpath)
                gen.generate_prep_sheet(selected_campaign, payload, base_url=APP_BASE_URL)
                with open(fpath, "rb") as f:
                   st.download_button(label="⬇️ Télécharger Fiche", data=f, file_name=fname, mime="application/pdf", key="dl_prep_sheet", use_container_width=True)
            st.success("✅ Fiche de préparation générée avec succès !")
            
    else:
        st.info("ℹ️ Aucune intervention planifiée trouvée pour cette campagne.")
except Exception as e:
    st.error(f"❌ Erreur lors du chargement du planning : {e}")

st.markdown('</div>', unsafe_allow_html=True)

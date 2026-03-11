import streamlit as st
import pandas as pd
import tempfile
import os
from report_gen import ReportGenerator
from shared import init_campaign_selector, APP_BASE_URL

st.set_page_config(page_title="Suivi Cultures", page_icon="🧪", layout="centered")

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
</style>
""", unsafe_allow_html=True)

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# --- FICHE PREPARATION PHYTO ---
st.header("🧪 Fiche de Préparation Phyto")
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
            if nb_parcelles <= 2:
                p_label = " & ".join(p_names)
            else:
                p_label = f"{nb_parcelles} Parcelles"
            
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
           selected_mix_lbl = st.selectbox("Choisir l'intervention prévue :", mix_options)
        
        if st.button("Générer Fiche Préparation"):
            key, intervs = mix_map[selected_mix_lbl]
            date_str, mix_sig = key
            
            total_surface = 0.0
            vol_ha_input = 0.0
            parcelles_info = []
            p_ids = []
            
            first_rows = intervs[0]['Rows']
            
            for interv in intervs:
                p_id = interv['Parcelle']
                p_ids.append(p_id)
                first_row_interv = interv['Rows'][0]
                
                try:
                    surf_val = first_row_interv.get('Surface_Travaillée_Ha', 0)
                    surface = float(surf_val) if pd.notnull(surf_val) else 0.0
                except:
                    surface = 0.0
                    
                total_surface += surface
                parcelles_info.append({'name': p_id, 'surface': surface})
                
                if vol_ha_input == 0.0:
                    try:
                        vol_val = first_row_interv.get('Volume_Bouillie_L_Ha', 0)
                        vol_ha_input = float(vol_val) if pd.notnull(vol_val) else 0.0
                    except:
                        pass
                        
            if vol_ha_input == 0:
                st.warning("⚠️ Attention : Volume Bouillie / ha non renseigné.")
            
            prods = []
            for r in first_rows:
                prods.append(r.to_dict())
            
            sorted_prods = active_loader.sort_products_by_formulation(prods)
            
            date_obj = first_rows[0]['Date']
            if isinstance(date_obj, str):
                try: date_obj = pd.to_datetime(date_obj)
                except: pass
                   
            if hasattr(date_obj, 'strftime'):
                clean_date = date_obj.strftime('%Y%m%d')
            else:
                clean_date = "00000000"
                
            intervention_id = f"{'|'.join(p_ids)}_{clean_date}"
            
            payload = {
                'Parcelles': parcelles_info,
                'Total_Surface': total_surface,
                'Date': date_obj,
                'Volume_Bouillie_Ha': vol_ha_input,
                'Products': sorted_prods,
                'Intervention_ID': intervention_id
            }
            
            with tempfile.TemporaryDirectory() as tmpdirname:
                fname = f"Fiche_Prep_{intervention_id}.pdf"
                fpath = os.path.join(tmpdirname, fname)
                
                gen = ReportGenerator(fpath)
                gen.generate_prep_sheet(selected_campaign, payload, base_url=APP_BASE_URL)
                
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

# --- Bilan Azoté (Suivi PPF) ---
st.header("🌾 Bilan Azoté (Suivi PPF)")
with st.expander("Voir le reste à apporter (N) par parcelle", expanded=False):
    try:
        df_ppf = active_loader.get_ppf(selected_campaign)
        if df_ppf.empty:
            st.info(f"Aucune donnée dans l'onglet PPF pour la campagne {selected_campaign}.")
        else:
            df_ferti_realized = df_campaign[df_campaign['Nature_Intervention'] == "Fertilisation"].copy()
            
            realized_n_by_parcel = {}
            if not df_ferti_realized.empty:
                df_ferti_realized['N/ha'] = pd.to_numeric(df_ferti_realized['N/ha'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                sum_n = df_ferti_realized.groupby('ID_Parcelle')['N/ha'].sum()
                realized_n_by_parcel = sum_n.to_dict()
                
            ppf_display_data = []
            
            for _, row in df_ppf.iterrows():
                p_id = str(row.get('ID_Parcelle', 'N/A')).strip()
                if p_id == 'N/A' or not p_id: continue
                
                dose_x_raw = str(row.get('Dose_X', '0')).replace(',', '.')
                try: dose_x = float(dose_x_raw)
                except: dose_x = 0.0
                
                n_apport = realized_n_by_parcel.get(p_id, 0.0)
                reste = dose_x - n_apport
                
                culture = str(row.get('Culture', ''))
                
                ppf_display_data.append({
                    'Parcelle': p_id,
                    'Culture': culture,
                    'Dose X Prévue (U)': int(round(dose_x)),
                    'N Apporté (U)': int(round(n_apport)),
                    'Reste à Apporter (U)': int(round(reste))
                })
                
            if ppf_display_data:
                df_ppf_vis = pd.DataFrame(ppf_display_data)
                
                def color_reste(val):
                    if val > 0:
                        return 'color: #d17a22' 
                    elif val < 0:
                        return 'color: #d32f2f' 
                    else:
                        return 'color: #388e3c' 
                        
                st.dataframe(
                    df_ppf_vis.style.map(color_reste, subset=['Reste à Apporter (U)']),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Impossible de lier les parcelles du PPF.")
                
    except Exception as e:
        st.error(f"Erreur lors du chargement du Bilan Azoté : {e}")

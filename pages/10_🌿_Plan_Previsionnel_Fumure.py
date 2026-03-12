import streamlit as st
import pandas as pd
from data_loader import DataLoader
from shared import init_campaign_selector
from report_gen import generate_ppf_pdf

st.set_page_config(page_title="Plan Prévisionnel de Fumure", page_icon="🌿", layout="wide")

st.title("🌿 Plan Prévisionnel de Fumure (PPF)")
st.markdown("---")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader
campagne_input = int(selected_campaign)

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")

# 1. Sélection de la Campagne et Parcelle
col1, col2 = st.columns(2)
with col1:
    st.text_input("Campagne Option", value=str(campagne_input), disabled=True)
with col2:
    # Récupérer les métadonnées de l'assolement pour la campagne
    parcels_meta = dl.get_parcel_metadata(campagne_input)
    if not parcels_meta:
        st.warning(f"Aucune parcelle trouvée pour la campagne {campagne_input}.")
        st.stop()
        
    liste_parcelles = sorted(list(parcels_meta.keys()))
    selected_parcelle = st.selectbox("Sélectionner la Parcelle", liste_parcelles)

if not selected_parcelle:
    st.stop()

# Info de la parcelle sélectionnée
p_data = parcels_meta[selected_parcelle]
surface = p_data.get('Surface', 0.0)
culture_asso = p_data.get('Culture', 'Inconnue')
variete_asso = p_data.get('Variete', '')
ilot_pac = p_data.get('Ilot_PAC', '')

st.info(f"📍 **{selected_parcelle}** - Ilot: {ilot_pac} | Surface: {surface} ha | Culture: {culture_asso} {variete_asso}")

# 2. Chargement des référentiels GREN
df_cipan = dl.get_ref_gren_cipan()
df_prec = dl.get_ref_gren_precedents()
df_humus = dl.get_ref_gren_humus()
df_coef = dl.get_ref_gren_coef()

# 3. Chargement de l'existant (si un PPF est déjà sauvegardé)
df_ppf_all = dl.get_ppf(campagne_input)
existing_ppf = {}
if not df_ppf_all.empty:
    mask = df_ppf_all['ID_Parcelle'].astype(str).str.strip() == str(selected_parcelle).strip()
    match = df_ppf_all[mask]
    if not match.empty:
        # Get the first match as dict
        existing_ppf = match.iloc[0].to_dict()

# Helper to provide default value (Existing > Assolment Defaults)
def get_val(col_name, default):
    val = existing_ppf.get(col_name)
    if pd.isna(val) or val is None or str(val).strip() == "":
        return default
    return val

# 4. Formulaire Paramètres Agronomiques
st.subheader("⚙️ Paramètres Agronomiques (GREN)")

col_a, col_b, col_c = st.columns(3)

with col_a:
    # 4.1 Besoin Culture
    culture_options = []
    if not df_coef.empty and 'Culture' in df_coef.columns:
        culture_options = df_coef['Culture'].dropna().unique().tolist()
    
    # Try to pre-select based on existing or assolement
    def_cult = get_val('Culture', culture_asso)
    if def_cult not in culture_options and culture_options:
        culture_options.append(def_cult)
    
    sel_culture = st.selectbox("Culture", culture_options, index=culture_options.index(def_cult) if def_cult in culture_options else 0)
    
    sel_variete = st.text_input("Variété", value=get_val('Variété', variete_asso))
    obj_rendement = st.number_input("Objectif Rendement (Qx/Ha)", value=float(get_val('Objectif_Rendement_Qx_Ha', 100.0)), step=1.0)
    
    # Récupérer Besoin Unitaire b
    b_unit = 0.0
    if not df_coef.empty:
        match_b = df_coef[df_coef['Culture'] == sel_culture]
        if not match_b.empty:
            # Assuming column is EXACTLY "Besoin__Culture_Unitaire " as per user prompt, let's clean spaces just in case
            col_b_name = 'Besoin__Culture_Unitaire '
            if col_b_name not in match_b.columns:
                # Find closest
                for c in match_b.columns:
                    if 'Besoin' in c and 'Unitaire' in c:
                        col_b_name = c; break
            
            try:
                b_unit = float(match_b[col_b_name].iloc[0])
            except:
                b_unit = 0.0
                
    b_unit_input = st.number_input("Besoin Unitaire (b)", value=float(get_val('Besoin__Culture_Unitaire ', b_unit)), format="%.3f")
    
    besoin_culture_pf = obj_rendement * b_unit_input
    st.markdown(f"**👉 Besoin Culture (Pf) : {besoin_culture_pf:.1f} kg N/ha**")

with col_b:
    # 4.2 Précédent
    prec_options = []
    if not df_prec.empty and 'Precedent_cultural' in df_prec.columns:
         prec_options = df_prec['Precedent_cultural'].dropna().unique().tolist()
         
    def_prec = get_val('Precedent_Cultural', p_data.get('Precedent', ''))
    if def_prec not in prec_options and prec_options:
        if def_prec: prec_options.append(def_prec)
        else: prec_options.append("Inconnu")
        
    sel_prec = st.selectbox("Précédent Cultural", prec_options, index=prec_options.index(def_prec) if def_prec in prec_options else 0)
    gestion_residus = st.selectbox("Gestion des Résidus", ["Enfouis", "Exportés", "Brûlés"], index=["Enfouis", "Exportés", "Brûlés"].index(get_val('Gestion_Résidus', "Enfouis")) if get_val('Gestion_Résidus', "Enfouis") in ["Enfouis", "Exportés", "Brûlés"] else 0)
    
    # Récupérer Mr
    mr_val = 0.0
    if not df_prec.empty:
        match_mr = df_prec[df_prec['Precedent_cultural'] == sel_prec]
        if not match_mr.empty and 'Effet_précédent(Mr)' in match_mr.columns:
            try: mr_val = float(match_mr['Effet_précédent(Mr)'].iloc[0])
            except: pass
            
    mr_input = st.number_input("Effet Précédent (Mr)", value=float(get_val('Effet_précédent(Mr)', mr_val)))
    
    # 4.3 Humus
    sol_options = []
    if not df_humus.empty and 'Type_sol' in df_humus.columns:
        sol_options = df_humus['Type_sol'].dropna().unique().tolist()
        
    def_sol = get_val('Type_sol', sol_options[0] if sol_options else "")
    sel_sol = st.selectbox("Type de sol", sol_options, index=sol_options.index(def_sol) if def_sol in sol_options else 0)
    
    # Récupérer Mh
    mh_val = 0.0
    if not df_humus.empty:
        match_mh = df_humus[df_humus['Type_sol'] == sel_sol]
        if not match_mh.empty and 'Minéralisation_Humus(Mh)' in match_mh.columns:
            try: mh_val = float(match_mh['Minéralisation_Humus(Mh)'].iloc[0])
            except: pass
            
    mh_input = st.number_input("Minéralisation Humus (Mh)", value=float(get_val('Minéralisation_Humus(Mh)', mh_val)))

with col_c:
    # 4.4 CIPAN
    cipan_types = []
    if not df_cipan.empty and 'CIPAN_Type' in df_cipan.columns:
         cipan_types = ["Aucun"] + df_cipan['CIPAN_Type'].dropna().unique().tolist()
         
    def_cipan = get_val('CIPAN_Type', "Aucun")
    sel_cipan = st.selectbox("Type CIPAN", cipan_types, index=cipan_types.index(def_cipan) if def_cipan in cipan_types else 0)
    
    dev_cipan_options = []
    if sel_cipan != "Aucun" and not df_cipan.empty:
         match_c = df_cipan[df_cipan['CIPAN_Type'] == sel_cipan]
         if 'Développement_CIPAN' in match_c.columns:
              dev_cipan_options = match_c['Développement_CIPAN'].dropna().unique().tolist()
              
    def_dev = get_val('Développement_CIPAN', dev_cipan_options[0] if dev_cipan_options else "")
    if def_dev not in dev_cipan_options and dev_cipan_options:
        def_dev = dev_cipan_options[0]
        
    sel_dev_cipan = st.selectbox("Développement CIPAN", dev_cipan_options if dev_cipan_options else ["N/A"], index=dev_cipan_options.index(def_dev) if def_dev in dev_cipan_options else 0)
    
    # Récupérer MrCi
    mrci_val = 0.0
    if sel_cipan != "Aucun" and not df_cipan.empty:
         match_mrci = df_cipan[(df_cipan['CIPAN_Type'] == sel_cipan) & (df_cipan['Développement_CIPAN'] == sel_dev_cipan)]
         if not match_mrci.empty and 'Effet_CIPAN(MrCi)' in match_mrci.columns:
             try: mrci_val = float(match_mrci['Effet_CIPAN(MrCi)'].iloc[0])
             except: pass
             
    mrci_input = st.number_input("Effet CIPAN (MrCi)", value=float(get_val('Effet_CIPAN(MrCi)', mrci_val)))


# 5. Autres éléments du Bilan
st.markdown("---")
st.subheader("📊 Autres postes du Bilan (Reliquats, Irrigation...)")

# Mettre en session temporairement pour forcer la maj si besoin
col_r1, col_r2, col_r3, col_r4 = st.columns(4)

with col_r1:
    rf_input = st.number_input("Azote Fermeture Bilan (Rf)", value=float(get_val('Azote_Fermeture_Bilan(Rf)', 30.0)))
with col_r2:    
    pi_input = st.number_input("Azote déjà absorbé (Pi)", value=float(get_val('Azote_deja_aborbé(Pi)', 0.0)))
with col_r3:
    ri_input = st.number_input("Reliquat Sortie Hiver (Ri)", value=float(get_val('Reliquat_Sortie_Hiver(Ri)', 40.0)))
with col_r4:
    nirr_input = st.number_input("Fourniture Irrigation (Nirr)", value=float(get_val('Fourniture_Irrigation(Nirr)', 0.0)))

# 6. CALCUL DOSE X
besoins_totaux = besoin_culture_pf + rf_input
fournitures_totales = pi_input + ri_input + mh_input + mr_input + mrci_input + nirr_input

dose_x = besoins_totaux - fournitures_totales
# Ne pas avoir de dose négative
dose_x = max(0, dose_x)

st.markdown("---")
col_dx, col_save = st.columns([2,1])

with col_dx:
    st.markdown(f"### 🎯 Dose X à apporter : **{dose_x:.1f} kg N/ha**")
    st.caption(f"Calcul : Besoin ({besoins_totaux:.1f}) - Fourniture ({fournitures_totales:.1f})")

# Calcul de la fertilisation Prévue
df_fert_prevue = dl.get_planned_fertilization(campagne_input)
total_n_prevu = 0.0
interv_list = []

if not df_fert_prevue.empty:
    # Filter by Parcelle
    mask_p = df_fert_prevue['ID_Parcelle'].astype(str).str.strip().str.contains(selected_parcelle, case=False, na=False)
    my_fert = df_fert_prevue[mask_p]
    
    if not my_fert.empty:
        # Assuming N contribution is calculated. Let's see how NPK is stored.
        # N, P, K columns are usually present in interventions for Fertilisation
        for idx, row in my_fert.iterrows():
            date_str = pd.to_datetime(row.get('Date', pd.Timestamp.now()), errors='coerce').strftime('%d/%m/%Y')
            produit = row.get('Nom_Produit', 'Engrais inconnu')
            qte = pd.to_numeric(row.get('Quantité_Totale_Produit', 0), errors='coerce')
            n_tot = pd.to_numeric(row.get('N_Total', 0), errors='coerce') # or similar column
            
            # If N_Total is not direct, check if we need to calculate it from Unit
            if pd.isna(n_tot) or n_tot == 0:
                 # Try to guess or fallback
                 # In a real scenario, we might merge with REF_INTRANTS to get NPK concentration.
                 # Let's hope N_Total is populated by the Saisie Intervention page.
                 n_tot = pd.to_numeric(row.get('N_Total_apporté', 0), errors='coerce')
                 if pd.isna(n_tot): n_tot = 0.0
                 
            # Convert total N applied on the parcel to N per hectare
            # Actually N_Total is usually PER HECTARE in standard ITK forms.
            # Let's assume the column is Dose/Ha or N/Ha. If row has N_Total, is it total or /ha? 
            # In general, doses in agri are expressed in kg/ha. Let's assume n_app= N/Ha
            
            # Look for N_Unitaire or N_Ha
            n_ha = pd.to_numeric(row.get('N_ha', pd.to_numeric(row.get('Dose_Unitaire_N', pd.to_numeric(row.get('N', 0), errors='coerce')), errors='coerce')), errors='coerce')
            
            if pd.isna(n_ha): n_ha = 0.0
            
            total_n_prevu += n_ha
            interv_list.append({
                'Date': date_str,
                'Produit': produit,
                'Dose_Ha': f"{n_ha:.1f} Unités"
            })


# VISUALISATION FRACTIONNEMENT
st.markdown("### 🚜 Interventions Prévues (Journal)")

if interv_list:
    df_show = pd.DataFrame(interv_list)
    st.table(df_show)
else:
    st.info("Aucune fertilisation prévue trouvée pour cette parcelle dans le journal.")

# Jauge / Alerte de dépassement
delta = total_n_prevu - dose_x

if dose_x > 0:
    progress_val = min(total_n_prevu / dose_x, 1.0)
    st.progress(progress_val)

if total_n_prevu > dose_x:
    st.error(f"⚠️ **ATTENTION :** La dose prévue totale ({total_n_prevu:.1f} U) dépasse la Dose X calculée ({dose_x:.1f} U) d'un écart de +{delta:.1f} Unités !")
elif total_n_prevu > 0:
    st.success(f"Dose prévue couverte à **{(total_n_prevu / dose_x * 100):.1f}%** ({total_n_prevu:.1f} U / {dose_x:.1f} U)")

with col_save:
    # Build dictionary to save
    ppf_dict = {
        'Campagne': str(campagne_input),
        'ID_Parcelle': str(selected_parcelle),
        'îlot PAC': str(ilot_pac),
        'Surface_Référence_Ha': surface,
        'Culture': str(sel_culture),
        'Variété': str(sel_variete),
        'Gestion_Résidus': str(gestion_residus),
        'Type_sol': str(sel_sol),
        'Objectif_Rendement_Qx_Ha': float(obj_rendement),
        'Precedent_Cultural': str(sel_prec),
        'CIPAN_Type': str(sel_cipan),
        'Développement_CIPAN': str(sel_dev_cipan),
        'Besoin__Culture_Unitaire ': float(b_unit_input),
        'Besoin_Culture(Pf)': float(besoin_culture_pf),
        'Azote_Fermeture_Bilan(Rf)': float(rf_input),
        'Azote_deja_aborbé(Pi)': float(pi_input),
        'Reliquat_Sortie_Hiver(Ri)': float(ri_input),
        'Minéralisation_Humus(Mh)': float(mh_input),
        'Effet_précédent(Mr)': float(mr_input),
        'Effet_CIPAN(MrCi)': float(mrci_input),
        'Fourniture_Irrigation(Nirr)': float(nirr_input),
        'Dose_X': float(dose_x)
    }
    
    if st.button("💾 Sauvegarder ce PPF", use_container_width=True, type="primary"):
        with st.spinner("Sauvegarde..."):
            if dl.update_ppf(ppf_dict):
                st.success("PPF Sauvegardé !")
                
    if st.button("📄 Éditer le PDF PPF", use_container_width=True):
        with st.spinner("Génération du document..."):
            pdf_data = generate_ppf_pdf(ppf_dict, interv_list)
            if pdf_data:
                 st.download_button(
                    label="⬇️ Télécharger le PPF",
                    data=pdf_data,
                    file_name=f"PPF_{campagne_input}_{selected_parcelle}.pdf",
                    mime="application/pdf",
                    use_container_width=True
                 )
            else:
                 st.error("Échec de la génération PDF.")

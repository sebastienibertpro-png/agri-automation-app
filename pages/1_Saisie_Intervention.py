import streamlit as st
import pandas as pd
import string
import random
from shared import init_campaign_selector

st.set_page_config(page_title="Saisie Intervention", page_icon="✍️", layout="centered")

st.title("✍️ Saisie Rapide Multi-Interventions")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

def generate_intervention_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

nature_options = ["Traitement", "Fertilisation", "Semis", "Déchaumage", "Préparation Printemps", "Binage", "Fissuration", "Récolte"]

st.markdown("##### 1. Informations Générales")

nature_interv = st.selectbox("Nature de l'intervention", nature_options)

col_g1, col_g2, col_g3 = st.columns(3)
with col_g1:
    date_interv = st.date_input("Date de l'intervention")
with col_g2:
    statut = st.selectbox("Statut", ["Prévu", "Réalisé"])
with col_g3:
    campagne_saisie = st.number_input("Campagne", value=int(selected_campaign), format="%d")
    
col_m1, col_m2, col_m3 = st.columns(3)
with col_m1:
    if nature_interv == "Traitement":
        type_interv = st.selectbox("Type d'intervention", ["Herbicide", "Fongicide", "Insecticide", "Régulateur", "Autre"])
    elif nature_interv == "Fertilisation":
        type_interv = st.selectbox("Type d'intervention", ["Minérale", "Organique", "Foliaire"])
    else:
        type_interv = st.text_input("Type d'intervention", disabled=True)
        
with col_m2:
    tracteur = st.selectbox("Tracteur", ["130_CVX", "220_CVX", "Berthoud_Raptor", "Axial_5140"])
with col_m3:
    outil = st.selectbox("Outil", ["- Aucun -", "Agata", "Ependeur_Engrais", "DDI", "Rotative", "Cultivateur_Bonnel", "Bineuse", "Fissurateur", "Rabe"])
    
stade = st.selectbox("Stade Culture", ["Pré-levée", "Levée", "2F", "4-6F", "8-10F", "12F", "Floraison", "Tallage", "Epis 1cm", "Montaison", "Maturité", "Récolte"])

if nature_interv == "Traitement":
    volume_bouillie = st.number_input("Volume Bouillie (L/ha)", min_value=0.0, value=100.0, step=10.0)
else:
    volume_bouillie = 0.0
    
observations = st.text_input("Observations")

st.markdown("##### 2. Choix des Parcelles")
selected_p_for_entry = st.multiselect("Parcelles concernées", available_parcelles)

parcelles_data = [] 
if selected_p_for_entry:
    st.markdown("*Surfaces travaillées (Ajustables)*")
    metadata = active_loader.get_parcel_metadata(campagne_saisie)
    cols = st.columns(len(selected_p_for_entry) if len(selected_p_for_entry) < 4 else 4)
    for i, p_id in enumerate(selected_p_for_entry):
        p_meta = metadata.get(p_id, {})
        culture_ref = p_meta.get('Culture', 'Inconnue')
        try:
            surf_ref = float(str(p_meta.get('Surface', 0.0)).replace(',', '.'))
        except:
            surf_ref = 0.0
            
        with cols[i % 4]:
             surf_input = st.number_input(f"{p_id} ({culture_ref})", value=surf_ref, step=0.5, key=f"surf_input_{p_id}")
             parcelles_data.append({'id': p_id, 'culture': culture_ref, 'surface': float(surf_input)})

st.markdown("##### 3. Détails de l'Intervention")

produits_data = []  
semis_data = {}     
recolte_data = {}   

try:
     df_intrants = active_loader._get_data("REF_INTRANTS")
except Exception:
     df_intrants = pd.DataFrame()

if nature_interv == "Traitement":
    liste_produits = []
    if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
        if 'Type' in df_intrants.columns:
             phyto_df = df_intrants[~df_intrants['Type'].str.contains('Engrais', na=False, case=False)]
             liste_produits = sorted(phyto_df['Nom_Produit'].dropna().unique().tolist())
        else:
             liste_produits = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
    
    if not liste_produits: liste_produits = ["(Saisir manuellement)"]

    try:
        df_usages_ref = active_loader.get_usages_phyto()
    except Exception:
        df_usages_ref = pd.DataFrame()

    def get_cibles_for_product(nom_produit):
        if df_usages_ref.empty or 'Nom_Produit' not in df_usages_ref.columns:
            return []
        sub = df_usages_ref[df_usages_ref['Nom_Produit'].astype(str).str.upper() == str(nom_produit).upper()]
        if sub.empty: return []
        cibles = sub['Cible'].dropna().unique().tolist()
        return sorted([str(c) for c in cibles if str(c).strip()])

    def get_dose_for_cible(nom_produit, cible):
        if df_usages_ref.empty: return None, None
        sub = df_usages_ref[
            (df_usages_ref['Nom_Produit'].astype(str).str.upper() == str(nom_produit).upper()) &
            (df_usages_ref['Cible'].astype(str) == str(cible))
        ]
        if sub.empty: return None, None
        dose_raw = str(sub['Dose_Max'].iloc[0]).replace(',', '.')
        dose = pd.to_numeric(dose_raw, errors='coerce')
        unite = sub['Unite_Dose'].iloc[0] if 'Unite_Dose' in sub.columns else None
        return (float(dose) if not pd.isna(dose) else None), unite
         
    for i in range(1, 6): 
        c1, c2, c3, c4 = st.columns([2, 1.5, 1, 1])
        with c1:
            prod = st.selectbox(f"Produit {i}", ["- Aucun -"] + liste_produits, key=f"prod_name_{i}")
        
        cible_val = ""
        if prod != "- Aucun -":
            cibles_dispo = get_cibles_for_product(prod)
            with c2:
                if cibles_dispo:
                    cible_val = st.selectbox(f"Cible {i}", [""] + cibles_dispo, key=f"prod_cible_{i}")
                else:
                    cible_val = st.text_input(f"Cible {i}", key=f"prod_cible_txt_{i}", placeholder="Saisir la cible")
            auto_dose, auto_unite = get_dose_for_cible(prod, cible_val) if cible_val else (None, None)
        else:
            with c2: st.text_input(f"Cible {i}", key=f"prod_cible_empty_{i}", disabled=True)
            auto_dose, auto_unite = None, None

        col_key_prod = f"last_prod_{i}"
        col_key_cible = f"last_cible_{i}"
        
        if col_key_prod not in st.session_state: st.session_state[col_key_prod] = "- Aucun -"
        if col_key_cible not in st.session_state: st.session_state[col_key_cible] = ""
            
        unite_options = ["L/ha", "Kg/ha", "g/ha"]

        if st.session_state[col_key_prod] != prod or st.session_state[col_key_cible] != cible_val:
            st.session_state[col_key_prod] = prod
            st.session_state[col_key_cible] = cible_val
            st.session_state[f"prod_dose_{i}"] = float(auto_dose) if auto_dose is not None else 0.0
            st.session_state[f"prod_unite_{i}"] = auto_unite if auto_unite in unite_options else "L/ha"

        with c3:
            dose = st.number_input(f"Dose/ha", min_value=0.0, step=0.1, key=f"prod_dose_{i}")
        with c4:
            unite = st.selectbox("Unité", unite_options, key=f"prod_unite_{i}")
        if prod != "- Aucun -":
            produits_data.append({'nom': prod, 'cible': cible_val, 'dose': dose, 'unite': unite})

elif nature_interv == "Fertilisation":
    liste_engrais = []
    if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
        if 'Type' in df_intrants.columns:
             ferti_df = df_intrants[df_intrants['Type'].str.contains('Engrais', na=False, case=False)]
             liste_engrais = sorted(ferti_df['Nom_Produit'].dropna().unique().tolist())
        else:
             liste_engrais = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
             
    if not liste_engrais: liste_engrais = ["(Saisir manuellement)"]
    
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
         engrais_prod = st.selectbox("Engrais", ["- Aucun -"] + liste_engrais)
    with c2:
         dose_ferti = st.number_input("Dose/ha", min_value=0.0, step=10.0, value=100.0)
    with c3:
         unite_ferti = st.selectbox("Unité", ["Kg/ha", "L/ha", "T/ha"])
         
    pct_n, pct_p, pct_k = 0.0, 0.0, 0.0
    if engrais_prod != "- Aucun -" and not df_intrants.empty:
         row_engrais = df_intrants[df_intrants['Nom_Produit'] == engrais_prod]
         if not row_engrais.empty:
             def get_safely(col):
                 if col in row_engrais.columns:
                     val = str(row_engrais[col].iloc[0]).replace(',', '.')
                     try: return float(val)
                     except: return 0.0
                 return 0.0
             pct_n = get_safely('Element_N')
             pct_p = get_safely('Element_P')
             pct_k = get_safely('Element_K')
    
    def get_npk_ratio(val):
        return val if abs(val) <= 1.0 and val != 0 else val / 100.0

    mult = 1000.0 if unite_ferti == "T/ha" else 1.0
    n_ha = round((dose_ferti * mult) * get_npk_ratio(pct_n), 1)
    p_ha = round((dose_ferti * mult) * get_npk_ratio(pct_p), 1)
    k_ha = round((dose_ferti * mult) * get_npk_ratio(pct_k), 1)
    
    st.markdown(f"**Apports Calculés:** N: `{n_ha}` | P: `{p_ha}` | K: `{k_ha}`")
    if engrais_prod != "- Aucun -":
        produits_data.append({
            'nom': engrais_prod, 'cible': '', 'dose': dose_ferti, 'unite': unite_ferti,
            'N_ha': n_ha, 'P_ha': p_ha, 'K_ha': k_ha
        })

elif nature_interv == "Semis":
    liste_semences = []
    if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
         if 'Type' in df_intrants.columns:
             sem_df = df_intrants[df_intrants['Type'].str.contains('Semence', na=False, case=False)]
             liste_semences = sorted(sem_df['Nom_Produit'].dropna().unique().tolist())
         else:
             liste_semences = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
    if not liste_semences: liste_semences = ["(Saisir manuellement)"]
             
    c1, c2, c3, c4 = st.columns(4)
    with c1: semence_prod = st.selectbox("Semence / Variété", ["- Aucun -"] + liste_semences)
    with c2: densite = st.number_input("Densité (Unité/ha)", min_value=0.0, step=1.0)
    with c3: unite_densite = st.selectbox("Unité Semis", ["Grains/m²", "Doses/ha", "Kg/ha"])
    with c4: pmg = st.number_input("PMG (g)", min_value=0.0, step=1.0)
    
    st.markdown("##### Produits Associés au Semis (Optionnel)")
    liste_autres = []
    if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
        liste_autres = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
    if not liste_autres: liste_autres = ["(Saisir manuellement)"]
    
    semis_assoc_prods = []
    for i in range(1, 4):
        c_p1, c_p2, c_p3 = st.columns([2, 1, 1])
        with c_p1:
            p_nom = st.selectbox(f"Produit Associé {i}", ["- Aucun -"] + liste_autres, key=f"semis_prod_{i}")
        with c_p2:
            p_dose = st.number_input(f"Dose/ha {i}", min_value=0.0, step=0.1, key=f"semis_dose_{i}")
        with c_p3:
            p_unite = st.selectbox(f"Unité {i}", ["Kg/ha", "L/ha", "g/ha"], key=f"semis_unite_{i}")
            
        if p_nom != "- Aucun -":
            pct_n, pct_p, pct_k = 0.0, 0.0, 0.0
            if not df_intrants.empty:
                 row_p = df_intrants[df_intrants['Nom_Produit'] == p_nom]
                 if not row_p.empty:
                     def get_safely_p(col):
                         if col in row_p.columns:
                             val = str(row_p[col].iloc[0]).replace(',', '.')
                             try: return float(val)
                             except: return 0.0
                         return 0.0
                     pct_n = get_safely_p('Element_N')
                     pct_p = get_safely_p('Element_P')
                     pct_k = get_safely_p('Element_K')
            
            def get_npk_ratio(val):
                return val if abs(val) <= 1.0 and val != 0 else val / 100.0

            n_ha = round(p_dose * get_npk_ratio(pct_n), 1)
            p_ha = round(p_dose * get_npk_ratio(pct_p), 1)
            k_ha = round(p_dose * get_npk_ratio(pct_k), 1)
            
            semis_assoc_prods.append({
                'nom': p_nom, 'dose': p_dose, 'unite': p_unite,
                'N_ha': n_ha, 'P_ha': p_ha, 'K_ha': k_ha
            })
    
    if semence_prod != "- Aucun -":
         semis_data = {
             'nom': semence_prod, 'densite': densite, 'unite': unite_densite, 'pmg': pmg,
             'assoc_prods': semis_assoc_prods
         }

elif nature_interv == "Récolte":
    c1, c2, c3, c4 = st.columns(4)
    with c1: prod_recolte = st.text_input("Produit Récolté", placeholder="Ex: Blé Tendre")
    with c2: rdt_ha = st.number_input("Rendement (Qx/ha ou T/ha)", min_value=0.0, step=0.1)
    with c3: humidite = st.number_input("Humidité (%)", min_value=0.0, value=14.0, step=0.1)
    with c4: ps = st.number_input("PS", min_value=0.0, value=76.0, step=0.1)
    
    if prod_recolte:
         recolte_data = {
             'produit': prod_recolte, 'rendement': rdt_ha, 'humidite': humidite, 'ps': ps
         }
         
elif nature_interv in ["Déchaumage", "Préparation Printemps", "Binage", "Fissuration"]:
    st.info(f"Aucun produit nécessaire pour l'intervention : {nature_interv}.")

st.markdown("<br>", unsafe_allow_html=True)

# Generate Button styling
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

submitted = st.button(f"Enregistrer ({nature_interv}) 🚀")

if submitted:
    if not selected_p_for_entry:
         st.error("Veuillez sélectionner au moins une parcelle.")
    elif nature_interv in ["Traitement", "Fertilisation"] and not produits_data:
         st.error("Veuillez ajouter au moins un produit.")
    elif nature_interv == "Semis" and not semis_data:
         st.error("Veuillez sélectionner une semence.")
    elif nature_interv == "Récolte" and not recolte_data:
         st.error("Veuillez saisir le produit récolté.")
    else:
         rows_to_insert = []
         for p in parcelles_data:
              uid = generate_intervention_id()
              
              base_row = {
                  'ID_Intervention': uid,
                  'ID_Parcelle': p['id'],
                  'Campagne': campagne_saisie,
                  'Date': date_interv.strftime('%d/%m/%Y'),
                  'Statut_Intervention': statut,
                  'Nature_Intervention': nature_interv,
                  'Type_Intervention': type_interv,
                  'Culture': p['culture'],
                  'Surface_Travaillée_Ha': p['surface'],
                  'Tracteur': tracteur,
                  'Outil': outil if outil != "- Aucun -" else "",
                  'Stade_Culture': stade,
                  'Observations': observations,
                  'Nom_Produit': '', 'Cible': '', 'Dose_Ha': '', 'Unité_Dose': '', 'Quantité_Totale_Produit': '', 'Unité_Quantité': '',
                  'N/ha': '', 'P/ha': '', 'K/ha': '',
                  'Volume_Bouillie_L_Ha': volume_bouillie if volume_bouillie > 0 else '', 'Volume_Total_Bouillie_L': '',
                  'Densité_Semis': '', 'Unité_Densité': '', 'PMG': '', 'Quantité_semence_totale': '',
                  'Produit_Récolté': '', 'Rendement_Ha': '', 'Humidité_récolte': '', 'PS': '', 'Quantité_Récoltée_Totale': ''
              }
              
              if nature_interv == "Traitement":
                  for prod in produits_data:
                       row = base_row.copy()
                       row['Nom_Produit'] = prod['nom']
                       row['Cible'] = prod.get('cible', '')
                       row['Dose_Ha'] = prod['dose']
                       row['Unité_Dose'] = prod['unite']
                       row['Quantité_Totale_Produit'] = round(prod['dose'] * p['surface'], 2)
                       row['Unité_Quantité'] = str(prod['unite']).replace('/ha', '').replace('/Ha', '')
                       row['Volume_Total_Bouillie_L'] = round(volume_bouillie * p['surface'], 2)
                       rows_to_insert.append(row)
                       
              elif nature_interv == "Fertilisation":
                  for prod in produits_data:
                       row = base_row.copy()
                       row['Nom_Produit'] = prod['nom']
                       row['Dose_Ha'] = prod['dose']
                       row['Unité_Dose'] = prod['unite']
                       row['Quantité_Totale_Produit'] = round(prod['dose'] * p['surface'], 2)
                       row['Unité_Quantité'] = str(prod['unite']).replace('/ha', '').replace('/Ha', '')
                       row['N/ha'] = prod['N_ha']
                       row['P/ha'] = prod['P_ha']
                       row['K/ha'] = prod['K_ha']
                       rows_to_insert.append(row)
                       
              elif nature_interv == "Semis":
                  row = base_row.copy()
                  row['Nom_Produit'] = semis_data['nom']
                  row['Densité_Semis'] = semis_data['densite']
                  row['Unité_Densité'] = semis_data['unite']
                  row['PMG'] = semis_data['pmg']
                  
                  if semis_data['unite'] == "Kg/ha":
                      qte = semis_data['densite'] * p['surface']
                  elif semis_data['unite'] == "Doses/ha":
                      qte = semis_data['densite'] * p['surface'] 
                  else: 
                      if semis_data['pmg'] > 0:
                           kg_ha = semis_data['densite'] * 10000 * semis_data['pmg'] / 1000000
                           qte = kg_ha * p['surface']
                      else: qte = 0
                  row['Quantité_semence_totale'] = round(qte, 2)
                  rows_to_insert.append(row)
                  
                  for p_assoc in semis_data.get('assoc_prods', []):
                       row_p = base_row.copy()
                       row_p['Nom_Produit'] = p_assoc['nom']
                       row_p['Dose_Ha'] = p_assoc['dose']
                       row_p['Unité_Dose'] = p_assoc['unite']
                       row_p['Quantité_Totale_Produit'] = round(p_assoc['dose'] * p['surface'], 2)
                       row_p['Unité_Quantité'] = str(p_assoc['unite']).replace('/ha', '').replace('/Ha', '')
                       row_p['N/ha'] = p_assoc['N_ha']
                       row_p['P/ha'] = p_assoc['P_ha']
                       row_p['K/ha'] = p_assoc['K_ha']
                       rows_to_insert.append(row_p)
                  
              elif nature_interv == "Récolte":
                  row = base_row.copy()
                  row['Produit_Récolté'] = recolte_data['produit']
                  row['Rendement_Ha'] = recolte_data['rendement']
                  row['Humidité_récolte'] = recolte_data['humidite']
                  row['PS'] = recolte_data['ps']
                  row['Quantité_Récoltée_Totale'] = round(recolte_data['rendement'] * p['surface'], 2)
                  rows_to_insert.append(row)
                  
              elif nature_interv in ["Déchaumage", "Préparation Printemps", "Binage", "Fissuration"]:
                  row = base_row.copy()
                  rows_to_insert.append(row)
         
         df_new = pd.DataFrame(rows_to_insert)
         
         with st.spinner(f"Insertion de {len(df_new)} ligne(s) dans le journal..."):
              success = active_loader.bulk_insert_interventions(df_new)
              if success:
                   st.success("✅ Interventions enregistrées avec succès ! (Rechargez la page pour la mise à jour des rapports)")
              else:
                   st.error("❌ Échec de l'insertion.")

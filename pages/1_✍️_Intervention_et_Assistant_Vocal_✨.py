import streamlit as st
import pandas as pd
import string
import random
from datetime import datetime
import requests
from streamlit_lottie import st_lottie
from shared import init_campaign_selector

# --- Imports optionnels pour le mode vocal ---
try:
    from audio_recorder_streamlit import audio_recorder
    AUDIO_RECORDER_AVAILABLE = True
except ImportError:
    AUDIO_RECORDER_AVAILABLE = False

try:
    from voice_processor import (
        build_context_from_loader,
        transcribe_audio_bytes,
        format_voice_summary
    )
    VOICE_PROCESSOR_AVAILABLE = True
except ImportError:
    VOICE_PROCESSOR_AVAILABLE = False

def load_lottieurl(url: str):
    try:
        r = requests.get(url)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None

st.set_page_config(page_title="Saisie d'Intervention", page_icon="✍️", layout="wide")

# ═══════════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════════
st.markdown("""
<style>
.manual-section-title {
    background: #f8f9fa;
    padding: 15px;
    border-radius: 10px;
    border-left: 5px solid #4CAF50;
    margin-top: 30px;
    margin-bottom: 20px;
}
.voice-box {
    background: linear-gradient(135deg, #e8f5e9 0%, #f1f8f1 100%);
    border: 1.5px solid #66bb6a;
    border-radius: 14px;
    padding: 20px 22px;
    margin-bottom: 18px;
}
.voice-result-card {
    background: linear-gradient(135deg, #e3f2fd 0%, #f0f7ff 100%);
    border-left: 4px solid #1976d2;
    border-radius: 10px;
    padding: 14px 18px;
    margin: 10px 0;
    font-size: 0.93em;
}
.voice-result-card h4 { margin: 0 0 8px 0; color: #1565c0; }
.prefill-banner {
    background: linear-gradient(135deg, #e8f5e9 0%, #dcedc8 100%);
    border: 1px solid #81c784;
    border-radius: 8px;
    padding: 10px 16px;
    margin-bottom: 12px;
    font-size: 0.9em;
    color: #2e7d32;
}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# GESTION DU MODE ÉDITION
# ═══════════════════════════════════════════════════════════════════
is_edit_mode = False
edit_data = {}
if "edit_intervention" in st.session_state and st.session_state.edit_intervention:
    is_edit_mode = True
    edit_data = st.session_state.edit_intervention
    st.title("✍️ Modifier l'Intervention")
    st.info(f"Mode Édition activé pour l'intervention du {edit_data.get('Date', '')} sur {edit_data.get('ID_Parcelle', '')}")
    if st.button("❌ Annuler l'édition"):
        st.session_state.edit_intervention = None
        st.rerun()
else:
    st.title("✍️ Saisie d'Intervention")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# ═══════════════════════════════════════════════════════════════════
# ASSISTANT VOCAL — SECTION EN HAUT
# ═══════════════════════════════════════════════════════════════════
if not is_edit_mode:
    with st.container(border=True):
        col_anim, col_text = st.columns([1, 4])
        
        with col_anim:
            lottie_tractor = load_lottieurl("https://lottie.host/819d4546-d248-4389-9b93-b6d4fe754a6d/m8e1Pz3C7H.json")
            if lottie_tractor:
                st_lottie(lottie_tractor, height=120, key="voice_fun_anim")
            else:
                st.markdown("<div style='font-size:3em;'>🎙️</div>", unsafe_allow_html=True)
                
        with col_text:
            st.markdown("### Assistant Vocal")
            st.caption("Gagnez du temps ! Décrivez votre intervention à haute voix.")
            st.caption("Ex : *« J'ai traité les Buissons avec du Peak à 0.25 L/ha avec le 220 CVX »*")

        if not AUDIO_RECORDER_AVAILABLE:
            st.warning("⚠️ Module vocal non disponible.")
        elif not VOICE_PROCESSOR_AVAILABLE:
            st.warning("⚠️ Processeur vocal non disponible.")
        else:
            api_key = st.secrets.get("GEMINI_API_KEY", "")
            if not api_key:
                st.warning("⚠️ Clé API manquante.")
            else:
                col_rec, col_status = st.columns([2, 5])
                with col_rec:
                    audio_bytes = audio_recorder(
                        text="Cliquer pour enregistrer",
                        recording_color="#e53935",
                        neutral_color="#43a047",
                        icon_size="2x",
                        pause_threshold=300.0,
                        sample_rate=16000,
                        key="voice_audio_recorder"
                    )
                with col_status:
                    if audio_bytes:
                        st.success("✅ Audio capturé - Prêt pour analyse")
                    elif "voice_result" in st.session_state and st.session_state.voice_result:
                        st.info("💡 Analyse en mémoire")
                    else:
                        st.caption("Appuyez sur le micro pour commencer, puis réappuyez pour stopper. L'icône devient rouge pendant l'enregistrement.")

                # Stocker les bytes audio en session pour pouvoir analyser
                if audio_bytes:
                    st.session_state["voice_audio_bytes"] = audio_bytes

                # Boutons d'action
                col_a1, col_a2 = st.columns([1, 1])
                with col_a1:
                    do_analyze = st.button("✨ Analyser avec l'IA", type="primary", use_container_width=True, key="btn_analyze_voice")
                with col_a2:
                    if st.button("🗑️ Effacer", use_container_width=True, key="btn_clear_voice"):
                        for k in ["voice_audio_bytes", "voice_result", "voice_prefill"]:
                            st.session_state.pop(k, None)
                        st.rerun()

            # --- Analyse Gemini ---
            if "do_analyze" in locals() and do_analyze and "voice_audio_bytes" in st.session_state:
                with st.spinner("🤖 Analyse en cours..."):
                    try:
                        context = build_context_from_loader(active_loader, selected_campaign)
                        result = transcribe_audio_bytes(
                            st.session_state["voice_audio_bytes"],
                            context,
                            api_key,
                            audio_format="wav"
                        )
                        st.session_state["voice_result"] = result
                        prefill_list = [item for item in result if item.get("Type_Action", "INTERVENTION") == "INTERVENTION" and "error" not in item]
                        st.session_state["voice_prefill"] = prefill_list
                    except Exception as e:
                        st.error(f"Erreur d'analyse : {e}")
                st.rerun()

            # --- Résultat affiché ---
            if "voice_result" in st.session_state and st.session_state.voice_result:
                result = st.session_state.voice_result
                summary = format_voice_summary(result)
                st.markdown('<div class="voice-result-card">', unsafe_allow_html=True)
                st.markdown("**🎯 Compris par l'IA :**")
                st.markdown(summary)
                st.markdown('</div>', unsafe_allow_html=True)

                if st.button("✅ Appliquer au formulaire ↓", type="primary", use_container_width=True, key="btn_apply_voice"):
                    st.rerun()

# ═══════════════════════════════════════════════════════════════════
# SAISIE MANUELLE
# ═══════════════════════════════════════════════════════════════════
st.markdown('<div class="manual-section-title"><h3>✍️ Saisie Manuelle de l\'Intervention</h3>'
            '<p style="margin:0; opacity:0.8;">Complétez ou ajustez les détails ci-dessous.</p></div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# Bannière si pré-remplissage actif
# ═══════════════════════════════════════════════════════════════════
voice_pf_list = st.session_state.get("voice_prefill", [])
_pf = voice_pf_list[0] if voice_pf_list else {}
if _pf:
    produits_lbl = " | ".join(filter(None, [p.get("Nom_Produit") for p in voice_pf_list]))
    st.markdown(f"""
    <div class="prefill-banner">
        🎙️ <b>Formulaire pré-rempli par l'assistant vocal</b> — Vérifiez chaque champ avant d'enregistrer.<br>
        <small>Nature : {_pf.get('Nature_Intervention','?')} | Parcelle : {_pf.get('ID_Parcelle','?')} | Produits : {produits_lbl}</small>
    </div>
    """, unsafe_allow_html=True)

st.divider()

def generate_intervention_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

nature_options = ["Traitement", "Fertilisation", "Semis", "Déchaumage", "Préparation Printemps", "Binage", "Fissuration", "Récolte"]

st.markdown("##### 1. Informations Générales")

# Détermination des index par défaut si mode édition
def get_index(options, value):
    try: return options.index(value)
    except: return 0

# Source des valeurs par défaut : édition > vocal > vide
_src = edit_data if is_edit_mode else _pf  # shortcut

default_nature = get_index(nature_options, _src.get('Nature_Intervention', 'Traitement'))
nature_interv = st.selectbox("Nature de l'intervention", nature_options, index=default_nature)

col_g1, col_g2, col_g3 = st.columns(3)
with col_g1:
    if is_edit_mode:
        raw_date = edit_data.get('Date')
        if hasattr(raw_date, 'date'):
            default_date = raw_date.date()
        elif isinstance(raw_date, str):
            try:
                default_date = datetime.strptime(raw_date, '%d/%m/%Y').date()
            except:
                default_date = datetime.now().date()
        else:
            default_date = datetime.now().date()
    elif _pf.get('Date'):
        try:
            default_date = datetime.strptime(_pf['Date'], '%d/%m/%Y').date()
        except:
            default_date = datetime.now().date()
    else:
        default_date = datetime.now().date()

    date_interv = st.date_input("Date de l'intervention", value=default_date)
with col_g2:
    default_statut = get_index(["Prévu", "Réalisé"], _src.get('Statut_Intervention', 'Réalisé'))
    statut = st.selectbox("Statut", ["Prévu", "Réalisé"], index=default_statut)
with col_g3:
    default_campagne = int(_src.get('Campagne', selected_campaign) or selected_campaign)
    campagne_saisie = st.number_input("Campagne", value=default_campagne, format="%d")

col_m1, col_m2, col_m3 = st.columns(3)
with col_m1:
    if nature_interv == "Traitement":
        default_type = get_index(["Herbicide", "Fongicide", "Insecticide", "Régulateur", "Autre"], _src.get('Type_Intervention', 'Herbicide'))
        type_interv = st.selectbox("Type d'intervention", ["Herbicide", "Fongicide", "Insecticide", "Régulateur", "Autre"], index=default_type)
    elif nature_interv == "Fertilisation":
        default_type = get_index(["Minérale", "Organique", "Foliaire"], _src.get('Type_Intervention', 'Minérale'))
        type_interv = st.selectbox("Type d'intervention", ["Minérale", "Organique", "Foliaire"], index=default_type)
    else:
        type_interv = st.text_input("Type d'intervention", value=_src.get('Type_Intervention', ''), disabled=True)

with col_m2:
    tracteur_options = ["130_CVX", "220_CVX", "Berthoud_Raptor", "Axial_5140"]
    default_tracteur = get_index(tracteur_options, _src.get('Tracteur', '130_CVX'))
    tracteur = st.selectbox("Tracteur", tracteur_options, index=default_tracteur)
with col_m3:
    outil_options = ["- Aucun -", "Agata", "Ependeur_Engrais", "DDI", "Rotative", "Cultivateur_Bonnel", "Bineuse", "Fissurateur", "Rabe"]
    default_outil = get_index(outil_options, _src.get('Outil', '- Aucun -'))
    outil = st.selectbox("Outil", outil_options, index=default_outil)

stade_options = ["", "Pré-levée", "Levée", "2F", "4-6F", "8-10F", "12F", "Floraison", "Tallage", "Epis 1cm", "Montaison", "Maturité", "Récolte"]
default_stade = get_index(stade_options, _src.get('Stade_Culture', ''))
stade = st.selectbox("Stade Culture", stade_options, index=default_stade)

if nature_interv == "Traitement":
    try: default_vol = float(_src.get('Volume_Bouillie_L_Ha', 100.0) or 100.0)
    except: default_vol = 100.0
    volume_bouillie = st.number_input("Volume Bouillie (L/ha)", min_value=0.0, value=default_vol, step=10.0)
else:
    volume_bouillie = 0.0

observations = st.text_input("Observations", value=_src.get('Observations', ''))

st.markdown("##### 2. Choix des Parcelles")

# Pré-sélection vocale de la parcelle
voice_parcelle = _pf.get('ID_Parcelle', '') if not is_edit_mode else ''
if is_edit_mode:
    default_parcelles = [edit_data['ID_Parcelle']]
elif voice_parcelle and voice_parcelle in available_parcelles:
    default_parcelles = [voice_parcelle]
else:
    default_parcelles = []

selected_p_for_entry = st.multiselect("Parcelles concernées", available_parcelles, default=default_parcelles)


parcelles_data = [] 
if selected_p_for_entry:
    st.markdown("*Surfaces travaillées (Ajustables)*")
    metadata = active_loader.get_parcel_metadata(campagne_saisie)
    cols = st.columns(len(selected_p_for_entry) if len(selected_p_for_entry) < 4 else 4)
    for i, p_id in enumerate(selected_p_for_entry):
        p_meta = metadata.get(p_id, {})
        culture_ref = p_meta.get('Culture', 'Inconnue')
        
        # En mode édition, si c'est la parcelle d'origine, on prend sa surface saisie
        if is_edit_mode and p_id == edit_data['ID_Parcelle']:
            try: surf_ref = float(edit_data.get('Surface_Travaillée_Ha', 0.0) or 0.0)
            except: surf_ref = 0.0
        else:
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

# Pré-remplissage des produits en mode édition / vocal
raw_products = []
if is_edit_mode:
    df_raw = active_loader.get_interventions()
    mask = df_raw['ID_Intervention'].isin(edit_data['ID_Intervention'])
    raw_products = df_raw[mask].to_dict('records')
elif voice_pf_list:
    raw_products = voice_pf_list

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
        
        # Valeurs par défaut en mode édition
        p_val = "- Aucun -"
        c_val = ""
        d_val = 0.0
        u_val = "L/ha"
        
        if (is_edit_mode or _pf) and (i-1) < len(raw_products):
            row_p = raw_products[i-1]
            p_val = row_p.get('Nom_Produit', "- Aucun -")
            c_val = row_p.get('Cible', "")
            try: d_val = float(row_p.get('Dose_Ha', 0.0))
            except: d_val = 0.0
            u_val = row_p.get('Unité_Dose', "L/ha")

        with c1:
            prod = st.selectbox(f"Produit {i}", ["- Aucun -"] + liste_produits, key=f"prod_name_{i}", index=get_index(["- Aucun -"] + liste_produits, p_val))
        
        cible_val = ""
        if prod != "- Aucun -":
            cibles_dispo = get_cibles_for_product(prod)
            with c2:
                if cibles_dispo:
                    cible_val = st.selectbox(f"Cible {i}", [""] + cibles_dispo, key=f"prod_cible_{i}", index=get_index([""] + cibles_dispo, c_val))
                else:
                    cible_val = st.text_input(f"Cible {i}", key=f"prod_cible_txt_{i}", value=c_val)
            
            auto_dose, auto_unite = get_dose_for_cible(prod, cible_val) if cible_val else (None, None)
        else:
            with c2: st.text_input(f"Cible {i}", key=f"prod_cible_empty_{i}", disabled=True)
            auto_dose, auto_unite = None, None

        col_key_prod = f"last_prod_{i}"
        col_key_cible = f"last_cible_{i}"
        
        if col_key_prod not in st.session_state: st.session_state[col_key_prod] = p_val
        if col_key_cible not in st.session_state: st.session_state[col_key_cible] = c_val
            
        unite_options = ["L/ha", "Kg/ha", "g/ha"]

        # Si l'utilisateur change de produit/cible, on applique la dose auto
        if st.session_state[col_key_prod] != prod or st.session_state[col_key_cible] != cible_val:
            st.session_state[col_key_prod] = prod
            st.session_state[col_key_cible] = cible_val
            st.session_state[f"prod_dose_{i}"] = float(auto_dose) if auto_dose is not None else 0.0
            st.session_state[f"prod_unite_{i}"] = auto_unite if auto_unite in unite_options else "L/ha"
        elif (is_edit_mode or _pf) and f"first_load_{i}" not in st.session_state:
            # Premier chargement en mode édition ou vocal : on force les valeurs de l'intervention
            st.session_state[f"prod_dose_{i}"] = d_val
            st.session_state[f"prod_unite_{i}"] = u_val
            st.session_state[f"first_load_{i}"] = True

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
    
    # Defaults
    e_val = "- Aucun -"
    d_f_val = 100.0
    u_f_val = "Kg/ha"
    
    if (is_edit_mode or _pf) and raw_products:
        row_e = raw_products[0]
        e_val = row_e.get('Nom_Produit', "- Aucun -")
        try: d_f_val = float(row_e.get('Dose_Ha', 0.0))
        except: d_f_val = 100.0
        u_f_val = row_e.get('Unité_Dose', "Kg/ha")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
         engrais_prod = st.selectbox("Engrais", ["- Aucun -"] + liste_engrais, index=get_index(["- Aucun -"] + liste_engrais, e_val))
    with c2:
         dose_ferti = st.number_input("Dose/ha", min_value=0.0, step=10.0, value=d_f_val)
    with c3:
         unite_ferti = st.selectbox("Unité", ["Kg/ha", "L/ha", "T/ha"], index=get_index(["Kg/ha", "L/ha", "T/ha"], u_f_val))
         
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
    
    s_val = "- Aucun -"
    dens_val = 0.0
    u_dens_val = "Grains/m²"
    pmg_val = 0.0
    
    if (is_edit_mode or _pf) and raw_products:
        row_s = raw_products[0]
        s_val = row_s.get('Nom_Produit', "- Aucun -")
        try: dens_val = float(row_s.get('Densité_Semis', 0.0))
        except: dens_val = 0.0
        u_dens_val = row_s.get('Unité_Densité', "Grains/m²")
        try: pmg_val = float(row_s.get('PMG', 0.0))
        except: pmg_val = 0.0
             
    c1, c2, c3, c4 = st.columns(4)
    with c1: semence_prod = st.selectbox("Semence / Variété", ["- Aucun -"] + liste_semences, index=get_index(["- Aucun -"] + liste_semences, s_val))
    with c2: densite = st.number_input("Densité (Unité/ha)", min_value=0.0, step=1.0, value=dens_val)
    with c3: unite_densite = st.selectbox("Unité Semis", ["Grains/m²", "Doses/ha", "Kg/ha"], index=get_index(["Grains/m²", "Doses/ha", "Kg/ha"], u_dens_val))
    with c4: pmg = st.number_input("PMG (g)", min_value=0.0, step=1.0, value=pmg_val)
    
    st.markdown("##### Produits Associés au Semis (Optionnel)")
    liste_autres = []
    if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
        liste_autres = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
    if not liste_autres: liste_autres = ["(Saisir manuellement)"]
    
    semis_assoc_prods = []
    assoc_rows = raw_products[1:] if (is_edit_mode or _pf) else []
    
    for i in range(1, 4):
        c_p1, c_p2, c_p3 = st.columns([2, 1, 1])
        ap_val = "- Aucun -"
        ad_val = 0.0
        au_val = "Kg/ha"
        if (i-1) < len(assoc_rows):
            r_ap = assoc_rows[i-1]
            ap_val = r_ap.get('Nom_Produit', "- Aucun -")
            try: ad_val = float(r_ap.get('Dose_Ha', 0.0))
            except: ad_val = 0.0
            au_val = r_ap.get('Unité_Dose', "Kg/ha")

        with c_p1:
            p_nom = st.selectbox(f"Produit Associé {i}", ["- Aucun -"] + liste_autres, key=f"semis_prod_{i}", index=get_index(["- Aucun -"] + liste_autres, ap_val))
        with c_p2:
            p_dose = st.number_input(f"Dose/ha {i}", min_value=0.0, step=0.1, key=f"semis_dose_{i}", value=ad_val)
        with c_p3:
            p_unite = st.selectbox(f"Unité {i}", ["Kg/ha", "L/ha", "g/ha"], key=f"semis_unite_{i}", index=get_index(["Kg/ha", "L/ha", "g/ha"], au_val))
            
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
            
            def get_npk_ratio_s(val):
                return val if abs(val) <= 1.0 and val != 0 else val / 100.0

            n_ha = round(p_dose * get_npk_ratio_s(pct_n), 1)
            p_ha = round(p_dose * get_npk_ratio_s(pct_p), 1)
            k_ha = round(p_dose * get_npk_ratio_s(pct_k), 1)
            semis_assoc_prods.append({'nom': p_nom, 'dose': p_dose, 'unite': p_unite, 'N_ha': n_ha, 'P_ha': p_ha, 'K_ha': k_ha})
    
    if semence_prod != "- Aucun -":
         semis_data = {'nom': semence_prod, 'densite': densite, 'unite': unite_densite, 'pmg': pmg, 'assoc_prods': semis_assoc_prods}

elif nature_interv == "Récolte":
    r_val = ""
    rdt_val = 0.0
    h_val = 14.0
    ps_val = 76.0
    if (is_edit_mode or _pf) and raw_products:
        row_r = raw_products[0]
        r_val = row_r.get('Produit_Récolté', "")
        try: rdt_val = float(row_r.get('Rendement_Ha', 0.0))
        except: rdt_val = 0.0
        try: h_val = float(row_r.get('Humidité_récolte', 14.0))
        except: h_val = 14.0
        try: ps_val = float(row_r.get('PS', 76.0))
        except: ps_val = 76.0

    c1, c2, c3, c4 = st.columns(4)
    with c1: prod_recolte = st.text_input("Produit Récolté", value=r_val, placeholder="Ex: Blé Tendre")
    with c2: rdt_ha = st.number_input("Rendement (Qx/ha ou T/ha)", min_value=0.0, step=0.1, value=rdt_val)
    with c3: humidite = st.number_input("Humidité (%)", min_value=0.0, value=h_val, step=0.1)
    with c4: ps = st.number_input("PS", min_value=0.0, value=ps_val, step=0.1)
    if prod_recolte: recolte_data = {'produit': prod_recolte, 'rendement': rdt_ha, 'humidite': humidite, 'ps': ps}

st.markdown("<br>", unsafe_allow_html=True)
btn_label = "Mettre à jour l'intervention 🔄" if is_edit_mode else f"Enregistrer ({nature_interv}) 🚀"
submitted = st.button(btn_label)

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
              uid = edit_data.get('ID_Intervention')[0] if is_edit_mode and isinstance(edit_data.get('ID_Intervention'), list) else (edit_data.get('ID_Intervention') or generate_intervention_id())
              if len(parcelles_data) > 1 and is_edit_mode: uid = generate_intervention_id() 

              base_row = {
                  'ID_Intervention': uid, 'ID_Parcelle': p['id'], 'Campagne': campagne_saisie,
                  'Date': date_interv.strftime('%d/%m/%Y'), 'Statut_Intervention': statut,
                  'Nature_Intervention': nature_interv, 'Type_Intervention': type_interv,
                  'Culture': p['culture'], 'Surface_Travaillée_Ha': p['surface'],
                  'Tracteur': tracteur, 'Outil': outil if outil != "- Aucun -" else "",
                  'Stade_Culture': stade, 'Observations': observations,
                  'Nom_Produit': '', 'Cible': '', 'Dose_Ha': '', 'Unité_Dose': '', 'Quantité_Totale_Produit': '', 'Unité_Quantité': '',
                  'N/ha': '', 'P/ha': '', 'K/ha': '', 'Volume_Bouillie_L_Ha': volume_bouillie if volume_bouillie > 0 else '',
                  'Volume_Total_Bouillie_L': '', 'Densité_Semis': '', 'Unité_Densité': '', 'PMG': '', 'Quantité_semence_totale': '',
                  'Produit_Récolté': '', 'Rendement_Ha': '', 'Humidité_récolte': '', 'PS': '', 'Quantité_Récoltée_Totale': ''
              }
              
              if nature_interv == "Traitement":
                  for prod in produits_data:
                       row = base_row.copy()
                       row['Nom_Produit'], row['Cible'], row['Dose_Ha'], row['Unité_Dose'] = prod['nom'], prod.get('cible', ''), prod['dose'], prod['unite']
                       row['Quantité_Totale_Produit'] = round(prod['dose'] * p['surface'], 2)
                       row['Unité_Quantité'] = str(prod['unite']).replace('/ha', '').replace('/Ha', '')
                       row['Volume_Total_Bouillie_L'] = round(volume_bouillie * p['surface'], 2)
                       rows_to_insert.append(row)
              elif nature_interv == "Fertilisation":
                  for prod in produits_data:
                       row = base_row.copy()
                       row['Nom_Produit'], row['Dose_Ha'], row['Unité_Dose'] = prod['nom'], prod['dose'], prod['unite']
                       row['Quantité_Totale_Produit'] = round(prod['dose'] * p['surface'], 2)
                       row['Unité_Quantité'] = str(prod['unite']).replace('/ha', '').replace('/Ha', '')
                       row['N/ha'], row['P/ha'], row['K/ha'] = prod['N_ha'], prod['P_ha'], prod['K_ha']
                       rows_to_insert.append(row)
              elif nature_interv == "Semis":
                  row = base_row.copy()
                  row['Nom_Produit'], row['Densité_Semis'], row['Unité_Densité'], row['PMG'] = semis_data['nom'], semis_data['densite'], semis_data['unite'], semis_data['pmg']
                  if semis_data['unite'] == "Kg/ha": qte = semis_data['densite'] * p['surface']
                  elif semis_data['unite'] == "Doses/ha": qte = semis_data['densite'] * p['surface'] 
                  else: qte = (semis_data['densite'] * 10000 * semis_data['pmg'] / 1000000) * p['surface'] if semis_data['pmg'] > 0 else 0
                  row['Quantité_semence_totale'] = round(qte, 2)
                  rows_to_insert.append(row)
                  for p_assoc in semis_data.get('assoc_prods', []):
                       row_p = base_row.copy()
                       row_p['Nom_Produit'], row_p['Dose_Ha'], row_p['Unité_Dose'] = p_assoc['nom'], p_assoc['dose'], p_assoc['unite']
                       row_p['Quantité_Totale_Produit'] = round(p_assoc['dose'] * p['surface'], 2)
                       row_p['Unité_Quantité'] = str(p_assoc['unite']).replace('/ha', '').replace('/Ha', '')
                       row_p['N/ha'], row_p['P/ha'], row_p['K/ha'] = p_assoc['N_ha'], p_assoc['P_ha'], p_assoc['K_ha']
                       rows_to_insert.append(row_p)
              elif nature_interv == "Récolte":
                  row = base_row.copy()
                  row['Produit_Récolté'], row['Rendement_Ha'], row['Humidité_récolte'], row['PS'] = recolte_data['produit'], recolte_data['rendement'], recolte_data['humidite'], recolte_data['ps']
                  row['Quantité_Récoltée_Totale'] = round(recolte_data['rendement'] * p['surface'], 2)
                  rows_to_insert.append(row)
              else: rows_to_insert.append(base_row)
          
         df_new = pd.DataFrame(rows_to_insert)
         with st.spinner("Mise à jour du journal..."):
              if is_edit_mode: active_loader.delete_interventions(edit_data['ID_Intervention'])
              success = active_loader.bulk_insert_interventions(df_new)
              if success:
                   st.success("✅ Mis à jour !" if is_edit_mode else "✅ Enregistré !")
                   if is_edit_mode:
                       st.session_state.edit_intervention = None
                       st.rerun()
              else: st.error("❌ Échec.")

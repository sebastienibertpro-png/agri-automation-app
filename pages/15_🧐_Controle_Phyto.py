import streamlit as st
import pandas as pd
import requests
import json
import google.generativeai as genai
from shared import init_campaign_selector, render_premium_header, render_premium_table, get_active_loader

try:
    from streamlit_lottie import st_lottie
    LOTTIE_AVAILABLE = True
except ImportError:
    LOTTIE_AVAILABLE = False

st.set_page_config(page_title="Contrôle Phyto", page_icon="🧐", layout="wide")

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

# Utilisation d'une animation lottie symbolisant l'inspection/sécurité
lottie_inspection = load_lottie_url("https://assets-v2.lottiefiles.com/a/97b3986c-1151-11ee-8260-5f2fc4514ba1/n2p92xQeJ9.json")

# --- Styles CSS Premium ---
st.markdown("""
<style>
    .ai-alert-box {
        background-color: #feeceb;
        border-left: 5px solid #e74c3c;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    .ai-warning-box {
        background-color: #fdfae3;
        border-left: 5px solid #f1c40f;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    .ai-success-box {
        background-color: #eafaf1;
        border-left: 5px solid #2ecc71;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# ─── HEADER ────────────────────────────────────────────────────────
col_l, col_r = st.columns([1, 4])
with col_l:
    if LOTTIE_AVAILABLE and lottie_inspection:
        st_lottie(lottie_inspection, height=120, key="lottie_inspection")
    else:
        st.markdown("<div style='font-size: 4em; text-align: center; padding: 10px;'>🧐</div>", unsafe_allow_html=True)
with col_r:
    st.markdown("<h1 style='margin-bottom: 0px;'>🧐 Assistant Contrôle Phyto</h1>", unsafe_allow_html=True)
    st.markdown("<p style='color: #666; font-size: 1.1em;'>Analysez vos Itinéraires Techniques et vérifiez la conformité réglementaire de vos traitements phytosanitaires grâce à l'IA.</p>", unsafe_allow_html=True)

st.divider()

# --- INITIALISATION API ---
api_key = st.secrets.get("GEMINI_API_KEY")
if not api_key:
    st.warning("⚠️ Clé d'API Gemini introuvable.")
    st.info("Ajoutez `GEMINI_API_KEY = 'votre_cle'` dans votre fichier `.streamlit/secrets.toml`.")
    st.stop()

genai.configure(api_key=api_key)

system_instruction = """
Tu es un expert agréé en réglementation phytosanitaire agricole française (CERTIPHYTO). 
Ton rôle est d'analyser l'Itinéraire Technique Cultural (ITK) d'une parcelle et d'identifier TOUTE non-conformité possible par rapport à la base de données E-Phy.

Tu dois vérifier strictement pour chaque produit phytosanitaire utilisé :
1. Dose homologuée par Hectare (y a t-il un surdosage ? Si la dose n'est pas renseignée ou égale à 0, ne signale pas d'erreur, demande juste de la renseigner).
2. Nombre maximum d'applications par an.
3. Le Délai Avant Récolte (DAR) (si tu ne trouves pas la date de récolte mais qu'un DAR élevé existe, mentionne-le).
4. Distances de sécurité des points d'eau (Zones Non Traitées aquatiques - ZNT).
5. Dispositif Végétalisé Permanent (DVP) (très imporant, p.e. monsoon = 20m).
6. Restrictions spécifiques de mélanges et d'usages selon la culture spécifiée.

Ne fais pas de long préambule. Analyse chronologiquement, ou produit par produit.
Formate ta réponse de manière très claire :
- Si danger absolu (ex: surdosage, interdiction) : utilise des alertes marquées 🚨 NON-CONFORMITÉ MAJEURE.
- Si point de vigilance (ex: Mélange potentiellement a surveiller, ou DVP strict) : utilise ⚠️ POINT DE VIGILANCE OU RAPPEL.
- Si tout est conforme : utilise ✅ CONFORME.
Explique brièvement, sois chiffré.
"""

@st.cache_resource
def get_model():
    return genai.GenerativeModel(
        model_name="gemini-1.5-pro",
        system_instruction=system_instruction
    )

model = get_model()

# --- SELECTION CONTEXTE ---
active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

if not available_parcelles:
    st.info(f"Aucune intervention trouvée pour la campagne {selected_campaign}.")
    st.stop()

st.subheader("1️⃣ Sélectionnez la parcelle à auditer")
col_sel1, col_sel2 = st.columns([1, 2])
with col_sel1:
    selected_parcelle = st.selectbox("Parcelle :", options=available_parcelles)

# Extraction des données liées à cette parcelle
df_parcelle_itk = df_campaign[df_campaign['ID_Parcelle'] == selected_parcelle].copy()
if not df_parcelle_itk.empty and 'Date' in df_parcelle_itk.columns:
    df_parcelle_itk['Date_dt'] = pd.to_datetime(df_parcelle_itk['Date'], errors='coerce', dayfirst=True)
    df_parcelle_itk = df_parcelle_itk.sort_values(by='Date_dt')

# Retrouver la culture
df_asso = active_loader.get_assolement(selected_campaign)
culture_en_place = "Inconnue"
surface_ha = 0.0
if not df_asso.empty:
    match_asso = df_asso[df_asso['ID_Parcelle'] == selected_parcelle]
    if not match_asso.empty:
        culture_en_place = match_asso.iloc[0].get('Culture', 'Inconnue')
        try:
             surface_ha = float(match_asso.iloc[0].get('Surface_Référence_Ha', 0))
        except:
             pass

with col_sel2:
    st.markdown(f"**Culture :** {culture_en_place}")
    st.markdown(f"**Surface :** {surface_ha} ha")
    st.markdown(f"**Nombre total d'interventions tracées :** {len(df_parcelle_itk)}")

# Affichage d'un aperçu
with st.expander("👀 Voir l'Itinéraire Technique Extrait", expanded=False):
    columns_to_show = ['Date', 'Nature_Intervention', 'Nom_Produit', 'Dose_Ha', 'Type_Intervention', 'Statut_Intervention']
    existing_columns = [c for c in columns_to_show if c in df_parcelle_itk.columns]
    st.dataframe(df_parcelle_itk[existing_columns], use_container_width=True)

# Lancement de l'analyse
st.subheader("2️⃣ Analyse IA")
st.markdown("Cliquez ci-dessous pour déclencher l'audit complet de l'ITK de cette parcelle vis-à-vis d'E-Phy.")

if st.button("🚀 Lancer le Contrôle Phyto", use_container_width=True, type="primary"):
    
    if df_parcelle_itk.empty:
        st.warning("L'Itinéraire Technique est vide. Rien à analyser.")
        st.stop()
        
    with st.spinner("🕵️‍♂️ L'IA inspecte les bases de données et vos traitements..."):
        # 1. Extraction des caractéristiques du référentiel pour les produits utilisés
        produits_utilises = df_parcelle_itk['Nom_Produit'].dropna().unique().tolist()
        
        # Enlever les Nan ou vides
        produits_utilises = [str(p).strip() for p in produits_utilises if str(p).strip() and str(p).lower() != 'nan' and str(p).lower() != 'none']
        
        df_intrants = active_loader.get_intrants()
        df_usages = active_loader.get_usages_phyto()
        
        docs_intrants = ""
        docs_usages = ""
        
        if not df_intrants.empty and len(produits_utilises) > 0:
            df_intrants_filtre = df_intrants[df_intrants['Nom_Produit'].isin(produits_utilises) | df_intrants['Nom_Produit'].apply(lambda x: any(p.lower() in str(x).lower() for p in produits_utilises))]
            docs_intrants = df_intrants_filtre.to_csv(index=False)
            
            # Extract N_AMM for usages fetching
            amms = df_intrants_filtre['N_AMM'].astype(str).tolist()
            if amms and not df_usages.empty:
                df_usages_filtre = df_usages[(df_usages['N_AMM'].astype(str).isin(amms)) & (df_usages['Culture'].str.contains(culture_en_place, case=False, na=False) | df_usages['Culture'].isna())]
                docs_usages = df_usages_filtre.to_csv(index=False)
                
        # Préparation du prompt final
        prompt_columns = ['Date', 'Nature_Intervention', 'Nom_Produit', 'Dose_Ha', 'Type_Intervention', 'Outil', 'Observations']
        prompt_existing = [c for c in prompt_columns if c in df_parcelle_itk.columns]
        prompt = f"""
Voici les données à analyser :
        
=== PARCELLE ===
ID : {selected_parcelle}
Culture en place : {culture_en_place}
Surface : {surface_ha} ha

=== HISTORIQUE DES INTERVENTIONS (ITK) ===
{df_parcelle_itk[prompt_existing].to_csv(index=False)}

=== REFERENTIEL E-PHY DES INTRANTS UTILES ===
{docs_intrants if docs_intrants else 'Aucune donnée intrant trouvée.'}

=== REFERENTIEL E-PHY DES USAGES UTILES (CULTURE ACTUELLE) ===
{docs_usages if docs_usages else 'Aucune donnée usage spécifique trouvée.'}

Mission : Contrôle d'éventuelles infractions ou points de vigilance sur les produits phytosanitaires de cet ITK.
"""
        
        import time
        max_retries = 2
        for attempt in range(max_retries):
            try:
                # Appel de Gemini
                chat = model.start_chat(history=[])
                response = chat.send_message(prompt, stream=True)
                
                st.markdown("### 📋 Rapport Synthétique :")
                
                # Affichage en streaming
                message_placeholder = st.empty()
                full_response = ""
                for chunk in response:
                    full_response += chunk.text
                    message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
                break  # Succès, on sort de la boucle de retry
                
            except Exception as e:
                error_str = str(e)
                if ("429" in error_str or "quota" in error_str.lower()) and attempt < max_retries - 1:
                    st.warning(f"⚠️ Limite de l'API gratuite atteinte. Nouvelle tentative dans 17 secondes...")
                    time.sleep(17)
                else:
                    st.error(f"Erreur lors de l'appel à l'IA : {e}")
                    break

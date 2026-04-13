import streamlit as st
import pandas as pd
import google.generativeai as genai
from shared import active_loader
import requests

try:
    from streamlit_lottie import st_lottie
    LOTTIE_AVAILABLE = True
except ImportError:
    LOTTIE_AVAILABLE = False

st.set_page_config(page_title="Assistant IA", page_icon="🤖", layout="wide")

# --- Lottie Animation ---
@st.cache_data(ttl=3600)
def load_lottie_url(url: str):
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

# Robot sympa / Assistant IA animation (Bisola Ogunye - Robo)
lottie_ai = load_lottie_url("https://assets-v2.lottiefiles.com/a/b37ba8ce-118a-11ee-8e0d-07358c4a8ac9/lsVmvnyDvw.json")
lottie_ai_fallback = load_lottie_url("https://assets-v2.lottiefiles.com/a/fe807c20-1183-11ee-a7e0-738836ffd98a/LVmAcqtb4Y.json")

col_ai_anim, col_ai_title = st.columns([1, 4])

with col_ai_anim:
    ai_data = lottie_ai or lottie_ai_fallback
    if LOTTIE_AVAILABLE and ai_data:
        st_lottie(ai_data, height=120, key="lottie_assistant_ia")
    else:
        st.markdown("<div style='font-size: 4em; text-align: center; padding: 10px;'>🤖</div>", unsafe_allow_html=True)

with col_ai_title:
    st.title("🤖 Assistant IA Agricole")
    st.markdown("""<p style="font-size: 1.05em; color: #666; margin-top: -10px;">
        Posez vos questions sur votre exploitation, vos campagnes, vos stocks ou demandez des simulations 
        (rendements, trésorerie). L'IA a accès à l'ensemble de vos données.
    </p>""", unsafe_allow_html=True)

# --- Vérification de la clé API ---
api_key = st.secrets.get("GEMINI_API_KEY")
if not api_key:
    # Peut-être qu'il y a une clé "gemini" ou un autre nom
    st.warning("⚠️ Clé d'API Gemini introuvable.")
    st.info("Ajoutez `GEMINI_API_KEY = 'votre_cle'` dans votre fichier `.streamlit/secrets.toml`.")
    st.stop()

genai.configure(api_key=api_key)

# Configuration du modèle et des instructions
system_instruction = """
Tu es un conseiller agricole et un expert financier spécialisé dans la gestion d'une exploitation agricole. 
Tu as accès aux données réelles de l'exploitation (interventions, achats, stocks, assolement, consommation).
Tes missions :
1. Répondre de manière précise aux questions de l'agriculteur sur ses parcelles, ses cultures, et ses données historiques.
2. Analyser plusieurs campagnes si nécessaire pour trouver des tendances de rendement ou de coût.
3. Réaliser des calculs prospectifs et des simulations (ex: "Quel impact sur ma trésorerie si je vends aujourd'hui ?", "Quel sera mon stock à la récolte basé sur un rendement X ?").
Sois professionnel, clair, et utilise des listes ou des tableaux dans tes réponses si cela aide à la compréhension. Si une donnée manque, précise-le.
"""

@st.cache_resource
def get_model():
    return genai.GenerativeModel(
        model_name="gemini-2.0-flash",
        system_instruction=system_instruction
    )

model = get_model()

# --- Chargement du contexte (Données de l'exploitation) ---
@st.cache_data(ttl=600)
def load_farm_context():
    if not active_loader:
        return "Aucune donnée disponible (Erreur de connexion à MASTER_EXPLOITATION)."
        
    context_parts = []
    
    # 1. ACHATS & STOCK (ACHAT_MASTER)
    try:
        df_achats = active_loader._get_data("ACHAT_MASTER")
        if not df_achats.empty:
            context_parts.append("### DONNÉES ACHATS ET STOCK\n" + df_achats.to_csv(index=False))
    except Exception:
        pass

    # 2. INTERVENTIONS (JOURNAL_INTERVENTION)
    try:
        df_interventions = active_loader.get_interventions()
        if not df_interventions.empty:
            # On prend tout pour permettre l'analyse inter-campagne
            context_parts.append("### JOURNAL DES INTERVENTIONS (Toutes campagnes)\n" + df_interventions.to_csv(index=False))
    except Exception:
        pass

    # 3. ASSOLEMENT (Toutes campagnes)
    try:
        df_asso = active_loader._get_data("ASSOLEMENT")
        if not df_asso.empty:
            context_parts.append("### ASSOLEMENT ET PARCELLES\n" + df_asso.to_csv(index=False))
    except Exception:
        pass

    # 4. MATERIEL ET FUEL
    try:
        df_fuel = active_loader.get_fuel_conso() # Sans specifier de campagne pour tout avoir
        if not df_fuel.empty:
            context_parts.append("### CONSOMMATION CARBURANT\n" + df_fuel.to_csv(index=False))
    except Exception:
        pass
        
    # 5. IRRIGATION
    try:
        df_irrigation = active_loader.get_journal_irrigation()
        if not df_irrigation.empty:
            context_parts.append("### JOURNAL IRRIGATION\n" + df_irrigation.to_csv(index=False))
    except Exception:
        pass

    # 6. RECOLTE ET STOCKAGE
    try:
        df_recolte = active_loader._get_data("RECOLTE_STOCKAGE")
        if not df_recolte.empty:
            context_parts.append("### RECOLTE ET STOCKAGE\n" + df_recolte.to_csv(index=False))
    except Exception:
        pass

    # 7. CONTRATS DE VENTES
    try:
        df_contrats = active_loader._get_data("CONTRATS_VENTES")
        if not df_contrats.empty:
            context_parts.append("### CONTRATS DE VENTES\n" + df_contrats.to_csv(index=False))
    except Exception:
        pass

    # 8. PPF
    try:
        df_ppf = active_loader._get_data("PPF")
        if not df_ppf.empty:
            context_parts.append("### PLAN PREVISIONNEL DE FUMURE (PPF)\n" + df_ppf.to_csv(index=False))
    except Exception:
        pass

    # 9. JOURNAL DE MAINTENANCE
    try:
        df_maint = active_loader._get_data("JOURNAL_MAINTENANCE")
        if not df_maint.empty:
            context_parts.append("### JOURNAL DE MAINTENANCE\n" + df_maint.to_csv(index=False))
    except Exception:
        pass

    # 10. REFERENTIEL INTRANTS
    try:
        df_intrants = active_loader.get_intrants()
        if not df_intrants.empty:
            context_parts.append("### REFERENTIEL INTRANTS\n" + df_intrants.to_csv(index=False))
    except Exception:
        pass

    # 11. REFERENTIEL USAGES PHYTO
    try:
        df_usages = active_loader.get_usages_phyto()
        if not df_usages.empty:
            context_parts.append("### REFERENTIEL USAGES PHYTO\n" + df_usages.to_csv(index=False))
    except Exception:
        pass

    # 12. REFERENTIEL PARCELLES
    try:
        df_parcelles = active_loader.get_parcelles()
        if not df_parcelles.empty:
            context_parts.append("### REFERENTIEL PARCELLES\n" + df_parcelles.to_csv(index=False))
    except Exception:
        pass

    # 13. REFERENTIEL MATERIELS
    try:
        df_mat = active_loader.get_materiels()
        if not df_mat.empty:
            context_parts.append("### REFERENTIEL MATERIELS\n" + df_mat.to_csv(index=False))
    except Exception:
        pass

    # 14. RELEVES COMPTEURS (irrigation)
    try:
        df_releves = active_loader.get_releves_compteurs()
        if not df_releves.empty:
            context_parts.append("### RELEVES COMPTEURS EAU\n" + df_releves.to_csv(index=False))
    except Exception:
        pass

    # 15. RH - SUIVI HORAIRES
    try:
        df_rh = active_loader._get_data("RH_SUIVI_HORAIRES")
        if not df_rh.empty:
            context_parts.append("### RH - SUIVI DES HORAIRES\n" + df_rh.to_csv(index=False))
    except Exception:
        pass

    return "\n\n".join(context_parts)

with st.spinner("Chargement des données de l'exploitation pour l'IA..."):
    farm_context = load_farm_context()

# --- Sidebar Actions ---
with st.sidebar:
    st.header("⚙️ Options IA")
    if st.button("🗑️ Nouvelle discussion", use_container_width=True):
        st.session_state.chat_history = []
        if "gemini_chat" in st.session_state:
            del st.session_state["gemini_chat"]
        st.rerun()

# --- Interface Chat ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Initialiser le chat object de Gemini si ce n'est pas fait
if "gemini_chat" not in st.session_state:
    st.session_state.gemini_chat = model.start_chat(history=[])
    
    # Envoyer le contexte caché lors de la première initialisation
    if farm_context and "Aucune donnée" not in farm_context:
        try:
            # On envoie les données en tant que message initial silencieux (caché à l'utilisateur)
            st.session_state.gemini_chat.send_message(f"Voici les données brutes de mon exploitation. Utilise-les pour répondre à mes prochaines questions :\n{farm_context}")
        except Exception as e:
            st.error(f"Erreur lors de l'envoi du contexte à l'IA : {e}")

# Afficher l'historique de discussion
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Zone de saisie
user_input = st.chat_input("Posez votre question (ex: 'Si je vends 50t de maïs à 200€, quel impact ?')")

if user_input:
    # Affichage immédiat de la question
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Appel à l'IA
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        with st.spinner("Analyse en cours..."):
            try:
                # Appel avec stream pour un affichage progressif
                response = st.session_state.gemini_chat.send_message(user_input, stream=True)
                full_response = ""
                for chunk in response:
                    full_response += chunk.text
                    message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
                
                # Sauvegarde dans l'historique
                st.session_state.chat_history.append({"role": "assistant", "content": full_response})
            except Exception as e:
                error_msg = f"Une erreur s'est produite : {e}"
                message_placeholder.error(error_msg)
                st.session_state.chat_history.append({"role": "assistant", "content": error_msg})

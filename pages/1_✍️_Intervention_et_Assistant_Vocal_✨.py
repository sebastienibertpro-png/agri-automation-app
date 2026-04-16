import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import string
import random
import io
from datetime import datetime
import requests
try:
    from streamlit_lottie import st_lottie
    LOTTIE_AVAILABLE = True
except ImportError:
    LOTTIE_AVAILABLE = False
try:
    from gtts import gTTS
    GTTS_AVAILABLE = True
except ImportError:
    GTTS_AVAILABLE = False
from shared import init_campaign_selector, render_brand_page_header

# ── Imports optionnels ───────────────────────────────────────────────────────
try:
    from audio_recorder_streamlit import audio_recorder
    AUDIO_RECORDER_AVAILABLE = True
except ImportError:
    AUDIO_RECORDER_AVAILABLE = False

try:
    from voice_processor import (
        build_context_from_loader,
        transcribe_audio_bytes,
        transcribe_audio_followup,
        transcribe_optional_fields_bulk,
        apply_updates_to_collected_data,
        check_collected_data,
        group_interventions_by_parcelle,
        format_grouped_summary_md,
        generate_tts_summary,
        convert_collected_data_to_rows,
        compute_audio_hash,
        FIELD_DISPLAY_NAMES_FR,
        _is_empty,
        _is_empty_or_zero,
    )
    VOICE_PROCESSOR_AVAILABLE = True
    _VP_ERROR = ""
except ImportError as e:
    VOICE_PROCESSOR_AVAILABLE = False
    _VP_ERROR = str(e)


# ════════════════════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Saisie d'Intervention", page_icon="✍️", layout="wide")


# ════════════════════════════════════════════════════════════════════════════
#  CSS
# ════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Cache les widgets audio/iframe TTS ── */
.stAudio, .stAudio > div, iframe[height="0"] { display:none !important; height:0 !important; overflow:hidden !important; }

/* ── Animation ondes sonores ── */
@keyframes wave-bar {
    0%, 100% { transform: scaleY(0.3); opacity:.5; }
    50%       { transform: scaleY(1.0); opacity:1; }
}
.wave-wrap {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    height: 48px;
}
.wave-wrap span {
    display: inline-block;
    width: 5px;
    height: 38px;
    border-radius: 3px;
    background: linear-gradient(180deg, #64b5f6 0%, #1976d2 100%);
    animation: wave-bar 0.9s ease-in-out infinite;
    transform-origin: center bottom;
}
.wave-wrap span:nth-child(1) { animation-delay: 0.00s; height:20px; }
.wave-wrap span:nth-child(2) { animation-delay: 0.12s; height:35px; }
.wave-wrap span:nth-child(3) { animation-delay: 0.24s; height:48px; }
.wave-wrap span:nth-child(4) { animation-delay: 0.36s; height:40px; }
.wave-wrap span:nth-child(5) { animation-delay: 0.48s; height:28px; }
.wave-wrap span:nth-child(6) { animation-delay: 0.36s; height:40px; }
.wave-wrap span:nth-child(7) { animation-delay: 0.24s; height:48px; }
.wave-wrap span:nth-child(8) { animation-delay: 0.12s; height:35px; }
.wave-wrap span:nth-child(9) { animation-delay: 0.00s; height:20px; }

.wave-wrap-idle span {
    animation: none;
    transform: scaleY(0.25);
    opacity: .35;
}

/* ── Panel principal vocal ── */
.voice-panel {
    background: linear-gradient(135deg,
        rgba(15,23,42,0.55) 0%,
        rgba(30,41,59,0.45) 100%);
    backdrop-filter: blur(18px);
    -webkit-backdrop-filter: blur(18px);
    border: 1px solid rgba(100,181,246,0.15);
    border-radius: 20px;
    padding: 28px 28px 22px;
    margin-bottom: 18px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.25), inset 0 1px 0 rgba(255,255,255,0.06);
}

/* ── Header du panel vocal ── */
.va-header {
    display: flex;
    align-items: center;
    gap: 18px;
    margin-bottom: 16px;
    padding-bottom: 16px;
    border-bottom: 1px solid rgba(100,181,246,0.12);
}
.va-header-text h3 {
    margin: 0 0 4px 0;
    font-size: 1.15em;
    font-weight: 600;
    color: #e2e8f0;
    letter-spacing: .01em;
}

/* ── Status badge ── */
.va-status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(100,181,246,0.12);
    border: 1px solid rgba(100,181,246,0.2);
    border-radius: 20px;
    padding: 3px 12px;
    font-size: .78em;
    color: #90caf9;
    font-weight: 500;
}

/* ── Chat bubbles ── */
.chat-wrap {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 12px 4px;
    max-height: 440px;
    overflow-y: auto;
    scrollbar-width: thin;
    scrollbar-color: rgba(100,181,246,.2) transparent;
}
.chat-wrap::-webkit-scrollbar { width:5px; }
.chat-wrap::-webkit-scrollbar-thumb { background:rgba(100,181,246,.2); border-radius:3px; }

/* Agriculteur */
.bubble-user {
    align-self: flex-end;
    display: flex;
    align-items: flex-end;
    gap: 8px;
    flex-direction: row-reverse;
}
.bubble-user .bubble-body {
    background: linear-gradient(135deg, #166534 0%, #15803d 100%);
    color: #f0fdf4;
    border-radius: 18px 18px 4px 18px;
    padding: 11px 15px;
    max-width: 72%;
    font-size: .9em;
    line-height: 1.5;
    box-shadow: 0 3px 12px rgba(22,101,52,.4);
}
.bubble-user .avatar {
    width: 30px; height: 30px;
    border-radius: 50%;
    background: linear-gradient(135deg,#166534,#15803d);
    display:flex; align-items:center; justify-content:center;
    font-size:1em; flex-shrink:0;
}

/* IA */
.bubble-ai {
    align-self: flex-start;
    display: flex;
    align-items: flex-end;
    gap: 10px;
}
.bubble-ai .ai-avatar {
    width: 36px; height: 36px;
    border-radius: 50%;
    background: linear-gradient(135deg, #1565c0 0%, #0d47a1 100%);
    border: 2px solid rgba(100,181,246,0.5);
    display: flex; align-items: center; justify-content: center;
    font-size: 1.15em;
    flex-shrink: 0;
    box-shadow: 0 0 14px rgba(21,101,192,.5);
}
.bubble-ai .bubble-body {
    background: linear-gradient(135deg, rgba(13,71,161,0.85) 0%, rgba(21,101,192,0.75) 100%);
    backdrop-filter: blur(8px);
    color: #e3f2fd;
    border-radius: 18px 18px 18px 4px;
    padding: 12px 16px;
    max-width: 78%;
    font-size: .91em;
    line-height: 1.55;
    box-shadow: 0 3px 14px rgba(13,71,161,.4);
    border: 1px solid rgba(100,181,246,0.2);
}

.bubble-system {
    align-self: center;
    background: rgba(255,255,255,.05);
    border: 1px solid rgba(255,255,255,.1);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: .76em;
    color: rgba(255,255,255,.45);
    text-align: center;
}

/* ── Summary card ── */
.summary-card {
    background: rgba(13,33,55,0.6);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(100,181,246,.25);
    border-radius: 14px;
    padding: 20px 22px;
    margin: 14px 0;
    font-size: .92em;
    line-height: 1.6;
    color: #e3f2fd;
}

/* ── Question highlight ── */
.question-box {
    background: linear-gradient(135deg, rgba(26,35,126,0.7) 0%, rgba(40,53,147,0.6) 100%);
    backdrop-filter: blur(8px);
    border-left: 3px solid #42a5f5;
    border-radius: 12px;
    padding: 14px 18px;
    margin: 10px 0;
    color: #e3f2fd;
    font-size: .93em;
}

/* ── Manual section ── */
.manual-section-title {
    background: linear-gradient(90deg, #e8f5e9 0%, #f1f8f1 100%);
    padding: 14px 18px;
    border-radius: 10px;
    border-left: 5px solid #43a047;
    margin-bottom: 20px;
}

/* ── Recording pulse ── */
@keyframes rec-pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(229,57,53,.6); }
    50%       { box-shadow: 0 0 0 8px rgba(229,57,53,0); }
}
.rec-active { animation: rec-pulse 1.2s ease-in-out infinite; border-radius: 50%; }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════════════════

def load_lottieurl(url: str):
    try:
        r = requests.get(url, timeout=4)
        return r.json() if r.status_code == 200 else None
    except:
        return None


def generate_intervention_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


def get_index(options, value):
    try:
        return options.index(value)
    except:
        return 0


def tts_speak(text: str):
    """Lit un texte en français (gTTS en priorité, fallback Web Speech).
    Dans les deux cas : aucun widget visible affiché.
    """
    if not text:
        return

    # ── Méthode A : gTTS — voix Google via base64 (invisible) ─────────────────
    if GTTS_AVAILABLE:
        try:
            import base64 as _b64
            tts = gTTS(text=text, lang='fr', slow=False)
            fp = io.BytesIO()
            tts.write_to_fp(fp)
            b64 = _b64.b64encode(fp.getvalue()).decode()
            components.html(
                f'<audio autoplay style="display:none">'
                f'<source src="data:audio/mp3;base64,{b64}" type="audio/mp3">'
                f'</audio>',
                height=0
            )
            return
        except Exception:
            pass  # Fallback ci-dessous

    # ── Fallback : Web Speech API (navigateur) ─────────────────────────────
    safe = (text
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", " ")
            .replace("\r", ""))
    html = f"""
    <script>
    (function() {{
        if (!('speechSynthesis' in window)) return;
        window.speechSynthesis.cancel();
        function pickBestFrVoice(voices) {{
            var g = voices.find(function(v) {{
                return v.lang && v.lang.startsWith('fr') && v.name.toLowerCase().indexOf('google') !== -1;
            }});
            if (g) return g;
            var m = voices.find(function(v) {{
                return v.lang && v.lang.startsWith('fr') && v.name.toLowerCase().indexOf('microsoft') !== -1;
            }});
            if (m) return m;
            return voices.find(function(v) {{ return v.lang && v.lang.startsWith('fr'); }});
        }}
        function doSpeak() {{
            var u = new SpeechSynthesisUtterance('{safe}');
            u.lang = 'fr-FR'; u.rate = 0.88; u.pitch = 1.0;
            var best = pickBestFrVoice(window.speechSynthesis.getVoices());
            if (best) u.voice = best;
            window.speechSynthesis.speak(u);
        }}
        if (window.speechSynthesis.getVoices().length > 0) {{ doSpeak(); }}
        else {{ window.speechSynthesis.addEventListener('voiceschanged', doSpeak, {{once:true}}); }}
    }})();
    </script>
    """
    components.html(html, height=0)


def init_voice_state():
    """Initialise le session_state pour l'assistant vocal."""
    defaults = {
        "va_state": "welcome",        # welcome | idle | processing | questioning_critical
                                      # questioning_optional | confirming | editing | saving | done
        "va_messages": [],            # [{role: user|ai|system, text: str}]
        "va_collected_data": [],      # données extraites (list of dicts)
        "va_missing_critical": [],    # champs critiques manquants
        "va_missing_optional": [],    # champs optionnels manquants
        "va_question_idx": 0,         # index dans la liste des manquants
        "va_current_question": "",    # question posée par l'IA
        "va_current_field": "",       # champ ciblé par la question
        "va_current_field_idx": 0,    # index dans collected_data
        "va_last_audio_hash": "",     # hash du dernier audio traité
        "va_tts_queue": "",           # texte à lire via TTS
        "va_edit_field": "",          # champ en cours d'édition
        "va_skip_optional_all": False,# l'agriculteur a dit "c'est bon" pour les optionnels
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def add_message(role: str, text: str):
    st.session_state["va_messages"].append({"role": role, "text": text})


def render_chat():
    """Affiche les bulles de conversation."""
    messages = st.session_state.get("va_messages", [])
    if not messages:
        return
    html_parts = ['<div class="chat-wrap" id="chat-end">']
    for msg in messages:
        role = msg.get("role", "system")
        text = msg.get("text", "").replace("<", "&lt;").replace(">", "&gt;")
        css = {"user": "bubble-user", "ai": "bubble-ai"}.get(role, "bubble-system")
        html_parts.append(f'<div class="{css}">{text}</div>')
    html_parts.append("</div>")
    html_parts.append("""
    <script>
    var el = document.getElementById('chat-end');
    if (el) el.scrollTop = el.scrollHeight;
    </script>
    """)
    st.markdown("\n".join(html_parts), unsafe_allow_html=True)


def transition_to_optional_or_confirm():
    """Passe aux questions optionnelles ou à la confirmation."""
    optional = st.session_state.get("va_missing_optional", [])
    skip_all = st.session_state.get("va_skip_optional_all", False)

    if not optional or skip_all:
        # Passe directement à la confirmation
        grouped = group_interventions_by_parcelle(st.session_state["va_collected_data"])
        tts_txt = generate_tts_summary(grouped)
        ai_msg = "Parfait, j'ai toutes les informations ! Voici le résumé de votre intervention :"
        add_message("ai", ai_msg)
        st.session_state["va_tts_queue"] = tts_txt
        st.session_state["va_state"] = "confirming"
    else:
        # UNE SEULE question globale pour tous les champs optionnels
        nature = ""
        if st.session_state.get("va_collected_data"):
            nature = str(st.session_state["va_collected_data"][0].get("Nature_Intervention", ""))
        extras = "tracteur, outil et stade de culture"
        if "Traitement" in nature:
            extras = "tracteur, outil, stade de culture et cible du traitement"
        elif "colte" in nature or "Moisson" in nature:
            extras = "tracteur, outil, humidité et poids spécifique"
        elif "Semis" in nature:
            extras = "tracteur, outil et PMG"
        question = (
            f"Voulez-vous ajouter des informations complémentaires comme {extras}, "
            f"des observations ou des conditions météo ? "
            f"Dites ce que vous voulez ajouter, ou dites \u00abrien\u00bb pour continuer."
        )
        ai_msg = f"Votre intervention est enregistrée ! {question}"
        add_message("ai", ai_msg)
        st.session_state["va_tts_queue"] = ai_msg
        st.session_state["va_current_question"] = question
        st.session_state["va_current_field"] = "_optional_bulk"
        st.session_state["va_state"] = "questioning_optional"


def process_initial_audio(audio_bytes: bytes, context: dict, api_key: str):
    """Traite le premier enregistrement (description complète de l'intervention)."""
    with st.spinner("🤖 Analyse de votre intervention en cours…"):
        result = transcribe_audio_bytes(audio_bytes, context, api_key)

    if not result:
        add_message("system", "❌ Aucune donnée extraite. Recommencez.")
        st.session_state["va_state"] = "idle"
        return

    first = result[0]
    if "error" in first:
        err = first.get("error", "")
        raw = first.get("raw", "")[:120]
        add_message("system", f"❌ Erreur : {err} — {raw}")
        st.session_state["va_state"] = "idle"
        return

    st.session_state["va_collected_data"] = result
    completeness = check_collected_data(result)
    st.session_state["va_missing_critical"] = completeness["critical_missing"]
    st.session_state["va_missing_optional"] = completeness["optional_missing"]

    if completeness["is_critical_complete"]:
        transition_to_optional_or_confirm()
    else:
        # Première question critique
        first_missing = completeness["critical_missing"][0]
        question = first_missing["label"]
        nb_missing = len(completeness["critical_missing"])
        if nb_missing == 1:
            ai_msg = f"J'ai bien compris votre intervention. Une seule chose manque : {question}"
        else:
            ai_msg = f"J'ai bien noté ! Il me manque quelques informations. D'abord : {question}"
        add_message("ai", ai_msg)
        st.session_state["va_tts_queue"] = ai_msg
        st.session_state["va_current_question"] = question
        st.session_state["va_current_field"] = first_missing["field"]
        st.session_state["va_current_field_idx"] = first_missing.get("item_index", 0)
        st.session_state["va_question_idx"] = 0
        st.session_state["va_state"] = "questioning_critical"


def process_followup_audio(audio_bytes: bytes, context: dict, api_key: str, optional_mode: bool = False):
    """Traite une réponse à une question de suivi."""
    current_data = st.session_state.get("va_collected_data", [])

    if optional_mode:
        # ── Question optionnelle GLOBALE : une seule passe ──
        with st.spinner("📝 Enregistrement des informations complémentaires…"):
            result = transcribe_optional_fields_bulk(audio_bytes, current_data, context, api_key)

        raw_text = result.get("raw_text", "…")
        add_message("user", raw_text)

        if result.get("error"):
            add_message("system", f"⚠️ Transcription : {result['error'][:80]}")

        if not result.get("skip") and result.get("updates"):
            st.session_state["va_collected_data"] = apply_updates_to_collected_data(
                current_data, result["updates"]
            )
            added = ", ".join(result["updates"].keys())
            ai_msg = f"Parfait ! J'ai ajouté : {added}. Voici le résumé final."
        else:
            ai_msg = "Très bien, on continue sans informations supplémentaires. Voici le résumé."

        add_message("ai", ai_msg)
        # Générer le résumé vocal et passer à la confirmation
        grouped = group_interventions_by_parcelle(st.session_state["va_collected_data"])
        tts_txt = generate_tts_summary(grouped)
        st.session_state["va_tts_queue"] = tts_txt
        st.session_state["va_skip_optional_all"] = True
        st.session_state["va_state"] = "confirming"
        return

    # ── Question critique classique ──
    question = st.session_state.get("va_current_question", "")
    field = st.session_state.get("va_current_field", "")
    field_idx = st.session_state.get("va_current_field_idx", 0)

    with st.spinner("📝 Mise à jour…"):
        result = transcribe_audio_followup(audio_bytes, question, field, current_data, context, api_key)

    raw_text = result.get("raw_text", "…")
    add_message("user", raw_text)

    if result.get("error"):
        add_message("system", f"⚠️ Erreur de transcription : {result['error'][:80]}")

    if not result.get("skip"):
        updates = result.get("updates", {})
        if updates:
            st.session_state["va_collected_data"] = apply_updates_to_collected_data(
                current_data, updates, field_idx
            )
    # Vérifier les champs critiques restants
    completeness = check_collected_data(st.session_state["va_collected_data"])
    st.session_state["va_missing_critical"] = completeness["critical_missing"]
    st.session_state["va_missing_optional"] = completeness["optional_missing"]

    if completeness["is_critical_complete"]:
        if result.get("skip"):
            ai_msg = "D'accord, je laisse ce champ vide. Passons à la suite."
            add_message("ai", ai_msg)
            st.session_state["va_tts_queue"] = ai_msg
        transition_to_optional_or_confirm()
    else:
        # Vérifier les champs critiques restants
        completeness = check_collected_data(st.session_state["va_collected_data"])
        st.session_state["va_missing_critical"] = completeness["critical_missing"]
        st.session_state["va_missing_optional"] = completeness["optional_missing"]

        if completeness["is_critical_complete"]:
            if result.get("skip"):
                ai_msg = "D'accord, je laisse ce champ vide. Passons à la suite."
                add_message("ai", ai_msg)
                st.session_state["va_tts_queue"] = ai_msg
            transition_to_optional_or_confirm()
        else:
            # Question critique suivante
            remaining = completeness["critical_missing"]
            next_missing = remaining[0]
            next_question = next_missing["label"]
            if result.get("skip"):
                ai_msg = f"D'accord. Alors : {next_question}"
            else:
                ai_msg = f"Parfait, merci ! {next_question}"
            add_message("ai", ai_msg)
            st.session_state["va_tts_queue"] = ai_msg
            st.session_state["va_current_question"] = next_question
            st.session_state["va_current_field"] = next_missing["field"]
            st.session_state["va_current_field_idx"] = next_missing.get("item_index", 0)


# ════════════════════════════════════════════════════════════════════════════
#  HEADER
# ════════════════════════════════════════════════════════════════════════════
is_edit_mode = False
edit_data = {}
if "edit_intervention" in st.session_state and st.session_state.edit_intervention:
    is_edit_mode = True
    edit_data = st.session_state.edit_intervention
    render_brand_page_header("Modifier l'Intervention", "Correction d'une saisie existante", icon="✍️")
    st.info(f"Mode Édition — {edit_data.get('Date', '')} | {edit_data.get('ID_Parcelle', '')}")
    if st.button("❌ Annuler l'édition"):
        st.session_state.edit_intervention = None
        st.rerun()
else:
    render_brand_page_header(
        "Saisie d'Intervention",
        "Dictez à la voix ou saisissez manuellement ✨",
        icon="✍️"
    )

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# ════════════════════════════════════════════════════════════════════════════
#  ONGLETS
# ════════════════════════════════════════════════════════════════════════════
if is_edit_mode:
    # Mode édition → forcer l'onglet manuel directement
    tab_voice, tab_manual = st.tabs(["🎙️ Assistant de Saisie Vocal", "✍️ Saisie Manuelle"])
else:
    tab_voice, tab_manual = st.tabs(["🎙️ Assistant de Saisie Vocal", "✍️ Saisie Manuelle"])


# ════════════════════════════════════════════════════════════════════════════
#  ONGLET 1 — ASSISTANT VOCAL
# ════════════════════════════════════════════════════════════════════════════
with tab_voice:
    init_voice_state()

    va_state = st.session_state["va_state"]
    api_key = st.secrets.get("GEMINI_API_KEY", "")

    # ── TTS : lire le texte en attente ────────────────────────────────────
    tts_text = st.session_state.get("va_tts_queue", "")
    if tts_text:
        tts_speak(tts_text)
        st.session_state["va_tts_queue"] = ""

    # ── Vérifications prérequis ───────────────────────────────────────────
    _voice_ok = True
    if not AUDIO_RECORDER_AVAILABLE:
        st.error("⚠️ Module `audio_recorder_streamlit` non installé. Lancez : `pip install audio-recorder-streamlit`")
        _voice_ok = False
    elif not VOICE_PROCESSOR_AVAILABLE:
        st.error(f"⚠️ Module vocal non disponible : {_VP_ERROR}")
        _voice_ok = False
    elif not api_key:
        st.error("⚠️ Clé API Gemini manquante dans les secrets.")
        _voice_ok = False

    # ── Chargement du contexte (uniquement si prérequis OK) ───────────────
    if _voice_ok:
        context = build_context_from_loader(active_loader, selected_campaign)

    # ── Panel principal ───────────────────────────────────────────────────
    st.markdown('<div class="voice-panel">', unsafe_allow_html=True)

    # Header du panel
    col_icon, col_title = st.columns([1, 6])
    with col_icon:
        if LOTTIE_AVAILABLE:
            lottie = load_lottieurl("https://lottie.host/819d4546-d248-4389-9b93-b6d4fe754a6d/m8e1Pz3C7H.json")
            if lottie:
                st_lottie(lottie, height=80, key="va_lottie")
            else:
                st.markdown("<div style='font-size:2.5em;text-align:center'>🎙️</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:2.5em;text-align:center'>🎙️</div>", unsafe_allow_html=True)
    with col_title:
        st.markdown("### 🤖 Assistant de Saisie Vocal")
        state_labels = {
            "welcome": "🟢 Prêt",
            "idle": "🟢 En attente",
            "processing": "⏳ Analyse…",
            "questioning_critical": "❓ Question",
            "questioning_optional": "💡 Question facultative",
            "confirming": "✅ Confirmation",
            "editing": "✏️ Modification",
            "saving": "💾 Enregistrement…",
            "done": "✅ Enregistré",
        }
        st.markdown(
            f'<div class="va-status">{state_labels.get(va_state, va_state)}</div>',
            unsafe_allow_html=True
        )

    st.divider()

    # ── Accueil (premier lancement) ───────────────────────────────────────
    if va_state == "welcome":
        st.markdown("""
        <div style="color:#90caf9; font-size:.95em; line-height:1.7; padding:10px 0">
        👋 <b>Bienvenue dans l'assistant vocal !</b><br>
        Décrivez votre intervention à voix haute — je m'occupe de tout enregistrer.<br>
        <br>
        Exemples de phrases :<br>
        &nbsp;• <i>"J'ai traité les Buissons avec du Peak à 0,25 L/ha avec le 220 CVX"</i><br>
        &nbsp;• <i>"Fongicide sur la Grande Plaine et la Petite, du Priori Xtra à 0,8 L/ha"</i><br>
        &nbsp;• <i>"Semis de blé sur la Longue à 330 grains par m², PMG 45 grammes"</i>
        </div>
        """, unsafe_allow_html=True)

        if st.button("🎙️ Commencer la saisie vocale", type="primary", use_container_width=True, key="btn_start_va"):
            welcome_msg = (
                "Bonjour ! Décrivez-moi votre intervention et j'enregistrerai tout pour vous. "
                "Par exemple : j'ai traité les Buissons avec du Peak à zéro virgule vingt-cinq litres par hectare avec le 220 CVX !"
            )
            add_message("ai", "Bonjour ! Décrivez-moi votre intervention et j'enregistrerai tout pour vous.")
            st.session_state["va_tts_queue"] = welcome_msg
            st.session_state["va_state"] = "idle"
            st.rerun()

    # ── États actifs ──────────────────────────────────────────────────────
    elif va_state in ["idle", "questioning_critical", "questioning_optional"]:

        # Afficher l'historique de conversation
        render_chat()

        # Question en cours (highlighting)
        if va_state in ["questioning_critical", "questioning_optional"] and st.session_state.get("va_current_question"):
            q_color = "#1a237e" if va_state == "questioning_critical" else "#1b3a2d"
            q_border = "#42a5f5" if va_state == "questioning_critical" else "#66bb6a"
            q_label = "❓ Question" if va_state == "questioning_critical" else "💡 Info facultative"
            st.markdown(
                f'<div class="question-box" style="background:linear-gradient(135deg,{q_color},#283593);border-left-color:{q_border};">'
                f'<b>{q_label} :</b> {st.session_state["va_current_question"]}'
                f'</div>',
                unsafe_allow_html=True
            )

        # Recorder audio
        st.markdown("<br>", unsafe_allow_html=True)
        if va_state == "idle":
            rec_label = "🎙️ Cliquez pour dicter votre intervention"
        else:
            rec_label = "🎙️ Cliquez pour répondre"

        col_rec, col_hint = st.columns([1, 2])
        with col_rec:
            audio_bytes = audio_recorder(
                text=rec_label,
                recording_color="#e53935",
                neutral_color="#1565c0",
                icon_size="2x",
                pause_threshold=300.0,
                sample_rate=16000,
                key=f"va_rec_{va_state}"
            )
        with col_hint:
            if va_state == "idle":
                st.caption("Appuyez sur le micro, parlez, puis réappuyez pour arrêter.")
                st.caption("L'icône devient **rouge** pendant l'enregistrement.")
            else:
                if va_state == "questioning_optional":
                    st.caption("💡 Ce champ est **facultatif** — dites «passer» ou «c'est bon» pour ignorer.")
                else:
                    st.caption("Répondez directement à la question posée.")

        # Traitement automatique si nouvel audio
        if audio_bytes:
            new_hash = compute_audio_hash(audio_bytes)
            if new_hash != st.session_state.get("va_last_audio_hash", ""):
                st.session_state["va_last_audio_hash"] = new_hash
                # Ajouter un message système
                if va_state == "idle":
                    add_message("user", "🎙️ [Audio enregistré]")
                    process_initial_audio(audio_bytes, context, api_key)
                elif va_state == "questioning_critical":
                    process_followup_audio(audio_bytes, context, api_key, optional_mode=False)
                elif va_state == "questioning_optional":
                    process_followup_audio(audio_bytes, context, api_key, optional_mode=True)
                st.rerun()

        # Bouton reset
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🗑️ Recommencer", key="btn_reset_va", use_container_width=False):
            for k in ["va_state","va_messages","va_collected_data","va_missing_critical",
                      "va_missing_optional","va_question_idx","va_current_question",
                      "va_current_field","va_current_field_idx","va_last_audio_hash",
                      "va_tts_queue","va_edit_field","va_skip_optional_all"]:
                st.session_state.pop(k, None)
            st.rerun()

    # ── CONFIRMATION ──────────────────────────────────────────────────────
    elif va_state == "confirming":

        render_chat()

        # Résumé groupé par parcelle
        collected = st.session_state.get("va_collected_data", [])
        if collected:
            grouped = group_interventions_by_parcelle(collected)
            summary_md = format_grouped_summary_md(grouped)
            st.markdown('<div class="summary-card">', unsafe_allow_html=True)
            st.markdown("#### 📋 Résumé de l'intervention")
            st.markdown(summary_md)
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.warning("Aucune donnée à afficher.")

        st.markdown("---")
        st.markdown("**Que souhaitez-vous faire ?**")

        col_v, col_m, col_a = st.columns(3)
        with col_v:
            if st.button("✅ Valider et enregistrer", type="primary", use_container_width=True, key="btn_va_validate"):
                add_message("user", "✅ Valider l'intervention")
                st.session_state["va_state"] = "saving"
                st.rerun()
        with col_m:
            if st.button("✏️ Modifier une donnée", use_container_width=True, key="btn_va_edit"):
                st.session_state["va_state"] = "editing"
                st.rerun()
        with col_a:
            if st.button("❌ Annuler", use_container_width=True, key="btn_va_cancel"):
                add_message("ai", "Intervention annulée. Vous pouvez recommencer quand vous voulez.")
                st.session_state["va_tts_queue"] = "Intervention annulée. Je réinitialise l'assistant."
                for k in ["va_state","va_messages","va_collected_data","va_missing_critical",
                          "va_missing_optional","va_question_idx","va_current_question",
                          "va_current_field","va_current_field_idx","va_last_audio_hash",
                          "va_edit_field","va_skip_optional_all"]:
                    st.session_state.pop(k, None)
                st.rerun()

    # ── EDITION ───────────────────────────────────────────────────────────
    elif va_state == "editing":

        st.markdown("#### ✏️ Modifier une donnée")
        st.caption("Sélectionnez le champ à modifier et saisissez la nouvelle valeur.")

        collected = st.session_state.get("va_collected_data", [])
        first_item = collected[0] if collected else {}

        # Construire les options de champs éditables
        editable_fields = {}
        skip_keys = {"Type_Action", "ID_Intervention", "ID_Irrigation", "Unité_Quantité",
                     "Quantité_Totale_Produit", "Volume_Total_Bouillie_L",
                     "Quantité_semence_totale", "Quantité_Récoltée_Totale",
                     "N/ha", "P/ha", "K/ha"}
        for k, v in first_item.items():
            if k not in skip_keys and not _is_empty(v):
                label = FIELD_DISPLAY_NAMES_FR.get(k, k)
                editable_fields[label] = k

        # Ajouter les champs vides importants pour les compléter
        import_fields_to_add = [
            "Tracteur","Outil","Stade_Culture","Observations","Cible",
            "Volume_Bouillie_L_Ha","Humidité_récolte","PS"
        ]
        for fld in import_fields_to_add:
            label = FIELD_DISPLAY_NAMES_FR.get(fld, fld)
            if label not in editable_fields:
                editable_fields[label] = fld

        if editable_fields:
            selected_label = st.selectbox("Champ à modifier", list(editable_fields.keys()), key="edit_field_sel")
            selected_field = editable_fields[selected_label]
            current_val = str(first_item.get(selected_field, ""))
            new_val = st.text_input(f"Nouvelle valeur pour « {selected_label} »", value=current_val, key="edit_field_val")

            col_ok, col_back = st.columns(2)
            with col_ok:
                if st.button("✅ Appliquer la modification", type="primary", use_container_width=True, key="btn_apply_edit"):
                    updates = {selected_field: new_val}
                    st.session_state["va_collected_data"] = apply_updates_to_collected_data(
                        collected, updates
                    )
                    st.session_state["va_state"] = "confirming"
                    st.rerun()
            with col_back:
                if st.button("← Retour au résumé", use_container_width=True, key="btn_back_confirm"):
                    st.session_state["va_state"] = "confirming"
                    st.rerun()

    # ── ENREGISTREMENT ────────────────────────────────────────────────────
    elif va_state == "saving":

        collected = st.session_state.get("va_collected_data", [])
        rows = convert_collected_data_to_rows(collected)

        if not rows:
            st.error("Aucune donnée d'intervention à enregistrer.")
            st.session_state["va_state"] = "confirming"
            st.rerun()

        df_new = pd.DataFrame(rows)

        with st.spinner("💾 Enregistrement dans Google Sheets…"):
            try:
                success = active_loader.bulk_insert_interventions(df_new)
            except Exception as e:
                success = False
                st.error(f"❌ Erreur : {e}")

        if success:
            ai_msg = "Votre intervention a bien été enregistrée ! Je réinitialise l'assistant pour une nouvelle saisie."
            add_message("ai", "✅ Intervention enregistrée avec succès !")
            st.session_state["va_tts_queue"] = ai_msg
            st.session_state["va_state"] = "done"
            st.rerun()
        else:
            st.error("❌ Échec de l'enregistrement. Veuillez réessayer.")
            st.session_state["va_state"] = "confirming"
            st.rerun()

    # ── DONE ──────────────────────────────────────────────────────────────
    elif va_state == "done":
        render_chat()
        st.success("✅ Intervention enregistrée avec succès dans Google Sheets !")

        col_new, col_consult = st.columns(2)
        with col_new:
            if st.button("➕ Nouvelle saisie vocale", type="primary", use_container_width=True, key="btn_new_va"):
                for k in ["va_state","va_messages","va_collected_data","va_missing_critical",
                          "va_missing_optional","va_question_idx","va_current_question",
                          "va_current_field","va_current_field_idx","va_last_audio_hash",
                          "va_tts_queue","va_edit_field","va_skip_optional_all"]:
                    st.session_state.pop(k, None)
                st.rerun()
        with col_consult:
            st.page_link("pages/2_📋_Consulter_Interventions.py", label="📋 Consulter les interventions", icon="📋")

        st.markdown("</div>", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
#  ONGLET 2 — SAISIE MANUELLE
# ════════════════════════════════════════════════════════════════════════════
with tab_manual:

    if is_edit_mode:
        st.info(f"Mode Édition activé pour l'intervention du {edit_data.get('Date', '')} sur {edit_data.get('ID_Parcelle', '')}")

    st.markdown(
        '<div class="manual-section-title"><h3>✍️ Saisie Manuelle de l\'Intervention</h3>'
        '<p style="margin:0;opacity:.75;">Remplissez le formulaire ci-dessous pour enregistrer une intervention.</p></div>',
        unsafe_allow_html=True
    )

    st.divider()

    # ── Source des valeurs par défaut ─────────────────────────────────────
    _src = edit_data if is_edit_mode else {}

    # ── 1. Informations Générales ─────────────────────────────────────────
    st.markdown("##### 1. Informations Générales")

    nature_options = ["Traitement","Fertilisation","Semis","Déchaumage","Préparation Printemps","Binage","Fissuration","Récolte"]
    default_nature = get_index(nature_options, _src.get('Nature_Intervention', 'Traitement'))
    nature_interv = st.selectbox("Nature de l'intervention", nature_options, index=default_nature, key="man_nature")

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
        else:
            default_date = datetime.now().date()
        date_interv = st.date_input("Date de l'intervention", value=default_date, key="man_date")
    with col_g2:
        default_statut = get_index(["Prévu","Réalisé"], _src.get('Statut_Intervention','Réalisé'))
        statut = st.selectbox("Statut", ["Prévu","Réalisé"], index=default_statut, key="man_statut")
    with col_g3:
        default_campagne = int(_src.get('Campagne', selected_campaign) or selected_campaign)
        campagne_saisie = st.number_input("Campagne", value=default_campagne, format="%d", key="man_campagne")

    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        if nature_interv == "Traitement":
            default_type = get_index(["Herbicide","Fongicide","Insecticide","Régulateur","Autre"], _src.get('Type_Intervention','Herbicide'))
            type_interv = st.selectbox("Type d'intervention", ["Herbicide","Fongicide","Insecticide","Régulateur","Autre"], index=default_type, key="man_type")
        elif nature_interv == "Fertilisation":
            default_type = get_index(["Minérale","Organique","Foliaire"], _src.get('Type_Intervention','Minérale'))
            type_interv = st.selectbox("Type d'intervention", ["Minérale","Organique","Foliaire"], index=default_type, key="man_type_f")
        else:
            type_interv = st.text_input("Type d'intervention", value=_src.get('Type_Intervention',''), disabled=True, key="man_type_other")
    with col_m2:
        tracteur_options = ["130_CVX","220_CVX","Berthoud_Raptor","Axial_5140"]
        default_tracteur = get_index(tracteur_options, _src.get('Tracteur','130_CVX'))
        tracteur = st.selectbox("Tracteur", tracteur_options, index=default_tracteur, key="man_tracteur")
    with col_m3:
        outil_options = ["- Aucun -","Agata","Ependeur_Engrais","DDI","Rotative","Cultivateur_Bonnel","Bineuse","Fissurateur","Rabe"]
        default_outil = get_index(outil_options, _src.get('Outil','- Aucun -'))
        outil = st.selectbox("Outil", outil_options, index=default_outil, key="man_outil")

    stade_options = ["","Pré-levée","Levée","2F","4-6F","8-10F","12F","Floraison","Tallage","Epis 1cm","Montaison","Maturité","Récolte"]
    default_stade = get_index(stade_options, _src.get('Stade_Culture',''))
    stade = st.selectbox("Stade Culture", stade_options, index=default_stade, key="man_stade")

    if nature_interv == "Traitement":
        try:
            default_vol = float(_src.get('Volume_Bouillie_L_Ha', 100.0) or 100.0)
        except:
            default_vol = 100.0
        volume_bouillie = st.number_input("Volume Bouillie (L/ha)", min_value=0.0, value=default_vol, step=10.0, key="man_vol_bouillie")
    else:
        volume_bouillie = 0.0

    observations = st.text_input("Observations", value=_src.get('Observations',''), key="man_obs")

    # ── 2. Parcelles ──────────────────────────────────────────────────────
    st.markdown("##### 2. Choix des Parcelles")

    if is_edit_mode:
        default_parcelles = [edit_data['ID_Parcelle']]
    else:
        default_parcelles = []

    selected_p_for_entry = st.multiselect("Parcelles concernées", available_parcelles, default=default_parcelles, key="man_parcelles")

    parcelles_data = []
    if selected_p_for_entry:
        st.markdown("*Surfaces travaillées (Ajustables)*")
        metadata = active_loader.get_parcel_metadata(campagne_saisie)
        cols = st.columns(len(selected_p_for_entry) if len(selected_p_for_entry) < 4 else 4)
        for i, p_id in enumerate(selected_p_for_entry):
            p_meta = metadata.get(p_id, {})
            culture_ref = p_meta.get('Culture', 'Inconnue')
            if is_edit_mode and p_id == edit_data['ID_Parcelle']:
                try:
                    surf_ref = float(edit_data.get('Surface_Travaillée_Ha', 0.0) or 0.0)
                except:
                    surf_ref = 0.0
            else:
                try:
                    surf_ref = float(str(p_meta.get('Surface', 0.0)).replace(',', '.'))
                except:
                    surf_ref = 0.0
            with cols[i % 4]:
                surf_input = st.number_input(f"{p_id} ({culture_ref})", value=surf_ref, step=0.5, key=f"man_surf_{p_id}")
                parcelles_data.append({'id': p_id, 'culture': culture_ref, 'surface': float(surf_input)})

    # ── 3. Détails Intervention ───────────────────────────────────────────
    st.markdown("##### 3. Détails de l'Intervention")

    produits_data = []
    semis_data = {}
    recolte_data = {}

    try:
        df_intrants = active_loader._get_data("REF_INTRANTS")
    except:
        df_intrants = pd.DataFrame()

    raw_products = []
    if is_edit_mode:
        df_raw = active_loader.get_interventions()
        mask = df_raw['ID_Intervention'].isin(edit_data['ID_Intervention'])
        raw_products = df_raw[mask].to_dict('records')

    if nature_interv == "Traitement":
        liste_produits = []
        if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
            if 'Type' in df_intrants.columns:
                phyto_df = df_intrants[~df_intrants['Type'].str.contains('Engrais', na=False, case=False)]
                liste_produits = sorted(phyto_df['Nom_Produit'].dropna().unique().tolist())
            else:
                liste_produits = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
        if not liste_produits:
            liste_produits = ["(Saisir manuellement)"]

        try:
            df_usages_ref = active_loader.get_usages_phyto()
        except:
            df_usages_ref = pd.DataFrame()

        def get_cibles_for_product(nom_produit):
            if df_usages_ref.empty or 'Nom_Produit' not in df_usages_ref.columns:
                return []
            sub = df_usages_ref[df_usages_ref['Nom_Produit'].astype(str).str.upper() == str(nom_produit).upper()]
            return sorted([str(c) for c in sub['Cible'].dropna().unique().tolist() if str(c).strip()])

        def get_dose_for_cible(nom_produit, cible):
            if df_usages_ref.empty:
                return None, None
            sub = df_usages_ref[
                (df_usages_ref['Nom_Produit'].astype(str).str.upper() == str(nom_produit).upper()) &
                (df_usages_ref['Cible'].astype(str) == str(cible))
            ]
            if sub.empty:
                return None, None
            dose_raw = str(sub['Dose_Max'].iloc[0]).replace(',', '.')
            dose = pd.to_numeric(dose_raw, errors='coerce')
            unite = sub['Unite_Dose'].iloc[0] if 'Unite_Dose' in sub.columns else None
            return (float(dose) if not pd.isna(dose) else None), unite

        for i in range(1, 6):
            c1, c2, c3, c4 = st.columns([2, 1.5, 1, 1])
            p_val = "- Aucun -"; c_val = ""; d_val = 0.0; u_val = "L/ha"

            if is_edit_mode and (i - 1) < len(raw_products):
                row_p = raw_products[i - 1]
                p_val = row_p.get('Nom_Produit', "- Aucun -")
                c_val = row_p.get('Cible', "")
                try:
                    d_val = float(row_p.get('Dose_Ha', 0.0))
                except:
                    d_val = 0.0
                u_val = row_p.get('Unité_Dose', "L/ha")

            with c1:
                prod = st.selectbox(f"Produit {i}", ["- Aucun -"] + liste_produits,
                                    key=f"man_prod_name_{i}",
                                    index=get_index(["- Aucun -"] + liste_produits, p_val))
            cible_val = ""
            if prod != "- Aucun -":
                cibles_dispo = get_cibles_for_product(prod)
                with c2:
                    if cibles_dispo:
                        cible_val = st.selectbox(f"Cible {i}", [""] + cibles_dispo,
                                                 key=f"man_prod_cible_{i}",
                                                 index=get_index([""] + cibles_dispo, c_val))
                    else:
                        cible_val = st.text_input(f"Cible {i}", key=f"man_prod_cible_txt_{i}", value=c_val)
                auto_dose, auto_unite = get_dose_for_cible(prod, cible_val) if cible_val else (None, None)
            else:
                with c2:
                    st.text_input(f"Cible {i}", key=f"man_cible_dis_{i}", disabled=True)
                auto_dose, auto_unite = None, None

            ck_prod = f"man_lp_{i}"; ck_cible = f"man_lc_{i}"
            if ck_prod not in st.session_state:
                st.session_state[ck_prod] = p_val
            if ck_cible not in st.session_state:
                st.session_state[ck_cible] = c_val

            unite_options = ["L/ha", "Kg/ha", "g/ha"]
            if st.session_state[ck_prod] != prod or st.session_state[ck_cible] != cible_val:
                st.session_state[ck_prod] = prod
                st.session_state[ck_cible] = cible_val
                st.session_state[f"man_prod_dose_{i}"] = float(auto_dose) if auto_dose is not None else 0.0
                st.session_state[f"man_prod_unite_{i}"] = auto_unite if auto_unite in unite_options else "L/ha"
            elif is_edit_mode and f"man_first_load_{i}" not in st.session_state:
                st.session_state[f"man_prod_dose_{i}"] = d_val
                st.session_state[f"man_prod_unite_{i}"] = u_val
                st.session_state[f"man_first_load_{i}"] = True

            with c3:
                dose = st.number_input(f"Dose/ha", min_value=0.0, step=0.1, key=f"man_prod_dose_{i}")
            with c4:
                unite = st.selectbox("Unité", unite_options, key=f"man_prod_unite_{i}")
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
        if not liste_engrais:
            liste_engrais = ["(Saisir manuellement)"]

        e_val = "- Aucun -"; d_f_val = 100.0; u_f_val = "Kg/ha"
        if is_edit_mode and raw_products:
            row_e = raw_products[0]
            e_val = row_e.get('Nom_Produit', "- Aucun -")
            try:
                d_f_val = float(row_e.get('Dose_Ha', 0.0))
            except:
                d_f_val = 100.0
            u_f_val = row_e.get('Unité_Dose', "Kg/ha")

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            engrais_prod = st.selectbox("Engrais", ["- Aucun -"] + liste_engrais,
                                        index=get_index(["- Aucun -"] + liste_engrais, e_val), key="man_engrais")
        with c2:
            dose_ferti = st.number_input("Dose/ha", min_value=0.0, step=10.0, value=d_f_val, key="man_dose_ferti")
        with c3:
            unite_ferti = st.selectbox("Unité", ["Kg/ha","L/ha","T/ha"],
                                       index=get_index(["Kg/ha","L/ha","T/ha"], u_f_val), key="man_unite_ferti")

        pct_n = pct_p = pct_k = 0.0
        if engrais_prod != "- Aucun -" and not df_intrants.empty:
            row_e2 = df_intrants[df_intrants['Nom_Produit'] == engrais_prod]
            if not row_e2.empty:
                def _g(col):
                    if col in row_e2.columns:
                        try:
                            return float(str(row_e2[col].iloc[0]).replace(',', '.'))
                        except:
                            return 0.0
                    return 0.0
                pct_n = _g('Element_N'); pct_p = _g('Element_P'); pct_k = _g('Element_K')

        def _ratio(v):
            return v if abs(v) <= 1.0 and v != 0 else v / 100.0

        mult = 1000.0 if unite_ferti == "T/ha" else 1.0
        n_ha = round((dose_ferti * mult) * _ratio(pct_n), 1)
        p_ha = round((dose_ferti * mult) * _ratio(pct_p), 1)
        k_ha = round((dose_ferti * mult) * _ratio(pct_k), 1)
        st.markdown(f"**Apports Calculés :** N: `{n_ha}` | P: `{p_ha}` | K: `{k_ha}`")

        if engrais_prod != "- Aucun -":
            produits_data.append({'nom': engrais_prod, 'cible': '', 'dose': dose_ferti,
                                  'unite': unite_ferti, 'N_ha': n_ha, 'P_ha': p_ha, 'K_ha': k_ha})

    elif nature_interv == "Semis":
        liste_semences = []
        if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
            if 'Type' in df_intrants.columns:
                sem_df = df_intrants[df_intrants['Type'].str.contains('Semence', na=False, case=False)]
                liste_semences = sorted(sem_df['Nom_Produit'].dropna().unique().tolist())
            else:
                liste_semences = sorted(df_intrants['Nom_Produit'].dropna().unique().tolist())
        if not liste_semences:
            liste_semences = ["(Saisir manuellement)"]

        s_val = "- Aucun -"; dens_val = 0.0; u_dens_val = "Grains/m²"; pmg_val = 0.0
        if is_edit_mode and raw_products:
            row_s = raw_products[0]
            s_val = row_s.get('Nom_Produit', "- Aucun -")
            try:
                dens_val = float(row_s.get('Densité_Semis', 0.0))
            except:
                dens_val = 0.0
            u_dens_val = row_s.get('Unité_Densité', "Grains/m²")
            try:
                pmg_val = float(row_s.get('PMG', 0.0))
            except:
                pmg_val = 0.0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            semence_prod = st.selectbox("Semence / Variété", ["- Aucun -"] + liste_semences,
                                        index=get_index(["- Aucun -"] + liste_semences, s_val), key="man_semence")
        with c2:
            densite = st.number_input("Densité (Unité/ha)", min_value=0.0, step=1.0, value=dens_val, key="man_densite")
        with c3:
            unite_densite = st.selectbox("Unité Semis", ["Grains/m²","Doses/ha","Kg/ha"],
                                         index=get_index(["Grains/m²","Doses/ha","Kg/ha"], u_dens_val), key="man_u_densite")
        with c4:
            pmg = st.number_input("PMG (g)", min_value=0.0, step=1.0, value=pmg_val, key="man_pmg")

        if semence_prod != "- Aucun -":
            semis_data = {'nom': semence_prod, 'densite': densite, 'unite': unite_densite, 'pmg': pmg, 'assoc_prods': []}

    elif nature_interv == "Récolte":
        r_val = ""; rdt_val = 0.0; h_val = 14.0; ps_val = 76.0
        if is_edit_mode and raw_products:
            row_r = raw_products[0]
            r_val = row_r.get('Produit_Récolté', "")
            try:
                rdt_val = float(row_r.get('Rendement_Ha', 0.0))
            except:
                rdt_val = 0.0
            try:
                h_val = float(row_r.get('Humidité_récolte', 14.0))
            except:
                h_val = 14.0
            try:
                ps_val = float(row_r.get('PS', 76.0))
            except:
                ps_val = 76.0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            prod_recolte = st.text_input("Produit Récolté", value=r_val, placeholder="Ex: Blé Tendre", key="man_prod_rec")
        with c2:
            rdt_ha = st.number_input("Rendement (Qx/ha ou T/ha)", min_value=0.0, step=0.1, value=rdt_val, key="man_rdt")
        with c3:
            humidite = st.number_input("Humidité (%)", min_value=0.0, value=h_val, step=0.1, key="man_hum")
        with c4:
            ps = st.number_input("PS", min_value=0.0, value=ps_val, step=0.1, key="man_ps")
        if prod_recolte:
            recolte_data = {'produit': prod_recolte, 'rendement': rdt_ha, 'humidite': humidite, 'ps': ps}

    # ── Bouton enregistrement ─────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    btn_label = "Mettre à jour l'intervention 🔄" if is_edit_mode else f"Enregistrer ({nature_interv}) 🚀"
    submitted = st.button(btn_label, type="primary", key="man_submit")

    if submitted:
        if not selected_p_for_entry:
            st.error("Veuillez sélectionner au moins une parcelle.")
        elif nature_interv in ["Traitement","Fertilisation"] and not produits_data:
            st.error("Veuillez ajouter au moins un produit.")
        elif nature_interv == "Semis" and not semis_data:
            st.error("Veuillez sélectionner une semence.")
        elif nature_interv == "Récolte" and not recolte_data:
            st.error("Veuillez saisir le produit récolté.")
        else:
            rows_to_insert = []
            for p in parcelles_data:
                uid = (edit_data.get('ID_Intervention')[0]
                       if is_edit_mode and isinstance(edit_data.get('ID_Intervention'), list)
                       else (edit_data.get('ID_Intervention') or generate_intervention_id()))
                if len(parcelles_data) > 1 and is_edit_mode:
                    uid = generate_intervention_id()

                base_row = {
                    'ID_Intervention': uid, 'ID_Parcelle': p['id'], 'Campagne': campagne_saisie,
                    'Date': date_interv.strftime('%d/%m/%Y'), 'Statut_Intervention': statut,
                    'Nature_Intervention': nature_interv, 'Type_Intervention': type_interv,
                    'Culture': p['culture'], 'Surface_Travaillée_Ha': p['surface'],
                    'Tracteur': tracteur, 'Outil': outil if outil != "- Aucun -" else "",
                    'Stade_Culture': stade, 'Observations': observations,
                    'Nom_Produit': '', 'Cible': '', 'Dose_Ha': '', 'Unité_Dose': '',
                    'Quantité_Totale_Produit': '', 'Unité_Quantité': '',
                    'N/ha': '', 'P/ha': '', 'K/ha': '',
                    'Volume_Bouillie_L_Ha': volume_bouillie if volume_bouillie > 0 else '',
                    'Volume_Total_Bouillie_L': '',
                    'Densité_Semis': '', 'Unité_Densité': '', 'PMG': '', 'Quantité_semence_totale': '',
                    'Produit_Récolté': '', 'Rendement_Ha': '', 'Humidité_récolte': '',
                    'PS': '', 'Quantité_Récoltée_Totale': ''
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
                        row['N/ha'] = prod.get('N_ha', '')
                        row['P/ha'] = prod.get('P_ha', '')
                        row['K/ha'] = prod.get('K_ha', '')
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
                        qte = (semis_data['densite'] * 10000 * semis_data['pmg'] / 1000000) * p['surface'] if semis_data['pmg'] > 0 else 0
                    row['Quantité_semence_totale'] = round(qte, 2)
                    rows_to_insert.append(row)

                elif nature_interv == "Récolte":
                    row = base_row.copy()
                    row['Produit_Récolté'] = recolte_data['produit']
                    row['Rendement_Ha'] = recolte_data['rendement']
                    row['Humidité_récolte'] = recolte_data['humidite']
                    row['PS'] = recolte_data['ps']
                    row['Quantité_Récoltée_Totale'] = round(recolte_data['rendement'] * p['surface'], 2)
                    rows_to_insert.append(row)

                else:
                    rows_to_insert.append(base_row)

            df_new = pd.DataFrame(rows_to_insert)
            with st.spinner("Mise à jour du journal…"):
                if is_edit_mode:
                    active_loader.delete_interventions(edit_data['ID_Intervention'])
                success = active_loader.bulk_insert_interventions(df_new)
                if success:
                    st.success("✅ Mis à jour !" if is_edit_mode else "✅ Enregistré avec succès !")
                    if is_edit_mode:
                        st.session_state.edit_intervention = None
                        st.rerun()
                else:
                    st.error("❌ Échec de l'enregistrement.")

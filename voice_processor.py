"""
voice_processor.py - Assistant Vocal Conversationnel v2.0

Supporte la conversation bidirectionnelle avec détection proactive des champs
manquants et réponse vocale de l'IA.
Rétrocompatible avec v1.
"""

import json
import tempfile
import os
import datetime
import hashlib
import google.generativeai as genai


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION DES CHAMPS
# ─────────────────────────────────────────────────────────────────────────────

CRITICAL_FIELDS_MAP = {
    "base": ["ID_Parcelle", "Nature_Intervention"],
    "Traitement": ["Nom_Produit", "Dose_Ha", "Unité_Dose", "Type_Intervention", "Volume_Bouillie_L_Ha"],
    "Fertilisation": ["Nom_Produit", "Dose_Ha", "Unité_Dose"],
    "Récolte": ["Produit_Récolté", "Rendement_Ha"],
    "Moisson": ["Produit_Récolté", "Rendement_Ha"],
    "Semis": ["Nom_Produit", "Densité_Semis"],
    "Déchaumage": [],
    "Préparation Printemps": [],
    "Binage": [],
    "Fissuration": [],
}

OPTIONAL_FIELDS_MAP = {
    "base": ["Tracteur", "Stade_Culture", "Observations"],
    "Traitement": ["Cible"],
    "Fertilisation": [],
    "Récolte": ["PS", "Humidité_récolte"],
    "Moisson": ["PS", "Humidité_récolte"],
    "Semis": ["PMG", "Unité_Densité"],
    "Déchaumage": [],
    "Préparation Printemps": [],
    "Binage": [],
    "Fissuration": [],
}

FIELD_QUESTIONS_FR = {
    "ID_Parcelle": "Sur quelle parcelle avez-vous travaillé ?",
    "Nature_Intervention": "Quel type d'intervention avez-vous réalisé ? Traitement, fertilisation, semis ou récolte ?",
    "Nom_Produit": "Quel produit avez-vous utilisé ?",
    "Dose_Ha": "Quelle dose avez-vous appliqué à l'hectare ?",
    "Unité_Dose": "En quelle unité ? Litres par hectare ou kilos par hectare ?",
    "Type_Intervention": "De quel type de traitement s'agissait-il ? Herbicide, fongicide, insecticide ou régulateur ?",
    "Volume_Bouillie_L_Ha": "Quel volume de bouillie avez-vous utilisé, en litres par hectare ?",
    "Rendement_Ha": "Quel est le rendement à l'hectare que vous avez obtenu ?",
    "Produit_Récolté": "Quel produit avez-vous récolté ?",
    "Densité_Semis": "Quelle densité de semis avez-vous utilisée ?",
    "Tracteur": "Quel tracteur avez-vous utilisé ?",
    "Stade_Culture": "À quel stade de culture se trouvait la culture au moment de l'intervention ?",
    "Cible": "Quelle était la cible du traitement ?",
    "PS": "Quel est le poids spécifique, en kilos par hectolitre ?",
    "Humidité_récolte": "Quelle était l'humidité à la récolte, en pourcent ?",
    "PMG": "Quel est le PMG de la semence, en grammes ?",
    "Unité_Densité": "En quelle unité la densité de semis ? Grains par mètre carré, doses à l'hectare, ou kilos à l'hectare ?",
    "Observations": "Avez-vous des observations à ajouter ?",
    "ID_Secteur": "Quel secteur d'irrigation ?",
    "Volume_mm": "Quel volume d'eau avez-vous apporté, en millimètres ?",
}

FIELD_DISPLAY_NAMES_FR = {
    "ID_Parcelle": "Parcelle",
    "Nature_Intervention": "Nature d'intervention",
    "Nom_Produit": "Produit",
    "Dose_Ha": "Dose à l'ha",
    "Unité_Dose": "Unité de dose",
    "Type_Intervention": "Type de traitement",
    "Volume_Bouillie_L_Ha": "Volume bouillie (L/ha)",
    "Rendement_Ha": "Rendement (Qx/ha)",
    "Produit_Récolté": "Produit récolté",
    "Densité_Semis": "Densité de semis",
    "Tracteur": "Tracteur",
    "Outil": "Outil",
    "Stade_Culture": "Stade de culture",
    "Cible": "Cible",
    "PS": "Poids spécifique",
    "Humidité_récolte": "Humidité récolte (%)",
    "PMG": "PMG (g)",
    "Unité_Densité": "Unité densité",
    "Observations": "Observations",
    "Date": "Date",
    "Statut_Intervention": "Statut",
    "Campagne": "Campagne",
    "Culture": "Culture",
    "Surface_Travaillée_Ha": "Surface travaillée (ha)",
}


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS INTERNES
# ─────────────────────────────────────────────────────────────────────────────

def _is_empty(val) -> bool:
    if val is None:
        return True
    return str(val).strip().lower() in ["", "null", "none", "nan", "inconnu", "?", "inconnue"]


def _is_empty_or_zero(val) -> bool:
    if _is_empty(val):
        return True
    try:
        return float(str(val).replace(',', '.')) <= 0
    except:
        return True


def compute_audio_hash(audio_bytes: bytes) -> str:
    return hashlib.md5(audio_bytes).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
#  CONTEXT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_context_from_loader(active_loader, selected_campaign) -> dict:
    """Construit le contexte de référence depuis le DataLoader."""
    context = {"parcelles": [], "intrants": [], "materiels": [], "secteurs": []}

    try:
        import pandas as pd
        df_parcelles = active_loader._get_data("REF_PARCELLES")
        df_asso = active_loader.get_assolement(selected_campaign)
        culture_map = {}
        if not df_asso.empty and 'ID_Parcelle' in df_asso.columns:
            for _, row in df_asso.iterrows():
                pid = str(row.get('ID_Parcelle', '')).strip()
                cult = str(row.get('Culture', '')).strip()
                if pid:
                    culture_map[pid] = cult
        if not df_parcelles.empty:
            for _, row in df_parcelles.iterrows():
                pid = str(row.get('ID_Parcelle', '') or row.get('ID', '')).strip()
                pnom = str(row.get('Nom_Terrain', '') or row.get('Nom', '')).strip()
                surf_raw = str(row.get('Surface_Référence_Ha', '') or row.get('Surface', '')).replace(',', '.').strip()
                try:
                    surf_str = f" ({float(surf_raw)} ha)"
                except:
                    surf_str = ""
                if not pid:
                    continue
                culture = culture_map.get(pid, "")
                name_part = f" - {pnom}" if pnom and pnom != pid else ""
                culture_part = f" [Culture: {culture}]" if culture else ""
                context["parcelles"].append(f"{pid}{name_part}{surf_str}{culture_part}")
    except Exception as e:
        print(f"[VP] Erreur parcelles: {e}")

    try:
        df_intrants = active_loader._get_data("REF_INTRANTS")
        if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
            context["intrants"] = sorted(
                df_intrants['Nom_Produit'].dropna().astype(str).str.strip().tolist()
            )
    except Exception as e:
        print(f"[VP] Erreur intrants: {e}")

    try:
        df_mat = active_loader.get_materiels()
        if not df_mat.empty:
            for col in ['ID_Materiel', 'ID']:
                if col in df_mat.columns:
                    context["materiels"] = df_mat[col].dropna().astype(str).tolist()
                    break
    except Exception as e:
        print(f"[VP] Erreur matériels: {e}")

    try:
        df_secteurs = active_loader._get_data("REF_SECTEURS")
        if not df_secteurs.empty and 'ID_Secteur' in df_secteurs.columns:
            for _, row in df_secteurs.iterrows():
                sid = str(row.get('ID_Secteur', '')).strip()
                if sid:
                    context["secteurs"].append({
                        "ID_Secteur": sid,
                        "ID_Parcelle": str(row.get('ID_Parcelle', '')),
                        "Surface_Secteur_Ha": str(row.get('Surface_Secteur_Ha', '')),
                    })
    except:
        pass

    return context


# ─────────────────────────────────────────────────────────────────────────────
#  PROMPT D'EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _build_extraction_prompt(context_data: dict) -> str:
    parcelles_str = "\n".join(context_data.get('parcelles', []))
    products_str = ", ".join(context_data.get('intrants', []))
    materiels_str = ", ".join(context_data.get('materiels', []))
    secteurs_list = context_data.get('secteurs', [])
    secteurs_str = ", ".join([s.get('ID_Secteur', '') for s in secteurs_list if s.get('ID_Secteur')])
    today_date = datetime.date.today().strftime("%d/%m/%Y")
    today_month = datetime.date.today().month
    today_year = datetime.date.today().year
    campagne_courante = today_year + 1 if today_month >= 7 else today_year

    return f"""Tu es un assistant agricole technique expert. Ton rôle est d'extraire les détails d'une intervention culturale depuis un enregistrement audio pour un logiciel de traçabilité.

Date du jour : {today_date} (Campagne par défaut : {campagne_courante})

DONNÉES DE RÉFÉRENCE (utilise EXCLUSIVEMENT ces noms s'ils correspondent phonétiquement) :
- Parcelles (format: ID - Nom (surface ha) [Culture]) :
{parcelles_str}
- Produits/Intrants : {products_str}
- Matériels (Tracteur/Outil) : {materiels_str}
- Secteurs Irrigation : {secteurs_str}

CONSIGNES :
1. Détermine si c'est une INTERVENTION culturale ou une IRRIGATION.
2. Pour INTERVENTION, identifie la Nature (Semis, Traitement, Fertilisation, Déchaumage, Préparation Printemps, Binage, Fissuration, Récolte, Moisson).
3. Si Traitement → Type = Herbicide, Fongicide, Insecticide, ou Régulateur. Si l'utilisateur parle de désherbage/désherbant → OBLIGATOIREMENT Herbicide.
4. ID_Parcelle : renvoie l'ID EXACT (avant le premier tiret). Ne PAS inclure le nom du terrain.
5. Surface_Travaillée_Ha : si non précisée, utilise la surface de référence entre parenthèses.
6. Quantité_Totale_Produit = Dose_Ha × Surface_Travaillée_Ha (calcule automatiquement).
7. Tracteur : seulement si c'est un engin motorisé (130_CVX, 220_CVX, Berthoud_Raptor, Axial_5140). Sinon → Outil.
8. Si plusieurs produits ou parcelles → une entrée JSON par produit ET par parcelle.
9. Culture : inscris la culture indiquée entre crochets [Culture: ...] de la parcelle concernée.
10. Statut : "Prévu" si intention future, sinon "Réalisé". Par défaut : "Réalisé".
11. ID_Intervention : génère un ID unique aléatoire de 8 caractères (majuscules + chiffres).

FORMAT DE SORTIE — liste JSON uniquement (commence par [ et finit par ]) :
[
  {{
    "Type_Action": "INTERVENTION",
    "ID_Intervention": "XXXXXXXX",
    "Date": "{today_date}",
    "Statut_Intervention": "Réalisé",
    "Campagne": {campagne_courante},
    "ID_Parcelle": "",
    "Culture": "",
    "Surface_Travaillée_Ha": "",
    "Nature_Intervention": "",
    "Type_Intervention": "",
    "Nom_Produit": "",
    "Cible": "",
    "Dose_Ha": "",
    "Unité_Dose": "",
    "Volume_Bouillie_L_Ha": "",
    "Quantité_Totale_Produit": "",
    "Unité_Quantité": "",
    "N/ha": "",
    "P/ha": "",
    "K/ha": "",
    "Densité_Semis": "",
    "Unité_Densité": "",
    "PMG": "",
    "Quantité_semence_totale": "",
    "Produit_Récolté": "",
    "Rendement_Ha": "",
    "Humidité_récolte": "",
    "PS": "",
    "Quantité_Récoltée_Totale": "",
    "Tracteur": "",
    "Outil": "",
    "Stade_Culture": "",
    "Observations": ""
  }}
]

Pour IRRIGATION :
[
  {{
    "Type_Action": "IRRIGATION",
    "ID_Irrigation": "XXXXXXXX",
    "Date": "JJ/MM/AAAA",
    "Campagne": {campagne_courante},
    "ID_Secteur": "",
    "Volume_mm": "",
    "Stade_Culture": ""
  }}
]

Valeurs numériques avec POINT (pas virgule). RÉPONDS UNIQUEMENT EN JSON brut sans balises markdown."""


# ─────────────────────────────────────────────────────────────────────────────
#  EXTRACTION INITIALE
# ─────────────────────────────────────────────────────────────────────────────

def transcribe_audio_bytes(audio_bytes: bytes, context_data: dict, api_key: str,
                            audio_format: str = "wav") -> list:
    """Transcrit et extrait les données d'intervention depuis des bytes audio."""
    if not api_key:
        return [{"error": "NO_API_KEY", "raw": "Clé API Gemini manquante."}]

    genai.configure(api_key=api_key)
    tmp_path = None
    audio_file = None

    try:
        with tempfile.NamedTemporaryFile(suffix=f".{audio_format}", delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name

        audio_file = genai.upload_file(path=tmp_path)
        prompt = _build_extraction_prompt(context_data)
        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content([prompt, audio_file])
        text = response.text.strip()

        for prefix in ["```json", "```"]:
            if text.startswith(prefix):
                text = text[len(prefix):]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        return json.loads(text)

    except json.JSONDecodeError:
        raw = text if 'text' in dir() else "Réponse vide"
        return [{"error": "JSON_ERROR", "raw": raw}]
    except Exception as e:
        return [{"error": "GEMINI_ERROR", "raw": str(e)}]
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        if audio_file:
            try:
                genai.delete_file(name=audio_file.name)
            except:
                pass


# ─────────────────────────────────────────────────────────────────────────────
#  DÉTECTION DES CHAMPS MANQUANTS
# ─────────────────────────────────────────────────────────────────────────────

def check_collected_data(collected_data: list) -> dict:
    """
    Analyse les données collectées et identifie les champs manquants.
    Returns: {
        'critical_missing': [{'field': str, 'item_index': int, 'label': str}],
        'optional_missing': [{'field': str, 'item_index': int, 'label': str}],
        'is_critical_complete': bool
    }
    """
    if not collected_data:
        return {
            'critical_missing': [
                {'field': 'ID_Parcelle', 'item_index': 0, 'label': FIELD_QUESTIONS_FR['ID_Parcelle']},
                {'field': 'Nature_Intervention', 'item_index': 0, 'label': FIELD_QUESTIONS_FR['Nature_Intervention']},
            ],
            'optional_missing': [],
            'is_critical_complete': False,
        }

    critical_missing = []
    optional_missing = []
    seen_critical = set()
    seen_optional = set()

    for idx, item in enumerate(collected_data):
        type_action = item.get("Type_Action", "INTERVENTION")

        if type_action == "IRRIGATION":
            for field in ["ID_Secteur", "Volume_mm"]:
                if field not in seen_critical and _is_empty(item.get(field)):
                    seen_critical.add(field)
                    critical_missing.append({
                        'field': field, 'item_index': idx,
                        'label': FIELD_QUESTIONS_FR.get(field, field)
                    })
            continue

        # Base critical fields
        for field in CRITICAL_FIELDS_MAP["base"]:
            if field not in seen_critical and _is_empty(item.get(field)):
                seen_critical.add(field)
                critical_missing.append({
                    'field': field, 'item_index': idx,
                    'label': FIELD_QUESTIONS_FR.get(field, field)
                })

        # Nature-specific critical fields
        nature = item.get("Nature_Intervention", "")
        for field in CRITICAL_FIELDS_MAP.get(nature, []):
            if field in seen_critical:
                continue
            val = item.get(field)
            numeric_fields = ["Dose_Ha", "Rendement_Ha", "Densité_Semis", "Volume_Bouillie_L_Ha"]
            is_missing = _is_empty_or_zero(val) if field in numeric_fields else _is_empty(val)
            if is_missing:
                seen_critical.add(field)
                critical_missing.append({
                    'field': field, 'item_index': idx,
                    'label': FIELD_QUESTIONS_FR.get(field, field)
                })

        # Optional fields
        opt_fields = OPTIONAL_FIELDS_MAP.get("base", []) + OPTIONAL_FIELDS_MAP.get(nature, [])
        for field in opt_fields:
            if field not in seen_optional and _is_empty(item.get(field)):
                seen_optional.add(field)
                optional_missing.append({
                    'field': field, 'item_index': idx,
                    'label': FIELD_QUESTIONS_FR.get(field, field)
                })

    return {
        'critical_missing': critical_missing,
        'optional_missing': optional_missing,
        'is_critical_complete': len(critical_missing) == 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  TRANSCRIPTION FOLLOW-UP (réponses aux questions de suivi)
# ─────────────────────────────────────────────────────────────────────────────

def transcribe_audio_followup(audio_bytes: bytes, question_asked: str, field_name: str,
                               current_data: list, context_data: dict, api_key: str,
                               audio_format: str = "wav") -> dict:
    """
    Transcrit une réponse vocale à une question de suivi ciblée.
    Returns: {'raw_text': str, 'skip': bool, 'updates': dict}
    """
    if not api_key:
        return {"raw_text": "", "skip": False, "updates": {}, "error": "NO_API_KEY"}

    genai.configure(api_key=api_key)
    tmp_path = None
    audio_file = None

    try:
        with tempfile.NamedTemporaryFile(suffix=f".{audio_format}", delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name

        audio_file = genai.upload_file(path=tmp_path)

        parcelles_str = "\n".join(context_data.get('parcelles', []))
        products_str = ", ".join(context_data.get('intrants', []))
        materiels_str = ", ".join(context_data.get('materiels', []))
        current_summary = json.dumps(current_data, ensure_ascii=False, indent=2)

        prompt = f"""Tu es un assistant agricole. Tu viens de poser cette question à un agriculteur :
« {question_asked} »

L'audio contient sa réponse. Les données déjà collectées sont :
{current_summary}

CONTEXTE DE RÉFÉRENCE :
- Parcelles : {parcelles_str}
- Produits : {products_str}
- Matériels : {materiels_str}

TÂCHE :
1. Transcris fidèlement ce que dit l'agriculteur.
2. Si l'agriculteur dit "c'est bon", "passer", "laisse vide", "ok", "non", "pas la peine", "ça va", "suivant", "skip" → c'est un SKIP (il ne veut pas préciser ce champ, mets skip=true).
3. Sinon, extrais la valeur pour le champ "{field_name}". Utilise les noms EXACTS des listes de référence pour les parcelles, produits et matériels.
4. Si l'agriculteur donne aussi des informations sur d'autres champs → capture-les dans "updates" également.
5. Pour un champ numérique, retourne un nombre (pas une chaîne).

RÉPONDS UNIQUEMENT avec ce JSON :
{{
  "raw_text": "transcription de ce qu'a dit l'agriculteur",
  "skip": false,
  "updates": {{
    "{field_name}": "valeur extraite (null si skip)",
    "autre_champ_optionnel_si_mentionné": "valeur"
  }}
}}"""

        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content([prompt, audio_file])
        text = response.text.strip()

        for prefix in ["```json", "```"]:
            if text.startswith(prefix):
                text = text[len(prefix):]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        return json.loads(text)

    except json.JSONDecodeError:
        return {"raw_text": text if 'text' in locals() else "", "skip": False, "updates": {}, "error": "JSON_ERROR"}
    except Exception as e:
        return {"raw_text": "", "skip": False, "updates": {}, "error": str(e)}
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        if audio_file:
            try:
                genai.delete_file(name=audio_file.name)
            except:
                pass


# ─────────────────────────────────────────────────────────────────────────────
#  MISE À JOUR DES DONNÉES COLLECTÉES
# ─────────────────────────────────────────────────────────────────────────────

def apply_updates_to_collected_data(collected_data: list, updates: dict,
                                     target_index: int = None) -> list:
    """Applique les mises à jour à la liste de données collectées."""
    if not collected_data:
        return collected_data

    updated = [item.copy() for item in collected_data]

    for field, value in updates.items():
        if value is None or (isinstance(value, str) and value.lower() in ["null", "none", ""]):
            continue

        if target_index is not None and target_index < len(updated):
            updated[target_index][field] = value
        else:
            # Update all items (typically same field across parcelles)
            for item in updated:
                current_val = item.get(field)
                if _is_empty(current_val):
                    item[field] = value

    return updated


# ─────────────────────────────────────────────────────────────────────────────
#  QUESTION UNIQUE – CHAMPS OPTIONNELS
# ─────────────────────────────────────────────────────────────────────────────

def transcribe_optional_fields_bulk(
    audio_bytes: bytes,
    current_data: list,
    context_data: dict,
    api_key: str,
    question_asked: str = "Souhaitez-vous ajouter des informations complémentaires ?"
) -> dict:
    """
    Transcrit la réponse globale de l'agriculteur à la question des champs optionnels.
    Extrait en une seule passe : tracteur, outil, stade de culture, conditions météo,
    cible, observations, et tout autre champ pertinent mentionné.
    Retourne : {"raw_text": str, "updates": dict, "skip": bool, "error": str|None}
    """
    import tempfile, os

    materiels = context_data.get("materiels", [])
    parcelles = context_data.get("parcelles", [])
    first = current_data[0] if current_data else {}
    nature = str(first.get("Nature_Intervention", "")).strip()

    prompt = f"""Tu es l'assistant vocal d'un agriculteur.
Il vient de répondre à la question : "{question_asked}"

Interprète sa réponse et extrais les informations facultatives mentionnées.

VALEURS DE RÉFÉRENCE :
- Matériels disponibles : {json.dumps(materiels, ensure_ascii=False)}
- Nature de l'intervention en cours : {nature}

Règles :
- Si l'agriculteur dit "non", "rien", "c'est bon", "continuer", "valider", "passer" ou similaire
  => retourne {{"skip": true, "updates": {{}}}}
- Sinon extrais tous les champs mentionnés parmi :
  - "Tracteur" : ex. "130 CVX", "220 CVX", "Berthoud Raptor" (normalise avec les matériels disponibles)
  - "Outil" : ex. "Agata", "DDI", "Rotative"
  - "Stade_Culture" : ex. "2F", "tallage", "montaison", "6F"
  - "Cible" : ex. "ray-grass", "vulpin", "blé", adventices
  - "Observations" : toute remarque, condition météo, note libre
  - "Humidite_recolte" : si mentionné en %
  - "PS" : poids spécifique si mentionné

Renvoie UNIQUEMENT ce JSON (pas de texte autour) :
{{
  "raw_text": "[verbatim]",
  "skip": false,
  "updates": {{
    "Tracteur": "...",
    "Outil": "...",
    "Stade_Culture": "...",
    "Cible": "...",
    "Observations": "..."
  }}
}}
N'inclure dans updates QUE les champs effectivement mentionnés (pas les champs vides).
"""

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            tmp.write(audio_bytes)
            tmp_path = tmp.name

        audio_file = genai.upload_file(path=tmp_path, mime_type="audio/wav")
        response = model.generate_content([prompt, audio_file])
        os.unlink(tmp_path)

        raw = response.text.strip()
        # Nettoyer markdown
        for marker in ["```json", "```JSON", "```"]:
            raw = raw.replace(marker, "")
        raw = raw.strip()

        data = json.loads(raw)
        return {
            "raw_text": data.get("raw_text", raw[:80]),
            "updates": data.get("updates", {}),
            "skip": bool(data.get("skip", False)),
            "error": None,
        }

    except Exception as e:
        return {
            "raw_text": "[Transcription échouée]",
            "updates": {},
            "skip": False,
            "error": str(e),
        }


# ─────────────────────────────────────────────────────────────────────────────
#  GROUPEMENT PAR PARCELLE
# ─────────────────────────────────────────────────────────────────────────────

def group_interventions_by_parcelle(data_list: list) -> dict:
    """Groupe les interventions par parcelle pour un affichage condensé."""
    groups = {}

    for item in data_list:
        type_action = item.get("Type_Action", "INTERVENTION")

        if type_action == "IRRIGATION":
            key = f"__irr__{item.get('ID_Secteur', '?')}"
            if key not in groups:
                groups[key] = {
                    "type": "IRRIGATION",
                    "secteur": item.get("ID_Secteur", "?"),
                    "volume": item.get("Volume_mm", "?"),
                    "date": item.get("Date", ""),
                    "stade": item.get("Stade_Culture", ""),
                }
            continue

        parcelle = item.get("ID_Parcelle") or "Parcelle inconnue"
        key = parcelle

        if key not in groups:
            groups[key] = {
                "type": "INTERVENTION",
                "parcelle": parcelle,
                "culture": item.get("Culture", ""),
                "surface": item.get("Surface_Travaillée_Ha", ""),
                "nature": item.get("Nature_Intervention", "Intervention"),
                "type_intervention": item.get("Type_Intervention", ""),
                "statut": item.get("Statut_Intervention", "Réalisé"),
                "date": item.get("Date", ""),
                "tracteur": item.get("Tracteur", ""),
                "outil": item.get("Outil", ""),
                "stade": item.get("Stade_Culture", ""),
                "volume_bouillie": item.get("Volume_Bouillie_L_Ha", ""),
                "observations": item.get("Observations", ""),
                "produits": [],
                "recolte": None,
                "semis": None,
            }

        nom_produit = item.get("Nom_Produit", "")
        if nom_produit and not _is_empty(nom_produit):
            groups[key]["produits"].append({
                "nom": nom_produit,
                "dose": item.get("Dose_Ha", ""),
                "unite": item.get("Unité_Dose", ""),
                "cible": item.get("Cible", ""),
                "qte_totale": item.get("Quantité_Totale_Produit", ""),
                "n_ha": item.get("N/ha", ""),
                "p_ha": item.get("P/ha", ""),
                "k_ha": item.get("K/ha", ""),
            })

        if item.get("Produit_Récolté") and not _is_empty(item.get("Produit_Récolté")):
            groups[key]["recolte"] = {
                "produit": item.get("Produit_Récolté", ""),
                "rendement": item.get("Rendement_Ha", ""),
                "humidite": item.get("Humidité_récolte", ""),
                "ps": item.get("PS", ""),
                "qte_totale": item.get("Quantité_Récoltée_Totale", ""),
            }

        if item.get("Densité_Semis") and not _is_empty_or_zero(item.get("Densité_Semis")):
            groups[key]["semis"] = {
                "variete": item.get("Nom_Produit", ""),
                "densite": item.get("Densité_Semis", ""),
                "unite": item.get("Unité_Densité", ""),
                "pmg": item.get("PMG", ""),
            }

    return groups


# ─────────────────────────────────────────────────────────────────────────────
#  GÉNÉRATION DU RÉSUMÉ
# ─────────────────────────────────────────────────────────────────────────────

def generate_tts_summary(grouped: dict) -> str:
    """Génère le texte du résumé destiné à être lu par la voix de l'IA."""
    parts = []
    for group in grouped.values():
        if group["type"] == "IRRIGATION":
            parts.append(
                f"Irrigation de {group['volume']} millimètres sur le secteur {group['secteur']}."
            )
            continue

        nature = group["nature"]
        type_int = group.get("type_intervention", "")
        parcelle = group["parcelle"]
        surface = group.get("surface", "")
        date = group.get("date", "")

        line = nature
        if type_int:
            line += f" de type {type_int}"
        line += f" sur la parcelle {parcelle}"
        if surface:
            line += f", {surface} hectares"
        if date:
            line += f", le {date}"
        line += "."

        for prod in group.get("produits", []):
            if prod["nom"]:
                p_line = f"Produit {prod['nom']}"
                if prod["dose"] and prod["unite"]:
                    p_line += f" à {prod['dose']} {prod['unite']}"
                line += " " + p_line + "."

        if group.get("recolte"):
            r = group["recolte"]
            r_line = f"Récolte de {r['produit']}"
            if r["rendement"]:
                r_line += f" à {r['rendement']} par hectare"
            line += " " + r_line + "."

        parts.append(line)

    if not parts:
        return "Aucune donnée à résumer."

    total = " ".join(parts)
    return (
        f"Voici ce que j'ai enregistré. {total} "
        f"Souhaitez-vous valider, annuler, ou modifier une donnée ?"
    )


def format_grouped_summary_md(grouped: dict) -> str:
    """Génère un résumé markdown groupé par parcelle."""
    lines = []

    for group in grouped.values():
        if group["type"] == "IRRIGATION":
            lines.append(f"### 💧 Irrigation — Secteur {group['secteur']}")
            lines.append(f"- Volume : **{group['volume']} mm**")
            if group.get("date"):
                lines.append(f"- Date : {group['date']}")
            lines.append("")
            continue

        nature = group["nature"]
        type_int = group.get("type_intervention", "")
        statut_emoji = "⏳" if str(group.get("statut", "")).lower() == "prévu" else "✅"

        header = f"### {statut_emoji} {nature}"
        if type_int:
            header += f" — {type_int}"
        lines.append(header)

        parcelle_line = f"📍 **{group['parcelle']}**"
        if group.get("culture"):
            parcelle_line += f"  [{group['culture']}]"
        if group.get("surface") and not _is_empty_or_zero(group.get("surface")):
            parcelle_line += f"  ·  {group['surface']} ha"
        if group.get("date"):
            parcelle_line += f"  ·  {group['date']}"
        lines.append(parcelle_line)

        for prod in group.get("produits", []):
            if prod["nom"]:
                p = f"  🧪 **{prod['nom']}**"
                if prod.get("dose") and not _is_empty_or_zero(prod.get("dose")):
                    p += f" — {prod['dose']} {prod.get('unite', '')}"
                if prod.get("qte_totale") and not _is_empty_or_zero(prod.get("qte_totale")):
                    unite_simple = str(prod.get("unite", "")).replace("/ha", "").replace("/Ha", "")
                    p += f"  *(total : {prod['qte_totale']} {unite_simple})*"
                if prod.get("cible") and not _is_empty(prod.get("cible")):
                    p += f"  ← *{prod['cible']}*"
                lines.append(p)
                # NPK for fertilisation
                if prod.get("n_ha") and not _is_empty_or_zero(prod.get("n_ha")):
                    lines.append(f"    N: {prod['n_ha']} | P: {prod.get('p_ha','')} | K: {prod.get('k_ha','')}")

        vol = group.get("volume_bouillie", "")
        if vol and not _is_empty_or_zero(vol):
            lines.append(f"  💧 Volume bouillie : **{vol} L/ha**")

        if group.get("recolte"):
            r = group["recolte"]
            r_line = f"  🌾 **{r['produit']}**"
            if r.get("rendement") and not _is_empty_or_zero(r.get("rendement")):
                r_line += f" — {r['rendement']} Qx/ha"
            if r.get("humidite") and not _is_empty_or_zero(r.get("humidite")):
                r_line += f" — Hum. {r['humidite']}%"
            if r.get("ps") and not _is_empty_or_zero(r.get("ps")):
                r_line += f" — PS {r['ps']}"
            if r.get("qte_totale") and not _is_empty_or_zero(r.get("qte_totale")):
                r_line += f"  *(total : {r['qte_totale']} Qx)*"
            lines.append(r_line)

        if group.get("semis"):
            s = group["semis"]
            s_line = f"  🌱 **{s['variete']}**"
            if s.get("densite") and not _is_empty_or_zero(s.get("densite")):
                s_line += f" — {s['densite']} {s.get('unite', '')}"
            if s.get("pmg") and not _is_empty_or_zero(s.get("pmg")):
                s_line += f" — PMG {s['pmg']} g"
            lines.append(s_line)

        equip = []
        if group.get("tracteur") and not _is_empty(group.get("tracteur")):
            equip.append(f"🚜 {group['tracteur']}")
        if group.get("outil") and not _is_empty(group.get("outil")):
            equip.append(f"⚙️ {group['outil']}")
        if group.get("stade") and not _is_empty(group.get("stade")):
            equip.append(f"🌿 Stade : {group['stade']}")
        if equip:
            lines.append("  " + " · ".join(equip))

        if group.get("observations") and not _is_empty(group.get("observations")):
            lines.append(f"  💬 *{group['observations']}*")

        lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
#  CONVERSION VERS FORMAT GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────

ALL_SHEET_COLS = [
    'ID_Intervention', 'ID_Parcelle', 'Campagne', 'Date', 'Statut_Intervention',
    'Nature_Intervention', 'Type_Intervention', 'Culture', 'Surface_Travaillée_Ha',
    'Tracteur', 'Outil', 'Stade_Culture', 'Observations',
    'Nom_Produit', 'Cible', 'Dose_Ha', 'Unité_Dose',
    'Quantité_Totale_Produit', 'Unité_Quantité',
    'N/ha', 'P/ha', 'K/ha',
    'Volume_Bouillie_L_Ha', 'Volume_Total_Bouillie_L',
    'Densité_Semis', 'Unité_Densité', 'PMG', 'Quantité_semence_totale',
    'Produit_Récolté', 'Rendement_Ha', 'Humidité_récolte', 'PS', 'Quantité_Récoltée_Totale',
]


def convert_collected_data_to_rows(extracted_data: list) -> list:
    """Convertit les données extraites en lignes DataFrame pour insertion dans Google Sheets."""
    import string
    import random
    rows = []

    for item in extracted_data:
        if item.get("Type_Action") == "IRRIGATION":
            continue  # Handled separately

        row = {col: '' for col in ALL_SHEET_COLS}

        # Copy matching fields
        for col in ALL_SHEET_COLS:
            val = item.get(col, '')
            if val is not None and str(val).lower() not in ['null', 'none']:
                row[col] = val

        # Fix missing or generic Identifiers
        if not row.get("ID_Intervention") or row.get("ID_Intervention") == "XXXXXXXX":
            row["ID_Intervention"] = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        if row.get("Date") == "JJ/MM/AAAA" or not row.get("Date"):
            row["Date"] = datetime.date.today().strftime("%d/%m/%Y")

        # Recalculate quantities
        try:
            dose = float(str(row.get('Dose_Ha') or 0).replace(',', '.'))
            surf = float(str(row.get('Surface_Travaillée_Ha') or 0).replace(',', '.'))
            if dose > 0 and surf > 0:
                qte = round(dose * surf, 2)
                if not row.get('Quantité_Totale_Produit') or _is_empty_or_zero(row.get('Quantité_Totale_Produit')):
                    row['Quantité_Totale_Produit'] = qte
                if row.get('Unité_Dose'):
                    row['Unité_Quantité'] = str(row['Unité_Dose']).replace('/ha', '').replace('/Ha', '')
                vol_ha = float(str(row.get('Volume_Bouillie_L_Ha') or 0).replace(',', '.'))
                if vol_ha > 0:
                    row['Volume_Total_Bouillie_L'] = round(vol_ha * surf, 2)
        except:
            pass

        # Récolte total
        try:
            rdt = float(str(row.get('Rendement_Ha') or 0).replace(',', '.'))
            surf = float(str(row.get('Surface_Travaillée_Ha') or 0).replace(',', '.'))
            if rdt > 0 and surf > 0:
                if _is_empty_or_zero(row.get('Quantité_Récoltée_Totale')):
                    row['Quantité_Récoltée_Totale'] = round(rdt * surf, 2)
        except:
            pass

        rows.append(row)

    return rows


# ─────────────────────────────────────────────────────────────────────────────
#  COMPATIBILITÉ v1
# ─────────────────────────────────────────────────────────────────────────────

def format_voice_summary(extracted_data: list) -> str:
    """Rétrocompatibilité v1 - formate les données en résumé lisible."""
    if not extracted_data:
        return "❌ Aucune donnée extraite."
    first = extracted_data[0]
    if "error" in first:
        return f"❌ Erreur : {first.get('error')} — {first.get('raw', '')[:200]}"
    grouped = group_interventions_by_parcelle(extracted_data)
    return format_grouped_summary_md(grouped)

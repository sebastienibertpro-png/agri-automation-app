"""
voice_processor.py - Assistant Vocal pour Saisie d'Interventions dans Streamlit
Adapté de voice_intervention_bot/gemini_processor.py
Fonctionne sans Telegram : reçoit des bytes audio depuis le navigateur.
"""

import json
import tempfile
import os
import datetime
from google import genai


def get_gemini_client(api_key: str):
    """Initialise et retourne un client Gemini."""
    return genai.Client(api_key=api_key)


def build_context_from_loader(active_loader, selected_campaign):
    """
    Construit le contexte de référence (parcelles, intrants, matériels, secteurs)
    depuis le DataLoader Streamlit existant, pour l'envoyer à Gemini.
    Retourne un dict compatible avec le prompt Gemini du voice_intervention_bot.
    """
    context = {
        "parcelles": [],
        "intrants": [],
        "materiels": [],
        "secteurs": [],
    }

    try:
        # --- Parcelles (avec surface et culture) ---
        import pandas as pd
        df_parcelles = active_loader._get_data("REF_PARCELLES")
        df_asso = active_loader.get_assolement(selected_campaign)

        # Map ID_Parcelle -> Culture pour la campagne courante
        culture_map = {}
        if not df_asso.empty and 'ID_Parcelle' in df_asso.columns and 'Culture' in df_asso.columns:
            for _, row in df_asso.iterrows():
                pid = str(row.get('ID_Parcelle', '')).strip()
                cult = str(row.get('Culture', '')).strip()
                if pid:
                    culture_map[pid] = cult

        if not df_parcelles.empty:
            for _, row in df_parcelles.iterrows():
                pid = str(row.get('ID_Parcelle', '') or row.get('ID', '')).strip()
                pnom = str(row.get('Nom_Terrain', '') or row.get('Nom', '')).strip()
                # Surface
                surf_raw = str(row.get('Surface_Référence_Ha', '') or row.get('Surface', '')).replace(',', '.').strip()
                try:
                    surf_val = float(surf_raw)
                    surf_str = f" ({surf_val} ha)"
                except:
                    surf_str = ""
                if not pid:
                    continue
                culture = culture_map.get(pid, "")
                culture_str = f" [Culture: {culture}]" if culture else ""
                name_part = f" - {pnom}" if pnom and pnom != pid else ""
                context["parcelles"].append(f"{pid}{name_part}{surf_str}{culture_str}")

    except Exception as e:
        print(f"[VoiceProcessor] Erreur chargement parcelles: {e}")

    try:
        # --- Intrants ---
        df_intrants = active_loader._get_data("REF_INTRANTS")
        if not df_intrants.empty and 'Nom_Produit' in df_intrants.columns:
            context["intrants"] = sorted(
                df_intrants['Nom_Produit'].dropna().astype(str).str.strip().tolist()
            )
    except Exception as e:
        print(f"[VoiceProcessor] Erreur chargement intrants: {e}")

    try:
        # --- Matériels ---
        df_mat = active_loader.get_materiels()
        if not df_mat.empty:
            ids = df_mat.get('ID_Materiel', df_mat.get('ID', [])).dropna().astype(str).tolist()
            marques = df_mat.get('Marque', [])
            modeles = df_mat.get('Modele', [])
            context["materiels"] = [m for m in ids if m.strip()]
    except Exception as e:
        print(f"[VoiceProcessor] Erreur chargement matériels: {e}")

    try:
        # --- Secteurs d'Irrigation (optionnel) ---
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
    except Exception:
        pass  # Secteurs non critiques

    return context


def transcribe_audio_bytes(audio_bytes: bytes, context_data: dict, api_key: str,
                            audio_format: str = "wav") -> list:
    """
    Transcrit et extrait les données d'intervention depuis des bytes audio.

    Args:
        audio_bytes: Bytes audio bruts (WAV, WebM, OGG...)
        context_data: Dict avec clés 'parcelles', 'intrants', 'materiels', 'secteurs'
        api_key: Clé API Gemini
        audio_format: Extension du fichier temporaire ('wav', 'ogg', 'webm')

    Returns:
        Liste de dicts structurés (une entrée par ligne d'intervention détectée)
    """
    if not api_key:
        return [{"error": "NO_API_KEY", "raw": "Clé API Gemini manquante dans les secrets."}]

    client = get_gemini_client(api_key)

    # Sauvegarder en fichier temporaire
    suffix = f".{audio_format}"
    tmp_path = None
    audio_file = None

    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name

        # Upload vers Gemini Files API
        audio_file = client.files.upload(file=tmp_path)

        # Construire le prompt (même logique que le voice_intervention_bot)
        parcelles_str = "\n".join(context_data.get('parcelles', []))
        products_str = ", ".join(context_data.get('intrants', []))
        materiels_str = ", ".join(context_data.get('materiels', []))
        secteurs_list = context_data.get('secteurs', [])
        secteurs_str = ", ".join([s.get('ID_Secteur', '') for s in secteurs_list if s.get('ID_Secteur')])

        today_date = datetime.date.today().strftime("%d/%m/%Y")
        today_year = datetime.date.today().year
        today_month = datetime.date.today().month
        campagne_courante = today_year + 1 if today_month >= 7 else today_year

        prompt = f"""
Tu es un assistant agricole technique expert. 
Ton rôle est d'extraire les détails d'une intervention culturale à partir d'un enregistrement audio pour remplir un logiciel de traçabilité.

Date du jour par défaut : {today_date} (Campagne par défaut: {campagne_courante})

DONNÉES DE RÉFÉRENCE (Utilise EXCLUSIVEMENT ces noms s'ils ressemblent phonétiquement) :
- Parcelles (format: ID - Nom (surface ha) [Culture]) :
{parcelles_str}
- Produits/Intrants : {products_str}
- Matériels/Outils (Tracteur/Outil) : {materiels_str}
- Secteurs d'Irrigation : {secteurs_str}

CONSIGNES VITALES :
1. DÉTERMINE LE TYPE D'ACTION : Analyse l'audio pour savoir s'il s'agit d'une intervention culturale classique ("Type_Action": "INTERVENTION") ou d'un apport d'eau/irrigation ("Type_Action": "IRRIGATION").

=== SI C'EST UNE INTERVENTION ===
2. Identifie la "Nature_Intervention" (CHOIX UNIQUE PARMI : Semis, Traitement, Fertilisation, Déchaumage, Préparation de printemps, Fissuration, Moisson, Récolte, Binage).
3. Si Nature=Traitement, identifie le "Type_Intervention" (CHOIX UNIQUE PARMI EXCLUSIF : Herbicide, Fongicide, Insecticide). Si l'utilisateur parle de désherbage, de désherbant ou d'un produit contre les mauvaises herbes, c'est OBLIGATOIREMENT "Herbicide".
4. Fais une correspondance flexible pour la Parcelle, mais renvoie son ID EXACT (tout ce qui se trouve AVANT le premier tiret `-` dans la liste de référence). N'INCLUS PAS le nom du terrain, la surface ou la culture dans le champ ID_Parcelle.
5. "Surface_Travaillée_Ha" : Si l'utilisateur ne précise pas la surface, tu DOIS utiliser la surface de référence indiquée entre parenthèses `(X ha)` à côté du nom de la parcelle.
6. "Quantité_Totale_Produit" : Si tu as une "Dose_Ha" et une "Surface_Travaillée_Ha", tu DOIS obligatoirement les multiplier et inscrire le résultat ici.
7. Matériel Motorisé vs Attelé : La colonne "Tracteur" DOIT OBLIGATOIREMENT recevoir le matériel s'il s'agit du `220_CVX`, du `130_CVX`, ou du `Berthoud_Raptor`. Tout autre matériel (pulvérisateur, semoir, déchaumeur, etc.) n'est PAS un Tracteur : inscris-le dans "Outil" et laisse "Tracteur" vide.
8. Si plusieurs produits ou parcelles sont mentionnés, crée une entrée JSON distincte PAR produit ET PAR parcelle.
9. "Culture" : Tu DOIS inscrire la culture indiquée entre crochets `[Culture: ...]` à côté du nom de la parcelle concernée.
10. "Statut_Intervention" : S'il s'agit d'une intention future (ex: "J'ai prévu de", "Je vais faire"), inscris "Prévu". Sinon (action passée/présente), inscris "Réalisé". Par défaut: "Réalisé".
11. "ID_Intervention" : GÉNÈRE UN IDENTIFIANT ALÉATOIRE UNIQUE (8 caractères, lettres majuscules et chiffres) POUR CHAQUE OBJET JSON.

=== SI C'EST UNE IRRIGATION ===
12. Identifie le Secteur ("ID_Secteur") parmi la liste de référence.
13. Extrais le volume en millimètres mentionné et place-le dans "Volume_mm".
14. "ID_Irrigation" : GÉNÈRE UN IDENTIFIANT ALÉATOIRE UNIQUE (8 caractères).

FORMAT DE SORTIE :
Tu dois IMPÉRATIVEMENT renvoyer une LISTE contenant des objets JSON.
Si l'info n'est pas mentionnée, mets null ou une chaîne vide "".

SCHÉMA POUR UNE INTERVENTION :
[
  {{
    "Type_Action": "INTERVENTION",
    "ID_Intervention": "GÉNÉRER_ID_UNIQUE",
    "Date": "JJ/MM/AAAA",
    "Statut_Intervention": "Prévu ou Réalisé",
    "Campagne": {campagne_courante},
    "Culture": "Ex: Blé tendre",
    "ID_Parcelle": "Nom exact de la liste",
    "Surface_Travaillée_Ha": "Nombre (ex: 5.5)",
    "Nature_Intervention": "Ex: Traitement",
    "Type_Intervention": "Ex: Herbicide",
    "Cible": "",
    "Nom_Produit": "Nom exact de la liste",
    "Dose_Ha": "Nombre",
    "Unité_Dose": "Ex: kg/ha, L/ha",
    "N/ha": "",
    "P/ha": "",
    "K/ha": "",
    "Volume_Bouillie_L_Ha": "",
    "Quantité_Totale_Produit": "",
    "Densité_Semis": "",
    "PMG": "",
    "Tracteur": "Nom exact de la liste",
    "Outil": "Nom exact de la liste",
    "Stade_Culture": "",
    "Observations": ""
  }}
]

SCHÉMA POUR UNE IRRIGATION :
[
  {{
    "Type_Action": "IRRIGATION",
    "ID_Irrigation": "GÉNÉRER_ID_UNIQUE",
    "Date": "JJ/MM/AAAA",
    "Campagne": {campagne_courante},
    "ID_Secteur": "Nom exact de la liste",
    "Volume_mm": "Nombre",
    "Stade_Culture": ""
  }}
]

Formate les valeurs numériques avec un POINT (pas de virgule). 
RÉPONDS UNIQUEMENT AU FORMAT JSON. SANS BALISES MARKDOWN (juste le texte brut commençant par [ et finissant par ]).
"""

        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[prompt, audio_file]
        )

        text = response.text.strip()
        # Nettoyage markdown éventuel
        for prefix in ["```json", "```"]:
            if text.startswith(prefix):
                text = text[len(prefix):]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        data = json.loads(text)
        return data

    except json.JSONDecodeError:
        return [{"error": "JSON_ERROR", "raw": text if 'text' in dir() else "Réponse vide"}]
    except Exception as e:
        return [{"error": "GEMINI_ERROR", "raw": str(e)}]
    finally:
        # Nettoyage
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        if audio_file:
            try:
                client.files.delete(name=audio_file.name)
            except:
                pass


def format_voice_summary(extracted_data: list) -> str:
    """
    Formate les données extraites par Gemini en un résumé lisible pour l'utilisateur.
    """
    if not extracted_data:
        return "❌ Aucune donnée extraite."

    first = extracted_data[0]
    if "error" in first:
        return f"❌ Erreur : {first.get('error')} — {first.get('raw', '')[:200]}"

    lines = []
    for item in extracted_data:
        type_action = item.get("Type_Action", "INTERVENTION")

        if type_action == "IRRIGATION":
            secteur = item.get("ID_Secteur", "Inconnu")
            vol = item.get("Volume_mm", "?")
            date = item.get("Date", "")
            lines.append(f"💧 **Irrigation** {vol}mm — Secteur **{secteur}**" + (f" le {date}" if date else ""))
        else:
            nature = item.get("Nature_Intervention", "Intervention")
            type_int = item.get("Type_Intervention", "")
            parcelle = item.get("ID_Parcelle", "?")
            culture = item.get("Culture", "")
            produit = item.get("Nom_Produit", "")
            dose = item.get("Dose_Ha", "")
            unite = item.get("Unité_Dose", "")
            surface = item.get("Surface_Travaillée_Ha", "")
            statut = item.get("Statut_Intervention", "Réalisé")
            date = item.get("Date", "")
            tracteur = item.get("Tracteur", "")
            outil = item.get("Outil", "")

            prefix = "⏳ Prévu" if statut.lower() == "prévu" else "✅ Réalisé"
            header = f"{prefix} — **{nature}**" + (f" ({type_int})" if type_int else "")
            detail = f"• Parcelle : **{parcelle}**" + (f" [{culture}]" if culture else "")
            if surface:
                detail += f" — {surface} ha"
            if date:
                detail += f" — {date}"
            prod_line = ""
            if produit:
                prod_line = f"• Produit : **{produit}**"
                if dose and unite:
                    prod_line += f" à {dose} {unite}"
            equip = []
            if tracteur:
                equip.append(f"🚜 {tracteur}")
            if outil:
                equip.append(f"⚙️ {outil}")
            equip_line = "• " + " + ".join(equip) if equip else ""

            lines.append("\n".join(filter(None, [header, detail, prod_line, equip_line])))

    return "\n\n".join(lines)

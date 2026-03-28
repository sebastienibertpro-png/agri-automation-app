import google.generativeai as genai
import json
import re
import time

class PDFAnalyzer:
    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        # gemini-2.5-flash pour support natif PDF et images
        self.model = genai.GenerativeModel('gemini-2.5-flash')

    def _build_prompt(self):
        return """
        Tu es un assistant comptable très précis, spécialisé dans l'agriculture (grandes cultures). 
        Analyse cette facture et extrais les informations pour remplir un tableau de bord comptable.
        
        CATÉGORIE COMPTABLE (Poste de charge) :
        Tu dois OBLIGATOIREMENT choisir l'une de ces catégories strictement écrites en MAJUSCULES pour chaque ligne (n'en invente aucune autre) :
        - SEMENCES & PLANTS
        - ENGRAIS & AMENDEMENTS
        - PRODUITS PHYTOSANITAIRES
        - EAU & IRRIGATION
        - ELECTRICITE GENERALE
        - CARBURANT & LUBRIFIANT
        - SECHAGE
        - PRESTATION
        - ENTRETIEN MATERIEL
        - ACHAT PETIT MATERIEL
        - FERMAGES & LOCATIONS
        - ASSURANCES
        - FRAIS GENERAUX & ADMIN
        - IMPOTS & TAXES
        - FRAIS DE PERSONNEL & MSA
        - FRAIS FINANCIERS
        - DIVERS / INCONNU
        
        RÈGLES SPÉCIALES À RESPECTER IMPÉRATIVEMENT :
        1. Si c'est une facture EDF/Electricité : Cherche 'irrigation', 'irrigants', 'pompage', 'forage', 'borne', 'moteur' -> 'EAU & IRRIGATION'. Sinon -> 'ELECTRICITE GENERALE'.
        2. Si c'est une facture de GAZ / PROPANE : Vérifie 'séchoir', 'séchage' -> 'SECHAGE'.
        3. Si c'est une facture CUMA : Si ça mentionne 'irrigation', 'redevance eau', 'm3' -> 'EAU & IRRIGATION'. Si travaux agricoles -> 'PRESTATION'.

        RÈGLES DE DÉCOUPAGE DES LIGNES :
        1. Factures ENGRAIS, SEMENCES, PHYTO : 1 ligne par produit distinct présent sur la facture.
        2. Factures MATÉRIEL (Réparation/Entretien) : 2 lignes maximum (regrouper 'Pièces' et 'Main d'œuvre').
        3. AUTRES (EDF, CUMA, Gaz, Assurance, Frais généraux, etc.) : 1 seule ligne globale.

        En plus des détails des lignes, choisis un DOSSIER DE STOCKAGE (Sous_Categorie_Stockage) court pour classer le PDF sur le Drive. Ex: 'Intrants', 'Materiel', 'Electricite', 'Fermage', 'GNR', 'ETA', 'Irrigation', 'Gaz', 'Autre'.

        Pour chaque ligne identifiée, renvoie un objet JSON avec EXACTEMENT ces clés (si l'info n'existe pas, mets null) :
        {
           "ID_Facture": "Numéro de la facture (lettres et chiffres exacts tels qu'ils apparaissent sur la facture)",
           "Date_facture": "YYYY-MM-DD",
           "Campagne": 2026,
           "Fournisseur": "Nom propre et court",
           "Catégorie": "Valeur EXACTE choisie dans la liste stricte ci-dessus",
           "Sous_Categorie_Stockage": "Choisis un nom de dossier court et sans espace",
           "Nom_Produit": "NOM COMMERCIAL PUR uniquement. SUPPRIMER impérativement : doses (ex: 250G, 5L), numéros AMM (ex: AMM N°2060051), conditionnement (ex: sac de 25kg, bidon). Ex: 'PEAK Dose 250G' -> 'PEAK' ; 'FLEXITY 5L AMM N° 2060051' -> 'FLEXITY' ; 'ROUNDUP 20L' -> 'ROUNDUP'.",
           "Quantité_Achetée": "Quantité (nombre, avec point pour décimale)",
           "Unité_Achat": "Ex: L, KG, Unité, Forfait, m3, kWh",
           "Prix_Unitaire_HT": "Prix unitaire HT (nombre)",
           "Montant_Total_Produit_HT": "Total HT pour cette ligne (nombre)",
           "Montant_Total_Facture_HT": "Total HT de TOUTE la facture (nombre)",
           "TVA_%": "Taux de TVA en pourcentage (nombre avec point, ex: 5.5 ou 20.0)",
           "Montant_Total_Facture_TTC": "Total TTC de TOUTE la facture (nombre)"
        }
        
        Renvoie SEULEMENT une liste JSON valide contenant ces objets, sans aucun autre texte ou markdown. Ex: [{...}, {...}]
        """

    def _parse_json_response(self, raw_text):
        """Extrait et parse le JSON depuis la réponse Gemini, même si entouré de markdown."""
        # Suppression des balises markdown courantes
        text = raw_text.strip()
        text = re.sub(r'^```json\s*', '', text)
        text = re.sub(r'^```\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        text = text.strip()

        # Tentative directe
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                data = [data]
            return data, None
        except json.JSONDecodeError:
            pass

        # Chercher un tableau JSON dans la réponse (fallback)
        match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(0))
                if isinstance(data, dict):
                    data = [data]
                return data, None
            except json.JSONDecodeError as e:
                return None, f"JSON invalide extrait : {e}\n\nRéponse brute :\n{raw_text[:800]}"

        return None, f"Impossible d'extraire un JSON valide.\n\nRéponse brute de Gemini :\n{raw_text[:800]}"

    def _extract_images_from_pdf(self, file_path):
        """Extrait les images d'un PDF scanné en fallback via PyMuPDF (fitz)."""
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(file_path)
            images = []
            for page in doc:
                pix = page.get_pixmap(dpi=150)
                img_bytes = pix.tobytes("jpeg")
                images.append(img_bytes)
            doc.close()
            return images
        except ImportError:
            return None  # PyMuPDF non installé
        except Exception as e:
            print(f"Erreur extraction images PDF : {e}")
            return None

    def analyze_invoice(self, file_path):
        """
        Analyse une facture PDF avec Gemini.
        Retourne une liste de dicts (une entrée par ligne produit) ou None en cas d'échec.
        En cas d'erreur, retourne aussi un message d'erreur détaillé via le tuple (data, error_msg).
        """
        prompt = self._build_prompt()
        pdf_file = None
        raw_text = None

        # --- Tentative 1 : Upload PDF natif ---
        try:
            pdf_file = genai.upload_file(file_path, mime_type="application/pdf")
            # Attendre que le fichier soit prêt
            for _ in range(10):
                f = genai.get_file(pdf_file.name)
                if f.state.name == "ACTIVE":
                    break
                time.sleep(2)

            response = self.model.generate_content([pdf_file, prompt])
            raw_text = response.text
            data, err = self._parse_json_response(raw_text)
            if data:
                return data, None
            # Si le parsing JSON échoue, on note l'erreur mais on tente le fallback image
            print(f"[Tentative PDF natif] Échec JSON : {err}")
        except Exception as e:
            print(f"[Tentative PDF natif] Exception : {e}")
        finally:
            if pdf_file:
                try:
                    genai.delete_file(pdf_file.name)
                except Exception:
                    pass

        # --- Tentative 2 : Fallback extraction image (PDFs scannés) ---
        images = self._extract_images_from_pdf(file_path)
        if images:
            try:
                import PIL.Image
                import io
                parts = []
                for img_bytes in images[:4]:  # Max 4 pages
                    pil_img = PIL.Image.open(io.BytesIO(img_bytes))
                    parts.append(pil_img)
                parts.append(prompt)

                response2 = self.model.generate_content(parts)
                raw_text = response2.text
                data, err = self._parse_json_response(raw_text)
                if data:
                    return data, None
                return None, f"[Fallback image] {err}"
            except Exception as e2:
                return None, f"[Fallback image] Exception : {e2}\n\nRéponse brute :\n{str(raw_text)[:800] if raw_text else 'Aucune'}"
        
        return None, f"Échec analyse PDF. Réponse brute Gemini :\n{str(raw_text)[:800] if raw_text else 'Aucune réponse obtenue.'}"

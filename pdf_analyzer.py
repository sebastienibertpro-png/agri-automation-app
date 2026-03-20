import google.generativeai as genai
import json
import time

class PDFAnalyzer:
    def __init__(self, api_key):
        genai.configure(api_key=api_key)
        # On utilise gemini-2.5-flash pour sa vitesse et son très bon support natif des PDF
        self.model = genai.GenerativeModel('gemini-2.5-flash')

    def analyze_invoice(self, file_path):
        """Envoie le PDF à Gemini pour extraire la date, le fournisseur et le montant total."""
        print(f"Analyse du PDF avec Gemini : {file_path}")
        
        # Upload le fichier dans l'API Gemini
        try:
           pdf_file = genai.upload_file(file_path, mime_type="application/pdf")
        except Exception as e:
           print(f"Erreur lors de l'upload vers Gemini : {e}")
           return None

        prompt = """
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
        1. Si c'est une facture EDF/Electricité : Cherche obligatoirement les mots 'irrigation', 'irrigants', 'pompage', 'forage', 'borne', 'moteur' ou des adresses/PDL spécifiques à l'eau -> 'EAU & IRRIGATION'. Sinon (Silo, Siège...) -> 'ELECTRICITE GENERALE'.
        2. Si c'est une facture de GAZ / PROPANE : Vérifie si ça mentionne 'séchoir', 'séchage' -> 'SECHAGE'.
        3. Si c'est une facture CUMA : Si le nom du fournisseur ou le détail mentionne 'irrigation', 'redevance eau', 'réseau collectif', 'm3' -> 'EAU & IRRIGATION'. Si on lit des travaux agricoles (traction, andainage, moisson, pressage) -> 'PRESTATION'.

        RÈGLES DE DÉCOUPAGE DES LIGNES :
        1. Factures ENGRAIS, SEMENCES, PHYTO : 1 ligne par produit distinct présent sur la facture.
        2. Factures MATÉRIEL (Réparation/Entretien) : 2 lignes maximum (regrouper 'Pièces' dans une ligne et 'Main d'œuvre' dans une autre si applicable).
        3. AUTRES (EDF, CUMA, Gaz, Assurance, Frais généraux, etc.) : Regroupe tout en 1 seule ligne globale pour toute la facture.

        En plus des détails des lignes, tu dois choisir un DOSSIER DE STOCKAGE (Sous_Categorie_Stockage) court pour classer le PDF sur le Drive. Ex: 'Intrants', 'Materiel', 'Electricite', 'Fermage', 'GNR', 'ETA', 'Irrigation', 'Gaz', 'Autre'.

        Pour chaque ligne identifiée, renvoie un objet JSON avec EXACTEMENT ces clés (Même si l'info n'existe pas, mets null) :
        {
           "ID_Facture": "Numéro de la facture (lettres et chiffres exacts)",
           "Date_facture": "YYYY-MM-DD",
           "Campagne": 2026, // Calcule : si Date_facture entre le 01/07/N-1 et le 30/06/N, la campagne est N (ex: 15/08/2025 -> 2026).
           "Fournisseur": "Nom propre et court",
           "Catégorie": "Valeur EXACTE choisie dans la liste stricte ci-dessus",
           "Sous_Categorie_Stockage": "Choisis un nom de dossier court et sans espace",
           "Nom_Produit": "Nom du produit exact ou résumé de la prestation",
           "Quantité_Achetée": "Quantité (nombre, avec point pour décimale)",
           "Unité_Achat": "Ex: L, KG, Unité, Forfait, m3, kWh",
           "Prix_Unitaire_HT": "Prix unitaire HT (nombre)",
           "Montant_Total_Produit_HT": "Total HT pour cette ligne (nombre)",
           "Montant_Total_Facture_HT": "Total HT de TOUTE la facture (nombre)",
           "TVA_%": "Taux de TVA en pourcentage tel qu'affiché (nombre avec un point, ex: 5.5 ou 20.0)",
           "Montant_Total_Facture_TTC": "Total TTC de TOUTE la facture (nombre)"
        }
        
        Renvoie SEULEMENT une liste JSON valide contenant ces objets, sans aucun autre texte ou markdown. Ex: [{...}, {...}]
        """

        try:
           response = self.model.generate_content([pdf_file, prompt])
           text = response.text.replace('```json', '').replace('```', '').strip()
           
           data = json.loads(text)
           # Ensure data is a list
           if isinstance(data, dict):
               data = [data]
           return data
        except Exception as e:
           print(f"Erreur lors de l'analyse avec Gemini : {e}")
           print(f"Réponse brute de Gemini: {response.text if 'response' in locals() else 'Aucune réponse'}")
           return None

        finally:
           # Tentative de nettoyage du fichier de l'API Gemini (pas strictement bloquant)
           try:
              genai.delete_file(pdf_file.name)
           except:
              pass

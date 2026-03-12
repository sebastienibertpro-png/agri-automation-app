import streamlit as st
import pandas as pd
import os
import tempfile
import shutil
from datetime import datetime
import time

from shared import get_dataloader, get_drive_uploader
from pdf_analyzer import PDFAnalyzer

# Configuration
DRIVE_FOLDER_NAME = "08_Factures_Achats_Ventes" # Dossier parent
DRIVE_SUBFOLDER_NAME = "A_Traiter"     # Dossier source des factures
SPREADSHEET_ID = "1rNY5Skg8hTekiKKbJrPibB4kmbkQjq0uQpzEnhXPmBA" 
SHEET_NAME = "ACHAT_MASTER"

st.set_page_config(page_title="Bot Comptable", page_icon="🤖", layout="wide")

st.title("🤖 Bot Comptable - Phase 2")
st.subheader("Analyse intelligente des factures et archivage automatique")

def get_compta_year(date_str):
    """Calcule l'année comptable (du 01/07 au 30/06) à partir d'une date YYYY-MM-DD."""
    try:
        parts = date_str.split('-')
        year = int(parts[0])
        month = int(parts[1])
        if month >= 7:
            return year + 1
        return year
    except:
        return "INCONNU"

def process_invoices_ui():
    uploader = get_drive_uploader()
    if not uploader:
        st.error("❌ Service Google Drive non initialisé. Vérifiez vos secrets.")
        return

    # Récupération de l'API Key Gemini depuis les secrets
    gemini_key = st.secrets.get("GEMINI_API_KEY")
    if not gemini_key:
        st.error("❌ GEMINI_API_KEY manquante dans les secrets Streamlit.")
        return

    analyzer = PDFAnalyzer(api_key=gemini_key)
    
    st.info(f"📂 Recherche des PDF dans le dossier Drive : `{DRIVE_FOLDER_NAME}/{DRIVE_SUBFOLDER_NAME}`")
    
    try:
        # 1. Obtenir les IDs des dossiers via le service drive de l'uploader
        # On doit d'abord trouver le dossier parent
        query_parent = f"name = '{DRIVE_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        parent_results = uploader.service.files().list(q=query_parent, fields="files(id, name)").execute()
        parent_folders = parent_results.get('files', [])
        
        if not parent_folders:
            st.error(f"❌ Dossier parent '{DRIVE_FOLDER_NAME}' introuvable sur votre Drive.")
            return
        
        parent_id = parent_folders[0]['id']
        
        # Trouver le sous-dossier A_Traiter
        query_sub = f"name = '{DRIVE_SUBFOLDER_NAME}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        sub_results = uploader.service.files().list(q=query_sub, fields="files(id, name)").execute()
        sub_folders = sub_results.get('files', [])
        
        if not sub_folders:
            st.error(f"❌ Dossier '{DRIVE_SUBFOLDER_NAME}' introuvable dans '{DRIVE_FOLDER_NAME}'.")
            return
            
        a_traiter_id = sub_folders[0]['id']
        
        # 2. Lister les fichiers PDF
        query_files = f"'{a_traiter_id}' in parents and mimeType='application/pdf' and trashed=false"
        file_results = uploader.service.files().list(q=query_files, fields='files(id, name, webViewLink)').execute()
        files = file_results.get('files', [])
        
        if not files:
            st.warning("📭 Aucune facture PDF trouvée dans 'A_Traiter'.")
            return
            
        st.success(f"📄 {len(files)} facture(s) détectée(s).")
        
        if st.button("🚀 Lancer l'analyse et l'archivage", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_container = st.container()
            
            download_dir = tempfile.mkdtemp()
            results_summary = []
            
            try:
                for idx, file in enumerate(files):
                    file_id = file.get('id')
                    original_name = file.get('name')
                    view_link = file.get('webViewLink')
                    
                    status_text.text(f"Traitement de {original_name} ({idx+1}/{len(files)})...")
                    
                    # Téléchargement
                    local_path = os.path.join(download_dir, original_name)
                    request = uploader.service.files().get_media(fileId=file_id)
                    with open(local_path, 'wb') as fh:
                        fh.write(request.execute())
                    
                    # Analyse IA
                    with st.spinner(f"Analyse IA en cours pour {original_name}..."):
                        rows_data = analyzer.analyze_invoice(local_path)
                    
                    if not rows_data:
                        st.error(f"⚠️ Échec analyse pour {original_name}")
                        continue
                    
                    # Préparation des données pour Sheets
                    first_row = rows_data[0]
                    compta_year = get_compta_year(first_row.get("Date_facture", ""))
                    
                    sheet_values = []
                    for row in rows_data:
                        formatted_row = [
                            row.get("ID_Facture", ""),
                            row.get("Date_facture", ""),
                            row.get("Campagne", compta_year),
                            row.get("Fournisseur", ""),
                            row.get("Catégorie", ""),
                            row.get("Nom_Produit", ""),
                            row.get("Quantité_Achetée", ""),
                            row.get("Unité_Achat", ""),
                            row.get("Prix_Unitaire_HT", ""),
                            row.get("Montant_Total_Produit_HT", ""),
                            row.get("Montant_Total_Facture_HT", ""),
                            row.get("TVA_%", ""),
                            row.get("Montant_Total_Facture_TTC", ""),
                            str(len(rows_data)),
                            "", # ID_Parcelle_Liée (Optionnel)
                            "", # Affectation_Type
                            view_link,
                            ""  # Commentaires
                        ]
                        sheet_values.append(formatted_row)
                    
                    # Insertion Sheets via DataLoader (on utilise le DataLoader partagé)
                    loader = get_dataloader()
                    if loader and loader.conn:
                        # On réutilise une méthode d'insertion ou on en crée une propre
                        # Pour simplifier, on concatène et update ACHAT_MASTER
                        try:
                            df_existing = loader.conn.read(worksheet=SHEET_NAME, ttl=0, spreadsheet=SPREADSHEET_ID)
                            df_new = pd.DataFrame(sheet_values, columns=df_existing.columns[:len(sheet_values[0])])
                            # S'assurer que les colonnes correspondent
                            df_final = pd.concat([df_existing, df_new], ignore_index=True)
                            loader.conn.update(worksheet=SHEET_NAME, data=df_final, spreadsheet=SPREADSHEET_ID)
                            
                            st.toast(f"✅ {original_name} ajouté au Sheet !")
                            
                            # Classement Drive
                            target_folder_name = f"Compta {compta_year}"
                            sous_cat = first_row.get("Sous_Categorie_Stockage", "Autre")
                            short_year = str(compta_year)[-2:] if str(compta_year).isdigit() else "XX"
                            subfolder_name = f"{sous_cat}_compta{short_year}"
                            
                            # Helper local pour dossier (car drive_utils est limité)
                            def get_or_create_sub(name, pid):
                                q = f"name = '{name}' and '{pid}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                                r = uploader.service.files().list(q=q).execute().get('files', [])
                                if r: return r[0]['id']
                                meta = {'name': name, 'parents': [pid], 'mimeType': 'application/vnd.google-apps.folder'}
                                return uploader.service.files().create(body=meta, fields='id').execute().get('id')

                            year_fid = get_or_create_sub(target_folder_name, parent_id)
                            final_fid = get_or_create_sub(subfolder_name, year_fid)
                            
                            # Déplacement
                            uploader.service.files().update(
                                fileId=file_id,
                                addParents=final_fid,
                                removeParents=a_traiter_id
                            ).execute()
                            
                            results_summary.append({
                                "Fichier": original_name,
                                "Fournisseur": first_row.get("Fournisseur"),
                                "Total TTC": first_row.get("Montant_Total_Facture_TTC"),
                                "Statut": "Traité & Archivé"
                            })
                            
                        except Exception as e_sheet:
                            st.error(f"❌ Erreur Sheets/Drive pour {original_name} : {e_sheet}")

                    # Mise à jour barre
                    progress_bar.progress((idx + 1) / len(files))
                
                status_text.text("✨ Traitement terminé !")
                if results_summary:
                    st.table(pd.DataFrame(results_summary))
                    st.balloons()
                    
            finally:
                shutil.rmtree(download_dir)

    except Exception as e_global:
        st.error(f"❌ Une erreur globale est survenue : {e_global}")

# Main execution
# On vérifie si on a soit les secrets Streamlit, soit un fichier credentials.json local
has_creds = ("gcp_service_account" in st.secrets) or os.path.exists("credentials.json")
has_gemini = "GEMINI_API_KEY" in st.secrets

if not has_creds:
    st.warning("⚠️ **Compte de service Google non configuré.**")
    st.markdown("""
    Pour utiliser le Bot Comptable sur Streamlit Cloud :
    1. Allez dans le tableau de bord Streamlit Cloud de votre app.
    2. Allez dans **Settings > Secrets**.
    3. Copiez-collez le contenu de votre fichier `credentials.json` sous la clé `gcp_service_account` comme ceci :
    ```toml
    [gcp_service_account]
    type = "service_account"
    project_id = "..."
    ...
    ```
    """)
elif not has_gemini:
    st.warning("⚠️ **Clé API Gemini manquante.**")
    st.info("Ajoutez `GEMINI_API_KEY = 'votre_cle_ici'` dans vos Secrets Streamlit.")
else:
    process_invoices_ui()

st.markdown("---")
st.info("💡 **Rappel** : Placez vos factures PDF dans le dossier Drive `AGRI_AUTOMATION/A_Traiter`. L'IA s'occupe de tout le reste.")

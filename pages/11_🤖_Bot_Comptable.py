import streamlit as st
import pandas as pd
import os
import tempfile
import shutil
from datetime import datetime
import time

from shared import get_dataloader, get_drive_uploader
from pdf_analyzer import PDFAnalyzer
import traceback
import io
from googleapiclient.http import MediaIoBaseDownload

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
        # On ajoute supportsAllDrives pour les dossiers partagés
        query_parent = f"name = '{DRIVE_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        parent_results = uploader.service.files().list(
            q=query_parent, 
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        parent_folders = parent_results.get('files', [])
        
        if not parent_folders:
            st.error(f"❌ Dossier parent '{DRIVE_FOLDER_NAME}' introuvable sur votre Drive.")
            
            # Aide au débogage : lister ce que le robot voit
            with st.expander("🔍 Pourquoi mon dossier n'est pas trouvé ?", expanded=True):
                st.write("Le robot (compte de service) ne voit que les dossiers partagés explicitement avec lui.")
                st.write(f"Adresse du robot : `{st.secrets['gcp_service_account']['client_email']}`")
                
                if st.button("👁️ Lister les dossiers accessibles"):
                    res = uploader.service.files().list(
                        q="mimeType = 'application/vnd.google-apps.folder' and trashed = false",
                        pageSize=20,
                        fields="files(name)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True
                    ).execute()
                    folders_seen = [f['name'] for f in res.get('files', [])]
                    if folders_seen:
                        st.write("Dossiers que le robot arrive à voir :")
                        for f in folders_seen:
                            st.write(f"- {f}")
                        if DRIVE_FOLDER_NAME not in folders_seen:
                            st.warning(f"⚠️ `{DRIVE_FOLDER_NAME}` n'est pas dans la liste. Vérifiez le partage !")
                    else:
                        st.warning("Le robot ne voit aucun dossier. Le partage n'a probablement pas été fait.")
            return
        
        parent_id = parent_folders[0]['id']
        
        # Trouver le sous-dossier A_Traiter
        query_sub = f"name = '{DRIVE_SUBFOLDER_NAME}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        sub_results = uploader.service.files().list(
            q=query_sub, 
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
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
                    
                    # Téléchargement robuste
                    # Assainir le nom de fichier : les '/' dans le nom (ex: date 27/02/2026)
                    # seraient interprétés comme des séparateurs de répertoires -> erreur FileNotFound
                    safe_name = original_name.replace('/', '-').replace('\\', '-')
                    local_path = os.path.join(download_dir, safe_name)
                    try:
                        request = uploader.service.files().get_media(fileId=file_id)
                        fh = io.FileIO(local_path, 'wb')
                        downloader = MediaIoBaseDownload(fh, request)
                        done = False
                        while done is False:
                            status, done = downloader.next_chunk()
                    except Exception as e_dl:
                        st.error(f"❌ Erreur téléchargement {original_name} : {e_dl}")
                        continue
                    
                    # Analyse IA
                    with st.spinner(f"Analyse IA en cours pour {original_name}..."):
                        try:
                            rows_data, ia_error = analyzer.analyze_invoice(local_path)
                        except Exception as e_ia:
                            st.error(f"❌ Erreur IA pour {original_name} : {e_ia}")
                            rows_data, ia_error = None, str(e_ia)
                    
                    if not rows_data:
                        st.error(f"⚠️ Échec analyse pour {original_name}")
                        if ia_error:
                            with st.expander("🔍 Détail de l'erreur Gemini", expanded=True):
                                st.code(ia_error)
                        continue
                    
                    # Préparation des données pour Sheets
                    first_row = rows_data[0]
                    compta_year = get_compta_year(first_row.get("Date_facture", ""))
                    
                    # --- Suffixe ID: FAC001 -> FAC001-1, FAC001-2, ... (si plusieurs lignes) ---
                    base_id = str(first_row.get("ID_Facture", "") or "").strip()
                    # Si le numéro de facture est vide, on utilise le nom du fichier sans extension
                    if not base_id:
                        import os as _os
                        base_id = _os.path.splitext(original_name)[0].replace(" ", "_")[:30]
                    
                    n_rows = len(rows_data)
                    
                    sheet_values = []
                    for line_idx, row in enumerate(rows_data, start=1):
                        # Identifiant unique par ligne : FAC001-1, FAC001-2...
                        if n_rows > 1:
                            row_id = f"{base_id}-{line_idx}"
                        else:
                            row_id = base_id
                        
                        formatted_row = [
                            row_id,
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
                            "", # ID_Parcelle_Liée
                            "", # Affectation_Type
                            view_link,
                            ""  # Commentaires
                        ]
                        sheet_values.append(formatted_row)
                    
                    # Insertion via DataLoader
                    try:
                        loader = get_dataloader()
                        if loader and loader.append_achat_master(sheet_values):
                            st.toast(f"✅ {original_name} ajouté au Sheet !")
                            
                            # Classement Drive
                            target_folder_name = f"Compta {compta_year}"
                            sous_cat = first_row.get("Sous_Categorie_Stockage", "Autre")
                            short_year = str(compta_year)[-2:] if str(compta_year).isdigit() else "XX"
                            subfolder_name = f"{sous_cat}_compta{short_year}"
                            
                            def get_or_create_sub(name, pid):
                                q = f"name = '{name}' and '{pid}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
                                r = uploader.service.files().list(q=q, supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get('files', [])
                                if r: return r[0]['id']
                                meta = {'name': name, 'parents': [pid], 'mimeType': 'application/vnd.google-apps.folder'}
                                return uploader.service.files().create(body=meta, fields='id', supportsAllDrives=True).execute().get('id')

                            year_fid = get_or_create_sub(target_folder_name, parent_id)
                            final_fid = get_or_create_sub(subfolder_name, year_fid)
                            
                            # Déplacement
                            uploader.service.files().update(
                                fileId=file_id,
                                addParents=final_fid,
                                removeParents=a_traiter_id,
                                supportsAllDrives=True
                            ).execute()
                            
                            results_summary.append({
                                "Fichier": original_name,
                                "Fournisseur": first_row.get("Fournisseur"),
                                "Total TTC": first_row.get("Montant_Total_Facture_TTC"),
                                "Statut": "Traité & Archivé"
                            })
                        else:
                            st.error(f"❌ Échec de l'insertion dans le Sheet pour {original_name}")
                            
                    except Exception as e_sheet:
                        st.error(f"❌ Erreur Sheets/Drive pour {original_name}")
                        st.exception(e_sheet)

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

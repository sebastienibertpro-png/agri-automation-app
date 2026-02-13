from data_loader import DataLoader
from report_gen import ReportGenerator
from drive_utils import DriveUploader
import pandas as pd
import os
from datetime import datetime

# Configuration
# Detect if running in Colab or Local
try:
    import google.colab
    IN_COLAB = True
    # In Colab, we rely on the notebook to set the CWD correctly
    BASE_DIR = os.getcwd() 
    print(f"Mode Google Colab détecté. Dossier de travail : {BASE_DIR}")
except:
    IN_COLAB = False
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    print(f"Mode Local détecté. Dossier de base : {BASE_DIR}")

# Paths relative to script location
# Assuming folder structure:
# /agri_automation/
#   main.py
#   credentials.json
#   /output/
#   MASTER_EXPLOITATION.xlsx (or in parent?)

# Adapting to user's current structure where .xlsx is in parent directory locally
# But for Colab/Portable, best to have everything in one folder or define clear structure.
# Let's check where xlsx is: currently c:\Users\Proprietaire\.gemini\antigravity\scratch\MASTER_EXPLOITATION.xlsx
# and script is in ...\scratch\agri_automation\main.py
# So xlsx is in "../MASTER_EXPLOITATION.xlsx" relative to script.

if IN_COLAB:
    FILE_PATH = os.path.join(BASE_DIR, "MASTER_EXPLOITATION.xlsx") # Expects file in same folder on Drive
else:
    # Keep local logic compatible with current setup
    # Check parent dir for xlsx
    parent_dir = os.path.dirname(BASE_DIR)
    FILE_PATH = os.path.join(parent_dir, "MASTER_EXPLOITATION.xlsx")
    if not os.path.exists(FILE_PATH):
         # Fallback to current dir
         FILE_PATH = os.path.join(BASE_DIR, "MASTER_EXPLOITATION.xlsx")

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")

FOLDER_IDS = {
    'PHYTO': '1YDTwRXHFTxPmM4QD84nTnQYmMZqz60dc',
    'FERTI': '12k4HTTCRIcp-RnVh9Vc4WjC9iJwVMLpq',
    'ITK': '1AJi3DzH5UdqcThmfyufSOLqntPN1RV1i'
}

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def main():
    print("--- Agri Automation ---")
    
    # 1. Load Data
    print("Chargement des données en cours...")
    loader = DataLoader(FILE_PATH)
    try:
        loader.load_source()
        # print("Données chargées.") - Message handled inside load_source
    except Exception as e:
        print(f"Erreur fatale: {e}")
    # 1. Load Data
    print("Chargement des données en cours...")
    
    # Init Drive Uploader
    uploader = DriveUploader(CREDENTIALS_PATH)
    
    loader = DataLoader(FILE_PATH)
    try:
        loader.load_source()
        # print("Données chargées.") - Message handled inside load_source
    except Exception as e:
        print(f"Erreur fatale: {e}")
        return

    # 2. Select Campaign
    try:
        df_intervention = loader.get_interventions()
        available_campaigns = sorted(df_intervention['Campagne'].unique())
        print(f"Campagnes disponibles dans le fichier : {available_campaigns}")
        
        target_campaign = input("Entrez l'année de la campagne souhaitée (ex: 2026) : ").strip()
        
        if target_campaign not in [str(c) for c in available_campaigns]:
             print(f"ATTENTION : La campagne '{target_campaign}' n'est pas dans la liste des disponibles.")
             confirm = input("Voulez-vous continuer quand même (pour utiliser des données fictives/test) ? (o/n) : ")
             if confirm.lower() != 'o':
                 return
    except Exception as e:
        print(f"Erreur lors de la récupération des campagnes: {e}")
        return

    # Filter by Campaign Global
    df_campaign = df_intervention[df_intervention['Campagne'].astype(str) == str(target_campaign)]
    
    # Identify Parcels (Phyto + Ferti)
    # We look at unique 'ID_Parcelle' in the filtered campaign data
    available_parcelles = sorted(df_campaign['ID_Parcelle'].unique())
    
    # If no data for this campaign, we might rely on Mock Data later, 
    # but let's assume if available_parcelles is empty we go to mock mode logic or warn.
    mock_mode = False
    if len(available_parcelles) == 0:
        print(f"Aucune donnée trouvée pour {target_campaign}. Mode Test/Mock activé.")
        mock_mode = True
        # Mock Parcels for selection
        available_parcelles = ["Parcelle_Test_1", "Parcelle_Test_2"]

    # --- Parcel Selection ---
    print(f"\nParcelles trouvées pour {target_campaign} (Colonne ID_Parcelle) :")
    for i, p in enumerate(available_parcelles):
        print(f"{i+1}. {p}")
    
    choice = input("\nEntrez le NUMÉRO de la parcelle à générer (ou 'T' pour Toutes) : ").strip()
    
    target_parcelles = []
    if choice.upper() == 'T':
        target_parcelles = available_parcelles
        print("Génération pour TOUTES les parcelles.")
    else:
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(available_parcelles):
                selected_p = available_parcelles[idx]
                target_parcelles = [selected_p]
                print(f"Génération uniquement pour : {selected_p}")
            else:
                print("Numéro invalide. Génération annulée.")
                return
        except ValueError:
             print("Saisie invalide. Génération annulée.")
             return

    # --- PART 1: Phyto ---
    print("\n--- Génération Registre Phytosanitaire ---")
    
    # Filter for Phyto
    df_phyto = df_campaign[df_campaign['Nature_Intervention'] == "Traitement"]
    
    # Apply Parcel Filter
    df_phyto = df_phyto[df_phyto['ID_Parcelle'].isin(target_parcelles)]
    
    print(f"{len(df_phyto)} interventions phyto trouvées pour la sélection.")

    if len(df_phyto) == 0 and mock_mode:
        print("Utilisation de données fictives Phyto.")
        mock_data = [
            {'Date': pd.Timestamp('2023-04-10'), 'Campagne': target_campaign, 'Nature_Intervention': 'Traitement', 'ID_Parcelle': 'Parcelle_Test_1', 'Culture': 'Blé', 'Nom_Produit': 'Fongicide X', 'Dose_Ha': 1.5, 'Surface_Travaillée_Ha': 10, 'Cible': 'Fusariose', 'Observations': 'RAS', 'Type_Intervention': 'Fongicide'},
            {'Date': pd.Timestamp('2023-05-15'), 'Campagne': target_campaign, 'Nature_Intervention': 'Traitement', 'ID_Parcelle': 'Parcelle_Test_1', 'Culture': 'Blé', 'Nom_Produit': 'Herbicide Y', 'Dose_Ha': 0.8, 'Surface_Travaillée_Ha': 10, 'Cible': 'Adventices', 'Observations': 'Vent faible', 'Type_Intervention': 'Herbicide'},
            {'Date': pd.Timestamp('2023-04-12'), 'Campagne': target_campaign, 'Nature_Intervention': 'Traitement', 'ID_Parcelle': 'Parcelle_Test_2', 'Culture': 'Orge', 'Nom_Produit': 'Fongicide Z', 'Dose_Ha': 1.0, 'Surface_Travaillée_Ha': 5, 'Cible': 'Oïdium', 'Observations': '', 'Type_Intervention': 'Fongicide'},
        ]
        df_mock = pd.DataFrame(mock_data)
        # Filter mock data by target_parcelles
        df_phyto = df_mock[df_mock['ID_Parcelle'].isin(target_parcelles)]

     # Get Metadata for all parcels in campaign (optimize: fetch once)
    metadata_map = loader.get_parcel_metadata(target_campaign)

    # Patch Surface: Check for scaling issues (e.g. 209 instead of 2.09)
    for pid, meta in metadata_map.items():
        try:
            surf = float(meta.get('Surface', 0))
            if surf > 50: # Heuristic: unlikely to have > 50ha parcels, likely scaling error from 2,09 -> 209
                meta['Surface'] = surf / 100
        except:
            pass

    # Patch Surface in DataFrames (Phyto & Ferti)
    # Applied to 'Surface_Travaillée_Ha' column
    def patch_surface_column(df):
        if 'Surface_Travaillée_Ha' in df.columns:
            # FORCE FLOAT conversion immediately to avoid int64 lock
            df['Surface_Travaillée_Ha'] = df['Surface_Travaillée_Ha'].astype(float)
            
            # Mask for values > 50 (heuristic)
            mask = df['Surface_Travaillée_Ha'] > 50
            df.loc[mask, 'Surface_Travaillée_Ha'] = df.loc[mask, 'Surface_Travaillée_Ha'] / 100
        return df

    df_phyto = patch_surface_column(df_phyto)
    # Also patch ferti df now (or later, but better here for consistency)
    # Ideally we patch the main df_campaign, but we already split.
    # We will patch df_ferti later when we filter it, or patch the source df_campaign?
    # Let's patch df_campaign actually, but we are past that point. 
    # Let's patch df_phyto here.

    # Group and Generate Phyto
    # Group and Generate Phyto
    # Refactored: One PDF per Parcel
    grouped_phyto = {}
    if not df_phyto.empty:
        for p in df_phyto['ID_Parcelle'].unique():
            subset = df_phyto[df_phyto['ID_Parcelle'] == p].sort_values(by='Date')
            p_meta = metadata_map.get(p, {})
            grouped_phyto[p] = {'data': subset.to_dict('records'), 'meta': p_meta}
    elif mock_mode:
             grouped_phyto['Parcelle_Test_1'] = {
                'data': mock_data[0:2],
                'meta': {'Culture': 'Blé', 'Surface': 10.5, 'Ilot_PAC': 'Ilot_123', 'Precedent': 'Colza'}
            }

    timestamp = datetime.now().strftime('%Y%m%d_%H%M')

    for p_id, p_data in grouped_phyto.items():
        safe_pid = str(p_id).replace(" ", "_").replace("/", "-")
        output_filename = os.path.join(OUTPUT_DIR, f"Registre_Phytosanitaire_{target_campaign}_{safe_pid}_{timestamp}.pdf")
        
        # Pass single parcel dict
        single_payload = {p_id: p_data}
        
        generator = ReportGenerator(output_filename)
        generator.generate_phyto_register(target_campaign, single_payload)
        print(f"Fichier généré : {output_filename}")
        
        if uploader:
            # uploader.upload_file(output_filename, FOLDER_IDS['PHYTO'])
            pass


    # --- PART 2: Fertilization ---
    print("\n--- Génération Bilan Fertilisation ---")
    
    # Filter for Ferti
    df_ferti = df_campaign[df_campaign['Nature_Intervention'] == "Fertilisation"]
    
    # Apply Parcel Filter
    df_ferti = df_ferti[df_ferti['ID_Parcelle'].isin(target_parcelles)]
    df_ferti = patch_surface_column(df_ferti)
    
    print(f"{len(df_ferti)} interventions fertilisation trouvées pour la sélection.")

    ferti_grouped = {}
    if len(df_ferti) == 0 and mock_mode:
        print("Utilisation de données fictives Fertilisation.")
        # Mock logic
        mock_parcelles_names = ['Parcelle_Test_1', 'Parcelle_Test_2']
        for p_name in mock_parcelles_names:
            if p_name in target_parcelles and p_name == 'Parcelle_Test_1':
                ferti_grouped[p_name] = {
                    'Apports': [{'Date': pd.Timestamp('2023-03-15'), 'Nom_Produit': 'Ammonitrate', 'Dose_Ha': 150, 'Unité_Dose': 'kg/ha', 'N/ha': 50, 'P/ha': 0, 'K/ha': 0}],
                    'Besoins': {'Culture': 'Blé', 'Besoin_N': 180, 'Besoin_P': 60, 'Besoin_K': 40},
                    'Sol': {'Reliquat': 30, 'Humus': 10},
                    'meta': {'Culture': 'Blé', 'Surface': 10.5, 'Ilot_PAC': 'Ilot_123', 'Precedent': 'Colza'}
                }
    elif not df_ferti.empty:
         for p in df_ferti['ID_Parcelle'].unique():
             p_meta = metadata_map.get(p, {})
             ferti_grouped[p] = {
                 'Apports': df_ferti[df_ferti['ID_Parcelle'] == p].to_dict('records'),
                 'Besoins': {'Culture': p_meta.get('Culture', 'Inconnue'), 'Besoin_N': 0, 'Besoin_P': 0, 'Besoin_K': 0},
                 'Sol': {},
                 'meta': p_meta
             }

    # Generate Individual PDF
    for p_id, p_data in ferti_grouped.items():
        safe_pid = str(p_id).replace(" ", "_").replace("/", "-")
        output_filename_ferti = os.path.join(OUTPUT_DIR, f"Bilan_Fertilisation_{target_campaign}_{safe_pid}_{timestamp}.pdf")
        
        single_payload = {p_id: p_data}
        
        generator_ferti = ReportGenerator(output_filename_ferti)
        generator_ferti.generate_ferti_balance(target_campaign, single_payload)
        print(f"Fichier généré : {output_filename_ferti}")

        if uploader:
            # uploader.upload_file(output_filename_ferti, FOLDER_IDS['FERTI'])
            pass

    # --- PART 3: ITK ---
    print("\n--- Génération Itinéraire Technique (ITK) ---")
    
    itk_grouped = {}
    
    # We need to process ALL interventions for the target parcels, not just filtered ones
    # Filter global df_campaign by target_parcelles
    df_itk_all = df_campaign[df_campaign['ID_Parcelle'].isin(target_parcelles)]
    df_itk_all = patch_surface_column(df_itk_all)

    if not df_itk_all.empty:
        for p in df_itk_all['ID_Parcelle'].unique():
             subset = df_itk_all[df_itk_all['ID_Parcelle'] == p].sort_values(by='Date')
             p_meta = metadata_map.get(p, {})
             
             # Categorize
             # Categories: Travail du sol, Semis, Fertilisation, Traitement, Récolte
             # Based on 'Nature_Intervention'
             
             cat_data = {
                 'meta': p_meta,
                 'Travail du sol': [],
                 'Semis': [],
                 'Fertilisation': [],
                 'Traitement': [],
                 'Récolte': []
             }
             
             for _, row in subset.iterrows():
                 nature = str(row['Nature_Intervention']).strip()
                 record = row.to_dict()
                 
                 if nature in ['Déchaumage', 'Labour', 'Travail du sol']:
                     cat_data['Travail du sol'].append(record)
                 elif nature in ['Semi', 'Semis']:
                     cat_data['Semis'].append(record)
                 elif nature == 'Fertilisation':
                     cat_data['Fertilisation'].append(record)
                 elif nature == 'Traitement':
                     cat_data['Traitement'].append(record)
                 elif nature in ['Récolte', 'Moisson']:
                     cat_data['Récolte'].append(record)
                 else:
                     # Fallback or Observation?
                     # Maybe append to Travail du Sol if it looks like mechanisation?
                     # or create 'Autres'?
                     pass 
             
             itk_grouped[p] = cat_data

    # Generate Individual PDF
    for p_id, p_data in itk_grouped.items():
        safe_pid = str(p_id).replace(" ", "_").replace("/", "-")
        output_filename_itk = os.path.join(OUTPUT_DIR, f"Itineraire_Technique_{target_campaign}_{safe_pid}_{timestamp}.pdf")
        
        single_payload = {p_id: p_data}
        
        generator_itk = ReportGenerator(output_filename_itk)
        generator_itk.generate_itk(target_campaign, single_payload)
        print(f"Fichier généré : {output_filename_itk}")
        
        if uploader:
            # uploader.upload_file(output_filename_itk, FOLDER_IDS['ITK'])
            pass

if __name__ == "__main__":
    main()

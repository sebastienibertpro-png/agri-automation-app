import pandas as pd
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class DataLoader:
    def __init__(self, file_path, use_cloud=True, credentials_dict=None):
        self.file_path = file_path
        self.use_cloud = use_cloud
        self.credentials_dict = credentials_dict
        self.xl = None
        self.gc = None
        self.sh = None
        self.credentials_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")

    def load_source(self):
        """Loads data source: Google Sheets if available/requested, else local Excel."""
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        if self.use_cloud:
            print(f"Tentative de connexion Google Sheets...")
            try:
                creds = None
                if self.credentials_dict:
                    # Priority 1: Dict provided (Streamlit Secrets)
                    # Create credentials object from dict
                    creds = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials_dict, scope)
                elif os.path.exists(self.credentials_path):
                    # Priority 2: Local file
                    creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
                
                if creds:
                    # Authorize gspread directly with credentials object
                    self.gc = gspread.authorize(creds)
                    
                    # Open by name
                    self.sh = self.gc.open("MASTER_EXPLOITATION") 
                    print("Connexion Cloud réussie !")
                    return True
                else:
                    print("Aucun identifiant Cloud trouvé (ni fichier, ni dict).")
            except Exception as e:
                print(f"Erreur connexion Cloud: {e}. Passage en mode Local.")
        
        # Fallback or Local Mode
        
        # Fallback or Local Mode
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Fichier local non trouvé: {self.file_path}")
        self.xl = pd.ExcelFile(self.file_path)
        print("Fichier Local chargé.")
        return False

    def _get_data(self, sheet_name):
        """Internal helper to get dataframe from active source."""
        if self.sh:
            try:
                worksheet = self.sh.worksheet(sheet_name)
                data = worksheet.get_all_records()
                return pd.DataFrame(data)
            except gspread.exceptions.WorksheetNotFound:
                print(f"Attention: Onglet '{sheet_name}' introuvable sur le Cloud.")
                return pd.DataFrame()
        elif self.xl:
            return pd.read_excel(self.file_path, sheet_name=sheet_name)
        else:
            raise Exception("Source de données non initialisée.")

    def get_interventions(self, campaign=None):
        """Loads JOURNAL_INTERVENTION."""
        df = self._get_data("JOURNAL_INTERVENTION")
        
        # Standardize Date
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        if campaign and not df.empty:
            df['Campagne'] = df['Campagne'].astype(str)
            df = df[df['Campagne'] == str(campaign)]
            
        return df

    def get_intrants(self):
        """Loads REF_INTRANTS."""
        return self._get_data("REF_INTRANTS")

    def get_parcelles(self):
        """Loads REF_PARCELLES."""
        return self._get_data("REF_PARCELLES")
    
    def get_assolement(self, campaign=None):
        """Loads ASSOLEMENT."""
        df = self._get_data("ASSOLEMENT")
        if campaign and not df.empty:
            df['Campagne'] = df['Campagne'].astype(str)
            df = df[df['Campagne'] == str(campaign)]
        return df

    def get_parcel_metadata(self, campaign):
        """
        Returns a dictionary keyed by ID_Parcelle containing: 
        Culture, Surface, Ilot_PAC, Precedent_Cultural
        Merges ASSOLEMENT and REF_PARCELLES.
        """
        df_asso = self.get_assolement(campaign)
        df_ref = self.get_parcelles()
        
        # Merge Assolement (Campagne specific) with Ref (Static)
        # Left join on ID_Parcelle
        merged = pd.merge(df_asso, df_ref, on='ID_Parcelle', how='left', suffixes=('', '_ref'))
        
        metadata = {}
        for _, row in merged.iterrows():
            p_id = row['ID_Parcelle']
            metadata[p_id] = {
                'Culture': row.get('Culture', 'N/A'),
                'Surface': row.get('Surface_Référence_Ha', 'N/A'),
                'Ilot_PAC': row.get('îlot PAC', 'N/A'), # From REF_PARCELLES
                'Precedent': row.get('Precedent_Cultural', 'N/A'),
                'Variete': row.get('Variété', '')
            }
        return metadata

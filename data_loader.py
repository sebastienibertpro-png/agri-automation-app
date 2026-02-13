import pandas as pd
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st # Ajouté pour afficher les erreurs

class DataLoader:
    def __init__(self, file_path, use_cloud=True, credentials_dict=None):
        self.file_path = file_path
        self.use_cloud = use_cloud and (credentials_dict or os.path.exists("credentials.json"))
        self.credentials_dict = credentials_dict
        self.xl = None
        self.gc = None
        self.sh = None
        self.credentials_path = "credentials.json"

    def load_source(self):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        if self.use_cloud:
            try:
                creds = None
                if self.credentials_dict:
                    creds = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials_dict, scope)
                elif os.path.exists(self.credentials_path):
                    creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_path, scope)
                
                if creds:
                    self.gc = gspread.authorize(creds)
                    self.sh = self.gc.open("MASTER_EXPLOITATION") 
                    return True
            except Exception as e:
                st.error(f"Erreur Connexion Cloud : {e}") # Affiche l'erreur en rouge
        
        if not os.path.exists(self.file_path):
            st.warning("Fichier local absent et connexion Cloud échouée.")
            return False
            
        self.xl = pd.ExcelFile(self.file_path)
        return False

    def _get_data(self, sheet_name):
        if self.sh:
            try:
                return pd.DataFrame(self.sh.worksheet(sheet_name).get_all_records())
            except Exception as e:
                st.error(f"Erreur lecture onglet '{sheet_name}' : {e}") # Affiche l'erreur précise
                return pd.DataFrame()
        elif self.xl:
            return pd.read_excel(self.file_path, sheet_name=sheet_name)
        return pd.DataFrame()

    def get_interventions(self, campaign=None):
        df = self._get_data("JOURNAL_INTERVENTION")
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        if campaign and not df.empty:
            df['Campagne'] = df['Campagne'].astype(str)
            df = df[df['Campagne'] == str(campaign)]
        return df

    def get_intrants(self):
        return self._get_data("REF_INTRANTS")

    def get_parcelles(self):
        return self._get_data("REF_PARCELLES")
    
    def get_assolement(self, campaign=None):
        df = self._get_data("ASSOLEMENT")
        if campaign and not df.empty:
            df['Campagne'] = df['Campagne'].astype(str)
            df = df[df['Campagne'] == str(campaign)]
        return df

    def get_parcel_metadata(self, campaign):
        df_asso = self.get_assolement(campaign)
        df_ref = self.get_parcelles()
        if df_asso.empty: return {}
        
        merged = pd.merge(df_asso, df_ref, on='ID_Parcelle', how='left', suffixes=('', '_ref'))
        metadata = {}
        for _, row in merged.iterrows():
            metadata[row['ID_Parcelle']] = {
                'Culture': row.get('Culture', 'N/A'),
                'Surface': row.get('Surface_Référence_Ha', 'N/A'),
                'Ilot_PAC': row.get('îlot PAC', 'N/A'),
                'Precedent': row.get('Precedent_Cultural', 'N/A'),
                'Variete': row.get('Variété', '')
            }
        return metadata

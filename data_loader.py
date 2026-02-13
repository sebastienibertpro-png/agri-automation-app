import pandas as pd
import os
import streamlit as st
from streamlit_gsheets import GSheetsConnection

class DataLoader:
    def __init__(self, file_path, use_cloud=True, credentials_dict=None):
        self.file_path = file_path
        self.use_cloud = use_cloud
        self.conn = None
        self.xl = None

    def load_source(self):
        if self.use_cloud:
            try:
                # Connexion simple et robuste via Streamlit
                self.conn = st.connection("gsheets", type=GSheetsConnection)
                print("Connexion Cloud initialisée via st.connection")
                return True
            except Exception as e:
                st.error(f"Erreur init connexion: {e}. Passage en mode Local.")
        
        if not os.path.exists(self.file_path):
            return False
        self.xl = pd.ExcelFile(self.file_path)
        return False

    def _get_data(self, sheet_name):
        # Nom de votre fichier Google Sheets
        SPREADSHEET_NAME = "MASTER_EXPLOITATION" 
        
        if self.conn:
            try:
                # Lecture directe et optimisée
                return self.conn.read(worksheet=sheet_name, spreadsheet=SPREADSHEET_NAME)
            except Exception as e:
                st.error(f"Erreur lecture onglet '{sheet_name}' : {e}")
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

import pandas as pd
import os
import streamlit as st
from streamlit_gsheets import GSheetsConnection

class DataLoader:
    def __init__(self, file_path, use_cloud=True, credentials_dict=None):
        self.file_path = file_path
        self.use_cloud = use_cloud
        self.conn = None
        # We don't need credentials_dict explicitly passed here if st.secrets is set up correctly
        # The GSheetsConnection handles it automatically from st.secrets
        self.xl = None # Keep for local Excel fallback

    def load_source(self):
        """Loads data source: Google Sheets if available/requested, else local Excel."""
        
        if self.use_cloud:
            try:
                self.conn = st.connection("gsheets", type=GSheetsConnection)
                # Test connection by reading one small thing
                # But GSheetsConnection is lazy, so we just assume True if no error
                print("Connexion Cloud initialisée via st.connection")
                return True
            except Exception as e:
                st.error(f"Erreur init connexion: {e}. Passage en mode Local.")
        
        # Fallback or Local Mode
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Fichier local non trouvé: {self.file_path}")
        self.xl = pd.ExcelFile(self.file_path)
        print("Fichier Local chargé.")
        return False

    def _get_data(self, sheet_name):
        """Internal helper to get dataframe from active source."""
        # GSheetsConnection reads the whole file or by worksheet
        # It usually expects a spreadsheet name or URL in the query or secrets
        # We can pass the spreadsheet name directly to read() if configured, 
        # or assuming the connection is bound to a specific sheet.
        # Actually, st-gsheets-connection .read() takes a worksheet parameter.
        
        # We need to specify the spreadsheet title here if it's not default
        SPREADSHEET_NAME = "MASTER_EXPLOITATION"
        
        if self.conn:
            try:
                # read() returns a DataFrame directly
                return self.conn.read(worksheet=sheet_name, spreadsheet=SPREADSHEET_NAME)
            except Exception as e:
                st.error(f"Erreur lecture onglet '{sheet_name}' : {e}")
                return pd.DataFrame()
        elif self.xl: # Check if local Excel is loaded
            return pd.read_excel(self.file_path, sheet_name=sheet_name)
        else:
            # This case should ideally not be reached if load_source was called correctly
            # and either conn or xl was initialized.
            raise Exception("Source de données non initialisée.")

    def get_interventions(self):
        try:
            if self.conn:
                df = self.conn.read(worksheet="JOURNAL_INTERVENTION", ttl=600, spreadsheet="MASTER_EXPLOITATION")
            else:
                df = self.xl.parse("JOURNAL_INTERVENTION")
            return df
        except Exception as e:
            st.error(f"Erreur lecture Interventions: {e}")
            return pd.DataFrame()

    def get_intrants(self):
        """Loads REF_INTRANTS."""
        return self._get_data("REF_INTRANTS")

    def get_parcelles(self):
        """Loads REF_PARCELLES."""
        return self._get_data("REF_PARCELLES")
    
    def get_assolement(self, campaign=None):
        try:
            if self.conn:
                df = self.conn.read(worksheet="ASSOLEMENT", ttl=600, spreadsheet="MASTER_EXPLOITATION")
            else:
                df = self.xl.parse("ASSOLEMENT")
            
            if campaign and not df.empty:
                df['Campagne'] = df['Campagne'].astype(str)
                df = df[df['Campagne'] == str(campaign)]
            return df
        except Exception as e:
            # Fallback or error
            return pd.DataFrame()

    def get_products_ref(self):
        try:
            if self.conn:
                # Assuming tab name is 'Produits' or 'Référentiel Produits'. Let's try 'Produits' first then 'Referentiel'
                try:
                    df = self.conn.read(worksheet="Produits", ttl=600, spreadsheet="MASTER_EXPLOITATION")
                except:
                    df = self.conn.read(worksheet="Référentiel Produits", ttl=600, spreadsheet="MASTER_EXPLOITATION")
            else:
                # Local
                try:
                    df = self.xl.parse("Produits")
                except:
                    df = self.xl.parse("Référentiel Produits")
            return df
        except Exception as e:
            return pd.DataFrame()

    def get_ref_compteurs(self):
        """Loads REF_COMPTEURS (ID_Compteur, Numero_Serie_Compteur, Reseau_type, Mail_Contact-Reseau, Usage%)."""
        return self._get_data("REF_COMPTEURS")

    def get_ref_secteurs(self):
        """Loads REF_SECTEURS (ID_Secteur, ID_Compteur, Surface_ha, etc.)."""
        return self._get_data("REF_SECTEURS")

    def get_releves_compteurs(self):
        """Loads RELEVES_COMPTEURS (ID_Compteur, Date_Relevé, Index_m3)."""
        return self._get_data("RELEVES_COMPTEURS")

    def get_journal_irrigation(self):
        """Loads JOURNAL_IRRIGATION (ID_Secteur, Date_Debut, Date_Fin, Vol_m3, etc.)."""
        return self._get_data("JOURNAL_IRRIGATION")

    def get_consumption_data(self, campaign):
        """
        Calculates consumption per meter for a given campaign.
        Consommation = (Index_N - Index_N-1) * Usage%
        """
        df_releves = self.get_releves_compteurs()
        df_ref = self.get_ref_compteurs()

        if df_releves.empty or df_ref.empty:
            return pd.DataFrame()

        # Filter by Campaign (assuming Date_Relevé contains year or we filter by campaign range)
        # Assuming campaign is a year like 2024
        df_releves['Date_Relevé'] = pd.to_datetime(df_releves['Date_Relevé'], errors='coerce')
        # Campaigns usually cover summer. Let's filter by year for now or specific month ranges
        df_releves = df_releves[df_releves['Date_Relevé'].dt.year == int(campaign)]

        if df_releves.empty:
            return pd.DataFrame()

        # Sort by date
        df_releves = df_releves.sort_values(by=['ID_Compteur', 'Date_Relevé'])

        # Calculate difference (Index - Previous Index)
        df_releves['Diff_m3'] = df_releves.groupby('ID_Compteur')['Index_m3'].diff()

        # Merge with Ref to get Usage% and Reseau_type
        # ID_cCompteur vs ID_Compteur? User said ID_cCompteur in REF_COMPTEURS, but Releves usually matches.
        # Let's check columns for ID_cCompteur or ID_Compteur
        id_col_ref = 'ID_cCompteur' if 'ID_cCompteur' in df_ref.columns else 'ID_Compteur'
        id_col_releves = 'ID_Compteur' # Assuming standard

        df_merged = pd.merge(df_releves, df_ref, left_on=id_col_releves, right_on=id_col_ref, how='left')

        # Apply Usage% (coerce to float if needed)
        df_merged['Usage%'] = pd.to_numeric(df_merged['Usage%'], errors='coerce').fillna(1.0)
        df_merged['Conso_Reelle_m3'] = df_merged['Diff_m3'] * df_merged['Usage%']

        return df_merged

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

    def get_planned_treatments(self, campaign):
        df = self.get_interventions()
        if df.empty: return pd.DataFrame()
        
        # Filter Campaign
        df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
        df = df[df['Campagne'] == int(campaign)]
        
        # Filter Planned & Treatment
        # Status column might be "Stat_Intervention", "Statut_Intervention", "Statut" or "Etat".
        status_col = None
        cols_to_check = ['Stat_Intervention', 'Statut_Intervention', 'Statut', 'Etat']
        
        for col in cols_to_check:
            if col in df.columns:
                status_col = col
                break
            
        if status_col:
            # Flexible check for "Prévu", "Prévue", "prévu "
            df = df[df[status_col].astype(str).str.strip().str.lower().str.startswith("prév")]
        
        df = df[df['Nature_Intervention'] == "Traitement"]
        return df

    def sort_products_by_formulation(self, products_list):
        """
        Sorts a list of product dicts based on formulation priority.
        Priority:
        1. Sachets hydrosolubles (Solu-Sachets)
        2. WP / WG (Poudres/Granulés)
        3. SC (Suspensions)
        4. EC (Emulsions)
        5. SL (Liquides)
        """
        # Load Ref Intrants (User said 'REF_INTRANTS' has the data)
        df_ref = self.get_intrants()
        
        # Load Ref Intrants (User said 'REF_INTRANTS' has the data)
        df_ref = self.get_intrants()
        
        # Create a mapping Product -> Formulation
        # Assuming cols in REF_INTRANTS: 'Nom_Intrant', 'Formulation'
        # User insists on 'Formulation' column only.
        form_map = {}
        if not df_ref.empty:
            # Normalize cols to match user request exactly
            # We look for a column that contains "Formulation" (case insensitive)
            target_col = None
            for col in df_ref.columns:
                if "formulation" in str(col).lower():
                    target_col = col
                    break
            
            # If not found, double check 'Type' just in case but prioritize Formulation
            if not target_col:
                target_col = 'Formulation' # Hope for the best or it will be empty
                
            for _, row in df_ref.iterrows():
                # Name is 'Nom_Produit' based on debug output
                p_name_ref = str(row.get('Nom_Produit', row.get('Nom_Intrant', ''))).strip().lower()
                form_val = str(row.get(target_col, '')).strip().upper()
                if p_name_ref:
                    form_map[p_name_ref] = form_val
        
        # Define Priority
        # We need to map actual codes (WG, EC...) to 1, 2, 3...
        # Sachet is tricky. Often "WS" or specific packaging? User said "Sachets hydrosolubles".
        # Let's assume Formulation field might contain "Sachet" or code "SB", "WS"?
        # Standard logic: Use specific codes if known, else keywords.
        
        
        def get_rank(p_item):
            # Try multiple keys for product name
            p_name = str(p_item.get('Produit', p_item.get('Nom_Produit', ''))).strip().lower()
            form = form_map.get(p_name, '')
            
            # --- CRITICAL FIX: Inject Formulation back into item ---
            p_item['Formulation'] = form
            # -------------------------------------------------------
            
            # 1. Sachets
            if 'SACHET' in form or 'HYDROSOLUBLE' in form or form in ['WS', 'SB']: return 1
            # 2. WP / WG
            if form in ['WP', 'WG', 'GR', 'SG', 'DG']: return 2
            # 3. SC
            if form in ['SC', 'CS', 'SE']: return 3
            # 4. EC
            if form in ['EC', 'EW', 'EO', 'ME']: return 4
            # 5. SL
            if form in ['SL', 'SP']: return 5
            
            return 99 # Unknown/Last
            
        return sorted(products_list, key=get_rank)

    def update_intervention_status(self, intervention_id, new_status="Réalisé"):
        """
        Updates the status of an intervention (or group) in the source.
        Only works if using Cloud Connection.
        intervention_id: Ideally a unique ID per row. 
        But here we grouped by (Parcelle + Date + Products).
        We define ID as a composite string or allow fuzzy matching?
        User prompt: "Génération : Antigravity insère un QR Code unique...".
        Let's assume we generated a unique ID (e.g., hash of row or composite key).
        For simplicity, let's say ID = "PARCELLE|DATE|PRODUIT" (or just first product).
        
        Or simpler: The QR Code contains the 'row_index' if we assume static data. But sorting changes row index.
        Best approach for Sheets without unique IDs: Use composite key to find row.
        Composite Key: Parcelle + Date + Nature + Produit.
        If multiple rows match (same product twice?), update all.
        """
        if not self.conn:
            st.error("Mise à jour impossible en local (Lecture seule).")
            return False
            
        try:
            # 1. Read fresh data
            df = self.conn.read(worksheet="JOURNAL_INTERVENTION", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            
            # 2. Parse ID to find rows
            # Expected ID Format: "P-{parcelle}_D-{date_str}" (Updates all treatments for this parcelle/date)
            # OR specific row ID. 
            # Given the Phyto Sheet is for a MIX (Bouillie), it applies to multiple rows (one per product).
            # So updating by Parcelle + Date + Nature='Traitement' is the logical action.
            
            # Helper to parse ID
            # Let's assume ID is "P_DATE" e.g. "Parcelle1_20240415"
            # FIX: Use rsplit to allow underscores in Parcelle Name
            # (If parcelle is "A2_Buissons", ID is "A2_Buissons_20240415", rsplit gives ["A2_Buissons", "20240415"])
            parts = intervention_id.rsplit('_', 1) 
            if len(parts) < 2: return False
            
            p_target = parts[0]
            d_target_str = parts[1] # YYYYMMDD
            
            # Normalize Date in DF
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            
            # Determine Status Column
            status_col = 'Statut_Intervention'
            if 'Statut_Intervention' not in df.columns:
                if 'Statut' in df.columns:
                    status_col = 'Statut'
                elif 'Etat' in df.columns:
                    status_col = 'Etat'
                else:
                    st.error("Colonne 'Statut_Intervention', 'Statut' ou 'Etat' introuvable dans JOURNAL_INTERVENTION.")
                    return False

            # Filter
            # Flexible Match
            # Date Matching (Flexible)
            df['Target_Date_Str'] = df['Date'].dt.strftime('%Y%m%d')
            
            m_p = df['ID_Parcelle'] == p_target
            m_d = df['Target_Date_Str'] == d_target_str
            m_n = df['Nature_Intervention'] == 'Traitement'
            m_s = df[status_col].astype(str).str.lower().str.startswith('prév')
            
            mask = m_p & m_d & m_n & m_s
                   
            if not df[mask].empty:
                # Update
                df.loc[mask, status_col] = new_status
                
                # Cleanup temporary column
                if 'Target_Date_Str' in df.columns:
                     df = df.drop(columns=['Target_Date_Str'])
                     
                # Write back
                self.conn.update(worksheet="JOURNAL_INTERVENTION", data=df, spreadsheet="MASTER_EXPLOITATION")
                return True
            else:
                st.warning("Aucune intervention correspondante trouvée (ou déjà réalisée).")
                return False
                
        except Exception as e:
            st.error(f"Erreur mise à jour: {e}")
            return False

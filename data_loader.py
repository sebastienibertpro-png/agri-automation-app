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
        self._cache = {} # Local session cache

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
        """Internal helper to get dataframe from active source with caching."""
        SPREADSHEET_NAME = "MASTER_EXPLOITATION"
        
        df = pd.DataFrame()
        if self.conn:
            try:
                # TTL à 60 secondes : rafraîchissement en moins d'une minute, sans exploser le quota API (60 req/min)
                df = self.conn.read(worksheet=sheet_name, spreadsheet=SPREADSHEET_NAME, ttl=60)
            except Exception as e:
                st.error(f"Erreur lecture onglet '{sheet_name}' : {e}")
        elif self.xl:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
        else:
            raise Exception("Source de données non initialisée.")
        
        return df

    def clear_cache(self):
        """Clears the local session cache."""
        st.cache_data.clear()

    def get_interventions(self):
        return self._get_data("JOURNAL_INTERVENTION")

    def get_cartographie_ref(self):
        """Loads REF_CARTOGRAPHIE for Telepac GeoJSON storage."""
        try:
            return self._get_data("REF_CARTOGRAPHIE")
        except:
             return pd.DataFrame()

    def get_intrants(self):
        """Loads REF_INTRANTS."""
        return self._get_data("REF_INTRANTS")

    def get_parcelles(self):
        """Loads REF_PARCELLES."""
        return self._get_data("REF_PARCELLES")
    
    def get_assolement(self, campaign=None):
        df = self._get_data("ASSOLEMENT")
        if campaign and not df.empty:
            df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            df = df[df['Campagne'] == int(campaign)]
        return df

    def get_products_ref(self):
        try:
            if self.conn:
                # Assuming tab name is 'Produits' or 'Référentiel Produits'. Let's try 'Produits' first then 'Referentiel'
                try:
                    df = self.conn.read(worksheet="Produits", ttl=60, spreadsheet="MASTER_EXPLOITATION")
                except:
                    df = self.conn.read(worksheet="Référentiel Produits", ttl=60, spreadsheet="MASTER_EXPLOITATION")
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

    def get_compteurs_gps(self):
        """Loads REF_COMPTEURS and cleans up Latitude and Longitude columns."""
        df = self.get_ref_compteurs()
        if df.empty:
            return pd.DataFrame()
            
        # Parse Localisation_GPS column
        # Example format "46.603354, 1.888334"
        if 'Localisation_GPS' in df.columns:
            # Drop empty strings or NAs
            df = df.dropna(subset=['Localisation_GPS'])
            df = df[df['Localisation_GPS'].astype(str).str.strip() != '']
            
            # Split by comma
            # expand=True returns a DataFrame with 2 columns
            gps_split = df['Localisation_GPS'].astype(str).str.split(',', expand=True)
            
            if gps_split.shape[1] >= 2:
                df['Latitude'] = pd.to_numeric(gps_split[0], errors='coerce')
                df['Longitude'] = pd.to_numeric(gps_split[1], errors='coerce')
            else:
                 df['Latitude'] = None
                 df['Longitude'] = None
        else:
             df['Latitude'] = None
             df['Longitude'] = None
        
        # Return only rows with valid GPS coordinates
        return df.dropna(subset=['Latitude', 'Longitude'])
        return df.dropna(subset=['Latitude', 'Longitude'])

    def get_ref_secteurs(self):
        """Loads REF_SECTEURS (ID_Secteur, ID_Compteur, Surface_ha, etc.)."""
        return self._get_data("REF_SECTEURS")

    def get_materiels(self):
        """Loads REF_MATERIELS (ID_Materiel, Marque, Modele, etc.)."""
        return self._get_data("REF_MATERIELS")

    def get_maintenance_history(self, id_materiel=None):
        """Loads JOURNAL_MAINTENANCE. Optionally filters by ID_Materiel."""
        df = self._get_data("JOURNAL_MAINTENANCE")
        if df.empty:
            return pd.DataFrame()
            
        # Ensure proper column is parsed
        if 'ID_Materiel' in df.columns and id_materiel:
             # Ensure string match
             df['ID_Materiel'] = df['ID_Materiel'].astype(str)
             df = df[df['ID_Materiel'] == str(id_materiel)]
             
        if 'Date' in df.columns:
            # Enforce datetime formatting
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
            df = df.sort_values(by='Date', ascending=False)
            
        return df

    def get_releves_compteurs(self):
        """Loads RELEVES_COMPTEURS (ID_Compteur, Date_Relevé, Index_m3)."""
        return self._get_data("RELEVES_COMPTEURS")

    def get_journal_irrigation(self):
        """Loads JOURNAL_IRRIGATION (ID_Secteur, Date_Debut, Date_Fin, Vol_m3, etc.)."""
        return self._get_data("JOURNAL_IRRIGATION")

    def get_ppf(self, campaign):
        """Loads PPF and filters by campaign."""
        df = self._get_data("PPF")
        if not df.empty and 'Campagne' in df.columns:
            df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            df = df[df['Campagne'] == int(campaign)]
        return df

    def get_ref_gren_cipan(self):
        return self._get_data("REF_GREN_CIPAN")
        
    def get_ref_gren_precedents(self):
        return self._get_data("REF_GREN_Précédents")
        
    def get_ref_gren_humus(self):
        return self._get_data("REF_GREN_Humus")
        
    def get_ref_gren_coef(self):
        return self._get_data("REF_GREN_Coef")

    def get_consumption_data(self, campaign):
        """
        Calculates consumption per meter for a given campaign.
        Consommation = (Index_N - Index_N-1) * Usage%
        """
        df_releves = self.get_releves_compteurs()
        df_ref = self.get_ref_compteurs()

        if df_releves.empty or df_ref.empty:
            return pd.DataFrame()

        # Sort by date for correct diff
        df_releves['Date_Relevé'] = pd.to_datetime(df_releves['Date_Relevé'], errors='coerce', dayfirst=True)
        df_releves = df_releves.sort_values(by=['ID_Compteur', 'Date_Relevé'])

        # Calculate difference (Index - Previous Index) BEFORE filtering
        # This allows getting the consumption for the first reading of a campaign
        df_releves['Diff_m3'] = df_releves.groupby('ID_Compteur')['Index_m3'].diff()

        # Filter by Campaign AFTER diff calculation
        df_filtered_releves = df_releves[df_releves['Date_Relevé'].dt.year == int(campaign)]

        if df_filtered_releves.empty:
            return pd.DataFrame()

        # Merge with Ref to get Usage% and Reseau_type
        id_col_ref = 'ID_cCompteur' if 'ID_cCompteur' in df_ref.columns else 'ID_Compteur'
        id_col_releves = 'ID_Compteur' 

        df_merged = pd.merge(df_filtered_releves, df_ref, left_on=id_col_releves, right_on=id_col_ref, how='left')

        # Apply Usage% (coerce to float and divide by 100)
        # If the sheet says 30, it means 30% -> 0.3
        df_merged['Usage_Ratio'] = pd.to_numeric(df_merged['Usage%'], errors='coerce').fillna(100.0) / 100.0
        df_merged['Conso_Reelle_m3'] = df_merged['Diff_m3'] * df_merged['Usage_Ratio']

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
        # We start from df_ref to ensure we have all reference parcels, then merge assolement info.
        merged = pd.merge(df_ref, df_asso, on='ID_Parcelle', how='left', suffixes=('', '_asso'))
        
        metadata = {}
        for _, row in merged.iterrows():
            p_id = row['ID_Parcelle']
            
            metadata[p_id] = {
                'Culture': row.get('Culture', 'Inconnue'),
                'Surface': row.get('Surface_Référence_Ha', 0.0),
                'Ilot_PAC': row.get('îlot PAC', 'N/A'),
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

    def get_planned_fertilization(self, campaign):
        df = self.get_interventions()
        if df.empty: return pd.DataFrame()
        
        # Filter Campaign
        df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
        df = df[df['Campagne'] == int(campaign)]
        
        # Filter Planned & Fertilisation
        status_col = None
        for col in ['Stat_Intervention', 'Statut_Intervention', 'Statut', 'Etat']:
            if col in df.columns:
                status_col = col; break
                
        if status_col:
            df = df[df[status_col].astype(str).str.strip().str.lower().str.startswith("prév")]
            
        df = df[df['Nature_Intervention'].astype(str).str.strip().str.lower() == "fertilisation"]
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
            form_orig = form_map.get(p_name, '')
            form_upper = form_orig.upper()
            
            # --- CRITICAL FIX: Abbreviate and Inject Formulation back into item ---
            # Default to original if no match
            short_form = form_orig
            rank = 99
            
            # 1. Sachets
            if any(k in form_upper for k in ['SACHET', 'HYDROSOLUBLE', 'WS', 'SB']): 
                short_form = "Sachet"
                rank = 1
            # 2. WP / WG (Poudres / Granulés)
            elif any(k in form_upper for k in ['POUDRE', 'GRANULE', 'WP', 'WG', 'SG', 'DG', 'GR']): 
                short_form = "WG/WP"
                rank = 2
            # 3. SC (Suspensions)
            elif any(k in form_upper for k in ['SUSPENSION', 'SC', 'CS', 'SE']): 
                short_form = "SC"
                rank = 3
            # 4. Emulsions (EC)
            elif any(k in form_upper for k in ['EMULSION', 'EC', 'EW', 'EO', 'ME']): 
                short_form = "EC"
                rank = 4
            # 5. Solutions (SL)
            elif any(k in form_upper for k in ['SOLUTION', 'LIQUIDE', 'SL', 'SP']): 
                short_form = "SL"
                rank = 5
            
            p_item['Formulation'] = short_form[:15] if len(short_form) > 15 else short_form
            return rank
            
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
            
            p_targets_str = parts[0]
            d_target_str = parts[1] # YYYYMMDD
            
            p_targets = p_targets_str.split('|')
            
            # Normalize Date in DF
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
            
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
            
            m_p = df['ID_Parcelle'].isin(p_targets)
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
                self._cache.pop("JOURNAL_INTERVENTION", None)
                st.cache_data.clear()
                return True
            else:
                st.warning("Aucune intervention correspondante trouvée (ou déjà réalisée).")
                return False
                
        except Exception as e:
            st.error(f"Erreur mise à jour: {e}")
            return False

    def bulk_insert_interventions(self, df_to_append):
        """
        Appends multiple new intervention rows to the JOURNAL_INTERVENTION sheet.
        """
        if not self.conn:
            st.error("Insertion impossible en local (Lecture seule).")
            return False
            
        try:
            # 1. Read existing data
            df_existing = self.conn.read(worksheet="JOURNAL_INTERVENTION", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            
            # 2. Append new data
            # Use pd.concat for pandas >= 1.4.0 instead of append
            df_updated = pd.concat([df_existing, df_to_append], ignore_index=True)
            
            # 3. Write back
            # Streamlit GSheets update replaces the entire worksheet's data with the dataframe
            self.conn.update(worksheet="JOURNAL_INTERVENTION", data=df_updated, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("JOURNAL_INTERVENTION", None)
            st.cache_data.clear()
            return True
            
        except Exception as e:
            st.error(f"Erreur lors de l'insertion en masse : {e}")
            return False

    def insert_row(self, sheet_name: str, row_dict: dict) -> bool:
        if not self.conn:
            st.error("Insertion impossible en local (Lecture seule).")
            return False
            
        try:
            # Relecture à chaud pour éviter les écrasements
            try:
                df = self.conn.read(worksheet=sheet_name, ttl=0, spreadsheet="MASTER_EXPLOITATION")
            except Exception:
                # L'onglet peut être vide ou nouveau
                df = pd.DataFrame()
            
            for col in row_dict:
                if col not in df.columns:
                    df[col] = "" # fallback creation
            
            new_row = pd.DataFrame([row_dict])
            df = pd.concat([df, new_row], ignore_index=True)
            self.conn.update(worksheet=sheet_name, data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop(sheet_name, None)
            st.cache_data.clear()
            return True
            
        except Exception as e:
            st.error(f"Erreur d'insertion dans {sheet_name} : {e}")
            return False

    def delete_interventions(self, intervention_ids: list) -> bool:
        """
        Deletes interventions from the JOURNAL_INTERVENTION sheet by ID.
        Requires Cloud Connection.
        """
        if not self.conn:
            st.error("Suppression impossible en local (Lecture seule).")
            return False
            
        try:
            # 1. Read fresh data
            df = self.conn.read(worksheet="JOURNAL_INTERVENTION", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            
            # 2. Filter out the IDs to delete
            # Convert both to string for robust comparison
            intervention_ids = [str(i) for i in intervention_ids]
            
            if 'ID_Intervention' not in df.columns:
                st.error("Colonne 'ID_Intervention' introuvable.")
                return False
                
            original_count = len(df)
            df = df[~df['ID_Intervention'].astype(str).isin(intervention_ids)]
            new_count = len(df)
            
            if original_count == new_count:
                st.warning("Aucune ligne correspondante trouvée pour la suppression.")
                return False
                
            # 3. Write back
            self.conn.update(worksheet="JOURNAL_INTERVENTION", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("JOURNAL_INTERVENTION", None)
            st.cache_data.clear()
            return True
            
        except Exception as e:
            st.error(f"Erreur lors de la suppression : {e}")
            return False

    def update_ppf(self, ppf_dict: dict) -> bool:
        """
        Inserts or updates a PPF entry for a given Campaign and Parcel.
        Requires Cloud Connection.
        """
        if not self.conn:
            st.error("Écriture impossible en local (Lecture seule).")
            return False
        try:
            try:
                df = self.conn.read(worksheet="PPF", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            except Exception:
                # If the tab does not exist, we cannot safely initialize it without full context.
                # Assuming PPF tab exists as stated.
                st.error("L'onglet PPF est introuvable.")
                return False

            campagne = str(ppf_dict.get("Campagne", "")).strip()
            parcelle = str(ppf_dict.get("ID_Parcelle", "")).strip()
            
            if not campagne or not parcelle:
                st.error("Campagne et ID_Parcelle sont obligatoires pour sauvegarder.")
                return False

            # Ensure all dictionary keys exist in the DF
            for col in ppf_dict:
                if col not in df.columns:
                    df[col] = ""

            # Check for existing match
            mask = (df["Campagne"].astype(str).str.strip() == campagne) & \
                   (df["ID_Parcelle"].astype(str).str.strip() == parcelle)

            new_row = pd.DataFrame([ppf_dict])

            if mask.any():
                idx = df[mask].index[0]
                for col in ppf_dict:
                    df.at[idx, col] = ppf_dict[col]
            else:
                df = pd.concat([df, new_row], ignore_index=True)

            self.conn.update(worksheet="PPF", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("PPF", None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur écriture PPF : {e}")
            return False

    def update_assolement(self, asso_dict: dict) -> bool:
        """
        Inserts or updates an Assolement entry for a given Campaign and Parcel.
        Requires Cloud Connection.
        """
        if not self.conn:
            st.error("Écriture impossible en local (Lecture seule).")
            return False
            
        try:
            # 1. Read fresh data
            df = self.conn.read(worksheet="ASSOLEMENT", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            
            campagne = str(asso_dict.get("Campagne", "")).strip()
            parcelle = str(asso_dict.get("ID_Parcelle", "")).strip()
            
            if not campagne or not parcelle:
                st.error("Campagne et ID_Parcelle sont requis.")
                return False
                
            # Ensure columns exist in the DataFrame
            for col in asso_dict:
                if col not in df.columns:
                    df[col] = ""

            # Standardize campaign as numeric for matching
            df['Camp_Int'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            target_camp = int(campagne)
            
            # Match by Campaign and ID_Parcelle
            mask = (df['Camp_Int'] == target_camp) & (df['ID_Parcelle'].astype(str).str.strip() == parcelle)
            
            if mask.any():
                # Update existing row
                idx = df[mask].index[0]
                for col, val in asso_dict.items():
                    if col in df.columns:
                        df.at[idx, col] = val
            else:
                # Append new row
                # Generate ID_Assolement if it exists in columns but not in dict
                if 'ID_Assolement' in df.columns and ('ID_Assolement' not in asso_dict or not asso_dict['ID_Assolement']):
                    asso_dict['ID_Assolement'] = f"ASSOL_{campagne}_{parcelle}"
                
                new_row = pd.DataFrame([asso_dict])
                df = pd.concat([df, new_row], ignore_index=True)

            # Cleanup
            if 'Camp_Int' in df.columns:
                df = df.drop(columns=['Camp_Int'])

            # 3. Write back
            self.conn.update(worksheet="ASSOLEMENT", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("ASSOLEMENT", None)
            st.cache_data.clear()
            return True
            
        except Exception as e:
            st.error(f"Erreur update_assolement : {e}")
            return False

    def get_observations(self, campagne=None):
        """
        Retrieves field observations from JOURNAL_INTERVENTION.
        """
        df = self.get_interventions()
        if df.empty:
            return df
        
        mask = df['Nature_Intervention'].astype(str).str.upper() == 'OBSERVATION'
        if campagne:
            mask = mask & (df['Campagne'].astype(str) == str(campagne))
            
        return df[mask].copy()

    # -----------------------------------------------------------------------
    # RÉFÉRENTIEL PHYTO — Écriture REF_INTRANTS + REF_USAGES_PHYTO
    # -----------------------------------------------------------------------

    def update_intrant(self, intrant_dict: dict, original_name: str = None) -> bool:
        """
        Ajoute ou met à jour un produit dans REF_INTRANTS (upsert par Nom_Produit).
        Si le produit existe déjà (même Nom_Produit ou original_name), sa ligne est écrasée avec les nouvelles données.
        Sinon, la ligne est ajoutée en bas.
        Fonctionne uniquement en mode Cloud.
        """
        if not self.conn:
            st.error("Écriture impossible en local (Lecture seule).")
            return False
        try:
            df = self.conn.read(worksheet="REF_INTRANTS", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            nom = str(intrant_dict.get("Nom_Produit", "")).strip().upper()

            # S'assurer que toutes les colonnes du dict existent dans le df
            for col in intrant_dict:
                if col not in df.columns:
                    df[col] = ""

            # Chercher une ligne existante par le nouveau nom ou l'ancien nom recherché
            mask = df["Nom_Produit"].astype(str).str.strip().str.upper() == nom
            if original_name:
                orig_nom = str(original_name).strip().upper()
                if orig_nom:
                    mask = mask | (df["Nom_Produit"].astype(str).str.strip().str.upper() == orig_nom)

            new_row = pd.DataFrame([intrant_dict])

            if mask.any():
                # Remplacer les cellules de la ligne existante pour écraser les vieilles données
                idx = df[mask].index[0]
                for col in intrant_dict:
                    df.at[idx, col] = intrant_dict[col]
            else:
                # Ajouter une nouvelle ligne
                df = pd.concat([df, new_row], ignore_index=True)

            self.conn.update(worksheet="REF_INTRANTS", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("REF_INTRANTS", None)  # Invalider le cache local
            st.cache_data.clear() # Force Streamlit GSheetsConnection to drop its TTL cache
            return True
        except Exception as e:
            st.error(f"Erreur écriture REF_INTRANTS : {e}")
            return False

    def overwrite_worksheet(self, sheet_name, data_df) -> bool:
        """
        Replaces the entire content of a worksheet with the provided DataFrame.
        Requires Cloud Connection.
        """
        if not self.conn:
            st.error("Écriture impossible en local (Lecture seule).")
            return False
        try:
            self.conn.update(worksheet=sheet_name, data=data_df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop(sheet_name, None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur overwrite_worksheet {sheet_name} : {e}")
            return False

    def update_usages_phyto(self, n_amm: str, usages: list[dict]) -> bool:
        """
        Remplace tous les usages existants (même N_AMM) dans REF_USAGES_PHYTO
        par la nouvelle liste fournie.
        Crée l'onglet s'il n'existe pas encore.
        """
        if not self.conn:
            st.error("Écriture impossible en local (Lecture seule).")
            return False
        try:
            try:
                df = self.conn.read(worksheet="REF_USAGES_PHYTO", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            except Exception:
                # Onglet inexistant : on part d'un DataFrame vide
                df = pd.DataFrame()

            # Supprimer les anciens usages pour ce N_AMM
            if not df.empty and "N_AMM" in df.columns:
                df = df[df["N_AMM"].astype(str).str.strip() != str(n_amm).strip()]

            # S'assurer des colonnes
            new_df = pd.DataFrame(usages)
            for col in new_df.columns:
                if col not in df.columns:
                    df[col] = ""

            df = pd.concat([df, new_df], ignore_index=True)
            self.conn.update(worksheet="REF_USAGES_PHYTO", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("REF_USAGES_PHYTO", None)
            st.cache_data.clear() # Force Streamlit GSheetsConnection to drop its TTL cache
            return True
        except Exception as e:
            st.error(f"Erreur écriture REF_USAGES_PHYTO : {e}")
            return False

    def get_usages_phyto(self, n_amm: str = None) -> pd.DataFrame:
        """
        Charge REF_USAGES_PHYTO (depuis GSheet ou cache).
        Filtre sur n_amm si fourni.
        """
        df = self._get_data("REF_USAGES_PHYTO")
        if not df.empty and n_amm and "N_AMM" in df.columns:
            df = df[df["N_AMM"].astype(str).str.strip() == str(n_amm).strip()]
        return df

    # -----------------------------------------------------------------------
    # SAUVEGARDE CARTOGRAPHIE DANS GOOGLE SHEETS
    # -----------------------------------------------------------------------

    def save_telepac_to_cloud(self, campaign: str, geojson_str: str) -> bool:
        """
        Sauvegarde le GeoJSON (sous forme de chaîne texte compressée ou brute) 
        dans l'onglet REF_CARTOGRAPHIE pour une campagne donnée.
        """
        if not self.conn:
            st.error("Sauvegarde Cloud impossible en local.")
            return False
            
        try:
            # 1. Lire (ou créer) l'onglet REF_CARTOGRAPHIE
            try:
                df = self.conn.read(worksheet="REF_CARTOGRAPHIE", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            except Exception:
                df = pd.DataFrame(columns=["Campagne", "GeoJSON_Data", "Date_Maj"])
                
            # 2. Préparer la nouvelle ligne
            chunk_size = 40000
            chunks = [geojson_str[i:i+chunk_size] for i in range(0, len(geojson_str), chunk_size)]
            
            new_rows = []
            for i, chunk in enumerate(chunks):
                 new_rows.append({
                     "Campagne": int(campaign),
                     "Chunk_Index": i,
                     "Total_Chunks": len(chunks),
                     "GeoJSON_Data": chunk,
                     "Date_Maj": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                 })
            
            new_df = pd.DataFrame(new_rows)

            # 3. Supprimer les anciennes données de cette campagne
            if not df.empty and "Campagne" in df.columns:
                # Robust conversion to int for filtering
                df['Camp_Int'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
                df = df[df["Camp_Int"] != int(campaign)]
                df = df.drop(columns=['Camp_Int'])
                
                # S'assurer que les colonnes chunk existent
                for col in ["Chunk_Index", "Total_Chunks", "GeoJSON_Data"]:
                     if col not in df.columns:
                          df[col] = 0 if "Index" in col or "Total" in col else ""
                
            # 4. Ajouter et pousser
            df = pd.concat([df, new_df], ignore_index=True)
            self.conn.update(worksheet="REF_CARTOGRAPHIE", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("REF_CARTOGRAPHIE", None)
            st.cache_data.clear()
            return True
            
        except Exception as e:
            st.error(f"Erreur lors de la sauvegarde cartographique sur le Cloud : {e}")
            return False

    def load_telepac_from_cloud(self, campaign: str) -> str:
        """
        Récupère le GeoJSON reconstitué depuis REF_CARTOGRAPHIE.
        Retourne la chaîne JSON, ou None si introuvable.
        """
        df = self.get_cartographie_ref()
        if df.empty or "Campagne" not in df.columns:
            return None
            
        # Filtrer la campagne (conversion robuste en int)
        try:
            df['Camp_Int'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            df_camp = df[df["Camp_Int"] == int(campaign)]
        except:
            # Fallback string matching if numeric conversion fails
            df_camp = df[df["Campagne"].astype(str).str.strip() == str(campaign).strip()]
        
        if df_camp.empty:
            return None
            
        # Trier par chunk index et reconstituer
        if "Chunk_Index" in df_camp.columns:
            df_camp = df_camp.sort_values("Chunk_Index")
            # Clear any potential NaNs in GeoJSON_Data to avoid "nan" string in concatenation
            return "".join(df_camp["GeoJSON_Data"].dropna().astype(str).tolist())
        else:
             # Ancien format (sans chunks)
             val = df_camp["GeoJSON_Data"].iloc[0]
             return str(val) if pd.notna(val) else None
    def get_fuel_conso(self, campaign=None):
        """Loads CONSO_FUEL and filters by campaign year."""
        df = self._get_data("CONSO_FUEL")
        if df.empty:
            return pd.DataFrame()
            
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
            if campaign:
                df = df[df['Date'].dt.year == int(campaign)]
        return df

    def get_achats(self, campaign=None):
        """Loads ACHAT_MASTER and filters by campaign year. ttl=0 ensures fresh data after a write."""
        df = pd.DataFrame()
        SPREADSHEET_NAME = "MASTER_EXPLOITATION"
        if self.conn:
            try:
                # ttl=0 : toujours lire en direct pour éviter d'afficher des données obsolètes
                # après une écriture (update/delete/insert)
                df = self.conn.read(worksheet="ACHAT_MASTER", spreadsheet=SPREADSHEET_NAME, ttl=0)
            except Exception as e:
                st.error(f"Erreur lecture ACHAT_MASTER : {e}")
                return pd.DataFrame()
        elif self.xl:
            df = pd.read_excel(self.file_path, sheet_name="ACHAT_MASTER")
        
        if df.empty:
            return pd.DataFrame()
            
        if 'Campagne' in df.columns and campaign:
            df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            df = df[df['Campagne'] == int(campaign)]
        elif 'Date_facture' in df.columns and campaign:
            df['Date_dt'] = pd.to_datetime(df['Date_facture'], errors='coerce', dayfirst=True)
            df = df[df['Date_dt'].dt.year == int(campaign)]
            
        return df

    def get_product_prices(self, campaign):
        """
        Calculates average unit price per product for a given campaign from ACHAT_MASTER.
        Returns a dictionary { 'PRODUCT_NAME_NORM': price_per_unit }
        """
        df_achats = self.get_achats(campaign)
        if df_achats.empty:
            return {}

        df_achats['Nom_Produit_Norm'] = df_achats['Nom_Produit'].astype(str).str.strip().str.upper()
        df_achats['Quantité_Achetée'] = pd.to_numeric(df_achats['Quantité_Achetée'], errors='coerce').fillna(0)
        df_achats['Montant_Total_Produit_HT'] = pd.to_numeric(df_achats['Montant_Total_Produit_HT'], errors='coerce').fillna(0)

        # Aggregate to get average price
        prices_agg = df_achats.groupby('Nom_Produit_Norm').agg({
            'Quantité_Achetée': 'sum',
            'Montant_Total_Produit_HT': 'sum'
        })

        prices_dict = {}
        for name, row in prices_agg.iterrows():
            if row['Quantité_Achetée'] > 0:
                prices_dict[name] = row['Montant_Total_Produit_HT'] / row['Quantité_Achetée']
            else:
                prices_dict[name] = 0.0
        
        return prices_dict

    def append_achat_master(self, sheet_values: list[list]) -> bool:
        """Appends new rows to the ACHAT_MASTER sheet."""
        if not self.conn:
            st.error("Écriture impossible en local.")
            return False
        try:
            SHEET_NAME = "ACHAT_MASTER"
            SPREADSHEET_NAME = "MASTER_EXPLOITATION"
            
            # Read current
            df_existing = self.conn.read(worksheet=SHEET_NAME, ttl=0, spreadsheet=SPREADSHEET_NAME)
            
            if df_existing.empty:
                # If first time, we just create it with some default columns or as provided
                df_final = pd.DataFrame(sheet_values)
            else:
                # Create new DF with matching columns
                df_new = pd.DataFrame(sheet_values, columns=df_existing.columns[:len(sheet_values[0])])
                # Combine
                df_final = pd.concat([df_existing, df_new], ignore_index=True)
            
            # Update
            self.conn.update(worksheet=SHEET_NAME, data=df_final, spreadsheet=SPREADSHEET_NAME)
            self._cache.pop(SHEET_NAME, None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur append_achat_master : {e}")
            return False

    def delete_achats(self, purchase_ids: list) -> bool:
        """Deletes purchase lines from ACHAT_MASTER by ID."""
        if not self.conn:
            st.error("Suppression impossible en local.")
            return False
        try:
            df = self.conn.read(worksheet="ACHAT_MASTER", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            # Chercher la colonne ID (flexible : ID_Facture ou ID_Achat)
            id_col = 'ID_Facture' if 'ID_Facture' in df.columns else 'ID_Achat' if 'ID_Achat' in df.columns else None
            if not id_col:
                st.error("Colonne ID (ID_Facture / ID_Achat) introuvable.")
                return False
            
            # Normalisation stricte des deux côtés pour éviter les faux non-matches
            purchase_ids = [str(i).strip() for i in purchase_ids]
            df = df[~df[id_col].astype(str).str.strip().isin(purchase_ids)]
            
            self.conn.update(worksheet="ACHAT_MASTER", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("ACHAT_MASTER", None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur delete_achats : {e}")
            return False

    def update_achat(self, purchase_id: str, new_data_dict: dict) -> bool:
        """Updates a purchase line in ACHAT_MASTER."""
        if not self.conn:
            st.error("Mise à jour impossible en local.")
            return False
        try:
            df = self.conn.read(worksheet="ACHAT_MASTER", ttl=0, spreadsheet="MASTER_EXPLOITATION")
            # Chercher la colonne ID (flexible)
            id_col = 'ID_Facture' if 'ID_Facture' in df.columns else 'ID_Achat' if 'ID_Achat' in df.columns else None
            if not id_col:
                st.error("Colonne ID (ID_Facture / ID_Achat) introuvable.")
                return False
                
            # Normalisation stricte des deux côtés (.strip() évite les espaces parasites)
            mask = df[id_col].astype(str).str.strip() == str(purchase_id).strip()
            if not mask.any():
                st.warning(f"Ligne d'achat '{purchase_id}' non trouvée dans le Sheet.")
                return False
            
            idx = df[mask].index[0]
            for col, val in new_data_dict.items():
                if col in df.columns:
                    df.at[idx, col] = val
                    
            self.conn.update(worksheet="ACHAT_MASTER", data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop("ACHAT_MASTER", None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur update_achat : {e}")
            return False

    def get_etat_stocks(self, campaign):
        """Calcule l'état des stocks (Achats - Consommations) pour une campagne donnée."""
        df_achats = self.get_achats(campaign)
        df_interv = self.get_interventions()
        
        if df_achats.empty:
            return pd.DataFrame()
            
        # 1. Agrégation des Achats
        df_achats['Nom_Produit_Norm'] = df_achats['Nom_Produit'].astype(str).str.strip().str.upper()
        df_achats['Quantité_Achetée'] = pd.to_numeric(df_achats['Quantité_Achetée'], errors='coerce').fillna(0)
        df_achats['Montant_Total_Produit_HT'] = pd.to_numeric(df_achats['Montant_Total_Produit_HT'], errors='coerce').fillna(0)
        
        achats_agg = df_achats.groupby('Nom_Produit_Norm').agg({
            'Nom_Produit': 'first',
            'Catégorie': 'first',
            'Unité_Achat': 'first',
            'Quantité_Achetée': 'sum',
            'Montant_Total_Produit_HT': 'sum'
        }).reset_index()
        
        # 2. Agrégation des Consommations
        consos_agg = pd.DataFrame(columns=['Nom_Produit_Norm', 'Quantité_Consommée'])
        if not df_interv.empty and 'Nom_Produit' in df_interv.columns:
            df_interv['Campagne'] = pd.to_numeric(df_interv['Campagne'], errors='coerce').fillna(0).astype(int)
            df_interv = df_interv[df_interv['Campagne'] == int(campaign)]
            
            # Seulement les statuts Réalisés
            status_col = None
            for col in ['Stat_Intervention', 'Statut_Intervention', 'Statut', 'Etat']:
                if col in df_interv.columns:
                    status_col = col; break
                    
            if status_col:
                df_interv = df_interv[df_interv[status_col].astype(str).str.strip().str.lower().str.startswith('réal')]
                
            df_interv['Nom_Produit_Norm'] = df_interv['Nom_Produit'].astype(str).str.strip().str.upper()
            
            # Les quantités sont soit dans Quantité_Totale_Produit (Phyto/Engrais) ou Quantité_semence_totale (Semis)
            q_phyto = pd.to_numeric(df_interv.get('Quantité_Totale_Produit', 0), errors='coerce').fillna(0)
            q_semis = pd.to_numeric(df_interv.get('Quantité_semence_totale', 0), errors='coerce').fillna(0)
            df_interv['Quantité_Consommée'] = q_phyto + q_semis
            
            consos_agg = df_interv.groupby('Nom_Produit_Norm')['Quantité_Consommée'].sum().reset_index()
            
        # 2a. Agrégation des Consommations GNR (Fuel)
        df_fuel = self.get_fuel_conso(campaign)
        if not df_fuel.empty and 'FUEL_quantité_L' in df_fuel.columns:
            # Aggregate fuel consumption
            fuel_vol = pd.to_numeric(df_fuel['FUEL_quantité_L'], errors='coerce').fillna(0).sum()
            # In ACHAT_MASTER, fuel might be named "GNR", "Carburant", "Fioul" etc. 
            # We add it as "GNR" by default but should match user's ACHAT_MASTER naming.
            # Assuming the name in ACHAT_MASTER is "GNR" or similar, we will append it.
            # If the user buys fuel under specific names, it might need fuzzy matching, 
            # but usually it's categorized and named 'GNR'.
            
            # Find the exact product name for GNR in purchases if possible, or just use 'GNR'
            fuel_names = df_achats[df_achats['Catégorie'].astype(str).str.contains('GNR|Carburant|Fuel', case=False, na=False)]['Nom_Produit_Norm'].unique()
            
            for fuel_name in fuel_names:
                # Get purchase unit for this fuel to convert L to m3 if needed
                try:
                    unit = achats_agg.loc[achats_agg['Nom_Produit_Norm'] == fuel_name, 'Unité_Achat'].iloc[0]
                    if pd.notna(unit) and str(unit).strip().lower() in ['m3', 'm³']:
                        converted_vol = fuel_vol / 1000.0
                    else:
                        converted_vol = fuel_vol
                except Exception:
                    converted_vol = fuel_vol
                
                # If there are multiple fuel entries, just assign total conso to the first one for simplicity, 
                # or distribute. Usually there's only one.
                fuel_row = pd.DataFrame([{'Nom_Produit_Norm': fuel_name, 'Quantité_Consommée': converted_vol}])
                consos_agg = pd.concat([consos_agg, fuel_row], ignore_index=True)
                break # Just apply to the first found fuel purchase entry
                
        # 3. Fusion et calculs
        df_stock = pd.merge(achats_agg, consos_agg, on='Nom_Produit_Norm', how='left')
        df_stock['Quantité_Consommée'] = df_stock['Quantité_Consommée'].fillna(0)
        
        df_stock['Reste_en_Stock'] = df_stock['Quantité_Achetée'] - df_stock['Quantité_Consommée']
        
        # Calcul du prix moyen unitaire et de la valeur estimée (Prorata)
        df_stock['Prix_Moyen_Unitaire'] = 0.0
        df_stock['Valeur_Stock_Estimee'] = 0.0
        
        mask = df_stock['Quantité_Achetée'] > 0
        df_stock.loc[mask, 'Prix_Moyen_Unitaire'] = df_stock.loc[mask, 'Montant_Total_Produit_HT'] / df_stock.loc[mask, 'Quantité_Achetée']
        df_stock.loc[mask, 'Valeur_Stock_Estimee'] = df_stock.loc[mask, 'Prix_Moyen_Unitaire'] * df_stock.loc[mask, 'Reste_en_Stock']
        
        # Clean columns
        df_stock = df_stock.drop(columns=['Nom_Produit_Norm'])
        
        return df_stock

    # -----------------------------------------------------------------------
    # GESTION RECOLTE, STOCKAGE ET CONTRATS
    # -----------------------------------------------------------------------

    def get_recolte_stockage(self, campaign=None):
        df = self._get_data("RECOLTE_STOCKAGE")
        if df.empty: return pd.DataFrame()
        if campaign and 'Campagne' in df.columns:
            df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            df = df[df['Campagne'] == int(campaign)]
        return df

    def get_contrats_ventes(self, campaign=None):
        df = self._get_data("CONTRATS_VENTES")
        if df.empty: return pd.DataFrame()
        if campaign and 'Campagne' in df.columns:
            df['Campagne'] = pd.to_numeric(df['Campagne'], errors='coerce').fillna(0).astype(int)
            df = df[df['Campagne'] == int(campaign)]
        return df

    def get_silos(self):
        df = self._get_data("SILO")
        if df.empty: return pd.DataFrame()
        return df

    def update_recolte_stockage(self, id_mouvement: str, row_dict: dict) -> bool:
        return self._upsert_row_by_id("RECOLTE_STOCKAGE", "ID_Mouvement", id_mouvement, row_dict)
        
    def delete_recolte_stockage(self, ids: list) -> bool:
        return self._delete_rows_by_id("RECOLTE_STOCKAGE", "ID_Mouvement", ids)

    def update_contrats_ventes(self, id_contrat: str, row_dict: dict) -> bool:
        return self._upsert_row_by_id("CONTRATS_VENTES", "ID_contrat", id_contrat, row_dict)

    def delete_contrats_ventes(self, ids: list) -> bool:
        return self._delete_rows_by_id("CONTRATS_VENTES", "ID_contrat", ids)

    def _upsert_row_by_id(self, sheet_name: str, id_col: str, id_value: str, row_dict: dict) -> bool:
        if not self.conn:
            st.error("Mise à jour impossible en local.")
            return False
        try:
            df = self.conn.read(worksheet=sheet_name, ttl=0, spreadsheet="MASTER_EXPLOITATION")
            if id_col not in df.columns:
                 df[id_col] = ""

            for col in row_dict:
                if col not in df.columns:
                    df[col] = ""

            mask = df[id_col].astype(str).str.strip() == str(id_value).strip()
            new_row = pd.DataFrame([row_dict])

            if mask.any():
                idx = df[mask].index[0]
                for col in row_dict:
                    df.at[idx, col] = row_dict[col]
            else:
                df = pd.concat([df, new_row], ignore_index=True)

            self.conn.update(worksheet=sheet_name, data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop(sheet_name, None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur upsert {sheet_name} : {e}")
            return False

    def _delete_rows_by_id(self, sheet_name: str, id_col: str, ids: list) -> bool:
        if not self.conn:
            st.error("Suppression impossible en local.")
            return False
        try:
            df = self.conn.read(worksheet=sheet_name, ttl=0, spreadsheet="MASTER_EXPLOITATION")
            if id_col not in df.columns:
                 return False

            ids_str = [str(i).strip() for i in ids]
            original_len = len(df)
            df = df[~df[id_col].astype(str).str.strip().isin(ids_str)]

            if len(df) == original_len:
                 return False

            self.conn.update(worksheet=sheet_name, data=df, spreadsheet="MASTER_EXPLOITATION")
            self._cache.pop(sheet_name, None)
            st.cache_data.clear()
            return True
        except Exception as e:
            st.error(f"Erreur delete {sheet_name} : {e}")
            return False

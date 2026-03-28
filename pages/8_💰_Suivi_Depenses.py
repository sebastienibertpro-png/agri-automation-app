import streamlit as st
import pandas as pd
import plotly.express as px
from shared import init_campaign_selector, inject_premium_css, render_premium_table, render_premium_header

st.set_page_config(page_title="Suivi Dépenses", page_icon="💰", layout="wide")

# Injection du CSS premium
inject_premium_css()

st.title("💰 Suivi des Dépenses par Poste")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

render_premium_header(f"Dépenses pour la campagne {selected_campaign}", "Suivi analytique par poste de dépense 💰", color="blue")

# Fetch the data
df_achats = active_loader.get_achats(selected_campaign)

if df_achats.empty:
    st.info(f"Aucune ligne d'achat trouvée pour la campagne {selected_campaign}.")
else:
    # Ensure necessary columns exist. Fallback if slightly misnamed.
    cat_col = 'Catégorie' if 'Catégorie' in df_achats.columns else 'Categorie' if 'Categorie' in df_achats.columns else None
    mnt_col = 'Montant_Total_Produit_HT' if 'Montant_Total_Produit_HT' in df_achats.columns else None
    fourn_col = 'Fournisseur' if 'Fournisseur' in df_achats.columns else None

    # Handle potentially missing columns gracefully
    missing_cols = []
    if not cat_col: missing_cols.append("Catégorie")
    if not mnt_col: missing_cols.append("Montant_Total_Produit_HT")
    
    if missing_cols:
         st.error(f"Fichier de données mal formé. Colonnes introuvables : {', '.join(missing_cols)}")
         st.write("Colonnes disponibles:", df_achats.columns.tolist())
    else:
        # Data preparation
        # Convert amount to numeric just in case
        df_achats[mnt_col] = pd.to_numeric(df_achats[mnt_col], errors='coerce').fillna(0.0)

        # Normalisation des catégories (fusionner "Matériel" et "MATERIEL")
        def clean_category(val):
            if pd.isna(val) or str(val).strip() == "": return "INCONNU"
            s = str(val).strip().upper()
            s = s.replace("É", "E").replace("È", "E").replace("Ê", "E").replace("Ë", "E")
            s = s.replace("À", "A").replace("Â", "A").replace("Ä", "A")
            s = s.replace("Î", "I").replace("Ï", "I")
            s = s.replace("Ô", "O").replace("Ö", "O")
            s = s.replace("Û", "U").replace("Ü", "U")
            s = s.replace("Ç", "C")
            return s
            
        df_achats[cat_col] = df_achats[cat_col].apply(clean_category)

        # Filters
        st.sidebar.markdown("### 🔍 Filtres Dépenses")
        
        # Determine available suppliers for the filter
        available_fournisseurs = []
        if fourn_col:
            available_fournisseurs = [f for f in df_achats[fourn_col].dropna().unique() if str(f).strip() != ""]
            available_fournisseurs.insert(0, "Tous")
            
            selected_fournisseur = st.sidebar.selectbox("Fournisseur", available_fournisseurs)
            
            if selected_fournisseur != "Tous":
                 df_achats = df_achats[df_achats[fourn_col] == selected_fournisseur]
        
        # Calculate summary by category
        df_summary = df_achats.groupby(cat_col)[mnt_col].sum().reset_index()
        df_summary = df_summary.sort_values(by=mnt_col, ascending=False)
        df_summary.columns = ['Catégorie', 'Montant Total HT (€)']

        total_depenses = df_summary['Montant Total HT (€)'].sum()

        # Display Top Metrics
        st.metric("Total des Dépenses HT", f"{total_depenses:,.2f} €".replace(',', ' '))
        
        st.divider()
        
        col1, col2 = st.columns([1, 1])

        with col1:
            render_premium_header("📑 Récapitulatif par Catégorie", color="blue")
            df_recap = df_summary.copy()
            df_recap['Montant Total HT (€)'] = df_recap['Montant Total HT (€)'].apply(lambda x: f"{float(x):,.2f} €".replace(',', ' '))
            render_premium_table(df_recap, color="blue")

        with col2:
            st.subheader("📊 Répartition Graphique")
            if not df_summary.empty and total_depenses > 0:
                fig = px.pie(
                    df_summary, 
                    values='Montant Total HT (€)', 
                    names='Catégorie', 
                    title="Répartition des dépenses HT",
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Montant total à 0, impossible d'afficher le graphique.")

        st.divider()

        # --- GESTION DES FACTURES DÉTAILLÉES ---
        st.write("") # Espace avant la section
        render_premium_header("📑 Gestion de vos Factures", "Consultez, ajoutez, modifiez ou supprimez vos données dans ce tableau interactif", color="blue")
        
        df_manage = df_achats.copy()
        
        # Determine ID column name (flexible for ID_Facture or ID_Achat)
        id_col = 'ID_Facture' if 'ID_Facture' in df_manage.columns else 'ID_Achat' if 'ID_Achat' in df_manage.columns else None
        
        if not id_col or id_col not in df_manage.columns:
            st.error(f"Données incomplètes : Colonne ID (Facture) manquante. Colonnes dispos: {df_manage.columns.tolist()}")
        else:
            # 1. Cleaning formatting
            cols_to_drop_keywords = ['id_parcelle_li', 'affectation_type']
            hidden_cols = [id_col, 'Montant'] # drop only standalone 'Montant'
            for c in df_manage.columns.tolist():
                c_lower = c.lower().replace('é', 'e').replace('è', 'e')
                if any(kw in c_lower for kw in cols_to_drop_keywords):
                    hidden_cols.append(c)

            # Ensure data is strictly string to enable unrestricted Streamlit editing
            df_manage = df_manage.astype(str).replace({'nan': '', 'None': '', '<NA>': '', 'NaT': ''})
            
            # 2. Configure aesthetic columns 
            col_conf = {c: None for c in hidden_cols if c in df_manage.columns}
            
            if 'Date_facture' in df_manage.columns:
                col_conf['Date_facture'] = st.column_config.TextColumn("Date")
            if 'Nom_Produit' in df_manage.columns:
                col_conf['Nom_Produit'] = st.column_config.TextColumn("Produit")
            if 'Quantité_Achetée' in df_manage.columns:
                col_conf['Quantité_Achetée'] = st.column_config.TextColumn("Quantité")
            if 'Montant_Total_Produit_HT' in df_manage.columns:
                col_conf['Montant_Total_Produit_HT'] = st.column_config.TextColumn("Total Produit HT")
            if 'Montant_Total_Facture_HT' in df_manage.columns:
                col_conf['Montant_Total_Facture_HT'] = st.column_config.TextColumn("Total Facture HT")
            if 'Montant_Total_Facture_TTC' in df_manage.columns:
                col_conf['Montant_Total_Facture_TTC'] = st.column_config.TextColumn("Total Facture TTC")
            
            link_col = 'Lien_facture' if 'Lien_facture' in df_manage.columns else 'Lien_Facture_Drive' if 'Lien_Facture_Drive' in df_manage.columns else 'Lien_Drive' if 'Lien_Drive' in df_manage.columns else None
            if link_col:
                col_conf[link_col] = st.column_config.LinkColumn("Lien Facture", display_text="Lien Drive")

            # 3. Interactive Component
            edited_df = st.data_editor(
                df_manage,
                column_config=col_conf,
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                key="editor_factures"
            )
            
            # 4. Persistence Logic
            if st.button("💾 Sauvegarder les modifications", type="primary"):
                import uuid
                
                # Separate rows by their ID to track modifications
                df_orig = df_manage.set_index(id_col)
                df_new = edited_df.dropna(subset=[id_col]).copy()
                
                # Additions (rows generated by num_rows="dynamic" have no ID or empty string)
                added_mask = (edited_df[id_col].isna()) | (edited_df[id_col] == "")
                added_rows = edited_df[added_mask].copy()
                edited_existing = edited_df[~added_mask].copy()
                
                df_new_indexed = edited_existing.set_index(id_col)
                
                original_ids = set(df_orig.index.tolist())
                current_ids = set(df_new_indexed.index.tolist())
                
                # Deletions (IDs from original missing in current)
                deleted_ids = list(original_ids - current_ids)
                
                # Updates (IDs present in both, with differing content)
                common_ids = original_ids.intersection(current_ids)
                updated_ids = []
                for cid in common_ids:
                    raw_old = df_orig.loc[cid]
                    row_old = (raw_old.iloc[0] if isinstance(raw_old, pd.DataFrame) else raw_old).astype(str).str.strip().fillna('')
                    raw_new = df_new_indexed.loc[cid]
                    row_new = (raw_new.iloc[0] if isinstance(raw_new, pd.DataFrame) else raw_new).astype(str).str.strip().fillna('')
                    if not row_old.equals(row_new):
                        updated_ids.append(cid)

                success_count = 0
                error_occurred = False
                
                # We assume the name is ACHAT_MASTER or MASTER_ACHAT. 
                # update_achat and delete_achats are directly connected onto the active_loader wrapper.
                with st.spinner("Application des modifications..."):
                    # Delete
                    if deleted_ids:
                        if active_loader.delete_achats(deleted_ids):
                            success_count += len(deleted_ids)
                        else:
                            error_occurred = True
                            
                    # Add
                    if not added_rows.empty:
                        sheet_name = "ACHAT_MASTER"
                        for _, row in added_rows.iterrows():
                            new_dict = row.to_dict()
                            new_id = str(uuid.uuid4())[:8].upper()
                            new_dict[id_col] = new_id
                            new_dict["Campagne"] = selected_campaign
                            if active_loader.insert_row(sheet_name, new_dict):
                                success_count += 1
                            else:
                                error_occurred = True
                            
                    # Update
                    for uid in updated_ids:
                        up_dict = df_new_indexed.loc[uid].to_dict()
                        up_dict[id_col] = uid  # Inject index back
                        if active_loader.update_achat(uid, up_dict):
                            success_count += 1
                        else:
                            error_occurred = True
                
                if success_count > 0 and not error_occurred:
                    st.success(f"✅ Terminé ! {success_count} action(s) (ajout/modification/suppression) sauvegardée(s).")
                    st.rerun()
                elif success_count > 0 and error_occurred:
                    st.warning(f"⚠️ Partiellement sauvegardé ({success_count} réussites), mais certaines opérations ont échoué.")
                elif error_occurred:
                    st.error("❌ Échec lors de la sauvegarde (vérifiez les autorisations/logs).")
                else:
                    st.info("Aucune modification par rapport à la base.")


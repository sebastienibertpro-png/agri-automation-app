import streamlit as st
import pandas as pd
import plotly.express as px
from shared import init_campaign_selector, inject_premium_css, render_premium_table, render_premium_header

st.set_page_config(page_title="Suivi Dépenses", page_icon="💰", layout="wide")

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
        st.header("📑 Gestion des Factures (Détails)")
        
        # Display selection for deletion
        df_manage = df_achats.copy()
        
        # Determine ID column name (flexible for ID_Facture or ID_Achat)
        id_col = 'ID_Facture' if 'ID_Facture' in df_manage.columns else 'ID_Achat' if 'ID_Achat' in df_manage.columns else None
        
        if not id_col or id_col not in df_manage.columns:
            st.error(f"Données incomplètes : Colonne ID (Facture) manquante. Colonnes dispos: {df_manage.columns.tolist()}")
        else:
            # Mode toggle
            manage_mode = st.radio("🛠️ Actions", ["👁️ Visualisation & Liens", "📝 Mode Édition", "🗑️ Sélection & Suppression"], horizontal=True)

            if manage_mode == "👁️ Visualisation & Liens":
                df_viz = df_manage.copy()
                if 'Date_facture' in df_viz.columns:
                    # Safe conversion to datetime
                    df_viz['Date_facture'] = pd.to_datetime(df_viz['Date_facture'], errors='coerce', dayfirst=True)
                    # Format while handling NaT
                    df_viz['Date_facture'] = df_viz['Date_facture'].dt.strftime('%d/%m/%Y').fillna("")
                
                # Render Drive Links
                link_col = 'Lien_facture' if 'Lien_facture' in df_viz.columns else 'Lien_Facture_Drive' if 'Lien_Facture_Drive' in df_viz.columns else 'Lien_Drive' if 'Lien_Drive' in df_viz.columns else None
                if link_col:
                    def make_link(url):
                        if pd.isna(url) or str(url).strip() == "": return ""
                        return f'<a href="{url}" target="_blank">📄 Voir</a>'
                    df_viz['Lien'] = df_viz[link_col].apply(make_link)
                    cols = ['Lien'] + [c for c in df_viz.columns if c not in ['Lien', link_col, id_col]]
                    df_viz = df_viz[cols]
                
                render_premium_table(df_viz, color="blue")
                
            elif manage_mode == "📝 Mode Édition":
                st.info("💡 Modifiez les valeurs directement dans le tableau ci-dessous, puis cliquez sur Sauvegarder.")
                edited_df = st.data_editor(
                    df_manage,
                    column_config={id_col: None, "Lien_facture": st.column_config.TextColumn("Lien Drive")},
                    hide_index=True,
                    use_container_width=True,
                    key="edit_editor"
                )
                
                if st.button("💾 Sauvegarder les modifications"):
                    diff_mask = (edited_df != df_manage).any(axis=1)
                    changed_rows = edited_df[diff_mask]
                    
                    if not changed_rows.empty:
                        with st.spinner("Mise à jour des factures..."):
                            success_count = 0
                            for _, row in changed_rows.iterrows():
                                if active_loader.update_achat(row[id_col], row.to_dict()):
                                    success_count += 1
                            if success_count > 0:
                                st.success(f"✅ {success_count} facture(s) mise(s) à jour !")
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de la mise à jour.")
                    else:
                        st.info("Aucun changement détecté.")

            else:
                df_delete = df_manage.copy()
                df_delete.insert(0, "Sélect. ✅", False)
                edited_df = st.data_editor(
                    df_delete,
                    column_config={
                        id_col: None,
                        "Sélect. ✅": st.column_config.CheckboxColumn("Sélect.", default=False)
                    },
                    disabled=[c for c in df_delete.columns if c != "Sélect. ✅"],
                    hide_index=True,
                    use_container_width=True,
                    key="delete_editor"
                )
                
                rows_to_delete = edited_df[edited_df["Sélect. ✅"] == True]
                if not rows_to_delete.empty:
                    st.warning(f"⚠️ {len(rows_to_delete)} ligne(s) sélectionnée(s) pour suppression.")
                    if st.button("🗑️ Confirmer la suppression", type="primary"):
                        ids = rows_to_delete[id_col].tolist()
                        if active_loader.delete_achats(ids):
                            st.success("✅ Factures supprimées !")
                            st.rerun()
                        else:
                            st.error("❌ Échec de la suppression.")


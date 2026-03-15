import streamlit as st
import pandas as pd
import plotly.express as px
from shared import init_campaign_selector

st.set_page_config(page_title="Suivi Dépenses", page_icon="💰", layout="wide")

st.title("💰 Suivi des Dépenses par Poste")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

st.markdown(f"### Dépenses pour la campagne {selected_campaign}")

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
            st.subheader("📑 Récapitulatif par Catégorie")
            st.dataframe(
                df_summary.style.format({"Montant Total HT (€)": "{:,.2f} €"}),
                use_container_width=True,
                hide_index=True
            )

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
        if 'ID_Achat' not in df_manage.columns:
            st.error("Données incomplètes : Colonne 'ID_Achat' manquante.")
        else:
            # Mode toggle
            manage_mode = st.radio("🛠️ Actions", ["👁️ Visualisation & Liens", "📝 Mode Édition", "🗑️ Sélection & Suppression"], horizontal=True)

            if manage_mode == "👁️ Visualisation & Liens":
                # ... (Visualisation logic remains same)
                df_viz = df_manage.copy()
                if 'Date_facture' in df_viz.columns:
                    df_viz['Date_facture'] = pd.to_datetime(df_viz['Date_facture']).dt.strftime('%d/%m/%Y')
                link_col = 'Lien_Facture_Drive' if 'Lien_Facture_Drive' in df_viz.columns else 'Lien_Drive' if 'Lien_Drive' in df_viz.columns else None
                if link_col:
                    def make_link(url):
                        if pd.isna(url) or str(url).strip() == "": return ""
                        return f'<a href="{url}" target="_blank">📄 Voir</a>'
                    df_viz['Lien'] = df_viz[link_col].apply(make_link)
                    cols = ['Lien'] + [c for c in df_viz.columns if c not in ['Lien', link_col, 'ID_Achat']]
                    df_viz = df_viz[cols]
                
                st.markdown("""
                <style>
                    .invoice-table { border-collapse: collapse; font-size: 0.85em; width: 100%; border-radius: 8px; overflow: hidden; }
                    .invoice-table thead tr { background-color: #2E7D32; color: white; text-align: center; }
                    .invoice-table th, .invoice-table td { padding: 8px 12px; border: 1px solid #eee; text-align: center; }
                    .invoice-table tbody tr:nth-of-type(even) { background-color: #f8f9fa; }
                    .invoice-table tbody tr:hover { background-color: #f1f8e9; }
                    .invoice-table a { color: #2E7D32; font-weight: bold; text-decoration: none; }
                </style>
                """, unsafe_allow_html=True)
                st.write(df_viz.to_html(escape=False, index=False, classes="invoice-table"), unsafe_allow_html=True)
                
            elif manage_mode == "📝 Mode Édition":
                st.info("💡 Modifiez les valeurs directement dans le tableau ci-dessous, puis cliquez sur Sauvegarder.")
                edited_df = st.data_editor(
                    df_manage,
                    column_config={"ID_Achat": None, "Lien_Facture_Drive": st.column_config.TextColumn("Lien Drive")},
                    hide_index=True,
                    use_container_width=True,
                    key="edit_editor"
                )
                
                if st.button("💾 Sauvegarder les modifications"):
                    # Find changed rows
                    # For simplicity in GSheets sync, we can just compare DFs or update row by row
                    diff_mask = (edited_df != df_manage).any(axis=1)
                    changed_rows = edited_df[diff_mask]
                    
                    if not changed_rows.empty:
                        with st.spinner("Mise à jour des factures..."):
                            success_count = 0
                            for _, row in changed_rows.iterrows():
                                if active_loader.update_achat(row['ID_Achat'], row.to_dict()):
                                    success_count += 1
                            
                            if success_count > 0:
                                st.success(f"✅ {success_count} facture(s) mise(s) à jour !")
                                st.rerun()
                            else:
                                st.error("❌ Erreur lors de la mise à jour.")
                    else:
                        st.info("Aucun changement détecté.")

            else:
                # Selectable table for deletion
                df_delete = df_manage.copy()
                df_delete.insert(0, "Sélect. ✅", False)
                edited_df = st.data_editor(
                    df_delete,
                    column_config={
                        "ID_Achat": None,
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
                        ids = rows_to_delete['ID_Achat'].tolist()
                        if active_loader.delete_achats(ids):
                            st.success("✅ Factures supprimées !")
                            st.rerun()
                        else:
                            st.error("❌ Échec de la suppression.")


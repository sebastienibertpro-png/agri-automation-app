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


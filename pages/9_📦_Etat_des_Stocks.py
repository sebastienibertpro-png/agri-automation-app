import streamlit as st
import pandas as pd
from shared import init_campaign_selector, inject_premium_css, render_premium_table
import io

st.set_page_config(page_title="État des Stocks", page_icon="📦", layout="wide")

st.title("📦 État des Stocks")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

render_premium_header(f"Stocks pour la campagne {selected_campaign}", "Situation consolidée des achats et consommations 📦", color="green")

# Fetch the stock data
df_stocks = active_loader.get_etat_stocks(selected_campaign)

if df_stocks is None or df_stocks.empty:
    st.info(f"Aucune donnée d'achat trouvée pour la campagne {selected_campaign}.")
else:
    cat_col = 'Catégorie' if 'Catégorie' in df_stocks.columns else 'Categorie'
    
    def get_df_for_category(keyword):
        if cat_col not in df_stocks.columns:
            return pd.DataFrame()
        # Case insensitive match
        mask = df_stocks[cat_col].astype(str).str.contains(keyword, case=False, na=False)
        return df_stocks[mask]

    # Calculate Top KPI only for relevant categories
    df_semences = get_df_for_category('Semence')
    df_engrais = get_df_for_category('Engrais')
    df_phyto = get_df_for_category('Phyto')
    df_gnr = get_df_for_category('GNR|Carburant|Fuel')
    
    total_valeur = 0
    for df_sub in [df_semences, df_engrais, df_phyto, df_gnr]:
        if not df_sub.empty:
            total_valeur += df_sub['Valeur_Stock_Estimee'].sum()
            
    st.metric("Valeur Globale Estimée du Stock Restant", f"{total_valeur:,.2f} €".replace(',', ' '))
    st.divider()

    # Define Categories based on common groupings if available
    # The prompt categorizes loosely into Semences, Engrais, Phytos
    
    
    # Let's map keywords to 3 main tabs, plus 'GNR'
    tab_semences, tab_engrais, tab_phyto, tab_gnr = st.tabs(["🌾 Semences", "🧪 Engrais", "🛡️ Phytosanitaires", "⛽ GNR (Carburant)"])
    
    def get_df_for_category(keyword):
        if cat_col not in df_stocks.columns:
            return pd.DataFrame()
        # Case insensitive match
        mask = df_stocks[cat_col].astype(str).str.contains(keyword, case=False, na=False)
        return df_stocks[mask]
        
    def render_table(df_subset):
        if df_subset.empty:
            st.info("Aucun stock dans cette catégorie.")
            return
            
        df_disp = df_subset[['Nom_Produit', 'Prix_Moyen_Unitaire', 'Quantité_Achetée', 'Quantité_Consommée', 'Reste_en_Stock', 'Unité_Achat', 'Valeur_Stock_Estimee']].copy()
        df_disp.columns = ['Produit', 'Prix (€)', 'Acheté', 'Consommé', 'Reste', 'Unité', 'Valeur (€)']
        
        # Sort by Value
        df_disp = df_disp.sort_values(by="Valeur (€)", ascending=False)
        total_valeur_cat = df_disp['Valeur (€)'].sum()
        
        # Formatting for HTML display
        df_disp['Prix (€)'] = df_disp['Prix (€)'].apply(lambda x: f"{float(x):.2f} €" if pd.notna(x) else "")
        df_disp['Acheté'] = df_disp['Acheté'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "")
        df_disp['Consommé'] = df_disp['Consommé'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "")
        df_disp['Reste'] = df_disp['Reste'].apply(lambda x: f"<b>{float(x):.2f}</b>" if pd.notna(x) and float(x) < 0 else f"{float(x):.2f}" if pd.notna(x) else "")
        df_disp['Valeur (€)'] = df_disp['Valeur (€)'].apply(lambda x: f"{float(x):,.2f} €".replace(',', ' ') if pd.notna(x) else "")
        
        # Add total row
        total_row = pd.DataFrame([['<b>TOTAL</b>', '', '', '', '', '', f"<b>{total_valeur_cat:,.2f} €".replace(',', ' ') + "</b>"]], columns=df_disp.columns)
        df_disp = pd.concat([df_disp, total_row], ignore_index=True)
        
        render_premium_table(df_disp, color="green")

    # Note: Using generic keywords 'SEMENCE', 'ENGRAIS', 'PHYTO' to catch categories.
    with tab_semences:
        render_table(get_df_for_category('Semence'))
    with tab_engrais:
         render_table(get_df_for_category('Engrais'))
    with tab_phyto:
         render_table(get_df_for_category('Phyto'))
    with tab_gnr:
         render_table(get_df_for_category('GNR|Carburant|Fuel'))
    st.divider()
    
    st.markdown("### 📥 Exporter un Rapport de Stocks")
    
    if st.button("Générer PDF 📄"):
        with st.spinner("Génération du rapport en cours..."):
            from report_gen import ReportGenerator
            import os
            
            # Ensure output dir
            out_dir = "output"
            if not os.path.exists(out_dir): os.makedirs(out_dir)
                
            filename = os.path.join(out_dir, f"Etat_Stocks_{selected_campaign}.pdf")
            
            # Create PDF
            rg = ReportGenerator(filename)
            rg.generate_etat_stocks_report(selected_campaign, df_stocks)
            
            # Provide Download
            if os.path.exists(filename):
                with open(filename, "rb") as pdf_file:
                    st.download_button(
                        label="⬇️ Télécharger le rapport de stock",
                        data=pdf_file,
                        file_name=f"Etat_Stocks_{selected_campaign}.pdf",
                        mime="application/pdf"
                    )
                st.success("Rapport généré avec succès!")
            else:
                st.error("Erreur lors de la génération du fichier.")

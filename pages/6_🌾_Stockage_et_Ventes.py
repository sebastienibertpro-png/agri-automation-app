import streamlit as st
import pandas as pd
import uuid
import time
from shared import init_campaign_selector

st.set_page_config(page_title="Gestion du stockage et des ventes", page_icon="🌾", layout="wide")

st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        text-align: center;
        margin-bottom: 20px;
        border-left: 5px solid #4CAF50;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #2E7D32;
    }
    .metric-label {
        font-size: 1.1rem;
        color: #6c757d;
        text-transform: uppercase;
        margin-bottom: 10px;
    }
    .metric-card-orange {
        border-left: 5px solid #ff9800;
    }
    .metric-card-orange .metric-value { color: #f57c00; }
    
    .metric-card-blue {
        border-left: 5px solid #2196F3;
    }
    .metric-card-blue .metric-value { color: #1976D2; }
</style>
""", unsafe_allow_html=True)

st.title("🌾 Gestion du stockage et des ventes")

st.markdown("Suivi unifié de vos récoltes, du remplissage de vos cellules et de l'exécution de vos contrats de vente pour la campagne sélectionnée.")

active_loader, selected_campaign, _, _ = init_campaign_selector()

if not active_loader:
    st.stop()

# --- CHARGEMENT DES DONNÉES ---
df_recolte = active_loader.get_recolte_stockage(selected_campaign)
df_contrats = active_loader.get_contrats_ventes(selected_campaign)
df_silos_ref = active_loader.get_silos()

silos_list = df_silos_ref['Lieu_Stockage'].dropna().unique().tolist() if not df_silos_ref.empty and 'Lieu_Stockage' in df_silos_ref.columns else []

if not silos_list:
    # Faute de référence SILO, on récupère les cellules existantes
    if not df_recolte.empty and 'Lieu_Stockage' in df_recolte.columns:
        silos_list = df_recolte['Lieu_Stockage'].dropna().unique().tolist()
    if not silos_list:
         silos_list = ["Cellule Principale", "Silo Extérieur"]

produits_list = ["Maïs", "Blé", "Orge", "Tournesol", "Soja", "Colza"]
if not df_recolte.empty and 'Produit' in df_recolte.columns:
    prods_used = df_recolte['Produit'].dropna().unique().tolist()
    produits_list = list(set(produits_list + prods_used))

contrats_ids = df_contrats['ID_contrat'].dropna().unique().tolist() if not df_contrats.empty and 'ID_contrat' in df_contrats.columns else []

# --- CALCULS PRELIMINAIRES ---
stock_par_silo = []
reliquat_silo = {}
livraison_contrat = {}
df_sorties = pd.DataFrame()

if not df_recolte.empty:
    df_recolte['Quantite_T'] = pd.to_numeric(df_recolte.get('Quantite_T', 0), errors='coerce').fillna(0)
    
    # 1. Calculs par Silo
    for silo in df_recolte['Lieu_Stockage'].dropna().unique():
        df_silo = df_recolte[df_recolte['Lieu_Stockage'] == silo]
        entrees = df_silo[df_silo['Type_Mouvement'] == 'Entrée']['Quantite_T'].sum()
        sorties = df_silo[df_silo['Type_Mouvement'] == 'Sortie']['Quantite_T'].sum()
        stock = entrees - sorties
        
        prods_list_silo = df_silo['Produit'].dropna().unique().tolist()
        prods_str = ", ".join(prods_list_silo)
        
        reliquat_silo[silo] = stock
        stock_par_silo.append({
            "Lieu de Stockage": silo,
            "Produits Stockés": prods_str,
            "Total Entrées (T)": entrees,
            "Total Sorties (T)": sorties,
            "Stock Actuel (T)": stock
        })
        
    # 2. Calculs Livraison par Contrat
    if 'ID_Contrat' in df_recolte.columns:
        df_sorties = df_recolte[df_recolte['Type_Mouvement'] == 'Sortie']
        for idx, row in df_sorties.iterrows():
            cid = row['ID_Contrat']
            if pd.notna(cid) and str(cid).strip() != "":
                livraison_contrat[cid] = livraison_contrat.get(cid, 0) + row['Quantite_T']

df_stock_silo = pd.DataFrame(stock_par_silo)


# --- TABS ---
tab1, tab2, tab3, tab4 = st.tabs([
    "🏢 Synthèse Silos", 
    "🔄 Mouvements de Stock", 
    "📜 Contrats de Vente", 
    "💰 Tableau de Trésorerie"
])

with tab1:
    st.header("État Actuel des Stocks (en Tonnes)")
    
    if not df_stock_silo.empty:
        tot_in = df_stock_silo["Total Entrées (T)"].sum()
        tot_out = df_stock_silo["Total Sorties (T)"].sum()
        tot_stock = df_stock_silo["Stock Actuel (T)"].sum()
        
        # Affichage métriques globales
        col1, col2, col3 = st.columns(3)
        col1.markdown(f'<div class="metric-card metric-card-blue"><div class="metric-label">Total Entré / Récolté</div><div class="metric-value">{tot_in:,.1f} T</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card metric-card-orange"><div class="metric-label">Total Expédié / Vendu</div><div class="metric-value">{tot_out:,.1f} T</div></div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="metric-card"><div class="metric-label">Stock Réel Restant</div><div class="metric-value">{tot_stock:,.1f} T</div></div>', unsafe_allow_html=True)
        
        st.subheader("Détail du Reliquat par Cellule")
        
        df_format_silo = df_stock_silo.copy()
        for col in ["Total Entrées (T)", "Total Sorties (T)", "Stock Actuel (T)"]:
             df_format_silo[col] = df_format_silo[col].apply(lambda x: f"{x:.2f} T")
        
        from shared import render_premium_table
        render_premium_table(df_format_silo, color="blue")
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.divider()
        st.subheader("🔍 Zoom sur l'historique d'une cellule")
        
        sel_silo_zoom = st.selectbox("Sélectionnez une cellule pour voir ses mouvements récents :", ["- Choisir -"] + silos_list, key="silo_zoom_explorer")
        
        if sel_silo_zoom != "- Choisir -":
            if not df_recolte.empty:
                df_zoom = df_recolte[df_recolte['Lieu_Stockage'] == sel_silo_zoom].copy()
                if not df_zoom.empty:
                    # Formattage pour le tableau premium
                    df_zoom_disp = df_zoom[['Date', 'Type_Mouvement', 'Produit', 'Quantite_T', 'Humidite_Moyenne', 'PS_Moyen']].copy()
                    df_zoom_disp = df_zoom_disp.rename(columns={
                        'Quantite_T': 'Quantité (T)',
                        'Humidite_Moyenne': 'Humidité',
                        'PS_Moyen': 'PS'
                    })
                    # Remplacer les None/nan par vide
                    df_zoom_disp = df_zoom_disp.fillna("")
                    
                    st.info(f"Mouvements enregistrés pour : **{sel_silo_zoom}**")
                    render_premium_table(df_zoom_disp, color="blue", compact=True)
                else:
                    st.info(f"Aucun mouvement trouvé pour {sel_silo_zoom}.")
            else:
                st.info("Aucune donnée disponible.")

        st.markdown("<br>", unsafe_allow_html=True)
        
        import plotly.express as px
        
        col_graph1, col_graph2 = st.columns(2)
        
        with col_graph1:
             fig_bar = px.bar(
                 df_stock_silo, 
                 x="Lieu de Stockage", 
                 y="Stock Actuel (T)", 
                 color="Lieu de Stockage",
                 text_auto='.1f',
                 hover_data=["Produits Stockés", "Total Entrées (T)"],
                 title="📊 Volume Stocké par Cellule"
             )
             fig_bar.update_layout(
                 plot_bgcolor="rgba(0,0,0,0)",
                 paper_bgcolor="rgba(0,0,0,0)",
                 showlegend=False,
                 title_font=dict(size=18, family="Segoe UI", color="#1E88E5"),
                 xaxis=dict(showgrid=False, title=""),
                 yaxis=dict(showgrid=True, gridcolor="#f0f0f0", title="Tonnes (T)")
             )
             fig_bar.update_traces(
                 marker_line_color='black', 
                 marker_line_width=1, 
                 opacity=0.85, 
                 textposition="auto", 
                 textfont_size=14, 
                 textfont_color="white"
             )
             st.plotly_chart(fig_bar, use_container_width=True)
             
        with col_graph2:
             fig_pie = px.pie(
                 df_stock_silo, 
                 values="Stock Actuel (T)", 
                 names="Produits Stockés",
                 hole=0.4,
                 title="🌾 Répartition par Produit"
             )
             fig_pie.update_layout(
                 plot_bgcolor="rgba(0,0,0,0)",
                 paper_bgcolor="rgba(0,0,0,0)",
                 title_font=dict(size=18, family="Segoe UI", color="#4CAF50"),
                 legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
             )
             fig_pie.update_traces(textposition='inside', textinfo='percent+label', marker=dict(line=dict(color='#000000', width=1)))
             st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Aucun mouvement enregistré pour vérifier l'état des silos sur cette campagne.")


with tab2:
    st.header("Saisie des Mouvements")
    
    with st.expander("📝 Nouvelle Entrée ou Sortie", expanded=False):
        with st.form("form_mouvement"):
            m_col1, m_col2, m_col3 = st.columns(3)
            date_m = m_col1.date_input("Date")
            type_m = m_col2.radio("Type de Mouvement", ["Entrée", "Sortie"], horizontal=True)
            produit_m = m_col3.selectbox("Produit", produits_list)
            
            m_col4, m_col5, m_col6 = st.columns(3)
            silo_m = m_col4.selectbox("Affecter à la cellule / silo", silos_list)
            qte_m = m_col5.number_input("Tonnage (T)", min_value=0.0, step=0.1)
            var_m = m_col6.text_input("Variété (Facultatif)")
            
            m_col7, m_col8, m_col9 = st.columns(3)
            hum_m = m_col7.number_input("Humidité Moyenne (%)", min_value=0.0, step=0.1)
            ps_m = m_col8.number_input("Poids Spécifique Moyen (PS)", min_value=0.0, step=0.1)
            
            st.markdown("##### Informations Complémentaires")
            bon_m = st.text_input("Lien Google Drive du Bon d'Enlèvement / Ticket de Pesée")
            
            contrat_m = ""
            if type_m == "Sortie":
                 contrat_opts = [""] + contrats_ids
                 contrat_m = st.selectbox("Lier cette sortie à un Contrat de Vente (Optionnel)", contrat_opts)
            
            submitted_m = st.form_submit_button("Enregistrer Mouvement", type="primary")
            if submitted_m:
                if qte_m > 0:
                    new_id = f"MVT_{uuid.uuid4().hex[:8].upper()}"
                    row_data = {
                        "ID_Mouvement": new_id,
                        "Date": date_m.strftime("%Y-%m-%d"),
                        "Campagne": selected_campaign,
                        "Type_Mouvement": type_m,
                        "Produit": produit_m,
                        "Variété": var_m,
                        "Lieu_Stockage": silo_m,
                        "Quantite_T": qte_m,
                        "Humidite_Moyenne": hum_m if hum_m > 0 else "",
                        "PS_Moyen": ps_m if ps_m > 0 else "",
                        "Bon_Enlèvement": bon_m,
                        "ID_Contrat": contrat_m
                    }
                    if active_loader.update_recolte_stockage(new_id, row_data):
                        st.success("Mouvement validé avec succès !")
                        time.sleep(1)
                        st.rerun()
                else:
                    st.error("❌ Tonnage invalide. Renseignez une quantité > 0.")

    st.subheader("Registres des Opérations")
    if not df_recolte.empty:
        # Trier du plus récent au plus ancien
        try:
             df_recolte['Date_sort'] = pd.to_datetime(df_recolte['Date'], dayfirst=True)
             df_recolte = df_recolte.sort_values('Date_sort', ascending=False)
             df_recolte = df_recolte.drop(columns=['Date_sort'])
        except:
             pass
             
        df_disp = df_recolte.drop(columns=['Campagne']) if 'Campagne' in df_recolte.columns else df_recolte
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        del_m_id = st.selectbox("Sélectionnez un identifiant de mouvement à supprimer", df_recolte['ID_Mouvement'].tolist())
        if st.button("🗑️ Supprimer ce mouvement", type="secondary"):
            if active_loader.delete_recolte_stockage([del_m_id]):
                 st.success("Supprimé !")
                 time.sleep(1)
                 st.rerun()
    else:
        st.info("Historique vide. Commencez à saisir vos entrées et sorties.")

with tab3:
    st.header("Gestion des Engagements (Contrats de Vente)")
    
    with st.expander("📝 Nouvel Engagement", expanded=False):
        with st.form("form_contrat"):
             c1, c2, c3 = st.columns(3)
             acht_val = c1.text_input("Acheteur / Organisme")
             prod_val = c2.selectbox("Produit concerné", produits_list)
             qte_val = c3.number_input("Tonnage Engagé (T)", min_value=0.0, step=1.0)
             
             c4, c5, c6 = st.columns(3)
             px_val = c4.number_input("Prix de Vente Fixé (€/T)", min_value=0.0, step=0.5)
             date_c = c5.date_input("Date du contrat")
             ech_val = c6.text_input("Mois/Année d'échéance (ex: Dec 2024)")
             
             lien_c = st.text_input("Lien Google Drive PDF du Contrat")
             
             submitted_c = st.form_submit_button("Valider le Contrat", type="primary")
             if submitted_c:
                 if acht_val and qte_val > 0 and px_val > 0:
                      new_c_id = f"CT_{uuid.uuid4().hex[:6].upper()}"
                      row_c = {
                          "Date_contrat": date_c.strftime("%Y-%m-%d"),
                          "Campagne": selected_campaign,
                          "ID_contrat": new_c_id,
                          "Acheteur": acht_val,
                          "Produit": prod_val,
                          "Quantité_engagée_T": qte_val,
                          "Prix_T": px_val,
                          "Echéance": ech_val,
                          "Lien_Contrat": lien_c
                      }
                      if active_loader.update_contrats_ventes(new_c_id, row_c):
                          st.success("Contrat sauvegardé !")
                          time.sleep(1)
                          st.rerun()
                 else:
                      st.error("❌ Acheteur, Tonnage et Prix sont obligatoires.")

    st.subheader("Suivi de l'Exécution")
    if not df_contrats.empty:
        df_contrats['Quantité_engagée_T'] = pd.to_numeric(df_contrats['Quantité_engagée_T'], errors='coerce').fillna(0)
        df_contrats['Prix_T'] = pd.to_numeric(df_contrats['Prix_T'], errors='coerce').fillna(0)
        
        c_list = []
        for idx, row in df_contrats.iterrows():
             c_id = row['ID_contrat']
             q_eng = row['Quantité_engagée_T']
             q_livre = livraison_contrat.get(c_id, 0.0)
             reste = q_eng - q_livre
             pct = min((q_livre / q_eng) if q_eng > 0 else 0, 1.0)
             
             c_list.append({
                 "ID Contrat": c_id,
                 "Acheteur": row.get('Acheteur', ''),
                 "Produit": row.get('Produit', ''),
                 "Quantité (T)": q_eng,
                 "Déjà Livré (T)": q_livre,
                 "Reste à Livrer (T)": reste,
                 "Prix Vente (€/T)": row.get('Prix_T', 0),
                 "% Réalisé": pct
             })
             
        # Visual Progress Bars
        for item in c_list:
             ct_col_text, ct_col_bar = st.columns([2, 5])
             ct_col_text.markdown(f"**{item['ID Contrat']} - {item['Produit']}**<br><span style='color:gray; font-size:0.9em'>{item['Acheteur']}</span>", unsafe_allow_html=True)
             
             if item['% Réalisé'] == 1.0:
                  ct_col_bar.progress(1.0, text=f"✅ CONTRAT SOUDÉ (Livré: {item['Déjà Livré (T)']} T)")
             else:
                  ct_col_bar.progress(item['% Réalisé'], text=f"Livré: {item['Déjà Livré (T)']} T / {item['Quantité (T)']} T (Reste : {item['Reste à Livrer (T)']} T)")
        
        st.markdown("---")
        df_c_disp = pd.DataFrame(c_list).drop(columns=["% Réalisé"])
        for col in ["Quantité (T)", "Déjà Livré (T)", "Reste à Livrer (T)"]:
             df_c_disp[col] = df_c_disp[col].apply(lambda x: f"{x:.2f}")
        df_c_disp["Prix Vente (€/T)"] = df_c_disp["Prix Vente (€/T)"].apply(lambda x: f"{x:.2f} €")
             
        st.dataframe(df_c_disp, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        del_id_c = st.selectbox("Sélectionnez un contrat à supprimer", df_contrats['ID_contrat'].tolist())
        if st.button("🗑️ Supprimer ce contrat"):
             if active_loader.delete_contrats_ventes([del_id_c]):
                  st.success("Le contrat a été supprimé.")
                  time.sleep(1)
                  st.rerun()
    else:
        st.info("Aucun contrat d'engagement enregistré pour cette campagne.")

with tab4:
    st.header("Analyse de Trésorerie Sécurisée")
    st.markdown("Calcul dynamique du CA basé sur **les quantités contractées et les prix de vente**, comparé aux flux réels générés par les livraisons.")
    
    if not df_contrats.empty:
        ca_securise = 0
        ca_livre = 0
        
        for idx, row in df_contrats.iterrows():
            c_id = row['ID_contrat']
            px = row.get('Prix_T', 0)
            q_tot = row.get('Quantité_engagée_T', 0)
            q_liv = livraison_contrat.get(c_id, 0)
            
            ca_securise += (q_tot * px)
            ca_livre += (q_liv * px)
            
        ca_attendu = ca_securise - ca_livre
        
        c_fin1, c_fin2, c_fin3 = st.columns(3)
        c_fin1.markdown(f'<div class="metric-card metric-card-blue"><div class="metric-label">CA Brut Sécurisé (Contrats)</div><div class="metric-value">{ca_securise:,.0f} €</div></div>', unsafe_allow_html=True)
        c_fin2.markdown(f'<div class="metric-card"><div class="metric-label">CA Rentré (Marchandise livrée)</div><div class="metric-value">{ca_livre:,.0f} €</div></div>', unsafe_allow_html=True)
        c_fin3.markdown(f'<div class="metric-card metric-card-orange"><div class="metric-label">CA Latent (Restant à facturer)</div><div class="metric-value">{ca_attendu:,.0f} €</div></div>', unsafe_allow_html=True)
        
        # Petit graph circulaire ou barre progression globable
        if ca_securise > 0:
            st.divider()
            pct_rentre = int((ca_livre / ca_securise) * 100)
            st.write(f"### Avancement Financier de la Campagne: **{pct_rentre}%**")
            st.progress(ca_livre / ca_securise)
            
    else:
        st.info("💡 Ajoutez vos contrats de vente pour activer le suivi des données financières et de votre trésorerie bloquée en silo.")

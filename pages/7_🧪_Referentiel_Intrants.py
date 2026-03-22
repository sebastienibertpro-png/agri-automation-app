import streamlit as st
import pandas as pd
from datetime import datetime
import os
from ephy_fetcher import EphyFetcher
from shared import get_dataloader, get_drive_uploader, EPHY_DRIVE_FOLDER_ID, inject_premium_css, render_premium_header
import time

st.set_page_config(page_title="Référentiel Intrants", page_icon="🧪", layout="wide")

st.title("🧪 Référentiel des Intrants")
inject_premium_css()

st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        border-radius: 8px;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
</style>
""", unsafe_allow_html=True)

active_loader = get_dataloader()

tab_all, tab_usages, tab_phyto, tab_ferti, tab_semences = st.tabs([
    "📊 Tableau des intrants",
    "🍃 Usages Phytosanitaires",
    "🌿 Ajouter Phyto (E-Phy)", 
    "🚜 Ajouter Engrais", 
    "🌱 Ajouter Semences"
])

with tab_phyto:
    st.subheader("Recherche et Ajout de Produits Phytosanitaires")
    
    if "ephy_fetcher" not in st.session_state:
        st.session_state["ephy_fetcher"] = None

    fetcher = st.session_state.get("ephy_fetcher")

    # --- Auto-chargement depuis Drive au démarrage si le fetcher est vide ---
    if not fetcher or fetcher.nb_produits == 0:
        uploader = get_drive_uploader()
        if uploader:
            with st.spinner("⏳ Chargement automatique de la base E-Phy depuis votre Drive..."):
                f = EphyFetcher(auto_refresh=False)
                ok = f.download_from_drive(uploader, EPHY_DRIVE_FOLDER_ID)
                if ok and f.nb_produits > 0:
                    st.session_state["ephy_fetcher"] = f
                    fetcher = f
                    st.success(f"✅ Base E-Phy chargée automatiquement ({f.nb_produits} produits)")
                    st.rerun()

    if not fetcher or fetcher.nb_produits == 0:
        st.info("La base de recherche E-Phy n'est pas disponible sur votre Drive. Veuillez la charger depuis ANSES.")
        if st.button("🔄 Charger depuis ANSES (Lent - 2min)"):
            f = EphyFetcher(auto_refresh=False)
            with st.spinner("Téléchargement ANSES... (~1-2 min)"):
                if f.refresh(force=True):
                    uploader = get_drive_uploader()
                    if uploader:
                        f.upload_to_drive(uploader, EPHY_DRIVE_FOLDER_ID)
                        st.success("✅ Base uploadée sur votre Drive pour les prochaines sessions !")
                    st.session_state["ephy_fetcher"] = f
                    st.rerun()
                else:
                    st.error("Échec ANSES.")
                           
    if fetcher and fetcher.nb_produits > 0:
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.metric("📂 Produits E-Phy indexés", fetcher.nb_produits)
        with col_info2:
            st.metric("📅 Dernière MAJ", fetcher.last_update)
        with col_info3:
            if st.button("🔄 Forcer MAJ ANSES", key="btn_refresh_ephy"):
                  with st.spinner("Téléchargement ANSES... (~1-2 min)"):
                      if fetcher.refresh(force=True):
                          uploader = get_drive_uploader()
                          if uploader:
                              fetcher.upload_to_drive(uploader, EPHY_DRIVE_FOLDER_ID)
                          st.rerun()
                      else:
                          st.error("Échec ANSES.")

        st.markdown("---")

        st.markdown("#### 🔍 Recherche par nom commercial")
        search_query = st.text_input(
            "Nom commercial du produit",
            placeholder="Ex: TOPSIN M 70 WG, ROUNDUP FLEX, COMET PRO...",
            key="ephy_search_query"
        )

        if search_query and fetcher:
            with st.spinner(f"Recherche de '{search_query}' dans E-Phy..."):
                results = fetcher.search(search_query, top_n=8)

            if not results:
                st.warning("⚠️ Aucun produit trouvé. Vérifiez l'orthographe ou essayez un nom partiel.")
            else:
                options_labels = []
                for r in results:
                    nom = r['intrant'].get('Nom_Produit', '?')
                    amm = r['intrant'].get('N_AMM', '')
                    score = r['score']
                    etat = r['intrant'].get('Etat_AMM', '')
                    badge = "✅" if "autoris" in str(etat).lower() else ("🔴" if "retir" in str(etat).lower() else "🟡")
                    label = f"{badge} {nom} | AMM: {amm} | Score: {score}%"
                    options_labels.append(label)

                selected_label = st.radio(
                    "Sélectionnez le produit correspondant :",
                    options_labels,
                    key="ephy_select_result"
                )
                selected_idx = options_labels.index(selected_label)
                selected_result = results[selected_idx]
                intrant = selected_result['intrant']
                usages  = selected_result['usages']

                st.markdown("#### 📄 Fiche réglementaire E-Phy")
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    st.markdown(f"**Nom** : {intrant.get('Nom_Produit', '')}")
                    st.markdown(f"**N° AMM** : `{intrant.get('N_AMM', '')}`")
                    st.markdown(f"**Type** : {intrant.get('Type', '')}")
                    st.markdown(f"**Formulation** : {intrant.get('Formulation', '')}")
                    st.markdown(f"**Titulaire** : {intrant.get('Titulaire_AMM', '')}")
                    st.markdown(f"**État AMM** : {intrant.get('Etat_AMM', '')}")
                    st.markdown(f"**Date fin AMM** : {intrant.get('Date_Fin_AMM', '')}")
                with col_f2:
                    st.markdown(f"**Matières actives** : {intrant.get('Matieres_Actives', '')}")
                    st.markdown(f"**Concentration** : {intrant.get('Concentration', '')}")
                    st.markdown(f"**Classement CMR** : {intrant.get('Classement_CMR', '')}")
                    st.markdown(f"**Mentions danger** : {intrant.get('Mentions_Danger', '')}")
                    st.markdown(f"**ZNT Aquatique** : {intrant.get('ZNT_Aqua', '')} m")
                    st.markdown(f"**ZNT Riverains** : {intrant.get('ZNT_Riverains', '')} m")
                    st.markdown(f"**DVP** : {intrant.get('DVP', '')}")
                    if intrant.get('Lien_Ephy'):
                        st.markdown(f"[🔗 Fiche officielle E-Phy]({intrant['Lien_Ephy']})")

                if usages:
                    st.markdown(f"#### 🌱 Usages homologués ({len(usages)} usage(s))")
                    df_usages_display = pd.DataFrame(usages)
                    cols_display = [c for c in ["Culture", "Cible", "Type_Cible", "Dose_Max", "Unite_Dose",
                                                 "Nb_Applications_Max", "Stades_Application", "Condition_Emploi", "DAR", "DVP", "ZNT_Aqua", "Etat_Usage"]
                                    if c in df_usages_display.columns]
                    st.dataframe(df_usages_display[cols_display], use_container_width=True, hide_index=True)
                else:
                    st.info("ℹ️ Aucun usage détaillé disponible pour ce N°AMM dans E-Phy.")

                st.markdown("---")

                # ─── BOUTON COMBINÉ : REF_INTRANTS + REF_USAGES_PHYTO ───────────────
                if st.button("💾 Enregistrer dans REF_INTRANTS + REF_USAGES_PHYTO", key="btn_add_all", type="primary", use_container_width=True):
                    intrant_to_write = {
                        "Nom_Produit":       intrant.get("Nom_Produit", ""),
                        "Type":              intrant.get("Type", "Phytosanitaire"),
                        "N_AMM":             intrant.get("N_AMM", ""),
                        "Matieres_Actives":  intrant.get("Matieres_Actives", ""),
                        "Concentration":     intrant.get("Concentration", ""),
                        "Culture":           intrant.get("Culture", ""),
                        "Nb_Applications_Max_An": intrant.get("Nb_Applications_Max_An", ""),
                        "ZNT_Aqua":          intrant.get("ZNT_Aqua", ""),
                        "ZNT_Riverains":     intrant.get("ZNT_Riverains", ""),
                        "DVP":               intrant.get("DVP", ""),
                        "DAR":               intrant.get("DAR", ""),
                        "Dose_Max_Homologuee": intrant.get("Dose_Max_Homologuee", ""),
                        "Mentions_Danger":   intrant.get("Mentions_Danger", ""),
                        "Unité_utilisation": intrant.get("Unité_utilisation", ""),
                        "Formulation":       intrant.get("Formulation", ""),
                        "Etat_AMM":          intrant.get("Etat_AMM", ""),
                        "Date_Fin_AMM":      intrant.get("Date_Fin_AMM", ""),
                        "Classement_CMR":    intrant.get("Classement_CMR", ""),
                        "Titulaire_AMM":     intrant.get("Titulaire_AMM", ""),
                        "Lien_Ephy":         intrant.get("Lien_Ephy", ""),
                        "Date_MAJ_Ephy":     datetime.now().strftime("%d/%m/%Y"),
                    }
                    # FIX: use the correct session_state key to avoid duplicates
                    orig_search = st.session_state.get("ephy_search_query", "")
                    
                    with st.spinner("Enregistrement dans REF_INTRANTS..."):
                        ok1 = active_loader.update_intrant(intrant_to_write, original_name=orig_search)
                    
                    ok2 = True
                    if usages:
                        n_amm = intrant.get("N_AMM", "")
                        with st.spinner(f"Enregistrement de {len(usages)} usage(s) dans REF_USAGES_PHYTO..."):
                            ok2 = active_loader.update_usages_phyto(n_amm, usages)
                    
                    if ok1 and ok2:
                        st.success(f"✅ '{intrant_to_write['Nom_Produit']}' enregistré dans REF_INTRANTS + {len(usages)} usages dans REF_USAGES_PHYTO !")

with tab_ferti:
    st.subheader("➕ Ajouter un Engrais / Amendement")
    st.info("Renseignez les informations de base de l'engrais. Les éléments sont en unités standard (ex: N en unités, P en P2O5, K en K2O).")
    
    with st.form("form_add_ferti"):
        f_nom = st.text_input("Nom de l'engrais (ex: Ammonitrate 33, DAP 18-46)")
        f_type = st.selectbox("Type d'Engrais", ["Engrais Minéral", "Engrais Organique", "Amendement Calcique", "Amendement Organique", "Autre Fertilisant"])
        f_formulation = st.selectbox("Formulation", ["Solide (Granulés)", "Liquide", "Poudre", "Bouchon", "Vrac"])
        
        st.markdown("##### Composition")
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: f_n = st.number_input("Azote (N)", min_value=0.0, step=1.0)
        with c2: f_p = st.number_input("Phosphore (P2O5)", min_value=0.0, step=1.0)
        with c3: f_k = st.number_input("Potasse (K2O)", min_value=0.0, step=1.0)
        with c4: f_s = st.number_input("Soufre (SO3)", min_value=0.0, step=1.0)
        with c5: f_cao = st.number_input("Calcium (CaO)", min_value=0.0, step=1.0)
        
        st.markdown("##### Stock et Achat")
        c6, c7, c8 = st.columns(3)
        with c6: f_unite = st.selectbox("Unité d'achat / gestion", ["Tonne", "kg", "L", "BigBag"])
        with c7: f_prix = st.number_input("Prix Unitaire Moyen (€/Ut)", min_value=0.0, step=10.0)
        with c8: f_stock = st.number_input("Stock Initial", min_value=0.0, step=1.0)
        
        if st.form_submit_button("Enregistrer l'Engrais 🚜"):
            if not f_nom:
                st.error("Le nom de l'engrais est obligatoire.")
            else:
                row_dict = {
                    "Nom_Produit": f_nom,
                    "Type": "Fertilisant", # Catégorie mère
                    "Formulation": f_formulation,
                    "Element_N": f_n if f_n > 0 else "",
                    "Element_P": f_p if f_p > 0 else "",
                    "Element_K": f_k if f_k > 0 else "",
                    "Element_S": f_s if f_s > 0 else "",
                    "Element_Ca0": f_cao if f_cao > 0 else "", # Correct column name matching sheet
                    "Unite_Achat": f_unite,
                    "Prix_Unitaire_Moyen": f_prix if f_prix > 0 else "",
                    "STOCK_ACTUEL": f_stock if f_stock > 0 else "0",
                    "Date_MAJ_Ephy": datetime.now().strftime("%d/%m/%Y") # Utilisé comme date de création
                }
                with st.spinner("Enregistrement..."):
                    if active_loader.update_intrant(row_dict):
                        st.success(f"Engrais '{f_nom}' enregistré avec succès !")

with tab_semences:
    st.subheader("➕ Ajouter une Semence / Plant")
    
    with st.form("form_add_semence"):
        s_nom = st.text_input("Nom de la Variété (ex: P9234, LG 30.215)")
        s_espece = st.selectbox("Espèce de Semence", ["Maïs", "Blé Tendre", "Tournesol", "Orge", "Soja", "Colza", "Sorgho", "Pois", "COUVERTS/CIPAN", "Autre Semence"])
        
        st.markdown("##### Stock et Achat")
        c1, c2, c3 = st.columns(3)
        with c1: s_unite = st.selectbox("Unité d'achat", ["Dose", "kg", "Sac (25kg)", "Quintal"])
        with c2: s_prix = st.number_input("Prix Unitaire Moyen (€/Ut)", min_value=0.0, step=5.0)
        with c3: s_stock = st.number_input("Stock Initial", min_value=0.0, step=1.0)
        
        if st.form_submit_button("Enregistrer la Semence 🌱"):
            if not s_nom:
                st.error("Le nom de la variété est obligatoire.")
            else:
                row_dict = {
                    "Nom_Produit": s_nom,
                    "Type": "Semence",
                    "Culture": s_espece, # Peut aussi servir pour le filtrage
                    "Espèce_Semence": s_espece,
                    "Unite_Achat": s_unite,
                    "Prix_Unitaire_Moyen": s_prix if s_prix > 0 else "",
                    "STOCK_ACTUEL": s_stock if s_stock > 0 else "0",
                    "Date_MAJ_Ephy": datetime.now().strftime("%d/%m/%Y")
                }
                with st.spinner("Enregistrement..."):
                    if active_loader.update_intrant(row_dict):
                        st.success(f"Semence '{s_nom}' enregistrée avec succès !")

with tab_all:
    render_premium_header("📊 Tableau des intrants", "Vue d'ensemble de tous les produits")
    try:
        df_ref_current = active_loader.get_intrants()
        if not df_ref_current.empty:
            type_filter = st.multiselect(
                "Filtrer par Type :", 
                options=df_ref_current["Type"].dropna().unique().tolist(),
                default=df_ref_current["Type"].dropna().unique().tolist()
            )
            
            if type_filter:
                df_display = df_ref_current[df_ref_current["Type"].isin(type_filter)]
            else:
                df_display = df_ref_current
                
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            st.caption(f"{len(df_display)} intrants affichés sur un total de {len(df_ref_current)}.")
        else:
            st.info("ℹ️ REF_INTRANTS est vide.")
    except Exception as e:
        st.error(f"Erreur de chargement : {e}")

with tab_usages:
    render_premium_header("🍃 Usages Phytosanitaires", "Vue détaillée des usages homologués", color="blue")
    try:
        df_usages = active_loader.get_usages_phyto()
        if not df_usages.empty:
            st.dataframe(df_usages, use_container_width=True, hide_index=True)
            st.caption(f"{len(df_usages)} usages affichés.")
        else:
            st.info("ℹ️ Aucun usage phytosanitaire disponible.")
    except Exception as e:
        st.error(f"Erreur de chargement : {e}")

import streamlit as st
import pandas as pd
from datetime import datetime
import os
from ephy_fetcher import EphyFetcher
from shared import get_dataloader

st.set_page_config(page_title="Référentiel E-Phy", page_icon="🌿", layout="wide")

st.title("🌿 Référentiel Phytosanitaire (E-Phy)")

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

with st.expander("🔍 Rechercher un produit et remplir REF_INTRANTS + REF_USAGES_PHYTO", expanded=False):

    if "ephy_fetcher" not in st.session_state:
        with st.spinner("🔄 Chargement du référentiel E-Phy (première fois : ~30s)..."):
            try:
                st.session_state["ephy_fetcher"] = EphyFetcher(auto_refresh=True)
            except Exception as e_init:
                st.error(f"❌ Erreur initialisation E-Phy : {e_init}")
                st.session_state["ephy_fetcher"] = None

    fetcher: EphyFetcher | None = st.session_state.get("ephy_fetcher")

    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.metric("📂 Produits E-Phy indexés", fetcher.nb_produits if fetcher else 0)
    with col_info2:
        st.metric("📅 Dernière MAJ", fetcher.last_update if fetcher else "N/A")
    with col_info3:
        if st.button("🔄 Forcer mise à jour E-Phy", key="btn_refresh_ephy"):
            with st.spinner("Téléchargement du référentiel E-Phy en cours..."):
                ok = fetcher.refresh(force=True) if fetcher else False
            if ok:
                st.success("✅ Référentiel E-Phy mis à jour !")
                st.rerun()
            else:
                st.error("❌ Échec de la mise à jour.")

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

            st.markdown("#### ✍️ Enregistrer dans MASTER_EXPLOITATION")
            st.caption("⚠️ Les colonnes `Element_N/P/K`, `Espèce_Semence`, `Unite_Achat`, `Prix_Unitaire_Moyen`, `STOCK_ACTUEL`, `Valeur_Stock` ne sont pas modifiées si le produit existe déjà.")

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("➕ Ajouter/MAJ dans REF_INTRANTS", key="btn_add_intrant", type="primary"):
                    intrant_to_write = {
                        "Nom_Produit":       intrant.get("Nom_Produit", ""),
                        "Type":              intrant.get("Type", ""),
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
                    with st.spinner("Enregistrement dans REF_INTRANTS..."):
                        orig_search = st.session_state.get("search_phyto", "")
                        ok = active_loader.update_intrant(intrant_to_write, original_name=orig_search)
                    if ok:
                        active_loader.clear_cache()  
                        st.success(f"✅ '{intrant_to_write['Nom_Produit']}' enregistré dans REF_INTRANTS !")
                        st.rerun()  

            with col_btn2:
                if usages and st.button("🌱 Enregistrer usages dans REF_USAGES_PHYTO", key="btn_add_usages"):
                    n_amm = intrant.get("N_AMM", "")
                    with st.spinner(f"Enregistrement de {len(usages)} usage(s) dans REF_USAGES_PHYTO..."):
                        ok = active_loader.update_usages_phyto(n_amm, usages)
                    if ok:
                        st.success(f"✅ {len(usages)} usages enregistrés dans REF_USAGES_PHYTO !")

            st.markdown(" ")
            if st.button("🚀 Tout enregistrer (REF_INTRANTS + REF_USAGES_PHYTO)",
                         key="btn_add_all", type="primary"):
                intrant_to_write = {
                    "Nom_Produit":       intrant.get("Nom_Produit", ""),
                    "Type":              intrant.get("Type", ""),
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
                with st.spinner("Enregistrement en cours..."):
                    orig_search = st.session_state.get("search_phyto", "")
                    ok1 = active_loader.update_intrant(intrant_to_write, original_name=orig_search)
                    n_amm = intrant.get("N_AMM", "")
                    ok2 = active_loader.update_usages_phyto(n_amm, usages) if usages else True
                if ok1 and ok2:
                    active_loader.clear_cache()  
                    st.success("✅ Produit enregistré dans REF_INTRANTS et REF_USAGES_PHYTO !")
                    st.balloons()
                    st.rerun()  

    elif not fetcher:
        st.error("❌ Le module E-Phy n'a pas pu être initialisé. Vérifiez la connexion internet.")

st.markdown("---")
st.markdown("#### 📊 REF_INTRANTS actuel (produits phytosanitaires)")
try:
    df_ref_current = active_loader.get_intrants()
    if not df_ref_current.empty:
        phyto_types = ["Herbicide", "Fongicide", "Insecticide", "Molluscicide",
                       "Régulateur de croissance", "Nématicide", "Acaricide"]
        if "Type" in df_ref_current.columns:
            df_phyto_only = df_ref_current[
                df_ref_current["Type"].astype(str).str.strip().isin(phyto_types)
            ]
        else:
            df_phyto_only = df_ref_current

        if not df_phyto_only.empty:
            cols_prio = ["Nom_Produit", "Type", "N_AMM", "Etat_AMM", "Date_Fin_AMM",
                         "Matieres_Actives", "Formulation", "DAR", "ZNT_Aqua", "DVP",
                         "Classement_CMR", "Date_MAJ_Ephy"]
            
            for c in cols_prio:
                if c not in df_phyto_only.columns:
                    df_phyto_only[c] = ""
            
            cols_show = cols_prio
            st.dataframe(df_phyto_only[cols_show], use_container_width=True, hide_index=True)
            st.caption(f"{len(df_phyto_only)} produit(s) phytosanitaire(s) dans REF_INTRANTS")
        else:
            st.info("ℹ️ Aucun produit phytosanitaire trouvé dans REF_INTRANTS (Type non reconnu).")
    else:
        st.info("ℹ️ REF_INTRANTS est vide.")
except Exception as e:
    st.error(f"Erreur chargement REF_INTRANTS : {e}")


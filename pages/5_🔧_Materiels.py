import streamlit as st
import pandas as pd
import tempfile
import os
import uuid
from report_gen import ReportGenerator
from shared import get_dataloader, init_campaign_selector, inject_premium_css, render_premium_header

st.set_page_config(page_title="Matériels", page_icon="🚜", layout="centered")

inject_premium_css()

st.title("🚜 Matériels")

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

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# --- Chargement Global des Matériels ---
with st.spinner("Chargement des matériels..."):
    df_materiels = active_loader.get_materiels()
    if not df_materiels.empty:
        # Forcer le type chaîne (str) pour déverrouiller l'édition dans Streamlit
        df_materiels = df_materiels.astype(str)
        df_materiels = df_materiels.replace(["nan", "None", "<NA>", "NaN"], "")

materiel_options = []
materiel_map = {} 
if not df_materiels.empty:
    for _, row in df_materiels.iterrows():
        m_id = str(row.get('ID_Materiel', ''))
        marque = str(row.get('Marque', ''))
        modele = str(row.get('Modele', ''))
        if m_id:
            label = f"{m_id} - {marque} {modele}".strip(" -")
            materiel_options.append(label)
            materiel_map[label] = row

# --- Utilisation des Onglets ---
tab_synthese, tab_mon_materiel, tab_maint, tab_fuel = st.tabs(["📊 Carnets & Synthèse", "🚜 Mon Matériel", "🔧 Saisie Entretien", "⛽ Saisie Conso GNR"])

with tab_synthese:
    st.subheader("📊 Synthèse & Carnets d'Entretien")
    try:
        with st.expander("📄 Générer Carnet d'Entretien", expanded=True):
            if not materiel_options:
                st.info("Aucun matériel disponible pour la génération.")
            else:
                col_m1, col_m2 = st.columns([2, 1])
                with col_m1:
                    selected_mat_label_gen = st.selectbox("Sélectionnez pour PDF", sorted(materiel_options), key="pdf_sel")
                    
                if st.button("📄 Générer PDF"):
                    selected_row_gen = materiel_map[selected_mat_label_gen]
                    m_id_gen = str(selected_row_gen.get('ID_Materiel', ''))
                    
                    with st.spinner(f"Récupération de l'historique pour {m_id_gen}..."):
                        df_history = active_loader.get_maintenance_history(m_id_gen)
                        
                        with tempfile.TemporaryDirectory() as tmpdirname:
                            fname = f"Carnet_Entretien_{m_id_gen}.pdf"
                            fpath = os.path.join(tmpdirname, fname)
                            
                            gen = ReportGenerator(fpath)
                            gen.generate_maintenance_log(selected_row_gen.to_dict(), df_history)
                            
                            if os.path.exists(fpath):
                                with open(fpath, "rb") as f:
                                    st.download_button(
                                        label=f"⬇️ Télécharger Carnet ({m_id_gen})",
                                        data=f,
                                        file_name=fname,
                                        mime="application/pdf"
                                    )
                                st.success("Carnet généré ! Cliquez ci-dessus pour le télécharger.")
                            else:
                                st.error("Échec de la génération du PDF.")

        st.divider()

        # --- SECTION CONSOMMATION FUEL ---
        st.subheader(f"⛽ Consommation Fuel - Campagne {selected_campaign}")
        
        with st.spinner("Analyse de la consommation..."):
            df_fuel = active_loader.get_fuel_conso(selected_campaign)
            df_ref_mat = active_loader.get_materiels()
            
            if df_fuel.empty:
                st.info(f"Aucune donnée de consommation pour la campagne {selected_campaign}.")
            else:
                # Merge to get Type_Materiel
                df_merged = pd.merge(
                    df_fuel, 
                    df_ref_mat[['ID_Materiel', 'Type_Materiel']], 
                    on='ID_Materiel', 
                    how='left'
                )
                
                df_merged['Type_Materiel'] = df_merged['Type_Materiel'].fillna('Autre')
                
                # Aggregation by Material and Type
                df_pivot = df_merged.groupby(['Type_Materiel', 'ID_Materiel'])['FUEL_quantité_L'].sum().reset_index()
                df_pivot.columns = ['Type', 'Matériel', 'Consommation (L)']
                
                total_conso = df_pivot['Consommation (L)'].sum()
                row_total = pd.DataFrame([{'Type': 'TOTAL', 'Matériel': '', 'Consommation (L)': total_conso}])
                df_pivot = pd.concat([df_pivot, row_total], ignore_index=True)
                
                df_pivot['Consommation (L)'] = df_pivot['Consommation (L)'].apply(lambda x: f"{x:,.0f} L" if pd.notnull(x) else "")
                
                st.dataframe(df_pivot, use_container_width=True, hide_index=True)
                
                st.markdown("### 📊 Récapitulatif par Type")
                type_conso = df_merged.groupby('Type_Materiel')['FUEL_quantité_L'].sum().sort_values(ascending=False)
                
                cols = st.columns(min(len(type_conso), 4))
                for i, (m_type, total) in enumerate(type_conso.items()):
                    if i < len(cols):
                        cols[i].metric(m_type, f"{total:,.0f} L")

    except Exception as e:
        st.error(f"Erreur lors du traitement du carnet d'entretien : {e}")
        st.exception(e)

with tab_mon_materiel:
    render_premium_header("🚜 Gestion de Mon Matériel", "Consultez, modifiez ou supprimez vos matériels dans ce tableau interactif.", color="green")
    st.write("") # small spacer
    
    edited_df = st.data_editor(
        df_materiels,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "Image": None
        },
        key="editor_materiels"
    )
    
    if st.button("💾 Sauvegarder les modifications", type="primary"):
        with st.spinner("Enregistrement dans REF_MATERIELS..."):
            success = active_loader.overwrite_worksheet("REF_MATERIELS", edited_df)
            if success:
                st.success("Modifications enregistrées avec succès !")
                st.rerun()

with tab_maint:
    st.subheader("Nouvelle Saisie d'Entretien")
    if not materiel_options:
        st.warning("Aucun matériel trouvé dans REF_MATERIELS.")
    else:
        with st.form("form_saisie_maint"):
            col_m1, col_m2, col_m3 = st.columns(3)
            with col_m1:
                m_date = st.date_input("Date de l'entretien")
                m_id_label = st.selectbox("Sélectionnez le matériel", sorted(materiel_options))
            with col_m2:
                m_type = st.selectbox("Type d'Intervention", ["Vidange", "Filtres", "Pneumatiques", "Réparation", "Révision", "Autre"])
                m_heures = st.number_input("Heures Moteur", min_value=0.0, step=10.0)
            with col_m3:
                m_intervenant = st.text_input("Intervenant")
                m_cout = st.number_input("Montant Réel HT (€)", min_value=0.0, step=10.0)
                
            m_desc = st.text_input("Description courte")
            
            c_cons1, c_cons2, c_cons3, c_cons4 = st.columns(4)
            with c_cons1: m_cons1 = st.text_input("Consommable 1")
            with c_cons2: m_qte1 = st.number_input("Qtité 1", min_value=0.0, step=1.0)
            with c_cons3: m_cons2 = st.text_input("Consommable 2")
            with c_cons4: m_qte2 = st.number_input("Qtité 2", min_value=0.0, step=1.0)
            
            c_fact1, c_fact2 = st.columns(2)
            with c_fact1: m_facture = st.text_input("ID Facture Associée")
            with c_fact2: m_commentaires = st.text_input("Commentaires supplémentaires")
            
            submit_maint = st.form_submit_button("Enregistrer l'Entretien 🛠️")
            if submit_maint:
                selected_row = materiel_map[m_id_label]
                row_dict = {
                    "ID_Entretien": str(uuid.uuid4())[:8].upper(),
                    "Date": m_date.strftime("%d/%m/%Y"),
                    "ID_Materiel": selected_row.get("ID_Materiel", ""),
                    "Type_Intervention": m_type,
                    "Description": m_desc,
                    "Heures_Moteur": m_heures if m_heures > 0 else "",
                    "Intervenant": m_intervenant,
                    "Consommables_1": m_cons1,
                    "Qtité_1": m_qte1 if m_qte1 > 0 else "",
                    "Consommable_2": m_cons2,
                    "Qtité_2": m_qte2 if m_qte2 > 0 else "",
                    "Montant_Reel_HT": m_cout if m_cout > 0 else "",
                    "ID_Facture_Associee": m_facture,
                    "Commentaires": m_commentaires
                }
                with st.spinner("Enregistrement en cours..."):
                    if active_loader.insert_row("JOURNAL_MAINTENANCE", row_dict):
                        st.success("Entretien enregistré avec succès !")
                    else:
                        st.error("Échec de l'enregistrement.")

with tab_fuel:
    st.subheader("Nouvelle Saisie de Consommation GNR")
    if not materiel_options:
        st.warning("Aucun matériel trouvé dans REF_MATERIELS.")
    else:
        with st.form("form_saisie_fuel"):
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                f_date = st.date_input("Date du plein")
                f_id_label = st.selectbox("Matériel concerné", sorted(materiel_options))
            with col_f2:
                f_qte = st.number_input("Quantité Fuel (Litres)", min_value=0.0, step=10.0)
                f_tache = st.text_input("Tâche réalisée")
            
            submit_fuel = st.form_submit_button("Enregistrer la Consommation ⛽")
            if submit_fuel:
                if f_qte <= 0:
                    st.error("Veuillez saisir une quantité supérieure à 0 L.")
                else:
                    selected_row = materiel_map[f_id_label]
                    row_dict = {
                        "ID_Conso_Fuel": str(uuid.uuid4())[:8].upper(),
                        "Date": f_date.strftime("%d/%m/%Y"),
                        "Campagne": selected_campaign,
                        "ID_Materiel": selected_row.get("ID_Materiel", ""),
                        "FUEL_quantité_L": f_qte,
                        "Tache_réalisée": f_tache
                    }
                    with st.spinner("Enregistrement en cours..."):
                        if active_loader.insert_row("CONSO_FUEL", row_dict):
                            st.success("Consommation enregistrée avec succès !")
                        else:
                            st.error("Échec de l'enregistrement.")

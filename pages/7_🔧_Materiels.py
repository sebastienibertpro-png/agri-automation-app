import streamlit as st
import pandas as pd
import tempfile
import os
import uuid
from report_gen import ReportGenerator
from shared import get_dataloader, init_campaign_selector, inject_premium_css, render_premium_header

st.set_page_config(page_title="Matériels", page_icon="🚜", layout="wide")

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

# ============================================================
# ONGLETS PRINCIPAUX
# ============================================================
tab_entretien_gnr, tab_mon_materiel, tab_journal_maint, tab_journal_fuel = st.tabs([
    "🔧 Entretien & Conso GNR",
    "🚜 Mon Matériel",
    "📋 Journal Entretien",
    "⛽ Journal Conso GNR"
])

# ============================================================
# ONGLET 1 : ENTRETIEN & CONSO GNR
# ============================================================
with tab_entretien_gnr:
    try:
        # ─── SECTION : CARNETS D'ENTRETIEN ───────────────────────────────
        st.subheader("📄 Carnets d'entretien")

        with st.expander("📄 Générer Carnet d'Entretien", expanded=True):
            if not materiel_options:
                st.info("Aucun matériel disponible pour la génération.")
            else:
                col_m1, col_m2 = st.columns([2, 1])
                with col_m1:
                    selected_mat_label_gen = st.selectbox(
                        "Sélectionnez pour PDF", sorted(materiel_options), key="pdf_sel"
                    )

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

        # ─── EXPANDER : SAISIE ENTRETIEN ─────────────────────────────────
        with st.expander("🔧 Saisie Entretien", expanded=False):
            if not materiel_options:
                st.warning("Aucun matériel trouvé dans REF_MATERIELS.")
            else:
                with st.form("form_saisie_maint_inline"):
                    col_m1, col_m2, col_m3 = st.columns(3)
                    with col_m1:
                        m_date = st.date_input("Date de l'entretien", key="maint_date_inline")
                        m_id_label = st.selectbox(
                            "Sélectionnez le matériel", sorted(materiel_options), key="maint_mat_inline"
                        )
                    with col_m2:
                        m_type = st.selectbox(
                            "Type d'Intervention",
                            ["Vidange", "Filtres", "Pneumatiques", "Réparation", "Révision", "Autre"],
                            key="maint_type_inline"
                        )
                        m_heures = st.number_input(
                            "Heures Moteur", min_value=0.0, step=10.0, key="maint_heures_inline"
                        )
                    with col_m3:
                        m_intervenant = st.text_input("Intervenant", key="maint_interv_inline")
                        m_cout = st.number_input(
                            "Montant Réel HT (€)", min_value=0.0, step=10.0, key="maint_cout_inline"
                        )

                    m_desc = st.text_input("Description courte", key="maint_desc_inline")

                    c_cons1, c_cons2, c_cons3, c_cons4 = st.columns(4)
                    with c_cons1: m_cons1 = st.text_input("Consommable 1", key="maint_cons1_inline")
                    with c_cons2: m_qte1 = st.number_input("Qtité 1", min_value=0.0, step=1.0, key="maint_qte1_inline")
                    with c_cons3: m_cons2 = st.text_input("Consommable 2", key="maint_cons2_inline")
                    with c_cons4: m_qte2 = st.number_input("Qtité 2", min_value=0.0, step=1.0, key="maint_qte2_inline")

                    c_fact1, c_fact2 = st.columns(2)
                    with c_fact1: m_facture = st.text_input("ID Facture Associée", key="maint_fact_inline")
                    with c_fact2: m_commentaires = st.text_input("Commentaires supplémentaires", key="maint_comm_inline")

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

        st.divider()

        # ─── SECTION : CONSOMMATION FUEL ─────────────────────────────────
        st.subheader(f"⛽ Consommation Fuel — Campagne {selected_campaign}")

        with st.spinner("Analyse de la consommation..."):
            df_fuel = active_loader.get_fuel_conso(selected_campaign)
            df_ref_mat = active_loader.get_materiels()

            if df_fuel.empty:
                st.info(f"Aucune donnée de consommation pour la campagne {selected_campaign}.")
            else:
                df_merged = pd.merge(
                    df_fuel,
                    df_ref_mat[['ID_Materiel', 'Type_Materiel']],
                    on='ID_Materiel',
                    how='left'
                )
                df_merged['Type_Materiel'] = df_merged['Type_Materiel'].fillna('Autre').astype(str).str.strip().str.title()

                df_pivot = df_merged.groupby(['Type_Materiel', 'ID_Materiel'])['FUEL_quantité_L'].sum().reset_index()
                df_pivot.columns = ['Type', 'Matériel', 'Consommation (L)']

                total_conso = df_pivot['Consommation (L)'].sum()
                row_total = pd.DataFrame([{'Type': 'TOTAL', 'Matériel': '', 'Consommation (L)': total_conso}])
                df_pivot = pd.concat([df_pivot, row_total], ignore_index=True)
                df_pivot['Consommation (L)'] = df_pivot['Consommation (L)'].apply(
                    lambda x: f"{x:,.0f} L" if pd.notnull(x) else ""
                )
                st.dataframe(df_pivot, use_container_width=True, hide_index=True)

                st.markdown("### 📊 Récapitulatif par Type")
                type_conso = df_merged.groupby('Type_Materiel')['FUEL_quantité_L'].sum().sort_values(ascending=False)
                cols_type = st.columns(min(max(len(type_conso), 1), 4))
                for i, (m_type, total) in enumerate(type_conso.items()):
                    if i < len(cols_type):
                        cols_type[i].metric(m_type, f"{total:,.0f} L")
                
                st.divider()

                st.markdown("### 📊 Récapitulatif par Tâche")
                if 'Tache_réalisée' in df_merged.columns:
                    df_merged['Tache_réalisée'] = df_merged['Tache_réalisée'].fillna('Non spécifié').astype(str).str.strip().str.title()
                    df_merged.loc[df_merged['Tache_réalisée'] == '', 'Tache_réalisée'] = 'Non spécifié'
                    tache_conso = df_merged.groupby('Tache_réalisée')['FUEL_quantité_L'].sum().sort_values(ascending=False)
                    cols_tache = st.columns(min(max(len(tache_conso), 1), 4))
                    for i, (t_type, total) in enumerate(tache_conso.items()):
                        if i < len(cols_tache):
                            cols_tache[i].metric(t_type, f"{total:,.0f} L")

        # ─── EXPANDER : SAISIE CONSO GNR ─────────────────────────────────
        with st.expander("⛽ Saisie Consommation GNR", expanded=False):
            if not materiel_options:
                st.warning("Aucun matériel trouvé dans REF_MATERIELS.")
            else:
                with st.form("form_saisie_fuel_inline"):
                    col_f1, col_f2 = st.columns(2)
                    with col_f1:
                        f_date = st.date_input("Date du plein", key="fuel_date_inline")
                        f_id_label = st.selectbox(
                            "Matériel concerné", sorted(materiel_options), key="fuel_mat_inline"
                        )
                    with col_f2:
                        f_qte = st.number_input(
                            "Quantité Fuel (Litres)", min_value=0.0, step=10.0, key="fuel_qte_inline"
                        )
                        f_tache = st.text_input("Tâche réalisée", key="fuel_tache_inline")

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

    except Exception as e:
        st.error(f"Erreur lors du traitement : {e}")
        st.exception(e)

# ============================================================
# ONGLET 2 : MON MATÉRIEL
# ============================================================
with tab_mon_materiel:
    render_premium_header(
        "🚜 Gestion de Mon Matériel",
        "Consultez, modifiez ou supprimez vos matériels dans ce tableau interactif.",
        color="green"
    )
    st.write("")

    # Nettoyage supplémentaire pour l'affichage (éviter les "None" résiduels)
    df_mon_mat = df_materiels.copy()
    for col in df_mon_mat.columns:
        df_mon_mat[col] = df_mon_mat[col].apply(lambda x: "" if pd.isna(x) or str(x).strip().lower() == "none" else x)

    # Configuration des colonnes
    mat_column_config = {
        "Image": None,
        "Cout_fixe_annuel_estime": None,
        "Cout_fixe_annuel_estime_€": None, # Au cas où
        "Statut": None
    }

    edited_df = st.data_editor(
        df_mon_mat,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config=mat_column_config,
        key="editor_materiels"
    )

    if st.button("💾 Sauvegarder les modifications", type="primary"):
        with st.spinner("Enregistrement dans REF_MATERIELS..."):
            success = active_loader.overwrite_worksheet("REF_MATERIELS", edited_df)
            if success:
                st.success("Modifications enregistrées avec succès !")
                st.rerun()

# ============================================================
# ONGLET 3 : JOURNAL ENTRETIEN
# ============================================================
with tab_journal_maint:
    render_premium_header(
        "📋 Journal Entretien",
        "Consultez, modifiez ou supprimez les entrées du journal de maintenance.",
        color="blue"
    )

    with st.spinner("Chargement du journal..."):
        df_journal_maint = active_loader.get_maintenance_history()  # Sans filtre → tout l'historique

    if df_journal_maint.empty:
        st.info("Aucune entrée dans le journal d'entretien.")
    else:
        # Nettoyage pour l'affichage
        df_journal_maint_display = df_journal_maint.copy()
        if 'Date' in df_journal_maint_display.columns:
            df_journal_maint_display['Date'] = pd.to_datetime(
                df_journal_maint_display['Date'], errors='coerce', dayfirst=True
            )
            df_journal_maint_display = df_journal_maint_display.sort_values('Date', ascending=False)
            df_journal_maint_display['Date'] = df_journal_maint_display['Date'].dt.strftime('%d/%m/%Y')

        # Filtres rapides
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filter_mat_maint = st.selectbox(
                "🔍 Filtrer par Matériel",
                ["Tous"] + sorted(materiel_options),
                key="filter_mat_journal_maint"
            )
        with col_f2:
            filter_type_maint = st.selectbox(
                "🔍 Filtrer par Type",
                ["Tous"] + sorted(df_journal_maint_display.get('Type_Intervention', pd.Series()).dropna().unique().tolist()),
                key="filter_type_journal_maint"
            ) if 'Type_Intervention' in df_journal_maint_display.columns else "Tous"

        df_filtered_maint = df_journal_maint_display.copy()

        if filter_mat_maint != "Tous":
            mat_id_filter = filter_mat_maint.split(" - ")[0] if " - " in filter_mat_maint else filter_mat_maint
            if 'ID_Materiel' in df_filtered_maint.columns:
                df_filtered_maint = df_filtered_maint[
                    df_filtered_maint['ID_Materiel'].astype(str).str.strip() == mat_id_filter
                ]

        if isinstance(filter_type_maint, str) and filter_type_maint != "Tous":
            if 'Type_Intervention' in df_filtered_maint.columns:
                df_filtered_maint = df_filtered_maint[
                    df_filtered_maint['Type_Intervention'].astype(str) == filter_type_maint
                ]

        st.caption(f"📊 {len(df_filtered_maint)} entrée(s) affichée(s)")

        # Nettoyage robuste pour l'affichage
        df_editor_maint = df_filtered_maint.reset_index(drop=True).copy()
        for col in df_editor_maint.columns:
            df_editor_maint[col] = df_editor_maint[col].apply(lambda x: "" if pd.isna(x) or str(x).strip().lower() == "none" else x)

        # Tableau éditable
        edited_journal_maint = st.data_editor(
            df_editor_maint,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="editor_journal_maint",
            column_config={
                "ID_Entretien": None,
                "Date": st.column_config.TextColumn("Date", width="small"),
                "ID_Materiel": st.column_config.TextColumn("Matériel", width="medium"),
                "Type_Intervention": st.column_config.TextColumn("Type", width="medium"),
                "Description": st.column_config.TextColumn("Description", width="large"),
                "Heures_Moteur": st.column_config.TextColumn("Heures", width="small"),
                "Montant_Reel_HT": st.column_config.TextColumn("Montant HT (€)", width="small"),
            }
        )

        col_save_maint, col_del_maint = st.columns([3, 1])

        with col_save_maint:
            if st.button("💾 Sauvegarder les modifications du Journal", type="primary", key="save_journal_maint"):
                with st.spinner("Mise à jour du journal..."):
                    # Recharger le journal complet, appliquer les changements sur la portion filtrée
                    df_full_maint = active_loader.get_maintenance_history()

                    if filter_mat_maint == "Tous" and (isinstance(filter_type_maint, str) and filter_type_maint == "Tous"):
                        # Réécriture totale
                        success = active_loader.overwrite_worksheet("JOURNAL_MAINTENANCE", edited_journal_maint)
                    else:
                        # Merge : remplacer les lignes modifiées par ID_Entretien
                        if 'ID_Entretien' in df_full_maint.columns and 'ID_Entretien' in edited_journal_maint.columns:
                            # Supprimer les lignes qui correspondent au filtre actuel
                            ids_in_edit = edited_journal_maint['ID_Entretien'].astype(str).tolist()
                            ids_in_filter = df_filtered_maint['ID_Entretien'].astype(str).tolist()

                            # Retirer les anciennes lignes du filtre du DataFrame complet
                            df_full_maint = df_full_maint[
                                ~df_full_maint['ID_Entretien'].astype(str).isin(ids_in_filter)
                            ]
                            # Ajouter les lignes éditées
                            df_full_maint = pd.concat([df_full_maint, edited_journal_maint], ignore_index=True)
                            success = active_loader.overwrite_worksheet("JOURNAL_MAINTENANCE", df_full_maint)
                        else:
                            success = active_loader.overwrite_worksheet("JOURNAL_MAINTENANCE", edited_journal_maint)

                    if success:
                        st.success("Journal mis à jour avec succès !")
                        st.rerun()

        with col_del_maint:
            if st.button("🗑️ Réinitialiser la vue", type="secondary", key="reset_journal_maint"):
                st.rerun()

# ============================================================
# ONGLET 4 : JOURNAL CONSO GNR
# ============================================================
with tab_journal_fuel:
    render_premium_header(
        "⛽ Journal Conso GNR",
        "Consultez, modifiez ou supprimez les entrées de consommation de carburant.",
        color="orange"
    )

    with st.spinner("Chargement du journal GNR..."):
        try:
            df_journal_fuel_all = active_loader._get_data("CONSO_FUEL")
        except Exception:
            df_journal_fuel_all = pd.DataFrame()

    if df_journal_fuel_all.empty:
        st.info("Aucune entrée dans le journal de consommation GNR.")
    else:
        df_journal_fuel_display = df_journal_fuel_all.copy()
        if 'Date' in df_journal_fuel_display.columns:
            df_journal_fuel_display['Date'] = pd.to_datetime(
                df_journal_fuel_display['Date'], errors='coerce', dayfirst=True
            )
            df_journal_fuel_display = df_journal_fuel_display.sort_values('Date', ascending=False)
            df_journal_fuel_display['Date'] = df_journal_fuel_display['Date'].dt.strftime('%d/%m/%Y')

        # Filtres rapides
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            filter_mat_fuel = st.selectbox(
                "🔍 Filtrer par Matériel",
                ["Tous"] + sorted(materiel_options),
                key="filter_mat_journal_fuel"
            )
        with col_g2:
            campagnes_dispo = ["Toutes"]
            if 'Campagne' in df_journal_fuel_display.columns:
                campagnes_dispo += sorted(
                    df_journal_fuel_display['Campagne'].dropna().astype(str).unique().tolist(), reverse=True
                )
            filter_camp_fuel = st.selectbox(
                "📅 Filtrer par Campagne",
                campagnes_dispo,
                key="filter_camp_journal_fuel"
            )

        df_filtered_fuel = df_journal_fuel_display.copy()

        if filter_mat_fuel != "Tous":
            mat_id_filter_f = filter_mat_fuel.split(" - ")[0] if " - " in filter_mat_fuel else filter_mat_fuel
            if 'ID_Materiel' in df_filtered_fuel.columns:
                df_filtered_fuel = df_filtered_fuel[
                    df_filtered_fuel['ID_Materiel'].astype(str).str.strip() == mat_id_filter_f
                ]

        if filter_camp_fuel != "Toutes":
            if 'Campagne' in df_filtered_fuel.columns:
                df_filtered_fuel = df_filtered_fuel[
                    df_filtered_fuel['Campagne'].astype(str) == filter_camp_fuel
                ]

        st.caption(f"📊 {len(df_filtered_fuel)} entrée(s) affichée(s)")

        # Nettoyage robuste pour l'affichage
        df_editor_fuel = df_filtered_fuel.reset_index(drop=True).copy()
        for col in df_editor_fuel.columns:
            df_editor_fuel[col] = df_editor_fuel[col].apply(lambda x: "" if pd.isna(x) or str(x).strip().lower() == "none" else x)

        # Tableau éditable
        edited_journal_fuel = st.data_editor(
            df_editor_fuel,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="editor_journal_fuel",
            column_config={
                "ID_Conso_Fuel": None,
                "Date": st.column_config.TextColumn("Date", width="small"),
                "Campagne": st.column_config.TextColumn("Campagne", width="small"),
                "ID_Materiel": st.column_config.TextColumn("Matériel", width="medium"),
                "FUEL_quantité_L": st.column_config.TextColumn("Quantité (L)", width="medium"),
                "Tache_réalisée": st.column_config.TextColumn("Tâche réalisée", width="large"),
            }
        )

        col_save_fuel, col_reset_fuel = st.columns([3, 1])

        with col_save_fuel:
            if st.button("💾 Sauvegarder les modifications du Journal GNR", type="primary", key="save_journal_fuel"):
                with st.spinner("Mise à jour du journal GNR..."):
                    df_full_fuel = active_loader._get_data("CONSO_FUEL")

                    if filter_mat_fuel == "Tous" and filter_camp_fuel == "Toutes":
                        success = active_loader.overwrite_worksheet("CONSO_FUEL", edited_journal_fuel)
                    else:
                        if 'ID_Conso_Fuel' in df_full_fuel.columns and 'ID_Conso_Fuel' in edited_journal_fuel.columns:
                            ids_in_filter_f = df_filtered_fuel['ID_Conso_Fuel'].astype(str).tolist()
                            df_full_fuel = df_full_fuel[
                                ~df_full_fuel['ID_Conso_Fuel'].astype(str).isin(ids_in_filter_f)
                            ]
                            df_full_fuel = pd.concat([df_full_fuel, edited_journal_fuel], ignore_index=True)
                            success = active_loader.overwrite_worksheet("CONSO_FUEL", df_full_fuel)
                        else:
                            success = active_loader.overwrite_worksheet("CONSO_FUEL", edited_journal_fuel)

                    if success:
                        st.success("Journal GNR mis à jour avec succès !")
                        st.rerun()

        with col_reset_fuel:
            if st.button("🗑️ Réinitialiser la vue", type="secondary", key="reset_journal_fuel"):
                st.rerun()

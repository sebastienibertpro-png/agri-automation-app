import streamlit as st
import pandas as pd
from data_loader import DataLoader
from shared import init_campaign_selector, inject_premium_css, render_premium_header
from report_gen import generate_ppf_pdf

st.set_page_config(page_title="Plan Prévisionnel de Fumure", page_icon="🌿", layout="wide")
inject_premium_css()

st.title("🌿 Plan Prévisionnel de Fumure (PPF)")
st.markdown("---")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()
dl = active_loader
campagne_input = int(selected_campaign)

if not dl:
    st.warning("⚠️ Mode Local actif (Lecture seule). Aucune sauvegarde possible.")

# ─── Chargement commun ────────────────────────────────────────────────────────
parcels_meta = dl.get_parcel_metadata(campagne_input)
if not parcels_meta:
    st.warning(f"Aucune parcelle trouvée pour la campagne {campagne_input}.")
    st.stop()

liste_parcelles = sorted(list(parcels_meta.keys()))
df_ppf_all = dl.get_ppf(campagne_input)
df_interventions = dl.get_interventions()

# Filtrer les interventions de fertilisation de la campagne
df_ferti_all = pd.DataFrame()
if not df_interventions.empty:
    df_interventions["Campagne"] = pd.to_numeric(df_interventions["Campagne"], errors="coerce").fillna(0).astype(int)
    df_ferti_all = df_interventions[
        (df_interventions["Campagne"] == campagne_input) &
        (df_interventions["Nature_Intervention"].astype(str).str.strip().str.lower() == "fertilisation")
    ].copy()
    if "Date" in df_ferti_all.columns:
        df_ferti_all["Date"] = pd.to_datetime(df_ferti_all["Date"], errors="coerce", dayfirst=True)

# ─── Onglets ──────────────────────────────────────────────────────────────────
tab_consult, tab_saisie = st.tabs(["📊 Consultation", "✍️ Saisie du PPF"])

# ═══════════════════════════════════════════════════════════════════════════════
# ─── ONGLET 1 : CONSULTATION ──────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
with tab_consult:

    # ── Tableau de synthèse de toutes les parcelles ──────────────────────────
    render_premium_header("📋 Synthèse PPF — Toutes les Parcelles", f"Campagne {campagne_input}", color="green")

    rows_summary = []
    for p_id in liste_parcelles:
        p_data = parcels_meta.get(p_id, {})
        surface = p_data.get("Surface", 0.0)
        culture = p_data.get("Culture", "—")
        variete = p_data.get("Variete", "")

        # Données PPF sauvegardées
        dose_x = "—"
        obj_rdt = "—"
        has_ppf = False
        if not df_ppf_all.empty:
            mask = df_ppf_all["ID_Parcelle"].astype(str).str.strip() == str(p_id).strip()
            row_ppf = df_ppf_all[mask]
            if not row_ppf.empty:
                has_ppf = True
                r = row_ppf.iloc[0]
                dose_x_val = pd.to_numeric(r.get("Dose_X"), errors="coerce")
                dose_x = f"{dose_x_val:.0f} U N/ha" if pd.notna(dose_x_val) else "—"
                rdt_val = pd.to_numeric(r.get("Objectif_Rendement_Qx_Ha"), errors="coerce")
                obj_rdt = f"{rdt_val:.0f} Qx/ha" if pd.notna(rdt_val) else "—"

        # Fertilisation réalisée
        total_n_realise = 0.0
        nb_apports = 0
        if not df_ferti_all.empty:
            df_p = df_ferti_all[df_ferti_all["ID_Parcelle"].astype(str).str.strip() == str(p_id).strip()]
            # Seulement les réalisées
            status_col = next((c for c in ["Statut_Intervention", "Statut", "Etat"] if c in df_p.columns), None)
            if status_col:
                df_real = df_p[df_p[status_col].astype(str).str.strip().str.lower().str.startswith("réal")]
            else:
                df_real = df_p
            nb_apports = len(df_real)
            for col_n in ["N/ha", "N_ha", "Dose_Unitaire_N", "N", "Dose_Ha"]:
                if col_n in df_real.columns:
                    total_n_realise = pd.to_numeric(df_real[col_n], errors="coerce").fillna(0).sum()
                    break

        rows_summary.append({
            "Parcelle": p_id,
            "Culture": f"{culture} {variete}".strip() if str(variete).lower() not in ("nan", "none", "") else culture,
            "Surface (ha)": f"{surface:.2f}",
            "Objectif Rdt": obj_rdt,
            "Dose X (U N/ha)": dose_x,
            "N Réalisé (U N/ha)": f"{total_n_realise:.0f}" if nb_apports > 0 else "0",
            "Nb Apports": nb_apports,
            "PPF Saisi": "✅" if has_ppf else "❌",
        })

    df_summary = pd.DataFrame(rows_summary)

    # Affichage avec sélection de ligne
    st.markdown("<br>", unsafe_allow_html=True)
    df_summary.insert(0, "Sélect.", False)
    edited = st.data_editor(
        df_summary,
        column_config={
            "Sélect.": st.column_config.CheckboxColumn("", default=False),
            "Parcelle": st.column_config.TextColumn("🌾 Parcelle"),
            "Culture": st.column_config.TextColumn("Culture"),
            "Surface (ha)": st.column_config.TextColumn("Surface"),
            "Objectif Rdt": st.column_config.TextColumn("🎯 Obj. Rdt"),
            "Dose X (U N/ha)": st.column_config.TextColumn("💊 Dose X"),
            "N Réalisé (U N/ha)": st.column_config.TextColumn("✅ N Réalisé"),
            "Nb Apports": st.column_config.NumberColumn("Nb Apports"),
            "PPF Saisi": st.column_config.TextColumn("PPF"),
        },
        disabled=[c for c in df_summary.columns if c != "Sélect."],
        hide_index=True,
        use_container_width=True,
        key="ppf_summary_editor",
    )

    selected_rows = edited[edited["Sélect."] == True]

    # ── Détail d'une parcelle sélectionnée ───────────────────────────────────
    if not selected_rows.empty:
        parcelle_sel = selected_rows.iloc[0]["Parcelle"]
        p_data_sel = parcels_meta.get(parcelle_sel, {})

        st.markdown("<br>", unsafe_allow_html=True)
        render_premium_header(
            f"📍 Détail — {parcelle_sel}",
            f"{p_data_sel.get('Culture','')} | {p_data_sel.get('Surface',0):.2f} ha",
            color="blue"
        )

        # KPIs du PPF
        dose_x_num = None
        if not df_ppf_all.empty:
            mask = df_ppf_all["ID_Parcelle"].astype(str).str.strip() == str(parcelle_sel).strip()
            row_ppf = df_ppf_all[mask]
            if not row_ppf.empty:
                r = row_ppf.iloc[0]
                dose_x_num = pd.to_numeric(r.get("Dose_X"), errors="coerce")
                besoins = pd.to_numeric(r.get("Besoin_Culture(Pf)"), errors="coerce") + pd.to_numeric(r.get("Azote_Fermeture_Bilan(Rf)"), errors="coerce")
                fournitures = sum([
                    pd.to_numeric(r.get(c, 0), errors="coerce") or 0
                    for c in ["Azote_deja_aborbé(Pi)", "Reliquat_Sortie_Hiver(Ri)", "Minéralisation_Humus(Mh)", "Effet_précédent(Mr)", "Effet_CIPAN(MrCi)", "Fourniture_Irrigation(Nirr)"]
                ])
                kpi1, kpi2, kpi3 = st.columns(3)
                kpi1.metric("Total Besoins", f"{besoins:.0f} U N/ha" if pd.notna(besoins) else "—")
                kpi2.metric("Total Fournitures", f"{fournitures:.0f} U N/ha")
                kpi3.metric("🎯 Dose X", f"{dose_x_num:.0f} U N/ha" if pd.notna(dose_x_num) else "—")

        st.markdown("<br>", unsafe_allow_html=True)

        # Interventions de fertilisation de la parcelle
        df_ferti_p = pd.DataFrame()
        if not df_ferti_all.empty:
            df_ferti_p = df_ferti_all[df_ferti_all["ID_Parcelle"].astype(str).str.strip() == str(parcelle_sel).strip()].copy()

        if df_ferti_p.empty:
            st.info("Aucune intervention de fertilisation enregistrée pour cette parcelle.")
        else:
            status_col = next((c for c in ["Statut_Intervention", "Statut", "Etat"] if c in df_ferti_p.columns), None)

            def render_section(df_sub, label, color_hex, icon):
                if df_sub.empty:
                    return
                # Déterminer la colonne N/ha disponible
                n_col = next((c for c in ["N/ha", "N_ha", "Dose_Unitaire_N", "N"] if c in df_sub.columns), None)
                cols_show = ["Date", "Nom_Produit", "Dose_Ha", "Unité_Dose", "Surface_Travaillée_Ha"]
                cols_show = [c for c in cols_show if c in df_sub.columns]
                df_display = df_sub[cols_show].copy()
                # Ajouter colonne N/ha calculée
                if n_col and n_col in df_sub.columns:
                    df_display.insert(len(df_display.columns), "N/ha (U)",
                        pd.to_numeric(df_sub[n_col], errors="coerce").fillna(0).apply(lambda x: f"{x:.0f} U"))
                elif "Dose_Ha" in df_sub.columns and "Unité_Dose" in df_sub.columns:
                    # Fallback : on indique la dose/ha si pas de colonne N spécifique
                    df_display.insert(len(df_display.columns), "N/ha (U)", "—")
                rename_map = {
                    "Date": "📅 Date",
                    "Nom_Produit": "🧪 Produit",
                    "Dose_Ha": "Dose/ha",
                    "Unité_Dose": "Unité",
                    "Surface_Travaillée_Ha": "Surface (ha)",
                }
                df_display.rename(columns={k: v for k, v in rename_map.items() if k in df_display.columns}, inplace=True)
                if "📅 Date" in df_display.columns:
                    df_display["📅 Date"] = df_display["📅 Date"].dt.strftime("%d/%m/%Y")

                # Render styled HTML table
                th_style = f'style="padding:10px 14px; background:{color_hex}; color:#fff; font-weight:bold; text-align:left; border:1px solid #eee; white-space:nowrap;"'
                td_even = 'style="padding:9px 14px; border:1px solid #eee; background:#f8f9fb;"'
                td_odd = 'style="padding:9px 14px; border:1px solid #eee; background:#ffffff;"'
                html = f'<p style="font-size:1em;font-weight:bold;color:{color_hex};margin:8px 0 4px 0;">{icon} {label} ({len(df_display)})</p>'
                html += f'<table style="border-collapse:collapse;width:100%;border-radius:8px;overflow:hidden;font-size:0.92em;box-shadow:0 2px 8px rgba(0,0,0,0.08);">'
                html += "<thead><tr>" + "".join(f"<th {th_style}>{c}</th>" for c in df_display.columns) + "</tr></thead><tbody>"
                for i, row in enumerate(df_display.itertuples(index=False)):
                    td = td_even if i % 2 == 1 else td_odd
                    html += "<tr>" + "".join(f"<td {td}>{v}</td>" for v in row) + "</tr>"
                html += "</tbody></table>"
                st.markdown(html, unsafe_allow_html=True)
                st.markdown("<br>", unsafe_allow_html=True)

            if status_col:
                df_prevue = df_ferti_p[df_ferti_p[status_col].astype(str).str.strip().str.lower().str.startswith("prév")].sort_values("Date")
                df_realisee = df_ferti_p[df_ferti_p[status_col].astype(str).str.strip().str.lower().str.startswith("réal")].sort_values("Date")
            else:
                df_prevue = pd.DataFrame()
                df_realisee = df_ferti_p.sort_values("Date")

            # Barre de progression si Dose X connue
            if dose_x_num and pd.notna(dose_x_num) and dose_x_num > 0 and not df_realisee.empty:
                n_col_jauge = next((c for c in ["N/ha", "N_ha", "Dose_Unitaire_N", "N"] if c in df_realisee.columns), None)
                if n_col_jauge:
                    total_n = pd.to_numeric(df_realisee[n_col_jauge], errors="coerce").fillna(0).sum()
                    pct_reel = total_n / dose_x_num
                    bar_color = "#e53935" if pct_reel > 1.0 else "#43a047"
                    bar_width = min(pct_reel * 100, 100)
                    st.markdown(f"""
                    <div style="margin-bottom:12px;">
                        <div style="font-size:0.9em;color:#555;margin-bottom:4px;">
                            N r\u00e9alis\u00e9 : <b>{total_n:.0f} U/ha</b> / Dose X : <b>{dose_x_num:.0f} U/ha</b>
                            \u2014 <span style="color:{bar_color};font-weight:bold;">{pct_reel*100:.0f}%</span>
                        </div>
                        <div style="background:#e0e0e0;border-radius:6px;height:14px;overflow:hidden;">
                            <div style="width:{bar_width:.0f}%;height:14px;background:{bar_color};border-radius:6px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    if pct_reel > 1.0:
                        st.warning(f"\u26a0\ufe0f **ATTENTION \u2014 D\u00e9passement de la Dose X !** N r\u00e9alis\u00e9 ({total_n:.0f} U/ha) d\u00e9passe la Dose X ({dose_x_num:.0f} U/ha) de **+{total_n - dose_x_num:.0f} U/ha** (+{(pct_reel-1)*100:.0f}%)")


            render_section(df_realisee, "Réalisées", "#2e7d32", "✅")
            render_section(df_prevue, "Prévues", "#1565c0", "📌")

        # Bouton PDF
        st.markdown("---")
        ppf_dict_detail = {}
        interv_list_detail = []
        if not df_ppf_all.empty:
            mask = df_ppf_all["ID_Parcelle"].astype(str).str.strip() == str(parcelle_sel).strip()
            row_ppf = df_ppf_all[mask]
            if not row_ppf.empty:
                ppf_dict_detail = row_ppf.iloc[0].to_dict()
        if not df_ferti_p.empty:
            for _, rr in df_ferti_p.iterrows():
                interv_list_detail.append({
                    "Date": rr.get("Date", "").strftime("%d/%m/%Y") if pd.notna(rr.get("Date")) else "—",
                    "Produit": rr.get("Nom_Produit", "—"),
                    "Dose_Ha": str(rr.get("Dose_Ha", "—")),
                })
        if ppf_dict_detail:
            if st.button("🖨️ Éditer le PDF PPF", use_container_width=True, type="primary"):
                with st.spinner("Génération du document..."):
                    pdf_data = generate_ppf_pdf(ppf_dict_detail, interv_list_detail)
                    if pdf_data:
                        st.download_button(
                            label="⬇️ Télécharger le PPF",
                            data=pdf_data,
                            file_name=f"PPF_{campagne_input}_{parcelle_sel}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
                    else:
                        st.error("Échec de la génération PDF.")

    else:
        st.info("👆 Cochez une parcelle dans le tableau ci-dessus pour voir le détail de ses interventions de fertilisation.")


# ═══════════════════════════════════════════════════════════════════════════════
# ─── ONGLET 2 : SAISIE PPF ────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
with tab_saisie:
    render_premium_header("✍️ Saisie du Plan Prévisionnel de Fumure", f"Campagne {campagne_input}", color="green")
    st.markdown("<br>", unsafe_allow_html=True)

    selected_parcelle = st.selectbox("🌾 Sélectionner la Parcelle", liste_parcelles, key="saisie_parcelle_sel")

    if not selected_parcelle:
        st.stop()

    p_data = parcels_meta[selected_parcelle]
    surface = p_data.get("Surface", 0.0)
    culture_asso = p_data.get("Culture", "Inconnue")
    variete_asso = p_data.get("Variete", "")
    ilot_pac = p_data.get("Ilot_PAC", "")

    st.info(f"📍 **{selected_parcelle}** — Ilot: {ilot_pac} | Surface: {surface} ha | Culture: {culture_asso} {variete_asso}")

    df_cipan = dl.get_ref_gren_cipan()
    df_prec = dl.get_ref_gren_precedents()
    df_humus = dl.get_ref_gren_humus()
    df_coef = dl.get_ref_gren_coef()

    existing_ppf = {}
    if not df_ppf_all.empty:
        mask = df_ppf_all["ID_Parcelle"].astype(str).str.strip() == str(selected_parcelle).strip()
        match = df_ppf_all[mask]
        if not match.empty:
            existing_ppf = match.iloc[0].to_dict()

    def get_val(col_name, default):
        val = existing_ppf.get(col_name)
        if pd.isna(val) or val is None or str(val).strip() == "":
            return default
        return val

    st.subheader("🌱 1. Détermination des Besoins (Pf)")
    col_n1, col_n2 = st.columns(2)

    with col_n1:
        culture_options = []
        if not df_coef.empty and "Culture" in df_coef.columns:
            culture_options = df_coef["Culture"].dropna().unique().tolist()
        def_cult = get_val("Culture", culture_asso)
        if def_cult not in culture_options and culture_options:
            culture_options.append(def_cult)
        sel_culture = st.selectbox("Culture", culture_options, index=culture_options.index(def_cult) if def_cult in culture_options else 0)
        sel_variete = st.text_input("Variété", value=get_val("Variété", variete_asso))
        obj_rendement = st.number_input("Objectif Rendement (Qx/Ha)", value=float(get_val("Objectif_Rendement_Qx_Ha", 100.0)), step=1.0)

    with col_n2:
        b_unit = 0.0
        if not df_coef.empty:
            match_b = df_coef[df_coef["Culture"] == sel_culture]
            if not match_b.empty:
                col_b_name = "Besoin__Culture_Unitaire "
                if col_b_name not in match_b.columns:
                    for c in match_b.columns:
                        if "Besoin" in c and "Unitaire" in c:
                            col_b_name = c; break
                try: b_unit = float(match_b[col_b_name].iloc[0])
                except: b_unit = 0.0
        b_unit_input = st.number_input("Besoin Unitaire (b)", value=float(get_val("Besoin__Culture_Unitaire ", b_unit)), format="%.3f")
        rf_input = st.number_input("Azote Fermeture Bilan (Rf)", value=float(get_val("Azote_Fermeture_Bilan(Rf)", 30.0)))
        besoin_culture_pf = obj_rendement * b_unit_input
        st.info(f"**👉 Besoin Culture (Pf) = {besoin_culture_pf:.1f} kg N/ha**")

    st.markdown("---")
    st.subheader("🌍 2. Évaluation des Fournitures")
    col_f1, col_f2 = st.columns(2)

    with col_f1:
        st.markdown("**Reliquats et Sol**")
        ri_input = st.number_input("Reliquat Sortie Hiver (Ri)", value=float(get_val("Reliquat_Sortie_Hiver(Ri)", 40.0)))
        sol_options = []
        if not df_humus.empty and "Type_sol" in df_humus.columns:
            sol_options = df_humus["Type_sol"].dropna().unique().tolist()
        def_sol = get_val("Type_sol", sol_options[0] if sol_options else "")
        sel_sol = st.selectbox("Type de sol", sol_options, index=sol_options.index(def_sol) if def_sol in sol_options else 0)
        mh_val = 0.0
        if not df_humus.empty:
            match_mh = df_humus[df_humus["Type_sol"] == sel_sol]
            if not match_mh.empty and "Minéralisation_Humus(Mh)" in match_mh.columns:
                try: mh_val = float(match_mh["Minéralisation_Humus(Mh)"].iloc[0])
                except: pass
        mh_input = st.number_input("Minéralisation Humus (Mh)", value=float(get_val("Minéralisation_Humus(Mh)", mh_val)))
        nirr_input = st.number_input("Fourniture Irrigation (Nirr)", value=float(get_val("Fourniture_Irrigation(Nirr)", 0.0)))
        pi_input = st.number_input("Azote déjà absorbé (Pi)", value=float(get_val("Azote_deja_aborbé(Pi)", 0.0)))

    with col_f2:
        st.markdown("**Précédent Cultural**")
        prec_options = []
        if not df_prec.empty and "Precedent_cultural" in df_prec.columns:
            prec_options = df_prec["Precedent_cultural"].dropna().unique().tolist()
        def_prec = get_val("Precedent_Cultural", p_data.get("Precedent", ""))
        if def_prec not in prec_options and prec_options:
            prec_options.append(def_prec if def_prec else "Inconnu")
        sel_prec = st.selectbox("Précédent Cultural", prec_options, index=prec_options.index(def_prec) if def_prec in prec_options else 0)
        gestion_residus = st.selectbox("Gestion des Résidus", ["Enfouis", "Exportés", "Brûlés"],
            index=["Enfouis", "Exportés", "Brûlés"].index(get_val("Gestion_Résidus", "Enfouis"))
            if get_val("Gestion_Résidus", "Enfouis") in ["Enfouis", "Exportés", "Brûlés"] else 0)
        mr_val = 0.0
        if not df_prec.empty:
            match_mr = df_prec[df_prec["Precedent_cultural"] == sel_prec]
            if not match_mr.empty and "Effet_précédent(Mr)" in match_mr.columns:
                try: mr_val = float(match_mr["Effet_précédent(Mr)"].iloc[0])
                except: pass
        mr_input = st.number_input("Effet Précédent (Mr)", value=float(get_val("Effet_précédent(Mr)", mr_val)))

        st.markdown("**CIPAN / Couverts**")
        cipan_types = []
        if not df_cipan.empty and "CIPAN_Type" in df_cipan.columns:
            cipan_types = ["Aucun"] + df_cipan["CIPAN_Type"].dropna().unique().tolist()
        def_cipan = get_val("CIPAN_Type", "Aucun")
        sel_cipan = st.selectbox("Type CIPAN", cipan_types, index=cipan_types.index(def_cipan) if def_cipan in cipan_types else 0)
        dev_cipan_options = []
        if sel_cipan != "Aucun" and not df_cipan.empty:
            match_c = df_cipan[df_cipan["CIPAN_Type"] == sel_cipan]
            if "Développement_CIPAN" in match_c.columns:
                dev_cipan_options = match_c["Développement_CIPAN"].dropna().unique().tolist()
        def_dev = get_val("Développement_CIPAN", dev_cipan_options[0] if dev_cipan_options else "")
        if def_dev not in dev_cipan_options and dev_cipan_options:
            def_dev = dev_cipan_options[0]
        sel_dev_cipan = st.selectbox("Développement CIPAN",
            dev_cipan_options if dev_cipan_options else ["N/A"],
            index=dev_cipan_options.index(def_dev) if def_dev in dev_cipan_options else 0)
        mrci_val = 0.0
        if sel_cipan != "Aucun" and not df_cipan.empty:
            match_mrci = df_cipan[(df_cipan["CIPAN_Type"] == sel_cipan) & (df_cipan["Développement_CIPAN"] == sel_dev_cipan)]
            if not match_mrci.empty and "Effet_CIPAN(MrCi)" in match_mrci.columns:
                try: mrci_val = float(match_mrci["Effet_CIPAN(MrCi)"].iloc[0])
                except: pass
        mrci_input = st.number_input("Effet CIPAN (MrCi)", value=float(get_val("Effet_CIPAN(MrCi)", mrci_val)))

    st.markdown("---")

    besoins_totaux = besoin_culture_pf + rf_input
    fournitures_totales = pi_input + ri_input + mh_input + mr_input + mrci_input + nirr_input
    dose_x = max(0, besoins_totaux - fournitures_totales)

    col_k1, col_k2, col_k3 = st.columns(3)
    col_k1.metric("Total Besoins", f"{besoins_totaux:.1f} kg N/ha")
    col_k2.metric("Total Fournitures", f"{fournitures_totales:.1f} kg N/ha")
    col_k3.metric("🎯 Dose X (Objectif)", f"{dose_x:.1f} kg N/ha")

    ppf_dict = {
        "Campagne": str(campagne_input),
        "ID_Parcelle": str(selected_parcelle),
        "îlot PAC": str(ilot_pac),
        "Surface_Référence_Ha": surface,
        "Culture": str(sel_culture),
        "Variété": str(sel_variete),
        "Gestion_Résidus": str(gestion_residus),
        "Type_sol": str(sel_sol),
        "Objectif_Rendement_Qx_Ha": float(obj_rendement),
        "Precedent_Cultural": str(sel_prec),
        "CIPAN_Type": str(sel_cipan),
        "Développement_CIPAN": str(sel_dev_cipan),
        "Besoin__Culture_Unitaire ": float(b_unit_input),
        "Besoin_Culture(Pf)": float(besoin_culture_pf),
        "Azote_Fermeture_Bilan(Rf)": float(rf_input),
        "Azote_deja_aborbé(Pi)": float(pi_input),
        "Reliquat_Sortie_Hiver(Ri)": float(ri_input),
        "Minéralisation_Humus(Mh)": float(mh_input),
        "Effet_précédent(Mr)": float(mr_input),
        "Effet_CIPAN(MrCi)": float(mrci_input),
        "Fourniture_Irrigation(Nirr)": float(nirr_input),
        "Dose_X": float(dose_x),
    }

    if st.button("💾 Sauvegarder les Paramètres du PPF", use_container_width=True, type="primary"):
        with st.spinner("Sauvegarde..."):
            if dl.update_ppf(ppf_dict):
                st.success("PPF Sauvegardé avec succès ! Consultez l'onglet Consultation pour voir le bilan.")
                st.rerun()

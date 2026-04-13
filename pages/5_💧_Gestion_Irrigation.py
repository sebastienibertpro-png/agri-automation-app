import streamlit as st
import pandas as pd
import tempfile
import os
from report_gen import ReportGenerator
from email_utils import send_email_with_attachment
from shared import init_campaign_selector, render_brand_page_header

st.set_page_config(page_title="Gestion Irrigation", page_icon="💧", layout="wide")

render_brand_page_header("Gestion de l'Irrigation", "Optimisez vos apports en eau et gérez vos relevés ✨", icon="💧")

active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

def calculate_summary_table(df_filtered, selected_nets):
    aggs = []
    total_m3_global = 0
    total_ha_global = 0
    
    for net in sorted(selected_nets):
        net_data = df_filtered[df_filtered['Reseau_type'] == net]
        if net_data.empty: continue
        
        total_m3 = net_data['Conso_Reelle_m3'].sum()
        total_m3_global += total_m3
        
        unique_meters = net_data.drop_duplicates(subset=['ID_cCompteur' if 'ID_cCompteur' in net_data.columns else 'ID_Compteur'])
        
        total_ha = 0
        if 'Ha_irrigués_compteur' in unique_meters.columns:
            total_ha = pd.to_numeric(unique_meters['Ha_irrigués_compteur'], errors='coerce').fillna(0).sum()
        
        total_ha_global += total_ha
        
        mm_ha = 0
        if total_ha > 0:
             mm_ha = (total_m3 / 10) / total_ha
             
        aggs.append({
            'Réseau': net,
            'Total m3': total_m3,
            'Volume (mm/ha)': mm_ha
        })
        
    df_agg = pd.DataFrame(aggs)
    
    if not df_agg.empty:
        mm_ha_global = 0
        if total_ha_global > 0:
             mm_ha_global = (total_m3_global / 10) / total_ha_global
        
        total_row = pd.DataFrame([{
            'Réseau': 'TOTAL',
            'Total m3': total_m3_global,
            'Volume (mm/ha)': mm_ha_global
        }])
        df_agg = pd.concat([df_agg, total_row], ignore_index=True)
        
    return df_agg

try:
    with st.spinner("Chargement des données d'irrigation..."):
        df_conso = active_loader.get_consumption_data(selected_campaign)

    if df_conso.empty:
        st.info(f"Aucune donnée d'irrigation trouvée pour la campagne {selected_campaign}.")
    else:
        networks = sorted(df_conso['Reseau_type'].unique())
        
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            selected_nets = st.multiselect("Filtre Réseau", networks, default=networks)
        
        df_net_filtered = df_conso[df_conso['Reseau_type'].isin(selected_nets)]
        available_meters = sorted(df_net_filtered['ID_Compteur'].unique()) if not df_net_filtered.empty else []
        
        with col_f2:
            selected_meters = st.multiselect("Filtre Compteurs", available_meters, default=available_meters)
            
        df_filtered = df_net_filtered[df_net_filtered['ID_Compteur'].isin(selected_meters)]
        
        french_months = {
            1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai', 6: 'Juin',
            7: 'Juillet', 8: 'Août', 9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'
        }
        
        reading_months = sorted(df_filtered['Date_Relevé'].dt.month.dropna().unique())
        month_options = []
        month_map = {} 
        
        for m in reading_months:
            conso_m_idx = m - 1 if m > 1 else 12
            label = f"{french_months[conso_m_idx]} (Relevé de {french_months[m]})"
            month_options.append(label)
            month_map[label] = m 
            
        with col_f1:
            selected_month_label = st.selectbox("📅 Mois de Consommation (Bilan Mensuel)", month_options)
            selected_reading_month = month_map[selected_month_label] if selected_month_label else None
            conso_month_name = selected_month_label.split(" (")[0] if selected_month_label else ""
        
        if df_filtered.empty:
            st.warning("Veuillez sélectionner au moins un réseau.")
        else:
            st.markdown(f"#### 📊 Consommation Campagne {selected_campaign}")
            
            df_agg = calculate_summary_table(df_filtered, selected_nets)
            
            if not df_agg.empty:
                df_display = df_agg.copy()
                df_display['Total m3'] = df_display['Total m3'].apply(lambda x: f"{x:.1f}")
                df_display['Volume (mm/ha)'] = df_display['Volume (mm/ha)'].apply(lambda x: f"{x:.1f}")
                
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("📄 Exporter Synthèse Multiannuelle PDF", key="btn_global_irr_export"):
                with st.spinner("Génération de la synthèse globale en cours..."):
                    df_releves = active_loader.get_releves_compteurs()
                    df_releves['Date_Relevé'] = pd.to_datetime(df_releves['Date_Relevé'], errors='coerce', dayfirst=True)
                    all_camps = [int(x) for x in df_releves['Date_Relevé'].dt.year.dropna().unique()]
                    all_camps.sort(reverse=True)
                    
                    campaign_summaries = {}
                    for camp in all_camps:
                        df_camp_conso = active_loader.get_consumption_data(camp)
                        if not df_camp_conso.empty:
                            df_camp_net_filtered = df_camp_conso[df_camp_conso['Reseau_type'].isin(selected_nets)]
                            df_camp_filtered = df_camp_net_filtered[df_camp_net_filtered['ID_Compteur'].isin(selected_meters)]
                            if not df_camp_filtered.empty:
                                df_camp_agg = calculate_summary_table(df_camp_filtered, selected_nets)
                                if not df_camp_agg.empty:
                                    campaign_summaries[camp] = df_camp_agg
                    
                    if campaign_summaries:
                        with tempfile.TemporaryDirectory() as tmpdirname:
                            global_fname = "Synthese_Globale_Irrigation.pdf"
                            global_fpath = os.path.join(tmpdirname, global_fname)
                            gen = ReportGenerator(global_fpath)
                            gen.generate_global_irrigation_summary(campaign_summaries)
                            
                            with open(global_fpath, "rb") as f:
                                st.download_button(
                                    label="⬇️ Télécharger Synthèse Multiannuelle",
                                    data=f,
                                    file_name=global_fname,
                                    mime="application/pdf",
                                    key="dl_global_irr"
                                )
                    else:
                        st.warning("Aucune donnée d'irrigation à exporter pour les filtres actuels.")
            st.markdown("<br>", unsafe_allow_html=True)
            
            for net in sorted(selected_nets):
                net_data = df_filtered[df_filtered['Reseau_type'] == net]
                if net_data.empty: continue
                
                with st.expander(f"Action pour le réseau : {net}"):
                    st.markdown("#### 📜 Bilan Campagne")
                    col_irr1, col_irr2 = st.columns(2)
                
                    with col_irr1:
                        if st.button(f"📄 PDF Campagne - {net}", key=f"btn_pdf_camp_{net}"):
                            with tempfile.TemporaryDirectory() as tmpdirname:
                                fname = f"Bilan_Campagne_Irrigation_{selected_campaign}_{net}.pdf"
                                fpath = os.path.join(tmpdirname, fname)
                                gen = ReportGenerator(fpath)
                                gen.generate_irrigation_report(selected_campaign, net, net_data)
                                with open(fpath, "rb") as f:
                                    st.download_button(label=f"⬇️ Télécharger PDF Campagne", data=f, file_name=fname, mime="application/pdf", key=f"dl_camp_{net}")

                    with col_irr2:
                        if net in ["CUMA_Irrigation", "ASA_SaintLoup"]:
                            recipient = net_data['Mail_Contact-Reseau'].iloc[0] if not net_data.empty and 'Mail_Contact-Reseau' in net_data.columns else None
                            
                            if st.button(f"📧 Envoyer Bilan Campagne - {net}", key=f"btn_mail_camp_{net}"):
                                if not recipient:
                                    st.error(f"Aucune adresse email trouvée pour le réseau {net}.")
                                else:
                                    with st.spinner(f"Envoi du bilan campagne à : {recipient}..."):
                                        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                                            fpath = tmp_file.name
                                            gen = ReportGenerator(fpath)
                                            gen.generate_irrigation_report(selected_campaign, net, net_data)
                                            
                                            sender_email = st.secrets.get("GMAIL_USER")
                                            sender_app_password = st.secrets.get("GMAIL_PASSWORD")
                                            
                                            if not sender_email:
                                                try:
                                                    sender_email = st.secrets["connections"]["gsheets"]["GMAIL_USER"]
                                                    sender_app_password = st.secrets["connections"]["gsheets"]["GMAIL_PASSWORD"]
                                                except Exception:
                                                    pass
                                                
                                            if not sender_email or not sender_app_password:
                                                st.error("Identifiants d'envoi d'email introuvables (GMAIL_USER, GMAIL_PASSWORD).")
                                            else:
                                                subject = f"Bilan Fin de Campagne Irrigation - {net} - {selected_campaign}"
                                                body_text = f"Bonjour,\n\nVeuillez trouver ci-joint le bilan de fin de campagne d'irrigation pour l'année {selected_campaign} concernant le réseau {net}.\n\nCordialement,\nAgri Automation"
                                                
                                                success = send_email_with_attachment(
                                                    sender_email,
                                                    sender_app_password,
                                                    recipient,
                                                    subject,
                                                    body_text,
                                                    fpath
                                                )
                                                
                                                if success:
                                                    st.success("Email envoyé avec succès !")
                                                else:
                                                    st.error("L'envoi a échoué. Consultez les logs locaux.")

                    st.divider()
                    st.markdown(f"#### 📅 Bilan Mensuel : {conso_month_name}")
                    col_irr_m1, col_irr_m2 = st.columns(2)
                    
                    monthly_data = net_data[net_data['Date_Relevé'].dt.month == selected_reading_month]
                    
                    with col_irr_m1:
                        if st.button(f"📄 PDF Mensuel - {net}", key=f"btn_pdf_month_{net}"):
                            with tempfile.TemporaryDirectory() as tmpdirname:
                                fname = f"Bilan_Mensuel_{conso_month_name}_{selected_campaign}_{net}.pdf"
                                fpath = os.path.join(tmpdirname, fname)
                                gen = ReportGenerator(fpath)
                                gen.generate_monthly_network_report(selected_campaign, conso_month_name, net, monthly_data)
                                with open(fpath, "rb") as f:
                                    st.download_button(label=f"⬇️ Télécharger PDF Mensuel", data=f, file_name=fname, mime="application/pdf", key=f"dl_month_{net}")
                                    
                    with col_irr_m2:
                        if net in ["CUMA_Irrigation", "ASA_SaintLoup"]:
                            recipient = monthly_data['Mail_Contact-Reseau'].iloc[0] if not monthly_data.empty and 'Mail_Contact-Reseau' in monthly_data.columns else None
                            
                            if st.button(f"📧 Envoyer Bilan Mensuel - {net}", key=f"btn_mail_month_{net}"):
                                if not recipient:
                                    st.error(f"Aucune adresse email trouvée pour le réseau {net}.")
                                else:
                                    with st.spinner(f"Envoi du bilan mensuel à : {recipient}..."):
                                        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                                            fpath = tmp_file.name
                                            gen = ReportGenerator(fpath)
                                            gen.generate_monthly_network_report(selected_campaign, conso_month_name, net, monthly_data)
                                            
                                            sender_email = st.secrets.get("GMAIL_USER")
                                            app_password = st.secrets.get("GMAIL_PASSWORD")
                                            
                                            if not sender_email:
                                                try:
                                                    sender_email = st.secrets["connections"]["gsheets"]["GMAIL_USER"]
                                                    app_password = st.secrets["connections"]["gsheets"]["GMAIL_PASSWORD"]
                                                except:
                                                    pass
                                            
                                            if not sender_email or not app_password:
                                                st.error("Identifiants Gmail manquants.")
                                            else:
                                                success = send_email_with_attachment(
                                                    sender_email, app_password, recipient,
                                                    f"Bilan Irrigation Mensuel ({conso_month_name}) - {net}",
                                                    f"Bonjour,\n\nVeuillez trouver ci-joint le bilan de consommation mensuel pour le réseau {net} (Mois concerné : {conso_month_name}).\n\nCordialement.",
                                                    fpath
                                                )
                                                if success: st.success(f"Email envoyé à {recipient} !")
                                                else: st.error("Échec de l'envoi.")
                                            if os.path.exists(fpath): os.remove(fpath)
                        else:
                            st.info("Privé : Email non requis.")

except Exception as e:
    st.error(f"Erreur lors du traitement de l'irrigation : {e}")

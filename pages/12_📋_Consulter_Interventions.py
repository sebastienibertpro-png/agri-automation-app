import streamlit as st
import pandas as pd
from datetime import datetime
from shared import init_campaign_selector

st.set_page_config(page_title="Consulter mes interventions", page_icon="📋", layout="wide")

st.title("📋 Consulter mes Interventions")

# 1. Utilisation du sélecteur de campagne partagé
# Il injecte le selectbox dans la barre latérale et renvoie les données filtrées
active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# 2. Filtres supplémentaires en haut de page
col_f1, col_f2 = st.columns([2, 1])

with col_f1:
    options_parcelles = ["Toutes"] + list(available_parcelles)
    selected_parcelle = st.selectbox("🌾 Choisir la Parcelle", options_parcelles)

# Filtrage par parcelle
if selected_parcelle != "Toutes":
    df_filtered = df_campaign[df_campaign['ID_Parcelle'] == selected_parcelle].copy()
else:
    df_filtered = df_campaign.copy()

if df_filtered.empty:
    st.info("Aucune intervention enregistrée pour cette sélection.")
    st.stop()

# 3. Logique de regroupement (Similaire ITK)
# On regroupe par Date, Parcelle, Nature_Intervention
# Mais on garde plus de colonnes

def group_interventions(df):
    # Préparation des colonnes pour le regroupement
    # Certaines colonnes sont identiques pour le groupe, d'autres doivent être agrégées (produits, doses)
    
    # On trie par date
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
    df = df.sort_values(by=['Date', 'ID_Parcelle'], ascending=[False, True])
    
    # Colonnes clés pour le regroupement
    group_cols = ['Date', 'ID_Parcelle', 'Nature_Intervention']
    
    # Colonnes à agréger par concaténation
    agg_cols = {
        'Nom_Produit': lambda x: "\n".join([str(i) for i in x if pd.notna(i) and str(i).strip() != ""]),
        'Type_Intervention': lambda x: "\n".join(list(dict.fromkeys([str(i) for i in x if pd.notna(i) and str(i).strip() != ""]))), # Unique types
        'Dose_Ha': lambda x: "\n".join([f"{i}" for i in x if pd.notna(i)]),
        'Unité_Dose': lambda x: "\n".join([str(i) for i in x if pd.notna(i)]),
        'Cible': lambda x: "\n".join(list(dict.fromkeys([str(i) for i in x if pd.notna(i) and str(i).strip() != ""]))),
        'ID_Intervention': lambda x: list(x.dropna().unique()), # Garder les IDs pour la suppression
    }
    
    # Colonnes à garder (on prend la première valeur du groupe)
    keep_cols = [
        'Surface_Travaillée_Ha', 'Tracteur', 'Outil', 'Stade_Culture', 
        'Conditions_Météo', 'Observations', 'Statut_Intervention'
    ]
    
    for col in keep_cols:
        if col in df.columns:
            agg_cols[col] = 'first'

    # Exécution du regroupement
    df_grouped = df.groupby(group_cols, as_index=False).agg(agg_cols)
    
    return df_grouped

df_display = group_interventions(df_filtered)

# 4. Affichage et Suppression
st.markdown("### Journal Détaillé")
st.info("💡 Les interventions sont regroupées par date et nature (comme dans l'ITK).")

# Configuration des colonnes pour st.data_editor (permet la sélection)
column_config = {
    "Select": st.column_config.CheckboxColumn("Sélect.", default=False),
    "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
    "ID_Parcelle": "Parcelle",
    "Nature_Intervention": "Nature",
    "Type_Intervention": st.column_config.TextColumn("Type", help="Type d'intervention (Herbicide, Fongicide...)"),
    "Nom_Produit": st.column_config.TextColumn("Produits", help="Produits utilisés"),
    "Dose_Ha": "Dose/ha",
    "Unité_Dose": "Unité",
    "Surface_Travaillée_Ha": "Surf. (ha)",
    "Tracteur": "Tracteur",
    "Outil": "Outil",
    "Statut_Intervention": "Statut",
    "ID_Intervention": None # On cache les IDs techniques
}

# On ajoute une colonne de sélection manuelle car st.data_editor ne renvoie pas facilement les lignes sélectionnées via checkbox native
if 'to_delete' not in st.session_state:
    st.session_state.to_delete = []

# Ajout de la colonne de sélection au DataFrame
df_display.insert(0, "Sélect. 🗑️", False)

# Utilisation de data_editor pour permettre la sélection
edited_df = st.data_editor(
    df_display,
    column_config=column_config,
    disabled=[c for c in df_display.columns if c != "Sélect. 🗑️"],
    hide_index=True,
    use_container_width=True,
    key="interventions_editor"
)

# 5. Traitement de la suppression
selected_rows = edited_df[edited_df["Sélect. 🗑️"] == True]

if not selected_rows.empty:
    st.warning(f"⚠️ Vous avez sélectionné {len(selected_rows)} groupe(s) d'interventions pour suppression.")
    if st.button("🔥 Confirmer la suppression définitive", type="primary"):
        all_ids_to_del = []
        for ids in selected_rows['ID_Intervention']:
            all_ids_to_del.extend(ids)
            
        if all_ids_to_del:
            with st.spinner("Suppression en cours..."):
                success = active_loader.delete_interventions(all_ids_to_del)
                if success:
                    st.success(f"{len(all_ids_to_del)} ligne(s) supprimée(s) du registre.")
                    st.balloons()
                    # Forcer le rechargement
                    st.cache_data.clear()
                    active_loader.clear_cache()
                    st.rerun()
                else:
                    st.error("Erreur lors de la suppression.")
else:
    st.write("---")
    st.caption("Sélectionnez des lignes via la première colonne pour les supprimer.")

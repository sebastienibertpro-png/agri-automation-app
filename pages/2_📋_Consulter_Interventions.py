import streamlit as st
import pandas as pd
from datetime import datetime
from shared import init_campaign_selector

st.set_page_config(page_title="Consulter mes interventions", page_icon="📋", layout="wide")

st.title("📋 Consulter mes Interventions")

# 1. Utilisation du sélecteur de campagne partagé
active_loader, selected_campaign, df_campaign, available_parcelles = init_campaign_selector()

# Récupération des prix des produits
product_prices = active_loader.get_product_prices(selected_campaign)

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
def group_interventions(df):
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
    df = df.sort_values(by=['Date', 'ID_Parcelle'], ascending=[False, True])
    group_cols = ['Date', 'ID_Parcelle', 'Nature_Intervention']
    agg_cols = {
        'Nom_Produit': lambda x: "\n".join([str(i) for i in x if pd.notna(i) and str(i).strip() != ""]),
        'Type_Intervention': lambda x: "\n".join(list(dict.fromkeys([str(i) for i in x if pd.notna(i) and str(i).strip() != ""]))),
        'Dose_Ha': lambda x: "\n".join([f"{i}" for i in x if pd.notna(i)]),
        'Unité_Dose': lambda x: "\n".join([str(i) for i in x if pd.notna(i)]),
        'Cible': lambda x: "\n".join(list(dict.fromkeys([str(i) for i in x if pd.notna(i) and str(i).strip() != ""]))),
        'ID_Intervention': lambda x: list(x.dropna().unique()),
    }
    keep_cols = ['Surface_Travaillée_Ha', 'Tracteur', 'Outil', 'Stade_Culture', 'Conditions_Météo', 'Observations', 'Statut_Intervention']
    for col in keep_cols:
        if col in df.columns: agg_cols[col] = 'first'
    def calculate_cost(row):
        prods = str(row['Nom_Produit']).split('\n')
        doses = str(row['Dose_Ha']).split('\n')
        total_cost = 0.0
        for p, d in zip(prods, doses):
            p_norm = p.strip().upper()
            if p_norm in product_prices:
                try:
                    total_cost += float(d) * product_prices[p_norm]
                except:
                    pass
        return total_cost

    df_grouped = df.groupby(group_cols, as_index=False).agg(agg_cols)
    
    # Ajout du coût à l'ha
    df_grouped['€/ha'] = df_grouped.apply(calculate_cost, axis=1).round(1)
    
    return df_grouped

df_display = group_interventions(df_filtered)

# 4. Affichage
st.markdown("### Journal Détaillé")

column_config = {
    "Select": st.column_config.CheckboxColumn("Sélect.", default=False),
    "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
    "ID_Parcelle": "Parcelle",
    "Nature_Intervention": "Nature",
    "Type_Intervention": st.column_config.TextColumn("Type"),
    "Nom_Produit": st.column_config.TextColumn("Produits"),
    "Dose_Ha": st.column_config.TextColumn("Dose/ha"),
    "Unité_Dose": st.column_config.TextColumn("Unité"),
    "Surface_Travaillée_Ha": "Surf. (ha)",
    "€/ha": st.column_config.NumberColumn("€/ha", format="%.1f €"),
    "Statut_Intervention": "Statut",
    "ID_Intervention": None,
    "Sélect. ✅": "Sélect."
}

df_display.insert(0, "Sélect. ✅", False)

edited_df = st.data_editor(
    df_display,
    column_config=column_config,
    disabled=[c for c in df_display.columns if c != "Sélect. ✅"],
    hide_index=True,
    use_container_width=True,
    key="interventions_editor"
)

# Affichage du total
total_cost_ha = edited_df['€/ha'].sum()
st.metric("💰 Coût Total à l'ha pour la sélection", f"{total_cost_ha:.1f} €")

# 5. Actions : Suppression et Modification
selected_rows = edited_df[edited_df["Sélect. ✅"] == True]

if not selected_rows.empty:
    col_act1, col_act2 = st.columns(2)
    
    with col_act1:
        st.warning(f"⚠️ {len(selected_rows)} sélection(s)")
        if st.button("🔥 Supprimer définitivement", type="primary", use_container_width=True):
            all_ids = []
            for ids in selected_rows['ID_Intervention']: all_ids.extend(ids)
            if active_loader.delete_interventions(all_ids):
                st.success("Supprimé !")
                st.cache_data.clear()
                active_loader.clear_cache()
                st.rerun()

    with col_act2:
        st.info("Modifier l'entrée")
        if st.button("✍️ Modifier cette intervention", use_container_width=True):
            if len(selected_rows) > 1:
                st.error("Sélectionnez une seule ligne pour modifier.")
            else:
                st.session_state.edit_intervention = selected_rows.iloc[0].to_dict()
                st.switch_page("pages/1_✍️_Saisie_Intervention.py")
else:
    st.write("---")
    st.caption("Cochez une ligne pour la supprimer ou la modifier.")

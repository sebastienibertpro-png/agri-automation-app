import streamlit as st
import pandas as pd
from datetime import datetime
from shared import init_campaign_selector, inject_premium_css, render_premium_header

st.set_page_config(page_title="Consulter mes interventions", page_icon="📋", layout="wide")
inject_premium_css()

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
def format_traitement(row):
    prod = str(row.get('Nom_Produit', ''))
    typ = str(row.get('Type_Intervention', ''))
    
    if (pd.isna(row.get('Nom_Produit')) or prod.strip() in ["", "None", "nan"]):
        # Pas de produit
        return f"🔧 {typ}" if pd.notna(row.get('Type_Intervention')) and typ.strip() not in ["", "None", "nan"] else ""
        
    dose = row.get('Dose_Ha', '')
    unit = row.get('Unité_Dose', '')
    cible = row.get('Cible', '')
    
    parts = [f"🔹 {prod.strip()}"]
    
    cost_part = ""
    
    # Dose
    if pd.notna(dose) and str(dose).strip() not in ["", "None", "nan", "0", "0.0"]:
        unit_str = str(unit) if pd.notna(unit) and str(unit).strip() not in ["", "None", "nan"] else ""
        parts.append(f"➔ {dose} {unit_str}".strip())
        
        # Calcul du coût
        p_norm = prod.strip().upper()
        if 'product_prices' in globals() and p_norm in product_prices:
            try:
                cst = float(dose) * product_prices[p_norm]
                cost_part = f"💰 {cst:.1f} €"
            except:
                pass
        
    # Type (Herbicide, Fongicide...)
    if pd.notna(typ) and typ.strip() not in ["", "None", "nan"]:
         parts.append(f"({typ.strip()})")
         
    # Cible
    if pd.notna(cible) and cible.strip() not in ["", "None", "nan"]:
         parts.append(f"🎯 {cible.strip()}")
         
    # Ajouter le coût à la fin
    if cost_part:
         parts.append(cost_part)
         
    return " ".join(parts)

def group_interventions(df):
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
    df = df.sort_values(by=['Date', 'ID_Parcelle'], ascending=[False, True])
    
    # Création de la ligne de détail agglomérée par produit
    df['Detail_Traitement'] = df.apply(format_traitement, axis=1)
    
    group_cols = ['Date', 'ID_Parcelle', 'Nature_Intervention']
    agg_cols = {
        'Detail_Traitement': lambda x: [str(i) for i in x if str(i).strip() != ""],
        'ID_Intervention': lambda x: list(x.dropna().unique()),
    }
    
    keep_cols = ['Surface_Travaillée_Ha', 'Tracteur', 'Outil', 'Stade_Culture', 'Conditions_Météo', 'Observations', 'Statut_Intervention']
    for col in keep_cols:
        if col in df.columns: agg_cols[col] = 'first'
        
    def calculate_cost(row):
        prods = str(row.get('Nom_Produit', '')).split('\n')
        doses = str(row.get('Dose_Ha', '')).split('\n')
        total_cost = 0.0
        for p, d in zip(prods, doses):
            p_norm = p.strip().upper()
            if p_norm in product_prices:
                try:
                    total_cost += float(d) * product_prices[p_norm]
                except:
                    pass
        return total_cost

    # Ajout de la colonne au DataFrame principal avant groupement
    df['€/ha'] = df.apply(calculate_cost, axis=1)
    agg_cols['€/ha'] = 'sum'

    df_grouped = df.groupby(group_cols, as_index=False).agg(agg_cols)
    
    # Nettoyage doublons éventuels dans les listes de traitements et suppression des listes vides
    df_grouped['Detail_Traitement'] = df_grouped['Detail_Traitement'].apply(lambda lst: list(dict.fromkeys(lst)))
    
    return df_grouped

df_display = group_interventions(df_filtered)

# Nettoyer l'affichage général (remplacer les "None" par rien)
df_display = df_display.replace(["None", "nan", "<NA>", "NaN"], "")
df_display = df_display.fillna("")

from shared import init_campaign_selector, inject_premium_css, render_premium_header

# 4. Affichage
render_premium_header("📖 Journal Détaillé", "Cochez une ligne pour agir ✍️🔥", color="blue")

column_config = {
    "Select": st.column_config.CheckboxColumn("Sélect.", default=False),
    "Date": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
    "ID_Parcelle": "Parcelle",
    "Nature_Intervention": "Nature",
    "Detail_Traitement": st.column_config.ListColumn("Produits & Détails", width="large"),
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
        st.warning(f"⚠️ {len(selected_rows)} ligne(s) sélectionnée(s) pour suppression")
        if st.button("🔥 Supprimer définitivement", type="primary", use_container_width=True):
            all_ids = []
            for ids in selected_rows['ID_Intervention']: all_ids.extend(ids)
            if active_loader.delete_interventions(all_ids):
                st.success("Supprimé !")
                st.cache_data.clear()
                active_loader.clear_cache()
                st.rerun()

    with col_act2:
        if len(selected_rows) > 1:
            st.warning("ℹ️ Pour modifier, veuillez ne cocher **qu'une seule ligne** à la fois.")
            st.button("✍️ Modifier cette intervention", disabled=True, use_container_width=True)
        else:
            st.success("✅ Ligne prête à être modifiée")
            if st.button("✍️ Modifier cette intervention", use_container_width=True):
                st.session_state.edit_intervention = selected_rows.iloc[0].to_dict()
                st.switch_page("pages/1_✍️_Intervention_et_Assistant_Vocal.py")
else:
    st.write("---")
    st.caption("Cochez une ligne pour la supprimer ou la modifier.")
